import asyncio
from dataclasses import replace
from types import TracebackType

from .domain import Candle, CandleReceived, ExecutionApplied, Message, OrderSnapshot, OrderUpdated
from .events import EventBus
from .ledger import PositionLedger
from .lifecycle import LifecycleStore, StrategySnapshot, StrategyStatus
from .optimization import (
    HistoryRequest, HistorySource, OptimizationError, OptimizationFunction,
    OptimizationHandle, OptimizationInput, Optimizer, ProcessOptimizer,
    validate_history, validate_worker,
)
from .persistence import DurableJournal
from .orders import OrderManager, strategy_recipient
from .state import JsonObject, json_object
from .simulation import Gateway
from .strategy import AutoStrategy, StrategyContext
from .time import ManualClock


class _StrategyRunner:
    def __init__(self, strategy: AutoStrategy, runtime: 'TradingRuntime') -> None:
        self.strategy = strategy
        self.runtime = runtime
        self.context: StrategyContext | None = None
        self.status = StrategyStatus.RUNNING
        self.reason: str | None = None
        self.resume_after: StrategyStatus | None = None
        self.lock = asyncio.Lock()
        self.callback_task: asyncio.Task[object] | None = None
        self.job: asyncio.Task[JsonObject] | None = None

    def checkpoint(self, status: StrategyStatus, reason: str | None = None,
                   resume_after: StrategyStatus | None = None) -> None:
        strategy = self.strategy
        snapshot = StrategySnapshot(
            strategy.name, f'{type(strategy).__module__}.{type(strategy).__qualname__}',
            strategy.state_version, strategy.instruments, strategy.parameters,
            json_object(strategy.save_state()), status, reason, resume_after,
        )
        self.runtime.lifecycle.save(snapshot, self.runtime.clock.now())
        self.status, self.reason, self.resume_after = status, reason, resume_after

    def transition(self, status: StrategyStatus, reason: str | None = None,
                   resume_after: StrategyStatus | None = None) -> None:
        try:
            self.checkpoint(status, reason, resume_after)
        except Exception as error:
            self.fail(error)
            raise

    def checkpoint_current(self) -> None:
        self.checkpoint(self.status, self.reason, self.resume_after)

    def fail(self, error: Exception) -> None:
        self.status, self.reason, self.resume_after = StrategyStatus.FAILED, str(error), None
        if self.job is not None and not self.job.done() and self.job is not asyncio.current_task():
            self.job.cancel()
        try:
            self.checkpoint_current()
        except Exception as checkpoint_error:
            saved = self.runtime.lifecycle.get(self.strategy.name)
            try:
                if saved is not None:
                    self.runtime.lifecycle.save(replace(saved, status=self.status,
                                                        reason=self.reason, resume_after=None),
                                                self.runtime.clock.now())
            except Exception as fallback_error:
                error.add_note(f'failed to persist failure status: {fallback_error}')
            error.add_note(f'failed computation checkpoint: {checkpoint_error}')

    async def handle(self, message: Message) -> None:
        if self.context is None:
            raise RuntimeError('strategy has no context')
        async with self.lock:
            self.callback_task = asyncio.current_task()
            try:
                if isinstance(message, CandleReceived):
                    if self.status is not StrategyStatus.RUNNING or not self.runtime.orders.ready:
                        return
                    await self.strategy.on_candle(self.context, message.candle)
                elif isinstance(message, ExecutionApplied):
                    await self.strategy.on_fill(self.context, message.execution)
                elif isinstance(message, OrderUpdated):
                    await self.strategy.on_order(self.context, message.order)
                else:
                    raise ValueError('unexpected strategy message')
                self.checkpoint_current()
            except Exception as error:
                self.fail(error)
                raise
            finally:
                self.callback_task = None

    async def initialize(self, saved: StrategySnapshot | None) -> None:
        if self.context is None:
            raise RuntimeError('strategy has no context')
        async with self.lock:
            self.callback_task = asyncio.current_task()
            try:
                if saved is None:
                    await self.strategy.on_start(self.context)
                else:
                    await self.strategy.on_restore(self.context)
                    if self.status is StrategyStatus.PAUSED and self.job is None:
                        preferred = StrategyStatus.PAUSED if saved.status is StrategyStatus.OPTIMIZING else saved.status
                        reason = 'optimization interrupted by restart' if saved.status is StrategyStatus.OPTIMIZING else saved.reason
                        self.checkpoint(preferred, reason)
                        return
                self.checkpoint_current()
            except Exception as error:
                self.fail(error)
                raise
            finally:
                self.callback_task = None


class TradingRuntime:
    """Offline composition root; finance, lifecycle and CPU jobs have separate owners."""

    def __init__(self, clock: ManualClock, *, journal: DurableJournal | None = None,
                 history: HistorySource | None = None, optimizer: Optimizer | None = None) -> None:
        self.clock = clock
        self.bus = EventBus()
        self.ledger = PositionLedger()
        self.orders = OrderManager(self.bus, clock, self.ledger, journal=journal)
        self.lifecycle = LifecycleStore(journal)
        self.history = history
        self.optimizer = optimizer if optimizer is not None else ProcessOptimizer()
        self._strategies: dict[str, _StrategyRunner] = {}
        self._optimization_tasks: set[asyncio.Task[JsonObject]] = set()
        self._active = False
        self._closing = False
        self._used = False

    async def __aenter__(self) -> 'TradingRuntime':
        if self._used:
            raise RuntimeError('runtime cannot be started twice')
        self._used = True
        self.lifecycle.restore()
        await self.orders.restore()
        self._active = True
        self.bus.subscribe('orders', self.orders.handle)
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, traceback: TracebackType | None) -> None:
        self._closing = True
        try:
            try:
                await self.bus.drain()
            finally:
                jobs = tuple(self._optimization_tasks)
                for job in jobs:
                    if not job.done() and not job.cancelling():
                        job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
                await self.bus.close()
        finally:
            self._active = False

    def add_gateway(self, gateway: Gateway) -> None:
        self._check_active()
        if gateway.gateway_id in self.orders.gateway_ids:
            raise ValueError('gateway already exists')
        gateway.attach(self.bus)
        self.orders.gateway_ids.add(gateway.gateway_id)

    async def start_strategy(self, strategy: AutoStrategy) -> StrategyContext:
        self._check_active()
        if strategy.name in self._strategies:
            raise ValueError('strategy name already exists')
        saved = self.lifecycle.get(strategy.name)
        if saved is not None:
            identity = f'{type(strategy).__module__}.{type(strategy).__qualname__}'
            if (identity != saved.strategy_type or strategy.state_version != saved.state_version
                    or strategy.instruments != saved.instruments):
                raise ValueError('saved strategy identity, instruments or state version differ')
            strategy.apply_parameters(saved.parameters)
            strategy.restore_state(json_object(saved.state))
        runner = _StrategyRunner(strategy, self)
        context = StrategyContext(
            strategy.name, self.bus, self.clock, self.ledger, self.orders,
            lambda: self._active and runner.status is StrategyStatus.RUNNING and self.orders.ready,
            can_cancel=lambda: self._active, get_status=lambda: runner.status,
            begin_optimization=lambda request, worker: self._begin_optimization(runner, request, worker),
        )
        runner.context = context
        initial = StrategyStatus.RUNNING if saved is None else (
            StrategyStatus.FAILED if saved.status is StrategyStatus.FAILED else StrategyStatus.PAUSED)
        runner.transition(initial, saved.reason if saved is not None else None)
        self._strategies[strategy.name] = runner
        self.bus.subscribe(strategy_recipient(strategy.name), runner.handle)
        await runner.initialize(saved)
        return context

    def _in_callback(self) -> bool:
        current = asyncio.current_task()
        return self.bus.in_consumer or any(runner.callback_task is current for runner in self._strategies.values())

    def _check_operator_call(self) -> None:
        self._check_active()
        if self._in_callback():
            raise RuntimeError('operator lifecycle commands must run outside callbacks')

    async def pause_strategy(self, name: str) -> None:
        self._check_operator_call()
        runner = self._strategies[name]
        async with runner.lock:
            if runner.status is StrategyStatus.OPTIMIZING:
                runner.transition(runner.status, runner.reason, StrategyStatus.PAUSED)
            elif runner.status is StrategyStatus.RUNNING:
                runner.transition(StrategyStatus.PAUSED)

    async def resume_strategy(self, name: str) -> None:
        self._check_operator_call()
        runner = self._strategies[name]
        async with runner.lock:
            if runner.status is StrategyStatus.OPTIMIZING:
                raise RuntimeError('optimization is still running')
            if not self.orders.ready:
                raise RuntimeError('storage and gateway reconciliation are not ready')
            runner.transition(StrategyStatus.RUNNING)

    async def checkpoint_strategy(self, name: str) -> None:
        self._check_operator_call()
        runner = self._strategies[name]
        async with runner.lock:
            runner.transition(runner.status, runner.reason, runner.resume_after)

    async def _begin_optimization(self, runner: _StrategyRunner, request: HistoryRequest,
                                  worker: OptimizationFunction) -> OptimizationHandle:
        self._check_active()
        if self._closing or self.history is None:
            raise RuntimeError('history source is unavailable or runtime is closing')
        validate_worker(worker)
        if request.end > self.clock.now():
            raise ValueError('optimization history cannot include future data')

        def begin() -> OptimizationHandle:
            if runner.status not in (StrategyStatus.RUNNING, StrategyStatus.PAUSED):
                raise RuntimeError('strategy cannot start another optimization')
            try:
                data = OptimizationInput((), runner.strategy.parameters, json_object(runner.strategy.save_state()))
                runner.checkpoint(StrategyStatus.OPTIMIZING, resume_after=runner.status)
            except Exception as error:
                runner.fail(error)
                raise
            runner.job = asyncio.create_task(self._optimize(runner, request, worker, data),
                                             name=f'optimize:{runner.strategy.name}')
            self._optimization_tasks.add(runner.job)
            runner.job.add_done_callback(self._job_finished)
            return OptimizationHandle(runner.job, lambda: not self._in_callback())

        if runner.callback_task is asyncio.current_task():
            return begin()
        if self._in_callback():
            raise RuntimeError('callback cannot start optimization for another strategy')
        async with runner.lock:
            return begin()

    def _job_finished(self, task: asyncio.Task[JsonObject]) -> None:
        self._optimization_tasks.discard(task)
        if not task.cancelled():
            task.exception()  # Failure is visible through the handle and strategy status.

    async def _optimize(self, runner: _StrategyRunner, request: HistoryRequest,
                        worker: OptimizationFunction, data: OptimizationInput) -> JsonObject:
        try:
            if self.history is None:
                raise RuntimeError('history source is unavailable')
            history = validate_history(request, await self.history.load(request))
            result = json_object(await self.optimizer.run(worker, replace(data, history=history)))
            async with runner.lock:
                if runner.status is not StrategyStatus.OPTIMIZING or runner.job is not asyncio.current_task():
                    raise OptimizationError('optimization result is no longer applicable')
                previous = runner.strategy.parameters
                try:
                    runner.strategy.apply_parameters(result)
                    target = runner.resume_after
                    if target is None:
                        raise RuntimeError('optimization has no resume status')
                    runner.checkpoint(target)
                except Exception:
                    AutoStrategy.apply_parameters(runner.strategy, previous)
                    raise
            return result
        except asyncio.CancelledError:
            async with runner.lock:
                if runner.status is StrategyStatus.OPTIMIZING and runner.job is asyncio.current_task():
                    runner.transition(StrategyStatus.PAUSED, 'optimization interrupted')
            raise
        except Exception as error:
            async with runner.lock:
                if runner.job is asyncio.current_task():
                    runner.fail(error)
            raise OptimizationError(str(error)) from error

    async def feed_candle(self, candle: Candle) -> None:
        self._check_active()
        await self.bus.drain()
        self.clock.advance_to(candle.closed_at)
        for name, runner in self._strategies.items():
            if (self.orders.ready and runner.status is StrategyStatus.RUNNING
                    and candle.instrument_id in runner.strategy.instruments):
                await self.bus.publish(strategy_recipient(name), CandleReceived(candle))

    async def reconcile_gateway(self, gateway_id: str, observed: tuple[OrderSnapshot, ...]) -> None:
        self._check_active()
        await self.bus.drain()
        await self.orders.reconcile_gateway(gateway_id, observed)
        await self.bus.drain()

    async def drain(self) -> None:
        await self.bus.drain()

    def _check_active(self) -> None:
        if not self._active:
            raise RuntimeError('runtime is not active')

from dataclasses import dataclass
from types import TracebackType

from .domain import Candle, CandleReceived, ExecutionApplied, Message, OrderUpdated
from .events import EventBus
from .ledger import PositionLedger
from .orders import OrderManager, strategy_recipient
from .simulation import Gateway
from .strategy import AutoStrategy, StrategyContext
from .time import ManualClock


@dataclass
class _StrategyRunner:
    strategy: AutoStrategy
    context: StrategyContext | None = None
    running: bool = True

    async def handle(self, message: Message) -> None:
        if self.context is None:
            raise RuntimeError('strategy has no context')
        try:
            if isinstance(message, CandleReceived):
                if self.running:
                    await self.strategy.on_candle(self.context, message.candle)
            elif isinstance(message, ExecutionApplied):
                await self.strategy.on_fill(self.context, message.execution)
            elif isinstance(message, OrderUpdated):
                await self.strategy.on_order(self.context, message.order)
            else:
                raise ValueError('unexpected strategy message')
        except Exception:
            self.running = False
            raise


class TradingRuntime:
    """Iteration-one composition root using controlled simulation time."""

    def __init__(self, clock: ManualClock) -> None:
        self.clock = clock
        self.bus = EventBus()
        self.ledger = PositionLedger()
        self.orders = OrderManager(self.bus, clock, self.ledger)
        self._strategies: dict[str, _StrategyRunner] = {}
        self._active = False
        self._used = False

    async def __aenter__(self) -> 'TradingRuntime':
        if self._used:
            raise RuntimeError('runtime cannot be started twice')
        self._used = True
        self._active = True
        self.bus.subscribe('orders', self.orders.handle)
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, traceback: TracebackType | None) -> None:
        try:
            await self.bus.close()
        finally:
            self._active = False
            for runner in self._strategies.values():
                runner.running = False

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
        runner = _StrategyRunner(strategy)
        context = StrategyContext(strategy.name, self.bus, self.clock, self.ledger, self.orders,
                                  lambda: self._active and runner.running)
        runner.context = context
        self._strategies[strategy.name] = runner
        self.bus.subscribe(strategy_recipient(strategy.name), runner.handle)
        try:
            await strategy.on_start(context)
        except Exception:
            runner.running = False
            raise
        return context

    async def feed_candle(self, candle: Candle) -> None:
        self._check_active()
        # Finish work at the current simulation time before advancing its clock.
        await self.bus.drain()
        self.clock.advance_to(candle.closed_at)
        for name, runner in self._strategies.items():
            if candle.instrument_id in runner.strategy.instruments:
                await self.bus.publish(strategy_recipient(name), CandleReceived(candle))

    async def drain(self) -> None:
        await self.bus.drain()

    def _check_active(self) -> None:
        if not self._active:
            raise RuntimeError('runtime is not active')

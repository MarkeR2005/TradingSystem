import asyncio
from dataclasses import replace
from datetime import timedelta
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.domain import OrderIntent, Side
from trading_system.events import DeliveryError
from trading_system.lifecycle import StrategyStatus
from trading_system.optimization import HistoryRequest, InMemoryHistory
from trading_system.persistence import DurableJournal, JournalError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

from tests_v2.helpers import INSTRUMENT, ROUTE, START, candle
from tests_v2.optimization_workers import failing_worker, select_period


REQUEST = HistoryRequest((INSTRUMENT.id,), START - timedelta(hours=1), START, timedelta(minutes=1))
HISTORY = (candle(START - timedelta(minutes=1)),)


class CountingStrategy(AutoStrategy):
    def __init__(self, name='counting'):
        super().__init__(name, (INSTRUMENT.id,), parameters={'period': 1})
        self.bars = 0
        self.starts = 0
        self.restores = 0
        self.fills = 0

    async def on_start(self, context):
        self.starts += 1

    async def on_restore(self, context):
        self.restores += 1

    async def on_candle(self, context, candle):
        self.bars += 1

    async def on_fill(self, context, execution):
        self.fills += 1

    def save_state(self):
        return {'bars': self.bars, 'starts': self.starts, 'fills': self.fills}

    def restore_state(self, state):
        self.bars = state['bars']
        self.starts = state['starts']
        self.fills = state['fills']


class DeferredOptimizer:
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.inputs = []

    async def run(self, worker, data):
        self.inputs.append(data)
        self.started.set()
        await self.release.wait()
        return worker(data)


class LifecycleTests(IsolatedAsyncioTestCase):
    async def test_pause_skips_market_and_blocks_place_replace_but_keeps_fill_and_cancel(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 3))
            await runtime.drain()
            order = context.orders()[0]
            await runtime.pause_strategy(strategy.name)
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            await runtime.feed_candle(candle())
            await runtime.drain()
            self.assertEqual(strategy.bars, 0)
            with self.assertRaises(RuntimeError):
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            with self.assertRaises(RuntimeError):
                await context.replace_order(order.order_id, OrderIntent(ROUTE, Side.BUY, 4))
            await gateway.fill(order.order_id, 100, quantity=1)
            await runtime.drain()
            self.assertEqual(context.position(ROUTE).quantity, 1)
            self.assertEqual(strategy.fills, 1)
            await context.cancel_order(order.order_id)
            await runtime.drain()
            await runtime.resume_strategy(strategy.name)
            await runtime.feed_candle(candle(runtime.clock.now()))
            await runtime.drain()
            self.assertEqual(strategy.bars, 1)

    async def test_optimization_uses_one_bulk_history_request_without_blocking_other_strategies(self):
        optimizer = DeferredOptimizer()
        history = InMemoryHistory(HISTORY)
        async with TradingRuntime(ManualClock(START), history=history, optimizer=optimizer) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            other = CountingStrategy('other')
            await runtime.start_strategy(other)
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
            await runtime.drain()
            job = await context.start_optimization(REQUEST, select_period)
            await optimizer.started.wait()
            self.assertEqual(context.status, StrategyStatus.OPTIMIZING)
            await runtime.feed_candle(candle())
            await runtime.drain()
            self.assertEqual(strategy.bars, 0)
            self.assertEqual(other.bars, 1)
            with self.assertRaises(RuntimeError):
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            await gateway.fill(context.orders()[0].order_id, 100, quantity=1)
            await runtime.drain()
            self.assertEqual(strategy.fills, 1)
            self.assertEqual(context.position(ROUTE).quantity, 1)
            with self.assertRaises(RuntimeError):
                await context.start_optimization(REQUEST, select_period)
            with self.assertRaises(RuntimeError):
                await runtime.resume_strategy(strategy.name)
            optimizer.release.set()
            result = await job.wait()
            self.assertEqual(result['period'], 3)
            self.assertEqual(context.status, StrategyStatus.RUNNING)
            self.assertEqual(strategy.parameters['period'], 3)
            self.assertEqual(strategy.fills, 1)  # Worker input must not overwrite newer fill callbacks.
            self.assertEqual(history.requests, [REQUEST])
            self.assertEqual(optimizer.inputs[0].state['fills'], 0)
            await runtime.feed_candle(candle(runtime.clock.now()))
            await runtime.drain()
            self.assertEqual(strategy.bars, 1)

    async def test_pause_during_optimization_is_preserved_after_completion(self):
        optimizer = DeferredOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, select_period)
            await optimizer.started.wait()
            await runtime.pause_strategy(strategy.name)
            optimizer.release.set()
            await job.wait()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertEqual(strategy.parameters['period'], 3)

    async def test_worker_error_leaves_failed_status_and_original_parameters(self):
        optimizer = DeferredOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, failing_worker)
            await optimizer.started.wait()
            optimizer.release.set()
            with self.assertRaises(RuntimeError):
                await job.wait()
            self.assertEqual(context.status, StrategyStatus.FAILED)
            self.assertEqual(strategy.parameters, {'period': 1})
            await runtime.feed_candle(candle())
            await runtime.drain()
            self.assertEqual(strategy.bars, 0)

    async def test_actual_optimizer_runs_in_another_process(self):
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY)) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, select_period)
            result = await asyncio.wait_for(job.wait(), 15)
            self.assertNotEqual(result['worker_pid'], os.getpid())
            self.assertEqual(context.status, StrategyStatus.RUNNING)

    async def test_saved_configuration_and_state_restore_without_repeating_on_start(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    await runtime.start_strategy(strategy)
                    await runtime.feed_candle(candle())
                    await runtime.drain()
                    await runtime.pause_strategy(strategy.name)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    context = await runtime.start_strategy(strategy)
                    self.assertEqual(strategy.bars, 1)
                    self.assertEqual(strategy.starts, 1)
                    self.assertEqual(strategy.restores, 1)
                    self.assertEqual(context.status, StrategyStatus.PAUSED)
                    self.assertEqual(strategy.parameters, {'period': 1})
                    await runtime.resume_strategy(strategy.name)
                    await runtime.feed_candle(candle(runtime.clock.now()))
                    await runtime.drain()
                    self.assertEqual(strategy.bars, 2)

    async def test_shutdown_interrupts_job_and_restart_keeps_pause(self):
        optimizer = DeferredOptimizer()
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal, history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
                    context = await runtime.start_strategy(CountingStrategy())
                    job = await context.start_optimization(REQUEST, select_period)
                    await optimizer.started.wait()
            self.assertTrue(job.done)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    context = await runtime.start_strategy(CountingStrategy())
                    self.assertEqual(context.status, StrategyStatus.PAUSED)

    async def test_state_version_mismatch_refuses_to_load_without_changing_saved_state(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    await runtime.start_strategy(CountingStrategy())
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    strategy.state_version = 2
                    before = journal.records
                    with self.assertRaises(ValueError):
                        await runtime.start_strategy(strategy)
                    self.assertEqual(journal.records, before)

    async def test_failed_checkpoint_blocks_strategy_without_erasing_financial_history(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    strategy = CountingStrategy()
                    context = await runtime.start_strategy(strategy)
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
                    await runtime.drain()
                    await gateway.fill(context.orders()[0].order_id, 100)
                    await runtime.drain()
                    with patch.object(journal, 'append', side_effect=JournalError('disk unavailable')):
                        await runtime.feed_candle(candle())
                        with self.assertRaises(DeliveryError):
                            await runtime.drain()
                    self.assertEqual(context.status, StrategyStatus.FAILED)
                    self.assertEqual(context.position(ROUTE).quantity, 1)
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

    async def test_future_history_is_rejected_before_strategy_leaves_running(self):
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY)) as runtime:
            context = await runtime.start_strategy(CountingStrategy())
            with self.assertRaises(ValueError):
                await context.start_optimization(replace(REQUEST, end=START + timedelta(minutes=1)), select_period)
            self.assertEqual(context.status, StrategyStatus.RUNNING)

    async def test_shutdown_terminates_an_actual_busy_worker(self):
        import multiprocessing
        from tests_v2.optimization_workers import busy_worker
        with TemporaryDirectory() as directory:
            marker = Path(directory) / 'worker.pid'
            runtime = TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY))
            await runtime.__aenter__()
            strategy = CountingStrategy()
            strategy.apply_parameters({'marker': str(marker)})
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, busy_worker)
            try:
                async def wait_for_worker():
                    while True:
                        try:
                            return int(marker.read_text())
                        except (OSError, ValueError):
                            await asyncio.sleep(0.01)
                worker_pid = await asyncio.wait_for(wait_for_worker(), 15)
            finally:
                await asyncio.wait_for(runtime.__aexit__(None, None, None), 10)
            self.assertTrue(job.done)
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertNotIn(worker_pid, [process.pid for process in multiprocessing.active_children()])

    async def test_crashed_optimizing_checkpoint_restores_paused_without_restarting_job(self):
        import subprocess
        import sys
        code = '''
import asyncio, importlib, os, sys
from pathlib import Path
sys.path.insert(0, 'tests_v2')
module = importlib.import_module(sys.argv[2])
async def run():
    journal = module.DurableJournal(Path(sys.argv[1]))
    optimizer = module.DeferredOptimizer()
    runtime = module.TradingRuntime(module.ManualClock(module.START), journal=journal,
                                   history=module.InMemoryHistory(module.HISTORY), optimizer=optimizer)
    await runtime.__aenter__()
    context = await runtime.start_strategy(module.CountingStrategy())
    await context.start_optimization(module.REQUEST, module.select_period)
    await optimizer.started.wait()
    os._exit(0)
asyncio.run(run())
'''
        with TemporaryDirectory() as directory:
            await asyncio.to_thread(subprocess.run, [sys.executable, '-c', code, directory, CountingStrategy.__module__],
                                    check=True, timeout=15)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    context = await runtime.start_strategy(CountingStrategy())
                    self.assertEqual(context.status, StrategyStatus.PAUSED)
                    self.assertEqual(runtime.lifecycle.get('counting').reason, 'optimization interrupted by restart')
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

    async def test_strategy_callback_cannot_wait_for_its_job_and_deadlock_fill_delivery(self):
        optimizer = DeferredOptimizer()
        class WaitingStrategy(CountingStrategy):
            async def on_candle(self, context, bar):
                job = await context.start_optimization(REQUEST, select_period)
                await job.wait()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            context = await runtime.start_strategy(WaitingStrategy())
            await runtime.feed_candle(candle())
            with self.assertRaises(DeliveryError):
                await asyncio.wait_for(runtime.drain(), 2)
            self.assertEqual(context.status, StrategyStatus.FAILED)

    async def test_invalid_computation_state_keeps_last_valid_checkpoint_and_fails_strategy(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    context = await runtime.start_strategy(strategy)
                    with patch.object(strategy, 'save_state', return_value={'invalid': float('nan')}):
                        await runtime.feed_candle(candle())
                        with self.assertRaises(DeliveryError):
                            await runtime.drain()
                    self.assertEqual(context.status, StrategyStatus.FAILED)
                    saved = runtime.lifecycle.get(strategy.name)
                    self.assertEqual(saved.status, StrategyStatus.FAILED)
                    self.assertEqual(saved.state['bars'], 0)

    async def test_late_cancellation_of_failed_job_does_not_pause_its_replacement(self):
        class SlowCancellationOptimizer(DeferredOptimizer):
            def __init__(self):
                super().__init__()
                self.cleaning = asyncio.Event()
                self.finish_cleanup = asyncio.Event()
            async def run(self, worker, data):
                first = not self.inputs
                try:
                    return await super().run(worker, data)
                finally:
                    if first:
                        self.cleaning.set()
                        await self.finish_cleanup.wait()
        class FailingFillStrategy(CountingStrategy):
            async def on_fill(self, context, execution):
                raise ValueError('failed fill callback')
        optimizer = SlowCancellationOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = FailingFillStrategy()
            context = await runtime.start_strategy(strategy)
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
            await runtime.drain()
            old = await context.start_optimization(REQUEST, select_period)
            await optimizer.started.wait()
            await gateway.fill(context.orders()[0].order_id, 100, quantity=1)
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            await optimizer.cleaning.wait()
            await runtime.resume_strategy(strategy.name)
            current = await context.start_optimization(REQUEST, select_period)
            optimizer.finish_cleanup.set()
            with self.assertRaises(asyncio.CancelledError):
                await old.wait()
            self.assertEqual(context.status, StrategyStatus.OPTIMIZING)
            optimizer.release.set()
            await current.wait()

    async def test_invalid_state_during_operator_pause_fails_closed(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            with patch.object(strategy, 'save_state', return_value={'not_json': object()}):
                with self.assertRaises(ValueError):
                    await runtime.pause_strategy(strategy.name)
            self.assertEqual(context.status, StrategyStatus.FAILED)

    async def test_optimized_parameters_restore_while_financial_history_remains_separate(self):
        optimizer = DeferredOptimizer()
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal, history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(CountingStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    await gateway.fill(context.orders()[0].order_id, 100, commission=1)
                    await runtime.drain()
                    job = await context.start_optimization(REQUEST, select_period)
                    await optimizer.started.wait()
                    optimizer.release.set()
                    await job.wait()
                    await context.place_order(OrderIntent(ROUTE, Side.SELL, 2))
                    await runtime.drain()
                    await gateway.fill(context.orders()[-1].order_id, 110, commission=1)
                    await runtime.drain()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    context = await runtime.start_strategy(strategy)
                    self.assertEqual(strategy.parameters['period'], 3)
                    self.assertEqual(strategy.fills, 2)
                    self.assertEqual(context.equity()[-1].value, 18)
                    self.assertEqual(len(context.trades()), 1)
                    self.assertEqual(strategy.starts, 1)
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

    async def test_out_of_window_history_never_reaches_optimizer(self):
        optimizer = DeferredOptimizer()
        class InvalidHistory:
            async def load(self, request):
                return (candle(),)  # Closes beyond REQUEST.end.
        async with TradingRuntime(ManualClock(START), history=InvalidHistory(), optimizer=optimizer) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, select_period)
            with self.assertRaises(RuntimeError):
                await job.wait()
            self.assertEqual(context.status, StrategyStatus.FAILED)
            self.assertEqual(optimizer.inputs, [])
            self.assertEqual(strategy.parameters, {'period': 1})

    async def test_worker_input_cannot_mutate_live_parameters_or_computation_state(self):
        class MutatingOptimizer(DeferredOptimizer):
            async def run(self, worker, data):
                data.parameters['period'] = 999
                data.state['starts'] = 999
                raise ValueError('worker failed after mutating its copy')
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=MutatingOptimizer()) as runtime:
            strategy = CountingStrategy()
            context = await runtime.start_strategy(strategy)
            job = await context.start_optimization(REQUEST, select_period)
            with self.assertRaises(RuntimeError):
                await job.wait()
            self.assertEqual(strategy.parameters, {'period': 1})
            self.assertEqual(strategy.starts, 1)

    async def test_failed_strategy_stays_failed_after_restart_until_explicit_resume(self):
        optimizer = DeferredOptimizer()
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal, history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
                    context = await runtime.start_strategy(CountingStrategy())
                    job = await context.start_optimization(REQUEST, failing_worker)
                    await optimizer.started.wait()
                    optimizer.release.set()
                    with self.assertRaises(RuntimeError):
                        await job.wait()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    strategy = CountingStrategy()
                    context = await runtime.start_strategy(strategy)
                    self.assertEqual(context.status, StrategyStatus.FAILED)
                    await runtime.feed_candle(candle())
                    await runtime.drain()
                    self.assertEqual(strategy.bars, 0)
                    await runtime.resume_strategy(strategy.name)
                    self.assertEqual(context.status, StrategyStatus.RUNNING)

    async def test_runtime_waits_for_cleanup_of_replaced_cancelled_job(self):
        class CleanupOptimizer(DeferredOptimizer):
            def __init__(self):
                super().__init__()
                self.cleaning = asyncio.Event()
                self.cleaned = asyncio.Event()
                self.finish_cleanup = asyncio.Event()
            async def run(self, worker, data):
                first = not self.inputs
                try:
                    return await super().run(worker, data)
                finally:
                    if first:
                        self.cleaning.set()
                        await self.finish_cleanup.wait()
                        self.cleaned.set()
        class FailingFillStrategy(CountingStrategy):
            async def on_fill(self, context, execution):
                raise ValueError('failed fill callback')
        optimizer = CleanupOptimizer()
        runtime = TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer)
        await runtime.__aenter__()
        gateway = SimulationGateway('sim', runtime.clock)
        runtime.add_gateway(gateway)
        strategy = FailingFillStrategy()
        context = await runtime.start_strategy(strategy)
        await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
        await runtime.drain()
        await context.start_optimization(REQUEST, select_period)
        await optimizer.started.wait()
        await gateway.fill(context.orders()[0].order_id, 100, quantity=1)
        with self.assertRaises(DeliveryError):
            await runtime.drain()
        await optimizer.cleaning.wait()
        await runtime.resume_strategy(strategy.name)
        await context.start_optimization(REQUEST, select_period)
        closing = asyncio.create_task(runtime.__aexit__(None, None, None))
        try:
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(closing), 0.05)
        finally:
            optimizer.finish_cleanup.set()
            await closing
        self.assertTrue(optimizer.cleaned.is_set())

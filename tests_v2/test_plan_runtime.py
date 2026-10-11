import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.domain import OrderIntent, Side
from trading_system.events import DeliveryError
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal
from trading_system.plans import AcknowledgePlan, FailurePolicy, PlanPhase
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy
from trading_system.time import ManualClock
from tests_v2.helpers import INSTRUMENT, ROUTE, START, candle
from tests_v2.test_plans import SECOND, capital, plan


class BasketStrategy(AutoStrategy):
    def __init__(self, name='a', broken=False):
        super().__init__(name, (INSTRUMENT.id, SECOND.instrument.id))
        self.updates = []
        self.bars = 0
        self.fills = 0
        self.restored_status = None
        self.broken = broken

    async def on_candle(self, context, candle):
        self.bars += 1

    async def on_fill(self, context, execution):
        self.fills += 1

    async def on_plan(self, context, update):
        self.updates.append(update)
        if self.broken and update.paused:
            raise RuntimeError('broken notification callback')

    async def on_restore(self, context):
        self.restored_status = context.status


class PlanRuntimeTests(IsolatedAsyncioTestCase):
    async def setup_runtime(self, runtime, broken=False):
        gateway = SimulationGateway('sim', runtime.clock)
        runtime.add_gateway(gateway)
        strategy = BasketStrategy(broken=broken)
        context = await runtime.start_strategy(strategy)
        await runtime.configure_capital(capital())
        return gateway, strategy, context

    async def test_failure_pauses_owner_and_keeps_other_strategy_and_fills_alive(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, strategy, context = await self.setup_runtime(runtime)
            other = BasketStrategy('b')
            await runtime.start_strategy(other)
            await context.place_plan(plan(FailurePolicy.CLOSE))
            await runtime.drain()
            first, second = context.orders()
            await gateway.fill(first.order_id, 100, quantity=1)
            await gateway.reject_order(second.order_id, 'failed')
            await runtime.drain()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertTrue(strategy.updates[-1].paused)
            with self.assertRaises(RuntimeError):
                await runtime.resume_strategy('a')
            with self.assertRaises(RuntimeError):
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))
            await runtime.feed_candle(candle())
            await runtime.drain()
            self.assertEqual((strategy.bars, other.bars), (0, 1))
            await gateway.fill(context.orders()[-1].order_id, 90)
            await runtime.drain()
            self.assertEqual(strategy.fills, 2)
            await runtime.acknowledge_plan(AcknowledgePlan('ack', runtime.orders.revision, 'basket'))
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            await runtime.resume_strategy('a')
            self.assertEqual(context.status, StrategyStatus.RUNNING)

    async def test_plan_api_checks_owner_and_declared_instruments(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            _, _, context = await self.setup_runtime(runtime)
            with self.assertRaises(ValueError):
                await context.place_plan(replace(plan(), strategy_id='b'))
            unknown = replace(SECOND, instrument=replace(SECOND.instrument, id=replace(SECOND.instrument.id, symbol='UNKNOWN')))
            legs = plan().legs
            with self.assertRaises(ValueError):
                await context.place_plan(replace(plan(), legs=(legs[0], replace(legs[1], intent=replace(legs[1].intent, route=unknown)))))
            self.assertEqual(context.orders(), ())

    async def test_broken_alert_callback_cannot_stop_emergency_close(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, _, context = await self.setup_runtime(runtime, broken=True)
            await context.place_plan(plan(FailurePolicy.CLOSE))
            await runtime.drain()
            first, second = context.orders()
            await gateway.fill(first.order_id, 100, quantity=1)
            await gateway.reject_order(second.order_id, 'failed')
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            self.assertEqual(context.status, StrategyStatus.FAILED)
            self.assertEqual(runtime.orders.plan('basket').phase, PlanPhase.CLOSING)
            await gateway.fill(context.orders()[-1].order_id, 90)
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            self.assertEqual(runtime.orders.ledger.position('a', ROUTE).quantity, 0)

    async def test_fault_restores_paused_before_strategy_restore_hook(self):
        with TemporaryDirectory() as directory:
            clock = ManualClock(START)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(clock, journal=journal) as runtime:
                    gateway, _, context = await self.setup_runtime(runtime)
                    await context.place_plan(plan())
                    await runtime.drain()
                    await gateway.fill(context.orders()[0].order_id, 100, quantity=1)
                    await gateway.reject_order(context.orders()[1].order_id, 'failed')
                    await runtime.drain()
                    observed = gateway.order_snapshots()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(clock, journal=journal) as runtime:
                    gateway = SimulationGateway('sim', clock)
                    gateway.restore_orders(observed)
                    runtime.add_gateway(gateway)
                    strategy = BasketStrategy()
                    context = await runtime.start_strategy(strategy)
                    self.assertEqual(strategy.restored_status, StrategyStatus.PAUSED)
                    self.assertEqual(strategy.updates, [])
                    await runtime.reconcile_gateway('sim', observed)
                    await runtime.configure_capital(capital(revision=2))
                    with self.assertRaises(RuntimeError):
                        await runtime.resume_strategy('a')

    async def test_plan_fault_cancels_optimization_without_losing_executions(self):
        from trading_system.optimization import InMemoryHistory
        from tests_v2.test_lifecycle import DeferredOptimizer, HISTORY, REQUEST
        from tests_v2.optimization_workers import select_period
        optimizer = DeferredOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            gateway, _, context = await self.setup_runtime(runtime)
            await context.place_plan(plan(FailurePolicy.CLOSE))
            await runtime.drain()
            job = await context.start_optimization(REQUEST, select_period)
            await asyncio.wait_for(optimizer.started.wait(), 1)
            first, second = context.orders()
            await gateway.fill(first.order_id, 100, quantity=1)
            await gateway.reject_order(second.order_id, 'failed')
            await runtime.drain()
            with self.assertRaises(asyncio.CancelledError):
                await job.wait()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertEqual(context.position(ROUTE).quantity, 1)
            self.assertEqual(context.plan('basket').phase, PlanPhase.CLOSING)
            with self.assertRaises(RuntimeError):
                await context.start_optimization(REQUEST, select_period)

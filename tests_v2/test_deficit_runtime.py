import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.deficit import AcknowledgeDeficit, DeficitPhase, StartDeficit
from trading_system.domain import OrderIntent, Side
from trading_system.events import DeliveryError
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START, candle
from tests_v2.test_admission import KEY, policy
from tests_v2.test_plan_runtime import BasketStrategy


class RiskStrategy(BasketStrategy):
    async def on_deficit(self, context, update):
        self.updates.append(update)
        if self.broken and update.paused:
            raise RuntimeError('broken risk notification')


def quotes(clock):
    from datetime import timedelta
    from trading_system.deficit import DeficitQuotes
    from trading_system.plans import PlanPrice
    return DeficitQuotes(clock.now(), clock.now() + timedelta(minutes=5), (PlanPrice(ROUTE, 100),))


class DeficitRuntimeTests(IsolatedAsyncioTestCase):
    async def setup_runtime(self, runtime, broken=False):
        gateway = SimulationGateway('sim', runtime.clock)
        runtime.add_gateway(gateway)
        strategy = RiskStrategy(broken=broken)
        context = await runtime.start_strategy(strategy)
        await runtime.configure_capital(policy(10000))
        await context.place_order(OrderIntent(ROUTE, Side.BUY, 10, 100))
        await runtime.drain()
        await gateway.fill(context.orders()[0].order_id, 100)
        await runtime.drain()
        await runtime.configure_capital(policy(800, revision=2))
        return gateway, strategy, context

    async def test_risk_pause_keeps_fills_alive_and_requires_explicit_resume(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, strategy, context = await self.setup_runtime(runtime)
            other = RiskStrategy('b')
            await runtime.start_strategy(other)
            await runtime.start_deficit(StartDeficit('risk', runtime.orders.revision, KEY, 0, 10, quotes(runtime.clock)))
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertTrue(strategy.updates[-1].paused)
            with self.assertRaises(RuntimeError):
                await runtime.resume_strategy('a')
            await runtime.feed_candle(candle())
            await runtime.drain()
            self.assertEqual((strategy.bars, other.bars), (0, 1))
            await gateway.fill(context.orders()[-1].order_id, 100)
            await runtime.drain()
            self.assertEqual(strategy.fills, 2)
            self.assertEqual(runtime.orders.deficit('risk').phase, DeficitPhase.RESOLVED)
            await runtime.acknowledge_deficit(AcknowledgeDeficit('ack', runtime.orders.revision, 'risk'))
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            await runtime.resume_strategy('a')
            self.assertEqual(context.status, StrategyStatus.RUNNING)

    async def test_broken_notification_cannot_prevent_or_rollback_risk_execution(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, _, context = await self.setup_runtime(runtime, broken=True)
            with self.assertRaises(DeliveryError):
                await runtime.start_deficit(StartDeficit('risk', runtime.orders.revision, KEY, 0, 10, quotes(runtime.clock)))
            self.assertEqual(context.status, StrategyStatus.FAILED)
            self.assertEqual(context.orders()[-1].intent.quantity, 2)
            await gateway.fill(context.orders()[-1].order_id, 100)
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            self.assertEqual(context.position(ROUTE).quantity, 8)
            self.assertEqual(runtime.orders.deficit('risk').phase, DeficitPhase.RESOLVED)

    async def test_restart_pauses_before_restore_hook_and_does_not_resubmit(self):
        with TemporaryDirectory() as directory:
            clock = ManualClock(START)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(clock, journal=journal) as runtime:
                    gateway, _, context = await self.setup_runtime(runtime)
                    await runtime.start_deficit(StartDeficit('risk', runtime.orders.revision, KEY, 0, 10, quotes(clock)))
                    observed = gateway.order_snapshots()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(clock, journal=journal) as runtime:
                    gateway = SimulationGateway('sim', clock)
                    gateway.restore_orders(observed)
                    runtime.add_gateway(gateway)
                    strategy = RiskStrategy()
                    context = await runtime.start_strategy(strategy)
                    self.assertEqual(strategy.restored_status, StrategyStatus.PAUSED)
                    self.assertEqual(strategy.updates, [])
                    await runtime.reconcile_gateway('sim', observed)
                    await runtime.configure_capital(policy(800, revision=3))
                    self.assertEqual(gateway.order_snapshots(), observed)
                    await gateway.fill(context.orders()[-1].order_id, 100)
                    await runtime.drain()
                    self.assertEqual(runtime.orders.deficit('risk').phase, DeficitPhase.RESOLVED)

    async def test_deficit_cancels_optimization_but_preserves_position_accounting(self):
        from trading_system.optimization import InMemoryHistory
        from tests_v2.test_lifecycle import DeferredOptimizer, HISTORY, REQUEST
        from tests_v2.optimization_workers import select_period
        optimizer = DeferredOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            gateway, _, context = await self.setup_runtime(runtime)
            job = await context.start_optimization(REQUEST, select_period)
            await asyncio.wait_for(optimizer.started.wait(), 1)
            await runtime.start_deficit(StartDeficit('risk', runtime.orders.revision, KEY, 0, 10, quotes(runtime.clock)))
            with self.assertRaises(asyncio.CancelledError):
                await job.wait()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            await gateway.fill(context.orders()[-1].order_id, 100)
            await runtime.drain()
            self.assertEqual(context.position(ROUTE).quantity, 8)

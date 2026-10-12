import asyncio
from dataclasses import replace
from datetime import timedelta
from unittest import IsolatedAsyncioTestCase

from trading_system.events import DeliveryError
from trading_system.lifecycle import StrategyStatus
from trading_system.plans import PlanMarks, PlanPhase, PlanPrice, PlanProtection, ProtectionKind, UpdatePlanProtection
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_plans import SECOND, capital, plan
from tests_v2.test_plan_runtime import BasketStrategy


class ProtectionRuntimeTests(IsolatedAsyncioTestCase):
    async def setup_runtime(self, runtime, broken=False):
        gateway = SimulationGateway('sim', runtime.clock)
        runtime.add_gateway(gateway)
        strategy = BasketStrategy(broken=broken)
        context = await runtime.start_strategy(strategy)
        await runtime.configure_capital(capital())
        await context.place_plan(replace(plan(), take_profit=20))
        await runtime.drain()
        for order in context.orders():
            await gateway.fill(order.order_id, 100)
        await runtime.drain()
        return gateway, strategy, context

    def marks(self, runtime, price, sequence=1):
        return PlanMarks('basket', sequence, runtime.clock.now(), runtime.clock.now() + timedelta(seconds=10),
                         (PlanPrice(ROUTE, price), PlanPrice(SECOND, 100)))

    async def test_strategy_api_updates_only_owned_plan_while_running(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            _, _, context = await self.setup_runtime(runtime)
            await runtime.mark_plan(self.marks(runtime, 100))
            command = UpdatePlanProtection('change', context.oms_revision, 'a', 'basket', PlanProtection(10, 30))
            with self.assertRaises(ValueError):
                await context.update_plan_protection(replace(command, strategy_id='b'))
            other = await runtime.start_strategy(BasketStrategy('b'))
            with self.assertRaises(ValueError):
                await other.update_plan_protection(replace(command, strategy_id='b'))
            await context.update_plan_protection(command)
            await runtime.drain()
            self.assertEqual(context.plan('basket').protection, command.protection)
            await runtime.pause_strategy('a')
            with self.assertRaises(RuntimeError):
                await context.update_plan_protection(replace(command, operation_id='paused'))

    async def test_operator_can_tighten_protection_during_pause_without_resuming(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, _, context = await self.setup_runtime(runtime)
            await runtime.mark_plan(self.marks(runtime, 95))
            await runtime.pause_strategy('a')
            await runtime.update_plan_protection(UpdatePlanProtection(
                'tighten', runtime.orders.revision, 'a', 'basket', PlanProtection(5)))
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertEqual(context.plan('basket').protection_trigger.kind, ProtectionKind.STOP_LOSS)
            for order in context.orders()[2:]:
                await gateway.fill(order.order_id, 100)
            await runtime.drain()
            self.assertEqual(context.plan('basket').phase, PlanPhase.CLOSED)
            self.assertEqual(context.status, StrategyStatus.PAUSED)

    async def test_take_profit_runs_during_optimization_and_cancels_worker(self):
        from trading_system.optimization import InMemoryHistory
        from tests_v2.test_lifecycle import DeferredOptimizer, HISTORY, REQUEST
        from tests_v2.optimization_workers import select_period
        optimizer = DeferredOptimizer()
        async with TradingRuntime(ManualClock(START), history=InMemoryHistory(HISTORY), optimizer=optimizer) as runtime:
            gateway, strategy, context = await self.setup_runtime(runtime)
            job = await context.start_optimization(REQUEST, select_period)
            await asyncio.wait_for(optimizer.started.wait(), 1)
            await runtime.mark_plan(self.marks(runtime, 110))
            with self.assertRaises(asyncio.CancelledError):
                await job.wait()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertEqual(context.plan('basket').protection_trigger.kind, ProtectionKind.TAKE_PROFIT)
            for order in context.orders()[2:]:
                await gateway.fill(order.order_id, 100)
            await runtime.drain()
            self.assertEqual(strategy.fills, 4)
            self.assertEqual(context.plan('basket').phase, PlanPhase.CLOSED)

    async def test_broken_plan_callback_cannot_prevent_take_profit_exit(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway, _, context = await self.setup_runtime(runtime, broken=True)
            with self.assertRaises(DeliveryError):
                await runtime.mark_plan(self.marks(runtime, 110))
            self.assertEqual(context.status, StrategyStatus.FAILED)
            self.assertEqual(len(context.orders()), 4)
            for order in context.orders()[2:]:
                await gateway.fill(order.order_id, 100)
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            self.assertEqual(context.plan('basket').phase, PlanPhase.CLOSED)
            self.assertEqual(runtime.ledger.positions(), ())

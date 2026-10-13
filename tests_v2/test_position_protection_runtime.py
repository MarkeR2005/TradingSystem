from dataclasses import replace
from datetime import timedelta
from unittest import IsolatedAsyncioTestCase

from trading_system.domain import OrderIntent, Side
from trading_system.inventory import MANUAL, TransferPosition
from trading_system.lifecycle import StrategyStatus
from trading_system.plans import (
    PlanMarks, PlanPhase, PlanPrice, PlanProtection, ProtectPositions, ReleasePositionProtection,
)
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock
from tests_v2.helpers import PassiveStrategy, ROUTE, START
from tests_v2.test_admission import policy
from tests_v2.test_inventory import report


def command(runtime, owner, price=100, identifier='guard'):
    marks = PlanMarks(identifier, 1, START, START + timedelta(minutes=1), (PlanPrice(ROUTE, price),))
    return ProtectPositions('attach-' + identifier, runtime.orders.revision, owner, marks,
                            PlanProtection(100, take_profit=20), (PlanPrice(ROUTE, 100),))


class PositionProtectionRuntimeTests(IsolatedAsyncioTestCase):
    async def test_manual_can_attach_release_transfer_part_and_protect_remainder(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            receiver = await runtime.start_strategy(PassiveStrategy('a'))
            await runtime.reconcile_inventory(report())
            await runtime.configure_capital(policy(2000, unit=150, grants=('a', MANUAL)))
            await runtime.protect_positions(command(runtime, MANUAL))
            await runtime.release_position_protection(ReleasePositionProtection(
                'release', runtime.orders.revision, MANUAL, 'guard'))
            await runtime.pause_strategy('a')
            await runtime.transfer_position(TransferPosition('move', runtime.orders.revision, MANUAL, 'a', ROUTE, 1))
            await runtime.configure_capital(policy(2000, revision=2, unit=150, grants=('a', MANUAL)))
            await runtime.protect_positions(command(runtime, MANUAL, 110, 'remaining'))
            close = runtime.orders.orders(MANUAL)[0]
            self.assertEqual(close.intent.quantity, 3)
            await gateway.fill(close.order_id, 110, commission=1)
            await runtime.drain()
            self.assertEqual(runtime.orders.plan('remaining').phase, PlanPhase.CLOSED)
            self.assertEqual(runtime.ledger.position('a', ROUTE).quantity, 1)
            self.assertEqual(receiver.status, StrategyStatus.PAUSED)
            self.assertEqual(runtime.ledger.settlements(MANUAL)[0].pnl, 29)

    async def test_strategy_attachment_enforces_owner_instruments_and_running_status(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy('a'))
            await runtime.configure_capital(policy(2000, unit=150))
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
            await runtime.drain()
            await gateway.fill(context.orders()[0].order_id, 100, commission=2)
            await runtime.drain()
            request = command(runtime, 'a')
            with self.assertRaises(ValueError):
                await context.protect_positions(replace(request, strategy_id='b'))
            await runtime.pause_strategy('a')
            with self.assertRaises(RuntimeError):
                await context.protect_positions(request)
            # The operator may protect a paused owner without resuming its decisions.
            await runtime.protect_positions(request)
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            self.assertEqual(context.plan('guard').pnl, -2)

    async def test_unknown_operator_owner_is_rejected_before_financial_commit(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            await runtime.reconcile_inventory(report())
            await runtime.configure_capital(policy(2000, grants=('a', MANUAL)))
            before = runtime.orders.revision
            with self.assertRaises(ValueError):
                await runtime.protect_positions(command(runtime, 'a'))
            self.assertEqual(runtime.orders.revision, before)
            self.assertTrue(runtime.orders.ready)

    async def test_running_strategy_can_attach_and_receive_protective_exit_fills(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = PassiveStrategy('a')
            context = await runtime.start_strategy(strategy)
            await runtime.configure_capital(policy(2000, unit=150))
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
            await runtime.drain()
            await gateway.fill(context.orders()[0].order_id, 100, commission=2)
            await runtime.drain()
            await context.protect_positions(command(runtime, 'a', 106))
            await runtime.drain()
            self.assertEqual(context.status, StrategyStatus.PAUSED)
            await gateway.fill(context.orders()[-1].order_id, 106, commission=1)
            await runtime.drain()
            self.assertEqual(len(strategy.fills), 2)
            self.assertEqual(context.trades()[0].pnl, 21)

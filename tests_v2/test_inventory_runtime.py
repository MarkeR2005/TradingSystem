import unittest

from trading_system.domain import PositionChanged
from trading_system.inventory import MANUAL, CloseManual, TransferPosition
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock
from tests_v2.helpers import PassiveStrategy, ROUTE, START
from tests_v2.test_admission import policy
from tests_v2.test_inventory import report


class ReceivingStrategy(PassiveStrategy):
    def __init__(self):
        super().__init__('a')
        self.changes = []

    async def on_position_change(self, context, change: PositionChanged):
        self.changes.append(change)


class InventoryRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_identifier_is_reserved(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            with self.assertRaises(ValueError):
                await runtime.start_strategy(PassiveStrategy(MANUAL))

    async def test_paused_receiver_is_notified_without_fake_fill_and_can_close_after_resume(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            strategy = ReceivingStrategy()
            await runtime.start_strategy(strategy)
            await runtime.reconcile_inventory(report())
            await runtime.configure_capital(policy(2000, grants=('a', MANUAL)))
            command = TransferPosition('transfer', runtime.orders.revision, MANUAL, 'a', ROUTE, 2)
            with self.assertRaises(RuntimeError):
                await runtime.transfer_position(command)
            await runtime.pause_strategy('a')
            await runtime.transfer_position(command)
            self.assertEqual(strategy.fills, [])
            self.assertEqual(len(strategy.changes), 1)
            self.assertEqual(strategy.changes[0].position.quantity, 2)
            with self.assertRaises(RuntimeError):
                await runtime.resume_strategy('a')
            await runtime.configure_capital(policy(2000, revision=2, grants=('a', MANUAL)))
            await runtime.resume_strategy('a')
            await runtime.close_manual(CloseManual('close', runtime.orders.revision, ROUTE, 1, 100))
            await runtime.bus.drain()
            self.assertEqual(len(runtime.orders.orders(MANUAL)), 1)

    async def test_unknown_destination_cannot_commit_then_fail_event_delivery(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            await runtime.reconcile_inventory(report())
            await runtime.configure_capital(policy(2000, grants=('a', MANUAL)))
            with self.assertRaises(ValueError):
                await runtime.transfer_position(TransferPosition('t', runtime.orders.revision, MANUAL, 'a', ROUTE, 1))
            self.assertEqual(runtime.ledger.position(MANUAL, ROUTE).quantity, 4)
            self.assertTrue(runtime.orders.ready)

    async def test_revision_change_during_drain_rejects_stale_operator_action(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            await runtime.reconcile_inventory(report())
            await runtime.configure_capital(policy(2000, grants=(MANUAL,)))
            command = CloseManual('close', runtime.orders.revision, ROUTE, 1, 100)
            await runtime.configure_capital(policy(2000, revision=2, grants=(MANUAL,)))
            with self.assertRaises(ValueError):
                await runtime.close_manual(command)
            self.assertEqual(runtime.orders.orders(MANUAL), ())

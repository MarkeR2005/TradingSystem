import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.domain import (
    CancelOrder, Execution, ExecutionApplied, GatewayCancelled, GatewaySubmit,
    OrderIntent, OrderSnapshot, OrderStatus, ReplaceOrder, Side, SubmitOrder,
)
from trading_system.inventory import (
    MANUAL, BrokerPosition, CloseManual, ExternalOrder, InventoryReport,
    OpeningBalance, TransferPosition,
)
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_admission import Recorder, policy


def report(revision=0, *, quantity=4, orders=(), operation='import', openings=None):
    return InventoryReport(operation, revision, 'sim', START,
                           (OpeningBalance(ROUTE, quantity, 100),) if openings is None else openings,
                           orders, (BrokerPosition(ROUTE, quantity),),
                           tuple(item.order for item in orders))


class InventoryTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bus = Recorder()
        self.clock = ManualClock(START)
        self.oms = OrderManager(self.bus, self.clock, PositionLedger())
        self.oms.gateway_ids.add('sim')

    async def refresh(self):
        revision = self.oms.capital_policy.revision + 1 if self.oms.capital_policy else 1
        await self.oms.configure_capital(policy(2000, revision=revision, grants=('a', 'b', MANUAL)))

    async def transfer(self, quantity=1, source=MANUAL, target='a', operation='transfer'):
        command = TransferPosition(operation, self.oms.revision, source, target, ROUTE, quantity)
        await self.oms.handle(command)
        return command

    async def test_import_has_cost_basis_but_no_fills_or_prehistory_profit(self):
        command = report()
        await self.oms.handle(command)
        position = self.oms.ledger.position(MANUAL, ROUTE)
        self.assertEqual((position.quantity, position.average_price, position.realized_pnl), (4, 100, 0))
        self.assertEqual(self.oms.ledger.trades(MANUAL), ())
        self.assertFalse(any(isinstance(m, (ExecutionApplied, GatewaySubmit)) for _, m in self.bus.events))
        self.assertFalse(self.oms.ready)
        await self.refresh()
        self.assertTrue(self.oms.ready)
        self.assertEqual(sum(u.positions for u in self.oms.capital_usage()), 400)

    async def test_partial_transfer_conserves_quantity_basis_and_is_idempotent(self):
        await self.oms.handle(report())
        await self.refresh()
        command = await self.transfer(2)
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 2)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).average_price, 100)
        before = self.oms.revision, tuple(self.bus.events)
        await self.oms.handle(command)
        self.assertEqual((self.oms.revision, tuple(self.bus.events)), before)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, quantity=1))
        self.assertFalse(self.oms.ready)
        await self.refresh()
        self.assertEqual(sum(u.positions for u in self.oms.capital_usage()), 400)

    async def test_realized_fees_stay_with_original_owner_after_full_transfer(self):
        await self.refresh()
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 2, 100)))
        order = self.oms.orders('a')[0]
        fill = Execution('open', order.order_id, ROUTE, Side.BUY, 2, 100, 2, START)
        await self.oms.handle(fill)
        await self.transfer(2, 'a', MANUAL)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).realized_pnl, -2)
        self.assertEqual(self.oms.ledger.trades('a'), ())
        self.assertEqual(self.oms.ledger.settlements('a')[0].pnl, -2)
        await self.refresh()
        await self.oms.handle(CloseManual('close', self.oms.revision, ROUTE, 2, 100))
        close = self.oms.orders(MANUAL)[-1]
        await self.oms.handle(Execution('close-fill', close.order_id, ROUTE, Side.SELL, 2, 110, 2, START))
        self.assertEqual(self.oms.ledger.equity(MANUAL)[-1].value, 18)
        self.assertEqual(self.oms.ledger.equity('a')[-1].value, -2)
        await self.oms.handle(fill)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 0)

    async def test_failed_broker_net_reconciliation_is_atomic(self):
        command = replace(report(), observed_positions=(BrokerPosition(ROUTE, 5),))
        with self.assertRaises(ValueError):
            await self.oms.handle(command)
        self.assertEqual(self.oms.ledger.positions(), ())
        self.assertEqual(self.oms.revision, 0)
        self.assertEqual(self.bus.events, [])

    async def test_transfer_rejects_overdraw_wrong_grid_and_stale_revision(self):
        await self.oms.handle(report())
        await self.refresh()
        base = TransferPosition('t', self.oms.revision, MANUAL, 'a', ROUTE, 1)
        for change in ({'quantity': 5}, {'quantity': .5}, {'expected_revision': 0},
                       {'target_id': MANUAL}, {'target_id': 'unknown'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                await self.oms.handle(replace(base, **change))
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 4)

    async def test_manual_can_only_close_and_cannot_oversell_reserved_exit(self):
        await self.oms.handle(report())
        await self.refresh()
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitOrder(MANUAL, OrderIntent(ROUTE, Side.BUY, 1, 100)))
        first = CloseManual('exit', self.oms.revision, ROUTE, 3, 100)
        await self.oms.handle(first)
        await self.oms.handle(first)
        self.assertEqual(len(self.oms.orders(MANUAL)), 1)
        with self.assertRaises(ValueError):
            await self.oms.handle(CloseManual('too-much', self.oms.revision, ROUTE, 2, 100))
        with self.assertRaises(ValueError):
            await self.transfer()
        order = self.oms.orders(MANUAL)[0]
        with self.assertRaises(ValueError):
            await self.oms.handle(ReplaceOrder(MANUAL, order.order_id, order.intent))
        await self.oms.handle(CancelOrder(MANUAL, order.order_id))
        pending = self.oms.orders(MANUAL)[0]
        with self.assertRaises(ValueError):
            await self.transfer()
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 0))
        await self.transfer()

    async def test_external_partial_order_import_deduplicates_past_and_applies_only_new_fills(self):
        order = OrderSnapshot('outside', 'broker-user', OrderIntent(ROUTE, Side.BUY, 5, 100),
                              START - timedelta(minutes=1), OrderStatus.PARTIALLY_FILLED, 2, 100, 2)
        past = Execution('past', order.order_id, ROUTE, Side.BUY, 2, 100, 2, START)
        command = report(quantity=2, orders=(ExternalOrder(order, (past,)),))
        await self.oms.handle(command)
        await self.oms.handle(past)
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 2)
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).realized_pnl, 0)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(past, execution_id='unknown-past', quantity=1))
        self.clock.advance_to(START + timedelta(seconds=1))
        await self.oms.handle(replace(past, execution_id='new', quantity=1, timestamp=self.clock.now()))
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 3)
        self.assertEqual(self.oms.orders(MANUAL)[0].filled_quantity, 3)
        self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))

    async def test_external_order_requires_complete_execution_history_and_observed_orders(self):
        order = OrderSnapshot('outside', 'external', OrderIntent(ROUTE, Side.BUY, 5, 100),
                              START, OrderStatus.PARTIALLY_FILLED, 2, 100, 2)
        with self.assertRaises(ValueError):
            await self.oms.handle(report(quantity=2, orders=(ExternalOrder(order, ()),)))
        order = replace(order, status=OrderStatus.ACCEPTED, filled_quantity=0, average_fill_price=0, commission=0)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(report(orders=(ExternalOrder(order, ()),)), observed_orders=()))
        self.assertEqual(self.oms.ledger.positions(), ())

    async def test_two_competing_transfers_cannot_overdraw_same_position(self):
        await self.oms.handle(report(quantity=1))
        await self.refresh()
        commands = [TransferPosition(n, self.oms.revision, MANUAL, n, ROUTE, 1) for n in ('a', 'b')]
        results = await asyncio.gather(*(self.oms.handle(c) for c in commands), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, ValueError) for r in results), 1)
        self.assertEqual(sum(p.quantity for _, p in self.oms.ledger.positions()), 1)

    async def test_restart_requires_positions_not_only_orders_and_replays_no_submissions(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                await self.oms.handle(report())
                await self.refresh()
                command = await self.transfer(2)
                expected = self.oms.ledger.positions()
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
                await restored.restore()
                restored.gateway_ids.add('sim')
                self.assertEqual(restored.ledger.positions(), expected)
                self.assertEqual(restored.revision, self.oms.revision)
                await restored.handle(command)
                await restored.configure_capital(policy(2000, revision=2, grants=('a', MANUAL)))
                await restored.reconcile_gateway('sim', ())
                self.assertFalse(restored.ready)
                await restored.handle(report(restored.revision, operation='restart', openings=()))
                self.assertTrue(restored.ready)
                self.assertEqual(restored.bus.events, [])

    async def test_journal_failure_does_not_apply_or_consume_operation_id(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            self.oms.journal = journal
            command = report()
            with patch.object(journal, 'append', side_effect=JournalError('disk')):
                with self.assertRaises(JournalError):
                    await self.oms.handle(command)
            self.assertEqual(self.oms.ledger.positions(), ())
            await self.oms.handle(command)
            self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 4)

    async def test_short_transfer_and_weighted_basis_do_not_net_or_create_profit(self):
        await self.oms.handle(report(quantity=-4))
        await self.refresh()
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.SELL, 2, 100)))
        order = self.oms.orders('a')[0]
        await self.oms.handle(Execution('a-short', order.order_id, ROUTE, Side.SELL, 2, 120, 2, START))
        await self.transfer(2)
        position = self.oms.ledger.position('a', ROUTE)
        self.assertEqual((position.quantity, position.average_price, position.realized_pnl), (-4, 110, -2))
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, -2)
        await self.refresh()
        await self.oms.handle(CloseManual('exit-short', self.oms.revision, ROUTE, 2, 100))
        close = self.oms.orders(MANUAL)[-1]
        self.assertEqual(close.intent.side, Side.BUY)
        await self.oms.handle(Execution('cover', close.order_id, ROUTE, Side.BUY, 2, 90, 1, START))
        self.assertEqual(self.oms.ledger.settlements(MANUAL)[0].pnl, 19)

    async def test_opposing_destination_and_opposing_opening_balance_are_atomic_errors(self):
        await self.oms.handle(report())
        await self.refresh()
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.SELL, 1, 100)))
        order = self.oms.orders('a')[0]
        await self.oms.handle(Execution('short', order.order_id, ROUTE, Side.SELL, 1, 100, 0, START))
        before = self.oms.ledger.positions()
        with self.assertRaises(ValueError):
            await self.transfer()
        command = report(self.oms.revision, operation='opposite', quantity=2,
                         openings=(OpeningBalance(ROUTE, -1, 100),))
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, observed_orders=self.oms.orders('a')))
        self.assertEqual(self.oms.ledger.positions(), before)

    async def test_import_residual_keeps_known_owner_and_rejects_metadata_alias(self):
        await self.refresh()
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 2, 100)))
        order = self.oms.orders('a')[0]
        await self.oms.handle(Execution('known', order.order_id, ROUTE, Side.BUY, 2, 100, 1, START))
        command = report(self.oms.revision, quantity=5, openings=(OpeningBalance(ROUTE, 3, 110),))
        command = replace(command, observed_orders=self.oms.orders('a'))
        alias = replace(ROUTE, instrument=replace(ROUTE.instrument, contract_multiplier=10))
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, openings=(OpeningBalance(alias, 3, 110),)))
        await self.oms.handle(command)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 2)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).realized_pnl, -1)
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).average_price, 110)

    async def test_manual_rebalance_weight_zero_preserves_occupied_floor(self):
        from tests_v2.test_rebalance import snapshot, target
        from trading_system.rebalance import ExternalCapital
        from trading_system.capital import AccountKey
        await self.oms.rebalance_capital(
            snapshot(external=(ExternalCapital(AccountKey.from_route(ROUTE), positions=400),)),
            (target(), target(MANUAL, weight=0)), expected_revision=0)
        await self.oms.handle(report(self.oms.revision))
        with self.assertRaises(ValueError):
            target(MANUAL, weight=1)
        plan = await self.oms.rebalance_capital(snapshot(sequence=2),
                                               (target(), target(MANUAL, weight=0)), expected_revision=1)
        self.assertEqual((plan.strategy(MANUAL).occupied, plan.strategy(MANUAL).additional), (400, 0))
        self.assertEqual(plan.strategy('a').additional, 600)
        self.assertEqual(self.oms.capital_policy.accounts[0].external_usage, 0)

    async def test_late_fill_during_cancel_must_finish_before_transfer(self):
        order = OrderSnapshot('outside', 'external', OrderIntent(ROUTE, Side.SELL, 2, 100), START,
                              OrderStatus.ACCEPTED)
        await self.oms.handle(report(orders=(ExternalOrder(order, ()),)))
        await self.refresh()
        await self.oms.handle(CancelOrder(MANUAL, order.order_id))
        pending = self.oms.orders(MANUAL)[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 1))
        with self.assertRaises(ValueError):
            await self.transfer(4)
        self.clock.advance_to(START + timedelta(seconds=1))
        await self.oms.handle(Execution('late', order.order_id, ROUTE, Side.SELL, 1, 110, 1, self.clock.now()))
        self.assertEqual(self.oms.orders(MANUAL)[0].status, OrderStatus.CANCELLED)
        await self.transfer(3)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 3)
        self.assertEqual(self.oms.ledger.settlements(MANUAL)[0].pnl, 9)

    async def test_import_future_stale_duplicate_and_invalid_order_reports_fail_closed(self):
        await self.oms.handle(report())
        revision = self.oms.revision
        variants = (replace(report(revision, operation='next', openings=()), as_of=START + timedelta(seconds=1)),
                    replace(report(revision, operation='next', openings=()), as_of=START - timedelta(seconds=1)),
                    report(0, operation='stale'),
                    replace(report(revision, operation='foreign'), gateway_id='missing'))
        for command in variants:
            with self.subTest(command=command), self.assertRaises(ValueError):
                await self.oms.handle(command)
        self.assertEqual(self.oms.revision, revision)
        base = OrderSnapshot('bad', 'external', OrderIntent(ROUTE, Side.BUY, 2, 100), START,
                             OrderStatus.ACCEPTED)
        for changes in ({'filled_quantity': -1}, {'status': OrderStatus.FILLED},
                        {'status': OrderStatus.CANCEL_PENDING}, {'commission': float('nan')},
                        {'average_fill_price': 100}, {'replaces_order_id': 'unknown'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ExternalOrder(replace(base, **changes), ())

    async def test_close_and_transfer_replay_through_two_restarts(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                await self.oms.handle(report())
                await self.refresh()
                await self.transfer(2)
                await self.refresh()
                command = CloseManual('exit', self.oms.revision, ROUTE, 2, 100)
                await self.oms.handle(command)
                order = self.oms.orders(MANUAL)[0]
                await self.oms.handle(Execution('exit-fill', order.order_id, ROUTE, Side.SELL, 2, 110, 1, START))
                expected = self.oms.ledger.settlements(MANUAL), self.oms.ledger.positions()
            for attempt in range(2):
                with DurableJournal(Path(directory)) as journal:
                    restored = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
                    await restored.restore()
                    restored.gateway_ids.add('sim')
                    await restored.handle(command)
                    self.assertEqual((restored.ledger.settlements(MANUAL), restored.ledger.positions()), expected)
                    await restored.configure_capital(policy(2000, revision=3 + attempt, grants=(MANUAL, 'a')))
                    reconcile = report(restored.revision, operation=f'restart-{attempt}', quantity=2, openings=())
                    await restored.handle(replace(reconcile, observed_orders=self.oms.orders(MANUAL)))
                    self.assertTrue(restored.ready)
                    self.assertEqual(restored.bus.events, [])

    async def test_small_transfer_cannot_create_quantity_at_unrepresentable_float_scale(self):
        await self.oms.handle(report(quantity=1e20))
        await self.refresh()
        before = self.oms.ledger.positions()
        with self.assertRaises(ValueError):
            await self.transfer(1)
        self.assertEqual(self.oms.ledger.positions(), before)

    async def test_decimal_grid_transfer_and_same_direction_import_preserve_basis(self):
        from trading_system.admission import RouteGrant, RouteValuation
        route = replace(ROUTE, instrument=replace(ROUTE.instrument, quantity_step=.1))
        command = InventoryReport('decimal', 0, 'sim', START, (OpeningBalance(route, .3, 100),), (),
                                  (BrokerPosition(route, .3),), ())
        await self.oms.handle(command)
        p = policy(2000, grants=('a', MANUAL))
        await self.oms.configure_capital(replace(p, valuations=(RouteValuation(route, 100),),
                                                grants=(RouteGrant('a', route), RouteGrant(MANUAL, route))))
        await self.oms.handle(TransferPosition('part', self.oms.revision, MANUAL, 'a', route, .1))
        self.assertAlmostEqual(self.oms.ledger.position(MANUAL, route).quantity, .2)
        await self.oms.handle(TransferPosition('rest', self.oms.revision, MANUAL, 'a', route, .2))
        self.assertEqual(self.oms.ledger.position(MANUAL, route).quantity, 0)
        self.assertAlmostEqual(self.oms.ledger.position('a', route).quantity, .3)
        self.assertAlmostEqual(self.oms.ledger.position('a', route).average_price, 100)

    async def test_post_commit_delivery_failure_restores_import_once(self):
        with TemporaryDirectory() as directory:
            command = report()
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                with patch.object(self.bus, 'publish', side_effect=RuntimeError('queue')):
                    with self.assertRaises(RuntimeError):
                        await self.oms.handle(command)
                self.assertFalse(self.oms.ready)
                self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 4)
                with self.assertRaises(JournalError):
                    await self.oms.handle(command)
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                await restored.handle(command)
                self.assertEqual(restored.ledger.position(MANUAL, ROUTE).quantity, 4)
                self.assertEqual(restored.bus.events, [])

    async def test_reconciliation_cannot_accept_orders_submitted_after_its_cutover(self):
        await self.refresh()
        self.clock.advance_to(START + timedelta(seconds=1))
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
        command = report(self.oms.revision, quantity=0, openings=())
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, observed_orders=self.oms.orders('a')))

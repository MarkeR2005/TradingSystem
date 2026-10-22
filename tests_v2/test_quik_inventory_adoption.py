import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import Execution, OrderStatus, OrderIntent, Side, PositionChanged
from trading_system.inventory import AdoptExternalInventory, MANUAL, InventoryReport
from trading_system.persistence import DurableJournal, JournalError
from trading_system.events import DeliveryError
from trading_system.quik_inventory import build_external_inventory_plan
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from trading_system.quik_durable import QuikDurableGateway
from tests_v2.test_quik_contract import SPEC, order_report, trade_report
from tests_v2.test_quik_inventory_plan import BASIS, ORIGIN, NOW, START, ROUTE, BROKER_ID, TRADE_ID, POLICY, snapshot
from tests_v2.test_quik_recovery_runtime import attach, capital


class InventoryAdoptionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = ManualClock(START)
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)
        await capital(self.runtime)
        self.clock.advance_to(NOW)
        self.policy = replace(POLICY, session_id='psb-fixture')
        self.raw = snapshot()
        await self.review()

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()

    async def review(self, operation_id='review', *, basis=(BASIS,), origins=(ORIGIN,)):
        self.plan = build_external_inventory_plan(operation_id, self.raw, self.policy,
            self.runtime.orders.observe_gateway('quik'), self.gateway.order_evidence,
            position_basis=basis, order_origins=origins, now=self.clock.now(), current_revision=self.runtime.orders.revision)
        await self.gateway.stage_external_inventory(self.plan, self.runtime.orders, self.raw, self.policy)
        self.command = self.gateway.inventory_adoption_command('adopt', self.runtime.orders, operation_id)

    async def adopt(self, command=None):
        await self.gateway.adopt_external_inventory(command or self.command, self.runtime.orders, self.raw, self.policy)
        await self.runtime.drain()

    async def restart(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)

    def state(self):
        return (self.runtime.orders.revision, self.runtime.ledger.positions(), self.runtime.orders.orders(MANUAL),
                self.oms.record_count, self.wire.record_count, self.gateway.broker_orders, self.gateway.transactions)

    def trade(self, *, number=TRADE_ID+1, second=3):
        raw = deepcopy(snapshot()['tables']['trades'][0])
        raw.update(trade_num=number, broker_comission='0.75')
        raw['datetime']['sec'] = second
        return raw

    def assert_imported(self):
        position = self.runtime.ledger.position(MANUAL, ROUTE)
        self.assertEqual((position.quantity, position.average_price, position.realized_pnl), (1, 95, 0))
        order = self.runtime.orders.orders(MANUAL)[0]
        self.assertEqual((order.filled_quantity, order.average_fill_price, order.commission), (1, 100, 1.25))
        self.assertEqual(self.gateway.broker_orders, ((BROKER_ID, order.order_id),))
        self.assertFalse(self.runtime.orders.ready)
        self.assertTrue(self.gateway.blocked)
        self.assertEqual(self.gateway.transactions, ())

    async def test_import_uses_declared_cost_and_historical_dedup_baseline(self):
        self.assertEqual(codec.decode(codec.encode(self.command)), self.command)
        await self.adopt()
        self.assert_imported()
        self.assertEqual(self.runtime.orders.external_inventory_adoption('adopt'), self.command)
        self.assertEqual(self.gateway.inventory_adoptions, (self.command,))
        before = self.runtime.ledger.positions()
        await self.gateway.feed('OnTrade', self.raw['tables']['trades'][0])
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.positions(), before)

    async def test_only_new_fill_changes_position_and_books_its_own_fee(self):
        await self.adopt()
        self.clock.advance_to(START+timedelta(seconds=4))
        await self.gateway.feed('OnTrade', self.trade())
        await self.runtime.drain()
        position = self.runtime.ledger.position(MANUAL, ROUTE)
        self.assertEqual((position.quantity, position.average_price, position.realized_pnl), (2, 97.5, -.75))
        self.assertEqual(self.runtime.orders.orders(MANUAL)[0].commission, 2)
        await self.restart()
        await self.adopt()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE), position)
        self.assertEqual(self.gateway.prepared, ())

    async def test_unknown_old_or_equal_boundary_fill_is_quarantined_without_financial_effect(self):
        await self.adopt()
        before = self.runtime.ledger.positions()
        for second in (0, 1):
            with self.assertRaises(ValueError):
                await self.gateway.feed('OnTrade', self.trade(second=second))
            await self.runtime.drain()
            self.assertEqual(self.runtime.ledger.positions(), before)
        await self.restart()
        self.assertEqual(self.runtime.ledger.positions(), before)

    async def test_concurrent_repeat_expiry_and_multiple_restarts_are_noop(self):
        await asyncio.gather(self.adopt(), self.adopt())
        self.clock.advance_to(NOW+timedelta(minutes=1))
        for _ in range(2):
            await self.restart()
            before = self.state()
            await self.adopt()
            self.assertEqual(self.state(), before)
            self.assert_imported()

    async def test_direct_command_and_unstaged_or_tampered_review_are_rejected(self):
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(self.command)
        before = self.state()
        for changes in (dict(operation_id='absent'), dict(snapshot_digest='b'*64)):
            with self.assertRaises((KeyError, ValueError)):
                await self.adopt(replace(self.command, review=replace(self.plan, **changes)))
        self.assertEqual(self.state(), before)

    async def test_stale_revision_expiry_or_changed_source_cannot_begin(self):
        before = self.state()
        with self.assertRaises(ValueError):
            await self.adopt(replace(self.command, expected_revision=self.command.expected_revision-1))
        with patch.object(self.gateway, '_number', self.gateway._number+1):
            with self.assertRaises(ValueError):
                await self.adopt()
        self.clock.advance_to(NOW+timedelta(minutes=1))
        with self.assertRaises(ValueError):
            await self.adopt()
        self.assertEqual(self.state(), before)

    async def test_failure_before_oms_commit_leaves_gateway_unbound_and_finances_empty(self):
        before = self.state()
        with patch.object(self.oms, 'append', side_effect=JournalError('OMS write failed')):
            with self.assertRaises(JournalError):
                await self.adopt()
        self.assertEqual(self.state(), before)

    async def test_crash_between_journals_finishes_from_saved_oms_decision_after_expiry(self):
        with patch.object(self.gateway, '_commit', side_effect=JournalError('crash before gateway input')):
            with self.assertRaises(JournalError):
                await self.adopt()
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        self.assertEqual(self.gateway.broker_orders, ())
        self.clock.advance_to(NOW+timedelta(minutes=1))
        await self.restart()
        self.command = self.runtime.orders.external_inventory_adoption('adopt')
        await self.adopt()
        self.assert_imported()

    async def test_gateway_input_without_result_is_completed_by_replay(self):
        append = self.gateway._append
        def fail_result(record):
            if record['kind'] == 'result':
                raise JournalError('crash before result')
            append(record)
        with patch.object(self.gateway, '_append', side_effect=fail_result):
            with self.assertRaises(JournalError):
                await self.adopt()
        await self.runtime.drain()
        await self.restart()
        await self.adopt()
        self.assert_imported()

    async def test_lost_delivery_of_post_boundary_pending_fill_is_replayed_once(self):
        # A previously unknown callback is parked before the plan is refreshed.
        self.clock.advance_to(START+timedelta(seconds=4))
        await self.gateway.feed('OnTrade', self.trade())
        await self.runtime.drain()
        await self.review('review-with-pending')
        publish = self.runtime.bus.publish
        async def lose_fill(recipient, message):
            if isinstance(message, Execution):
                raise RuntimeError('lost delivery')
            await publish(recipient, message)
        with patch.object(self.runtime.bus, 'publish', side_effect=lose_fill):
            with self.assertRaises(RuntimeError):
                await self.adopt()
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        await self.restart()
        await self.adopt()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 2)
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).realized_pnl, -.75)

    async def test_import_never_removes_full_reconciliation_block(self):
        await self.adopt()
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2, as_of=NOW))
        await self.runtime.orders.reconcile_gateway('quik', self.runtime.orders.orders(MANUAL))
        self.assertFalse(self.runtime.orders.ready)
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(InventoryReport('bypass', self.runtime.orders.revision, 'quik', NOW, (), (), (), ()))

    async def test_active_plan_or_deficit_prevents_financial_import(self):
        for method in ('trading_system.plan_execution.PlanBook.active', 'trading_system.deficit_execution.DeficitBook.active'):
            before = self.state()
            with patch(method, return_value=True):
                with self.assertRaises(ValueError):
                    await self.adopt()
            self.assertEqual(self.state(), before)

    async def test_mismatched_journals_and_conflicting_operation_id_are_refused(self):
        await self.adopt()
        with self.assertRaises(ValueError):
            await self.adopt(replace(self.command, expected_revision=self.command.expected_revision+1))
        with patch.object(self.runtime.orders, '_inventory_operations', {}):
            with self.assertRaises(JournalError):
                await self.adopt()

    async def test_fsync_failure_after_oms_record_recovers_one_import_then_binds(self):
        with patch('trading_system.persistence.os.fsync', side_effect=OSError('uncertain fsync')):
            with self.assertRaises(JournalError):
                await self.adopt()
        self.assertEqual(self.runtime.ledger.positions(), ())
        self.assertEqual(self.gateway.broker_orders, ())
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        with self.assertRaises(JournalError):
            await self.adopt()
        await self.restart()
        await self.adopt()
        self.assert_imported()

    async def test_notification_failure_after_oms_commit_requires_restart_before_binding(self):
        publish = self.runtime.bus.publish
        async def fail_notification(recipient, message):
            if isinstance(message, PositionChanged):
                raise RuntimeError('notification failed after commit')
            await publish(recipient, message)
        with patch.object(self.runtime.bus, 'publish', side_effect=fail_notification):
            with self.assertRaises(RuntimeError):
                await self.adopt()
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        self.assertEqual(self.gateway.broker_orders, ())
        with self.assertRaises(JournalError):
            await self.adopt()
        await self.restart()
        await self.adopt()
        self.assert_imported()

    async def test_gateway_fsync_failure_recovers_durable_input_and_keeps_single_baseline(self):
        append = self.wire.append
        def fail_fsync(record):
            with patch('trading_system.persistence.os.fsync', side_effect=OSError('gateway fsync')):
                append(record)
        with patch.object(self.wire, 'append', side_effect=fail_fsync):
            with self.assertRaises(JournalError):
                await self.adopt()
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        await self.restart()
        await self.adopt()
        self.assert_imported()

    async def test_wrong_gateway_prefix_cannot_resume_a_saved_decision(self):
        with patch.object(self.gateway, '_commit', side_effect=JournalError('between journals')):
            with self.assertRaises(JournalError):
                await self.adopt()
        await self.runtime.drain()
        await self.restart()
        before = self.state()
        with patch.object(self.wire, '_entries', []):
            with self.assertRaises(JournalError):
                await self.adopt()
        self.assertEqual(self.state(), before)

    async def test_recovery_refuses_wrong_oms_before_replaying_financial_messages(self):
        await self.adopt()
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.oms = DurableJournal(self.root/'different-oms')
        self.wire = DurableJournal(self.root/'wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway = await QuikDurableGateway.open('quik', (SPEC,), self.clock, journal=self.wire,
            session_id='psb-fixture', timezone_offset=timedelta(hours=3))
        self.runtime.add_gateway(self.gateway)
        with patch.object(self.runtime.bus, 'publish') as publish:
            with self.assertRaises(JournalError):
                await self.gateway.recover(self.runtime.orders)
            publish.assert_not_called()
        self.assertEqual(self.runtime.ledger.positions(), ())

    async def test_pending_historical_duplicates_are_absorbed_without_rebooking(self):
        await self.gateway.feed('OnTrade', self.raw['tables']['trades'][0])
        await self.gateway.feed('OnOrder', self.raw['tables']['orders'][0])
        await self.runtime.drain()
        await self.review('with-history')
        await self.adopt()
        self.assert_imported()
        self.assertEqual(self.gateway.unmatched, ())

    async def test_conflicting_pending_history_is_rejected_before_either_adoption_commit(self):
        conflicting = dict(self.raw['tables']['trades'][0], broker_comission='9')
        await self.gateway.feed('OnTrade', conflicting)
        await self.runtime.drain()
        await self.review('conflicting-history')
        before = self.state()
        with self.assertRaises(ValueError):
            await self.adopt()
        self.assertEqual(self.state(), before)

    async def test_unrelated_unmatched_order_blocks_adoption(self):
        await self.gateway.feed('OnOrder', dict(self.raw['tables']['orders'][0], order_num=123))
        await self.runtime.drain()
        await self.review('unrelated-order')
        before = self.state()
        with self.assertRaises(ValueError):
            await self.adopt()
        self.assertEqual(self.state(), before)

    async def test_changed_callback_contract_is_refused(self):
        before = self.state()
        for policy in (replace(self.policy, timezone_offset=timedelta(hours=2)),
                       replace(self.policy, routes=(replace(self.policy.routes[0], commission_fields=()),))):
            with self.assertRaises(ValueError):
                await self.gateway.adopt_external_inventory(self.command, self.runtime.orders, self.raw, policy)
        self.assertEqual(self.state(), before)

    async def test_conflicting_historical_callback_after_adoption_is_quarantined(self):
        await self.adopt()
        before = self.runtime.ledger.positions()
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', dict(self.raw['tables']['trades'][0], price=99))
        await self.runtime.drain()
        await self.restart()
        self.assertEqual(self.runtime.ledger.positions(), before)

    async def known_order(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.clock = ManualClock(START)
        self.oms = DurableJournal(self.root/'known-oms')
        self.wire = DurableJournal(self.root/'known-wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)
        await capital(self.runtime)
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        await self.gateway.feed('OnOrder', order_report(1, balance=3, order_num=123))
        await self.gateway.feed('OnTrade', trade_report(trade_num=456, order_num=123))
        await self.runtime.drain()
        self.clock.advance_to(NOW)
        for key in ('tables', 'previous_tables'):
            self.raw[key]['orders'].append(dict(self.raw[key]['orders'][0], order_num=123))
            self.raw[key]['trades'].append(dict(self.raw[key]['trades'][0], trade_num=456, order_num=123))
            self.raw[key]['holdings'][0]['totalnet'] = 2
        await self.review('known-and-external')

    async def test_external_order_with_colliding_transaction_number_uses_its_broker_binding(self):
        await self.known_order()
        known = self.context.orders()[0]
        await self.adopt()
        await self.gateway.feed('OnOrder', dict(self.raw['tables']['orders'][0], trans_id=1))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0], known)
        self.assertEqual(len(self.gateway.broker_orders), 2)
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)

    async def test_known_orders_receive_the_same_cutoff_as_oms(self):
        await self.known_order()
        await self.adopt()
        before = self.runtime.ledger.positions()
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', trade_report(trade_num=457, order_num=123))
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.positions(), before)

    async def test_cancelled_external_order_is_not_resurrected_by_old_active_report(self):
        for key in ('tables', 'previous_tables'):
            self.raw[key]['orders'][0]['flags'] = 10
        await self.review('cancelled')
        await self.adopt()
        await self.gateway.feed('OnOrder', dict(self.raw['tables']['orders'][0], flags=9, balance=4))
        await self.runtime.drain()
        self.assertEqual(self.runtime.orders.orders(MANUAL)[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.gateway.transactions, ())

    async def test_carry_position_without_order_history_is_adopted_without_synthetic_execution(self):
        for key in ('tables', 'previous_tables'):
            self.raw[key]['orders'] = []
            self.raw[key]['trades'] = []
        await self.review('carry', origins=())
        await self.adopt()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)
        self.assertEqual(self.runtime.orders.orders(MANUAL), ())
        self.assertEqual(self.gateway.execution_evidence.executions, ())
        self.assertFalse(self.runtime.orders.ready)

    async def test_unfilled_external_order_can_receive_its_first_post_boundary_execution(self):
        for key in ('tables', 'previous_tables'):
            self.raw[key]['orders'][0]['balance'] = 4
            self.raw[key]['trades'] = []
            self.raw[key]['holdings'][0]['totalnet'] = 0
        await self.review('unfilled', basis=())
        await self.adopt()
        self.assertEqual(self.runtime.ledger.positions(), ())
        self.clock.advance_to(START+timedelta(seconds=4))
        await self.gateway.feed('OnTrade', self.trade())
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 1)

    async def test_recovered_partial_adoption_handles_new_pending_fill_after_saved_boundary(self):
        with patch.object(self.gateway, '_commit', side_effect=JournalError('between journals')):
            with self.assertRaises(JournalError):
                await self.adopt()
        await self.runtime.drain()
        self.clock.advance_to(NOW+timedelta(minutes=1))
        await self.restart()
        await self.gateway.feed('OnTrade', self.trade())
        await self.runtime.drain()
        await self.adopt()
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).quantity, 2)
        self.assertEqual(self.runtime.ledger.position(MANUAL, ROUTE).realized_pnl, -.75)

    async def test_missing_or_degraded_journals_and_failed_gateway_refuse_adoption(self):
        before = self.state()
        for obj, field, value in ((self.runtime.orders, 'journal', None), (self.oms, 'degraded', True),
                                  (self.wire, 'degraded', True), (self.gateway, '_failed', True)):
            with patch.object(obj, field, value):
                with self.assertRaises(JournalError):
                    await self.adopt()
            self.assertEqual(self.state(), before)

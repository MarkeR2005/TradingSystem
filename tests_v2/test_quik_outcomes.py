import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import (Execution, GatewayAccepted, GatewayCancelled, GatewayCancelRejected,
                                  GatewayRejected, GatewayUncertain, OrderIntent, OrderStatus, OrderUpdated, Side)
from trading_system.inventory import ResolveOrders
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_outcomes import build_order_resolution
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.test_quik_compare import POLICY, NOW, START, ROUTE, snapshot, change_tables, order_report, trade_report
from tests_v2.test_quik_recovery_runtime import attach, capital


class OrderResolutionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = ManualClock(START)
        self.oms = DurableJournal(self.root / 'oms')
        self.wire = DurableJournal(self.root / 'quik')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)
        await capital(self.runtime)
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        self.order_id = self.context.orders()[0].order_id
        self.policy = replace(POLICY, session_id='psb-fixture')

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()

    async def feed(self, event, data, drop=()):
        publish = self.runtime.bus.publish

        async def omit(recipient, message):
            if not isinstance(message, drop):
                await publish(recipient, message)

        with patch.object(self.runtime.bus, 'publish', side_effect=omit):
            await self.gateway.feed(event, data)
        await self.runtime.drain()

    def raw(self, flags=9, filled=0, absent=False):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows.clear() if absent else
            rows[0].update(flags=flags, balance=4-filled))
        if not filled:
            raw = change_tables(raw, 'trades', lambda rows: rows.clear())
        return change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=filled))

    def build(self, raw=None, **kwargs):
        if self.clock.now() < NOW:
            self.clock.advance_to(NOW)
        raw = self.raw() if raw is None else raw
        raw['finished_at'] = self.clock.now().isoformat()
        return build_order_resolution('resolve-1', raw, self.policy,
            self.runtime.orders.observe_gateway('quik'), self.gateway.order_evidence,
            now=self.clock.now(), current_revision=self.runtime.orders.revision, **kwargs)

    async def apply(self, command):
        await self.gateway.apply_order_resolution(command, self.runtime.orders)
        await self.runtime.drain()

    def state(self):
        return (self.runtime.orders.revision, self.context.orders(), self.runtime.ledger.positions(),
                self.runtime.orders.capital_usage(), self.oms.record_count, self.wire.record_count,
                self.gateway.transactions)

    async def accepted(self, lost=False):
        await self.feed('OnOrder', order_report(1), (GatewayAccepted,) if lost else ())

    async def cancellation(self, *, replace_order=False, filled=0, lost=True):
        await self.accepted()
        if filled:
            await self.feed('OnTrade', trade_report())
        if replace_order:
            await self.context.replace_order(self.order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
        else:
            await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnOrder', order_report(1, balance=4-filled, flags=10),
                        (GatewayCancelled,) if lost else ())

    async def restart(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.oms = DurableJournal(self.root / 'oms')
        self.wire = DurableJournal(self.root / 'quik')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)

    async def test_lost_acceptance_becomes_one_durable_resolution_and_retains_reserve(self):
        await self.accepted(lost=True)
        before = self.state()
        command = self.build()
        self.assertEqual(codec.decode(codec.encode(command)), command)
        self.assertEqual(self.state(), before)
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 600)
        self.assertFalse(self.runtime.orders.ready)
        records = [r for r in self.oms.records if r.get('message', {}).get('type') == 'ResolveOrders']
        self.assertEqual(len(records), 1)
        self.assertEqual(len(self.gateway.transactions), 1)

    async def test_missing_order_without_positive_outcome_never_means_rejected(self):
        before = self.state()
        with self.assertRaises(ValueError):
            self.build(self.raw(absent=True))
        self.assertEqual(self.state(), before)

    async def test_saved_rejection_resolves_unknown_submit_without_a_broker_order(self):
        await self.feed('OnTransReply', dict(trans_id=1, status=5, result_msg='fixture reject'), (GatewayRejected,))
        command = self.build(self.raw(absent=True))
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)
        self.assertEqual(self.context.orders()[0].rejection_reason, 'fixture reject')
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 0)
        self.assertFalse(self.runtime.orders.ready)

    async def test_confirmed_cancel_releases_only_the_unfilled_order_and_preserves_position(self):
        await self.cancellation(filled=1)
        position = self.runtime.ledger.positions()
        command = self.build(self.raw(flags=10, filled=1))
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.runtime.ledger.positions(), position)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 0)
        self.assertEqual(sum(u.positions for u in self.runtime.orders.capital_usage()), 150)
        self.assertFalse(self.runtime.orders.ready)

    async def test_pending_replacement_requires_explicit_abandonment(self):
        await self.cancellation(replace_order=True)
        command = self.build(self.raw(flags=10))
        before = self.state()
        with self.assertRaisesRegex(ValueError, 'replacement'):
            await self.apply(command)
        self.assertEqual(self.state(), before)
        command = self.build(self.raw(flags=10), abandon_replacements=(self.order_id,))
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(len(self.gateway.transactions), 2)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 0)

    async def test_cancel_rejection_restores_active_order_and_keeps_reserve(self):
        await self.accepted()
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnTransReply', dict(trans_id=2, status=5, result_msg='cancel refused'), (GatewayCancelRejected,))
        await self.apply(self.build())
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)
        self.assertEqual(self.context.orders()[0].cancel_rejection_reason, 'cancel refused')
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 600)

    async def test_successful_kill_transaction_is_not_final_cancellation_evidence(self):
        await self.accepted()
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnTransReply', dict(trans_id=2, status=3))
        with self.assertRaises(ValueError):
            self.build(self.raw(flags=10))

    async def test_snapshot_cancellation_without_saved_barrier_is_refused(self):
        await self.accepted()
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        with self.assertRaises(ValueError):
            self.build(self.raw(flags=10))

    async def test_missing_late_fill_must_be_repaired_before_releasing_cancel_reserve(self):
        await self.accepted()
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnOrder', order_report(1, flags=10, balance=3), (GatewayCancelled,))
        await self.feed('OnTrade', trade_report(), (Execution,))
        before = self.state()
        with self.assertRaises(ValueError):
            self.build(self.raw(flags=10, filled=1))
        self.assertEqual(self.state(), before)

    async def test_unchanged_orders_do_not_create_empty_resolution(self):
        await self.accepted()
        with self.assertRaises(ValueError):
            self.build()

    async def test_external_order_is_not_imported_by_resolution(self):
        await self.accepted(lost=True)
        raw = change_tables(self.raw(), 'orders', lambda rows: rows.append(dict(rows[0], order_num=999)))
        with self.assertRaises(ValueError):
            self.build(raw)

    async def test_stale_oms_or_source_revision_leaves_state_unchanged(self):
        await self.accepted(lost=True)
        command = self.build()
        await self.gateway.feed('OnConnected', {})
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(command)
        self.assertEqual(self.state(), before)
        command = self.build()
        await self.runtime.orders.handle(GatewayUncertain('quik', 'changed'))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(command)
        self.assertEqual(self.state(), before)

    async def test_expired_resolution_is_refused(self):
        await self.accepted(lost=True)
        command = self.build()
        self.clock.advance_to(NOW + timedelta(minutes=1))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(command)
        self.assertEqual(self.state(), before)

    async def test_direct_oms_command_is_refused(self):
        await self.accepted(lost=True)
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(self.build())

    async def test_exact_repeat_after_expiry_and_source_change_is_noop(self):
        await self.accepted(lost=True)
        command = self.build()
        await asyncio.gather(self.apply(command), self.apply(command))
        self.clock.advance_to(NOW + timedelta(minutes=1))
        await self.gateway.feed('OnConnected', {})
        before = self.state()
        await self.apply(command)
        self.assertEqual(self.state(), before)
        with self.assertRaises(ValueError):
            await self.apply(replace(command, snapshot_digest='a'*64))

    async def test_append_failure_exposes_no_changes_or_notifications(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10))
        before = self.state()
        with patch.object(self.oms, 'append', side_effect=JournalError('write failed')):
            with patch.object(self.runtime.bus, 'publish') as publish:
                with self.assertRaises(JournalError):
                    await self.apply(command)
                publish.assert_not_called()
        self.assertEqual(self.state(), before)

    async def test_restart_and_redelivery_keep_resolved_cancel_and_discarded_replacement(self):
        await self.cancellation(replace_order=True, filled=1)
        command = self.build(self.raw(flags=10, filled=1), abandon_replacements=(self.order_id,))
        await self.apply(command)
        position = self.runtime.ledger.positions()
        for _ in range(2):
            await self.restart()
            self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
            self.assertEqual(self.runtime.ledger.positions(), position)
            self.assertEqual(len(self.context.orders()), 1)
            self.assertEqual(self.gateway.prepared, ())
            self.assertFalse(self.runtime.orders.ready)
            before = self.state()
            await self.apply(command)
            self.assertEqual(self.state(), before)

    async def test_notification_failure_requires_restart_without_releasing_twice(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10))
        publish = self.runtime.bus.publish

        async def fail(recipient, message):
            if isinstance(message, OrderUpdated):
                raise RuntimeError('after commit')
            await publish(recipient, message)

        with patch.object(self.runtime.bus, 'publish', side_effect=fail):
            with self.assertRaises(RuntimeError):
                await self.apply(command)
        with self.assertRaises(JournalError):
            await self.apply(command)
        await self.restart()
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 0)

    async def test_previous_known_rejection_does_not_block_another_order_acceptance(self):
        await self.feed('OnTransReply', dict(trans_id=1, status=5, result_msg='old rejection'))
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        await self.feed('OnOrder', order_report(2), (GatewayAccepted,))
        await self.apply(self.build())
        self.assertEqual([o.status for o in self.context.orders()], [OrderStatus.REJECTED, OrderStatus.ACCEPTED])
        await self.restart()
        self.assertEqual([o.status for o in self.context.orders()], [OrderStatus.REJECTED, OrderStatus.ACCEPTED])

    async def test_lost_acceptance_before_failed_cancel_restores_accepted_not_submitted(self):
        await self.accepted(lost=True)
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnTransReply', dict(trans_id=2, status=5, result_msg='refused'), (GatewayCancelRejected,))
        await self.apply(self.build())
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 600)

    async def test_known_barrier_waiting_for_repaired_fill_can_be_resolved_explicitly(self):
        from trading_system.quik_repair import build_execution_repair
        await self.accepted()
        await self.context.replace_order(self.order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
        await self.runtime.drain()
        await self.feed('OnOrder', order_report(1, flags=10, balance=3))
        await self.feed('OnTrade', trade_report(), (Execution,))
        self.clock.advance_to(NOW)
        raw = self.raw(flags=10, filled=1)
        repair = build_execution_repair('fill-first', raw, self.policy, self.runtime.orders.observe_gateway('quik'),
            self.gateway.execution_evidence, now=NOW, current_revision=self.runtime.orders.revision)
        await self.gateway.apply_execution_repair(repair, self.runtime.orders)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        command = self.build(raw, abandon_replacements=(self.order_id,))
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)
        self.assertEqual(len(self.context.orders()), 1)

    async def test_forged_payload_cannot_release_reserve(self):
        await self.cancellation(filled=1)
        command = self.build(self.raw(flags=10, filled=1))
        for changes in (dict(filled_quantity=0), dict(request_id='wrong-request')):
            altered = tuple(replace(m, **changes) if isinstance(m, GatewayCancelled) else m for m in command.outcomes)
            before = self.state()
            with self.assertRaises(ValueError):
                await self.apply(replace(command, outcomes=altered))
            self.assertEqual(self.state(), before)

    async def test_wrong_final_status_or_net_position_discards_all_outcomes(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10))
        for changes in (dict(order_targets=(replace(command.order_targets[0], status=OrderStatus.ACCEPTED),)),
                        dict(observed_positions=(replace(command.observed_positions[0], quantity=1),))):
            before = self.state()
            with self.assertRaises(ValueError):
                await self.apply(replace(command, **changes))
            self.assertEqual(self.state(), before)

    async def test_abandonment_cannot_name_an_order_without_pending_replacement(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10), abandon_replacements=(self.order_id,))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(command)
        self.assertEqual(self.state(), before)

    async def test_malformed_command_and_unknown_abandonment_are_rejected(self):
        await self.accepted(lost=True)
        command = self.build()
        for changes in (dict(outcomes=()), dict(outcomes=command.outcomes*2), dict(gateway_id='wrong'),
                        dict(source_digest='bad'), dict(expires_at=START), dict(abandon_replacements=('unknown',))):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(command, **changes)

    async def test_wrong_session_and_stale_snapshot_cannot_produce_plan(self):
        await self.accepted(lost=True)
        with patch.object(self, 'policy', replace(self.policy, session_id='other')):
            with self.assertRaises(ValueError):
                self.build()
        self.clock.advance_to(NOW+timedelta(minutes=1))
        with self.assertRaises(ValueError):
            self.build()

    async def test_fsync_failure_requires_restart_and_restores_one_resolution(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10))
        before = self.context.orders()
        with patch('trading_system.persistence.os.fsync', side_effect=OSError('fsync failed')):
            with self.assertRaises(JournalError):
                await self.apply(command)
        self.assertEqual(self.context.orders(), before)
        with self.assertRaises(JournalError):
            await self.apply(command)
        await self.restart()
        await self.apply(command)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)

    async def test_failed_or_degraded_source_and_missing_oms_journal_are_refused(self):
        await self.accepted(lost=True)
        command = self.build()
        before = self.state()
        for obj, field, value in ((self.gateway, '_failed', True), (self.gateway, '_tail', -1),
                                  (self.wire, 'degraded', True), (self.runtime.orders, 'journal', None)):
            with patch.object(obj, field, value):
                with self.assertRaises(JournalError):
                    await self.apply(command)
            self.assertEqual(self.state(), before)

    async def test_active_plan_and_deficit_do_not_advance_during_resolution(self):
        await self.cancellation()
        command = self.build(self.raw(flags=10))
        for method in ('trading_system.plan_execution.PlanBook.active',
                       'trading_system.deficit_execution.DeficitBook.owns_route'):
            before = self.state()
            with patch(method, return_value=True):
                with self.assertRaises(ValueError):
                    await self.apply(command)
            self.assertEqual(self.state(), before)

    async def test_batch_failure_on_second_order_does_not_commit_first_acceptance(self):
        from trading_system.orders import OrderManager
        await self.accepted(lost=True)
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        second = self.context.orders()[1].order_id
        await self.feed('OnOrder', order_report(2, order_num=12345), (GatewayAccepted,))
        raw = change_tables(self.raw(), 'orders', lambda rows: rows.append(dict(rows[0], order_num=12345, trans_id=2)))
        command = self.build(raw)
        original = OrderManager._accepted

        async def fail_second(oms, message):
            if message.order_id == second:
                raise ValueError('second outcome failed')
            await original(oms, message)

        before = self.state()
        with patch.object(OrderManager, '_accepted', new=fail_second):
            with self.assertRaisesRegex(ValueError, 'second outcome'):
                await self.apply(command)
        self.assertEqual(self.state(), before)
        await self.apply(command)
        self.assertTrue(all(o.status is OrderStatus.ACCEPTED for o in self.context.orders()))

    async def test_prior_cancel_rejection_cannot_resolve_a_new_request(self):
        await self.accepted()
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        await self.feed('OnTransReply', dict(trans_id=2, status=5, result_msg='old refusal'))
        await self.context.cancel_order(self.order_id)
        await self.runtime.drain()
        with self.assertRaises(ValueError):
            self.build()

    async def test_fresh_capital_after_resolution_does_not_resume_trading(self):
        await self.accepted(lost=True)
        await self.apply(self.build())
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2, as_of=NOW))
        self.assertFalse(self.runtime.orders.ready)
        with self.assertRaises(RuntimeError):
            await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))

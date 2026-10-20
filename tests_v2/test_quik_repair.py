from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import Execution, ExecutionApplied, GatewayUncertain, OrderIntent, OrderStatus, Side
from trading_system.inventory import RepairExecutions
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_repair import ExecutionEvidence, build_execution_repair
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.test_quik_compare import (POLICY, OMS, LINKS, FILL, ORDER, NOW, snapshot, change_tables, ROUTE,
                                        BROKER_ID, START, trade_report, order_report)
from tests_v2.test_quik_recovery_runtime import attach, capital

EMPTY = replace(OMS, orders=(replace(ORDER, status=OrderStatus.ACCEPTED, filled_quantity=0,
                                    average_fill_price=0, commission=0),), executions=(), positions=())
EVIDENCE = ExecutionEvidence(LINKS, (FILL,))


def plan(raw=None, view=EMPTY, evidence=EVIDENCE, **kwargs):
    return build_execution_repair('repair-1', snapshot() if raw is None else raw, POLICY, view, evidence,
                                  now=kwargs.get('now', NOW), current_revision=kwargs.get('revision', view.revision))


class RepairPlanningTests(TestCase):
    def test_missing_fill_becomes_exact_durable_plan_without_applying_it(self):
        command = plan()
        self.assertEqual(command.executions, (FILL,))
        self.assertEqual(command.expected_revision, EMPTY.revision)
        self.assertEqual(command.source_revision, LINKS.revision)
        self.assertEqual(command.observed_positions[0].quantity, 1)
        self.assertEqual(command.order_targets[0].filled_quantity, 1)
        self.assertEqual(EMPTY.executions, ())
        self.assertEqual(codec.decode(codec.encode(command)), command)

    def test_duplicate_snapshot_trade_is_not_two_repairs(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(deepcopy(rows[0])))
        self.assertEqual(plan(raw).executions, (FILL,))

    def test_already_applied_or_no_difference_does_not_create_empty_operation(self):
        with self.assertRaises(ValueError):
            plan(view=OMS)

    def test_committed_execution_must_match_every_financial_field(self):
        for field, value in [('price', 99), ('commission', 7), ('quantity', 2), ('side', Side.SELL),
                             ('order_id', 'other'), ('timestamp', START - timedelta(seconds=1))]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                plan(evidence=ExecutionEvidence(LINKS, (replace(FILL, **{field: value}),)))

    def test_binding_without_a_saved_payload_is_insufficient(self):
        with self.assertRaises(ValueError):
            plan(evidence=ExecutionEvidence(LINKS, ()))

    def test_foreign_session_changed_revision_and_stale_snapshot_are_refused(self):
        with self.assertRaises(ValueError):
            plan(evidence=ExecutionEvidence(replace(LINKS, session_id='wrong'), (FILL,)))
        with self.assertRaises(ValueError):
            plan(revision=8)
        with self.assertRaises(ValueError):
            plan(now=NOW + timedelta(minutes=1))

    def test_unrelated_external_order_and_wrong_price_are_not_repaired(self):
        for update in (lambda rows: rows.append(dict(rows[0], order_num=999)),
                       lambda rows: rows[0].update(price=101)):
            with self.subTest(update=update), self.assertRaises(ValueError):
                plan(change_tables(snapshot(), 'orders', update))

    def test_pending_cancel_can_receive_positive_fill_without_a_cancellation_decision(self):
        view = replace(EMPTY, orders=(replace(EMPTY.orders[0], status=OrderStatus.CANCEL_PENDING),))
        self.assertEqual(plan(view=view).executions, (FILL,))

    def test_hole_before_known_later_execution_requires_history_rebuild(self):
        later = replace(FILL, execution_id='fixture:9007199254740996')
        links = replace(LINKS, executions=LINKS.executions + (replace(LINKS.executions[0],
                         broker_id=9007199254740996, local_id=later.execution_id),))
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0], trade_num=9007199254740996)))
        raw = change_tables(raw, 'orders', lambda rows: rows[0].update(balance=2))
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=2))
        with self.assertRaisesRegex(ValueError, 'suffix'):
            plan(raw, view=replace(OMS, executions=(later,)), evidence=ExecutionEvidence(links, (FILL, later)))

    def test_digest_changes_with_payload_mapping_and_revision(self):
        self.assertNotEqual(EVIDENCE.digest, ExecutionEvidence(replace(LINKS, revision=5), (FILL,)).digest)
        self.assertNotEqual(EVIDENCE.digest, ExecutionEvidence(LINKS, (replace(FILL, price=99),)).digest)
        self.assertEqual(EVIDENCE.digest, ExecutionEvidence(LINKS, (FILL,)).digest)

    def test_plan_rejects_empty_duplicate_wrong_gateway_or_invalid_deadline(self):
        command = plan()
        for change in (dict(executions=()), dict(executions=(FILL, FILL)),
                       dict(gateway_id='other'), dict(expires_at=START), dict(source_digest='bad')):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(command, **change)


class RepairApplicationTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = ManualClock(START)
        self.oms_journal = DurableJournal(self.root / 'oms')
        self.gateway_journal = DurableJournal(self.root / 'gateway')
        self.runtime = TradingRuntime(self.clock, journal=self.oms_journal, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.gateway_journal, self.clock)
        await capital(self.runtime)
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
        await self.runtime.drain()
        await self.gateway.feed('OnOrder', order_report(1))
        await self.runtime.drain()
        publish = self.runtime.bus.publish

        async def lose_execution(recipient, message):
            if not isinstance(message, Execution):
                await publish(recipient, message)

        with patch.object(self.runtime.bus, 'publish', side_effect=lose_execution):
            await self.gateway.feed('OnTrade', trade_report())
        await self.runtime.drain()
        self.clock.advance_to(NOW)
        self.policy = replace(POLICY, session_id='psb-fixture')
        self.command = self.build()

    def build(self, operation='repair-1', raw=None):
        view = self.runtime.orders.observe_gateway('quik')
        return build_execution_repair(operation, raw or snapshot(), self.policy, view,
            self.gateway.execution_evidence, now=self.clock.now(), current_revision=self.runtime.orders.revision)

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms_journal.close()
        self.gateway_journal.close()

    def state(self):
        return (self.runtime.orders.revision, self.runtime.ledger.positions(),
                self.runtime.orders.capital_usage(), self.oms_journal.record_count,
                self.gateway_journal.record_count, self.gateway.prepared)

    async def apply(self, command=None):
        await self.gateway.apply_execution_repair(command or self.command, self.runtime.orders)
        await self.runtime.drain()

    async def test_apply_records_one_atomic_batch_then_books_fill_and_keeps_trading_blocked(self):
        before = self.oms_journal.record_count
        await self.apply()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)
        self.assertEqual(self.context.orders()[0].commission, 1.25)
        self.assertEqual(sum(u.orders for u in self.runtime.orders.capital_usage()), 450)
        self.assertFalse(self.runtime.orders.ready)
        repairs = [r for r in self.oms_journal.records[before:] if r.get('message', {}).get('type') == 'RepairExecutions']
        self.assertEqual(len(repairs), 1)
        self.assertEqual(len(self.gateway.prepared), 1)
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2, as_of=NOW))
        self.assertFalse(self.runtime.orders.ready)

    async def test_exact_retry_after_deadline_and_gateway_progress_is_a_noop(self):
        await self.apply()
        self.clock.advance_to(NOW + timedelta(minutes=2))
        await self.gateway.feed('OnConnected', {})
        before = self.state()
        await self.apply()
        self.assertEqual(self.state(), before)

    async def test_conflicting_reuse_of_operation_id_is_rejected(self):
        await self.apply()
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(replace(self.command, snapshot_digest='a' * 64))
        self.assertEqual(self.state(), before)

    async def test_stale_oms_revision_is_rejected_without_mutation(self):
        await self.runtime.orders.handle(GatewayUncertain('quik', 'changed'))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def test_changed_gateway_revision_is_rejected_without_mutation(self):
        await self.gateway.feed('OnConnected', {})
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def test_expired_plan_rejected_without_mutation(self):
        self.clock.advance_to(NOW + timedelta(minutes=1))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def test_unverified_direct_oms_message_is_refused(self):
        before = self.state()
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(self.command)
        self.assertEqual(self.state(), before)

    async def test_tampered_execution_cannot_pass_saved_gateway_evidence(self):
        command = replace(self.command, executions=(replace(self.command.executions[0], commission=99),))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(command)
        self.assertEqual(self.state(), before)

    async def test_failed_final_position_check_discards_entire_candidate(self):
        target = replace(self.command.observed_positions[0], quantity=2)
        before = self.state()
        with self.assertRaises(ValueError):
            await self.apply(replace(self.command, observed_positions=(target,)))
        self.assertEqual(self.state(), before)

    async def test_journal_append_failure_does_not_expose_candidate_or_notifications(self):
        before = self.state()
        with patch.object(self.oms_journal, 'append', side_effect=JournalError('write failed')):
            with self.assertRaises(JournalError):
                await self.apply()
        self.assertEqual(self.state(), before)
        await self.apply()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)

    async def test_concurrent_retries_commit_once(self):
        import asyncio
        await asyncio.gather(self.apply(), self.apply())
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)
        records = [r for r in self.oms_journal.records if r.get('message', {}).get('type') == 'RepairExecutions']
        self.assertEqual(len(records), 1)

    async def test_replacement_cancel_is_not_completed_or_sent_by_repair(self):
        await self.context.replace_order(self.context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
        await self.runtime.drain()
        self.command = self.build()
        before = len(self.gateway.transactions)
        await self.apply()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(len(self.gateway.transactions), before)

    async def test_ordinary_redelivery_after_repair_is_deduplicated(self):
        await self.apply()
        before = self.runtime.ledger.positions()
        await self.runtime.orders.handle(self.command.executions[0])
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.positions(), before)

    async def test_closed_or_failed_source_cannot_authorize_a_new_repair(self):
        before = self.state()
        self.gateway._failed = True
        with self.assertRaises(JournalError):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def restart(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms_journal.close()
        self.gateway_journal.close()
        self.oms_journal = DurableJournal(self.root / 'oms')
        self.gateway_journal = DurableJournal(self.root / 'gateway')
        self.runtime = TradingRuntime(self.clock, journal=self.oms_journal, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.gateway_journal, self.clock)

    async def test_restart_replays_batch_and_gateway_redelivery_once_then_accepts_exact_retry(self):
        await self.apply()
        expected = self.runtime.ledger.positions()
        for _ in range(2):
            await self.restart()
            self.assertEqual(self.runtime.ledger.positions(), expected)
            self.assertEqual(self.context.orders()[0].commission, 1.25)
            self.assertFalse(self.runtime.orders.ready)
            self.assertEqual(self.gateway.prepared, ())
            before = self.state()
            await self.apply()
            self.assertEqual(self.state(), before)

    async def test_notification_failure_requires_restart_and_does_not_repeat_financial_effects(self):
        publish = self.runtime.bus.publish

        async def fail_notification(recipient, message):
            if isinstance(message, ExecutionApplied):
                raise RuntimeError('notification failed after commit')
            await publish(recipient, message)

        with patch.object(self.runtime.bus, 'publish', side_effect=fail_notification):
            with self.assertRaisesRegex(RuntimeError, 'after commit'):
                await self.apply()
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)
        before = self.state()
        with self.assertRaises(JournalError):
            await self.apply()
        self.assertEqual(self.state(), before)
        await self.restart()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).realized_pnl, -1.25)
        await self.apply()
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)

    async def test_failure_to_fsync_requires_restart_even_if_complete_record_reached_disk(self):
        before = self.runtime.ledger.positions()
        with patch('trading_system.persistence.os.fsync', side_effect=OSError('fsync failed')):
            with self.assertRaises(JournalError):
                await self.apply()
        self.assertEqual(self.runtime.ledger.positions(), before)
        self.assertFalse(self.oms_journal.writable)
        with self.assertRaises(JournalError):
            await self.apply()
        await self.restart()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 1)
        self.assertEqual(self.context.orders()[0].commission, 1.25)
        self.assertFalse(self.runtime.orders.ready)

    async def missing_second_fill(self):
        publish = self.runtime.bus.publish

        async def lose_execution(recipient, message):
            if not isinstance(message, Execution):
                await publish(recipient, message)

        # Lower ID deliberately arrives later: never sort the replay by trade ID.
        with patch.object(self.runtime.bus, 'publish', side_effect=lose_execution):
            await self.gateway.feed('OnTrade', trade_report(trade_num=9007199254740994, price=99))
        await self.runtime.drain()
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0],
            trade_num=9007199254740994, price=99)))
        raw = change_tables(raw, 'orders', lambda rows: rows[0].update(balance=2))
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=2))
        self.command = self.build(raw=raw)

    async def test_multiple_executions_are_committed_in_protocol_order_as_one_batch(self):
        await self.missing_second_fill()
        self.assertEqual([e.price for e in self.command.executions], [100, 99])
        before = self.oms_journal.record_count
        await self.apply()
        self.assertEqual(self.context.orders()[0].average_fill_price, 99.5)
        self.assertEqual(self.context.orders()[0].commission, 2.5)
        records = [r for r in self.oms_journal.records[before:] if r.get('message', {}).get('type') == 'RepairExecutions']
        self.assertEqual(len(records), 1)
        await self.restart()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 2)
        self.assertEqual(self.context.orders()[0].average_fill_price, 99.5)

    async def test_booked_prefix_is_preserved_when_only_the_later_fill_is_missing(self):
        await self.runtime.orders.handle(self.command.executions[0])
        await self.runtime.drain()
        await self.missing_second_fill()
        self.assertEqual(len(self.command.executions), 1)
        self.assertEqual(self.command.executions[0].price, 99)
        await self.apply()
        self.assertEqual(self.runtime.ledger.position('a', ROUTE).quantity, 2)
        self.assertEqual(self.context.orders()[0].average_fill_price, 99.5)
        self.assertEqual(self.context.orders()[0].commission, 2.5)

    async def test_failure_on_second_execution_leaves_no_partial_batch_or_notifications(self):
        await self.missing_second_fill()
        from trading_system.ledger import PositionLedger
        original = PositionLedger.apply

        def fail_second(ledger, owner, execution):
            if execution.price == 99:
                raise ValueError('second execution failed')
            original(ledger, owner, execution)

        before = self.state()
        with patch.object(PositionLedger, 'apply', new=fail_second), patch.object(self.runtime.bus, 'publish') as publish:
            with self.assertRaisesRegex(ValueError, 'second execution'):
                await self.apply()
            publish.assert_not_called()
        self.assertEqual(self.state(), before)

    async def test_reordered_or_partial_suffix_is_rejected_by_live_source(self):
        await self.missing_second_fill()
        for executions in (self.command.executions[::-1], self.command.executions[:1]):
            before = self.state()
            with self.assertRaises(ValueError):
                await self.apply(replace(self.command, executions=executions))
            self.assertEqual(self.state(), before)

    async def test_invalid_cumulative_target_rolls_back_execution(self):
        before = self.state()
        target = replace(self.command.order_targets[0], filled_quantity=2)
        with self.assertRaises(ValueError):
            await self.apply(replace(self.command, order_targets=(target,)))
        self.assertEqual(self.state(), before)

    async def test_active_plan_or_deficit_cannot_be_advanced_implicitly(self):
        for module, name in [('trading_system.plan_execution.PlanBook', 'active'),
                             ('trading_system.deficit_execution.DeficitBook', 'owns_route')]:
            before = self.state()
            with patch(module + '.' + name, return_value=True):
                with self.assertRaisesRegex(ValueError, 'separate recovery'):
                    await self.apply()
            self.assertEqual(self.state(), before)

    async def test_inventory_cutover_requires_separate_recovery(self):
        self.runtime.orders._inventory_gateways['quik'] = START
        before = self.state()
        with self.assertRaisesRegex(ValueError, 'cutovers'):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def test_missing_or_degraded_oms_journal_is_refused(self):
        before = self.state()
        with patch.object(self.runtime.orders, 'journal', None):
            with self.assertRaises(JournalError):
                await self.apply()
        with patch.object(self.oms_journal, 'degraded', True):
            with self.assertRaises(JournalError):
                await self.apply()
        self.assertEqual(self.state(), before)

    async def test_foreign_runtime_is_refused(self):
        async with TradingRuntime(self.clock) as other:
            before = self.state()
            with self.assertRaises(ValueError):
                await self.gateway.apply_execution_repair(self.command, other.orders)
            self.assertEqual(self.state(), before)

    async def test_degraded_closed_or_changed_gateway_journal_is_refused(self):
        before = self.state()
        with patch.object(self.gateway_journal, 'degraded', True):
            with self.assertRaises(JournalError):
                await self.apply()
        with patch.object(self.gateway, '_tail', -1):
            with self.assertRaises(JournalError):
                await self.apply()
        self.gateway_journal.close()
        with self.assertRaises(JournalError):
            await self.apply()
        self.assertEqual(self.state(), before)

    async def test_known_cancel_barrier_is_preserved_without_starting_replacement(self):
        await self.context.replace_order(self.context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
        await self.runtime.drain()
        await self.gateway.feed('OnOrder', order_report(1, balance=3, flags=10))
        await self.runtime.drain()
        raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].update(flags=10))
        self.command = self.build(raw=raw)
        before = len(self.gateway.transactions)
        await self.apply()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(self.context.orders()[0].filled_quantity, 1)
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(len(self.gateway.transactions), before)
        await self.restart()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(len(self.context.orders()), 1)

    async def test_operator_api_cannot_wait_inside_event_consumer(self):
        import asyncio
        error = asyncio.get_running_loop().create_future()

        async def consumer(message):
            try:
                await self.gateway.apply_execution_repair(self.command, self.runtime.orders)
            except RuntimeError as exc:
                error.set_result(str(exc))

        self.runtime.bus.subscribe('repair-test', consumer)
        before = self.state()
        await self.runtime.bus.publish('repair-test', GatewayUncertain('quik', 'test'))
        await self.runtime.drain()
        self.assertIn('consumer', error.result())
        self.assertEqual(self.state(), before)

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.inventory import MANUAL
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_durable import QuikDurableGateway
from trading_system.quik_inventory import build_external_inventory_plan
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.test_quik_inventory_plan import BASIS, ORIGIN, NOW, START, ROUTE, TRADE_ID, POLICY, snapshot
from tests_v2.test_quik_recovery_runtime import attach, capital
from tests_v2.test_quik_contract import SPEC


class ReconciliationCheckpointTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = ManualClock(NOW)
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)
        await capital(self.runtime)
        self.policy = replace(POLICY, session_id='psb-fixture')
        raw = snapshot()
        review = build_external_inventory_plan('review', raw, self.policy,
            self.runtime.orders.observe_gateway('quik'), self.gateway.order_evidence,
            position_basis=(BASIS,), order_origins=(ORIGIN,), now=self.clock.now(),
            current_revision=self.runtime.orders.revision)
        await self.gateway.stage_external_inventory(review, self.runtime.orders, raw, self.policy)
        adoption = self.gateway.inventory_adoption_command('adopt', self.runtime.orders, 'review')
        await self.gateway.adopt_external_inventory(adoption, self.runtime.orders, raw, self.policy)
        await self.runtime.drain()
        self.clock.advance_to(START+timedelta(seconds=7))
        self.raw = snapshot()
        self.raw.update(started_at=(START+timedelta(seconds=5)).isoformat(),
                        finished_at=(START+timedelta(seconds=6)).isoformat())

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()

    def build(self, operation_id='check', raw=None, policy=None):
        return self.gateway.reconciliation_checkpoint(operation_id, self.runtime.orders,
            self.raw if raw is None else raw, self.policy if policy is None else policy)

    async def record(self, checkpoint=None, raw=None, policy=None):
        checkpoint = self.build() if checkpoint is None else checkpoint
        await self.gateway.record_reconciliation(checkpoint, self.runtime.orders,
            self.raw if raw is None else raw, self.policy if policy is None else policy)
        return checkpoint

    def status(self, operation_id='check'):
        return self.gateway.reconciliation_status(operation_id, self.runtime.orders)

    async def restart(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'wire')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)

    def finances(self):
        return (self.runtime.ledger.positions(), self.runtime.orders.orders(MANUAL),
                self.runtime.orders.revision, self.oms.record_count, self.gateway.transactions)

    async def test_checkpoint_records_consistency_without_financial_changes_or_admission(self):
        before = self.finances()
        checkpoint = await self.record()
        self.assertEqual(codec.decode(codec.encode(checkpoint)), checkpoint)
        self.assertEqual(self.gateway.reconciliation_checkpoints, (checkpoint,))
        self.assertEqual(self.finances(), before)
        status = self.status()
        self.assertTrue(status.current)
        self.assertEqual(status.invalidated_by, ())
        self.assertIn('history_coverage_unverified', status.blockers)
        self.assertIn('gateway_cutover_unverified', status.blockers)
        self.assertFalse(status.reconciled)
        self.assertFalse(status.eligible_for_trading)
        self.assertFalse(status.eligible_for_capital)
        self.assertFalse(self.runtime.orders.ready)
        self.assertTrue(self.gateway.blocked)

    async def test_original_inventory_observation_is_not_a_post_adoption_check(self):
        with self.assertRaisesRegex(ValueError, 'predates'):
            self.build(raw=snapshot())

    async def test_repeat_is_idempotent_and_conflicting_id_is_refused(self):
        checkpoint = self.build()
        await asyncio.gather(self.record(checkpoint), self.record(checkpoint))
        before = self.wire.record_count
        await self.record(checkpoint)
        self.assertEqual(self.wire.record_count, before)
        with self.assertRaises(ValueError):
            await self.record(replace(checkpoint, snapshot_digest='b'*64))

    async def test_duplicate_callback_invalidates_even_without_new_financial_effect(self):
        await self.record()
        before = self.runtime.ledger.positions()
        await self.gateway.feed('OnTrade', self.raw['tables']['trades'][0])
        await self.runtime.drain()
        self.assertEqual(self.runtime.ledger.positions(), before)
        self.assertFalse(self.status().current)
        self.assertIn('gateway_changed', self.status().invalidated_by)

    async def test_disconnect_invalidates_and_reconnect_does_not_restore_checkpoint(self):
        await self.record()
        await self.gateway.feed('OnDisconnected', {})
        await self.gateway.feed('OnConnected', {})
        await self.runtime.drain()
        self.assertFalse(self.status().current)
        self.assertFalse(self.runtime.orders.ready)

    async def test_expiry_and_oms_change_invalidate(self):
        await self.record()
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2,
            as_of=self.clock.now()))
        self.assertIn('oms_changed', self.status().invalidated_by)
        self.clock.advance_to(START+timedelta(minutes=1))
        self.assertIn('expired', self.status().invalidated_by)

    async def test_restart_preserves_historical_receipt_but_not_current_status(self):
        checkpoint = await self.record()
        await self.restart()
        self.assertEqual(self.gateway.reconciliation_checkpoints, (checkpoint,))
        self.assertFalse(self.status().current)
        before = self.wire.record_count
        await self.record(checkpoint)
        self.assertEqual(self.wire.record_count, before)
        self.assertFalse(self.status().current)

    async def test_changed_source_during_review_cannot_be_recorded(self):
        checkpoint = self.build()
        await self.gateway.feed('OnConnected', {})
        with self.assertRaises(ValueError):
            await self.record(checkpoint)
        self.assertEqual(self.gateway.reconciliation_checkpoints, ())

    async def test_changed_oms_during_review_cannot_be_recorded(self):
        checkpoint = self.build()
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2,
            as_of=self.clock.now()))
        with self.assertRaises(ValueError):
            await self.record(checkpoint)

    async def test_changed_snapshot_is_refused_even_if_still_consistent(self):
        checkpoint = self.build()
        raw = deepcopy(self.raw)
        raw['connection_id'] = 'c'*32
        with self.assertRaises(ValueError):
            await self.record(checkpoint, raw=raw)

    async def test_new_fill_requires_matching_fresh_snapshot(self):
        await self.record()
        trade = deepcopy(self.raw['tables']['trades'][0])
        trade.update(trade_num=TRADE_ID+1, broker_comission='0.75')
        trade['datetime']['sec'] = 7
        await self.gateway.feed('OnTrade', trade)
        await self.runtime.drain()
        with self.assertRaises(ValueError):
            self.build('after-fill')
        self.clock.advance_to(START+timedelta(seconds=10))
        self.raw.update(started_at=(START+timedelta(seconds=8)).isoformat(),
                        finished_at=(START+timedelta(seconds=9)).isoformat())
        self.raw['tables']['orders'][0]['balance'] = 2
        self.raw['tables']['holdings'][0]['totalnet'] = 2
        self.raw['tables']['trades'].append(trade)
        self.raw['previous_tables'] = deepcopy(self.raw['tables'])
        checkpoint = await self.record(self.build('after-fill'))
        self.assertTrue(self.status('after-fill').current)
        self.assertFalse(self.status().current)
        self.assertEqual(checkpoint.execution_count, 2)
        position = self.runtime.ledger.position(MANUAL, ROUTE)
        self.assertEqual((position.quantity, position.average_price, position.realized_pnl), (2, 97.5, -.75))

    async def test_missing_history_or_net_mismatch_cannot_get_checkpoint(self):
        for table, change in (('trades', lambda rows: rows.clear()),
                              ('holdings', lambda rows: rows[0].update(totalnet=2))):
            raw = deepcopy(self.raw)
            change(raw['tables'][table])
            raw['previous_tables'] = deepcopy(raw['tables'])
            with self.assertRaises(ValueError):
                self.build(raw=raw)

    async def test_scope_and_callback_contract_must_match_all_configured_routes(self):
        for changes in (dict(session_id='other'), dict(timezone_offset=timedelta()),
                        dict(routes=(replace(self.policy.routes[0], commission_fields=()),))):
            with self.assertRaises(ValueError):
                self.build(policy=replace(self.policy, **changes))

    async def test_missing_adoption_in_either_journal_is_refused(self):
        with patch.object(self.runtime.orders, '_inventory_operations', {}):
            with self.assertRaises(JournalError):
                self.build()
        with patch.object(self.gateway._engine, 'inventory_adoptions', {}):
            with self.assertRaises(ValueError):
                self.build()

    async def test_missing_oms_execution_is_not_hidden_by_matching_net(self):
        with patch.object(self.runtime.orders, '_executions', {}):
            with self.assertRaises(ValueError):
                self.build()

    async def test_unmatched_callback_prevents_checkpoint(self):
        raw = deepcopy(self.raw['tables']['orders'][0])
        raw['order_num'] += 7
        await self.gateway.feed('OnOrder', raw)
        await self.runtime.drain()
        self.clock.advance_to(START+timedelta(seconds=10))
        self.raw.update(started_at=(START+timedelta(seconds=8)).isoformat(),
                        finished_at=(START+timedelta(seconds=9)).isoformat())
        with self.assertRaisesRegex(ValueError, 'unmatched'):
            self.build()

    async def test_write_failure_exposes_no_current_checkpoint(self):
        checkpoint = self.build()
        with patch.object(self.wire, 'append', side_effect=JournalError('write failed')):
            with self.assertRaises(JournalError):
                await self.record(checkpoint)
        self.assertEqual(self.gateway.reconciliation_checkpoints, ())
        self.assertTrue(self.gateway.blocked)

    async def test_incomplete_input_result_pair_finishes_on_restart_without_admission(self):
        checkpoint = self.build()
        append = self.gateway._append
        def fail_result(record):
            if record['kind'] == 'result':
                raise JournalError('crash before result')
            append(record)
        with patch.object(self.gateway, '_append', side_effect=fail_result):
            with self.assertRaises(JournalError):
                await self.record(checkpoint)
        await self.runtime.drain()
        await self.restart()
        self.assertEqual(self.gateway.reconciliation_checkpoints, (checkpoint,))
        self.assertFalse(self.status().current)
        self.assertFalse(self.runtime.orders.ready)

    async def test_storage_or_delivery_failure_invalidates_status_and_blocks_new_checks(self):
        await self.record()
        for target, name in ((self.wire, 'degraded'), (self.oms, 'degraded'),
                             (self.gateway, '_failed'), (self.runtime.orders, '_failed_after_commit')):
            with patch.object(target, name, True):
                self.assertFalse(self.status().current)
                with self.assertRaises(JournalError):
                    self.build('new')

    async def test_unrecorded_or_unknown_checkpoint_has_no_current_status(self):
        self.build()
        with self.assertRaises(KeyError):
            self.status()

    async def test_strategy_journal_change_invalidates_even_without_oms_revision_change(self):
        await self.record()
        revision = self.runtime.orders.revision
        # Lifecycle events share the OMS journal but have their own state/revision.
        strategy_record = deepcopy(self.oms.records[0])
        strategy_record['time'] = self.clock.now().isoformat()
        self.oms.append(strategy_record)
        self.assertEqual(self.runtime.orders.revision, revision)
        self.assertIn('oms_changed', self.status().invalidated_by)

    async def test_scope_cannot_omit_an_unused_configured_route(self):
        extra = replace(SPEC, route=replace(ROUTE, account_id='other-demo'))
        with patch.object(self.gateway._engine, 'routes', (SPEC, extra)):
            with self.assertRaisesRegex(ValueError, 'complete callback contract'):
                self.build()

    async def test_tampered_proposal_and_changed_policy_are_refused(self):
        checkpoint = self.build()
        for changes in (dict(order_count=0), dict(execution_count=0), dict(oms_journal_digest='a'*64),
                        dict(gateway_journal_digest='b'*64), dict(source_digest='c'*64),
                        dict(adoption_id='unknown'), dict(policy_digest='d'*64)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await self.record(replace(checkpoint, **changes))
        with self.assertRaises(ValueError):
            await self.record(checkpoint, policy=replace(self.policy, max_age=timedelta(minutes=1)))
        self.assertEqual(self.gateway.reconciliation_checkpoints, ())

    async def test_expired_proposal_cannot_be_saved_and_clock_regression_invalidates(self):
        checkpoint = self.build()
        self.clock.advance_to(checkpoint.expires_at + timedelta(microseconds=1))
        with self.assertRaises(ValueError):
            await self.record(checkpoint)
        self.assertEqual(self.gateway.reconciliation_checkpoints, ())
        # Use a new observation after the old proposal expired.
        self.raw.update(started_at=self.clock.now().isoformat(), finished_at=self.clock.now().isoformat())
        await self.record()
        with patch.object(self.clock, 'now', return_value=START):
            self.assertIn('clock_regressed', self.status().invalidated_by)

    async def test_expiry_crossing_after_locked_validation_keeps_replayable_input(self):
        checkpoint = self.build()
        self.clock.advance_to(checkpoint.expires_at)
        commit = self.gateway._commit
        async def crossing(payload, **kwargs):
            self.clock.advance_to(checkpoint.expires_at + timedelta(microseconds=1))
            await commit(payload, **kwargs)
        with patch.object(self.gateway, '_commit', side_effect=crossing):
            await self.record(checkpoint)
        self.assertIn('expired', self.status().invalidated_by)
        await self.restart()
        self.assertEqual(self.gateway.reconciliation_checkpoints, (checkpoint,))

    async def test_fsync_uncertainty_requires_restart_and_does_not_renew_receipt(self):
        checkpoint = self.build()
        with patch('trading_system.persistence.os.fsync', side_effect=OSError('uncertain fsync')):
            with self.assertRaises(JournalError):
                await self.record(checkpoint)
        self.assertEqual(self.gateway.reconciliation_checkpoints, ())
        await self.runtime.drain()
        await self.restart()
        self.assertEqual(self.gateway.reconciliation_checkpoints, (checkpoint,))
        self.assertFalse(self.status().current)

    async def test_replay_checks_receipt_source_against_preceding_protocol_state(self):
        await self.record()
        with DurableJournal(self.root/'tampered-wire') as journal:
            for original in self.wire.records:
                record = deepcopy(original)
                if record.get('payload', {}).get('op') == 'checkpoint':
                    record['payload']['checkpoint']['fields']['source_digest'] = 'b'*64
                journal.append(record)
            with self.assertRaisesRegex(JournalError, 'committed gateway source'):
                await QuikDurableGateway.open('quik', (SPEC,), self.clock, journal=journal,
                    session_id='psb-fixture', timezone_offset=timedelta(hours=3))

    async def test_wrong_oms_prefix_is_detected_before_financial_redelivery(self):
        checkpoint = await self.record()
        with DurableJournal(self.root/'other-oms') as other:
            for original in self.oms.records:
                record = deepcopy(original)
                if record.get('kind') == 'strategy':
                    record['snapshot']['parameters']['marker'] = 'different-history'
                other.append(record)
            with patch.object(self.runtime.orders, 'journal', other):
                self.assertIn('journal_mismatch', self.status().invalidated_by)
                with self.assertRaises(JournalError):
                    await self.record(checkpoint)
                with patch.object(self.gateway, '_started', False):
                    with self.assertRaisesRegex(JournalError, 'checkpoint prefixes'):
                        await self.gateway.recover(self.runtime.orders)

    async def test_bad_transport_boundary_and_changed_trading_date_cannot_get_checkpoint(self):
        for change in (lambda r: r['callback_boundary'].update(delivery_verified=False),
                       lambda r: r.update(trading_date_after='02.01.2026'),
                       lambda r: r['previous_tables']['trades'].clear()):
            raw = deepcopy(self.raw)
            change(raw)
            with self.assertRaises(ValueError):
                self.build(raw=raw)

    async def test_new_check_after_restart_requires_a_new_observation(self):
        await self.record()
        await self.restart()
        with self.assertRaisesRegex(ValueError, 'predates'):
            self.build('new')
        self.clock.advance_to(START+timedelta(seconds=10))
        self.raw.update(started_at=(START+timedelta(seconds=8)).isoformat(),
                        finished_at=(START+timedelta(seconds=9)).isoformat())
        await self.record(self.build('new'))
        self.assertTrue(self.status('new').current)
        self.assertFalse(self.status().current)
        self.assertFalse(self.status('new').eligible_for_trading)

    async def test_rejected_old_fill_cannot_be_hidden_by_fresh_snapshot_or_restart(self):
        trade = deepcopy(self.raw['tables']['trades'][0])
        trade['trade_num'] += 1  # Unknown execution before the adopted financial boundary.
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', trade)
        await self.runtime.drain()
        self.clock.advance_to(START+timedelta(seconds=10))
        self.raw.update(started_at=(START+timedelta(seconds=8)).isoformat(),
                        finished_at=(START+timedelta(seconds=9)).isoformat())
        with self.assertRaisesRegex(ValueError, 'protocol input errors'):
            self.build()

        await self.restart()
        self.clock.advance_to(START+timedelta(seconds=13))
        self.raw.update(started_at=(START+timedelta(seconds=11)).isoformat(),
                        finished_at=(START+timedelta(seconds=12)).isoformat())
        with self.assertRaisesRegex(ValueError, 'protocol input errors'):
            self.build()

    async def test_input_rejected_before_parsing_also_blocks_new_checkpoint(self):
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnTrade', {'not_json': object()})
        await self.runtime.drain()
        self.clock.advance_to(START+timedelta(seconds=10))
        self.raw.update(started_at=(START+timedelta(seconds=8)).isoformat(),
                        finished_at=(START+timedelta(seconds=9)).isoformat())
        with self.assertRaisesRegex(ValueError, 'protocol input errors'):
            self.build()
        await self.restart()
        self.clock.advance_to(START+timedelta(seconds=13))
        self.raw.update(started_at=(START+timedelta(seconds=11)).isoformat(),
                        finished_at=(START+timedelta(seconds=12)).isoformat())
        with self.assertRaisesRegex(ValueError, 'protocol input errors'):
            self.build()

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import GatewayRejected, OrderStatus, Position
from trading_system.inventory import (MANUAL, InventoryProvenance, ExternalPositionBasis, ExternalOrderOrigin,
                                      OpeningBalance, OmsObservation, InventoryReport, StageExternalInventory)
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_compare import QuikBindings
from trading_system.quik_inventory import build_external_inventory_plan
from trading_system.quik_outcomes import OrderEvidence
from trading_system.quik_repair import ExecutionEvidence
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.test_quik_compare import (POLICY, START, NOW, ROUTE, BROKER_ID, TRADE_ID,
                                        OMS, LINKS, FILL, snapshot, change_tables)
from tests_v2.test_quik_recovery_runtime import attach, capital

END = START + timedelta(seconds=1)
PROVENANCE = InventoryProvenance(MANUAL, END, 'synthetic-statement', 'a'*64)
BASIS = ExternalPositionBasis(OpeningBalance(ROUTE, 1, 95), PROVENANCE)
ORIGIN = ExternalOrderOrigin(ROUTE, BROKER_ID, START, PROVENANCE)
EMPTY = OmsObservation(0, 'quik', NOW, (), (), (), ())
EVIDENCE = OrderEvidence(ExecutionEvidence(QuikBindings('quik', 'fixture', 0, (), ()), ()), ())


def plan(raw=None, *, view=EMPTY, evidence=EVIDENCE, basis=(BASIS,), origins=(ORIGIN,), policy=POLICY, now=NOW):
    return build_external_inventory_plan('inventory-review', raw or snapshot(), policy, view, evidence,
        position_basis=basis, order_origins=origins, now=now, current_revision=view.revision)


class InventoryPlanningTests(TestCase):
    def test_partial_external_order_has_stable_ids_history_and_explicit_residual_basis(self):
        raw = snapshot()
        before = deepcopy(raw)
        result = plan(raw)
        external = result.imports[0]
        self.assertEqual(external.order.strategy_id, MANUAL)
        self.assertEqual(external.order.order_id, f'fixture:external-order:{BROKER_ID}')
        self.assertEqual(external.executions[0].execution_id, f'fixture:{TRADE_ID}')
        self.assertEqual(external.order.average_fill_price, 100)
        self.assertEqual(result.position_basis[0].opening.average_price, 95)
        self.assertEqual(external.order.commission, 1.25)
        self.assertFalse(result.can_apply)
        self.assertFalse(result.reconciled)
        self.assertFalse(result.eligible_for_capital)
        self.assertEqual(codec.decode(codec.encode(result)), result)
        self.assertEqual(raw, before)
        self.assertEqual(EMPTY.positions, ())

    def test_missing_cost_basis_is_never_inferred_from_trade_or_market_price(self):
        with self.assertRaises(ValueError):
            plan(basis=())

    def test_missing_or_extra_order_origin_is_rejected(self):
        for origins in ((), (ORIGIN, replace(ORIGIN, broker_order_id=123))):
            with self.subTest(origins=origins), self.assertRaises(ValueError):
                plan(origins=origins)

    def test_provenance_requires_manual_owner_digest_and_exact_observation_time(self):
        for changes in (dict(owner_id='alpha'), dict(reference=''), dict(digest='bad')):
            with self.assertRaises(ValueError):
                replace(PROVENANCE, **changes)
        with self.assertRaises(ValueError):
            plan(basis=(replace(BASIS, provenance=replace(PROVENANCE, as_of=START)),))

    def test_wrong_residual_quantity_and_duplicate_basis_are_rejected(self):
        for basis in ((replace(BASIS, opening=OpeningBalance(ROUTE, 2, 95)),), (BASIS, BASIS)):
            with self.assertRaises(ValueError):
                plan(basis=basis)

    def test_history_gap_unknown_fee_and_unowned_trade_are_rejected(self):
        variants = [change_tables(snapshot(), 'trades', lambda rows: rows.clear()),
                    change_tables(snapshot(), 'trades', lambda rows: rows[0].pop('broker_comission')),
                    change_tables(snapshot(), 'trades', lambda rows: rows[0].update(order_num=123))]
        for raw in variants:
            with self.assertRaises(ValueError):
                plan(raw)

    def test_submitted_after_trade_or_observation_is_rejected(self):
        for submitted in (END, NOW):
            with self.assertRaises(ValueError):
                plan(origins=(replace(ORIGIN, submitted_at=submitted),))

    def test_identical_trade_duplicates_are_folded_and_conflicts_refused(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0])))
        self.assertEqual(len(plan(raw).imports[0].executions), 1)
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0], price=99)))
        with self.assertRaises(ValueError):
            plan(raw)

    def test_carry_position_without_orders_needs_only_its_own_basis(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows.clear())
        raw = change_tables(raw, 'trades', lambda rows: rows.clear())
        result = plan(raw, origins=())
        self.assertEqual(result.imports, ())
        self.assertEqual(result.position_basis, (BASIS,))

    def test_unfilled_external_order_needs_no_opening_or_synthetic_fee(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].update(balance=4))
        raw = change_tables(raw, 'trades', lambda rows: rows.clear())
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=0))
        result = plan(raw, basis=())
        self.assertEqual(result.imports[0].executions, ())
        self.assertEqual(result.imports[0].order.commission, 0)

    def test_short_residual_preserves_sign_and_explicit_basis(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].update(flags=13))
        raw = change_tables(raw, 'trades', lambda rows: rows[0].update(flags=4))
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=-1))
        basis = replace(BASIS, opening=OpeningBalance(ROUTE, -1, 110))
        self.assertEqual(plan(raw, basis=(basis,)).position_basis, (basis,))

    def test_net_residual_preserves_opposing_known_strategy_positions(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows.append(dict(rows[0], order_num=123, qty=3, balance=0, flags=8)))
        raw = change_tables(raw, 'trades', lambda rows: rows.append(dict(rows[0], trade_num=456, order_num=123, qty=3)))
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=4))
        view = replace(OMS, positions=(('alpha', Position(ROUTE, 3, 90)), ('beta', Position(ROUTE, -2, 120))))
        basis = replace(BASIS, opening=OpeningBalance(ROUTE, 3, 70))
        evidence = OrderEvidence(ExecutionEvidence(LINKS, (FILL,)), ())
        result = plan(raw, view=view, evidence=evidence, basis=(basis,), origins=(replace(ORIGIN, broker_order_id=123),))
        self.assertEqual(result.position_basis, (basis,))
        self.assertEqual(len(result.imports), 1)
        self.assertEqual(view.positions[0][1].quantity, 3)
        self.assertEqual(view.positions[1][1].quantity, -2)

    def test_missing_known_fill_cannot_be_reclassified_as_external_position(self):
        view = replace(OMS, executions=(), positions=(), orders=(replace(OMS.orders[0], filled_quantity=0,
            average_fill_price=0, commission=0, status=OrderStatus.ACCEPTED),))
        with self.assertRaises(ValueError):
            plan(view=view, evidence=OrderEvidence(ExecutionEvidence(LINKS, (FILL,)), ()))

    def test_opposing_manual_residual_cannot_hide_a_close(self):
        view = replace(EMPTY, positions=((MANUAL, Position(ROUTE, 2, 100)),))
        with self.assertRaises(ValueError):
            plan(view=view, basis=(replace(BASIS, opening=OpeningBalance(ROUTE, -1, 95)),))

    def test_empty_plan_stale_snapshot_and_foreign_session_are_refused(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows.clear())
        raw = change_tables(raw, 'trades', lambda rows: rows.clear())
        raw = change_tables(raw, 'holdings', lambda rows: rows[0].update(totalnet=0))
        with self.assertRaises(ValueError):
            plan(raw, basis=(), origins=())
        with self.assertRaises(ValueError):
            plan(now=NOW+timedelta(minutes=1))
        with self.assertRaises(ValueError):
            plan(policy=replace(POLICY, session_id='wrong'))

    def test_known_rejection_requires_positive_saved_evidence(self):
        rejected = replace(OMS.orders[0], status=OrderStatus.REJECTED, filled_quantity=0,
                           average_fill_price=0, commission=0, rejection_reason='broker rejection')
        view = replace(EMPTY, orders=(rejected,))
        with self.assertRaises(ValueError):
            plan(view=view)
        evidence = replace(EVIDENCE, outcomes=(GatewayRejected(rejected.order_id, 'quik', 'broker rejection'),))
        self.assertEqual(len(plan(view=view, evidence=evidence).imports), 1)

    def test_proposed_local_order_identifier_cannot_collide_with_rejected_history(self):
        local_id = f'fixture:external-order:{BROKER_ID}'
        rejected = replace(OMS.orders[0], order_id=local_id, status=OrderStatus.REJECTED,
                           filled_quantity=0, average_fill_price=0, commission=0, rejection_reason='rejected')
        view = replace(EMPTY, orders=(rejected,))
        evidence = replace(EVIDENCE, outcomes=(GatewayRejected(local_id, 'quik', 'rejected'),))
        with self.assertRaisesRegex(ValueError, 'ID is already in use'):
            plan(view=view, evidence=evidence)

    def test_untracked_fill_of_bound_order_cannot_be_imported(self):
        view = replace(OMS, executions=(), positions=())
        evidence = OrderEvidence(ExecutionEvidence(replace(LINKS, executions=()), ()), ())
        with self.assertRaisesRegex(ValueError, 'known order'):
            plan(view=view, evidence=evidence, origins=())

    def test_broker_identifiers_must_be_globally_unique_across_routes(self):
        route = replace(ROUTE, instrument=replace(ROUTE.instrument, id=replace(ROUTE.instrument.id, symbol='OTHER')))
        policy = replace(POLICY, routes=POLICY.routes+(replace(POLICY.routes[0], route=route),))
        raw = snapshot()
        for table in ('orders', 'trades', 'holdings'):
            raw = change_tables(raw, table, lambda rows: rows.append(dict(rows[0], sec_code='OTHER')))
        with self.assertRaisesRegex(ValueError, 'unique within the gateway session'):
            plan(raw, policy=policy)

    def test_commission_precision_is_not_silently_lost(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows[0].update(broker_comission='1.250000000000000000001'))
        with self.assertRaisesRegex(ValueError, 'round-trip'):
            plan(raw)

    def test_malformed_review_shape_and_digest_are_rejected(self):
        command = plan()
        for changes in (dict(imports=list(command.imports)), dict(position_basis=(BASIS, BASIS)),
                        dict(order_origins=(ORIGIN, ORIGIN)), dict(gateway_id='wrong'),
                        dict(source_digest='bad'), dict(expires_at=END), dict(expected_revision=True)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(command, **changes)


class InventoryStagingTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = ManualClock(START)
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'quik')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)
        await capital(self.runtime)
        self.clock.advance_to(NOW)
        self.policy = replace(POLICY, session_id='psb-fixture')
        self.raw = snapshot()
        self.command = build_external_inventory_plan('inventory-review', self.raw, self.policy,
            self.runtime.orders.observe_gateway('quik'), self.gateway.order_evidence,
            position_basis=(BASIS,), order_origins=(ORIGIN,), now=NOW, current_revision=self.runtime.orders.revision)

    async def asyncTearDown(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()

    def state(self):
        return (self.runtime.orders.revision, self.runtime.ledger.positions(), self.runtime.orders.orders(MANUAL),
                self.runtime.orders.capital_usage(), self.oms.record_count, self.wire.record_count, self.gateway.transactions)

    async def stage(self, command=None, raw=None):
        await self.gateway.stage_external_inventory(command or self.command, self.runtime.orders,
            raw or self.raw, self.policy)
        await self.runtime.drain()

    async def restart(self):
        await self.runtime.__aexit__(None, None, None)
        self.oms.close()
        self.wire.close()
        self.oms = DurableJournal(self.root/'oms')
        self.wire = DurableJournal(self.root/'quik')
        self.runtime = TradingRuntime(self.clock, journal=self.oms, require_capital=True)
        await self.runtime.__aenter__()
        self.gateway, self.context = await attach(self.runtime, self.wire, self.clock)

    async def test_stage_is_durable_review_and_blocks_trading_without_financial_import(self):
        before = self.state()
        self.assertTrue(self.runtime.orders.ready)
        await self.stage()
        self.assertEqual(self.runtime.orders.external_inventory_review('inventory-review'), self.command)
        self.assertEqual(self.runtime.ledger.positions(), before[1])
        self.assertEqual(self.runtime.orders.orders(MANUAL), ())
        self.assertEqual(self.gateway.transactions, ())
        self.assertEqual(self.runtime.orders.capital_usage(), before[3])
        self.assertFalse(self.runtime.orders.ready)
        records = [r for r in self.oms.records if r.get('message',{}).get('type')=='StageExternalInventory']
        self.assertEqual(len(records),1)

    async def test_ordinary_order_reconciliation_and_fresh_capital_do_not_clear_pending_review(self):
        await self.stage()
        await self.runtime.configure_capital(replace(self.runtime.orders.capital_policy, revision=2, as_of=NOW))
        await self.runtime.orders.reconcile_gateway('quik', ())
        self.assertFalse(self.runtime.orders.ready)
        report = InventoryReport('bypass', self.runtime.orders.revision, 'quik', NOW, (), (), (), ())
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(report)
        self.assertFalse(self.runtime.orders.ready)

    async def test_concurrent_repeat_and_restart_preserve_one_review_without_importing(self):
        await asyncio.gather(self.stage(), self.stage())
        for _ in range(2):
            await self.restart()
            before = self.state()
            await self.stage()
            self.assertEqual(self.state(), before)
            self.assertEqual(self.runtime.orders.external_inventory_review('inventory-review'), self.command)
            self.assertEqual(self.runtime.ledger.positions(), ())
            self.assertFalse(self.runtime.orders.ready)

    async def test_stale_oms_and_gateway_revision_are_refused(self):
        for obj, field, value in ((self.gateway, '_number', self.gateway._number+1),
                                  (self.runtime.orders, '_revision', self.runtime.orders.revision+1)):
            before = self.state()
            with patch.object(obj, field, value):
                with self.assertRaises(ValueError):
                    await self.stage()
            self.assertEqual(self.state(), before)

    async def test_snapshot_or_proposed_history_tampering_is_refused(self):
        changed = change_tables(deepcopy(self.raw), 'holdings', lambda rows: rows[0].update(totalnet=2))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.stage(raw=changed)
        item = self.command.imports[0]
        altered = replace(item, order=replace(item.order, average_fill_price=99),
                          executions=(replace(item.executions[0], price=99),))
        with self.assertRaises(ValueError):
            await self.stage(replace(self.command, imports=(altered,)))
        self.assertEqual(self.state(), before)

    async def test_direct_oms_command_is_refused(self):
        with self.assertRaises(ValueError):
            await self.runtime.orders.handle(self.command)

    async def test_append_failure_leaves_no_review_or_block_or_notifications(self):
        before = self.state()
        with patch.object(self.oms, 'append', side_effect=JournalError('write failed')):
            with patch.object(self.runtime.bus, 'publish') as publish:
                with self.assertRaises(JournalError):
                    await self.stage()
                publish.assert_not_called()
        self.assertEqual(self.state(), before)
        self.assertTrue(self.runtime.orders.ready)
        with self.assertRaises(KeyError):
            self.runtime.orders.external_inventory_review('inventory-review')

    async def test_expired_new_plan_and_conflicting_repeat_are_refused(self):
        await self.stage()
        with self.assertRaises(ValueError):
            await self.stage(replace(self.command, snapshot_digest='b'*64))
        self.clock.advance_to(NOW+timedelta(minutes=1))
        before = self.state()
        await self.stage()
        self.assertEqual(self.state(), before)
        with self.assertRaises(ValueError):
            await self.stage(replace(self.command, operation_id='new-review', expected_revision=self.runtime.orders.revision))

    async def test_expiry_between_build_and_stage_does_not_save_a_review(self):
        self.clock.advance_to(NOW+timedelta(minutes=1))
        before = self.state()
        with self.assertRaises(ValueError):
            await self.stage()
        self.assertEqual(self.state(), before)

    async def test_failed_source_or_missing_healthy_journal_cannot_stage(self):
        before = self.state()
        for obj, field, value in ((self.gateway, '_failed', True), (self.gateway, '_tail', -1),
                                  (self.wire, 'degraded', True), (self.oms, 'degraded', True),
                                  (self.runtime.orders, 'journal', None)):
            with patch.object(obj, field, value):
                with self.assertRaises(JournalError):
                    await self.stage()
            self.assertEqual(self.state(), before)

    async def test_fsync_failure_requires_restart_and_recovers_pending_review_without_import(self):
        before = self.state()
        with patch('trading_system.persistence.os.fsync', side_effect=OSError('fsync failed')):
            with self.assertRaises(JournalError):
                await self.stage()
        self.assertEqual(self.runtime.ledger.positions(), before[1])
        with self.assertRaises(KeyError):
            self.runtime.orders.external_inventory_review('inventory-review')
        self.assertFalse(self.runtime.orders.ready)
        with self.assertRaises(JournalError):
            await self.stage()
        await self.restart()
        await self.stage()
        self.assertEqual(self.runtime.orders.external_inventory_review('inventory-review'), self.command)
        self.assertEqual(self.runtime.ledger.positions(), before[1])
        self.assertFalse(self.runtime.orders.ready)

    async def test_staging_does_not_advance_plans_or_deficit_recovery(self):
        with patch('trading_system.plan_execution.PlanBook.advance') as plans:
            with patch('trading_system.deficit_execution.DeficitBook.advance') as deficits:
                await self.stage()
                plans.assert_not_called()
                deficits.assert_not_called()

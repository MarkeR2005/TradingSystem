from pathlib import Path
from tempfile import TemporaryDirectory
from trading_system.domain import GatewayUncertain
from trading_system.inventory import OpeningBalance
from trading_system.persistence import DurableJournal
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.test_quik_recovery_runtime import attach, capital
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase, TestCase

from trading_system.domain import Execution, OrderIntent, OrderSnapshot, OrderStatus, Position, Side
from trading_system.inventory import OmsObservation
from trading_system.quik_compare import (BrokerLink, ComparisonPolicy, ObservedRoute, QuikBindings,
                                         compare_quik, normalize_quik_snapshot)
from tests_v2.helpers import START
from tests_v2.test_quik_contract import ROUTE, BROKER_ID, order_report, trade_report

NOW = START + timedelta(seconds=2)
SPEC = ObservedRoute('DEMO', ROUTE, 2, Decimal('0.5'), ('broker_comission',))
POLICY = ComparisonPolicy('demo-quik', '13.2.0.31', 'quik', 'fixture', START.date(),
                          timedelta(hours=3), timedelta(seconds=30), (SPEC,))
TRADE_ID = 9007199254740995
ORDER = OrderSnapshot('local-order', 'alpha', OrderIntent(ROUTE, Side.BUY, 4, 100), START,
                      OrderStatus.PARTIALLY_FILLED, 1, 100, 1.25)
FILL = Execution('fixture:' + str(TRADE_ID), ORDER.order_id, ROUTE, Side.BUY, 1, 100, 1.25, START)
OMS = OmsObservation(7, 'quik', NOW, (ORDER,), (FILL,), (('alpha', Position(ROUTE, 1, 100)),), ())
LINKS = QuikBindings('quik', 'fixture', 4, (BrokerLink(ROUTE, BROKER_ID, ORDER.order_id),),
                     (BrokerLink(ROUTE, TRADE_ID, FILL.execution_id),))


def snapshot():
    cursor = dict(bridge_session='a' * 32, sequence=5, failed_sends=0)
    tables = dict(accounts=[dict(firmid='DEMO', trdaccid=ROUTE.account_id, class_codes='|SPBFUT|')],
        orders=[dict(order_report(1, balance=3), firmid='DEMO')],
        trades=[dict(trade_report(), firmid='DEMO')],
        holdings=[dict(firmid='DEMO', trdaccid=ROUTE.account_id, sec_code='TEST', type=2, totalnet=1)], limits=[])
    return dict(schema=1, mode='read_only', runtime=dict(protocol=1, mode='read_only', profile='demo-quik',
        quik_version='13.2.0.31', lua_version='Lua 5.4', callback_cursor=cursor), connection_id='b' * 32,
        started_at=START.isoformat(), finished_at=(START + timedelta(seconds=1)).isoformat(),
        trading_date_before='01.01.2026', trading_date_after='01.01.2026',
        tables=tables, previous_tables=deepcopy(tables), issues=[], repeatable_observation=True,
        reconciled=False, callbacks_before=0, callbacks_after=0,
        callback_boundary=dict(schema=1, kind='transport_cursor', scope='after_connection_baseline',
             begin=deepcopy(cursor), end=deepcopy(cursor), delivery_verified=True, quiet=True, broker_atomic=False))


def change_tables(raw, table, transform):
    transform(raw['tables'][table])
    raw['previous_tables'] = deepcopy(raw['tables'])
    return raw


def compare(raw=None, oms=OMS, links=LINKS, policy=POLICY, **kw):
    return compare_quik(snapshot() if raw is None else raw, policy, oms, links,
                        now=kw.get('now', NOW), current_revision=kw.get('revision', oms.revision))


def codes(report):
    return {item.code for item in report.differences}


class NormalizationTests(TestCase):
    def test_complete_nonempty_observation_preserves_exact_ids_prices_and_signed_lots(self):
        raw = change_tables(snapshot(), 'holdings', lambda rows: rows[0].update(totalnet=-3))
        observed = normalize_quik_snapshot(raw, POLICY, now=NOW)
        self.assertEqual(observed.issues, ())
        self.assertEqual(observed.orders[0].broker_id, BROKER_ID)
        self.assertEqual(observed.trades[0].price, Decimal('100'))
        self.assertEqual(observed.trades[0].commission, Decimal('1.25'))
        self.assertEqual(observed.positions[0].quantity, -3)
        self.assertEqual(observed.trades[0].occurred_at, START)

    def test_identical_trade_duplicates_count_once_without_mutating_source(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(deepcopy(rows[0])))
        before = deepcopy(raw)
        observed = normalize_quik_snapshot(raw, POLICY, now=NOW)
        self.assertEqual((len(observed.trades), observed.duplicate_trades, observed.issues), (1, 1, ()))
        self.assertEqual(raw, before)

    def test_conflicting_trade_identity_is_not_last_write_wins(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0], price=101)))
        self.assertIn('trades.conflicting_duplicate', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)
        self.assertFalse(compare(raw).consistent)

    def test_unknown_commission_is_not_zero(self):
        observed = normalize_quik_snapshot(snapshot(), replace(POLICY, routes=(replace(SPEC, commission_fields=None),)), now=NOW)
        self.assertIsNone(observed.trades[0].commission)
        self.assertIn('commission_unverified', codes(compare(policy=replace(POLICY, routes=(replace(SPEC, commission_fields=None),)))))

    def test_declared_commission_must_be_present(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows[0].pop('broker_comission'))
        self.assertIn('trades.invalid_row', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_invalid_numbers_and_cancelled_trades_cannot_be_normal_fills(self):
        for field, value in [('trade_num', float(TRADE_ID)), ('qty', True), ('qty', 1.5),
                             ('price', 'NaN'), ('price', '100.1'), ('flags', 16)]:
            raw = change_tables(snapshot(), 'trades', lambda rows: rows[0].update({field: value}))
            with self.subTest(field=field, value=value):
                self.assertIn('trades.invalid_row', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_holdings_type_is_explicit_and_never_summed_across_types(self):
        raw = change_tables(snapshot(), 'holdings', lambda rows: rows.append(dict(rows[0], type=1, totalnet=999)))
        observed = normalize_quik_snapshot(raw, POLICY, now=NOW)
        self.assertEqual(observed.positions[0].quantity, 1)
        self.assertEqual(dict(observed.excluded_rows)['holdings'], 1)
        for value in (True, 1.5, '1', None):
            bad = change_tables(snapshot(), 'holdings', lambda rows: rows[0].update(totalnet=value))
            with self.subTest(value=value):
                self.assertIn('holdings.invalid_row', normalize_quik_snapshot(bad, POLICY, now=NOW).issues)

    def test_duplicate_holdings_and_orders_are_ambiguous(self):
        for table in ('orders', 'holdings'):
            raw = change_tables(snapshot(), table, lambda rows: rows.append(deepcopy(rows[0])))
            with self.subTest(table=table):
                self.assertIn(table + '.duplicate', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_unknown_route_in_selected_account_is_visible(self):
        raw = change_tables(snapshot(), 'holdings', lambda rows: rows.append(dict(rows[0], sec_code='OTHER')))
        self.assertIn('holdings.unconfigured_route', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_other_account_is_counted_as_excluded_not_imported(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows.append(dict(rows[0], account='OTHER', order_num=7)))
        observed = normalize_quik_snapshot(raw, POLICY, now=NOW)
        self.assertEqual((len(observed.orders), dict(observed.excluded_rows)['orders']), (1, 1))
        self.assertEqual(observed.issues, ())

    def test_account_membership_requires_exact_firm_and_spbfut(self):
        for field, value in [('firmid', 'OTHER'), ('class_codes', '|TQBR|')]:
            raw = change_tables(snapshot(), 'accounts', lambda rows: rows[0].update({field: value}))
            with self.subTest(field=field):
                self.assertIn('accounts.scope_unverified', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_no_cached_flag_can_hide_changed_tables_or_missing_table(self):
        raw = snapshot()
        raw['previous_tables']['holdings'][0]['totalnet'] = 2
        self.assertIn('snapshot.tables_changed', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)
        raw = snapshot()
        raw['tables']['trades'] = None
        self.assertIn('snapshot.invalid_tables', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_boundary_baseline_failure_session_and_activity_are_rechecked(self):
        for field, value in [('sequence', 6), ('failed_sends', 1), ('bridge_session', 'c' * 32)]:
            raw = snapshot()
            raw['callback_boundary']['end'][field] = value
            with self.subTest(field=field):
                self.assertIn('snapshot.boundary_unverified', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)
        for field, value in [('delivery_verified', 1), ('quiet', False), ('broker_atomic', True)]:
            raw = snapshot()
            raw['callback_boundary'][field] = value
            self.assertIn('snapshot.boundary_unverified', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_profile_version_date_freshness_and_timezone_are_required(self):
        cases = []
        for field, value in [('profile', 'psb-live'), ('quik_version', '13.0.2.20')]:
            raw = snapshot(); raw['runtime'][field] = value; cases.append(raw)
        raw = snapshot(); raw['trading_date_after'] = '02.01.2026'; cases.append(raw)
        raw = snapshot(); raw['finished_at'] = '2026-01-01T10:00:01'; cases.append(raw)
        for raw in cases:
            self.assertFalse(compare(raw).consistent)
        self.assertFalse(compare(now=NOW + timedelta(minutes=1)).consistent)
        self.assertFalse(compare(now=START).consistent)

    def test_market_orders_are_observed_without_inventing_limit_price(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].update(flags=1, price=0))
        observed = normalize_quik_snapshot(raw, POLICY, now=NOW)
        self.assertEqual(observed.issues, ())
        self.assertIsNone(observed.orders[0].price)
        self.assertIn('order_mismatch', codes(compare(raw)))

    def test_policy_rejects_ambiguous_routes_invalid_type_and_implicit_session(self):
        for kwargs in (dict(routes=(SPEC, SPEC)), dict(session_id=''), dict(max_age=timedelta()),
                       dict(quik_version='13.2'), dict(routes=())):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                replace(POLICY, **kwargs)
        with self.assertRaises(ValueError):
            replace(SPEC, holding_type=True)


class ComparisonTests(TestCase):
    def test_matching_observations_never_authorize_apply_or_trading(self):
        result = compare()
        self.assertTrue(result.consistent)
        self.assertEqual(result.differences, ())
        self.assertIn('broker_atomicity_unverified', result.limitations)
        self.assertIn('history_coverage_unverified', result.limitations)
        self.assertFalse(result.reconciled)
        self.assertFalse(result.can_apply)
        self.assertFalse(result.eligible_for_trading)
        self.assertFalse(result.eligible_for_capital)
        self.assertEqual(result.scope, POLICY)
        self.assertEqual((result.oms_revision, result.gateway_revision), (7, 4))

    def test_positions_compare_aggregate_owners_and_preserve_ownership(self):
        oms = replace(OMS, positions=(('alpha', Position(ROUTE, 3, 90)), ('beta', Position(ROUTE, -2, 120))))
        self.assertTrue(compare(oms=oms).consistent)
        self.assertEqual(oms.positions[0][1].quantity, 3)

    def test_missing_holding_is_not_an_assumed_zero_position(self):
        raw = change_tables(snapshot(), 'holdings', list.clear)
        self.assertIn('position_not_observed', codes(compare(raw)))

    def test_position_difference_is_not_imported_into_manual(self):
        raw = change_tables(snapshot(), 'holdings', lambda rows: rows[0].update(totalnet=3))
        self.assertIn('position_mismatch', codes(compare(raw)))
        self.assertEqual(OMS.positions, (('alpha', Position(ROUTE, 1, 100)),))

    def test_unmapped_order_is_not_correlated_by_trans_id_or_contents(self):
        result = compare(links=replace(LINKS, orders=()))
        self.assertIn('external_order', codes(result))
        self.assertIn('order_unmapped', codes(result))

    def test_missing_order_and_unknown_submit_cancel_remain_explicit(self):
        raw = change_tables(snapshot(), 'orders', list.clear)
        self.assertIn('order_not_observed', codes(compare(raw)))
        for state in (OrderStatus.SUBMITTED, OrderStatus.CANCEL_PENDING):
            result = compare(oms=replace(OMS, orders=(replace(ORDER, status=state),)))
            with self.subTest(state=state):
                self.assertIn('order_outcome_unresolved', codes(result))

    def test_partial_fill_quantity_side_price_and_status_differences(self):
        for changes in (dict(balance=2), dict(flags=13), dict(price=101), dict(flags=10)):
            raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].update(changes))
            self.assertIn('order_mismatch', codes(compare(raw)))

    def test_repeat_trade_is_once_and_conflicting_financial_data_is_reported(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(deepcopy(rows[0])))
        self.assertTrue(compare(raw).consistent)
        for field, value in [('price', 99), ('commission', 2), ('quantity', 2)]:
            self.assertIn('execution_mismatch', codes(compare(oms=replace(OMS, executions=(replace(FILL, **{field: value}),)))))

    def test_trade_present_at_gateway_but_missing_in_oms_is_delivery_gap(self):
        self.assertIn('execution_not_recorded', codes(compare(oms=replace(OMS, executions=()))))

    def test_external_trade_and_missing_local_trade_are_distinct(self):
        self.assertIn('external_execution', codes(compare(links=replace(LINKS, executions=()))))
        raw = change_tables(snapshot(), 'trades', list.clear)
        self.assertIn('execution_not_observed', codes(compare(raw)))

    def test_execution_order_binding_must_match(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows[0].update(order_num=777))
        self.assertIn('execution_mismatch', codes(compare(raw)))

    def test_changed_revision_foreign_session_and_future_local_state_block_comparison(self):
        self.assertIn('oms_revision_changed', codes(compare(revision=8)))
        self.assertIn('binding_scope_mismatch', codes(compare(links=replace(LINKS, session_id='other'))))
        self.assertIn('oms_time_unverified', codes(compare(oms=replace(OMS, captured_at=NOW + timedelta(seconds=1)))))

    def test_ambiguous_binding_never_uses_last_link(self):
        links = replace(LINKS, orders=LINKS.orders + (BrokerLink(ROUTE, BROKER_ID, 'other'),))
        self.assertIn('binding_ambiguous', codes(compare(links=links)))

    def test_unconfigured_local_route_cannot_disappear(self):
        other = replace(ROUTE, account_id='OTHER')
        self.assertIn('local_route_outside_scope', codes(compare(oms=replace(OMS, positions=(('alpha', Position(other, 1, 100)),)))))

    def test_result_and_local_view_are_immutable(self):
        with self.assertRaises(FrozenInstanceError):
            compare().reconciled = True
        with self.assertRaises(FrozenInstanceError):
            OMS.revision = 8


class ComparisonBoundaryRegressions(TestCase):
    def test_missing_class_in_selected_account_is_not_silently_excluded(self):
        raw = change_tables(snapshot(), 'orders', lambda rows: rows[0].pop('class_code'))
        self.assertIn('orders.invalid_row', normalize_quik_snapshot(raw, POLICY, now=NOW).issues)

    def test_missing_end_time_returns_unverified_report(self):
        raw = snapshot()
        del raw['finished_at']
        self.assertFalse(compare(raw).consistent)

    def test_incomplete_execution_history_is_explicit(self):
        raw = change_tables(snapshot(), 'trades', list.clear)
        self.assertIn('order_execution_coverage_gap', codes(compare(raw)))

    def test_unused_binding_to_unknown_local_record_is_not_silent(self):
        links = replace(LINKS, orders=LINKS.orders + (BrokerLink(ROUTE, 777, 'absent'),))
        self.assertIn('binding_target_missing', codes(compare(links=links)))

    def test_local_events_after_observation_are_not_a_consistent_view(self):
        oms = replace(OMS, orders=(replace(ORDER, submitted_at=NOW),))
        self.assertIn('local_event_after_observation', codes(compare(oms=oms)))

    def test_cancelled_trade_state_does_not_hide_in_duplicate_normalization(self):
        raw = change_tables(snapshot(), 'trades', lambda rows: rows.append(dict(rows[0], flags=16)))
        self.assertFalse(compare(raw).consistent)


class ComparisonRuntimeTests(IsolatedAsyncioTestCase):
    async def test_durable_mappings_and_oms_view_survive_restart_without_applying_report(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            clock = ManualClock(START)
            for iteration in range(2):
                with DurableJournal(root / 'oms') as oms_journal, DurableJournal(root / 'quik') as gateway_journal:
                    async with TradingRuntime(clock, journal=oms_journal, require_capital=True) as runtime:
                        gateway, context = await attach(runtime, gateway_journal, clock)
                        if iteration == 0:
                            await capital(runtime)
                            await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                            await runtime.drain()
                            await gateway.feed('OnOrder', order_report(1))
                            await gateway.feed('OnTrade', trade_report())
                            await runtime.drain()
                            clock.advance_to(NOW)
                            await runtime.orders.handle(GatewayUncertain('quik', 'diagnostic test'))
                            await runtime.drain()
                        view = runtime.orders.observe_gateway('quik')
                        bindings = gateway.comparison_bindings
                        policy = replace(POLICY, session_id='psb-fixture')
                        before = (runtime.orders.revision, runtime.orders.ready, runtime.orders.capital_usage(),
                                  runtime.ledger.positions(), len(oms_journal.records), len(gateway_journal.records),
                                  gateway.prepared)
                        result = compare_quik(snapshot(), policy, view, bindings, now=clock.now(),
                                              current_revision=runtime.orders.revision)
                        self.assertTrue(result.consistent, result)
                        self.assertFalse(result.can_apply)
                        self.assertFalse(runtime.orders.ready)
                        self.assertEqual(sum(u.orders for u in runtime.orders.capital_usage()), 450)
                        self.assertEqual(before, (runtime.orders.revision, runtime.orders.ready,
                            runtime.orders.capital_usage(), runtime.ledger.positions(), len(oms_journal.records),
                            len(gateway_journal.records), gateway.prepared))
                        self.assertEqual(bindings.executions[0].local_id, view.executions[0].execution_id)
                        self.assertEqual(bindings.orders[0].local_id, view.orders[0].order_id)
                        if iteration:
                            self.assertEqual(gateway.prepared, ())

    async def test_export_covers_all_owners_and_detects_later_revision(self):
        clock = ManualClock(START)
        async with TradingRuntime(clock) as runtime:
            runtime.orders.gateway_ids.add('quik')
            runtime.ledger.import_balance('alpha', OpeningBalance(ROUTE, 3, 90), START)
            runtime.ledger.import_balance('manual', OpeningBalance(ROUTE, -2, 120), START)
            view = runtime.orders.observe_gateway('quik')
            self.assertEqual({owner for owner, _ in view.positions}, {'alpha', 'manual'})
            with self.assertRaises(ValueError):
                runtime.orders.observe_gateway('missing')
            await runtime.orders.handle(GatewayUncertain('quik', 'revision moved'))
            self.assertIn('oms_revision_changed', codes(compare_quik(snapshot(), POLICY, view, LINKS,
                now=NOW, current_revision=runtime.orders.revision)))

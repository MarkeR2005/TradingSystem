"""Synthetic resource scenarios; no user balances or brokerage credentials."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal, localcontext
from unittest import TestCase

from trading_system.quik_capital import (
    FuturesResourcePolicy, LocalMarginReservation, ReservationState,
    assess_futures_resource,
)
from trading_system.quik_limits import FuturesLimitKey, build_limits_report
from tests_v2.quik_limits_fixture import accounts, limit_rows


NOW = datetime(2026, 10, 19, 12, tzinfo=UTC)
KEY = FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR')


def policy():
    return FuturesResourcePolicy('demo-quik', '13.2.0.1', KEY, date(2026, 10, 19), timedelta(seconds=30))


def snapshot():
    tables = {'accounts': accounts(), 'limits': limit_rows(), 'orders': [], 'trades': [], 'holdings': []}
    return {'schema': 1, 'mode': 'read_only', 'runtime': {'protocol': 1, 'mode': 'read_only',
        'profile': 'demo-quik', 'quik_version': '13.2.0.1', 'lua_version': 'Lua 5.4'},
        'connection_id': 'synthetic-session', 'started_at': (NOW - timedelta(seconds=2)).isoformat(),
        'finished_at': (NOW - timedelta(seconds=1)).isoformat(),
        'trading_date_before': '19.10.2026', 'trading_date_after': '19.10.2026',
        'classes': ['SPBFUT', 'QJSIM'], 'tables': tables, 'previous_tables': deepcopy(tables),
        'limits': build_limits_report(accounts(), limit_rows(), accounts(), limit_rows(), selection=KEY),
        'financial': None, 'issues': [], 'callbacks_before': 0, 'callbacks_after': 0,
        'repeatable_observation': True, 'reconciled': False}


def refresh_limits(value):
    a, b = value['previous_tables'], value['tables']
    value['limits'] = build_limits_report(a['accounts'], a['limits'], b['accounts'], b['limits'], selection=KEY)


def reserve(amount='1000.25', state=ReservationState.LOCAL_ONLY, reservation_id='intent-1'):
    return LocalMarginReservation(reservation_id, KEY, 'RUB', Decimal(amount), state)


class FuturesResourceTests(TestCase):
    def assess(self, value=None, reservations=(), **kwargs):
        return assess_futures_resource(snapshot() if value is None else value,
                                       policy(), now=NOW, reservations=reservations, **kwargs)

    def assert_blocked(self, value, issue):
        result = self.assess(value)
        self.assertIn(issue, result.issues)
        self.assertIsNone(result.candidate_free)
        self.assertFalse(result.complete)
        return result

    def test_observed_free_does_not_subtract_broker_usage_or_add_variation_margin(self):
        result = self.assess()
        self.assertTrue(result.complete)
        self.assertEqual(result.broker_free, Decimal('84999.75'))
        self.assertEqual(result.candidate_free, Decimal('84999.75'))
        self.assertEqual(result.local_reserved, Decimal(0))
        self.assertEqual(result.currency, 'RUB')
        self.assertEqual(result.selection, KEY)
        self.assertEqual(result.policy_version, 'quik.cbplplanned.v1')
        self.assertEqual(result.connection_id, 'synthetic-session')
        self.assertEqual(result.observed_at, NOW - timedelta(seconds=2))
        self.assertFalse(result.eligible_for_capital)
        self.assertFalse(result.eligible_for_trading)
        self.assertFalse(result.reconciled)

    def test_only_local_margin_is_subtracted_once(self):
        result = self.assess(reservations=(reserve(), reserve('500.10', reservation_id='intent-2')))
        self.assertEqual(result.local_reserved, Decimal('1500.35'))
        self.assertEqual(result.candidate_free, Decimal('83499.40'))

    def test_submitted_obligation_requires_cutover_even_if_amount_is_zero(self):
        for amount in ('1000', '0'):
            with self.subTest(amount=amount):
                result = self.assess(reservations=(reserve(amount, ReservationState.SUBMITTED_UNRECONCILED),))
                self.assertEqual(result.broker_free, Decimal('84999.75'))
                self.assertIsNone(result.candidate_free)
                self.assertIn('reservations.cutover_required', result.issues)

    def test_partial_fill_does_not_release_or_double_subtract_unreconciled_reserve(self):
        value = snapshot()
        for tables in (value['tables'], value['previous_tables']):
            tables['orders'] = [{'order_num': 501, 'trans_id': 1, 'qty': 4, 'balance': 2}]
            tables['trades'] = [{'trade_num': 901, 'order_num': 501, 'qty': 2}]
            tables['holdings'] = [{'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'sec_code': 'TEST', 'totalnet': 2}]
        result = self.assess(value, reservations=(reserve('2000', ReservationState.SUBMITTED_UNRECONCILED),))
        self.assertIn('reservations.cutover_required', result.issues)
        self.assertIsNone(result.candidate_free)

    def test_empty_broker_tables_do_not_prove_cancelled_reserve_is_released(self):
        # A cancel timeout / lost callback cannot be resolved by two empty reads.
        result = self.assess(reservations=(reserve('2000', ReservationState.SUBMITTED_UNRECONCILED),))
        self.assertIsNone(result.candidate_free)
        self.assertFalse(result.reconciled)

    def test_broker_occupation_moves_from_orders_to_positions_without_changing_free(self):
        value = snapshot()
        for tables in (value['tables'], value['previous_tables']):
            tables['limits'][0].update(cbplused_for_orders=0, cbplused_for_positions=15000.5)
        refresh_limits(value)
        self.assertEqual(self.assess(value).candidate_free, Decimal('84999.75'))

    def test_negative_free_and_overlay_deficit_remain_signed(self):
        value = snapshot()
        for tables in (value['tables'], value['previous_tables']):
            tables['limits'][0]['cbplplanned'] = '-12.50'
        refresh_limits(value)
        result = self.assess(value, reservations=(reserve('1.25'),))
        self.assertTrue(result.complete)
        self.assertEqual(result.broker_free, Decimal('-12.5'))
        self.assertEqual(result.candidate_free, Decimal('-13.75'))
        self.assertFalse(result.eligible_for_trading)

    def test_zero_free_is_valid_and_never_replaced_with_cbplimit(self):
        value = snapshot()
        for tables in (value['tables'], value['previous_tables']):
            tables['limits'][0]['cbplplanned'] = 0
        refresh_limits(value)
        self.assertEqual(self.assess(value).candidate_free, Decimal(0))

    def test_missing_or_invalid_money_never_becomes_zero(self):
        for invalid in (None, True, 'NaN', 'Infinity', '12,5', '1e3'):
            with self.subTest(invalid=invalid):
                value = snapshot()
                for tables in (value['tables'], value['previous_tables']):
                    tables['limits'][0]['cbplplanned'] = invalid
                refresh_limits(value)
                self.assert_blocked(value, 'limits.unverified')

    def test_selection_cannot_be_guessed_from_cash_row(self):
        value = snapshot()
        value['limits'] = build_limits_report(accounts(), limit_rows(), accounts(), limit_rows())
        self.assert_blocked(value, 'limits.selection_mismatch')

    def test_currency_alias_does_not_change_exact_selection(self):
        value = snapshot()
        value['limits']['selection']['currcode'] = 'RUB'
        self.assert_blocked(value, 'limits.selection_mismatch')

    def test_forged_cached_amount_does_not_override_raw_rows(self):
        value = snapshot()
        value['limits']['selected_observation']['values']['cbplplanned'] = '9999999'
        self.assert_blocked(value, 'limits.report_mismatch')

    def test_changed_raw_rows_block_even_when_cached_flags_claim_success(self):
        value = snapshot()
        value['tables']['limits'][0]['cbplplanned'] = 1
        self.assert_blocked(value, 'snapshot.tables_changed')

    def test_change_to_other_tables_also_blocks_resource(self):
        for name, row in (('holdings', {'totalnet': 1}), ('orders', {'order_num': 7, 'trans_id': 8}),
                          ('trades', {'trade_num': 9, 'order_num': 7})):
            with self.subTest(name=name):
                value = snapshot()
                value['tables'][name].append(row)
                self.assert_blocked(value, 'snapshot.tables_changed')

    def test_duplicate_key_and_wrong_account_are_not_ignored(self):
        for duplicate in (True, False):
            value = snapshot()
            for tables in (value['tables'], value['previous_tables']):
                if duplicate:
                    tables['limits'].append(deepcopy(tables['limits'][0]))
                else:
                    tables['accounts'][0]['class_codes'] = '|QJSIM|'
            refresh_limits(value)
            self.assert_blocked(value, 'limits.unverified')

    def test_malformed_or_missing_raw_tables_are_rejected(self):
        for bad in (None, {}, '[]', [None]):
            value = snapshot()
            value['tables']['orders'] = bad
            self.assert_blocked(value, 'snapshot.invalid_tables')

    def test_reordering_rows_is_harmless(self):
        value = snapshot()
        for key in ('accounts', 'limits'):
            value['tables'][key].reverse()
        self.assertTrue(self.assess(value).complete)

    def test_equal_python_values_do_not_hide_raw_type_changes(self):
        value = snapshot()
        value['previous_tables']['holdings'] = [{'totalnet': 1}]
        value['tables']['holdings'] = [{'totalnet': True}]
        self.assert_blocked(value, 'snapshot.tables_changed')

    def test_cached_report_rejects_boolean_in_place_of_numeric_identity(self):
        value = snapshot()
        value['limits']['selection']['limit_type'] = False
        self.assert_blocked(value, 'limits.selection_mismatch')

    def test_callback_activity_is_not_an_atomic_cutover(self):
        value = snapshot()
        value['callbacks_after'] = 1
        self.assert_blocked(value, 'snapshot.callbacks_changed')

    def test_invalid_callback_counters_are_rejected(self):
        for bad in (True, -1, '0', None):
            value = snapshot()
            value['callbacks_before'] = value['callbacks_after'] = bad
            self.assert_blocked(value, 'snapshot.invalid_callbacks')

    def test_runtime_identity_is_exact_including_patch_version(self):
        for field, bad in (('profile', 'psb-live'), ('quik_version', '13.2.0.2'),
                           ('lua_version', 'Lua 5.3'), ('protocol', True), ('mode', 'trading')):
            value = snapshot()
            value['runtime'][field] = bad
            self.assert_blocked(value, 'snapshot.runtime_mismatch')

    def test_declared_snapshot_issues_and_failure_flags_block(self):
        for field, bad in (('issues', ['connection_lost']), ('repeatable_observation', False),
                           ('repeatable_observation', 1), ('mode', 'trading'), ('schema', True),
                           ('reconciled', True)):
            value = snapshot()
            value[field] = bad
            self.assert_blocked(value, 'snapshot.unverified')

    def test_financial_failure_does_not_get_lost(self):
        value = snapshot()
        value['financial'] = {'complete': False, 'repeatable_observation': False, 'issues': ['missing']}
        self.assert_blocked(value, 'snapshot.financial_unverified')

    def test_missing_spbfut_or_session_identity_blocks(self):
        value = snapshot()
        value['classes'] = ['QJSIM']
        self.assert_blocked(value, 'snapshot.no_spbfut')
        value = snapshot()
        value['connection_id'] = ''
        self.assert_blocked(value, 'snapshot.invalid_connection')

    def test_trading_date_is_explicit_and_cannot_roll_over_during_scan(self):
        for before, after in (('18.10.2026', '18.10.2026'), ('19.10.2026', '20.10.2026'),
                              ('bad', 'bad')):
            value = snapshot()
            value['trading_date_before'], value['trading_date_after'] = before, after
            self.assert_blocked(value, 'snapshot.trading_date_mismatch')

    def test_expiry_uses_first_read_not_recent_finish(self):
        value = snapshot()
        value['started_at'] = (NOW - timedelta(seconds=30)).isoformat()
        self.assert_blocked(value, 'snapshot.expired')

    def test_freshness_boundary_is_exclusive(self):
        value = snapshot()
        value['started_at'] = (NOW - timedelta(seconds=29, microseconds=999999)).isoformat()
        self.assertTrue(self.assess(value).complete)

    def test_future_reversed_naive_and_malformed_timestamps_block(self):
        for field, bad in (('finished_at', (NOW + timedelta(seconds=1)).isoformat()),
                           ('started_at', NOW.isoformat()), ('started_at', '2026-10-19T12:00:00'),
                           ('finished_at', 'bad'), ('finished_at', None)):
            value = snapshot()
            value[field] = bad
            self.assert_blocked(value, 'snapshot.invalid_time')

    def test_timezone_offsets_compare_as_instants(self):
        value = snapshot()
        value['started_at'] = (NOW - timedelta(seconds=2)).astimezone(timezone(timedelta(hours=3))).isoformat()
        self.assertTrue(self.assess(value).complete)

    def test_reservation_identity_currency_and_duplicates_block_overlay(self):
        for reservations, issue in (
            ((replace(reserve(), selection=replace(KEY, trdaccid='OTHER')),), 'reservations.selection_mismatch'),
            ((replace(reserve(), currency='USD'),), 'reservations.currency_mismatch'),
            ((reserve(), reserve()), 'reservations.duplicate_id'),
        ):
            result = self.assess(reservations=reservations)
            self.assertIn(issue, result.issues)
            self.assertIsNone(result.candidate_free)

    def test_precision_is_independent_of_ambient_decimal_context(self):
        with localcontext() as ctx:
            ctx.prec = 3
            result = self.assess(reservations=(reserve('0.00000001'),))
        self.assertEqual(result.candidate_free, Decimal('84999.74999999'))

    def test_deterministic_and_no_input_mutation(self):
        value = snapshot()
        before = deepcopy(value)
        first = self.assess(value, reservations=(reserve(),))
        second = self.assess(value, reservations=(reserve(),))
        self.assertEqual(asdict(first), asdict(second))
        self.assertEqual(value, before)

    def test_bad_policy_configuration_is_rejected(self):
        for changes in ({'profile': ''}, {'quik_version': '13.2'}, {'selection': replace(KEY, limit_type=6)},
                        {'selection': replace(KEY, currcode='USD')}, {'trading_date': NOW},
                        {'max_age': timedelta(0)}, {'max_age': timedelta(seconds=-1)}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(policy(), **changes)

    def test_reservations_require_finite_bounded_decimal_and_explicit_state(self):
        for changes in ({'amount': 1.25}, {'amount': Decimal('-1')}, {'amount': Decimal('NaN')},
                        {'amount': Decimal('Infinity')}, {'amount': Decimal('1e999999')},
                        {'amount': Decimal('1e-999999')}, {'state': 'local_only'}, {'reservation_id': ''}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(reserve(), **changes)

    def test_untyped_or_unbounded_overlay_and_naive_now_are_programmer_errors(self):
        for reservations in ([], (None,), (reserve(),) * 50001):
            with self.assertRaises(ValueError):
                self.assess(reservations=reservations)
        with self.assertRaises(ValueError):
            assess_futures_resource(snapshot(), policy(), now=NOW.replace(tzinfo=None), reservations=())

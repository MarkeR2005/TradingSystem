from copy import deepcopy
from dataclasses import replace
from unittest import TestCase

from trading_system.quik_limits import FuturesLimitKey, build_limits_report
from tests_v2.quik_limits_fixture import accounts, limit_row, limit_rows


class FuturesLimitTests(TestCase):
    def setUp(self):
        self.key = FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR')

    def report(self, rows=None, account_rows=None, selection=True):
        rows = limit_rows() if rows is None else rows
        account_rows = accounts() if account_rows is None else account_rows
        return build_limits_report(account_rows, rows, deepcopy(account_rows), deepcopy(rows),
                                   selection=self.key if selection else None)

    def test_discovery_keeps_four_limit_types_separate_without_selection(self):
        result = self.report(selection=False)
        self.assertTrue(result['complete'])
        self.assertTrue(result['repeatable_observation'])
        self.assertEqual(len(result['rows']), 4)
        self.assertEqual({r['key']['limit_type'] for r in result['rows']}, {0, 3, 6, 7})
        self.assertFalse(result['selection_verified'])
        self.assertIsNone(result['selected_observation'])
        self.assertFalse(result['eligible_for_capital'])

    def test_exact_cash_selection_preserves_observed_fields_not_sum_or_equity(self):
        result = self.report()
        self.assertTrue(result['selection_verified'], result['issues'])
        selected = result['selected_observation']
        self.assertEqual(selected['key']['limit_type'], 0)
        self.assertEqual(selected['values']['cbplimit'], '100000.25')
        self.assertEqual(selected['values']['cbplplanned'], '84999.75')
        self.assertEqual(selected['values']['varmargin'], '-12.5')
        self.assertEqual(selected['currency'], 'RUB')
        self.assertNotIn('equity', selected)
        self.assertNotIn('available_capital', result)
        self.assertFalse(result['eligible_for_capital'])
        self.assertFalse(result['eligible_for_trading'])
        self.assertFalse(result['reconciled'])

    def test_missing_key_never_falls_back_to_another_account_type_or_currency(self):
        for key in (replace(self.key, firmid='OTHER'), replace(self.key, trdaccid='OTHER'),
                    replace(self.key, limit_type=2), replace(self.key, currcode='RUB')):
            with self.subTest(key=key):
                result = build_limits_report(accounts(), limit_rows(), accounts(), limit_rows(), selection=key)
                self.assertFalse(result['selection_verified'])
                self.assertIn('selection.not_unique', result['issues'])

    def test_sur_and_rub_keys_are_distinct_despite_normalized_currency(self):
        rows = [limit_row(), limit_row(currcode='RUB')]
        rows[1]['cbplimit'] = 2
        result = self.report(rows)
        self.assertTrue(result['selection_verified'])
        self.assertEqual(result['selected_observation']['values']['cbplimit'], '100000.25')

    def test_duplicate_key_blocks_selection_even_for_identical_rows(self):
        row = limit_row()
        result = self.report([row, deepcopy(row)])
        self.assertFalse(result['selection_verified'])
        self.assertIn('duplicate_key', result['rows'][0]['issues'])

    def test_unknown_or_ambiguous_account_is_not_accepted(self):
        for acc in ([], [accounts()[1]], accounts() + [accounts()[0]]):
            with self.subTest(accounts=acc):
                result = self.report(account_rows=acc)
                self.assertFalse(result['selection_verified'])
                self.assertTrue(any('account.not_unique' in r['issues'] for r in result['rows']))

    def test_class_membership_must_be_exact(self):
        acc = accounts()
        acc[0]['class_codes'] = '|NOTSPBFUT|'
        result = self.report(account_rows=acc)
        self.assertIn('account.no_spbfut', result['rows'][0]['issues'])
        self.assertFalse(result['selection_verified'])

    def test_other_limit_types_and_empty_currency_are_not_implicit_cash(self):
        for limit_type, currency in ((3, ''), (6, 'SUR'), (7, 'RUB'), (0, 'USD')):
            rows = [limit_row(limit_type, currency)]
            key = FuturesLimitKey('DEMO', 'FUTURES', limit_type, currency)
            result = build_limits_report(accounts(), rows, accounts(), rows, selection=key)
            with self.subTest(limit_type=limit_type, currency=currency):
                self.assertIn('selection.unsupported_policy', result['issues'])
                self.assertFalse(result['selection_verified'])
                self.assertEqual(result['rows'][0]['key']['currcode'], currency)

    def test_missing_money_is_unknown_not_zero(self):
        for name in ('cbplimit', 'cbplused', 'cbplplanned', 'cbplused_for_orders',
                     'cbplused_for_positions', 'varmargin', 'real_varmargin', 'kgo'):
            rows = [limit_row()]
            del rows[0][name]
            with self.subTest(name=name):
                result = self.report(rows)
                self.assertIsNone(result['rows'][0]['values'][name])
                self.assertFalse(result['selection_verified'])

    def test_malformed_machine_values_do_not_become_numbers(self):
        for value in (None, True, '1 000', '1_000', '1,25', 'NaN', 'Infinity', '1e4',
                      float('nan'), float('inf'), '9' * 81, [], {}):
            rows = [limit_row()]
            rows[0]['cbplimit'] = value
            with self.subTest(value=value):
                result = self.report(rows)
                self.assertIsNone(result['rows'][0]['values']['cbplimit'])
                self.assertFalse(result['selection_verified'])

    def test_negative_and_zero_balances_stay_observations_without_deficit_action(self):
        for value in ('-10.25', 0):
            rows = [limit_row()]
            rows[0]['cbplimit'] = value
            with self.subTest(value=value):
                result = self.report(rows)
                self.assertTrue(result['selection_verified'])
                self.assertEqual(result['selected_observation']['values']['cbplimit'], str(value))
                self.assertFalse(result['eligible_for_capital'])
                self.assertNotIn('deficit', result)

    def test_optional_fields_are_not_defaulted_and_invalid_present_values_block(self):
        rows = [limit_row()]
        del rows[0]['go_planned']
        result = self.report(rows)
        self.assertTrue(result['selection_verified'])
        self.assertIsNone(result['rows'][0]['values']['go_planned'])
        rows[0]['go_planned'] = 'not a number'
        self.assertFalse(self.report(rows)['selection_verified'])

    def test_unknown_limit_type_is_retained_without_crashing(self):
        result = self.report([limit_row(99)], selection=False)
        self.assertEqual(result['rows'][0]['key']['limit_type'], 99)
        self.assertEqual(result['rows'][0]['limit_type_name'], 'unknown')
        self.assertFalse(result['complete'])

    def test_invalid_identity_and_boolean_type_cannot_match_zero(self):
        for field, value in (('firmid', ''), ('trdaccid', None), ('limit_type', False),
                             ('limit_type', 0.0), ('currcode', None)):
            rows = [limit_row()]
            rows[0][field] = value
            with self.subTest(field=field):
                result = self.report(rows)
                self.assertFalse(result['complete'])
                self.assertFalse(result['selection_verified'])

    def test_key_rejects_ambiguous_or_invalid_config(self):
        for changes in ({'firmid': ''}, {'trdaccid': 'bad|account'}, {'limit_type': True},
                        {'limit_type': -1}, {'currcode': 'sur'}, {'currcode': None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.key, **changes)

    def test_reordering_does_not_change_observation(self):
        result = build_limits_report(accounts(), limit_rows(), list(reversed(accounts())),
                                     list(reversed(limit_rows())), selection=self.key)
        self.assertTrue(result['selection_verified'])

    def test_first_pass_failure_is_not_hidden_by_second_pass(self):
        first = [limit_row()]
        del first[0]['cbplimit']
        result = build_limits_report(accounts(), first, accounts(), [limit_row()], selection=self.key)
        self.assertFalse(result['complete'])
        self.assertFalse(result['selection_verified'])

    def test_any_table_change_or_snapshot_issue_withholds_selection(self):
        changed = limit_rows()
        changed[3]['cbplimit'] = -1
        result = build_limits_report(accounts(), limit_rows(), accounts(), changed, selection=self.key)
        self.assertIn('limits_changed', result['issues'])
        self.assertFalse(result['selection_verified'])
        for issue in ('callbacks_during_scan', 'trading_date_changed', 'financial_parameters_incomplete'):
            result = build_limits_report(accounts(), limit_rows(), accounts(), limit_rows(),
                                         selection=self.key, snapshot_issues=(issue,))
            with self.subTest(issue=issue):
                self.assertIn('snapshot_unverified', result['issues'])
                self.assertFalse(result['selection_verified'])

    def test_account_changes_withhold_selection(self):
        second = accounts()
        second[0]['class_codes'] = '|SPBFUT|'
        result = build_limits_report(accounts(), limit_rows(), second, limit_rows(), selection=self.key)
        self.assertIn('accounts_changed', result['issues'])
        self.assertFalse(result['selection_verified'])

    def test_empty_limits_are_not_a_confirmed_zero_account(self):
        result = self.report([], selection=False)
        self.assertFalse(result['complete'])
        self.assertIn('limits.empty', result['issues'])

    def test_output_does_not_alias_input(self):
        rows = [limit_row()]
        result = self.report(rows)
        rows[0]['cbplimit'] = 4
        self.assertEqual(result['selected_observation']['values']['cbplimit'], '100000.25')

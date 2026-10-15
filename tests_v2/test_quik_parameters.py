from copy import deepcopy
from unittest import TestCase

from trading_system.quik_parameters import build_financial_report
from tests_v2.quik_parameters_fixture import parameters, security


class FinancialParametersTests(TestCase):
    def report(self, sec=None, params=None):
        sec = security() if sec is None else sec
        params = parameters() if params is None else params
        return build_financial_report(sec, params, deepcopy(sec), deepcopy(params))

    def test_siz6_point_value_is_not_underlying_lot_size(self):
        report = self.report()
        self.assertTrue(report['complete'])
        self.assertTrue(report['repeatable_observation'])
        self.assertEqual(report['values']['lot_size'], '1000')
        self.assertEqual(report['values']['currency'], 'RUB')
        self.assertEqual(report['point_value_candidate'], '1')
        self.assertFalse(report['eligible_for_trading'])

    def test_point_value_uses_price_step_and_step_value_exactly(self):
        sec, params = security(), parameters()
        sec['min_price_step'] = 0.01
        params['SEC_PRICE_STEP']['param_value'] = '0.010000'
        params['STEPPRICE']['param_value'] = '5.000000'
        self.assertEqual(self.report(sec, params)['point_value_candidate'], '500')

    def test_missing_parameter_is_not_zero_even_with_positive_payload(self):
        params = parameters()
        params['BUYDEPO']['result'] = '0'
        report = self.report(params=params)
        self.assertFalse(report['complete'])
        self.assertIsNone(report['values']['buy_depo'])
        self.assertIsNone(report['point_value_candidate'])
        self.assertIn('BUYDEPO.unavailable', report['issues'])

    def test_numeric_fields_require_finite_machine_strings_not_display_values(self):
        for value in ('', '1,25', 'NaN', 'Infinity', '1e999999', '1_000', ' 1 ', '9' * 81, True, 1.0, None):
            params = parameters()
            params['STEPPRICE'].update(param_value=value, param_image='1,00')
            with self.subTest(value=value):
                result = self.report(params=params)
                self.assertFalse(result['complete'])
                self.assertIsNone(result['values']['step_price'])

    def test_result_and_type_must_match_the_wire_contract(self):
        for key, value in (('result', 1), ('result', True), ('param_type', '3'), ('param_type', '4')):
            params = parameters()
            params['STEPPRICE'][key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(self.report(params=params)['complete'])
        params = parameters()
        params['STEPPRICE'].update(param_type='2', param_value='1.5')
        self.assertFalse(self.report(params=params)['complete'])

    def test_currency_uses_char_image_and_checks_metadata(self):
        sec, params = security(), parameters()
        params['CURSTEPPRICE']['param_image'] = 'RUB'
        self.assertTrue(self.report(sec, params)['complete'])
        for value in ('', 'USD', '0'):
            params['CURSTEPPRICE']['param_image'] = value
            with self.subTest(value=value):
                self.assertIsNone(self.report(sec, params)['point_value_candidate'])
        params['CURSTEPPRICE']['param_image'] = 'RUB'
        sec['step_price_currency'] = 'USD'
        self.assertIn('security.currency_mismatch', self.report(sec, params)['issues'])

    def test_zero_and_negative_values_are_not_usable_margin_or_tick_data(self):
        for name in ('SEC_PRICE_STEP', 'STEPPRICE', 'BUYDEPO', 'SELLDEPO'):
            for value in ('0.000000', '-1'):
                params = parameters()
                params[name]['param_value'] = value
                with self.subTest(name=name, value=value):
                    self.assertFalse(self.report(params=params)['complete'])

    def test_security_tick_and_contract_units_must_be_consistent(self):
        for key, value in (('min_price_step', 0.5), ('qty_multiplier', 2), ('qty_scale', 1),
                           ('lot_size', 0), ('lot_size', 0.5), ('class_code', 'QJSIM'), ('sec_code', 'OTHER')):
            sec = security()
            sec[key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(self.report(sec)['complete'])

    def test_changed_margin_or_security_withholds_point_value(self):
        first, second = parameters(), parameters()
        second['BUYDEPO']['param_value'] = '11000'
        result = build_financial_report(security(), first, security(), second)
        self.assertTrue(result['complete'])
        self.assertFalse(result['repeatable_observation'])
        self.assertIsNone(result['point_value_candidate'])
        self.assertIn('parameters_changed', result['issues'])
        changed = security()
        changed['exp_date'] = 20270318
        result = build_financial_report(security(), first, changed, first)
        self.assertIn('security_changed', result['issues'])

    def test_numeric_display_formatting_does_not_change_validated_values(self):
        first, second = parameters(), parameters()
        second['STEPPRICE'].update(param_value='1', param_image='1,00')
        report = build_financial_report(security(), first, security(), second)
        self.assertTrue(report['repeatable_observation'])

    def test_inexact_ratio_is_not_silently_rounded(self):
        sec, params = security(), parameters()
        sec['min_price_step'] = 3
        params['SEC_PRICE_STEP']['param_value'] = '3'
        report = self.report(sec, params)
        self.assertIsNone(report['point_value_candidate'])
        self.assertIn('point_value_inexact', report['issues'])

    def test_raw_reads_are_preserved_without_aliasing(self):
        params = parameters()
        report = self.report(params=params)
        params['BUYDEPO']['param_value'] = '999'
        self.assertEqual(report['parameters']['BUYDEPO']['param_value'], '10000.000000')

"""Diagnostic futures parameters; never constructs a Route or a capital policy."""

from copy import deepcopy
from decimal import Decimal, DecimalException, Inexact, localcontext
import re
from typing import Any

from .persistence import canonical
from .quik_contract import decimal, identifier

PARAMETER_CAPABILITY = 'instrument_parameters_v1'
PARAMETER_NAMES = ('SEC_PRICE_STEP', 'STEPPRICE', 'CURSTEPPRICE', 'BUYDEPO', 'SELLDEPO')
NUMERIC_FIELDS = {'SEC_PRICE_STEP': 'price_step', 'STEPPRICE': 'step_price',
                  'BUYDEPO': 'buy_depo', 'SELLDEPO': 'sell_depo'}
# Explicit policy for this RUB-only diagnostic. No inference from display names or FX conversion.
CURRENCIES = {'SUR': 'RUB', 'RUB': 'RUB'}


def _text(value: Decimal) -> str:
    result = format(value, 'f')
    return result.rstrip('0').rstrip('.') if '.' in result else result


def _parameter(raw: Any, name: str, issues: list[str]) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or raw.get('result') != '1':
        issues.append(name + '.unavailable')
        return None
    return raw


def _values(security: dict[str, Any], parameters: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    values: dict[str, Any] = {}
    for name, field in NUMERIC_FIELDS.items():
        values[field] = None
        raw = _parameter(parameters.get(name), name, issues)
        if raw is None:
            continue
        value = raw.get('param_value')
        if (raw.get('param_type') not in ('1', '2') or not isinstance(value, str)
                or len(value) > 80 or re.fullmatch(r'[+-]?[0-9]+(?:\.[0-9]+)?', value) is None):
            issues.append(name + '.invalid_numeric_value')
            continue
        number = decimal(value)
        # Demo QUIK 13.2 reports fractional BUYDEPO/SELLDEPO as type "2".
        # Preserve their decimal machine value; never truncate money to an integer.
        if (raw['param_type'] == '2' and name not in ('BUYDEPO', 'SELLDEPO')
                and number != number.to_integral_value()):
            issues.append(name + '.invalid_integer_value')
            continue
        if number <= 0:
            issues.append(name + '.nonpositive_value')
            continue
        values[field] = _text(number)

    values['currency'] = None
    raw_currency = _parameter(parameters.get('CURSTEPPRICE'), 'CURSTEPPRICE', issues)
    if raw_currency is not None:
        code = raw_currency.get('param_image')
        if raw_currency.get('param_type') == '3' and isinstance(code, str) and code in CURRENCIES:
            values['currency'] = CURRENCIES[code]
        else:
            issues.append('CURSTEPPRICE.unsupported_currency')
    metadata_currencies = [CURRENCIES.get(value) if isinstance(value, str) else None
                           for value in (security.get('step_price_currency'), security.get('trade_currency'))]
    if any(value is None or value != values['currency'] for value in metadata_currencies):
        issues.append('security.currency_mismatch')

    values.update(lot_size=None, qty_multiplier=None, qty_scale=None)
    try:
        if security.get('class_code') != 'SPBFUT':
            raise ValueError('unsupported class')
        code = identifier(security.get('code'))
        if security.get('sec_code', code) != code:
            raise ValueError('conflicting security code')
    except ValueError:
        issues.append('security.invalid_identity')
    try:
        lot = decimal(security.get('lot_size'))
        if lot <= 0 or lot != lot.to_integral_value():
            raise ValueError('invalid underlying lot size')
        values['lot_size'] = _text(lot)
        multiplier = decimal(security.get('qty_multiplier'))
        if multiplier != 1 or type(security.get('qty_scale')) is not int or security['qty_scale'] != 0:
            raise ValueError('unverified contract quantity units')
        values.update(qty_multiplier='1', qty_scale=0)
    except ValueError:
        issues.append('security.unsupported_quantity_units')
    try:
        tick = decimal(security.get('min_price_step'))
        if tick <= 0 or values['price_step'] is None or tick != Decimal(values['price_step']):
            raise ValueError('tick mismatch')
    except ValueError:
        issues.append('security.price_step_mismatch')
    return values, issues


def build_financial_report(first_security: dict[str, Any], first: dict[str, Any],
                           second_security: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    before, first_issues = _values(first_security, first)
    after, second_issues = _values(second_security, second)
    issues = list(dict.fromkeys(first_issues + second_issues))
    complete = not issues
    if before != after:
        issues.append('parameters_changed')
    if canonical(first_security) != canonical(second_security):
        issues.append('security_changed')
    candidate: str | None = None
    if not issues:
        try:
            with localcontext() as context:
                context.prec = 160
                context.traps[Inexact] = True
                candidate = _text(Decimal(after['step_price']) / Decimal(after['price_step']))
        except DecimalException:
            issues.append('point_value_inexact')
    return {'schema': 1, 'parameters': deepcopy(second), 'previous_parameters': deepcopy(first),
            'previous_security': deepcopy(first_security), 'values': after, 'previous_values': before,
            'issues': issues, 'complete': complete, 'repeatable_observation': not issues,
            'point_value_candidate': candidate, 'eligible_for_trading': False,
            'limitation': 'Observed instrument parameters are not account-specific margin, capital or trading permission.'}

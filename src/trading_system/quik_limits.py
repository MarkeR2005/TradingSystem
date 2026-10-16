"""Read-only futures limit observations; never constructs capital or deficit commands."""

from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import asdict, dataclass
import re
from typing import Any

from .persistence import canonical
from .quik_contract import decimal, identifier
from .quik_parameters import CURRENCIES

LIMIT_TYPES = {0: 'cash', 1: 'secured_cash', 2: 'combined_assets', 3: 'clearing_cash',
               4: 'clearing_secured_cash', 5: 'spot_open_positions',
               6: 'foreign_collateral_in_rubles', 7: 'foreign_collateral'}
REQUIRED_FIELDS = ('cbplimit', 'cbplused', 'cbplplanned', 'cbplused_for_orders',
                   'cbplused_for_positions', 'varmargin', 'real_varmargin', 'kgo')
OPTIONAL_FIELDS = ('cbp_prev_limit', 'liquidity_coef', 'accruedint', 'options_premium',
                   'ts_comission', 'go_without_orders', 'go_planned',
                   'indicative_varmargin', 'net_option_value')


@dataclass(frozen=True)
class FuturesLimitKey:
    firmid: str
    trdaccid: str
    limit_type: int
    currcode: str

    def __post_init__(self) -> None:
        identifier(self.firmid)
        identifier(self.trdaccid)
        if type(self.limit_type) is not int or not 0 <= self.limit_type <= 2**31 - 1:
            raise ValueError('limit type must be a nonnegative integer')
        # Empty currency is an observed identity, not an implicit SUR/RUB alias.
        if not isinstance(self.currcode, str) or re.fullmatch(r'[A-Z0-9]{0,16}', self.currcode) is None:
            raise ValueError('invalid exact limit currency code')


def _money(value: Any) -> str:
    if type(value) not in (str, int, float):
        raise ValueError('expected a finite machine number')
    if isinstance(value, str) and re.fullmatch(r'[+-]?[0-9]+(?:\.[0-9]+)?', value) is None:
        raise ValueError('invalid decimal machine string')
    number = decimal(value)
    text = format(number, 'f')
    if len(text) > 80:
        raise ValueError('numeric observation exceeds reporting range')
    if number == 0:
        return '0'
    return text.rstrip('0').rstrip('.') if '.' in text else text


def _table(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 50000 or any(not isinstance(row, dict) for row in value):
        raise ValueError('expected a bounded explicit list of records')
    return value


def _key(raw: dict[str, Any]) -> FuturesLimitKey:
    limit_type, currency = raw.get('limit_type'), raw.get('currcode')
    if type(limit_type) is not int or not isinstance(currency, str):
        raise ValueError('missing or mistyped limit identity')
    return FuturesLimitKey(identifier(raw.get('firmid')), identifier(raw.get('trdaccid')), limit_type, currency)


def _inventory(account_rows: list[dict[str, Any]], limit_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    accounts: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    invalid_account = False
    for account in _table(account_rows):
        try:
            key = (identifier(account.get('firmid')), identifier(account.get('trdaccid')))
            accounts[key].append(account)
        except ValueError:
            invalid_account = True
    records: list[dict[str, Any]] = []
    for raw in _table(limit_rows):
        issues: list[str] = []
        limit_key = None
        try:
            limit_key = _key(raw)
        except ValueError:
            issues.append('invalid_identity')
        if limit_key is not None:
            if limit_key.limit_type not in LIMIT_TYPES:
                issues.append('unsupported_limit_type')
            matched = accounts.get((limit_key.firmid, limit_key.trdaccid), [])
            if len(matched) != 1:
                issues.append('account.not_unique')
            else:
                classes = matched[0].get('class_codes')
                if not isinstance(classes, str) or 'SPBFUT' not in classes.split('|'):
                    issues.append('account.no_spbfut')
        values: dict[str, Any] = {}
        for field in REQUIRED_FIELDS + OPTIONAL_FIELDS:
            values[field] = None
            if field not in raw and field in OPTIONAL_FIELDS:
                continue
            try:
                values[field] = _money(raw.get(field))
            except ValueError:
                issues.append(field + '.invalid_value')
        values['risk_level'] = None
        if 'risk_level' in raw:
            risk = raw['risk_level']
            if type(risk) is int and 0 <= risk <= 4:
                values['risk_level'] = risk
            else:
                issues.append('risk_level.invalid_value')
        records.append({'key': asdict(limit_key) if limit_key is not None else None,
                        'limit_type_name': LIMIT_TYPES.get(limit_key.limit_type, 'unknown') if limit_key else 'unknown',
                        'currency': CURRENCIES.get(limit_key.currcode) if limit_key else None,
                        'values': values, 'issues': issues})
    counts = Counter(canonical(row['key']) for row in records if row['key'] is not None)
    for row in records:
        if row['key'] is not None and counts[canonical(row['key'])] != 1:
            row['issues'].append('duplicate_key')
    return sorted(records, key=canonical), invalid_account


def _same_table(first: list[dict[str, Any]], second: list[dict[str, Any]]) -> bool:
    try:
        return sorted(map(canonical, first)) == sorted(map(canonical, second))
    except (ValueError, TypeError):
        return False


def build_limits_report(first_accounts: list[dict[str, Any]], first_limits: list[dict[str, Any]],
                        second_accounts: list[dict[str, Any]], second_limits: list[dict[str, Any]], *,
                        selection: FuturesLimitKey | None = None,
                        snapshot_issues: tuple[str, ...] = ()) -> dict[str, Any]:
    if selection is not None and not isinstance(selection, FuturesLimitKey):
        raise ValueError('selection requires a validated full futures limit key')
    before, first_invalid = _inventory(first_accounts, first_limits)
    after, second_invalid = _inventory(second_accounts, second_limits)
    issues: list[str] = []
    if not before or not after:
        issues.append('limits.empty')
    if first_invalid or second_invalid:
        issues.append('accounts.invalid_identity')
    if any(row['issues'] for row in before + after):
        issues.append('limits.invalid_rows')
    complete = not issues
    if not _same_table(first_limits, second_limits):
        issues.append('limits_changed')
    if not _same_table(first_accounts, second_accounts):
        issues.append('accounts_changed')
    if snapshot_issues:
        issues.append('snapshot_unverified')
    selected = None
    key = asdict(selection) if selection is not None else None
    if selection is not None:
        first_match = [row for row in before if row['key'] == key]
        second_match = [row for row in after if row['key'] == key]
        if len(first_match) != 1 or len(second_match) != 1:
            issues.append('selection.not_unique')
        if selection.limit_type != 0 or selection.currcode not in CURRENCIES:
            issues.append('selection.unsupported_policy')
        if not issues:
            selected = deepcopy(second_match[0])
    return {'schema': 1, 'rows': after, 'previous_rows': before, 'selection': key,
            'selected_observation': selected, 'selection_verified': selected is not None,
            'complete': complete, 'repeatable_observation': not issues,
            'issues': issues, 'snapshot_issues': list(snapshot_issues),
            'eligible_for_capital': False, 'eligible_for_trading': False, 'reconciled': False,
            'limitation': 'Exact limit observations are not equity, available capital, an OMS cutover or a deficit action.'}

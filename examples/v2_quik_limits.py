"""Invented futures limit observations: exact identity, no automatic capital mapping."""

from copy import deepcopy
from typing import Any

from trading_system.quik_limits import FuturesLimitKey, build_limits_report


def main() -> None:
    accounts: list[dict[str, Any]] = [
        {'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'class_codes': '|SPBFUT|SPBOPT|'}]
    cash: dict[str, Any] = {'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'limit_type': 0, 'currcode': 'SUR',
        'cbplimit': 100000.25, 'cbplused': 15000.5, 'cbplplanned': 84999.75,
        'cbplused_for_orders': 5000.25, 'cbplused_for_positions': 10000.25,
        'varmargin': -12.5, 'real_varmargin': 25.5, 'kgo': 1}
    rows = [cash, {**cash, 'limit_type': 3, 'currcode': '', 'cbplimit': 0},
            {**cash, 'limit_type': 6, 'cbplimit': -9000},
            {**cash, 'limit_type': 7, 'currcode': 'RUB', 'cbplimit': -9000}]
    discovery = build_limits_report(accounts, rows, accounts, deepcopy(rows))
    assert discovery['repeatable_observation'] and not discovery['selection_verified']
    assert len(discovery['rows']) == 4
    assert any(row['values']['cbplimit'] == '-9000' for row in discovery['rows'])
    print('Four synthetic limit rows stay separate; discovery does not choose an account or add balances.')

    key = FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR')
    selected = build_limits_report(accounts, rows, accounts, rows, selection=key)
    assert selected['selection_verified']
    assert selected['selected_observation']['values']['cbplimit'] == '100000.25'
    assert not selected['eligible_for_capital'] and not selected['eligible_for_trading']
    print('Exact type-0 SUR observation verified; it creates no available-capital value or trading permission.')

    duplicate = rows + [deepcopy(cash)]
    blocked = build_limits_report(accounts, duplicate, accounts, duplicate, selection=key)
    assert not blocked['selection_verified'] and blocked['selected_observation'] is None
    print('Duplicate identity blocks selection, even when both amounts agree; no fallback is attempted.')


if __name__ == '__main__':
    main()

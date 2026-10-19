"""Synthetic, offline resource assessment. Does not connect to any terminal."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from trading_system.quik_capital import (
    FuturesResourcePolicy, LocalMarginReservation, ReservationState,
    assess_futures_resource,
)
from trading_system.quik_limits import FuturesLimitKey, build_limits_report


def main() -> None:
    now = datetime(2026, 10, 19, 12, tzinfo=UTC)
    key = FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR')
    policy = FuturesResourcePolicy('demo-quik', '13.2.0.1', key, now.date(), timedelta(seconds=30))
    accounts: list[dict[str, Any]] = [
        {'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'class_codes': '|SPBFUT|'}]
    limits: list[dict[str, Any]] = [
        {'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'limit_type': 0, 'currcode': 'SUR',
         'cbplimit': '100000.25', 'cbplused': '15000.5', 'cbplplanned': '84999.75',
         'cbplused_for_orders': '5000.25', 'cbplused_for_positions': '10000.25',
         'varmargin': '-12.5', 'real_varmargin': '25.5', 'kgo': '1'}]
    tables: dict[str, Any] = {'accounts': accounts, 'limits': limits,
        'orders': [{'order_num': 501, 'trans_id': 1, 'qty': 4, 'balance': 2}],
        'trades': [{'trade_num': 901, 'order_num': 501, 'qty': 2}],
        'holdings': [{'firmid': 'DEMO', 'trdaccid': 'FUTURES', 'sec_code': 'TEST', 'totalnet': 2}]}
    snapshot: dict[str, Any] = {'schema': 1, 'mode': 'read_only',
        'runtime': {'protocol': 1, 'mode': 'read_only', 'profile': 'demo-quik',
                    'quik_version': '13.2.0.1', 'lua_version': 'Lua 5.4'},
        'connection_id': 'offline-example', 'started_at': (now - timedelta(seconds=2)).isoformat(),
        'finished_at': (now - timedelta(seconds=1)).isoformat(),
        'trading_date_before': '19.10.2026', 'trading_date_after': '19.10.2026',
        'classes': ['SPBFUT'], 'tables': tables, 'previous_tables': deepcopy(tables),
        'limits': build_limits_report(accounts, limits, accounts, limits, selection=key),
        'financial': None, 'issues': [], 'repeatable_observation': True,
        'callbacks_before': 0, 'callbacks_after': 0, 'reconciled': False}

    observed = assess_futures_resource(snapshot, policy, now=now, reservations=())
    assert observed.complete and observed.broker_free == Decimal('84999.75')
    assert observed.candidate_free == observed.broker_free
    print(f'Broker-reported free funds: {observed.broker_free} RUB. Broker usage is not deducted again.')

    pending = LocalMarginReservation('intent-2', key, 'RUB', Decimal('1000.25'), ReservationState.LOCAL_ONLY)
    local = assess_futures_resource(snapshot, policy, now=now, reservations=(pending,))
    assert local.candidate_free == Decimal('83999.50')
    print(f'With explicitly unsubmitted local margin: {local.candidate_free} RUB (assessment only).')

    submitted = replace(pending, state=ReservationState.SUBMITTED_UNRECONCILED)
    blocked = assess_futures_resource(snapshot, policy, now=now, reservations=(submitted,))
    assert blocked.candidate_free is None and 'reservations.cutover_required' in blocked.issues
    print('Possible submission / partial fill / unconfirmed cancel requires an independent cutover.')

    expired = assess_futures_resource(snapshot, policy, now=now + timedelta(seconds=30), reservations=())
    assert expired.candidate_free is None and 'snapshot.expired' in expired.issues
    for result in (observed, local, blocked, expired):
        assert not result.eligible_for_capital and not result.eligible_for_trading and not result.reconciled
    print('Expired observations are rejected. Every result remains read-only; OMS and weights are unchanged.')


if __name__ == '__main__':
    main()

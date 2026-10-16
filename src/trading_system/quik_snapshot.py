"""Independent terminal observations, deliberately NOT an OMS reconciliation command."""

from datetime import UTC, datetime
from typing import Any

from .persistence import canonical
from .quik_contract import decimal, identifier, integer
from .quik_parameters import PARAMETER_CAPABILITY, PARAMETER_NAMES, build_financial_report
from .quik_limits import FuturesLimitKey, build_limits_report
from .quik_readonly import ReadOnlyQuik

TABLES = {'accounts': 'getTradeAccounts', 'orders': 'get_orders', 'trades': 'get_trades',
          'holdings': 'getFuturesClientHoldings', 'limits': 'getFuturesClientLimits'}


class SnapshotError(ValueError):
    pass


def codes(value: object) -> list[str]:
    if not isinstance(value, str):
        raise SnapshotError('terminal code list is missing or not a string')
    result = value.removesuffix(',').split(',') if value else []
    try:
        for code in result:
            identifier(code)
    except ValueError as error:
        raise SnapshotError('terminal code list contains invalid identifiers') from error
    if len(result) != len(set(result)):
        raise SnapshotError('duplicate terminal code')
    return result


def rows(value: Any, name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 50000 or any(not isinstance(r, dict) for r in value):
        raise SnapshotError(name + ' must be an explicit bounded list of records, not null')
    seen: set[int] = set()
    try:
        for row in value:
            if name in ('orders', 'trades'):
                key = integer(row.get('order_num' if name == 'orders' else 'trade_num'))
                if key in seen:
                    raise ValueError('duplicate broker identity')
                seen.add(key)
                integer(row.get('trans_id'), zero=True) if name == 'orders' else integer(row.get('order_num'))
    except ValueError as error:
        raise SnapshotError(name + ': invalid or duplicate broker identifiers') from error
    return sorted(value, key=canonical)


async def _connected(client: ReadOnlyQuik) -> None:
    value = await client.request('isConnected')
    if type(value) is not int or value != 1 or not client.healthy:
        raise SnapshotError('terminal disconnected during snapshot')


async def _date(client: ReadOnlyQuik) -> str:
    value = await client.request('getInfoParam', 'TRADEDATE')
    if not isinstance(value, str):
        raise SnapshotError('trading date is unavailable')
    try:
        datetime.strptime(value, '%d.%m.%Y')
    except ValueError as error:
        raise SnapshotError('unexpected trading date format') from error
    return value


async def capture_snapshot(client: ReadOnlyQuik, *, class_code: str | None = None,
                           symbol: str | None = None, include_financial: bool = False,
                           include_limits: bool = False,
                           limit_selection: FuturesLimitKey | None = None) -> dict[str, Any]:
    if not client.healthy:
        raise SnapshotError('connect and validate the read-only terminal first')
    if symbol is not None and class_code is None:
        raise SnapshotError('instrument lookup requires an explicit class')
    if limit_selection is not None and (not include_limits or not isinstance(limit_selection, FuturesLimitKey)):
        raise SnapshotError('limit selection requires limits mode and a validated full key')
    if include_financial:
        if class_code != 'SPBFUT' or symbol is None:
            raise SnapshotError('financial parameters require an explicit SPBFUT instrument')
        capabilities = client.runtime_info.get('capabilities')
        if not isinstance(capabilities, list) or PARAMETER_CAPABILITY not in capabilities:
            raise SnapshotError('financial parameters unavailable in this bridge; '
                                'update and restart TradingSystemReadOnly.lua')
    for value in (class_code, symbol):
        if value is not None:
            identifier(value)
    started = datetime.now(UTC).isoformat()
    callbacks_before = client.callback_count
    await _connected(client)
    date_before = await _date(client)
    classes = codes(await client.request('getClassesList'))
    instruments: list[str] | None = None
    security: dict[str, Any] | None = None
    if class_code is not None:
        if class_code not in classes:
            raise SnapshotError('requested class is not offered by this terminal; inspect available classes first')
        instruments = codes(await client.request('getClassSecurities', class_code))
    if symbol is not None:
        if instruments is None or symbol not in instruments:
            raise SnapshotError('requested instrument is not offered in this class')
        security = await _security(client, class_code, symbol)
    passes: list[dict[str, list[dict[str, Any]]]] = []
    parameter_reads: list[dict[str, Any]] = []
    security_reads: list[dict[str, Any]] = []
    for index in range(2):
        if include_financial:
            assert class_code is not None and symbol is not None and security is not None
            if index:
                security = await _security(client, class_code, symbol)
            security_reads.append(security)
            parameters = {}
            for name in PARAMETER_NAMES:
                parameters[name] = await client.request('getParamEx', f'{class_code}|{symbol}|{name}')
            parameter_reads.append(parameters)
        observed = {}
        for name, command in TABLES.items():
            observed[name] = rows(await client.request(command), name)
        passes.append(observed)
    date_after = await _date(client)
    await _connected(client)
    issues = []
    if canonical(passes[0]) != canonical(passes[1]):
        issues.append('tables_changed')
    if date_before != date_after:
        issues.append('trading_date_changed')
    if client.callback_count != callbacks_before:
        issues.append('callbacks_during_scan')
    financial = None
    if include_financial:
        financial = build_financial_report(security_reads[0], parameter_reads[0],
                                           security_reads[1], parameter_reads[1])
        if issues:
            financial['point_value_candidate'] = None
            financial['repeatable_observation'] = False
            financial['issues'].append('snapshot_unstable')
        if not financial['complete']:
            issues.append('financial_parameters_incomplete')
        elif not financial['repeatable_observation']:
            issues.append('financial_parameters_unverified')
    limits = None
    if include_limits:
        limits = build_limits_report(passes[0]['accounts'], passes[0]['limits'],
                                     passes[1]['accounts'], passes[1]['limits'],
                                     selection=limit_selection, snapshot_issues=tuple(issues))
        if not limits['repeatable_observation']:
            issues.append('limits_unverified')
            if financial is not None:
                financial['point_value_candidate'] = None
                financial['repeatable_observation'] = False
                if 'snapshot_unstable' not in financial['issues']:
                    financial['issues'].append('snapshot_unstable')
    return {'schema': 1, 'mode': 'read_only', 'runtime': client.runtime_info,
            'connection_id': client.connection_id, 'started_at': started, 'finished_at': datetime.now(UTC).isoformat(),
            'trading_date_before': date_before, 'trading_date_after': date_after,
            'classes': classes, 'class_code': class_code, 'instruments': instruments, 'security': security,
            'financial': financial, 'limits': limits,
            'tables': passes[-1], 'previous_tables': passes[0], 'issues': issues,
            'callbacks_before': callbacks_before, 'callbacks_after': client.callback_count,
            'repeatable_observation': not issues, 'reconciled': False,
            'limitation': 'Two reads are not an atomic broker cutover and never authorize trading or capital changes.'}


async def _security(client: ReadOnlyQuik, class_code: str | None, symbol: str) -> dict[str, Any]:
    raw = await client.request('getSecurityInfo', f'{class_code}|{symbol}')
    if (not isinstance(raw, dict) or raw.get('code') != symbol or raw.get('class_code') != class_code
            or raw.get('sec_code', symbol) != symbol):
        raise SnapshotError('instrument metadata is missing or has a different identity')
    try:
        if decimal(raw.get('min_price_step')) <= 0 or decimal(raw.get('lot_size')) <= 0:
            raise ValueError('invalid tick or lot size')
    except ValueError as error:
        raise SnapshotError('instrument tick/lot metadata is incomplete') from error
    return raw

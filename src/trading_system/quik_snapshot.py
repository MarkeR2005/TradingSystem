"""Independent terminal observations, deliberately NOT an OMS reconciliation command."""

from datetime import UTC, datetime
from typing import Any

from .persistence import canonical
from .quik_contract import decimal, identifier, integer
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
                           symbol: str | None = None) -> dict[str, Any]:
    if not client.healthy:
        raise SnapshotError('connect and validate the read-only terminal first')
    if symbol is not None and class_code is None:
        raise SnapshotError('instrument lookup requires an explicit class')
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
        raw = await client.request('getSecurityInfo', f'{class_code}|{symbol}')
        if not isinstance(raw, dict) or raw.get('code') != symbol or raw.get('class_code') != class_code:
            raise SnapshotError('instrument metadata is missing or has a different identity')
        try:
            if decimal(raw.get('min_price_step')) <= 0 or decimal(raw.get('lot_size')) <= 0:
                raise ValueError('invalid tick or lot size')
        except ValueError as error:
            raise SnapshotError('instrument tick/lot metadata is incomplete') from error
        security = raw
    passes: list[dict[str, list[dict[str, Any]]]] = []
    for _ in range(2):
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
    return {'schema': 1, 'mode': 'read_only', 'runtime': client.runtime_info,
            'connection_id': client.connection_id, 'started_at': started, 'finished_at': datetime.now(UTC).isoformat(),
            'trading_date_before': date_before, 'trading_date_after': date_after,
            'classes': classes, 'class_code': class_code, 'instruments': instruments, 'security': security,
            'tables': passes[-1], 'previous_tables': passes[0], 'issues': issues,
            'callbacks_before': callbacks_before, 'callbacks_after': client.callback_count,
            'repeatable_observation': not issues, 'reconciled': False,
            'limitation': 'Two reads are not an atomic broker cutover and never authorize trading or capital changes.'}

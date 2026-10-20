"""Pure difference reports over synthetic QUIK and OMS data; no terminal or I/O."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from trading_system.domain import Execution, Instrument, InstrumentId, OrderIntent, OrderSnapshot, OrderStatus, Position, Route, Side
from trading_system.inventory import OmsObservation
from trading_system.quik_compare import BrokerLink, ComparisonPolicy, ObservedRoute, QuikBindings, compare_quik


def main() -> None:
    start = datetime(2026, 10, 20, 10, tzinfo=UTC)
    now = start + timedelta(seconds=2)
    route = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'DEMO')
    spec = ObservedRoute('DEMO-FIRM', route, 2, Decimal('0.5'), ('broker_comission',))
    policy = ComparisonPolicy('demo-quik', '13.2.0.31', 'quik', 'example-session', start.date(),
                              timedelta(hours=3), timedelta(seconds=30), (spec,))
    local_order = OrderSnapshot('order-1', 'alpha', OrderIntent(route, Side.BUY, 4, 100), start,
                                OrderStatus.PARTIALLY_FILLED, 1, 100, 1.25)
    fill = Execution('example-session:902', 'order-1', route, Side.BUY, 1, 100, 1.25, start)
    oms = OmsObservation(7, 'quik', now, (local_order,), (fill,),
                         (('alpha', Position(route, 3, 90)), ('beta', Position(route, -2, 120))), ())
    bindings = QuikBindings('quik', 'example-session', 4,
        (BrokerLink(route, 901, 'order-1'),), (BrokerLink(route, 902, fill.execution_id),))
    cursor = dict(bridge_session='a' * 32, sequence=5, failed_sends=0)
    tables = dict(accounts=[dict(firmid='DEMO-FIRM', trdaccid='DEMO', class_codes='|SPBFUT|')],
        orders=[dict(firmid='DEMO-FIRM', account='DEMO', class_code='SPBFUT', sec_code='TEST',
                     order_num=901, trans_id=1, flags=9, qty=4, balance=3, price=100)],
        trades=[dict(firmid='DEMO-FIRM', account='DEMO', class_code='SPBFUT', sec_code='TEST',
                     trade_num=902, order_num=901, flags=0, qty=1, price=100, broker_comission='1.25',
                     datetime=dict(year=2026, month=10, day=20, hour=13, min=0, sec=0))],
        holdings=[dict(firmid='DEMO-FIRM', trdaccid='DEMO', sec_code='TEST', type=2, totalnet=1)], limits=[])
    raw: dict[str, Any] = dict(schema=1, mode='read_only', runtime=dict(protocol=1, mode='read_only',
        profile='demo-quik', quik_version='13.2.0.31', lua_version='Lua 5.4', callback_cursor=cursor),
        connection_id='b' * 32, started_at=start.isoformat(), finished_at=(start + timedelta(seconds=1)).isoformat(),
        trading_date_before='20.10.2026', trading_date_after='20.10.2026', tables=tables, previous_tables=deepcopy(tables),
        issues=[], repeatable_observation=True, reconciled=False, callbacks_before=0, callbacks_after=0,
        callback_boundary=dict(schema=1, kind='transport_cursor', scope='after_connection_baseline',
                               begin=cursor, end=cursor, delivery_verified=True, quiet=True, broker_atomic=False))
    matched = compare_quik(raw, policy, oms, bindings, now=now, current_revision=7)
    assert matched.consistent and not matched.can_apply and not matched.reconciled
    print('Observed net 1 matches owners +3 and -2; ownership is unchanged.')
    repeated = deepcopy(raw)
    repeated['tables']['trades'].append(deepcopy(repeated['tables']['trades'][0]))
    repeated['previous_tables'] = deepcopy(repeated['tables'])
    checked = compare_quik(repeated, policy, oms, bindings, now=now, current_revision=7)
    assert checked.consistent and checked.observation.duplicate_trades == 1
    print('A repeated trade is represented once; no execution is applied.')
    changed = deepcopy(raw)
    changed['tables']['holdings'][0]['totalnet'] = 3
    changed['tables']['orders'] = []
    changed['previous_tables'] = deepcopy(changed['tables'])
    pending = replace(oms, orders=(replace(local_order, status=OrderStatus.CANCEL_PENDING),))
    report = compare_quik(changed, policy, pending, bindings, now=now, current_revision=7)
    codes = {d.code for d in report.differences}
    assert {'position_mismatch', 'order_not_observed', 'order_outcome_unresolved'} <= codes
    assert not report.consistent and not report.eligible_for_trading
    assert oms.revision == 7 and oms.positions[0][1].quantity == 3
    print('Missing order, pending cancel and net mismatch are reported without releasing obligations.')
    print('All reports retain unverified broker atomicity/history and cannot authorize reconciliation.')


if __name__ == '__main__':
    main()

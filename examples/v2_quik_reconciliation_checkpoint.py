"""A saved local comparison expires on new input and survives restart only as history; no terminal."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from trading_system.domain import Instrument, InstrumentId, Route
from trading_system.inventory import MANUAL, ExternalOrderOrigin, ExternalPositionBasis, InventoryProvenance, OpeningBalance
from trading_system.persistence import DurableJournal
from trading_system.quik_compare import ComparisonPolicy, ObservedRoute
from trading_system.quik_contract import QuikRoute
from trading_system.quik_durable import QuikDurableGateway
from trading_system.quik_inventory import build_external_inventory_plan
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
ROUTE = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
SPEC = QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',))
POLICY = ComparisonPolicy('demo-quik', '13.2.0.31', 'quik', 'synthetic-session', START.date(),
    timedelta(hours=3), timedelta(seconds=30),
    (ObservedRoute('DEMO', ROUTE, 2, Decimal('0.5'), ('broker_comission',)),))


def snapshot(start: int, end: int) -> dict[str, Any]:
    cursor = dict(bridge_session='a'*32, sequence=5, failed_sends=0)
    tables = dict(accounts=[dict(firmid='DEMO', trdaccid='demo', class_codes='|SPBFUT|')],
        orders=[dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
            order_num=101, trans_id=0, flags=9, qty=4, balance=3, price=100)],
        trades=[dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
            order_num=101, trade_num=201, flags=0, qty=1, price=100, broker_comission='1.25',
            datetime=dict(year=2026, month=1, day=1, hour=13, min=0, sec=0))],
        limits=[], holdings=[dict(firmid='DEMO', trdaccid='demo', sec_code='TEST', type=2, totalnet=1)])
    return dict(schema=1, mode='read_only', runtime=dict(protocol=1, mode='read_only', profile='demo-quik',
        quik_version='13.2.0.31', lua_version='Lua 5.4', callback_cursor=cursor), connection_id='b'*32,
        started_at=(START+timedelta(seconds=start)).isoformat(), finished_at=(START+timedelta(seconds=end)).isoformat(),
        trading_date_before='01.01.2026', trading_date_after='01.01.2026', tables=tables,
        previous_tables=deepcopy(tables), issues=[], repeatable_observation=True, reconciled=False,
        callbacks_before=0, callbacks_after=0,
        callback_boundary=dict(schema=1, kind='transport_cursor', scope='after_connection_baseline',
            begin=cursor, end=cursor, delivery_verified=True, quiet=True, broker_atomic=False))


async def main() -> None:
    clock = ManualClock(START+timedelta(seconds=2))
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for attempt in range(2):
            with DurableJournal(root/'oms') as oms, DurableJournal(root/'quik') as journal:
                async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                    gateway = await QuikDurableGateway.open('quik', (SPEC,), clock, journal=journal,
                        session_id='synthetic-session', timezone_offset=timedelta(hours=3))
                    runtime.add_gateway(gateway)
                    await gateway.recover(runtime.orders)
                    await runtime.drain()
                    if attempt == 0:
                        raw = snapshot(0, 1)
                        provenance = InventoryProvenance(MANUAL, START+timedelta(seconds=1),
                            'synthetic-statement', 'a'*64)
                        review = build_external_inventory_plan('review', raw, POLICY,
                            runtime.orders.observe_gateway('quik'), gateway.order_evidence,
                            position_basis=(ExternalPositionBasis(OpeningBalance(ROUTE, 1, 95), provenance),),
                            order_origins=(ExternalOrderOrigin(ROUTE, 101, START, provenance),),
                            now=clock.now(), current_revision=runtime.orders.revision)
                        await gateway.stage_external_inventory(review, runtime.orders, raw, POLICY)
                        adoption = gateway.inventory_adoption_command('adopt', runtime.orders, 'review')
                        await gateway.adopt_external_inventory(adoption, runtime.orders, raw, POLICY)
                        await runtime.drain()
                        clock.advance_to(START+timedelta(seconds=7))
                        raw = snapshot(5, 6)
                        checkpoint = gateway.reconciliation_checkpoint('check', runtime.orders, raw, POLICY)
                        await gateway.record_reconciliation(checkpoint, runtime.orders, raw, POLICY)
                        status = gateway.reconciliation_status('check', runtime.orders)
                        assert status.current and not status.eligible_for_trading
                        print('Local comparison saved: 1 order, 1 historical execution, net +1. Trading blocked.')
                        print('Remaining broker evidence:', ', '.join(status.blockers))
                        trade = deepcopy(raw['tables']['trades'][0])
                        trade.update(trade_num=202, broker_comission='0.75')
                        trade['datetime']['sec'] = 7
                        await gateway.feed('OnTrade', trade)
                        await runtime.drain()
                        assert not gateway.reconciliation_status('check', runtime.orders).current
                        clock.advance_to(START+timedelta(seconds=10))
                        raw = snapshot(8, 9)
                        raw['tables']['orders'][0]['balance'] = 2
                        raw['tables']['holdings'][0]['totalnet'] = 2
                        raw['tables']['trades'].append(trade)
                        raw['previous_tables'] = deepcopy(raw['tables'])
                        checkpoint = gateway.reconciliation_checkpoint('after-fill', runtime.orders, raw, POLICY)
                        await gateway.record_reconciliation(checkpoint, runtime.orders, raw, POLICY)
                        assert gateway.reconciliation_status('after-fill', runtime.orders).current
                        print('New fill invalidates the old receipt. A fresh observation confirms net +2.')
                    else:
                        checkpoint = gateway.reconciliation_checkpoints[-1]
                        before = journal.record_count
                        await gateway.record_reconciliation(checkpoint, runtime.orders, {}, POLICY)
                        assert journal.record_count == before
                        assert not gateway.reconciliation_status('after-fill', runtime.orders).current
                        print('Restart preserves both receipts as history; retry does not renew their validity.')
                    position = runtime.ledger.position(MANUAL, ROUTE)
                    assert (position.quantity, position.average_price, position.realized_pnl) == (2, 97.5, -.75)
                    assert not runtime.orders.ready and gateway.blocked
                    assert gateway.transactions == () and gateway.prepared == ()
        print('No broker reconciliation, trading permission, or live transactions were generated.')


if __name__ == '__main__':
    asyncio.run(main())

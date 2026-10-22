"""Adopt a declared inventory baseline across two journals, crash, resume and deduplicate; offline."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from trading_system.domain import Instrument, InstrumentId, Route
from trading_system.inventory import (MANUAL, ExternalOrderOrigin, ExternalPositionBasis, InventoryProvenance,
                                      OpeningBalance, AdoptExternalInventory)
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_compare import ComparisonPolicy, ObservedRoute
from trading_system.quik_contract import QuikRoute
from trading_system.quik_durable import QuikDurableGateway
from trading_system.quik_inventory import build_external_inventory_plan
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
END = START + timedelta(seconds=1)
ROUTE = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
BROKER_ID = 9007199254740993


def snapshot() -> dict[str, Any]:
    cursor = dict(bridge_session='a'*32, sequence=5, failed_sends=0)
    tables = dict(accounts=[dict(firmid='DEMO', trdaccid='demo', class_codes='|SPBFUT|')],
        orders=[dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
            order_num=BROKER_ID, trans_id=0, flags=9, qty=4, balance=3, price=100)],
        trades=[dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
            order_num=BROKER_ID, trade_num=9007199254740995, flags=0, qty=1, price=100,
            broker_comission='1.25', datetime=dict(year=2026, month=1, day=1, hour=13, min=0, sec=0))],
        limits=[], holdings=[dict(firmid='DEMO', trdaccid='demo', sec_code='TEST', type=2, totalnet=1)])
    return dict(schema=1, mode='read_only', runtime=dict(protocol=1, mode='read_only', profile='demo-quik',
        quik_version='13.2.0.31', lua_version='Lua 5.4', callback_cursor=cursor), connection_id='b'*32,
        started_at=START.isoformat(), finished_at=END.isoformat(),
        trading_date_before='01.01.2026', trading_date_after='01.01.2026', tables=tables,
        previous_tables=deepcopy(tables), issues=[], repeatable_observation=True, reconciled=False,
        callbacks_before=0, callbacks_after=0,
        callback_boundary=dict(schema=1, kind='transport_cursor', scope='after_connection_baseline',
            begin=cursor, end=cursor, delivery_verified=True, quiet=True, broker_atomic=False))


async def main() -> None:
    clock = ManualClock(START+timedelta(seconds=2))
    command: AdoptExternalInventory | None = None
    policy = ComparisonPolicy('demo-quik', '13.2.0.31', 'quik', 'synthetic-session', START.date(),
        timedelta(hours=3), timedelta(seconds=30),
        (ObservedRoute('DEMO', ROUTE, 2, Decimal('0.5'), ('broker_comission',)),))
    statement = b'SYNTHETIC: external residual +1 at 95; order submitted 2026-01-01T10:00:00Z'
    provenance = InventoryProvenance(MANUAL, END, 'synthetic-statement', sha256(statement).hexdigest())
    raw = snapshot()
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for attempt in range(3):
            with DurableJournal(root/'oms') as oms, DurableJournal(root/'quik') as journal:
                async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                    gateway = await QuikDurableGateway.open('quik',
                        (QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',)),), clock, journal=journal,
                        session_id='synthetic-session', timezone_offset=timedelta(hours=3))
                    runtime.add_gateway(gateway)
                    # After adoption, financial redelivery requires the matching OMS decisions.
                    await gateway.recover(runtime.orders)
                    await runtime.drain()
                    if attempt == 0:
                        review = build_external_inventory_plan('review', raw, policy,
                            runtime.orders.observe_gateway('quik'), gateway.order_evidence,
                            position_basis=(ExternalPositionBasis(OpeningBalance(ROUTE, 1, 95), provenance),),
                            order_origins=(ExternalOrderOrigin(ROUTE, BROKER_ID, START, provenance),),
                            now=clock.now(), current_revision=runtime.orders.revision)
                        await gateway.stage_external_inventory(review, runtime.orders, raw, policy)
                        command = gateway.inventory_adoption_command('adopt', runtime.orders, 'review')
                        with patch.object(gateway, '_commit', side_effect=JournalError('simulated crash between journals')):
                            try:
                                await gateway.adopt_external_inventory(command, runtime.orders, raw, policy)
                            except JournalError:
                                pass
                            else:
                                raise AssertionError('crash was not reached')
                        await runtime.drain()
                        position = runtime.ledger.position(MANUAL, ROUTE)
                        assert (position.quantity, position.average_price, position.realized_pnl) == (1, 95, 0)
                        assert gateway.broker_orders == ()
                        print('Simulated crash: OMS baseline committed, gateway binding still absent. Trading blocked.')
                    else:
                        command = runtime.orders.external_inventory_adoption('adopt')
                        await gateway.adopt_external_inventory(command, runtime.orders, raw, policy)
                        await runtime.drain()
                        assert len(gateway.broker_orders) == 1
                        await gateway.feed('OnTrade', raw['tables']['trades'][0])
                        await runtime.drain()
                        if attempt == 1:
                            assert runtime.ledger.position(MANUAL, ROUTE).quantity == 1
                            print('Restart: saved decision completes binding; historical fill is not booked twice.')
                            clock.advance_to(START+timedelta(seconds=4))
                            trade = deepcopy(raw['tables']['trades'][0])
                            trade.update(trade_num=9007199254740996, broker_comission='0.75')
                            trade['datetime']['sec'] = 3
                            await gateway.feed('OnTrade', trade)
                            await runtime.drain()
                        position = runtime.ledger.position(MANUAL, ROUTE)
                        assert (position.quantity, position.average_price, position.realized_pnl) == (2, 97.5, -.75)
                        assert runtime.orders.orders(MANUAL)[0].commission == 2
                        print(f'Pass {attempt}: position 2 at 97.5; only new fee 0.75 RUB changes P&L.')
                    assert not runtime.orders.ready and gateway.blocked
                    assert gateway.transactions == () and gateway.prepared == ()
        print('No live transactions. Full broker reconciliation and trading admission remain separate.')


if __name__ == '__main__':
    asyncio.run(main())

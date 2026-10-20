"""Apply a missing committed execution, retry and restart; all data is synthetic."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Execution, Instrument, InstrumentId, Message, OrderIntent, Route, Side
from trading_system.inventory import RepairExecutions
from trading_system.persistence import DurableJournal
from trading_system.quik_compare import ComparisonPolicy, ObservedRoute
from trading_system.quik_contract import QuikRoute
from trading_system.quik_durable import QuikDurableGateway
from trading_system.quik_repair import build_execution_repair
from trading_system.runtime import TradingRuntime
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
ROUTE = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
SPEC = QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',))
ORDER = dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
             order_num=9007199254740993, trans_id=1, flags=9, qty=4, balance=3, price=100)
TRADE = dict(firmid='DEMO', account='demo', class_code='SPBFUT', sec_code='TEST',
    order_num=9007199254740993, trade_num=9007199254740995, flags=0, qty=1, price=100,
    broker_comission='1.25', datetime=dict(year=2026, month=1, day=1, hour=13, min=0, sec=0))


def snapshot() -> dict[str, Any]:
    cursor = dict(bridge_session='a' * 32, sequence=5, failed_sends=0)
    tables = dict(accounts=[dict(firmid='DEMO', trdaccid='demo', class_codes='|SPBFUT|')],
        orders=[ORDER], trades=[TRADE], limits=[],
        holdings=[dict(firmid='DEMO', trdaccid='demo', sec_code='TEST', type=2, totalnet=1)])
    return dict(schema=1, mode='read_only', runtime=dict(protocol=1, mode='read_only', profile='demo-quik',
        quik_version='13.2.0.31', lua_version='Lua 5.4', callback_cursor=cursor), connection_id='b' * 32,
        started_at=START.isoformat(), finished_at=(START + timedelta(seconds=1)).isoformat(),
        trading_date_before='01.01.2026', trading_date_after='01.01.2026',
        tables=tables, previous_tables=deepcopy(tables), issues=[], repeatable_observation=True,
        reconciled=False, callbacks_before=0, callbacks_after=0,
        callback_boundary=dict(schema=1, kind='transport_cursor', scope='after_connection_baseline',
             begin=cursor, end=cursor, delivery_verified=True, quiet=True, broker_atomic=False))


class Strategy(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('example', (ROUTE.instrument.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


async def main() -> None:
    clock = ManualClock(START)
    command: RepairExecutions | None = None
    policy = ComparisonPolicy('demo-quik', '13.2.0.31', 'quik', 'synthetic-session', START.date(),
        timedelta(hours=3), timedelta(seconds=30),
        (ObservedRoute('DEMO', ROUTE, 2, Decimal('0.5'), ('broker_comission',)),))
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for attempt in range(2):
            with DurableJournal(root / 'oms') as oms, DurableJournal(root / 'quik') as journal:
                async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                    gateway = await QuikDurableGateway.open('quik', (SPEC,), clock, journal=journal,
                        session_id='synthetic-session', timezone_offset=timedelta(hours=3))
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(Strategy())
                    await gateway.recover()
                    await runtime.drain()
                    if attempt == 0:
                        account = AccountKey.from_route(ROUTE)
                        await runtime.configure_capital(CapitalPolicy(1, START, START + timedelta(hours=1), 'RUB',
                            (AccountLimit(account, 2000),), (RouteValuation(ROUTE, 150),),
                            (RouteGrant('example', ROUTE),), (StrategyLimit('example', account, 2000),)))
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                        await runtime.drain()
                        await gateway.feed('OnOrder', dict(ORDER, balance=4))
                        await runtime.drain()
                        publish = runtime.bus.publish

                        async def drop_fill(recipient: str, message: Message) -> None:
                            if not isinstance(message, Execution):
                                await publish(recipient, message)

                        # Fault injection: durable source is healthy, but OMS missed its output.
                        with patch.object(runtime.bus, 'publish', side_effect=drop_fill):
                            await gateway.feed('OnTrade', TRADE)
                        await runtime.drain()
                        clock.advance_to(START + timedelta(seconds=2))
                        assert runtime.ledger.positions() == ()
                        command = build_execution_repair('example-repair', snapshot(), policy,
                            runtime.orders.observe_gateway('quik'), gateway.execution_evidence,
                            now=clock.now(), current_revision=runtime.orders.revision)
                        await gateway.apply_execution_repair(command, runtime.orders)
                        await runtime.drain()
                        print('One committed fill repaired: position 1, fee 1.25 RUB, order reserve 450 RUB.')
                    assert command is not None
                    before = (runtime.orders.revision, oms.record_count, journal.record_count)
                    await gateway.apply_execution_repair(command, runtime.orders)
                    await runtime.drain()
                    assert before == (runtime.orders.revision, oms.record_count, journal.record_count)
                    position = runtime.ledger.position('example', ROUTE)
                    assert position.quantity == 1 and position.realized_pnl == -1.25
                    assert context.orders()[0].commission == 1.25
                    assert sum(u.orders for u in runtime.orders.capital_usage()) == 450
                    assert not runtime.orders.ready
                    if attempt:
                        assert gateway.prepared == ()
                        print('Restart and exact retry: no duplicate fill, fee or prepared transaction.')
        print('Trading remains blocked; external inventory and full reconciliation are separate steps.')


if __name__ == '__main__':
    asyncio.run(main())

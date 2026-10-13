"""Recover a durable QUIK callback that never reached OMS; no network or terminal."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Execution, Instrument, InstrumentId, Message, OrderIntent, Route, Side
from trading_system.persistence import DurableJournal
from trading_system.quik_contract import QuikRoute
from trading_system.quik_durable import QuikDurableGateway
from trading_system.runtime import TradingRuntime
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
ROUTE = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
SPEC = QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',))
BROKER_ID = 9007199254740993
TRADE: dict[str, object] = dict(trade_num=9007199254740995, order_num=BROKER_ID,
    account='demo', class_code='SPBFUT', sec_code='TEST', qty=1, price=100, flags=0,
    broker_comission='1.25', datetime=dict(year=2026, month=1, day=1, hour=13, min=0, sec=0))


class Strategy(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('example', (ROUTE.instrument.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


async def main() -> None:
    clock = ManualClock(START)
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for attempt in range(3):
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
                        await gateway.feed('OnOrder', dict(trans_id=1, order_num=BROKER_ID, account='demo',
                            class_code='SPBFUT', sec_code='TEST', qty=4, balance=4, flags=9, price=100))
                        await runtime.drain()
                        publish = runtime.bus.publish

                        async def stop_delivery(recipient: str, message: Message) -> None:
                            if isinstance(message, Execution):
                                raise RuntimeError('simulated interruption before OMS delivery')
                            await publish(recipient, message)

                        with patch.object(runtime.bus, 'publish', side_effect=stop_delivery):
                            try:
                                await gateway.feed('OnTrade', TRADE)
                            except RuntimeError as error:
                                assert str(error) == 'simulated interruption before OMS delivery'
                            else:
                                raise AssertionError('expected simulated delivery failure')
                        await runtime.drain()
                        assert runtime.ledger.positions() == ()
                        print('Before restart: transaction 1 and raw trade saved; OMS has not received the fill.')
                    else:
                        position = runtime.ledger.position('example', ROUTE)
                        assert position.quantity == 1 and position.realized_pnl == -1.25
                        assert context.orders()[0].filled_quantity == 1
                        assert sum(u.orders for u in runtime.orders.capital_usage()) == 450
                        assert gateway.broker_orders == ((BROKER_ID, context.orders()[0].order_id),)
                        assert gateway.next_transaction_id == 2 and gateway.prepared == ()
                        assert not runtime.orders.ready and gateway.blocked
                        await gateway.feed('OnTrade', TRADE)
                        await runtime.drain()
                        assert runtime.ledger.position('example', ROUTE) == position
                        print(f'Restart {attempt}: one lot, fee 1.25 RUB, reserve 450 RUB; no duplicate or resubmission.')
        print('Broker ID and next TRANS_ID retained; execution remains blocked until independent reconciliation.')


if __name__ == '__main__':
    asyncio.run(main())

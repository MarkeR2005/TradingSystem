"""Replay synthetic QUIK callbacks through the real OMS, without a terminal or network."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, OrderStatus, Route, Side
from trading_system.persistence import DurableJournal
from trading_system.quik_contract import QuikRoute
from trading_system.quik_replay import QuikReplayGateway
from trading_system.runtime import TradingRuntime
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
ROUTE = Route(Instrument(InstrumentId('quik', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
SPEC = QuikRoute(ROUTE, Decimal('0.5'), ('broker_comission',))


class Strategy(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('example', (ROUTE.instrument.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


async def replay(runtime: TradingRuntime, gateway: QuikReplayGateway, stage: str) -> None:
    fixture = Path(__file__).parent / 'fixtures' / 'quik_order_flow.json'
    records = json.loads(fixture.read_text(encoding='utf-8'))['events']
    for record in records:
        if record['stage'] == stage:
            await gateway.feed(record['event'], record['data'])
            await runtime.drain()


def gateway(clock: ManualClock) -> QuikReplayGateway:
    return QuikReplayGateway('quik', (SPEC,), clock, session_id='fixture-2026-01-01',
                             timezone_offset=timedelta(hours=3))


async def main() -> None:
    clock = ManualClock(START)
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                bridge = gateway(clock)
                runtime.add_gateway(bridge)
                context = await runtime.start_strategy(Strategy())
                account = AccountKey.from_route(ROUTE)
                await runtime.configure_capital(CapitalPolicy(1, START, START + timedelta(hours=1), 'RUB',
                    (AccountLimit(account, 2000),), (RouteValuation(ROUTE, 150),),
                    (RouteGrant('example', ROUTE),), (StrategyLimit('example', account, 2000),)))
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                await runtime.drain()
                original = context.orders()[0]
                assert original.status is OrderStatus.SUBMITTED
                await bridge.transport_response(1, {'cmd': 'sendTransaction', 'data': True})
                await replay(runtime, bridge, 'submit')
                assert context.orders()[0].filled_quantity == 1
                await context.replace_order(original.order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
                await runtime.drain()
                await replay(runtime, bridge, 'replace')
                assert context.orders()[0].status is OrderStatus.CANCELLED
                assert context.orders()[1].intent.quantity == 2
                await replay(runtime, bridge, 'child')
                assert runtime.ledger.position('example', ROUTE).quantity == 4
                await context.place_order(OrderIntent(ROUTE, Side.SELL, 4, 105))
                await runtime.drain()
                await replay(runtime, bridge, 'exit')
                assert not runtime.ledger.positions()
                assert context.trades()[0].pnl == 18
                print('QUIK fixture: early trade buffered, cancel barrier retained late fill; replacement 2 lots at 99.')
                print('Round trip: 18 RUB after 4 commissions; duplicate trade did not change ledger.')
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))
                await runtime.drain()
                clock.advance_to(START + timedelta(seconds=90))
                await bridge.check_deadlines()
                await runtime.drain()
                assert not runtime.orders.ready and bridge.blocked
                assert context.orders()[-1].status is OrderStatus.SUBMITTED
                assert sum(u.orders for u in runtime.orders.capital_usage()) == 150
                assert len(bridge.transactions) == 5  # Four entries/exits plus KILL, without a retry.
                expected = context.orders()
                print('90-second timeout: pending order and 150 RUB reservation retained; new decisions blocked.')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                bridge = gateway(clock)
                runtime.add_gateway(bridge)
                context = await runtime.start_strategy(Strategy())
                assert context.orders() == expected
                assert context.trades()[0].pnl == 18
                assert not runtime.orders.ready
                assert bridge.transactions == ()
                print('OMS restart: finance and uncertainty restored without resubmission. Broker mapping recovery is a later live-transport gate.')


if __name__ == '__main__':
    asyncio.run(main())

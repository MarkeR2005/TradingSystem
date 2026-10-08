"""Controlled offline round trip: run with PYTHONPATH=src python examples/v2_round_trip.py."""

import asyncio
from datetime import UTC, datetime, timedelta
import json

from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock


class RoundTripStrategy(AutoStrategy):
    def __init__(self, route: Route) -> None:
        super().__init__('demo', (route.instrument.id,))
        self.route = route
        self._bars = 0

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        self._bars += 1
        if self._bars <= 2:
            side = Side.BUY if self._bars == 1 else Side.SELL
            await context.place_order(OrderIntent(self.route, side, 2))


async def main() -> None:
    clock = ManualClock(datetime(2026, 1, 1, 10, tzinfo=UTC))
    instrument = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
    route = Route(instrument, 'demo-account')
    async with TradingRuntime(clock) as runtime:
        gateway = SimulationGateway('sim', clock)
        runtime.add_gateway(gateway)
        context = await runtime.start_strategy(RoundTripStrategy(route))
        for price in (100.0, 110.0):
            bar = Candle(instrument.id, clock.now(), timedelta(minutes=1),
                         price, price + 1, price - 1, price, 10)
            await runtime.feed_candle(bar)
            await runtime.drain()
            # The scenario driver supplies fills; the strategy only knows its context.
            await gateway.fill(context.orders()[-1].order_id, price, commission=1)
            await runtime.drain()
        print(json.dumps({
            'position': context.position(route).quantity,
            'closed_trades': len(context.trades()),
            'closed_pnl_rub': context.trades()[-1].pnl,
            'closed_equity_rub': context.equity()[-1].value,
            'timestamp': context.equity()[-1].timestamp.isoformat(),
        }, indent=2))


if __name__ == '__main__':
    asyncio.run(main())

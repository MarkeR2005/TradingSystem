"""Controlled cancel/fill race followed by a closed trade."""

import asyncio
from datetime import UTC, datetime, timedelta
import json

from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock


class LimitRepriceStrategy(AutoStrategy):
    def __init__(self, route: Route) -> None:
        super().__init__('reprice-demo', (route.instrument.id,))
        self.route = route
        self._bars = 0

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        self._bars += 1
        if self._bars == 1:
            await context.place_order(OrderIntent(self.route, Side.BUY, 5, limit_price=100))
        elif self._bars == 2:
            await context.replace_order(context.orders()[0].order_id,
                                        OrderIntent(self.route, Side.BUY, 5, limit_price=99))
        elif self._bars == 3:
            await context.place_order(OrderIntent(self.route, Side.SELL, 5, limit_price=110))


async def main() -> None:
    clock = ManualClock(datetime(2026, 1, 1, 10, tzinfo=UTC))
    instrument = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
    route = Route(instrument, 'demo-account')
    async with TradingRuntime(clock) as runtime:
        gateway = SimulationGateway('sim', clock, auto_confirm_cancels=False)
        runtime.add_gateway(gateway)
        context = await runtime.start_strategy(LimitRepriceStrategy(route))

        async def bar(price: float) -> None:
            await runtime.feed_candle(Candle(instrument.id, clock.now(), timedelta(minutes=1),
                                             price, price + 1, price - 1, price, 10))
            await runtime.drain()

        await bar(100)
        old_id = context.orders()[0].order_id
        await gateway.fill(old_id, 100, quantity=2, commission=1)
        await runtime.drain()
        await bar(99)
        # One more lot executes while the old order is awaiting cancellation.
        await gateway.fill(old_id, 100, quantity=1, commission=1)
        await gateway.confirm_cancel(old_id)
        await runtime.drain()
        child = context.orders()[1]
        await gateway.fill(child.order_id, 99, commission=1)
        await runtime.drain()
        await bar(110)
        await gateway.fill(context.orders()[2].order_id, 110, commission=1)
        await runtime.drain()
        print(json.dumps({
            'original_filled_quantity': context.orders()[0].filled_quantity,
            'replacement_quantity': child.intent.quantity,
            'order_statuses': [order.status.value for order in context.orders()],
            'position': context.position(route).quantity,
            'closed_pnl_rub': round(context.trades()[-1].pnl, 2),
            'closed_equity_rub': round(context.equity()[-1].value, 2),
        }, indent=2))


if __name__ == '__main__':
    asyncio.run(main())

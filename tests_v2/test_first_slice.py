from datetime import UTC, datetime, timedelta
import unittest

from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock


class RoundTripStrategy(AutoStrategy):
    def __init__(self, name: str, route: Route) -> None:
        super().__init__(name, (route.instrument.id,))
        self.route = route
        self.bars = 0
        self.fill_positions: list[float] = []

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        self.bars += 1
        side = Side.BUY if self.bars == 1 else Side.SELL
        await context.place_order(OrderIntent(self.route, side, 2.0))

    async def on_fill(self, context: StrategyContext, execution: object) -> None:
        self.fill_positions.append(context.position(self.route).quantity)


class FirstSliceTests(unittest.IsolatedAsyncioTestCase):
    async def test_candle_to_order_to_closed_trade(self) -> None:
        clock = ManualClock(datetime(2026, 1, 1, 10, tzinfo=UTC))
        instrument = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
        route = Route(instrument, 'demo')
        strategy = RoundTripStrategy('round_trip', route)
        async with TradingRuntime(clock) as runtime:
            gateway = SimulationGateway('sim', clock)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(strategy)
            first = Candle(instrument.id, clock.now(), timedelta(minutes=1), 100, 101, 99, 100, 10)
            await runtime.feed_candle(first)
            await runtime.drain()
            self.assertEqual(context.position(route).quantity, 0)
            self.assertEqual(len(context.orders()), 1)
            self.assertEqual(context.equity(), ())
            await gateway.fill(context.orders()[0].order_id, 100, commission=1)
            await runtime.drain()
            self.assertEqual(context.position(route).quantity, 2)
            self.assertEqual(context.equity(), ())
            second = Candle(instrument.id, clock.now(), timedelta(minutes=1), 110, 111, 109, 110, 10)
            await runtime.feed_candle(second)
            await runtime.drain()
            await gateway.fill(context.orders()[-1].order_id, 110, commission=1)
            await runtime.drain()
            self.assertEqual(context.position(route).quantity, 0)
            self.assertEqual(strategy.fill_positions, [2, 0])
            self.assertEqual(len(context.trades()), 1)
            self.assertAlmostEqual(context.trades()[0].pnl, 18)
            self.assertEqual(len(context.equity()), 1)
            self.assertAlmostEqual(context.equity()[0].value, 18)
            self.assertEqual(context.equity()[0].timestamp, second.closed_at)


if __name__ == '__main__':
    unittest.main()

"""Deterministic next-bar execution, warm-up and a calendar-supplied gap."""

import asyncio
from datetime import UTC, datetime, timedelta

from trading_system.backtest import BacktestConfig, SingleBacktest
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.market_data import fill_gaps
from trading_system.optimization import HistoryRequest
from trading_system.strategy import AutoStrategy, StrategyContext


START = datetime(2026, 1, 1, 10, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
INSTRUMENT = Instrument(InstrumentId('history', 'SPBFUT', 'TEST'), 'RUB')
ROUTE = Route(INSTRUMENT, 'demo-account')


class RoundTrip(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('historical_round_trip', (INSTRUMENT.id,))
        self.bars = 0
        self.warmed = 0

    async def on_start(self, context: StrategyContext) -> None:
        history = await context.load_history(HistoryRequest(
            self.instruments, START - MINUTE, START, MINUTE))
        self.warmed = len(history)

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        self.bars += 1
        if self.bars == 1:
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2, limit_price=100))
        elif self.bars == 3:
            await context.place_order(OrderIntent(ROUTE, Side.SELL, 2, limit_price=110))


async def main() -> None:
    previous = Candle(INSTRUMENT.id, START - MINUTE, MINUTE, 100, 101, 99, 100, 10)
    first = Candle(INSTRUMENT.id, START, MINUTE, 100, 101, 99, 100, 10)
    entry = Candle(INSTRUMENT.id, START + 2 * MINUTE, MINUTE, 98, 100, 97, 99, 10)
    exit_bar = Candle(INSTRUMENT.id, START + 3 * MINUTE, MINUTE, 112, 113, 110, 112, 10)
    openings = tuple(START + i * MINUTE for i in range(4))
    bars = (previous,) + fill_gaps(INSTRUMENT.id, MINUTE, openings, (first, entry, exit_bar))
    config = BacktestConfig(INSTRUMENT, START, START + 4 * MINUTE, MINUTE, commission_per_lot=0.5)
    backtest = SingleBacktest(config, bars)
    result = await backtest.run(RoundTrip)
    repeated = await backtest.run(RoundTrip)
    assert result == repeated
    assert [execution.price for execution in result.executions] == [100, 110]
    assert result.closed_pnl == 18 and result.pending_orders == ()
    print(f'Bars: {result.processed_bars}; synthetic: {result.synthetic_bars}')
    print(f'Execution prices: {[execution.price for execution in result.executions]}')
    print(f'Position: {result.positions[0].quantity:g}')
    print(f'Closed P&L: {result.closed_pnl:g} RUB; commission: {result.commission:g} RUB')
    print(f'Repeated result identical: {result == repeated}')
    print(f'Data SHA-256: {result.data_digest}')


if __name__ == '__main__':
    asyncio.run(main())

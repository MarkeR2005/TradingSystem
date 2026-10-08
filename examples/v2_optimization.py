"""Strategy-owned optimization, live accounting, checkpoint and verified restart."""

import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.domain import Candle, Execution, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.optimization import HistoryRequest, InMemoryHistory, OptimizationHandle, OptimizationInput
from trading_system.persistence import DurableJournal
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.state import JsonObject
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock


def choose_parameters(data: OptimizationInput) -> JsonObject:
    return {'period': len(data.history) + 1, 'worker_pid': os.getpid()}


class DemoHistory(InMemoryHistory):
    def __init__(self, bars: tuple[Candle, ...]) -> None:
        super().__init__(bars)
        self.release = asyncio.Event()

    async def load(self, request: HistoryRequest) -> tuple[Candle, ...]:
        await self.release.wait()  # Make the demonstration's ordering reproducible.
        return await super().load(request)


class CountingStrategy(AutoStrategy):
    def __init__(self, name: str, instrument_id: InstrumentId) -> None:
        super().__init__(name, (instrument_id,), parameters={'period': 1})
        self.bars = 0
        self.fills = 0

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        self.bars += 1

    async def on_fill(self, context: StrategyContext, execution: Execution) -> None:
        self.fills += 1

    def save_state(self) -> JsonObject:
        return {'bars': self.bars, 'fills': self.fills}

    def restore_state(self, state: JsonObject) -> None:
        bars, fills = state['bars'], state['fills']
        if not isinstance(bars, int) or not isinstance(fills, int):
            raise ValueError('invalid counter checkpoint')
        self.bars, self.fills = bars, fills


class OptimizingStrategy(CountingStrategy):
    def __init__(self, instrument_id: InstrumentId, request: HistoryRequest) -> None:
        super().__init__('optimizing-demo', instrument_id)
        self.request = request
        self.job: OptimizationHandle | None = None

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        await super().on_candle(context, candle)
        if self.bars == 1:
            self.job = await context.start_optimization(self.request, choose_parameters)


async def main() -> None:
    start = datetime(2026, 1, 1, 10, tzinfo=UTC)
    instrument = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
    route = Route(instrument, 'demo-account')

    def bar(opened_at: datetime) -> Candle:
        return Candle(instrument.id, opened_at, timedelta(minutes=1), 100, 101, 99, 100, 10)

    request = HistoryRequest((instrument.id,), start - timedelta(hours=1), start, timedelta(minutes=1))
    history = DemoHistory((bar(start - timedelta(minutes=2)), bar(start - timedelta(minutes=1))))
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(start), journal=journal, history=history) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                runtime.add_gateway(gateway)
                strategy = OptimizingStrategy(instrument.id, request)
                context = await runtime.start_strategy(strategy)
                other = CountingStrategy('other-demo', instrument.id)
                await runtime.start_strategy(other)
                await context.place_order(OrderIntent(route, Side.BUY, 2, 100))
                await runtime.drain()
                await runtime.feed_candle(bar(start))
                await runtime.drain()
                status_during_job = context.status.value
                await runtime.feed_candle(bar(runtime.clock.now()))
                await runtime.drain()
                bars_during_job = strategy.bars
                other_bars = other.bars
                await gateway.fill(context.orders()[0].order_id, 100, commission=1)
                await runtime.drain()
                history.release.set()
                if strategy.job is None:
                    raise RuntimeError('strategy did not start optimization')
                result = await strategy.job.wait()
                separate_process = result['worker_pid'] != os.getpid()
                broker_orders = gateway.order_snapshots()
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(start), journal=journal) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                gateway.restore_orders(broker_orders)
                runtime.add_gateway(gateway)
                strategy = OptimizingStrategy(instrument.id, request)
                context = await runtime.start_strategy(strategy)
                restored_period = strategy.parameters['period']
                restored_fills = strategy.fills
                await runtime.reconcile_gateway('sim', gateway.order_snapshots())
                await context.place_order(OrderIntent(route, Side.SELL, 2, 110))
                await runtime.drain()
                await gateway.fill(context.orders()[-1].order_id, 110, commission=1)
                await runtime.drain()
                print(json.dumps({
                    'status_during_job': status_during_job,
                    'strategy_bars_after_skipped_candle': bars_during_job,
                    'other_strategy_bars': other_bars,
                    'bulk_history_requests': len(history.requests),
                    'separate_worker_process': separate_process,
                    'restored_period': restored_period,
                    'restored_fill_callbacks': restored_fills,
                    'position': context.position(route).quantity,
                    'closed_equity_rub': context.equity()[-1].value,
                }, indent=2))


if __name__ == '__main__':
    asyncio.run(main())

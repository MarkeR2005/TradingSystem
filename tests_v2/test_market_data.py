from dataclasses import replace
from datetime import timedelta
import unittest

from trading_system.domain import CandleCorrected, CandleReceived, OrderIntent, Side
from trading_system.market_data import CandleBook, fill_gaps
from trading_system.optimization import HistoryRequest
from trading_system.events import DeliveryError
from trading_system.lifecycle import StrategyStatus
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy
from trading_system.time import ManualClock
from helpers import START, INSTRUMENT, ROUTE, candle
from optimization_workers import select_period


MINUTE = timedelta(minutes=1)


class RecordingStrategy(AutoStrategy):
    def __init__(self, name='recording', timeframe=None):
        super().__init__(name, (INSTRUMENT.id,))
        self.timeframe = timeframe
        self.bars = []
        self.corrections = []
        self.warmup = ()

    def accepts_candle(self, bar):
        return super().accepts_candle(bar) and (self.timeframe is None or bar.timeframe == self.timeframe)

    async def on_start(self, context):
        self.warmup = await context.load_history(HistoryRequest(
            self.instruments, START - 2 * MINUTE, START, MINUTE))

    async def on_candle(self, context, bar):
        self.bars.append(bar)

    async def on_correction(self, context, previous, corrected):
        self.corrections.append((previous, corrected))


class GapTests(unittest.TestCase):
    def test_previous_close_and_zero_volume_are_explicitly_synthetic(self):
        first = replace(candle(), close=100.5)
        last = candle(START + 3 * MINUTE)
        openings = tuple(START + i * MINUTE for i in range(4))
        bars = fill_gaps(INSTRUMENT.id, MINUTE, openings, (last, first))
        self.assertEqual(bars[0], first)
        self.assertEqual(bars[-1], last)
        for bar in bars[1:3]:
            self.assertEqual((bar.open, bar.high, bar.low, bar.close, bar.volume), (100.5,)*4 + (0,))
            self.assertTrue(bar.synthetic)

    def test_initial_gap_needs_seed_and_never_uses_future_close(self):
        openings = (START, START + MINUTE)
        with self.assertRaisesRegex(ValueError, 'seed'):
            fill_gaps(INSTRUMENT.id, MINUTE, openings, (candle(START + MINUTE),))
        bars = fill_gaps(INSTRUMENT.id, MINUTE, openings, (candle(START + MINUTE),), seed=90)
        self.assertEqual(bars[0].close, 90)
        self.assertTrue(bars[0].synthetic)

    def test_calendar_breaks_do_not_generate_bars(self):
        openings = (START, START + 60 * MINUTE)
        bars = fill_gaps(INSTRUMENT.id, MINUTE, openings, (candle(),))
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[1].opened_at, openings[1])

    def test_rejects_ambiguous_or_overlapping_schedule_and_foreign_bars(self):
        for openings, bars in [((START, START), (candle(),)),
                               ((START + MINUTE, START), ()),
                               ((START, START + MINUTE / 2), ()),
                               ((START,), (candle(), candle())),
                               ((START,), (candle(START + MINUTE),))]:
            with self.subTest(openings=openings, bars=bars), self.assertRaises(ValueError):
                fill_gaps(INSTRUMENT.id, MINUTE, openings, bars, seed=100)

    def test_invalid_seed_and_synthetic_marker_are_rejected(self):
        with self.assertRaises(ValueError):
            fill_gaps(INSTRUMENT.id, MINUTE, (START,), (), seed=float('nan'))
        with self.assertRaises(ValueError):
            replace(candle(), synthetic='yes')


class BookTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_history_dedup_correction_and_future_cutoff(self):
        clock = ManualClock(START + MINUTE)
        book = CandleBook(clock)
        first = candle()
        self.assertEqual(book.put(first), CandleReceived(first))
        self.assertIsNone(book.put(first))
        corrected = replace(first, close=100.5)
        self.assertEqual(book.put(corrected), CandleCorrected(first, corrected))
        request = HistoryRequest((INSTRUMENT.id,), START, START + MINUTE, MINUTE)
        self.assertEqual(await book.load(request), (corrected,))
        with self.assertRaisesRegex(ValueError, 'future'):
            await book.load(replace(request, end=START + 2 * MINUTE))
        with self.assertRaisesRegex(ValueError, 'future'):
            book.put(candle(START + MINUTE))
        self.assertEqual(await book.load(request), (corrected,))

    async def test_late_unknown_bar_is_a_gap_not_an_implicit_correction(self):
        book = CandleBook(ManualClock(START + 3 * MINUTE))
        book.put(candle(START + 2 * MINUTE))
        with self.assertRaisesRegex(ValueError, 'out of order'):
            book.put(candle())

    async def test_timeframes_and_instruments_have_independent_series(self):
        book = CandleBook(ManualClock(START + 5 * MINUTE))
        book.put(candle())
        book.put(replace(candle(), timeframe=5 * MINUTE))
        request = HistoryRequest((INSTRUMENT.id,), START, START + 5 * MINUTE, MINUTE)
        self.assertEqual(await book.load(request), (candle(),))


class MarketRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_warmup_shared_delivery_and_explicit_timeframe_filter(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            previous = candle(START - MINUTE)
            runtime.market.put(previous)
            first, second = RecordingStrategy('one', MINUTE), RecordingStrategy('two', 5 * MINUTE)
            a = await runtime.start_strategy(first)
            await runtime.start_strategy(second)
            self.assertEqual(first.warmup, (previous,))
            await runtime.feed_candle(candle())
            await runtime.drain()
            await runtime.feed_candle(replace(candle(), timeframe=5 * MINUTE))
            await runtime.drain()
            self.assertEqual(len(first.bars), 1)
            self.assertEqual(len(second.bars), 1)
            self.assertEqual(await a.load_history(HistoryRequest(
                (INSTRUMENT.id,), START - MINUTE, runtime.clock.now(), MINUTE)), (previous, candle()))

    async def test_duplicate_and_correction_do_not_repeat_normal_candle(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            strategy = RecordingStrategy()
            await runtime.start_strategy(strategy)
            first = candle()
            await runtime.feed_candle(first)
            await runtime.feed_candle(first)
            await runtime.feed_candle(candle(START + MINUTE))
            corrected = replace(first, close=100.5)
            await runtime.feed_candle(corrected)
            await runtime.drain()
            self.assertEqual(strategy.bars, [first, candle(START + MINUTE)])
            self.assertEqual(strategy.corrections, [(first, corrected)])
            self.assertEqual(runtime.clock.now(), START + 2 * MINUTE)

    async def test_paused_strategy_skips_correction_but_history_updates(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            strategy = RecordingStrategy()
            context = await runtime.start_strategy(strategy)
            await runtime.feed_candle(candle())
            await runtime.pause_strategy(strategy.name)
            corrected = replace(candle(), close=100.5)
            await runtime.feed_candle(corrected)
            await runtime.drain()
            self.assertEqual(strategy.corrections, [])
            self.assertEqual(await context.load_history(HistoryRequest(
                (INSTRUMENT.id,), START, START + MINUTE, MINUTE)), (corrected,))

    async def test_future_history_is_rejected_inside_startup(self):
        class FutureReader(RecordingStrategy):
            async def on_start(self, context):
                await context.load_history(HistoryRequest(self.instruments, START, START + MINUTE, MINUTE))
        async with TradingRuntime(ManualClock(START)) as runtime:
            with self.assertRaisesRegex(ValueError, 'future'):
                await runtime.start_strategy(FutureReader())

    async def test_filter_failure_isolated_like_other_strategy_callbacks(self):
        class Broken(RecordingStrategy):
            def accepts_candle(self, bar):
                raise ValueError('filter failed')
        async with TradingRuntime(ManualClock(START)) as runtime:
            failed = await runtime.start_strategy(Broken('broken'))
            healthy = RecordingStrategy('healthy')
            await runtime.start_strategy(healthy)
            await runtime.feed_candle(candle())
            with self.assertRaises(DeliveryError):
                await runtime.drain()
            self.assertEqual(failed.status, StrategyStatus.FAILED)
            self.assertEqual(healthy.bars, [candle()])

    async def test_invalid_overlap_does_not_advance_clock_or_change_history(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            await runtime.feed_candle(candle())
            with self.assertRaises(ValueError):
                await runtime.feed_candle(candle(START + MINUTE / 2))
            self.assertEqual(runtime.clock.now(), START + MINUTE)
            request = HistoryRequest((INSTRUMENT.id,), START, START + MINUTE, MINUTE)
            self.assertEqual(await runtime.market.load(request), (candle(),))

    async def test_correction_keeps_existing_orders_and_financial_history(self):
        class Buyer(RecordingStrategy):
            async def on_candle(self, context, bar):
                await super().on_candle(context, bar)
                if len(self.bars) == 1:
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = Buyer()
            context = await runtime.start_strategy(strategy)
            await runtime.feed_candle(candle())
            await runtime.drain()
            await gateway.fill(context.orders()[0].order_id, 101, commission=1)
            await runtime.drain()
            await runtime.feed_candle(candle(START + MINUTE))
            await runtime.drain()
            before = (context.position(ROUTE), context.orders(), context.trades(), context.equity())
            await runtime.feed_candle(replace(candle(), close=100.5))
            await runtime.drain()
            self.assertEqual(before, (context.position(ROUTE), context.orders(), context.trades(), context.equity()))
            self.assertEqual(len(strategy.bars), 2)
            self.assertEqual(len(strategy.corrections), 1)

    async def test_optimizer_uses_shared_completed_history_by_default(self):
        class RecordingOptimizer:
            async def run(self, worker, data):
                self.history = data.history
                return {'period': len(data.history)}
        optimizer = RecordingOptimizer()
        async with TradingRuntime(ManualClock(START), optimizer=optimizer) as runtime:
            strategy = RecordingStrategy()
            context = await runtime.start_strategy(strategy)
            await runtime.feed_candle(candle())
            await runtime.drain()
            handle = await context.start_optimization(HistoryRequest(
                (INSTRUMENT.id,), START, runtime.clock.now(), MINUTE), select_period)
            self.assertEqual(await handle.wait(), {'period': 1})
            self.assertEqual(optimizer.history, (candle(),))
            self.assertEqual(context.status, StrategyStatus.RUNNING)
            self.assertEqual(strategy.parameters, {'period': 1})

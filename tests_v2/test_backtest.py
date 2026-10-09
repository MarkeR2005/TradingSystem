from dataclasses import replace
from datetime import timedelta
import unittest

from trading_system.backtest import BacktestConfig, HistoricalGateway, SingleBacktest
from trading_system.domain import OrderIntent, OrderStatus, Side
from trading_system.optimization import HistoryRequest
from trading_system.runtime import TradingRuntime
from trading_system.strategy import AutoStrategy
from trading_system.time import ManualClock
from helpers import START, INSTRUMENT, ROUTE, candle, PassiveStrategy
from optimization_workers import select_period

MINUTE = timedelta(minutes=1)


class SignalStrategy(AutoStrategy):
    def __init__(self, *, limit=None, quantity=2):
        super().__init__('signals', (INSTRUMENT.id,))
        self.bars = 0
        self.limit = limit
        self.quantity = quantity
        self.events = []
        self.warmup = ()

    async def on_start(self, context):
        self.warmup = await context.load_history(HistoryRequest(
            self.instruments, START - MINUTE, START, MINUTE))

    async def on_candle(self, context, bar):
        self.bars += 1
        self.events.append(('candle', context.position(ROUTE).quantity))
        if self.bars in (1, 2):
            await context.place_order(OrderIntent(
                ROUTE, Side.BUY if self.bars == 1 else Side.SELL, self.quantity, self.limit))

    async def on_fill(self, context, fill):
        self.events.append(('fill', context.position(ROUTE).quantity))


class HistoricalExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_limits_execute_at_limit_even_with_favorable_gap(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = HistoricalGateway('sim', runtime.clock, MINUTE, commission_per_lot=0.5)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2, 105))
            await context.place_order(OrderIntent(ROUTE, Side.SELL, 1, 95))
            await runtime.drain()
            runtime.clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            await runtime.drain()
            self.assertEqual([e.price for e in gateway.executions], [105, 95])
            self.assertEqual([e.commission for e in gateway.executions], [1, 0.5])

    async def test_untouched_limit_stays_pending_until_later_bar(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = HistoricalGateway('sim', runtime.clock, MINUTE)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2, 98))
            await runtime.drain()
            runtime.clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            await runtime.drain()
            self.assertEqual(context.orders()[0].status, OrderStatus.ACCEPTED)
            runtime.clock.advance_to(START + 2 * MINUTE)
            await gateway.process_bar(replace(candle(START + MINUTE), low=97))
            await runtime.drain()
            self.assertEqual(gateway.executions[0].price, 98)

    async def test_synthetic_bar_and_mid_bar_submission_never_fill(self):
        async with TradingRuntime(ManualClock(START + MINUTE / 2)) as runtime:
            gateway = HistoricalGateway('sim', runtime.clock, MINUTE)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            await runtime.drain()
            runtime.clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            runtime.clock.advance_to(START + 2 * MINUTE)
            await gateway.process_bar(replace(candle(START + MINUTE), synthetic=True, volume=0))
            await runtime.drain()
            self.assertEqual(gateway.executions, ())
            runtime.clock.advance_to(START + 3 * MINUTE)
            await gateway.process_bar(candle(START + 2 * MINUTE))
            await runtime.drain()
            self.assertEqual(len(gateway.executions), 1)

    async def test_fill_callback_order_waits_for_next_bar(self):
        class Chained(PassiveStrategy):
            async def on_fill(self, context, execution):
                await super().on_fill(context, execution)
                if len(self.fills) == 1:
                    await context.place_order(OrderIntent(ROUTE, Side.SELL, 1))
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = HistoricalGateway('sim', runtime.clock, MINUTE)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(Chained())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            await runtime.drain()
            runtime.clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            await runtime.drain()
            self.assertEqual(len(gateway.executions), 1)
            self.assertEqual(context.orders()[1].status, OrderStatus.ACCEPTED)
            with self.assertRaises(ValueError):
                await gateway.process_bar(candle())
            runtime.clock.advance_to(START + 2 * MINUTE)
            await gateway.process_bar(candle(START + MINUTE))
            await runtime.drain()
            self.assertEqual(len(gateway.executions), 2)

    async def test_cancel_replace_uses_new_price_and_next_bar(self):
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = HistoricalGateway('sim', runtime.clock, MINUTE)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2, 98))
            await runtime.drain()
            runtime.clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            await context.replace_order(context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 2, 100))
            await runtime.drain()
            runtime.clock.advance_to(START + 2 * MINUTE)
            await gateway.process_bar(candle(START + MINUTE))
            await runtime.drain()
            self.assertEqual([o.status for o in context.orders()], [OrderStatus.CANCELLED, OrderStatus.FILLED])
            self.assertEqual([e.price for e in gateway.executions], [100])

    async def test_gateway_rejects_wrong_clock_timeframe_and_overlap_before_fill(self):
        clock = ManualClock(START)
        async with TradingRuntime(clock) as runtime:
            gateway = HistoricalGateway('sim', clock, MINUTE)
            runtime.add_gateway(gateway)
            with self.assertRaises(ValueError):
                await gateway.process_bar(candle())
            clock.advance_to(START + MINUTE)
            await gateway.process_bar(candle())
            clock.advance_to(START + MINUTE + MINUTE / 2)
            with self.assertRaises(ValueError):
                await gateway.process_bar(candle(START + MINUTE / 2))
            clock.advance_to(START + 5 * MINUTE)
            with self.assertRaises(ValueError):
                await gateway.process_bar(replace(candle(), timeframe=5 * MINUTE))


class SingleBacktestTests(unittest.IsolatedAsyncioTestCase):
    def config(self, bars=3, fee=0.5):
        return BacktestConfig(INSTRUMENT, START, START + bars * MINUTE, MINUTE, fee)

    async def test_same_runtime_next_bar_worst_market_price_and_closed_pnl(self):
        strategy = SignalStrategy()
        bars = (candle(), candle(START + MINUTE),
                replace(candle(START + 2 * MINUTE), open=110, high=111, low=109, close=110))
        result = await SingleBacktest(self.config(), bars).run(lambda: strategy)
        self.assertEqual([e.price for e in result.executions], [101, 109])
        self.assertEqual(strategy.events, [('candle', 0), ('fill', 2), ('candle', 2), ('fill', 0), ('candle', 0)])
        self.assertEqual(result.closed_pnl, 14)
        self.assertEqual(result.realized_pnl, 14)
        self.assertEqual(result.unrealized_pnl, 0)
        self.assertEqual(result.total_pnl, 14)
        self.assertEqual(result.commission, 2)
        self.assertEqual(result.positions[0].quantity, 0)
        self.assertEqual(result.equity[0].value, 14)
        self.assertEqual(result.pending_orders, ())
        self.assertEqual(result.processed_bars, 3)

    async def test_last_signal_is_pending_and_open_position_is_not_force_closed(self):
        result = await SingleBacktest(self.config(2), (candle(), candle(START + MINUTE))).run(SignalStrategy)
        self.assertEqual(len(result.executions), 1)
        self.assertEqual(result.positions[0].quantity, 2)
        self.assertEqual(result.closed_pnl, 0)
        self.assertEqual(result.realized_pnl, -1)
        self.assertEqual(result.unrealized_pnl, -2)
        self.assertEqual(result.total_pnl, -3)
        self.assertEqual(len(result.pending_orders), 1)
        self.assertEqual(result.trades, ())

    async def test_repeated_runs_are_identical_and_do_not_reuse_state(self):
        bars = tuple(candle(START + i * MINUTE) for i in range(3))
        engine = SingleBacktest(self.config(), bars)
        first = await engine.run(SignalStrategy)
        second = await engine.run(SignalStrategy)
        self.assertEqual(first, second)
        self.assertEqual(len(first.data_digest), 64)
        other = await SingleBacktest(self.config(), (replace(bars[0], volume=11), *bars[1:])).run(SignalStrategy)
        self.assertNotEqual(first.data_digest, other.data_digest)

    async def test_warmup_is_before_start_not_trade_or_future_data(self):
        strategy = SignalStrategy()
        previous = candle(START - MINUTE)
        result = await SingleBacktest(self.config(),
            (previous, candle(), candle(START + MINUTE), candle(START + 2 * MINUTE))).run(lambda: strategy)
        self.assertEqual(strategy.warmup, (previous,))
        self.assertEqual(result.processed_bars, 3)
        self.assertEqual(len(result.executions), 2)

    async def test_synthetic_counts_and_no_fill_on_last_synthetic_bar(self):
        result = await SingleBacktest(self.config(2),
            (candle(), replace(candle(START + MINUTE), synthetic=True, volume=0))).run(SignalStrategy)
        self.assertEqual(result.synthetic_bars, 1)
        self.assertEqual(result.executions, ())
        self.assertEqual(len(result.pending_orders), 2)

    async def test_empty_duplicate_overlapping_foreign_and_straddling_history_rejected(self):
        for bars in [(), (candle(), candle()), (candle(), candle(START + MINUTE / 2)),
                     (replace(candle(), instrument_id=replace(INSTRUMENT.id, symbol='OTHER')),),
                     (candle(START - MINUTE / 2),),
                     (candle(START + 2 * MINUTE + MINUTE / 2),)]:
            with self.subTest(bars=bars), self.assertRaises(ValueError):
                SingleBacktest(self.config(), bars)

    async def test_optimization_has_no_wall_clock_race_in_basic_backtest(self):
        class Optimizing(SignalStrategy):
            async def on_candle(self, context, bar):
                await context.start_optimization(HistoryRequest(
                    self.instruments, START, bar.closed_at, MINUTE), select_period)
        with self.assertRaisesRegex(Exception, 'optimization is disabled'):
            await SingleBacktest(self.config(), tuple(candle(START + i * MINUTE) for i in range(3))).run(Optimizing)

    def test_invalid_config_rejected(self):
        for changes in [dict(start=START + 4 * MINUTE), dict(timeframe=timedelta(0)),
                        dict(commission_per_lot=-1), dict(commission_per_lot=float('inf'))]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.config(), **changes)

    async def test_wrong_route_metadata_or_instrument_cannot_produce_report(self):
        for instrument in [replace(INSTRUMENT, contract_multiplier=10),
                           replace(INSTRUMENT, id=replace(INSTRUMENT.id, symbol='OTHER'))]:
            class WrongRoute(SignalStrategy):
                async def on_candle(self, context, bar):
                    await context.place_order(OrderIntent(replace(ROUTE, instrument=instrument), Side.BUY, 1))
            with self.subTest(instrument=instrument), self.assertRaisesRegex(ValueError, 'configured instrument'):
                await SingleBacktest(self.config(), tuple(candle(START + i * MINUTE) for i in range(3))).run(WrongRoute)

    async def test_factory_cannot_reuse_a_previous_run_instance(self):
        strategy = SignalStrategy()
        engine = SingleBacktest(self.config(), tuple(candle(START + i * MINUTE) for i in range(3)))
        await engine.run(lambda: strategy)
        with self.assertRaisesRegex(ValueError, 'fresh'):
            await engine.run(lambda: strategy)

    async def test_open_short_mark_uses_contract_multiplier(self):
        instrument = replace(INSTRUMENT, contract_multiplier=10)
        route = replace(ROUTE, instrument=instrument)
        class Short(AutoStrategy):
            def __init__(self):
                super().__init__('short', (instrument.id,))
            async def on_start(self, context):
                await context.place_order(OrderIntent(route, Side.SELL, 2))
            async def on_candle(self, context, bar):
                pass
        config = replace(self.config(1), instrument=instrument)
        result = await SingleBacktest(config, (candle(),)).run(Short)
        self.assertEqual(result.executions[0].price, 99)
        self.assertEqual(result.unrealized_pnl, -20)
        self.assertEqual(result.total_pnl, -21)
        self.assertEqual(result.marked_at, START + MINUTE)
        self.assertEqual(result.mark_price, 100)

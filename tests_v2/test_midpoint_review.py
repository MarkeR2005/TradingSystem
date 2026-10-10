import asyncio
from contextlib import closing, suppress
from pathlib import Path
import sqlite3
import tempfile
import unittest

from trading_system.domain import CandleReceived, OrderIntent, Side
from trading_system.events import DeliveryError, EventBus
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal, JournalError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from tests_v2.helpers import START, ROUTE, PassiveStrategy, candle
from trading_system.time import ManualClock


class CancellationReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_handler_does_not_strand_following_messages(self):
        bus = EventBus()
        seen = []
        async def handler(message):
            if not seen:
                seen.append('cancelled')
                raise asyncio.CancelledError('callback dependency cancelled')
            seen.append('next')
        bus.subscribe('test', handler)
        try:
            await bus.publish('test', CandleReceived(candle()))
            await bus.publish('test', CandleReceived(candle()))
            with self.assertRaises(DeliveryError) as error:
                await asyncio.wait_for(bus.drain(), 0.5)
            self.assertIn('cancelled', str(error.exception))
            self.assertEqual(seen, ['cancelled', 'next'])
        finally:
            with suppress(TimeoutError):
                await asyncio.wait_for(bus.close(), 0.5)

    async def test_cancelled_strategy_fails_but_subsequent_fills_and_cancel_continue(self):
        class Cancelled(PassiveStrategy):
            async def on_candle(self, context, bar):
                raise asyncio.CancelledError('callback dependency cancelled')
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            strategy = Cancelled()
            context = await runtime.start_strategy(strategy)
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
            await runtime.drain()
            order_id = context.orders()[0].order_id
            await runtime.feed_candle(candle())
            with self.assertRaises(DeliveryError):
                await asyncio.wait_for(runtime.drain(), 0.5)
            self.assertEqual(context.status, StrategyStatus.FAILED)
            await gateway.fill(order_id, 100, quantity=1)
            await asyncio.wait_for(runtime.drain(), 0.5)
            self.assertEqual(len(strategy.fills), 1)
            self.assertEqual(context.position(ROUTE).quantity, 1)
            with self.assertRaises(RuntimeError):
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            await context.cancel_order(order_id)
            await runtime.drain()
            self.assertTrue(context.orders()[0].status.terminal)

    async def test_self_cancel_without_yield_fails_strategy_without_killing_worker(self):
        class SelfCancelled(PassiveStrategy):
            async def on_candle(self, context, bar):
                asyncio.current_task().cancel()
        async with TradingRuntime(ManualClock(START)) as runtime:
            strategy = SelfCancelled()
            context = await runtime.start_strategy(strategy)
            await runtime.feed_candle(candle())
            with self.assertRaises(DeliveryError):
                await asyncio.wait_for(runtime.drain(), 0.5)
            self.assertEqual(context.status, StrategyStatus.FAILED)
            await runtime.resume_strategy(strategy.name)
            await runtime.feed_candle(candle(candle().closed_at))
            with self.assertRaises(DeliveryError):
                await asyncio.wait_for(runtime.drain(), 0.5)

    async def test_cancelled_startup_is_not_left_running(self):
        class CancelledStart(PassiveStrategy):
            async def on_start(self, context):
                self.context = context
                raise asyncio.CancelledError()
        async with TradingRuntime(ManualClock(START)) as runtime:
            strategy = CancelledStart()
            with self.assertRaises(asyncio.CancelledError):
                await runtime.start_strategy(strategy)
            self.assertEqual(strategy.context.status, StrategyStatus.FAILED)


class ProjectionReviewTests(unittest.TestCase):
    def test_retry_conflict_blocks_further_journal_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                journal.append({'event': 'original'})
                before = journal.records
                with closing(sqlite3.connect(Path(directory) / 'events.sqlite3')) as db:
                    db.execute("UPDATE events SET payload = '{}' WHERE sequence = 1")
                    db.commit()
                with self.assertRaises(JournalError):
                    journal.retry_projection()
                self.assertFalse(journal.writable)
                with self.assertRaises(JournalError):
                    journal.append({'event': 'must not be recorded'})
                self.assertEqual(journal.records, before)

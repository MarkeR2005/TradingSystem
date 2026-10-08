import asyncio
import unittest

from trading_system.domain import CandleReceived, Message
from trading_system.events import DeliveryError, EventBus
from tests_v2.helpers import candle


class EventBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_waiting_consumer_does_not_block_other_mailboxes(self) -> None:
        bus = EventBus()
        entered, release, delivered = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def slow(message: Message) -> None:
            entered.set()
            await release.wait()

        async def fast(message: Message) -> None:
            delivered.set()

        bus.subscribe('slow', slow)
        bus.subscribe('fast', fast)
        try:
            await bus.publish('slow', CandleReceived(candle()))
            await asyncio.wait_for(entered.wait(), 1)
            await bus.publish('fast', CandleReceived(candle()))
            await asyncio.wait_for(delivered.wait(), 1)
        finally:
            release.set()
            await bus.close()

    async def test_mailbox_is_sequential_and_drain_includes_child_messages(self) -> None:
        bus = EventBus()
        seen: list[Message] = []
        first = CandleReceived(candle())
        second = CandleReceived(candle(candle().closed_at))

        async def handler(message: Message) -> None:
            seen.append(message)
            await asyncio.sleep(0)
            if message == first:
                await bus.publish('same', second)

        bus.subscribe('same', handler)
        await bus.publish('same', first)
        await bus.drain()
        self.assertEqual(seen, [first, second])
        await bus.close()

    async def test_handler_failure_is_reported_and_other_consumer_completes(self) -> None:
        bus = EventBus()
        seen: list[Message] = []

        async def broken(message: Message) -> None:
            raise ValueError('strategy failed')

        async def healthy(message: Message) -> None:
            seen.append(message)

        bus.subscribe('broken', broken)
        bus.subscribe('healthy', healthy)
        message = CandleReceived(candle())
        await bus.publish('broken', message)
        await bus.publish('healthy', message)
        with self.assertRaises(DeliveryError) as raised:
            await asyncio.wait_for(bus.drain(), 1)
        self.assertEqual(raised.exception.failures[0].recipient, 'broken')
        self.assertEqual(seen, [message])
        await bus.close()

    async def test_consumer_cannot_deadlock_on_its_own_drain(self) -> None:
        bus = EventBus()

        async def handler(message: Message) -> None:
            await bus.drain()

        bus.subscribe('self', handler)
        await bus.publish('self', CandleReceived(candle()))
        with self.assertRaises(DeliveryError) as raised:
            await asyncio.wait_for(bus.drain(), 1)
        self.assertIsInstance(raised.exception.failures[0].error, RuntimeError)
        await bus.close()

    async def test_unsubscribe_drains_then_rejects_delivery(self) -> None:
        bus = EventBus()
        seen: list[Message] = []

        async def handler(message: Message) -> None:
            seen.append(message)

        bus.subscribe('one', handler)
        await bus.publish('one', CandleReceived(candle()))
        await bus.unsubscribe('one')
        self.assertEqual(len(seen), 1)
        with self.assertRaises(ValueError):
            await bus.publish('one', CandleReceived(candle()))
        await bus.close()

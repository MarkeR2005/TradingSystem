import asyncio
from dataclasses import replace
from datetime import timedelta
import unittest

from trading_system.domain import Candle, GatewayAccepted, OrderIntent, OrderStatus, Side
from trading_system.events import DeliveryError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import StrategyContext
from trading_system.time import ManualClock
from tests_v2.helpers import INSTRUMENT, ROUTE, START, PassiveStrategy, candle, execution


class OrdersRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = ManualClock(START)
        self.runtime = TradingRuntime(self.clock)
        await self.runtime.__aenter__()
        self.gateway = SimulationGateway('sim', self.clock)
        self.runtime.add_gateway(self.gateway)
        self.strategy = PassiveStrategy()
        self.context = await self.runtime.start_strategy(self.strategy)

    async def asyncTearDown(self) -> None:
        await self.runtime.__aexit__(None, None, None)

    async def submit(self, quantity: float = 2) -> str:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, quantity))
        await self.runtime.drain()
        return self.context.orders()[-1].order_id

    async def test_partial_fills_update_position_and_weighted_order_price(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100, quantity=1, commission=1)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.context.position(ROUTE).quantity, 1)
        await self.gateway.fill(order_id, 120, commission=2)
        await self.runtime.drain()
        order = self.context.orders()[0]
        self.assertEqual(order.status, OrderStatus.FILLED)
        self.assertEqual(order.average_fill_price, 110)
        self.assertEqual(order.commission, 3)
        self.assertEqual(len(self.strategy.fills), 2)

    async def test_execution_before_ack_is_applied_and_late_ack_does_not_regress(self) -> None:
        # A deliberately detached OMS gateway lets the execution precede its ack.
        self.runtime.orders.gateway_ids.add('early')
        route = replace(ROUTE, instrument=replace(INSTRUMENT, id=replace(INSTRUMENT.id, gateway_id='early')))

        async def no_ack(message: object) -> None:
            pass

        self.runtime.bus.subscribe('gateway:early', no_ack)
        await self.context.place_order(OrderIntent(route, Side.BUY, 1))
        await self.runtime.drain()
        order = self.context.orders()[0]
        self.assertEqual(order.status, OrderStatus.SUBMITTED)
        fill = replace(execution('early', Side.BUY, 1, 100, route=route), order_id=order.order_id)
        await self.runtime.bus.publish('orders', fill)
        await self.runtime.bus.publish('orders', GatewayAccepted(order.order_id, 'early'))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.FILLED)
        self.assertEqual(self.context.position(route).quantity, 1)

    async def test_duplicate_execution_not_reapplied_or_notified_twice(self) -> None:
        order_id = await self.submit(1)
        fill = await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', fill)
        await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 1)
        self.assertEqual(len(self.strategy.fills), 1)
        await self.runtime.bus.publish('orders', replace(fill, price=101))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 1)

    async def test_invalid_execution_has_no_accounting_effect(self) -> None:
        order_id = await self.submit(1)
        valid = replace(execution('invalid', Side.BUY, 1, 100), order_id=order_id)
        for invalid in (replace(valid, side=Side.SELL), replace(valid, quantity=2),
                        replace(valid, timestamp=START - timedelta(seconds=1)),
                        replace(valid, route=replace(ROUTE, account_id='wrong'))):
            with self.subTest(execution=invalid):
                await self.runtime.bus.publish('orders', invalid)
                with self.assertRaises(DeliveryError):
                    await self.runtime.drain()
                self.assertEqual(self.context.position(ROUTE).quantity, 0)
                self.assertEqual(self.context.orders()[0].filled_quantity, 0)
        await self.runtime.bus.publish('orders', valid)
        await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 1)

    async def test_unknown_order_execution_has_no_accounting_effect(self) -> None:
        await self.runtime.bus.publish('orders', execution('unknown', Side.BUY, 1, 100))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.orders(), ())
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_execution_must_match_quantity_step(self) -> None:
        order_id = await self.submit(1)
        invalid = replace(execution('fraction', Side.BUY, 0.5, 100), order_id=order_id)
        await self.runtime.bus.publish('orders', invalid)
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_tiny_quantity_cannot_round_down_to_zero_lots(self) -> None:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 1e-15))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)

    async def test_invalid_intent_is_rejected_without_filling(self) -> None:
        for route, quantity in ((ROUTE, 0.5),
                                (replace(ROUTE, instrument=replace(INSTRUMENT, currency='USD')), 1),
                                (replace(ROUTE, instrument=replace(INSTRUMENT, id=replace(INSTRUMENT.id, gateway_id='unknown'))), 1)):
            await self.context.place_order(OrderIntent(route, Side.BUY, quantity))
        await self.runtime.drain()
        self.assertTrue(all(order.status is OrderStatus.REJECTED for order in self.context.orders()))
        self.assertTrue(all(order.rejection_reason for order in self.context.orders()))
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_failed_strategy_stops_new_orders_but_existing_fill_is_accounted(self) -> None:
        class BrokenStrategy(PassiveStrategy):
            async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
                raise ValueError('broken')

        broken = BrokenStrategy('broken')
        context = await self.runtime.start_strategy(broken)
        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        await self.runtime.drain()
        await self.runtime.feed_candle(candle())
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        with self.assertRaises(RuntimeError):
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        await self.gateway.fill(context.orders()[0].order_id, 100)
        await self.runtime.drain()
        self.assertEqual(context.position(ROUTE).quantity, 1)
        self.assertEqual(len(broken.fills), 1)

    async def test_simulation_clock_does_not_overtake_queued_candles(self) -> None:
        times = []

        class Recorder(PassiveStrategy):
            async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
                await asyncio.sleep(0)
                times.append(context.clock.now())

        await self.runtime.start_strategy(Recorder('recorder'))
        first = candle()
        second = candle(first.closed_at)
        await self.runtime.feed_candle(first)
        await self.runtime.feed_candle(second)
        await self.runtime.drain()
        self.assertEqual(times, [first.closed_at, second.closed_at])

    async def test_runtime_exit_finishes_queued_strategy_callbacks(self) -> None:
        class Buyer(PassiveStrategy):
            async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            context = await runtime.start_strategy(Buyer('buyer'))
            await runtime.feed_candle(candle())
        self.assertEqual(context.orders()[0].status, OrderStatus.ACCEPTED)
        with self.assertRaises(RuntimeError):
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

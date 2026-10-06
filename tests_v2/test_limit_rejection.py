from dataclasses import replace
import unittest

from trading_system.domain import (
    Candle, GatewayAccepted, GatewayCancelled, GatewayRejected, OrderIntent,
    OrderStatus, Side,
)
from trading_system.events import DeliveryError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import StrategyContext
from trading_system.time import ManualClock
from tests_v2.helpers import INSTRUMENT, ROUTE, START, PassiveStrategy, candle, execution


class LimitRejectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = TradingRuntime(ManualClock(START))
        await self.runtime.__aenter__()
        self.gateway = SimulationGateway('sim', self.runtime.clock)
        self.runtime.add_gateway(self.gateway)
        self.context = await self.runtime.start_strategy(PassiveStrategy())

    async def asyncTearDown(self) -> None:
        await self.runtime.__aexit__(None, None, None)

    async def test_simulator_and_oms_enforce_limit_on_both_sides(self) -> None:
        for side, bad, good in ((Side.BUY, 101, 99), (Side.SELL, 99, 101)):
            await self.context.place_order(OrderIntent(ROUTE, side, 1, limit_price=100))
            await self.runtime.drain()
            order = self.context.orders()[-1]
            with self.assertRaises(ValueError):
                await self.gateway.fill(order.order_id, bad)
            invalid = replace(execution('limit-' + side.value, side, 1, bad), order_id=order.order_id)
            await self.runtime.bus.publish('orders', invalid)
            with self.assertRaises(DeliveryError):
                await self.runtime.drain()
            self.assertEqual(self.context.orders()[-1].filled_quantity, 0)
            await self.gateway.fill(order.order_id, good)
            await self.runtime.drain()
            self.assertEqual(self.context.orders()[-1].status, OrderStatus.FILLED)
        self.assertEqual(self.context.trades()[0].pnl, 2)

    async def test_gateway_rejection_is_terminal_and_duplicate_is_idempotent(self) -> None:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        await self.runtime.drain()
        order = self.context.orders()[0]
        refusal = await self.gateway.reject_order(order.order_id, 'exchange rejected')
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', refusal)
        await self.runtime.bus.publish('orders', GatewayAccepted(order.order_id, 'sim'))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.REJECTED)
        self.assertEqual(self.context.orders()[0].rejection_reason, 'exchange rejected')
        fill = replace(execution('after-reject', Side.BUY, 1, 100), order_id=order.order_id)
        await self.runtime.bus.publish('orders', fill)
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_rejection_after_known_execution_cannot_erase_position(self) -> None:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
        await self.runtime.drain()
        order_id = self.context.orders()[0].order_id
        await self.gateway.fill(order_id, 100, quantity=1)
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', GatewayRejected(order_id, 'sim', 'invalid rejection'))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(self.context.position(ROUTE).quantity, 1)

    async def test_automatic_cancel_and_unknown_command(self) -> None:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        await self.runtime.drain()
        order_id = self.context.orders()[0].order_id
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        await self.context.cancel_order('unknown')
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()

    async def test_repeated_replacements_keep_history_and_residual_quantities(self) -> None:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, 5, limit_price=100))
        await self.runtime.drain()
        first = self.context.orders()[0]
        await self.gateway.fill(first.order_id, 100, quantity=1)
        await self.runtime.drain()
        await self.context.replace_order(first.order_id, replace(first.intent, limit_price=99))
        await self.runtime.drain()
        second = self.context.orders()[1]
        self.assertEqual(second.intent.quantity, 4)
        await self.gateway.fill(second.order_id, 99, quantity=1)
        await self.runtime.drain()
        await self.context.replace_order(second.order_id, replace(second.intent, limit_price=98))
        await self.runtime.drain()
        third = self.context.orders()[2]
        self.assertEqual(third.intent.quantity, 3)
        await self.gateway.fill(third.order_id, 98)
        await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 5)
        self.assertEqual(self.context.orders()[0].replacement_order_id, second.order_id)
        self.assertEqual(self.context.orders()[1].replacement_order_id, third.order_id)

    async def test_decimal_quantity_replacement_remains_on_lot_grid(self) -> None:
        route = replace(ROUTE, instrument=replace(INSTRUMENT, quantity_step=0.1))
        await self.context.place_order(OrderIntent(route, Side.BUY, 0.3, limit_price=100))
        await self.runtime.drain()
        first = self.context.orders()[0]
        await self.gateway.fill(first.order_id, 100, quantity=0.1)
        await self.runtime.drain()
        await self.context.replace_order(first.order_id, replace(first.intent, limit_price=99))
        await self.runtime.drain()
        second = self.context.orders()[1]
        self.assertAlmostEqual(second.intent.quantity, 0.2)
        await self.gateway.fill(second.order_id, 99)
        await self.runtime.drain()
        self.assertAlmostEqual(self.context.position(route).quantity, 0.3)

    async def test_failed_strategy_can_cancel_but_cannot_replace_existing_order(self) -> None:
        class Broken(PassiveStrategy):
            async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
                raise ValueError('failed')

        context = await self.runtime.start_strategy(Broken('broken'))
        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        await self.runtime.drain()
        await self.runtime.feed_candle(candle())
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        order = context.orders()[0]
        with self.assertRaises(RuntimeError):
            await context.replace_order(order.order_id, order.intent)
        await context.cancel_order(order.order_id)
        await self.runtime.drain()
        self.assertEqual(context.orders()[0].status, OrderStatus.CANCELLED)

    async def test_cancel_response_from_wrong_gateway_or_request_cannot_replace(self) -> None:
        async with TradingRuntime(ManualClock(START)) as runtime:
            gateway = SimulationGateway('sim', runtime.clock, auto_confirm_cancels=False)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
            await runtime.drain()
            await context.replace_order(context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 2, limit_price=99))
            await runtime.drain()
            old = context.orders()[0]
            for message in (GatewayCancelled(old.order_id, 'wrong', old.cancel_request_id, 0),
                            GatewayCancelled(old.order_id, 'sim', 'wrong-request', 0)):
                await runtime.bus.publish('orders', message)
                with self.assertRaises(DeliveryError):
                    await runtime.drain()
                self.assertEqual(len(context.orders()), 1)
            await gateway.confirm_cancel(old.order_id)
            await runtime.drain()
            self.assertEqual(len(context.orders()), 2)

    async def test_invalid_limit_and_cancel_cumulative_data_rejected_at_boundary(self) -> None:
        for price in (float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                OrderIntent(ROUTE, Side.BUY, 1, limit_price=price)
        for total in (-1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                GatewayCancelled('order', 'sim', 'request', total)

    async def test_cancel_is_not_available_after_runtime_exit(self) -> None:
        async with TradingRuntime(ManualClock(START)) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            context = await runtime.start_strategy(PassiveStrategy())
            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
        with self.assertRaises(RuntimeError):
            await context.cancel_order(context.orders()[0].order_id)

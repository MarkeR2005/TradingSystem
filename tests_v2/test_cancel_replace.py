from dataclasses import replace
import unittest

from trading_system.domain import (
    GatewayAccepted, GatewayCancelled, GatewayCancelRejected, OrderIntent, OrderStatus, Side,
)
from trading_system.events import DeliveryError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START, PassiveStrategy, execution


class CancelReplaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = ManualClock(START)
        self.runtime = TradingRuntime(self.clock)
        await self.runtime.__aenter__()
        self.gateway = SimulationGateway('sim', self.clock, auto_confirm_cancels=False)
        self.runtime.add_gateway(self.gateway)
        self.strategy = PassiveStrategy()
        self.context = await self.runtime.start_strategy(self.strategy)

    async def asyncTearDown(self) -> None:
        await self.runtime.__aexit__(None, None, None)

    async def submit(self, quantity: float = 5) -> str:
        await self.context.place_order(OrderIntent(ROUTE, Side.BUY, quantity, limit_price=100))
        await self.runtime.drain()
        return self.context.orders()[-1].order_id

    async def replace_order(self, order_id: str, quantity: float = 5) -> None:
        await self.context.replace_order(order_id, OrderIntent(ROUTE, Side.BUY, quantity, limit_price=99))
        await self.runtime.drain()

    async def test_cancel_request_does_not_cancel_before_ack(self) -> None:
        order_id = await self.submit()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(self.context.position(ROUTE).quantity, 0)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        with self.assertRaises(ValueError):
            await self.gateway.fill(order_id, 100)

    async def test_replace_waits_for_cancel_and_subtracts_racing_fills(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100, quantity=2, commission=1)
        await self.runtime.drain()
        await self.replace_order(order_id)
        self.assertEqual(len(self.context.orders()), 1)
        await self.gateway.fill(order_id, 100, quantity=1, commission=1)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        old, new = self.context.orders()
        self.assertEqual(old.status, OrderStatus.CANCELLED)
        self.assertEqual(new.intent.quantity, 2)
        self.assertEqual(new.intent.limit_price, 99)
        self.assertEqual(new.status, OrderStatus.ACCEPTED)
        self.assertEqual(new.replaces_order_id, old.order_id)
        self.assertEqual(old.replacement_order_id, new.order_id)
        await self.gateway.fill(new.order_id, 99, commission=1)
        await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 5)
        self.assertEqual(sum(order.commission for order in self.context.orders()), 3)

    async def test_cancel_ack_before_delayed_execution_waits_for_accounting(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        request_id = self.context.orders()[0].cancel_request_id
        ack = GatewayCancelled(order_id, 'sim', request_id, filled_quantity=2)
        await self.runtime.bus.publish('orders', ack)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        fill = replace(execution('delayed', Side.BUY, 2, 100), order_id=order_id)
        await self.runtime.bus.publish('orders', fill)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.context.orders()[1].intent.quantity, 3)
        self.assertEqual(self.context.position(ROUTE).quantity, 2)
        # Reordered duplicate reports cannot create another replacement or fill.
        await self.runtime.bus.publish('orders', ack)
        await self.runtime.bus.publish('orders', fill)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 2)
        self.assertEqual(len(self.strategy.fills), 1)

    async def test_full_fill_during_cancel_produces_no_zero_quantity_replacement(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.FILLED)
        self.assertEqual(self.context.position(ROUTE).quantity, 5)

    async def test_cancel_rejection_preserves_live_order_and_drops_replacement(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        await self.gateway.fill(order_id, 100, quantity=2)
        await self.runtime.drain()
        await self.gateway.reject_cancel(order_id, 'temporarily unavailable')
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        old = self.context.orders()[0]
        self.assertEqual(old.status, OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(old.cancel_rejection_reason, 'temporarily unavailable')
        await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 5)

    async def test_retry_cancel_has_new_identifier_and_old_duplicate_is_ignored(self) -> None:
        order_id = await self.submit()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        first_id = self.context.orders()[0].cancel_request_id
        refusal = await self.gateway.reject_cancel(order_id, 'retry')
        await self.runtime.drain()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        second_id = self.context.orders()[0].cancel_request_id
        self.assertNotEqual(first_id, second_id)
        await self.runtime.bus.publish('orders', refusal)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        self.assertEqual(self.context.orders()[0].cancel_request_id, second_id)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)

    async def test_second_cancel_abandons_pending_replacement(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        request_id = self.context.orders()[0].cancel_request_id
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].cancel_request_id, request_id)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)

    async def test_reduced_target_already_filled_only_cancels_remaining(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100, quantity=3)
        await self.runtime.drain()
        await self.replace_order(order_id, quantity=2)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        self.assertEqual(self.context.position(ROUTE).quantity, 3)

    async def test_different_strategy_cannot_cancel_or_replace_order(self) -> None:
        order_id = await self.submit()
        other = await self.runtime.start_strategy(PassiveStrategy('other'))
        for action in (other.cancel_order(order_id),
                       other.replace_order(order_id, OrderIntent(ROUTE, Side.BUY, 5, limit_price=99))):
            await action
            with self.assertRaises(DeliveryError):
                await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)

    async def test_invalid_replacement_leaves_original_live(self) -> None:
        order_id = await self.submit()
        for intent in (OrderIntent(ROUTE, Side.SELL, 5, limit_price=99),
                       OrderIntent(replace(ROUTE, account_id='other'), Side.BUY, 5, limit_price=99),
                       OrderIntent(ROUTE, Side.BUY, 0.5, limit_price=99)):
            await self.context.replace_order(order_id, intent)
            with self.assertRaises(DeliveryError):
                await self.runtime.drain()
            self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)

    async def test_late_ack_cannot_regress_cancel_pending_or_cancelled(self) -> None:
        order_id = await self.submit()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', GatewayAccepted(order_id, 'sim'))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
        ack = await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', GatewayAccepted(order_id, 'sim'))
        await self.runtime.bus.publish('orders', ack)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)

    async def test_cancel_ack_with_impossible_total_is_rejected(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100, quantity=2)
        await self.runtime.drain()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        request_id = self.context.orders()[0].cancel_request_id
        for total in (1, 6):
            await self.runtime.bus.publish('orders', GatewayCancelled(order_id, 'sim', request_id, total))
            with self.assertRaises(DeliveryError):
                await self.runtime.drain()
            self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCEL_PENDING)
            self.assertEqual(self.context.position(ROUTE).quantity, 2)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()

    async def test_new_fill_after_confirmed_cancel_is_rejected(self) -> None:
        order_id = await self.submit()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        fill = replace(execution('too-late', Side.BUY, 1, 100), order_id=order_id)
        await self.runtime.bus.publish('orders', fill)
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_replacement_of_terminal_order_is_rejected(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        await self.context.replace_order(order_id, OrderIntent(ROUTE, Side.BUY, 5, limit_price=99))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)

    async def test_missing_execution_reports_must_all_arrive_before_replacement(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        request_id = self.context.orders()[0].cancel_request_id
        ack = GatewayCancelled(order_id, 'sim', request_id, 2)
        await self.runtime.bus.publish('orders', ack)
        await self.runtime.drain()
        for index in (1, 2):
            fill = replace(execution('part-' + str(index), Side.BUY, 1, 100), order_id=order_id)
            await self.runtime.bus.publish('orders', fill)
            await self.runtime.drain()
            self.assertEqual(len(self.context.orders()), 1 if index == 1 else 2)
        self.assertEqual(self.context.orders()[1].intent.quantity, 3)

    async def test_conflicting_cancel_outcome_never_changes_terminal_state(self) -> None:
        order_id = await self.submit()
        await self.context.cancel_order(order_id)
        await self.runtime.drain()
        ack = await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        await self.runtime.bus.publish('orders', replace(ack, filled_quantity=1))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.context.position(ROUTE).quantity, 0)

    async def test_duplicate_replace_is_idempotent_but_changed_pending_target_is_rejected(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        await self.replace_order(order_id)
        await self.context.replace_order(order_id, OrderIntent(ROUTE, Side.BUY, 5, limit_price=98))
        with self.assertRaises(DeliveryError):
            await self.runtime.drain()
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 2)
        self.assertEqual(self.context.orders()[1].intent.limit_price, 99)

    async def test_rejected_replacement_does_not_reopen_cancelled_order(self) -> None:
        order_id = await self.submit()
        await self.gateway.fill(order_id, 100, quantity=2)
        await self.runtime.drain()
        await self.replace_order(order_id)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        child = self.context.orders()[1]
        await self.gateway.reject_order(child.order_id, 'replacement rejected')
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.CANCELLED)
        self.assertEqual(self.context.orders()[1].status, OrderStatus.REJECTED)
        self.assertEqual(self.context.position(ROUTE).quantity, 2)

    async def test_increased_target_uses_residual_after_original_full_fill(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id, quantity=7)
        await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        await self.gateway.confirm_cancel(order_id)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.FILLED)
        self.assertEqual(self.context.orders()[1].intent.quantity, 2)

    async def test_full_cancel_total_before_fill_finishes_without_replacement(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        request_id = self.context.orders()[0].cancel_request_id
        await self.runtime.bus.publish('orders', GatewayCancelled(order_id, 'sim', request_id, 5))
        await self.runtime.drain()
        self.assertEqual(len(self.context.orders()), 1)
        fill = replace(execution('full-delayed', Side.BUY, 5, 100), order_id=order_id)
        await self.runtime.bus.publish('orders', fill)
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.FILLED)
        self.assertEqual(len(self.context.orders()), 1)

    async def test_cancel_rejection_after_full_fill_keeps_filled_state(self) -> None:
        order_id = await self.submit()
        await self.replace_order(order_id)
        await self.gateway.fill(order_id, 100)
        await self.runtime.drain()
        await self.gateway.reject_cancel(order_id, 'already filled')
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.FILLED)
        self.assertEqual(len(self.context.orders()), 1)

    async def test_cancel_before_acceptance_restores_accepted_state_after_refusal(self) -> None:
        route = replace(ROUTE, instrument=replace(ROUTE.instrument, id=replace(ROUTE.instrument.id, gateway_id='slow')))
        self.runtime.orders.gateway_ids.add('slow')

        async def delayed_gateway(message: object) -> None:
            pass

        self.runtime.bus.subscribe('gateway:slow', delayed_gateway)
        await self.context.place_order(OrderIntent(route, Side.BUY, 1))
        await self.runtime.drain()
        old = self.context.orders()[0]
        self.assertEqual(old.status, OrderStatus.SUBMITTED)
        await self.context.cancel_order(old.order_id)
        await self.runtime.drain()
        pending = self.context.orders()[0]
        await self.runtime.bus.publish('orders', GatewayAccepted(old.order_id, 'slow'))
        await self.runtime.bus.publish('orders', GatewayCancelRejected(old.order_id, 'slow', pending.cancel_request_id, 'refused'))
        await self.runtime.drain()
        self.assertEqual(self.context.orders()[0].status, OrderStatus.ACCEPTED)

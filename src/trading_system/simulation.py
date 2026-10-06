"""Controlled executions and cancellation reports, not an OHLC backtester."""

from dataclasses import replace
from typing import Protocol
from uuid import uuid4

from .domain import (
    Execution, GatewayAccepted, GatewayCancel, GatewayCancelled,
    GatewayCancelRejected, GatewayRejected, GatewaySubmit, Message,
    OrderSnapshot, OrderStatus,
)
from .events import EventBus
from .orders import gateway_recipient
from .time import Clock


class Gateway(Protocol):
    @property
    def gateway_id(self) -> str: ...

    def attach(self, bus: EventBus) -> None: ...


class SimulationGateway:
    def __init__(self, gateway_id: str, clock: Clock, *, auto_confirm_cancels: bool = True) -> None:
        self.gateway_id = gateway_id
        self.clock = clock
        self.auto_confirm_cancels = auto_confirm_cancels
        self._bus: EventBus | None = None
        self._orders: dict[str, OrderSnapshot] = {}
        self._cancellations: dict[str, GatewayCancel] = {}

    def attach(self, bus: EventBus) -> None:
        if self._bus is not None:
            raise ValueError('gateway is already attached')
        bus.subscribe(gateway_recipient(self.gateway_id), self._handle)
        self._bus = bus

    async def _handle(self, message: Message) -> None:
        if isinstance(message, GatewaySubmit):
            order = message.order
            if order.intent.route.instrument.id.gateway_id != self.gateway_id:
                raise ValueError('order was routed to the wrong gateway')
            self._orders[order.order_id] = replace(order, status=OrderStatus.ACCEPTED)
            await self._publish(GatewayAccepted(order.order_id, self.gateway_id))
        elif isinstance(message, GatewayCancel):
            if message.gateway_id != self.gateway_id:
                raise ValueError('cancellation was routed to the wrong gateway')
            self._orders[message.order_id]
            previous = self._cancellations.get(message.order_id)
            if previous is not None:
                if previous != message:
                    raise ValueError('another cancellation is already pending')
                return
            self._cancellations[message.order_id] = message
            if self.auto_confirm_cancels:
                await self.confirm_cancel(message.order_id)
        else:
            raise ValueError('unexpected simulation message')

    async def _publish(self, message: Message) -> None:
        if self._bus is None:
            raise RuntimeError('gateway is not attached')
        await self._bus.publish('orders', message)

    async def confirm_cancel(self, order_id: str) -> GatewayCancelled:
        request = self._cancellations[order_id]
        order = self._orders[order_id]
        message = GatewayCancelled(order_id, self.gateway_id, request.request_id, order.filled_quantity)
        await self._publish(message)
        status = OrderStatus.FILLED if order.status is OrderStatus.FILLED else OrderStatus.CANCELLED
        self._orders[order_id] = replace(order, status=status)
        del self._cancellations[order_id]
        return message

    async def reject_cancel(self, order_id: str, reason: str) -> GatewayCancelRejected:
        request = self._cancellations[order_id]
        message = GatewayCancelRejected(order_id, self.gateway_id, request.request_id, reason)
        await self._publish(message)
        del self._cancellations[order_id]
        return message

    async def reject_order(self, order_id: str, reason: str) -> GatewayRejected:
        order = self._orders[order_id]
        if order.status.terminal or order.filled_quantity > 0:
            raise ValueError('cannot reject an executed or terminal order')
        message = GatewayRejected(order_id, self.gateway_id, reason)
        await self._publish(message)
        self._orders[order_id] = replace(order, status=OrderStatus.REJECTED, rejection_reason=reason)
        self._cancellations.pop(order_id, None)
        return message

    async def fill(self, order_id: str, price: float, *, quantity: float | None = None, commission: float = 0.0) -> Execution:
        order = self._orders[order_id]
        if order.status.terminal:
            raise ValueError('cannot fill a terminal order')
        remaining = order.remaining_quantity
        quantity = remaining if quantity is None else quantity
        instrument = order.intent.route.instrument
        if not instrument.accepts_quantity(quantity):
            raise ValueError('execution quantity does not match instrument step')
        if not order.intent.accepts_price(price):
            raise ValueError('execution price violates order limit')
        full = instrument.quantities_equal(quantity, remaining)
        if quantity > remaining and not full:
            raise ValueError('fill exceeds remaining quantity')
        execution = Execution(uuid4().hex, order_id, order.intent.route, order.intent.side,
                              quantity, price, commission, self.clock.now())
        await self._publish(execution)
        self._orders[order_id] = replace(
            order, filled_quantity=order.intent.quantity if full else order.filled_quantity + quantity,
            status=OrderStatus.FILLED if full else OrderStatus.PARTIALLY_FILLED,
        )
        return execution

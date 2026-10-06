"""Controlled executions for integration scenarios, not an OHLC backtester."""

from dataclasses import replace
from math import isclose
from typing import Protocol
from uuid import uuid4

from .domain import Execution, GatewayAccepted, GatewaySubmit, Message, OrderSnapshot, OrderStatus
from .events import EventBus
from .orders import gateway_recipient
from .time import Clock


class Gateway(Protocol):
    @property
    def gateway_id(self) -> str: ...

    def attach(self, bus: EventBus) -> None: ...


class SimulationGateway:
    def __init__(self, gateway_id: str, clock: Clock) -> None:
        self.gateway_id = gateway_id
        self.clock = clock
        self._bus: EventBus | None = None
        self._orders: dict[str, OrderSnapshot] = {}

    def attach(self, bus: EventBus) -> None:
        if self._bus is not None:
            raise ValueError('gateway is already attached')
        bus.subscribe(gateway_recipient(self.gateway_id), self._handle)
        self._bus = bus

    async def _handle(self, message: Message) -> None:
        if not isinstance(message, GatewaySubmit):
            raise ValueError('unexpected simulation message')
        order = message.order
        if order.intent.route.instrument.id.gateway_id != self.gateway_id:
            raise ValueError('order was routed to the wrong gateway')
        self._orders[order.order_id] = order
        await self._publish(GatewayAccepted(order.order_id, self.gateway_id))

    async def _publish(self, message: Message) -> None:
        if self._bus is None:
            raise RuntimeError('gateway is not attached')
        await self._bus.publish('orders', message)

    async def fill(self, order_id: str, price: float, *, quantity: float | None = None, commission: float = 0.0) -> Execution:
        order = self._orders[order_id]
        remaining = order.intent.quantity - order.filled_quantity
        quantity = remaining if quantity is None else quantity
        if not order.intent.route.instrument.accepts_quantity(quantity):
            raise ValueError('execution quantity does not match instrument step')
        full = isclose(quantity, remaining, rel_tol=1e-12, abs_tol=0)
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

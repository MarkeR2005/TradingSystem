from dataclasses import replace
from math import isclose
from uuid import uuid4

from .domain import Execution, ExecutionApplied, GatewayAccepted, GatewaySubmit, Message, OrderSnapshot, OrderStatus, OrderUpdated, SubmitOrder
from .events import EventBus
from .ledger import PositionLedger
from .time import Clock


def strategy_recipient(strategy_id: str) -> str:
    return f'strategy:{strategy_id}'


def gateway_recipient(gateway_id: str) -> str:
    return f'gateway:{gateway_id}'


class OrderManager:
    def __init__(self, bus: EventBus, clock: Clock, ledger: PositionLedger) -> None:
        self.bus = bus
        self.clock = clock
        self.ledger = ledger
        self.gateway_ids: set[str] = set()
        self._orders: dict[str, OrderSnapshot] = {}
        self._executions: dict[tuple[str, str], Execution] = {}

    def orders(self, strategy_id: str) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self._orders.values() if order.strategy_id == strategy_id)

    async def handle(self, message: Message) -> None:
        if isinstance(message, SubmitOrder):
            await self._submit(message)
        elif isinstance(message, GatewayAccepted):
            order = self._orders[message.order_id]
            if message.gateway_id != order.intent.route.instrument.id.gateway_id:
                raise ValueError('acknowledgement gateway does not match order')
            if order.status is OrderStatus.SUBMITTED:
                await self._update(replace(order, status=OrderStatus.ACCEPTED))
        elif isinstance(message, Execution):
            await self._execute(message)
        else:
            raise ValueError('unexpected order-manager message')

    async def _submit(self, message: SubmitOrder) -> None:
        order = OrderSnapshot(uuid4().hex, message.strategy_id, message.intent, self.clock.now())
        intent = message.intent
        gateway_id = intent.route.instrument.id.gateway_id
        reason = None
        if gateway_id not in self.gateway_ids:
            reason = 'gateway is not registered'
        elif intent.route.instrument.currency != self.ledger.report_currency:
            reason = 'FX conversion is not implemented'
        elif not intent.route.instrument.accepts_quantity(intent.quantity):
            reason = 'quantity does not match instrument step'
        if reason:
            await self._update(replace(order, status=OrderStatus.REJECTED, rejection_reason=reason))
            return
        await self._update(order)
        await self.bus.publish(gateway_recipient(gateway_id), GatewaySubmit(order))

    async def _update(self, order: OrderSnapshot) -> None:
        self._orders[order.order_id] = order
        await self.bus.publish(strategy_recipient(order.strategy_id), OrderUpdated(order))

    async def _execute(self, execution: Execution) -> None:
        previous = self._executions.get(execution.key)
        if previous is not None:
            if previous != execution:
                raise ValueError('execution identifier has conflicting contents')
            return
        order = self._orders[execution.order_id]
        if order.status in (OrderStatus.REJECTED, OrderStatus.FILLED):
            raise ValueError('order cannot receive further executions')
        if execution.route != order.intent.route or execution.side != order.intent.side:
            raise ValueError('execution route or side does not match order')
        if execution.timestamp < order.submitted_at:
            raise ValueError('execution predates submission')
        if not execution.route.instrument.accepts_quantity(execution.quantity):
            raise ValueError('execution quantity does not match instrument step')
        total = order.filled_quantity + execution.quantity
        full = isclose(total, order.intent.quantity, rel_tol=1e-12, abs_tol=0)
        if total > order.intent.quantity and not full:
            raise ValueError('execution exceeds remaining quantity')
        self.ledger.apply(order.strategy_id, execution)
        self._executions[execution.key] = execution
        average = (order.average_fill_price * order.filled_quantity + execution.price * execution.quantity) / total
        await self._update(replace(
            order, filled_quantity=order.intent.quantity if full else total,
            average_fill_price=average, commission=order.commission + execution.commission,
            status=OrderStatus.FILLED if full else OrderStatus.PARTIALLY_FILLED,
        ))
        await self.bus.publish(strategy_recipient(order.strategy_id), ExecutionApplied(execution))

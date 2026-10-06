from dataclasses import dataclass, replace
from uuid import uuid4

from .domain import (
    CancelOrder, Execution, ExecutionApplied, GatewayAccepted, GatewayCancel,
    GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewaySubmit,
    Message, OrderIntent, OrderSnapshot, OrderStatus, OrderUpdated, ReplaceOrder,
    SubmitOrder,
)
from .events import EventBus
from .ledger import PositionLedger
from .time import Clock


def strategy_recipient(strategy_id: str) -> str:
    return f'strategy:{strategy_id}'


def gateway_recipient(gateway_id: str) -> str:
    return f'gateway:{gateway_id}'


@dataclass
class _Cancellation:
    request_id: str
    prior_status: OrderStatus
    replacement: OrderIntent | None = None
    acknowledgement: GatewayCancelled | None = None


class OrderManager:
    def __init__(self, bus: EventBus, clock: Clock, ledger: PositionLedger) -> None:
        self.bus = bus
        self.clock = clock
        self.ledger = ledger
        self.gateway_ids: set[str] = set()
        self._orders: dict[str, OrderSnapshot] = {}
        self._executions: dict[tuple[str, str], Execution] = {}
        self._cancellations: dict[str, _Cancellation] = {}
        self._cancel_outcomes: dict[tuple[str, str], GatewayCancelled | GatewayCancelRejected] = {}

    def orders(self, strategy_id: str) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self._orders.values() if order.strategy_id == strategy_id)

    async def handle(self, message: Message) -> None:
        if isinstance(message, SubmitOrder):
            await self._new_order(message.strategy_id, message.intent)
        elif isinstance(message, (CancelOrder, ReplaceOrder)):
            await self._request_cancel(message)
        elif isinstance(message, GatewayAccepted):
            await self._accepted(message)
        elif isinstance(message, (GatewayCancelled, GatewayCancelRejected)):
            await self._cancel_response(message)
        elif isinstance(message, GatewayRejected):
            await self._rejected(message)
        elif isinstance(message, Execution):
            await self._execute(message)
        else:
            raise ValueError('unexpected order-manager message')

    def _intent_error(self, intent: OrderIntent) -> str | None:
        if intent.route.instrument.id.gateway_id not in self.gateway_ids:
            return 'gateway is not registered'
        if intent.route.instrument.currency != self.ledger.report_currency:
            return 'FX conversion is not implemented'
        if not intent.route.instrument.accepts_quantity(intent.quantity):
            return 'quantity does not match instrument step'
        return None

    async def _new_order(self, strategy_id: str, intent: OrderIntent,
                         replaces_order_id: str | None = None) -> OrderSnapshot:
        order = OrderSnapshot(uuid4().hex, strategy_id, intent, self.clock.now(),
                              replaces_order_id=replaces_order_id)
        reason = self._intent_error(intent)
        if reason:
            order = replace(order, status=OrderStatus.REJECTED, rejection_reason=reason)
            await self._update(order)
            return order
        await self._update(order)
        await self.bus.publish(gateway_recipient(intent.route.instrument.id.gateway_id), GatewaySubmit(order))
        return order

    async def _update(self, order: OrderSnapshot) -> None:
        self._orders[order.order_id] = order
        await self.bus.publish(strategy_recipient(order.strategy_id), OrderUpdated(order))

    def _from_gateway(self, order_id: str, gateway_id: str) -> OrderSnapshot:
        order = self._orders[order_id]
        if gateway_id != order.intent.route.instrument.id.gateway_id:
            raise ValueError('gateway does not match order')
        return order

    async def _accepted(self, message: GatewayAccepted) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        pending = self._cancellations.get(order.order_id)
        if pending is not None and pending.prior_status is OrderStatus.SUBMITTED:
            pending.prior_status = OrderStatus.ACCEPTED
        if order.status is OrderStatus.SUBMITTED:
            await self._update(replace(order, status=OrderStatus.ACCEPTED))

    async def _request_cancel(self, message: CancelOrder | ReplaceOrder) -> None:
        order = self._orders[message.order_id]
        if order.strategy_id != message.strategy_id:
            raise ValueError('strategy does not own order')
        intent = message.intent if isinstance(message, ReplaceOrder) else None
        if intent is not None:
            if order.status.terminal:
                raise ValueError('terminal order cannot be replaced')
            if intent.route != order.intent.route or intent.side != order.intent.side:
                raise ValueError('replacement must keep route and side')
            reason = self._intent_error(intent)
            if reason:
                raise ValueError(reason)
        pending = self._cancellations.get(order.order_id)
        if pending is not None:
            if intent is None:
                pending.replacement = None
            elif intent != pending.replacement:
                raise ValueError('another cancellation is already pending')
            return
        if order.status.terminal:
            return
        request_id = uuid4().hex
        self._cancellations[order.order_id] = _Cancellation(request_id, order.status, intent)
        await self._update(replace(order, status=OrderStatus.CANCEL_PENDING,
                                   cancel_request_id=request_id, cancel_rejection_reason=None))
        gateway_id = order.intent.route.instrument.id.gateway_id
        await self.bus.publish(gateway_recipient(gateway_id), GatewayCancel(order.order_id, gateway_id, request_id))

    async def _cancel_response(self, message: GatewayCancelled | GatewayCancelRejected) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        key = message.gateway_id, message.request_id
        previous = self._cancel_outcomes.get(key)
        if previous is not None:
            if previous != message:
                raise ValueError('cancellation identifier has conflicting outcome')
            return
        pending = self._cancellations.get(order.order_id)
        if pending is None or pending.request_id != message.request_id:
            raise ValueError('cancellation response has no matching request')
        instrument = order.intent.route.instrument
        if isinstance(message, GatewayCancelled):
            total = message.filled_quantity
            if total != 0 and not instrument.accepts_quantity(total):
                raise ValueError('cancelled quantity does not match instrument step')
            if total < order.filled_quantity and not instrument.quantities_equal(total, order.filled_quantity):
                raise ValueError('cancelled quantity is below known executions')
            if total > order.intent.quantity and not instrument.quantities_equal(total, order.intent.quantity):
                raise ValueError('cancelled quantity exceeds order')
            pending.acknowledgement = message
            self._cancel_outcomes[key] = message
            await self._finish_cancel(order.order_id)
        else:
            del self._cancellations[order.order_id]
            self._cancel_outcomes[key] = message
            status = order.status if order.status.terminal else (
                OrderStatus.PARTIALLY_FILLED if order.filled_quantity > 0 else pending.prior_status
            )
            await self._update(replace(order, status=status, cancel_rejection_reason=message.reason))

    async def _finish_cancel(self, order_id: str) -> None:
        pending = self._cancellations.get(order_id)
        if pending is None or pending.acknowledgement is None:
            return
        order = self._orders[order_id]
        instrument = order.intent.route.instrument
        if not instrument.quantities_equal(order.filled_quantity, pending.acknowledgement.filled_quantity):
            return
        del self._cancellations[order_id]
        status = OrderStatus.FILLED if instrument.quantities_equal(order.filled_quantity, order.intent.quantity) else OrderStatus.CANCELLED
        order = replace(order, status=status)
        await self._update(order)
        intent = pending.replacement
        if intent is None or intent.quantity <= order.filled_quantity or instrument.quantities_equal(intent.quantity, order.filled_quantity):
            return
        remaining = instrument.quantity_step * round((intent.quantity - order.filled_quantity) / instrument.quantity_step)
        child = await self._new_order(order.strategy_id, replace(intent, quantity=remaining), order_id)
        await self._update(replace(order, replacement_order_id=child.order_id))

    async def _rejected(self, message: GatewayRejected) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        if order.status is OrderStatus.REJECTED and order.rejection_reason == message.reason:
            return
        if order.status.terminal or order.filled_quantity > 0:
            raise ValueError('rejection contradicts existing order state')
        self._cancellations.pop(order.order_id, None)
        await self._update(replace(order, status=OrderStatus.REJECTED, rejection_reason=message.reason))

    async def _execute(self, execution: Execution) -> None:
        previous = self._executions.get(execution.key)
        if previous is not None:
            if previous != execution:
                raise ValueError('execution identifier has conflicting contents')
            return
        order = self._orders[execution.order_id]
        if order.status.terminal:
            raise ValueError('order cannot receive further executions')
        if execution.route != order.intent.route or execution.side != order.intent.side:
            raise ValueError('execution route or side does not match order')
        if execution.timestamp < order.submitted_at:
            raise ValueError('execution predates submission')
        instrument = execution.route.instrument
        if not instrument.accepts_quantity(execution.quantity):
            raise ValueError('execution quantity does not match instrument step')
        if not order.intent.accepts_price(execution.price):
            raise ValueError('execution price violates order limit')
        total = order.filled_quantity + execution.quantity
        full = instrument.quantities_equal(total, order.intent.quantity)
        if total > order.intent.quantity and not full:
            raise ValueError('execution exceeds remaining quantity')
        pending = self._cancellations.get(order.order_id)
        if pending is not None and pending.acknowledgement is not None:
            final_total = pending.acknowledgement.filled_quantity
            if total > final_total and not instrument.quantities_equal(total, final_total):
                raise ValueError('execution contradicts cancellation barrier')
        self.ledger.apply(order.strategy_id, execution)
        self._executions[execution.key] = execution
        average = (order.average_fill_price * order.filled_quantity + execution.price * execution.quantity) / total
        status = OrderStatus.FILLED if full else (
            OrderStatus.CANCEL_PENDING if pending is not None else OrderStatus.PARTIALLY_FILLED
        )
        await self._update(replace(
            order, filled_quantity=order.intent.quantity if full else total,
            average_fill_price=average, commission=order.commission + execution.commission,
            status=status,
        ))
        await self.bus.publish(strategy_recipient(order.strategy_id), ExecutionApplied(execution))
        await self._finish_cancel(order.order_id)

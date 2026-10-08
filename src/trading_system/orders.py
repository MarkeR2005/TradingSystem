from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from .domain import (
    CancelOrder, Execution, ExecutionApplied, GatewayAccepted, GatewayCancel,
    GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewaySubmit,
    GatewayReconciled, Message, OrderIntent, OrderSnapshot, OrderStatus, OrderUpdated, ReplaceOrder,
    SubmitOrder,
)
from . import codec
from .persistence import DurableJournal, JournalError
from .ledger import PositionLedger
from .lifecycle import decode_strategy_record
from .time import Clock, ManualClock


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


class _Publisher(Protocol):
    async def publish(self, recipient: str, message: Message) -> None: ...


class _PlannedEvents:
    def __init__(self) -> None:
        self.events: list[tuple[str, Message]] = []

    async def publish(self, recipient: str, message: Message) -> None:
        self.events.append((recipient, message))


class OrderManager:
    def __init__(self, bus: _Publisher, clock: Clock, ledger: PositionLedger, *,
                 journal: DurableJournal | None = None,
                 id_factory: Callable[[], str] = lambda: uuid4().hex) -> None:
        self.journal = journal
        self._id_factory = id_factory
        self._unreconciled: set[str] = set()
        self._storage_blocked = False
        self._failed_after_commit = False
        self.bus = bus
        self.clock = clock
        self.ledger = ledger
        self.gateway_ids: set[str] = set()
        self._orders: dict[str, OrderSnapshot] = {}
        self._local_rejections: set[str] = set()
        self._executions: dict[tuple[str, str], Execution] = {}
        self._cancellations: dict[str, _Cancellation] = {}
        self._cancel_outcomes: dict[tuple[str, str], GatewayCancelled | GatewayCancelRejected] = {}

    @property
    def ready(self) -> bool:
        return (not self._unreconciled and not self._storage_blocked and not self._failed_after_commit
                and (self.journal is None or (self.journal.writable and not self.journal.degraded)))

    def _copy_state(self, candidate: 'OrderManager') -> None:
        candidate.gateway_ids = self.gateway_ids.copy()
        candidate._orders = self._orders.copy()
        candidate._local_rejections = self._local_rejections.copy()
        candidate._executions = self._executions.copy()
        candidate._cancellations = {key: replace(value) for key, value in self._cancellations.items()}
        candidate._cancel_outcomes = self._cancel_outcomes.copy()
        candidate._unreconciled = self._unreconciled.copy()

    def _adopt(self, candidate: 'OrderManager') -> None:
        self._orders = candidate._orders
        self._local_rejections = candidate._local_rejections
        self._executions = candidate._executions
        self._cancellations = candidate._cancellations
        self._cancel_outcomes = candidate._cancel_outcomes
        self._unreconciled = candidate._unreconciled
        self.ledger.adopt(candidate.ledger)

    async def handle(self, message: Message) -> None:
        if self._failed_after_commit:
            raise JournalError('durable transition could not be delivered; restart and reconcile required')
        if isinstance(message, (SubmitOrder, ReplaceOrder)) and not self.ready:
            raise RuntimeError('trading is blocked until storage and gateway reconciliation are ready')
        if self.journal is None:
            await self._handle(message)
            return
        generated: list[str] = []

        def identifier() -> str:
            result = self._id_factory()
            generated.append(result)
            return result

        events = _PlannedEvents()
        candidate = OrderManager(events, self.clock, self.ledger.fork(), id_factory=identifier)
        self._copy_state(candidate)
        candidate._storage_blocked = not self.journal.writable or self.journal.degraded
        await candidate._handle(message)
        record = {'version': 1, 'time': self.clock.now().isoformat(), 'ids': generated,
                  'gateways': sorted(self.gateway_ids), 'unreconciled': sorted(self._unreconciled),
                  'storage_blocked': candidate._storage_blocked, 'message': codec.encode(message)}
        self.journal.append(record)
        try:
            self._adopt(candidate)
            for recipient, outgoing in events.events:
                await self.bus.publish(recipient, outgoing)
        except BaseException:
            self._failed_after_commit = True
            raise

    async def restore(self) -> None:
        if self.journal is None:
            return
        if self._orders:
            raise RuntimeError('restore requires an empty order manager')
        candidate = OrderManager(_PlannedEvents(), self.clock, PositionLedger(self.ledger.report_currency))
        latest: datetime | None = None
        for record in self.journal.records:
            try:
                if record.get('kind') == 'strategy':
                    timestamp, _ = decode_strategy_record(record)
                    if latest is not None and timestamp < latest:
                        raise JournalError('journal clock is invalid')
                    latest = timestamp
                    continue
                if (set(record) != {'version', 'time', 'ids', 'gateways', 'unreconciled',
                                    'storage_blocked', 'message'} or record['version'] != 1
                        or not isinstance(record['storage_blocked'], bool)):
                    raise JournalError('unsupported order journal schema')
                timestamp = datetime.fromisoformat(record['time'])
                if timestamp.utcoffset() is None or (latest is not None and timestamp < latest):
                    raise JournalError('journal clock is invalid')
                for key in ('ids', 'gateways', 'unreconciled'):
                    if not isinstance(record[key], list) or not all(isinstance(v, str) and v for v in record[key]):
                        raise JournalError('invalid order journal identifiers')
                identifiers = iter(record['ids'])
                candidate.bus = _PlannedEvents()
                candidate._id_factory = lambda: next(identifiers)
                candidate.clock = ManualClock(timestamp)
                candidate.gateway_ids = set(record['gateways'])
                candidate._unreconciled = set(record['unreconciled'])
                candidate._storage_blocked = record['storage_blocked']
                await candidate._handle(codec.decode(record['message']))
                if next(identifiers, None) is not None:
                    raise JournalError('journal has unused generated identifiers')
                latest = timestamp
            except (ValueError, TypeError, KeyError, StopIteration, AttributeError, RuntimeError) as error:
                raise JournalError('cannot replay order journal') from error
        self._adopt(candidate)
        self._unreconciled = {o.intent.route.instrument.id.gateway_id for o in self._orders.values()
                             if o.order_id not in self._local_rejections}
        if latest is not None and isinstance(self.clock, ManualClock) and self.clock.now() < latest:
            self.clock.advance_to(latest)

    async def reconcile_gateway(self, gateway_id: str, observed: tuple[OrderSnapshot, ...]) -> None:
        await self.handle(GatewayReconciled(gateway_id, observed))

    def _validate_reconciliation(self, gateway_id: str, observed: tuple[OrderSnapshot, ...]) -> None:
        if gateway_id not in self.gateway_ids:
            raise ValueError('gateway is not registered')
        expected = {key: value for key, value in self._orders.items()
                    if value.intent.route.instrument.id.gateway_id == gateway_id
                    and key not in self._local_rejections}
        actual = {order.order_id: order for order in observed}
        if len(actual) != len(observed) or set(actual) != set(expected):
            raise ValueError('gateway order identifiers differ; missing/external orders need reconciliation')
        for key, order in expected.items():
            other = actual[key]
            status = order.status
            pending = self._cancellations.get(key)
            if pending is not None and pending.acknowledgement is not None:
                if order.intent.route.instrument.quantities_equal(
                        order.filled_quantity, pending.acknowledgement.filled_quantity):
                    status = OrderStatus.FILLED if order.remaining_quantity == 0 else OrderStatus.CANCELLED
            if (order.intent != other.intent or order.submitted_at != other.submitted_at
                    or status != other.status or order.filled_quantity != other.filled_quantity
                    or order.average_fill_price != other.average_fill_price
                    or order.commission != other.commission
                    or (status is OrderStatus.CANCEL_PENDING
                        and order.cancel_request_id != other.cancel_request_id)):
                raise ValueError('gateway order contents differ; reconcile reports before resuming')

    def orders(self, strategy_id: str) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self._orders.values() if order.strategy_id == strategy_id)

    async def _handle(self, message: Message) -> None:
        if isinstance(message, GatewayReconciled):
            self._validate_reconciliation(message.gateway_id, message.observed)
            self._unreconciled.discard(message.gateway_id)
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, SubmitOrder):
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
        order = OrderSnapshot(self._id_factory(), strategy_id, intent, self.clock.now(),
                              replaces_order_id=replaces_order_id)
        reason = self._intent_error(intent)
        if reason:
            self._local_rejections.add(order.order_id)
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
                await self._finish_cancel(order.order_id)
            elif intent != pending.replacement:
                raise ValueError('another cancellation is already pending')
            return
        if order.status.terminal:
            return
        request_id = self._id_factory()
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
        intent = pending.replacement
        needs_child = (intent is not None and intent.quantity > order.filled_quantity
                       and not instrument.quantities_equal(intent.quantity, order.filled_quantity))
        if needs_child and not self.ready:
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

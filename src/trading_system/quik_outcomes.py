"""Resolve missing acknowledgements from durable evidence, never from absence alone."""

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any

from . import codec
from .domain import GatewayAccepted, GatewayRejected, GatewayCancelled, GatewayCancelRejected, OrderStatus, utc
from .inventory import (OUTCOME_TYPES, BrokerPosition, OmsObservation, OrderOutcome,
                        ResolveOrders, ResolvedOrderTarget)
from .persistence import canonical
from .quik_compare import ComparisonPolicy, compare_quik
from .quik_repair import ExecutionEvidence


@dataclass(frozen=True)
class OrderEvidence:
    executions: ExecutionEvidence
    outcomes: tuple[OrderOutcome, ...]

    def __post_init__(self) -> None:
        if (not isinstance(self.executions, ExecutionEvidence) or not isinstance(self.outcomes, tuple)
                or not all(isinstance(m, OUTCOME_TYPES) for m in self.outcomes)
                or any(m.gateway_id != self.executions.bindings.gateway_id for m in self.outcomes)):
            raise ValueError('order evidence requires immutable committed outcomes of one gateway')

    @property
    def digest(self) -> str:
        return sha256(canonical({'executions': self.executions.digest,
                                  'outcomes': codec.encode(self.outcomes)}).encode()).hexdigest()


def select_order_outcomes(evidence: OrderEvidence, oms: OmsObservation) -> tuple[OrderOutcome, ...]:
    if {e.key: e for e in evidence.executions.executions} != {e.key: e for e in oms.executions}:
        raise ValueError('resolve missing or conflicting executions before order outcomes')
    primary: dict[str, GatewayAccepted | GatewayRejected] = {}
    cancels: dict[tuple[str, str], GatewayCancelled | GatewayCancelRejected] = {}
    for message in evidence.outcomes:
        if isinstance(message, (GatewayAccepted, GatewayRejected)):
            prior = primary.get(message.order_id)
            if prior is not None and prior != message:
                raise ValueError('conflicting registration evidence')
            primary[message.order_id] = message
        else:
            key = message.order_id, message.request_id
            previous = cancels.get(key)
            if previous is not None and previous != message:
                raise ValueError('conflicting cancellation evidence')
            cancels[key] = message
    selected: set[OrderOutcome] = set()
    for order in oms.orders:
        if order.order_id in oms.local_rejections or order.status not in (OrderStatus.SUBMITTED, OrderStatus.CANCEL_PENDING):
            continue
        registration = primary.get(order.order_id)
        if isinstance(registration, GatewayRejected):
            selected.add(registration)
        elif order.status is OrderStatus.SUBMITTED:
            if not isinstance(registration, GatewayAccepted):
                raise ValueError('submit has no positive saved outcome')
            selected.add(registration)
        else:
            cancellation = cancels.get((order.order_id, order.cancel_request_id or ''))
            if cancellation is None or not isinstance(registration, GatewayAccepted):
                raise ValueError('cancel has no matching saved outcome and registration')
            selected.update((registration, cancellation))
    if not selected:
        raise ValueError('no unresolved order outcomes')
    # Preserve the committed publication order and fold identical repeated evidence.
    return tuple(dict.fromkeys(m for m in evidence.outcomes if m in selected))


def build_order_resolution(operation_id: str, raw: dict[str, Any], policy: ComparisonPolicy,
                           oms: OmsObservation, evidence: OrderEvidence, *, now: datetime,
                           current_revision: int, abandon_replacements: tuple[str, ...] = ()) -> ResolveOrders:
    outcomes = select_order_outcomes(evidence, oms)
    bindings = evidence.executions.bindings
    report = compare_quik(raw, policy, oms, bindings, now=now, current_revision=current_revision)
    rejections = {m.order_id: m for m in evidence.outcomes if isinstance(m, GatewayRejected)}
    rejected = {o.order_id for o in oms.orders if o.order_id in rejections}
    for order in oms.orders:
        if order.order_id in rejected and (order.filled_quantity or
                (order.status is OrderStatus.REJECTED and order.rejection_reason != rejections[order.order_id].reason)):
            raise ValueError('recorded rejection differs from saved evidence')
    for difference in report.differences:
        if difference.code == 'order_outcome_unresolved':
            continue
        if difference.code == 'order_mismatch' and set(difference.fields) == {'status'}:
            continue
        if difference.code in ('order_unmapped', 'order_not_observed') and difference.local_id in rejected:
            continue
        raise ValueError('observation contains differences outside order resolution')
    if report.observation.issues:
        raise ValueError('unverified broker observation')
    observed = {(o.route, o.broker_id): o for o in report.observation.orders}
    links = {b.local_id: (b.route, b.broker_id) for b in bindings.orders}
    targets: list[ResolvedOrderTarget] = []
    for order in oms.orders:
        if order.order_id in oms.local_rejections:
            continue
        status = order.status
        for message in outcomes:
            if message.order_id != order.order_id:
                continue
            if isinstance(message, GatewayAccepted) and status is OrderStatus.SUBMITTED:
                status = OrderStatus.ACCEPTED
            elif isinstance(message, GatewayRejected):
                if order.filled_quantity or order.order_id in links:
                    raise ValueError('rejection contradicts broker registration or execution')
                status = OrderStatus.REJECTED
            elif isinstance(message, GatewayCancelled):
                if message.filled_quantity != order.filled_quantity:
                    raise ValueError('cancel barrier awaits missing executions')
                status = OrderStatus.FILLED if order.filled_quantity == order.intent.quantity else OrderStatus.CANCELLED
            elif isinstance(message, GatewayCancelRejected):
                status = OrderStatus.PARTIALLY_FILLED if order.filled_quantity else OrderStatus.ACCEPTED
        target = observed.get(links[order.order_id]) if order.order_id in links else None
        if order.order_id not in rejected and (target is None or target.status != status):
            raise ValueError('saved outcome and observed order status disagree')
        targets.append(ResolvedOrderTarget(order.order_id, order.intent, order.filled_quantity, status))
    return ResolveOrders(operation_id, oms.revision, policy.gateway_id, policy.session_id, bindings.revision,
        evidence.digest, sha256(canonical(raw).encode()).hexdigest(), utc(datetime.fromisoformat(raw['finished_at'])),
        utc(now), utc(datetime.fromisoformat(raw['started_at'])) + policy.max_age, outcomes, tuple(targets),
        tuple(BrokerPosition(p.route, p.quantity) for p in report.observation.positions), abandon_replacements)

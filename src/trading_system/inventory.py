"""Explicit inventory cutovers and operator ownership commands, never synthetic fills."""

from dataclasses import dataclass
from datetime import datetime
from math import fsum, isclose
import re

from .domain import Execution, OrderIntent, OrderSnapshot, OrderStatus, Position, Route, finite, positive, utc
from .domain import GatewayAccepted, GatewayRejected, GatewayCancelled, GatewayCancelRejected

MANUAL = 'manual'


@dataclass(frozen=True)
class OmsObservation:
    """Read-only export at one OMS revision; never an inventory command."""

    revision: int
    gateway_id: str
    captured_at: datetime
    orders: tuple[OrderSnapshot, ...]
    executions: tuple[Execution, ...]
    positions: tuple[tuple[str, Position], ...]
    local_rejections: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.revision) is not int or self.revision < 0 or not self.gateway_id:
            raise ValueError('observation requires a gateway and nonnegative revision')
        object.__setattr__(self, 'captured_at', utc(self.captured_at))
        if (not isinstance(self.orders, tuple) or not all(isinstance(o, OrderSnapshot) for o in self.orders)
                or not isinstance(self.executions, tuple) or not all(isinstance(e, Execution) for e in self.executions)
                or not isinstance(self.positions, tuple)
                or not all(isinstance(p, tuple) and len(p) == 2 and isinstance(p[0], str)
                           and isinstance(p[1], Position) for p in self.positions)
                or not isinstance(self.local_rejections, tuple)
                or not all(isinstance(i, str) for i in self.local_rejections)):
            raise ValueError('observation requires immutable typed tuples')


def _identity(operation_id: str, expected_revision: int) -> None:
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError('operation identifier must be nonempty')
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError('expected revision must be a nonnegative integer')


def _quantity(route: Route, quantity: float, *, allow_zero: bool = False) -> None:
    if not isinstance(route, Route):
        raise ValueError('inventory requires a route')
    finite(quantity, 'inventory quantity')
    if quantity == 0 and allow_zero:
        return
    if not route.instrument.accepts_quantity(abs(quantity)):
        raise ValueError('inventory quantity must match instrument step')


@dataclass(frozen=True)
class OpeningBalance:
    """Untracked residual with its own cost basis, not the broker aggregate basis."""

    route: Route
    quantity: float
    average_price: float

    def __post_init__(self) -> None:
        _quantity(self.route, self.quantity)
        finite(self.average_price, 'opening cost basis')


@dataclass(frozen=True)
class BrokerPosition:
    route: Route
    quantity: float

    def __post_init__(self) -> None:
        _quantity(self.route, self.quantity, allow_zero=True)


@dataclass(frozen=True)
class ExternalOrder:
    order: OrderSnapshot
    executions: tuple[Execution, ...]

    def __post_init__(self) -> None:
        order = self.order
        if not isinstance(order, OrderSnapshot) or not order.order_id:
            raise ValueError('external order requires an identifier')
        utc(order.submitted_at)
        if (not isinstance(order.status, OrderStatus)
                or order.status in (OrderStatus.SUBMITTED, OrderStatus.CANCEL_PENDING)
                or any((order.cancel_request_id, order.cancel_rejection_reason,
                        order.replaces_order_id, order.replacement_order_id))):
            raise ValueError('external order requires a settled broker acknowledgement')
        _quantity(order.intent.route, order.intent.quantity)
        _quantity(order.intent.route, order.filled_quantity, allow_zero=True)
        if not 0 <= order.filled_quantity <= order.intent.quantity:
            raise ValueError('external order cumulative quantity is invalid')
        if ((order.status is OrderStatus.FILLED) != (order.filled_quantity == order.intent.quantity)
                or (order.status in (OrderStatus.ACCEPTED, OrderStatus.REJECTED) and order.filled_quantity != 0)
                or (order.status is OrderStatus.PARTIALLY_FILLED and order.filled_quantity == 0)):
            raise ValueError('external order status contradicts cumulative quantity')
        finite(order.average_fill_price, 'external average price')
        finite(order.commission, 'external commission')
        if not isinstance(self.executions, tuple) or not all(isinstance(e, Execution) for e in self.executions):
            raise ValueError('external fills must be an immutable typed tuple')
        if len({e.key for e in self.executions}) != len(self.executions):
            raise ValueError('duplicate historical execution')
        for execution in self.executions:
            if (execution.order_id != order.order_id or execution.route != order.intent.route
                    or execution.side != order.intent.side or execution.timestamp < order.submitted_at
                    or not order.intent.accepts_price(execution.price)):
                raise ValueError('external execution contradicts its order')
            _quantity(execution.route, execution.quantity)
        total = fsum(e.quantity for e in self.executions)
        average = fsum(e.price * (e.quantity / total) for e in self.executions) if total else 0
        if (not order.intent.route.instrument.quantities_equal(total, order.filled_quantity)
                or not isclose(average, order.average_fill_price, rel_tol=1e-12, abs_tol=1e-9)
                or not isclose(fsum(e.commission for e in self.executions), order.commission,
                               rel_tol=1e-12, abs_tol=1e-9)):
            raise ValueError('external order needs complete historical execution identifiers')


@dataclass(frozen=True)
class InventoryReport:
    operation_id: str
    expected_revision: int
    gateway_id: str
    as_of: datetime
    openings: tuple[OpeningBalance, ...]
    imports: tuple[ExternalOrder, ...]
    observed_positions: tuple[BrokerPosition, ...]
    observed_orders: tuple[OrderSnapshot, ...]

    def __post_init__(self) -> None:
        _identity(self.operation_id, self.expected_revision)
        if not isinstance(self.gateway_id, str) or not self.gateway_id:
            raise ValueError('inventory gateway is required')
        object.__setattr__(self, 'as_of', utc(self.as_of))
        for items, cls in ((self.openings, OpeningBalance), (self.imports, ExternalOrder),
                           (self.observed_positions, BrokerPosition), (self.observed_orders, OrderSnapshot)):
            if not isinstance(items, tuple) or not all(isinstance(item, cls) for item in items):
                raise ValueError('inventory report requires immutable typed tuples')
        if (len({p.route for p in self.openings}) != len(self.openings)
                or len({p.route for p in self.observed_positions}) != len(self.observed_positions)):
            raise ValueError('duplicate inventory route')


@dataclass(frozen=True)
class TransferPosition:
    operation_id: str
    expected_revision: int
    source_id: str
    target_id: str
    route: Route
    quantity: float

    def __post_init__(self) -> None:
        _identity(self.operation_id, self.expected_revision)
        if (not all(isinstance(s, str) and s.strip() for s in (self.source_id, self.target_id))
                or self.source_id == self.target_id):
            raise ValueError('transfer requires different nonempty owners')
        positive(self.quantity, 'transfer quantity')
        _quantity(self.route, self.quantity)


@dataclass(frozen=True)
class CloseManual:
    operation_id: str
    expected_revision: int
    route: Route
    quantity: float
    limit_price: float

    def __post_init__(self) -> None:
        _identity(self.operation_id, self.expected_revision)
        positive(self.quantity, 'close quantity')
        _quantity(self.route, self.quantity)
        finite(self.limit_price, 'close limit price')


@dataclass(frozen=True)
class RepairOrderTarget:
    """Observed cumulative facts, without deciding a pending cancellation."""

    order_id: str
    intent: OrderIntent
    filled_quantity: float

    def __post_init__(self) -> None:
        if not isinstance(self.order_id, str) or not self.order_id or not isinstance(self.intent, OrderIntent):
            raise ValueError('repair target needs a known order and intent')
        _quantity(self.intent.route, self.filled_quantity, allow_zero=True)
        if not 0 <= self.filled_quantity <= self.intent.quantity:
            raise ValueError('invalid repair cumulative quantity')


@dataclass(frozen=True)
class RepairExecutions:
    """Journaled positive evidence; never a reconciliation or resume command."""

    operation_id: str
    expected_revision: int
    gateway_id: str
    session_id: str
    source_revision: int
    source_digest: str
    snapshot_digest: str
    observed_at: datetime
    created_at: datetime
    expires_at: datetime
    executions: tuple[Execution, ...]
    order_targets: tuple[RepairOrderTarget, ...]
    observed_positions: tuple[BrokerPosition, ...]

    def __post_init__(self) -> None:
        _identity(self.operation_id, self.expected_revision)
        _identity(self.session_id, self.source_revision)
        if not isinstance(self.gateway_id, str) or not self.gateway_id:
            raise ValueError('repair gateway is required')
        for digest in (self.source_digest, self.snapshot_digest):
            if not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None:
                raise ValueError('repair requires SHA-256 evidence digests')
        for name in ('observed_at', 'created_at', 'expires_at'):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if not self.observed_at <= self.created_at <= self.expires_at or self.observed_at == self.expires_at:
            raise ValueError('invalid repair observation window')
        for group, kind in ((self.executions, Execution), (self.order_targets, RepairOrderTarget),
                            (self.observed_positions, BrokerPosition)):
            if (not isinstance(group, tuple) or not 0 < len(group) <= 10_000
                    or not all(isinstance(item, kind) for item in group)):
                raise ValueError('repair needs bounded nonempty immutable typed tuples')
        if (len({e.key for e in self.executions}) != len(self.executions)
                or len({o.order_id for o in self.order_targets}) != len(self.order_targets)
                or len({p.route for p in self.observed_positions}) != len(self.observed_positions)):
            raise ValueError('duplicate repair identity')
        routes = {p.route for p in self.observed_positions}
        targets = {o.order_id: o for o in self.order_targets}
        if (any(r.instrument.id.gateway_id != self.gateway_id for r in routes)
                or any(o.intent.route not in routes for o in self.order_targets)):
            raise ValueError('repair routes must belong to the observed gateway')
        for execution in self.executions:
            target = targets.get(execution.order_id)
            if (target is None or execution.route != target.intent.route or execution.side != target.intent.side
                    or execution.timestamp > self.observed_at or not target.intent.accepts_price(execution.price)):
                raise ValueError('execution contradicts repair target')
            _quantity(execution.route, execution.quantity)


type OrderOutcome = GatewayAccepted | GatewayRejected | GatewayCancelled | GatewayCancelRejected
OUTCOME_TYPES = (GatewayAccepted, GatewayRejected, GatewayCancelled, GatewayCancelRejected)


@dataclass(frozen=True)
class ResolvedOrderTarget(RepairOrderTarget):
    status: OrderStatus

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.status, OrderStatus) or self.status in (OrderStatus.SUBMITTED, OrderStatus.CANCEL_PENDING):
            raise ValueError('resolution target must have a known outcome')


@dataclass(frozen=True)
class ResolveOrders:
    """Apply saved acknowledgements; abandoning a replacement is an explicit decision."""

    operation_id: str
    expected_revision: int
    gateway_id: str
    session_id: str
    source_revision: int
    source_digest: str
    snapshot_digest: str
    observed_at: datetime
    created_at: datetime
    expires_at: datetime
    outcomes: tuple[OrderOutcome, ...]
    order_targets: tuple[ResolvedOrderTarget, ...]
    observed_positions: tuple[BrokerPosition, ...]
    abandon_replacements: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identity(self.operation_id, self.expected_revision)
        _identity(self.session_id, self.source_revision)
        if not isinstance(self.gateway_id, str) or not self.gateway_id:
            raise ValueError('resolution gateway is required')
        for digest in (self.source_digest, self.snapshot_digest):
            if not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None:
                raise ValueError('resolution requires SHA-256 evidence digests')
        for name in ('observed_at', 'created_at', 'expires_at'):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if not self.observed_at <= self.created_at <= self.expires_at or self.observed_at == self.expires_at:
            raise ValueError('invalid resolution observation window')
        for group, kind in ((self.outcomes, OUTCOME_TYPES), (self.order_targets, ResolvedOrderTarget),
                            (self.observed_positions, BrokerPosition)):
            if (not isinstance(group, tuple) or not 0 < len(group) <= 10_000
                    or not all(isinstance(item, kind) for item in group)):
                raise ValueError('resolution requires bounded immutable typed tuples')
        targets = {o.order_id: o for o in self.order_targets}
        routes = {p.route for p in self.observed_positions}
        if (len(targets) != len(self.order_targets) or len(routes) != len(self.observed_positions)
                or len(set(self.outcomes)) != len(self.outcomes)
                or any(r.instrument.id.gateway_id != self.gateway_id for r in routes)
                or any(o.intent.route not in routes for o in self.order_targets)
                or any(m.gateway_id != self.gateway_id or m.order_id not in targets for m in self.outcomes)):
            raise ValueError('ambiguous or foreign resolution scope')
        if (not isinstance(self.abandon_replacements, tuple)
                or not all(isinstance(i, str) and i for i in self.abandon_replacements)
                or len(set(self.abandon_replacements)) != len(self.abandon_replacements)
                or not set(self.abandon_replacements) <= {m.order_id for m in self.outcomes if isinstance(m, GatewayCancelled)}):
            raise ValueError('replacement decision must name distinct confirmed cancellations')


type InventoryCommand = InventoryReport | TransferPosition | CloseManual | RepairExecutions | ResolveOrders

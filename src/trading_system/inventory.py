"""Explicit inventory cutovers and operator ownership commands, never synthetic fills."""

from dataclasses import dataclass
from datetime import datetime
from math import fsum, isclose

from .domain import Execution, OrderSnapshot, OrderStatus, Route, finite, positive, utc

MANUAL = 'manual'


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


type InventoryCommand = InventoryReport | TransferPosition | CloseManual

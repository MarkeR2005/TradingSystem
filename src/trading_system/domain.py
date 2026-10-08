"""Immutable data crossing component boundaries."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from math import isclose, isfinite


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('timestamp must be timezone-aware')
    return value.astimezone(UTC)


def finite(value: float, name: str) -> None:
    if not isfinite(value):
        raise ValueError(f'{name} must be finite')


def positive(value: float, name: str) -> None:
    finite(value, name)
    if value <= 0:
        raise ValueError(f'{name} must be positive')


class Side(str, Enum):
    BUY = 'buy'
    SELL = 'sell'

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1


class OrderStatus(str, Enum):
    SUBMITTED = 'submitted'
    ACCEPTED = 'accepted'
    PARTIALLY_FILLED = 'partially_filled'
    CANCEL_PENDING = 'cancel_pending'
    CANCELLED = 'cancelled'
    FILLED = 'filled'
    REJECTED = 'rejected'

    @property
    def terminal(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


@dataclass(frozen=True)
class InstrumentId:
    gateway_id: str
    venue: str
    symbol: str

    def __post_init__(self) -> None:
        if not all((self.gateway_id, self.venue, self.symbol)):
            raise ValueError('instrument identifiers must not be empty')


@dataclass(frozen=True)
class Instrument:
    id: InstrumentId
    currency: str
    quantity_step: float = 1.0
    contract_multiplier: float = 1.0

    def __post_init__(self) -> None:
        if not self.currency:
            raise ValueError('currency must not be empty')
        positive(self.quantity_step, 'quantity_step')
        positive(self.contract_multiplier, 'contract_multiplier')

    def accepts_quantity(self, quantity: float) -> bool:
        units = quantity / self.quantity_step
        if not isfinite(units):
            return False
        nearest = round(units)
        return nearest >= 1 and isclose(units, nearest, rel_tol=0, abs_tol=1e-9)

    def quantities_equal(self, left: float, right: float) -> bool:
        return isclose(left, right, rel_tol=0, abs_tol=self.quantity_step * 1e-9)


@dataclass(frozen=True)
class Route:
    instrument: Instrument
    account_id: str

    def __post_init__(self) -> None:
        if not self.account_id:
            raise ValueError('account_id must not be empty')


@dataclass(frozen=True)
class Candle:
    instrument_id: InstrumentId
    opened_at: datetime
    timeframe: timedelta
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        object.__setattr__(self, 'opened_at', utc(self.opened_at))
        if self.timeframe <= timedelta(0):
            raise ValueError('timeframe must be positive')
        for name in ('open', 'high', 'low', 'close', 'volume'):
            finite(getattr(self, name), name)
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError('OHLC values are inconsistent')
        if self.volume < 0:
            raise ValueError('volume must not be negative')

    @property
    def closed_at(self) -> datetime:
        return self.opened_at + self.timeframe


@dataclass(frozen=True)
class OrderIntent:
    """Market intent when limit_price is None, otherwise a limit intent."""

    route: Route
    side: Side
    quantity: float
    limit_price: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.side, Side):
            raise ValueError('side must be a Side')
        positive(self.quantity, 'quantity')
        if self.limit_price is not None:
            finite(self.limit_price, 'limit price')

    def accepts_price(self, price: float) -> bool:
        if self.limit_price is None:
            return True
        return price <= self.limit_price if self.side is Side.BUY else price >= self.limit_price


@dataclass(frozen=True)
class OrderSnapshot:
    order_id: str
    strategy_id: str
    intent: OrderIntent
    submitted_at: datetime
    status: OrderStatus = OrderStatus.SUBMITTED
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    commission: float = 0.0
    rejection_reason: str | None = None
    cancel_request_id: str | None = None
    cancel_rejection_reason: str | None = None
    replaces_order_id: str | None = None
    replacement_order_id: str | None = None

    @property
    def remaining_quantity(self) -> float:
        return max(0.0, self.intent.quantity - self.filled_quantity)


@dataclass(frozen=True)
class Execution:
    execution_id: str
    order_id: str
    route: Route
    side: Side
    quantity: float
    price: float
    commission: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if not self.execution_id or not self.order_id:
            raise ValueError('execution and order identifiers must not be empty')
        if not isinstance(self.side, Side):
            raise ValueError('side must be a Side')
        positive(self.quantity, 'execution quantity')
        finite(self.price, 'execution price')
        finite(self.commission, 'commission')
        object.__setattr__(self, 'timestamp', utc(self.timestamp))

    @property
    def key(self) -> tuple[str, str]:
        return self.route.instrument.id.gateway_id, self.execution_id


@dataclass(frozen=True)
class Position:
    route: Route
    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0


@dataclass(frozen=True)
class Trade:
    opened_at: datetime
    closed_at: datetime
    pnl: float


@dataclass(frozen=True)
class EquityPoint:
    timestamp: datetime
    value: float


@dataclass(frozen=True)
class CandleReceived:
    candle: Candle


@dataclass(frozen=True)
class SubmitOrder:
    strategy_id: str
    intent: OrderIntent


@dataclass(frozen=True)
class CancelOrder:
    strategy_id: str
    order_id: str


@dataclass(frozen=True)
class ReplaceOrder:
    strategy_id: str
    order_id: str
    intent: OrderIntent


@dataclass(frozen=True)
class GatewaySubmit:
    order: OrderSnapshot


@dataclass(frozen=True)
class GatewayAccepted:
    order_id: str
    gateway_id: str


@dataclass(frozen=True)
class GatewayCancel:
    order_id: str
    gateway_id: str
    request_id: str


@dataclass(frozen=True)
class GatewayCancelled:
    """Final cumulative executed quantity at the gateway's cancellation barrier."""

    order_id: str
    gateway_id: str
    request_id: str
    filled_quantity: float

    def __post_init__(self) -> None:
        if not all((self.order_id, self.gateway_id, self.request_id)):
            raise ValueError('cancellation identifiers must not be empty')
        finite(self.filled_quantity, 'cancelled order filled quantity')
        if self.filled_quantity < 0:
            raise ValueError('cancelled order filled quantity must not be negative')


@dataclass(frozen=True)
class GatewayCancelRejected:
    order_id: str
    gateway_id: str
    request_id: str
    reason: str

    def __post_init__(self) -> None:
        if not all((self.order_id, self.gateway_id, self.request_id, self.reason)):
            raise ValueError('cancellation identifiers and reason must not be empty')


@dataclass(frozen=True)
class GatewayRejected:
    order_id: str
    gateway_id: str
    reason: str

    def __post_init__(self) -> None:
        if not all((self.order_id, self.gateway_id, self.reason)):
            raise ValueError('rejection identifiers and reason must not be empty')


@dataclass(frozen=True)
class OrderUpdated:
    order: OrderSnapshot


@dataclass(frozen=True)
class ExecutionApplied:
    execution: Execution


type Message = (
    CandleReceived | SubmitOrder | CancelOrder | ReplaceOrder | GatewaySubmit
    | GatewayAccepted | GatewayCancel | GatewayCancelled | GatewayCancelRejected
    | GatewayRejected | OrderUpdated | Execution | ExecutionApplied
)

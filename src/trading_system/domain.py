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
    FILLED = 'filled'
    REJECTED = 'rejected'


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
    """Market intent for iteration one; other order types follow separately."""

    route: Route
    side: Side
    quantity: float

    def __post_init__(self) -> None:
        if not isinstance(self.side, Side):
            raise ValueError('side must be a Side')
        positive(self.quantity, 'quantity')


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
class GatewaySubmit:
    order: OrderSnapshot


@dataclass(frozen=True)
class GatewayAccepted:
    order_id: str
    gateway_id: str


@dataclass(frozen=True)
class OrderUpdated:
    order: OrderSnapshot


@dataclass(frozen=True)
class ExecutionApplied:
    execution: Execution


type Message = CandleReceived | SubmitOrder | GatewaySubmit | GatewayAccepted | OrderUpdated | Execution | ExecutionApplied

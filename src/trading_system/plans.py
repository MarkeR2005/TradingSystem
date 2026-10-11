"""Immutable multi-leg plans and explicit offline risk inputs."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from .domain import OrderIntent, Route, finite, positive, utc


class FailurePolicy(str, Enum):
    PROTECT = 'protect'
    COMPLETE = 'complete'
    CLOSE = 'close'


class PlanPhase(str, Enum):
    ENTERING = 'entering'
    CANCELLING = 'cancelling'
    COMPLETING = 'completing'
    OPEN = 'open'
    HELD = 'held'
    CLOSING = 'closing'
    BLOCKED = 'blocked'
    CLOSED = 'closed'


def name(value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError('plan identifiers must be nonempty')


@dataclass(frozen=True)
class PlanLeg:
    intent: OrderIntent
    exit_limit: float

    def __post_init__(self) -> None:
        if not isinstance(self.intent, OrderIntent) or self.intent.limit_price is None:
            raise ValueError('plan legs require limit intents')
        if not self.intent.route.instrument.accepts_quantity(self.intent.quantity):
            raise ValueError('plan quantity must match instrument step')
        finite(self.exit_limit, 'exit limit')


@dataclass(frozen=True)
class TradePlan:
    plan_id: str
    strategy_id: str
    legs: tuple[PlanLeg, ...]
    failure_policy: FailurePolicy
    expires_at: datetime
    stop_loss: float
    recovery_timeout: float = 30

    def __post_init__(self) -> None:
        name(self.plan_id)
        name(self.strategy_id)
        if not isinstance(self.failure_policy, FailurePolicy):
            raise ValueError('plan must declare a failure policy')
        if not isinstance(self.legs, tuple) or not all(isinstance(leg, PlanLeg) for leg in self.legs):
            raise ValueError('plan requires immutable typed legs')
        if len(self.legs) < 2 or len({leg.intent.route for leg in self.legs}) != len(self.legs):
            raise ValueError('plan requires at least two distinct routes')
        object.__setattr__(self, 'expires_at', utc(self.expires_at))
        if isinstance(self.stop_loss, bool) or isinstance(self.recovery_timeout, bool):
            raise ValueError('plan risk limits must be numeric, not bool')
        positive(self.stop_loss, 'plan stop loss')
        positive(self.recovery_timeout, 'plan recovery timeout')
        try:
            self.expires_at + timedelta(seconds=self.recovery_timeout)
        except OverflowError as error:
            raise ValueError('recovery timeout exceeds the supported clock range') from error


@dataclass(frozen=True)
class SubmitPlan:
    plan: TradePlan


@dataclass(frozen=True)
class ClosePlan:
    strategy_id: str
    plan_id: str

    def __post_init__(self) -> None:
        name(self.strategy_id)
        name(self.plan_id)


@dataclass(frozen=True)
class CheckPlanDeadlines:
    """A deterministic timer tick evaluated at the OMS clock, not wall-clock sleep."""


@dataclass(frozen=True)
class PlanPrice:
    route: Route
    price: float

    def __post_init__(self) -> None:
        if not isinstance(self.route, Route):
            raise ValueError('plan price requires a route')
        finite(self.price, 'plan price')


def prices(items: tuple[PlanPrice, ...]) -> None:
    if not isinstance(items, tuple) or not all(isinstance(item, PlanPrice) for item in items):
        raise ValueError('plan prices require an immutable typed tuple')
    if len({item.route for item in items}) != len(items):
        raise ValueError('duplicate plan price route')


@dataclass(frozen=True)
class PlanMarks:
    plan_id: str
    sequence: int
    as_of: datetime
    expires_at: datetime
    prices: tuple[PlanPrice, ...]

    def __post_init__(self) -> None:
        name(self.plan_id)
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError('mark sequence must be a positive integer')
        object.__setattr__(self, 'as_of', utc(self.as_of))
        object.__setattr__(self, 'expires_at', utc(self.expires_at))
        prices(self.prices)


def operation(operation_id: str, expected_revision: int) -> None:
    name(operation_id)
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError('expected revision must be nonnegative')


@dataclass(frozen=True)
class RetryPlan:
    """Operator authorizes a bounded exit after blocked/held recovery; never another completion attempt."""

    operation_id: str
    expected_revision: int
    plan_id: str
    exit_prices: tuple[PlanPrice, ...]

    def __post_init__(self) -> None:
        operation(self.operation_id, self.expected_revision)
        name(self.plan_id)
        prices(self.exit_prices)


@dataclass(frozen=True)
class AcknowledgePlan:
    operation_id: str
    expected_revision: int
    plan_id: str

    def __post_init__(self) -> None:
        operation(self.operation_id, self.expected_revision)
        name(self.plan_id)


@dataclass(frozen=True)
class PlanAlert:
    timestamp: datetime
    reason: str


@dataclass(frozen=True)
class PlanSnapshot:
    plan: TradePlan
    phase: PlanPhase
    deadline: datetime
    entry_ids: tuple[str, ...] = ()
    exit_ids: tuple[str, ...] = ()
    paused: bool = False
    alerts: tuple[PlanAlert, ...] = ()
    goal: FailurePolicy | None = None
    recovery_attempted: bool = False
    close_requested: bool = False
    exit_prices: tuple[PlanPrice, ...] = ()
    marks: PlanMarks | None = None
    pnl: float | None = None


type PlanCommand = SubmitPlan | ClosePlan | CheckPlanDeadlines | PlanMarks | RetryPlan | AcknowledgePlan

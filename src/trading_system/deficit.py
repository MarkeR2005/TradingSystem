"""Explicit, immutable commands for account-deficit recovery in the offline OMS."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from fractions import Fraction

from .capital import AccountKey
from .domain import Route, finite, positive, utc
from .plans import PlanAlert, PlanPrice, name, operation, prices


class DeficitPhase(str, Enum):
    WAITING = 'waiting'
    CANCELLING = 'cancelling'
    REDUCING = 'reducing'
    BLOCKED = 'blocked'
    RESOLVED = 'resolved'


@dataclass(frozen=True)
class DeficitQuotes:
    as_of: datetime
    expires_at: datetime
    prices: tuple[PlanPrice, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, 'as_of', utc(self.as_of))
        object.__setattr__(self, 'expires_at', utc(self.expires_at))
        if self.expires_at <= self.as_of:
            raise ValueError('deficit quote validity interval is empty')
        prices(self.prices)


@dataclass(frozen=True)
class StartDeficit:
    operation_id: str
    expected_revision: int
    account: AccountKey
    wait_seconds: float
    order_timeout: float
    quotes: DeficitQuotes

    def __post_init__(self) -> None:
        operation(self.operation_id, self.expected_revision)
        if not isinstance(self.account, AccountKey) or not isinstance(self.quotes, DeficitQuotes):
            raise ValueError('deficit requires a typed account and quotes')
        for value in (self.wait_seconds, self.order_timeout):
            if isinstance(value, bool):
                raise ValueError('deficit durations must not be bool')
            finite(value, 'deficit duration')
        if self.wait_seconds < 0:
            raise ValueError('deficit wait must be nonnegative')
        positive(self.order_timeout, 'deficit order timeout')
        try:
            self.quotes.expires_at + timedelta(seconds=self.wait_seconds + self.order_timeout)
        except (OverflowError, ValueError) as error:
            raise ValueError('deficit duration exceeds supported clock range') from error


@dataclass(frozen=True)
class RetryDeficit:
    operation_id: str
    expected_revision: int
    case_id: str
    quotes: DeficitQuotes

    def __post_init__(self) -> None:
        operation(self.operation_id, self.expected_revision)
        name(self.case_id)
        if not isinstance(self.quotes, DeficitQuotes):
            raise ValueError('retry requires typed quotes')


@dataclass(frozen=True)
class AcknowledgeDeficit:
    operation_id: str
    expected_revision: int
    case_id: str

    def __post_init__(self) -> None:
        operation(self.operation_id, self.expected_revision)
        name(self.case_id)


@dataclass(frozen=True)
class CheckDeficits:
    """Deterministic timer tick. No background scheduler or broker I/O here."""


@dataclass(frozen=True)
class DeficitGroup:
    strategy_id: str
    routes: tuple[Route, ...]
    plan_id: str | None = None


@dataclass(frozen=True)
class ReductionTarget:
    group: DeficitGroup
    quantities: tuple[tuple[Route, Fraction], ...]
    initial: tuple[tuple[Route, Fraction], ...]


@dataclass(frozen=True)
class DeficitSnapshot:
    request: StartDeficit
    phase: DeficitPhase
    deadline: datetime
    groups: tuple[DeficitGroup, ...]
    quotes: DeficitQuotes
    alerts: tuple[PlanAlert, ...]
    wave: tuple[str, ...] = ()
    exit_ids: tuple[str, ...] = ()
    acknowledged: bool = False
    target: ReductionTarget | None = None


type DeficitCommand = StartDeficit | RetryDeficit | AcknowledgeDeficit | CheckDeficits

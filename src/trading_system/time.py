from datetime import datetime
from typing import Protocol

from .domain import utc


class Clock(Protocol):
    def now(self) -> datetime: ...


class ManualClock:
    """Controlled time for simulations; timers are a later iteration."""

    def __init__(self, initial: datetime) -> None:
        self._now = utc(initial)

    def now(self) -> datetime:
        return self._now

    def advance_to(self, timestamp: datetime) -> None:
        timestamp = utc(timestamp)
        if timestamp < self._now:
            raise ValueError('clock cannot move backwards')
        self._now = timestamp

"""Transport delivery cursors. They are not broker-state or OMS cutovers."""

from dataclasses import dataclass
import re

CURSOR_CAPABILITY = 'callback_cursor_v1'
MAX_CURSOR = 2**31 - 1


@dataclass(frozen=True)
class CallbackCursor:
    bridge_session: str
    sequence: int
    failed_sends: int

    def __post_init__(self) -> None:
        if not isinstance(self.bridge_session, str) or not re.fullmatch(r'[0-9a-f]{32}', self.bridge_session):
            raise ValueError('invalid callback bridge session')
        if any(type(n) is not int or not 0 <= n <= MAX_CURSOR for n in (self.sequence, self.failed_sends)):
            raise ValueError('invalid or exhausted callback cursor counter')

    @classmethod
    def parse(cls, value: object) -> 'CallbackCursor':
        if not isinstance(value, dict) or set(value) != {'bridge_session', 'sequence', 'failed_sends'}:
            raise ValueError('invalid callback cursor schema')
        return cls(value['bridge_session'], value['sequence'], value['failed_sends'])


class CallbackTracker:
    """A connection starts at its runtime-info cursor, not at script sequence 0.

    Earlier frames may already be in the callback socket. Their metadata is
    checked, but the baseline does not prove delivery before this connection.
    The callback channel has one writer; response-channel cursors may lag it.
    """

    def __init__(self, baseline: CallbackCursor) -> None:
        self.baseline = baseline
        self.received_sequence = baseline.sequence
        self._wire_sequence: int | None = None
        self._target_sequence = baseline.sequence

    def observe(self, cursor: CallbackCursor) -> None:
        if cursor.bridge_session != self.baseline.bridge_session:
            raise ValueError('callback belongs to another bridge session')
        if (cursor.failed_sends > self.baseline.failed_sends
                or (cursor.sequence > self.baseline.sequence and cursor.failed_sends != self.baseline.failed_sends)):
            raise ValueError('callback delivery failure since connection baseline')
        if (cursor.sequence == 0
                or (self._wire_sequence is not None and cursor.sequence != self._wire_sequence + 1)
                or (cursor.sequence > self.baseline.sequence and cursor.sequence != self.received_sequence + 1)):
            raise ValueError('callback sequence gap, duplicate or regression')
        self._wire_sequence = cursor.sequence
        self.received_sequence = max(self.received_sequence, cursor.sequence)

    def target(self, cursor: CallbackCursor) -> None:
        if cursor.bridge_session != self.baseline.bridge_session:
            raise ValueError('cursor belongs to another bridge session')
        if cursor.failed_sends != self.baseline.failed_sends:
            raise ValueError('callback delivery failure since connection baseline')
        if cursor.sequence < self._target_sequence:
            raise ValueError('callback cursor sequence regressed')
        self._target_sequence = cursor.sequence

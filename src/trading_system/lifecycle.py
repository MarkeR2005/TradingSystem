"""Durable configuration, lifecycle and explicit computation checkpoints."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from .codec import decode, encode
from .domain import InstrumentId, utc
from .persistence import DurableJournal, JournalError
from .state import JsonObject, json_object


class StrategyStatus(str, Enum):
    RUNNING = 'running'
    PAUSED = 'paused'
    OPTIMIZING = 'optimizing'
    FAILED = 'failed'


@dataclass(frozen=True)
class StrategySnapshot:
    name: str
    strategy_type: str
    state_version: int
    instruments: tuple[InstrumentId, ...]
    parameters: JsonObject
    state: JsonObject
    status: StrategyStatus
    reason: str | None = None
    resume_after: StrategyStatus | None = None


def decode_strategy_record(record: dict[str, Any]) -> tuple[datetime, StrategySnapshot]:
    try:
        if set(record) != {'kind', 'version', 'time', 'snapshot'} or record['kind'] != 'strategy' or type(record['version']) is not int or record['version'] != 1:
            raise ValueError('unsupported strategy schema')
        values = dict(record['snapshot'])
        if set(values) != {field for field in StrategySnapshot.__dataclass_fields__}:
            raise ValueError('invalid strategy snapshot fields')
        timestamp = utc(datetime.fromisoformat(record['time']))
        values['instruments'] = decode(values['instruments'])
        if (not isinstance(values['instruments'], tuple)
                or not all(isinstance(item, InstrumentId) for item in values['instruments'])
                or len(set(values['instruments'])) != len(values['instruments'])):
            raise ValueError('invalid strategy instruments')
        if (not isinstance(values['name'], str) or not values['name']
                or not isinstance(values['strategy_type'], str) or not values['strategy_type']
                or type(values['state_version']) is not int or values['state_version'] < 1
                or (values['reason'] is not None and not isinstance(values['reason'], str))):
            raise ValueError('invalid strategy identity')
        values['parameters'] = json_object(values['parameters'])
        values['state'] = json_object(values['state'])
        values['status'] = StrategyStatus(values['status'])
        values['resume_after'] = StrategyStatus(values['resume_after']) if values['resume_after'] is not None else None
        if ((values['status'] is StrategyStatus.OPTIMIZING
             and values['resume_after'] not in (StrategyStatus.RUNNING, StrategyStatus.PAUSED))
                or (values['status'] is not StrategyStatus.OPTIMIZING and values['resume_after'] is not None)):
            raise ValueError('invalid optimization resume status')
        return timestamp, StrategySnapshot(**values)
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise JournalError('invalid strategy checkpoint') from error


class LifecycleStore:
    def __init__(self, journal: DurableJournal | None) -> None:
        self.journal = journal
        self._saved: dict[str, StrategySnapshot] = {}

    def restore(self) -> None:
        if self.journal is None:
            return
        for record in self.journal.records:
            if 'kind' not in record:
                continue  # Legacy OMS records are validated by OrderManager.restore.
            _, snapshot = decode_strategy_record(record)
            self._saved[snapshot.name] = snapshot

    def get(self, name: str) -> StrategySnapshot | None:
        return deepcopy(self._saved.get(name))

    def save(self, snapshot: StrategySnapshot, timestamp: datetime) -> None:
        payload = {key: getattr(snapshot, key) for key in snapshot.__dataclass_fields__}
        payload['instruments'] = encode(snapshot.instruments)
        payload['parameters'] = json_object(snapshot.parameters)
        payload['state'] = json_object(snapshot.state)
        record = {'kind': 'strategy', 'version': 1, 'time': utc(timestamp).isoformat(), 'snapshot': payload}
        _, copied = decode_strategy_record(record)
        if self.journal is not None:
            self.journal.append(record)
        self._saved[snapshot.name] = copied

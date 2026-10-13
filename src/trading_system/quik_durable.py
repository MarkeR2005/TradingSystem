"""Durable QUIK protocol boundary, still offline: prepared does not mean sent.

Inputs are fsynced before evaluation; derived transactions and OMS messages are
fsynced before exposure. Recovery redelivers financial messages, never orders.
The caller owns the dedicated DurableJournal and must keep it open.
"""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

from . import codec
from .domain import GatewayCancel, GatewaySubmit, GatewayUncertain, Message, utc
from .events import EventBus
from .orders import gateway_recipient
from .persistence import DurableJournal, JournalError, canonical
from .quik_contract import QuikOrder, QuikRoute, QuikTrade
from .quik_replay import QuikReplayGateway, QuikTransaction
from .state import json_object
from .time import Clock, ManualClock

RECOVERY_REASON = 'QUIK journal restored; independent broker reconciliation and fresh capital required'
MAX_INPUT_BYTES = 1_048_576


class _Protocol(QuikReplayGateway):
    """Isolated candidate: no EventBus and no external effects while evaluating."""

    def __init__(self, gateway_id: str, routes: tuple[QuikRoute, ...], clock: ManualClock, *,
                 session_id: str, timezone_offset: timedelta, first_transaction_id: int,
                 response_timeout: float, max_unmatched: int) -> None:
        super().__init__(gateway_id, routes, clock, session_id=session_id, timezone_offset=timezone_offset,
                         first_transaction_id=first_transaction_id, response_timeout=response_timeout,
                         max_unmatched=max_unmatched)
        self.outputs: list[Message] = []

    async def _publish(self, message: Message) -> None:
        self.outputs.append(message)

    def fork(self, at: datetime) -> '_Protocol':
        clock = ManualClock(self.clock.now())
        clock.advance_to(at)
        candidate = deepcopy(self, {id(self._lock): asyncio.Lock(), id(self.clock): clock})
        candidate.outputs = []
        return candidate

    @property
    def next_id(self) -> int:
        return self._next_id

    @property
    def broker_orders(self) -> tuple[tuple[int, str], ...]:
        return tuple(sorted(self._by_broker.items()))

    async def apply(self, payload: dict[str, Any]) -> None:
        op = payload.get('op')
        shapes = {'command': {'op', 'message'}, 'callback': {'op', 'event', 'data'},
                  'response': {'op', 'transaction_id', 'data'}, 'deadlines': {'op'},
                  'restart': {'op'}, 'fault': {'op', 'reason'}}
        if not isinstance(op, str) or op not in shapes or set(payload) != shapes[op]:
            raise JournalError('unknown QUIK input schema')
        if op == 'command':
            command = codec.decode(payload['message'])
            if not isinstance(command, (GatewaySubmit, GatewayCancel)):
                raise JournalError('unexpected durable gateway command')
            await self._handle(command)
        elif op == 'callback':
            if not isinstance(payload['event'], str) or not isinstance(payload['data'], dict):
                raise JournalError('invalid durable callback envelope')
            await self.feed(payload['event'], payload['data'])
        elif op == 'response':
            if not isinstance(payload['data'], dict):
                raise JournalError('invalid durable transport envelope')
            await self.transport_response(payload['transaction_id'], payload['data'])
        elif op == 'deadlines':
            await self.check_deadlines()
        elif op == 'restart':
            await self._fault(RECOVERY_REASON)
        else:
            if not isinstance(payload['reason'], str) or not payload['reason']:
                raise JournalError('invalid durable fault')
            await self._fault(payload['reason'])


class QuikDurableGateway:
    """Create with await open(), attach, then await recover() before accepting input.

    ``transactions`` is the complete audit trail. ``prepared`` only contains new
    durable preparations in this process; neither property is a transport queue.
    Every reopened journal remains blocked; this slice has no live resume API.
    """

    gateway_id: str
    clock: Clock
    journal: DurableJournal
    _bus: EventBus | None
    _lock: asyncio.Lock
    _started: bool
    _failed: bool
    _number: int
    _prepared: list[QuikTransaction]
    _recovery_messages: list[Message]
    _engine: _Protocol
    _reopened: bool
    _tail: int

    @classmethod
    async def open(cls, gateway_id: str, routes: tuple[QuikRoute, ...], clock: Clock, *,
                   journal: DurableJournal, session_id: str, timezone_offset: timedelta,
                   first_transaction_id: int = 1, response_timeout: float = 90,
                   max_unmatched: int = 1000) -> 'QuikDurableGateway':
        self = cls()
        self.gateway_id, self.clock, self.journal = gateway_id, clock, journal
        self._bus = None
        self._lock = asyncio.Lock()
        self._started, self._failed = False, False
        self._number = 0
        self._prepared = []
        self._recovery_messages = []
        now = utc(clock.now())
        self._engine = _Protocol(gateway_id, routes, ManualClock(now), session_id=session_id,
            timezone_offset=timezone_offset, first_transaction_id=first_transaction_id,
            response_timeout=response_timeout, max_unmatched=max_unmatched)
        profile = {'gateway_id': gateway_id, 'session_id': session_id,
                   'routes': [{'route': codec.encode(r.route), 'price_step': str(r.price_step),
                               'commission_fields': list(r.commission_fields)} for r in routes],
                   'timezone_microseconds': timezone_offset // timedelta(microseconds=1),
                   'first_transaction_id': first_transaction_id, 'response_timeout': response_timeout,
                   'max_unmatched': max_unmatched}
        records = journal.records
        self._tail = len(records)
        self._reopened = bool(records)
        if not records:
            self._append({'kind': 'session', 'schema': 1, 'profile': profile, 'created_at': codec.encode(now)})
        else:
            header = records[0]
            if (set(header) != {'kind', 'schema', 'profile', 'created_at'} or header['kind'] != 'session'
                    or type(header['schema']) is not int or header['schema'] != 1
                    or canonical(header['profile']) != canonical(profile)):
                raise JournalError('QUIK journal schema or immutable session profile differs')
            self._engine.clock = ManualClock(self._time(header['created_at']))
            if self._engine.clock.now() > now:
                raise JournalError('clock predates the QUIK session')
            await self._restore(records[1:], now)
            await self._commit({'op': 'restart'}, publish=False)
        return self

    @staticmethod
    def _time(value: object) -> datetime:
        result = codec.decode(value)
        if not isinstance(result, datetime):
            raise JournalError('QUIK record requires a timestamp')
        return utc(result)

    def _append(self, record: dict[str, Any]) -> None:
        if self.journal.record_count != self._tail:
            raise JournalError('QUIK journal was changed by another owner')
        self.journal.append(record)
        self._tail += 1

    async def _evaluate(self, payload: dict[str, Any], at: datetime, number: int) -> tuple[_Protocol, dict[str, Any]]:
        candidate = self._engine.fork(at)
        error: str | None = None
        try:
            await candidate.apply(payload)
        except ValueError as invalid:
            # Keep raw input but roll back any partial bind/output from the parser.
            error = str(invalid)
            candidate = self._engine.fork(at)
            await candidate._fault('invalid QUIK input: ' + error)
        transactions = candidate.transactions[len(self._engine.transactions):]
        result = {'kind': 'result', 'number': number, 'error': error,
                  'transactions': [{'transaction_id': t.transaction_id, 'order_id': t.order_id,
                                    'request_id': t.request_id, 'fields': dict(t.fields)} for t in transactions],
                  'messages': [codec.encode(m) for m in candidate.outputs]}
        return candidate, result

    async def _restore(self, records: tuple[dict[str, Any], ...], now: datetime) -> None:
        for index in range(0, len(records), 2):
            record = records[index]
            if (set(record) != {'kind', 'number', 'at', 'payload'} or record['kind'] != 'input'
                    or type(record['number']) is not int or record['number'] != self._number + 1
                    or not isinstance(record['payload'], dict)):
                raise JournalError('invalid QUIK input sequence')
            at = self._time(record['at'])
            if at < self._engine.clock.now() or at > now:
                raise JournalError('QUIK input time is outside the recovery interval')
            candidate, result = await self._evaluate(record['payload'], at, record['number'])
            if index + 1 < len(records):
                if canonical(records[index + 1]) != canonical(result):
                    raise JournalError('QUIK derived result differs from deterministic replay')
            else:
                # A crash left a durable inbox entry. Finish locally, never send.
                self._append(result)
            self._engine, self._number = candidate, record['number']
            self._recovery_messages.extend(candidate.outputs)

    @property
    def transactions(self) -> tuple[QuikTransaction, ...]:
        return self._engine.transactions

    @property
    def prepared(self) -> tuple[QuikTransaction, ...]:
        return tuple(self._prepared)

    @property
    def next_transaction_id(self) -> int:
        return self._engine.next_id

    @property
    def broker_orders(self) -> tuple[tuple[int, str], ...]:
        return self._engine.broker_orders

    @property
    def unmatched(self) -> tuple[QuikOrder | QuikTrade, ...]:
        return self._engine.unmatched

    @property
    def incidents(self) -> tuple[str, ...]:
        return self._engine.incidents

    @property
    def blocked(self) -> bool:
        return self._failed or not self.journal.writable or not self._started or self._engine.blocked

    def attach(self, bus: EventBus) -> None:
        if self._bus is not None:
            raise ValueError('gateway is already attached')
        bus.subscribe(gateway_recipient(self.gateway_id), self._handle)
        self._bus = bus

    async def _publish(self, messages: list[Message]) -> None:
        if self._bus is None:
            raise RuntimeError('gateway is not attached')
        for message in messages:
            await self._bus.publish('orders', message)

    async def _stop(self) -> None:
        self._failed = True
        try:
            await self._publish([GatewayUncertain(self.gateway_id, 'QUIK persistence or delivery failed; restart required')])
        except Exception:
            pass  # A broken bus cannot carry the alert; the caller still receives the original failure.

    async def _require_started(self) -> None:
        if self._failed or not self.journal.writable:
            await self._stop()
            raise JournalError('QUIK journal or delivery is unavailable; restart required')
        if not self._started:
            await self._stop()
            raise RuntimeError('attach and recover the QUIK gateway before accepting input')

    async def recover(self) -> None:
        async with self._lock:
            if self._started:
                await self._require_started()
                return
            if self._failed or self._bus is None or not self.journal.writable:
                raise JournalError('gateway must be healthy and attached before recovery')
            try:
                if self._reopened:
                    await self._publish([GatewayUncertain(self.gateway_id, RECOVERY_REASON)])
                await self._publish(self._recovery_messages)
                self._started = True
                self._recovery_messages.clear()
            except BaseException:
                await self._stop()
                raise

    async def _commit(self, payload: dict[str, Any], *, publish: bool = True) -> None:
        try:
            at = utc(self.clock.now())
            if at < self._engine.clock.now():
                raise JournalError('QUIK clock moved backwards')
            number = self._number + 1
            self._append({'kind': 'input', 'number': number, 'at': codec.encode(at), 'payload': payload})
            candidate, result = await self._evaluate(payload, at, number)
            self._append(result)
            transactions = candidate.transactions[len(self._engine.transactions):]
            self._engine, self._number = candidate, number
            if publish:
                self._prepared.extend(transactions)
                await self._publish(candidate.outputs)
            else:
                self._recovery_messages.extend(candidate.outputs)
        except BaseException:
            await self._stop()
            raise
        if result['error'] is not None:
            raise ValueError(result['error'])

    async def _handle(self, message: Message) -> None:
        async with self._lock:
            await self._require_started()
            if not isinstance(message, (GatewaySubmit, GatewayCancel)):
                raise ValueError('unexpected QUIK gateway command')
            await self._commit({'op': 'command', 'message': codec.encode(message)})

    async def _raw(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            await self._require_started()
            try:
                if ((payload['op'] == 'callback' and not isinstance(payload['event'], str))
                        or not isinstance(payload['data'], Mapping)):
                    raise ValueError('invalid callback envelope')
                payload = {**payload, 'data': dict(payload['data'])}
                frozen = json_object(payload)
                if len(canonical(frozen).encode('utf-8')) > MAX_INPUT_BYTES:
                    raise ValueError('QUIK input exceeds the one MiB limit')
            except (ValueError, UnicodeError) as invalid:
                await self._commit({'op': 'fault', 'reason': 'QUIK input is not a bounded JSON object'})
                raise ValueError('callback rejected before journaling: invalid JSON envelope') from invalid
            await self._commit(frozen)

    async def feed(self, event: str, data: Mapping[str, object]) -> None:
        await self._raw({'op': 'callback', 'event': event, 'data': data})

    async def transport_response(self, transaction_id: int, response: Mapping[str, object]) -> None:
        await self._raw({'op': 'response', 'transaction_id': transaction_id, 'data': response})

    async def check_deadlines(self) -> None:
        async with self._lock:
            await self._require_started()
            await self._commit({'op': 'deadlines'})

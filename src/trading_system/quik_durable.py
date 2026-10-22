"""Durable QUIK protocol boundary, still offline: prepared does not mean sent.

Inputs are fsynced before evaluation; derived transactions and OMS messages are
fsynced before exposure. Recovery redelivers financial messages, never orders.
The caller owns the dedicated DurableJournal and must keep it open.
"""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any

from . import codec
from .domain import Execution, GatewayCancel, GatewaySubmit, GatewayUncertain, Message, utc
from .events import EventBus
from .orders import OrderManager, gateway_recipient
from .inventory import OUTCOME_TYPES, OrderOutcome, RepairExecutions, ResolveOrders, StageExternalInventory, AdoptExternalInventory
from .persistence import DurableJournal, JournalError, canonical
from .quik_contract import QuikOrder, QuikRoute, QuikTrade, integer
from .quik_replay import QuikReplayGateway, QuikTransaction, _Order
from .quik_compare import BrokerLink, ComparisonPolicy, QuikBindings
from .quik_repair import ExecutionEvidence, missing_suffix
from .quik_outcomes import OrderEvidence, select_order_outcomes
from .quik_inventory import build_external_inventory_plan
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
        self.outcomes: list[OrderOutcome] = []
        self.inventory_adoptions: dict[str, AdoptExternalInventory] = {}

    async def _publish(self, message: Message) -> None:
        self.outputs.append(message)
        if isinstance(message, OUTCOME_TYPES):
            self.outcomes.append(message)

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

    def comparison_bindings(self, revision: int) -> QuikBindings:
        return QuikBindings(self.gateway_id, self.session_id, revision,
            tuple(BrokerLink(self._orders[local].snapshot.intent.route, number, local)
                  for number, local in sorted(self._by_broker.items())),
            tuple(BrokerLink(t.route, number, f'{self.session_id}:{number}')
                  for number, t in sorted(self._trades.items())))

    def execution_evidence(self, revision: int) -> ExecutionEvidence:
        return ExecutionEvidence(self.comparison_bindings(revision), tuple(
            Execution(f'{self.session_id}:{t.trade_id}', self._by_broker[t.order_id],
                      t.route, t.side, t.quantity, t.price, t.commission, t.timestamp)
            for t in self._trades.values()))

    async def adopt_inventory(self, command: AdoptExternalInventory) -> None:
        previous = self.inventory_adoptions.get(command.operation_id)
        if previous is not None:
            if previous != command:
                raise ValueError('conflicting gateway inventory operation')
            return
        review = command.review
        if (self.inventory_adoptions or review.gateway_id != self.gateway_id or review.session_id != self.session_id
                or any(p.route not in {s.route for s in self.routes} for p in review.observed_positions)):
            raise ValueError('gateway adoption requires its first inventory baseline in the configured scope')
        incoming = {origin.broker_order_id for origin in review.order_origins}
        if (any(number not in incoming for number in self._external)
                or any(t.order_id not in incoming for t, _ in self._pending.values())):
            raise ValueError('unmatched callback outside proposed inventory')
        for origin, item in zip(review.order_origins, review.imports, strict=True):
            order, number = item.order, origin.broker_order_id
            if (number in self._by_broker or order.order_id in self._orders
                    or order.order_id != f'{self.session_id}:external-order:{number}'):
                raise ValueError('external broker or local order identity is already bound')
            if order.intent.limit_price is None:
                raise ValueError('gateway adoption requires a supported limit order')
            spec = next(s for s in self.routes if s.route == order.intent.route)
            spec.price(order.intent.limit_price)
            filled = integer(order.filled_quantity, zero=True)
            self._orders[order.order_id] = _Order(order, 0, self._deadline(), broker_id=number,
                accepted=True, filled=filled, reported=filled,
                final=filled if order.status.terminal else None, inventory_cutoff=review.observed_at, external=True)
            self._by_broker[number] = order.order_id
            for execution in item.executions:
                prefix = self.session_id + ':'
                if not execution.execution_id.startswith(prefix):
                    raise ValueError('execution is outside the adopted session')
                key = integer(execution.execution_id[len(prefix):])
                if key in self._trades:
                    raise ValueError('historical execution already bound')
                trade = QuikTrade(key, number, execution.route, execution.side, integer(execution.quantity),
                                  execution.price, execution.commission, execution.timestamp)
                pending = self._pending.get(key)
                if pending is not None and pending[0] != trade:
                    raise ValueError('pending trade conflicts with historical inventory')
                self._trades[key] = trade
                self._pending.pop(key, None)
        # OMS installs the same cutoff on every known order of this gateway.
        for state in self._orders.values():
            state.inventory_cutoff = review.observed_at
        pending_trades = tuple(t for t, _ in self._pending.values())
        self._pending.clear()
        for trade in pending_trades:
            await self._trade(trade)  # Only new, post-boundary trades may produce financial messages.
        external = tuple(self._external.values())
        self._external.clear()
        for report in external:
            await self._order(report)
        self.inventory_adoptions[command.operation_id] = command
        self._blocked = True  # Adoption never constitutes independent broker reconciliation.

    async def apply(self, payload: dict[str, Any]) -> None:
        op = payload.get('op')
        shapes = {'command': {'op', 'message'}, 'callback': {'op', 'event', 'data'},
                  'response': {'op', 'transaction_id', 'data'}, 'deadlines': {'op'},
                  'restart': {'op'}, 'fault': {'op', 'reason'}, 'inventory': {'op', 'command'}}
        if not isinstance(op, str) or op not in shapes or set(payload) != shapes[op]:
            raise JournalError('unknown QUIK input schema')
        if op == 'inventory':
            adoption = codec.decode(payload['command'])
            if not isinstance(adoption, AdoptExternalInventory):
                raise JournalError('invalid inventory adoption input')
            await self.adopt_inventory(adoption)
        elif op == 'command':
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
    def comparison_bindings(self) -> QuikBindings:
        """Immutable mappings from committed protocol state; no delivery or apply."""
        return self._engine.comparison_bindings(self._number)

    @property
    def execution_evidence(self) -> ExecutionEvidence:
        return self._engine.execution_evidence(self._number)

    async def apply_execution_repair(self, command: RepairExecutions, oms: OrderManager) -> None:
        """Operator call, outside consumers; no transport or automatic resume."""
        if not isinstance(command, RepairExecutions):
            raise ValueError('expected an execution repair command')
        if self._bus is None or oms.bus is not self._bus:
            raise ValueError('repair requires the attached OMS event bus')
        await self._bus.drain()  # Never drain while holding a lock needed by a consumer.
        async with self._lock:
            def verify_source() -> None:
                if (not self._started or self._failed or not self.journal.writable or self.journal.degraded
                        or self.journal.record_count != self._tail):
                    raise JournalError('execution source is unavailable; restart and inspect its journal')
                evidence = self.execution_evidence
                if (command.gateway_id, command.session_id, command.source_revision, command.source_digest) != (
                        self.gateway_id, self._engine.session_id, self._number, evidence.digest):
                    raise ValueError('execution source changed; rebuild repair')
                if command.executions != missing_suffix(evidence, oms.observe_gateway(self.gateway_id)):
                    raise ValueError('repair payload differs from the committed missing suffix')

            await oms._apply_guarded_repair(command, verify_source)

    @property
    def order_evidence(self) -> OrderEvidence:
        return OrderEvidence(self.execution_evidence, tuple(self._engine.outcomes))

    async def apply_order_resolution(self, command: ResolveOrders, oms: OrderManager) -> None:
        """Apply saved outcomes; replacement abandonment must be named explicitly."""
        if not isinstance(command, ResolveOrders):
            raise ValueError('expected an order resolution command')
        if self._bus is None or oms.bus is not self._bus:
            raise ValueError('resolution requires the attached OMS event bus')
        await self._bus.drain()
        async with self._lock:
            def verify_source() -> None:
                if (not self._started or self._failed or not self.journal.writable or self.journal.degraded
                        or self.journal.record_count != self._tail):
                    raise JournalError('order source is unavailable; restart and inspect its journal')
                evidence = self.order_evidence
                if (command.gateway_id, command.session_id, command.source_revision, command.source_digest) != (
                        self.gateway_id, self._engine.session_id, self._number, evidence.digest):
                    raise ValueError('order source changed; rebuild resolution')
                if command.outcomes != select_order_outcomes(evidence, oms.observe_gateway(self.gateway_id)):
                    raise ValueError('resolution payload differs from committed outcomes')

            await oms._apply_guarded_repair(command, verify_source)

    async def stage_external_inventory(self, command: StageExternalInventory, oms: OrderManager,
                                       raw: dict[str, Any], policy: ComparisonPolicy) -> None:
        """Revalidate and save a review. Does not bind external orders or import inventory."""
        if not isinstance(command, StageExternalInventory):
            raise ValueError('expected an external inventory review')
        if self._bus is None or oms.bus is not self._bus:
            raise ValueError('inventory review requires the attached OMS event bus')
        await self._bus.drain()
        async with self._lock:
            def verify_source() -> None:
                if (not self._started or self._failed or not self.journal.writable or self.journal.degraded
                        or self.journal.record_count != self._tail):
                    raise JournalError('inventory source is unavailable; restart and inspect its journal')
                rebuilt = build_external_inventory_plan(command.operation_id, raw, policy,
                    oms.observe_gateway(self.gateway_id), self.order_evidence, position_basis=command.position_basis,
                    order_origins=command.order_origins, now=oms.clock.now(), current_revision=oms.revision)
                if replace(rebuilt, created_at=command.created_at) != command:
                    raise ValueError('inventory evidence or proposed contents changed; rebuild review')

            await oms._apply_guarded_repair(command, verify_source)

    @property
    def inventory_adoptions(self) -> tuple[AdoptExternalInventory, ...]:
        return tuple(self._engine.inventory_adoptions.values())

    def inventory_adoption_command(self, operation_id: str, oms: OrderManager, review_id: str) -> AdoptExternalInventory:
        """Capture a command; application rechecks the journal prefix and both live revisions."""
        return AdoptExternalInventory(operation_id, oms.revision, oms.external_inventory_review(review_id),
            self.journal.record_count, sha256(canonical(self.journal.records).encode()).hexdigest())

    async def adopt_external_inventory(self, command: AdoptExternalInventory, oms: OrderManager,
                                       raw: dict[str, Any], policy: ComparisonPolicy) -> None:
        """OMS decision first, gateway binding second; exact retry finishes an interrupted adoption."""
        if not isinstance(command, AdoptExternalInventory) or self._bus is None or oms.bus is not self._bus:
            raise ValueError('adoption requires its command and the attached OMS event bus')
        await self._bus.drain()
        async with self._lock:
            async with oms._transition_lock:
                if (oms._failed_after_commit or oms.journal is None or not oms.journal.writable or oms.journal.degraded
                        or not self._started or self._failed or not self.journal.writable or self.journal.degraded
                        or self.journal.record_count != self._tail):
                    raise JournalError('inventory adoption requires healthy journals and delivery; restart first')
                saved = oms._inventory_operations.get(command.operation_id)
                bound = self._engine.inventory_adoptions.get(command.operation_id)
                if (command.gateway_record_count > self.journal.record_count
                        or sha256(canonical(self.journal.records[:command.gateway_record_count]).encode()).hexdigest()
                        != command.gateway_journal_digest):
                    raise JournalError('gateway journal does not contain the approved inventory source prefix')
                if saved is not None and saved != command:
                    raise ValueError('conflicting OMS inventory operation')
                if bound is not None:
                    if bound != command or saved != command:
                        raise JournalError('gateway and OMS inventory decisions differ')
                    return
                review = command.review
                if saved is None:
                    if (command.expected_revision != oms.revision
                            or oms.external_inventory_review(review.operation_id) != review):
                        raise ValueError('inventory adoption view changed')
                    if (policy.timezone_offset != self._engine.timezone_offset
                            or {(s.route, s.price_step, s.commission_fields) for s in policy.routes}
                            != {(s.route, s.price_step, s.commission_fields) for s in self._engine.routes}):
                        raise ValueError('comparison and callback contracts differ')
                    rebuilt = build_external_inventory_plan(review.operation_id, raw, policy,
                        oms.observe_gateway(self.gateway_id), self.order_evidence, position_basis=review.position_basis,
                        order_origins=review.order_origins, now=oms.clock.now(), current_revision=oms.revision)
                    if replace(rebuilt, expected_revision=review.expected_revision, created_at=review.created_at) != review:
                        raise ValueError('inventory evidence changed; rebuild and stage a fresh review')
                candidate = self._engine.fork(utc(self.clock.now()))
                await candidate.adopt_inventory(command)  # Validate gateway side before any financial commit.
                if saved is None:
                    try:
                        await oms._transition(command)
                    except BaseException:
                        if oms._failed_after_commit or not oms.journal.writable:
                            await self._stop()
                        raise
                try:
                    if oms.journal.degraded or not oms.journal.writable:
                        raise JournalError('OMS storage became unavailable after the inventory decision')
                    await self._commit({'op': 'inventory', 'command': codec.encode(command)})
                except BaseException:
                    await self._stop()
                    raise

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

    async def recover(self, oms: OrderManager | None = None) -> None:
        async with self._lock:
            if self._started:
                await self._require_started()
                return
            if self._failed or self._bus is None or not self.journal.writable:
                raise JournalError('gateway must be healthy and attached before recovery')
            if self.inventory_adoptions:
                if (oms is None or oms.bus is not self._bus or oms.journal is None
                        or not oms.journal.writable or oms.journal.degraded or oms._failed_after_commit
                        or any(oms._inventory_operations.get(c.operation_id) != c for c in self.inventory_adoptions)):
                    raise JournalError('restore the matching OMS inventory decisions before gateway delivery')
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

"""Offline QUIK contract exerciser. Records transactions; NEVER opens a socket.

Correlations are deliberately session-local. A live transport requires durable
transaction allocation, mapping and an independent broker snapshot first.
"""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .domain import (
    Execution, GatewayAccepted, GatewayCancel, GatewayCancelled, GatewayCancelRejected,
    GatewayRejected, GatewaySubmit, GatewayUncertain, Message, OrderSnapshot, positive,
)
from .events import EventBus
from .orders import gateway_recipient
from .quik_contract import (
    MAX_TRANSACTION_ID, QuikOrder, QuikRoute, QuikTrade, identifier, integer,
    limit_transaction, parse_order, parse_trade,
)
from .time import Clock


@dataclass(frozen=True)
class QuikTransaction:
    transaction_id: int
    order_id: str
    request_id: str | None
    fields: tuple[tuple[str, str], ...]


@dataclass
class _Cancel:
    message: GatewayCancel
    deadline: datetime
    transaction_id: int | None = None
    done: bool = False


@dataclass
class _Order:
    snapshot: OrderSnapshot
    transaction_id: int
    deadline: datetime
    broker_id: int | None = None
    accepted: bool = False
    rejected: bool = False
    filled: int = 0
    reported: int = 0
    final: int | None = None
    gap_deadline: datetime | None = None
    cancel: _Cancel | None = None


class QuikReplayGateway:
    def __init__(self, gateway_id: str, routes: tuple[QuikRoute, ...], clock: Clock, *,
                 session_id: str, timezone_offset: timedelta, first_transaction_id: int = 1,
                 response_timeout: float = 90, max_unmatched: int = 1000) -> None:
        identifier(gateway_id)
        identifier(session_id)
        timezone(timezone_offset)
        first_transaction_id = integer(first_transaction_id, maximum=MAX_TRANSACTION_ID)
        max_unmatched = integer(max_unmatched, maximum=100000)
        if isinstance(response_timeout, bool):
            raise ValueError('timeout must be numeric')
        positive(response_timeout, 'QUIK response timeout')
        if (not isinstance(routes, tuple) or not routes or len({r.route for r in routes}) != len(routes)
                or any(r.route.instrument.id.gateway_id != gateway_id for r in routes)):
            raise ValueError('declare distinct routes of this gateway')
        wire_keys = {(r.route.account_id, r.route.instrument.id.venue, r.route.instrument.id.symbol) for r in routes}
        if len(wire_keys) != len(routes):
            raise ValueError('conflicting metadata for a broker route')
        self.gateway_id, self.routes, self.clock = gateway_id, routes, clock
        self.session_id, self.timezone_offset = session_id, timezone_offset
        self.response_timeout, self.max_unmatched = response_timeout, max_unmatched
        self._next_id = first_transaction_id
        self._bus: EventBus | None = None
        self._lock = asyncio.Lock()
        self._orders: dict[str, _Order] = {}
        self._by_broker: dict[int, str] = {}
        self._transactions: list[QuikTransaction] = []
        self._by_transaction: dict[int, QuikTransaction] = {}
        self._trades: dict[int, QuikTrade] = {}
        self._pending: dict[int, tuple[QuikTrade, datetime]] = {}
        self._external: dict[int, QuikOrder] = {}
        self._incidents: list[str] = []
        self._blocked = False
        self._deadline()  # Validate timeout range before attaching to a bus.

    @property
    def transactions(self) -> tuple[QuikTransaction, ...]:
        return tuple(self._transactions)

    @property
    def unmatched(self) -> tuple[QuikOrder | QuikTrade, ...]:
        return tuple(self._external.values()) + tuple(t for t, _ in self._pending.values())

    @property
    def incidents(self) -> tuple[str, ...]:
        return tuple(self._incidents)

    @property
    def blocked(self) -> bool:
        return self._blocked

    def attach(self, bus: EventBus) -> None:
        if self._bus is not None:
            raise ValueError('gateway is already attached')
        bus.subscribe(gateway_recipient(self.gateway_id), self._handle)
        self._bus = bus

    def _deadline(self) -> datetime:
        try:
            return self.clock.now() + timedelta(seconds=self.response_timeout)
        except OverflowError as error:
            raise ValueError('response timeout exceeds clock range') from error

    async def _publish(self, message: Message) -> None:
        if self._bus is None:
            raise RuntimeError('gateway is not attached')
        await self._bus.publish('orders', message)

    async def _fault(self, reason: str) -> None:
        self._blocked = True
        if not self._incidents or self._incidents[-1] != reason:
            self._incidents.append(reason)
        await self._publish(GatewayUncertain(self.gateway_id, reason))

    def _record(self, order_id: str, request_id: str | None, fields: dict[str, str]) -> int:
        key = integer(self._next_id, maximum=MAX_TRANSACTION_ID)
        transaction = QuikTransaction(key, order_id, request_id, tuple(fields.items()))
        self._transactions.append(transaction)
        self._by_transaction[key] = transaction
        self._next_id += 1
        return key

    async def _handle(self, message: Message) -> None:
        async with self._lock:
            if self.blocked:
                await self._fault('gateway is blocked; queued command requires reconciliation')
                return
            if isinstance(message, GatewaySubmit):
                original = message.order
                prior = self._orders.get(original.order_id)
                if prior is not None:
                    if prior.snapshot != original:
                        await self._fault('conflicting submit identifier')
                    return
                try:
                    spec = next((r for r in self.routes if r.route == original.intent.route), None)
                    if spec is None:
                        raise ValueError('route is not configured for QUIK')
                    fields = limit_transaction(spec, original.intent, self._next_id)
                except ValueError as error:
                    await self._publish(GatewayRejected(original.order_id, self.gateway_id, str(error)))
                    return
                transaction = self._record(original.order_id, None, fields)
                self._orders[original.order_id] = _Order(original, transaction, self._deadline())
            elif isinstance(message, GatewayCancel):
                if message.gateway_id != self.gateway_id or message.order_id not in self._orders:
                    await self._fault('cancel has no correlated order')
                    return
                state = self._orders[message.order_id]
                if state.cancel is not None:
                    if state.cancel.message == message:
                        return
                    if not state.cancel.done:
                        await self._fault('conflicting cancellation identifier')
                        return
                state.cancel = _Cancel(message, self._deadline())
                await self._send_cancel(state)
            else:
                raise ValueError('unexpected QUIK gateway command')

    async def _send_cancel(self, state: _Order) -> None:
        cancel = state.cancel
        if cancel is None or cancel.done or cancel.transaction_id is not None or state.broker_id is None:
            return
        if state.final is not None:
            await self._publish(GatewayCancelled(state.snapshot.order_id, self.gateway_id,
                                                  cancel.message.request_id, state.final))
            cancel.done = True
            return
        if self.blocked:
            return
        if self._next_id > MAX_TRANSACTION_ID:
            await self._fault('transaction namespace exhausted; cancellation not sent')
            return
        route = state.snapshot.intent.route
        fields = {'TRANS_ID': str(self._next_id), 'ACTION': 'KILL_ORDER', 'CLASSCODE': route.instrument.id.venue,
                  'SECCODE': route.instrument.id.symbol, 'ACCOUNT': route.account_id, 'ORDER_KEY': str(state.broker_id)}
        cancel.transaction_id = self._record(state.snapshot.order_id, cancel.message.request_id, fields)

    async def feed(self, event: str, data: Mapping[str, object]) -> None:
        async with self._lock:
            try:
                if not isinstance(data, Mapping):
                    raise ValueError('callback data must be a mapping')
                if event == 'OnTransReply':
                    await self._reply(data)
                elif event == 'OnOrder':
                    await self._order(parse_order(data, self.routes))
                elif event == 'OnTrade':
                    await self._trade(parse_trade(data, self.routes, self.timezone_offset))
                elif event == 'OnDisconnected':
                    await self._fault('QUIK disconnected; broker outcome may be unknown')
                elif event != 'OnConnected':
                    raise ValueError('unsupported QUIK callback')
                # OnConnected is not evidence that orders/positions are reconciled.
            except ValueError:
                await self._fault('invalid or contradictory QUIK callback; reconciliation required')
                raise

    async def _bind(self, state: _Order, broker_id: int) -> None:
        owner = self._by_broker.get(broker_id)
        if ((owner is not None and owner != state.snapshot.order_id)
                or state.broker_id not in (None, broker_id) or state.rejected):
            raise ValueError('conflicting broker order identity')
        state.broker_id = broker_id
        self._by_broker[broker_id] = state.snapshot.order_id
        if not state.accepted:
            state.accepted = True
            await self._publish(GatewayAccepted(state.snapshot.order_id, self.gateway_id))
        for key, (trade, _) in tuple(self._pending.items()):
            if trade.order_id == broker_id:
                await self._apply_trade(state, trade)
                del self._pending[key]
        await self._send_cancel(state)

    async def _reply(self, data: Mapping[str, object]) -> None:
        key = integer(data.get('trans_id'), maximum=MAX_TRANSACTION_ID)
        status = integer(data.get('status'), zero=True, maximum=MAX_TRANSACTION_ID)
        transaction = self._by_transaction.get(key)
        if transaction is None:
            await self._fault('unknown transaction reply requires reconciliation')
            return
        state = self._orders[transaction.order_id]
        if status in (0, 1):
            return
        if status == 3:
            if transaction.request_id is None:
                await self._bind(state, integer(data.get('order_num')))
            # A successful KILL transaction is NOT the final order balance.
        elif status in (2, 4, 5, 6, 10, 11, 13):
            reason = data.get('result_msg')
            if not isinstance(reason, str) or not reason:
                reason = f'QUIK transaction status {status}'
            await self._reject(transaction, reason)
        else:
            await self._fault('QUIK transaction outcome unknown; no automatic retry')

    async def _reject(self, transaction: QuikTransaction, reason: str) -> None:
        state = self._orders[transaction.order_id]
        if transaction.request_id is None:
            if state.accepted or state.filled:
                raise ValueError('rejection contradicts registered order')
            if not state.rejected:
                state.rejected = True
                if state.cancel is not None:
                    state.cancel.done = True
                await self._publish(GatewayRejected(transaction.order_id, self.gateway_id, reason))
        elif state.cancel is not None and state.cancel.message.request_id == transaction.request_id and not state.cancel.done:
            state.cancel.done = True
            await self._publish(GatewayCancelRejected(transaction.order_id, self.gateway_id, transaction.request_id, reason))

    async def _order(self, report: QuikOrder) -> None:
        transaction = self._by_transaction.get(report.transaction_id)
        local_id = self._by_broker.get(report.order_id)
        if transaction is not None and transaction.request_id is None:
            if local_id is not None and local_id != transaction.order_id:
                raise ValueError('order transaction and broker identities disagree')
            local_id = transaction.order_id
        if local_id is None:
            if report.order_id not in self._external and len(self.unmatched) >= self.max_unmatched:
                raise ValueError('unmatched callback buffer full')
            self._external[report.order_id] = report
            await self._fault('external or uncorrelated order requires inventory reconciliation')
            return
        state = self._orders[local_id]
        intent = state.snapshot.intent
        if (report.route != intent.route or report.side != intent.side or report.quantity != intent.quantity
                or report.price != intent.limit_price):
            raise ValueError('order report differs from submitted intent')
        total = report.quantity - report.balance
        if report.active and (state.final is not None or total < state.reported):
            return  # Delayed active snapshots must not resurrect terminal state.
        if not report.active and (total < max(state.filled, state.reported) or state.final not in (None, total)):
            raise ValueError('final order balance contradicts known execution')
        state.reported = max(state.reported, total)
        if not report.active:
            state.final = total
        await self._bind(state, report.order_id)
        state.gap_deadline = self._deadline() if state.reported > state.filled and state.gap_deadline is None else state.gap_deadline
        if report.cancelled and state.cancel is None:
            await self._fault('external order cancellation requires reconciliation')
        elif state.final is not None and state.cancel is not None and not state.cancel.done:
            await self._publish(GatewayCancelled(local_id, self.gateway_id, state.cancel.message.request_id, total))
            state.cancel.done = True

    async def _trade(self, trade: QuikTrade) -> None:
        previous = self._trades.get(trade.trade_id)
        pending = self._pending.get(trade.trade_id)
        if previous is not None or pending is not None:
            if (previous if previous is not None else pending[0] if pending is not None else None) != trade:
                raise ValueError('conflicting trade identifier')
            return
        if trade.timestamp > self.clock.now():
            raise ValueError('trade is future-dated')
        local_id = self._by_broker.get(trade.order_id)
        if local_id is None:
            if len(self.unmatched) >= self.max_unmatched:
                raise ValueError('unmatched callback buffer full')
            self._pending[trade.trade_id] = trade, self._deadline()
            possible = any(s.broker_id is None and not s.rejected
                           and s.snapshot.intent.route == trade.route and s.snapshot.intent.side == trade.side
                           and s.snapshot.intent.quantity >= trade.quantity
                           and s.snapshot.intent.accepts_price(trade.price)
                           and s.snapshot.submitted_at <= trade.timestamp for s in self._orders.values())
            if not possible:
                await self._fault('external trade has no possible pending entry; inventory reconciliation required')
            return
        await self._apply_trade(self._orders[local_id], trade)

    async def _apply_trade(self, state: _Order, trade: QuikTrade) -> None:
        intent = state.snapshot.intent
        total = state.filled + trade.quantity
        if (trade.route != intent.route or trade.side != intent.side or not intent.accepts_price(trade.price)
                or total > intent.quantity or (state.final is not None and total > state.final)
                or trade.timestamp < state.snapshot.submitted_at or state.rejected):
            raise ValueError('trade contradicts the correlated order')
        execution = Execution(f'{self.session_id}:{trade.trade_id}', state.snapshot.order_id,
                              trade.route, trade.side, trade.quantity, trade.price, trade.commission, trade.timestamp)
        await self._publish(execution)
        self._trades[trade.trade_id] = trade
        state.filled = total
        if state.filled >= state.reported:
            state.gap_deadline = None

    async def transport_response(self, transaction_id: int, response: Mapping[str, object]) -> None:
        async with self._lock:
            try:
                transaction = self._by_transaction.get(integer(transaction_id, maximum=MAX_TRANSACTION_ID))
                if transaction is None or not isinstance(response, Mapping):
                    raise ValueError('transport response has no correlated transaction')
                if response.get('cmd') == 'sendTransaction' and response.get('data') is True:
                    return  # Sent to terminal, not registered or executed.
                if response.get('cmd') == 'lua_transaction_error' and isinstance(response.get('lua_error'), str):
                    await self._reject(transaction, str(response['lua_error']) or 'QUIK local transaction error')
                else:
                    await self._fault('ambiguous transport response; no automatic retry')
            except ValueError:
                await self._fault('contradictory transport response; reconciliation required')
                raise

    async def check_deadlines(self) -> None:
        async with self._lock:
            now = self.clock.now()
            if any((not s.accepted and not s.rejected and now >= s.deadline)
                   or (s.cancel is not None and not s.cancel.done and now >= s.cancel.deadline)
                   or (s.gap_deadline is not None and now >= s.gap_deadline) for s in self._orders.values()) \
                    or any(now >= deadline for _, deadline in self._pending.values()):
                await self._fault('QUIK response/fill deadline expired; outcome remains unknown')

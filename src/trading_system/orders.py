import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from fractions import Fraction
from typing import Protocol
from uuid import uuid4

from .domain import (
    CancelOrder, Execution, ExecutionApplied, GatewayAccepted, GatewayCancel,
    GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewaySubmit,
    GatewayReconciled, Message, OrderIntent, OrderSnapshot, OrderStatus, OrderUpdated, ReplaceOrder,
    SubmitOrder, Route, Side,
)
from . import codec
from .admission import CapitalDeficit, CapitalPolicy, ConfigureCapital, RouteExposure, usage_from_exposure
from .capital import AccountKey, CapitalUsage, _exact, _number
from .persistence import DurableJournal, JournalError
from .ledger import PositionLedger
from .lifecycle import decode_strategy_record
from .time import Clock, ManualClock


def strategy_recipient(strategy_id: str) -> str:
    return f'strategy:{strategy_id}'


def gateway_recipient(gateway_id: str) -> str:
    return f'gateway:{gateway_id}'


@dataclass
class _Cancellation:
    request_id: str
    prior_status: OrderStatus
    replacement: OrderIntent | None = None
    acknowledgement: GatewayCancelled | None = None


class _Publisher(Protocol):
    async def publish(self, recipient: str, message: Message) -> None: ...


class _PlannedEvents:
    def __init__(self) -> None:
        self.events: list[tuple[str, Message]] = []

    async def publish(self, recipient: str, message: Message) -> None:
        self.events.append((recipient, message))


class OrderManager:
    def __init__(self, bus: _Publisher, clock: Clock, ledger: PositionLedger, *,
                 journal: DurableJournal | None = None, require_capital: bool = False,
                 id_factory: Callable[[], str] = lambda: uuid4().hex) -> None:
        self.journal = journal
        self._transition_lock = asyncio.Lock()
        self._capital_required = require_capital
        self._capital_refreshed = not require_capital
        self._capital_policy: CapitalPolicy | None = None
        self._observed_prices: dict[Route, float] = {}
        self._id_factory = id_factory
        self._unreconciled: set[str] = set()
        self._storage_blocked = False
        self._failed_after_commit = False
        self.bus = bus
        self.clock = clock
        self.ledger = ledger
        self.gateway_ids: set[str] = set()
        self._orders: dict[str, OrderSnapshot] = {}
        self._local_rejections: set[str] = set()
        self._executions: dict[tuple[str, str], Execution] = {}
        self._cancellations: dict[str, _Cancellation] = {}
        self._cancel_outcomes: dict[tuple[str, str], GatewayCancelled | GatewayCancelRejected] = {}

    @property
    def ready(self) -> bool:
        return (self._capital_refreshed and not self._unreconciled
                and not self._storage_blocked and not self._failed_after_commit
                and (self.journal is None or (self.journal.writable and not self.journal.degraded)))

    def _copy_state(self, candidate: 'OrderManager') -> None:
        candidate._capital_required = self._capital_required
        candidate._capital_refreshed = self._capital_refreshed
        candidate._capital_policy = self._capital_policy
        candidate._observed_prices = self._observed_prices.copy()
        candidate.gateway_ids = self.gateway_ids.copy()
        candidate._orders = self._orders.copy()
        candidate._local_rejections = self._local_rejections.copy()
        candidate._executions = self._executions.copy()
        candidate._cancellations = {key: replace(value) for key, value in self._cancellations.items()}
        candidate._cancel_outcomes = self._cancel_outcomes.copy()
        candidate._unreconciled = self._unreconciled.copy()

    def _adopt(self, candidate: 'OrderManager') -> None:
        self._capital_required = candidate._capital_required
        self._capital_refreshed = candidate._capital_refreshed
        self._capital_policy = candidate._capital_policy
        self._observed_prices = candidate._observed_prices
        self._orders = candidate._orders
        self._local_rejections = candidate._local_rejections
        self._executions = candidate._executions
        self._cancellations = candidate._cancellations
        self._cancel_outcomes = candidate._cancel_outcomes
        self._unreconciled = candidate._unreconciled
        self.ledger.adopt(candidate.ledger)

    async def handle(self, message: Message | ConfigureCapital) -> None:
        async with self._transition_lock:
            await self._transition(message)

    async def configure_capital(self, policy: CapitalPolicy) -> None:
        await self.handle(ConfigureCapital(policy))

    async def _transition(self, message: Message | ConfigureCapital) -> None:
        if self._failed_after_commit:
            raise JournalError('durable transition could not be delivered; restart and reconcile required')
        if isinstance(message, (SubmitOrder, ReplaceOrder)) and not self.ready:
            raise RuntimeError('trading is blocked until storage, capital and gateway reconciliation are ready')
        generated: list[str] = []

        def identifier() -> str:
            result = self._id_factory()
            generated.append(result)
            return result

        events = _PlannedEvents()
        candidate = OrderManager(events, self.clock, self.ledger.fork(), id_factory=identifier)
        self._copy_state(candidate)
        candidate._storage_blocked = (self.journal is not None
                                      and (not self.journal.writable or self.journal.degraded))
        await candidate._handle(message)
        record = {'version': 2, 'capital_refreshed': self._capital_refreshed,
                  'time': self.clock.now().isoformat(), 'ids': generated,
                  'gateways': sorted(self.gateway_ids), 'unreconciled': sorted(self._unreconciled),
                  'storage_blocked': candidate._storage_blocked, 'message': codec.encode(message)}
        if self.journal is not None:
            self.journal.append(record)
        try:
            self._adopt(candidate)
            for recipient, outgoing in events.events:
                await self.bus.publish(recipient, outgoing)
        except BaseException:
            self._failed_after_commit = True
            raise

    async def restore(self) -> None:
        if self.journal is None:
            return
        if self._orders:
            raise RuntimeError('restore requires an empty order manager')
        candidate = OrderManager(_PlannedEvents(), self.clock, PositionLedger(self.ledger.report_currency))
        # Historical commands replay in their original mode; the caller's stricter
        # startup requirement is applied after replay, never to past admissions.
        latest: datetime | None = None
        for record in self.journal.records:
            try:
                if record.get('kind') == 'strategy':
                    timestamp, _ = decode_strategy_record(record)
                    if latest is not None and timestamp < latest:
                        raise JournalError('journal clock is invalid')
                    latest = timestamp
                    continue
                expected = {'version', 'time', 'ids', 'gateways', 'unreconciled',
                            'storage_blocked', 'message'}
                if record.get('version') == 2:
                    expected.add('capital_refreshed')
                if (set(record) != expected or type(record['version']) is not int
                        or record['version'] not in (1, 2)
                        or not isinstance(record['storage_blocked'], bool)
                        or ('capital_refreshed' in record
                            and not isinstance(record['capital_refreshed'], bool))):
                    raise JournalError('unsupported order journal schema')
                timestamp = datetime.fromisoformat(record['time'])
                if timestamp.utcoffset() is None or (latest is not None and timestamp < latest):
                    raise JournalError('journal clock is invalid')
                for key in ('ids', 'gateways', 'unreconciled'):
                    if not isinstance(record[key], list) or not all(isinstance(v, str) and v for v in record[key]):
                        raise JournalError('invalid order journal identifiers')
                identifiers = iter(record['ids'])
                candidate.bus = _PlannedEvents()
                candidate._id_factory = lambda: next(identifiers)
                candidate.clock = ManualClock(timestamp)
                candidate.gateway_ids = set(record['gateways'])
                candidate._unreconciled = set(record['unreconciled'])
                candidate._storage_blocked = record['storage_blocked']
                candidate._capital_refreshed = record.get('capital_refreshed', True)
                await candidate._handle(codec.decode(record['message']))
                if next(identifiers, None) is not None:
                    raise JournalError('journal has unused generated identifiers')
                latest = timestamp
            except (ValueError, TypeError, KeyError, StopIteration, AttributeError, RuntimeError) as error:
                raise JournalError('cannot replay order journal') from error
        candidate._capital_required |= self._capital_required
        self._adopt(candidate)
        self._capital_refreshed = not self._capital_required
        self._unreconciled = {o.intent.route.instrument.id.gateway_id for o in self._orders.values()
                             if o.order_id not in self._local_rejections}
        if latest is not None and isinstance(self.clock, ManualClock) and self.clock.now() < latest:
            self.clock.advance_to(latest)

    async def reconcile_gateway(self, gateway_id: str, observed: tuple[OrderSnapshot, ...]) -> None:
        await self.handle(GatewayReconciled(gateway_id, observed))

    def _validate_reconciliation(self, gateway_id: str, observed: tuple[OrderSnapshot, ...]) -> None:
        if gateway_id not in self.gateway_ids:
            raise ValueError('gateway is not registered')
        expected = {key: value for key, value in self._orders.items()
                    if value.intent.route.instrument.id.gateway_id == gateway_id
                    and key not in self._local_rejections}
        actual = {order.order_id: order for order in observed}
        if len(actual) != len(observed) or set(actual) != set(expected):
            raise ValueError('gateway order identifiers differ; missing/external orders need reconciliation')
        for key, order in expected.items():
            other = actual[key]
            status = order.status
            pending = self._cancellations.get(key)
            if pending is not None and pending.acknowledgement is not None:
                if order.intent.route.instrument.quantities_equal(
                        order.filled_quantity, pending.acknowledgement.filled_quantity):
                    status = OrderStatus.FILLED if order.remaining_quantity == 0 else OrderStatus.CANCELLED
            if (order.intent != other.intent or order.submitted_at != other.submitted_at
                    or status != other.status or order.filled_quantity != other.filled_quantity
                    or order.average_fill_price != other.average_fill_price
                    or order.commission != other.commission
                    or (status is OrderStatus.CANCEL_PENDING
                        and order.cancel_request_id != other.cancel_request_id)):
                raise ValueError('gateway order contents differ; reconcile reports before resuming')

    def orders(self, strategy_id: str) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self._orders.values() if order.strategy_id == strategy_id)

    async def _handle(self, message: Message | ConfigureCapital) -> None:
        if isinstance(message, ConfigureCapital):
            self._configure_capital(message.policy)
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, GatewayReconciled):
            self._validate_reconciliation(message.gateway_id, message.observed)
            self._unreconciled.discard(message.gateway_id)
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, SubmitOrder):
            await self._new_order(message.strategy_id, message.intent)
        elif isinstance(message, (CancelOrder, ReplaceOrder)):
            await self._request_cancel(message)
        elif isinstance(message, GatewayAccepted):
            await self._accepted(message)
        elif isinstance(message, (GatewayCancelled, GatewayCancelRejected)):
            await self._cancel_response(message)
        elif isinstance(message, GatewayRejected):
            await self._rejected(message)
        elif isinstance(message, Execution):
            await self._execute(message)
        else:
            raise ValueError('unexpected order-manager message')

    @property
    def capital_policy(self) -> CapitalPolicy | None:
        return self._capital_policy

    def _configure_capital(self, policy: CapitalPolicy) -> None:
        current = self._capital_policy
        if policy.report_currency != self.ledger.report_currency:
            raise ValueError('capital and ledger currencies differ')
        if not policy.as_of <= self.clock.now() < policy.expires_at:
            raise ValueError('capital snapshot is future-dated or expired')
        if current is not None and (policy.revision <= current.revision or policy.as_of < current.as_of):
            raise ValueError('capital revision or timestamp is stale')
        if any(v.route.instrument.id.gateway_id not in self.gateway_ids for v in policy.valuations):
            raise ValueError('capital gateway is not registered')
        active = {position.route for _, position in self.ledger.positions()}
        active.update(order.intent.route for order in self._orders.values()
                      if not order.status.terminal or self._has_replacement(order.order_id))
        for route in active:
            policy.price(route)  # Revoking grants cannot hide occupied financial risk.
        self._capital_policy = policy
        self._capital_required = True
        self._capital_refreshed = True
        self._observed_prices = {}
        self.capital_usage()  # Validate finite reporting values before commit.

    def _has_replacement(self, order_id: str) -> bool:
        pending = self._cancellations.get(order_id)
        return pending is not None and pending.replacement is not None

    def _exposures(self, extra: OrderSnapshot | None = None,
                   replacement: tuple[str, OrderIntent] | None = None) -> tuple[RouteExposure, ...]:
        policy = self._capital_policy
        if policy is None:
            return ()
        quantities: dict[tuple[str, Route], list[Fraction]] = {}
        prices = self._observed_prices.copy()

        def entry(owner: str, route: Route) -> list[Fraction]:
            return quantities.setdefault((owner, route), [Fraction(), Fraction(), Fraction()])

        def quantity(route: Route, value: float) -> Fraction:
            step = route.instrument.quantity_step
            return _exact(step) * round(value / step)

        for owner, position in self.ledger.positions():
            entry(owner, position.route)[0] = quantity(position.route, position.quantity)
        orders = tuple(self._orders.values()) + (() if extra is None else (extra,))
        for order in orders:
            pending = self._cancellations.get(order.order_id)
            target = pending.replacement if pending is not None else None
            if replacement is not None and replacement[0] == order.order_id:
                target = replacement[1]
            remaining = order.remaining_quantity if not order.status.terminal else 0
            if target is not None:
                remaining = max(remaining, target.quantity - order.filled_quantity)
            if remaining > 0:
                route = order.intent.route
                limits = [prices.get(route, 0)]
                if not order.status.terminal and order.intent.limit_price is not None:
                    limits.append(abs(order.intent.limit_price))
                if target is not None and target.quantity > order.filled_quantity and target.limit_price is not None:
                    limits.append(abs(target.limit_price))
                prices[route] = max(limits)
                index = 1 if order.intent.side is Side.BUY else 2
                entry(order.strategy_id, order.intent.route)[index] += quantity(order.intent.route, remaining)
        return tuple(RouteExposure(owner, route, values[0], values[1], values[2],
                                   _exact(max(policy.price(route), prices.get(route, 0))))
                     for (owner, route), values in quantities.items())

    def capital_usage(self) -> tuple[CapitalUsage, ...]:
        """Current position notional and incremental worst-case order exposure."""
        return usage_from_exposure(self._exposures())

    def _totals(self, exposures: tuple[RouteExposure, ...]) -> tuple[
            dict[AccountKey, Fraction], dict[tuple[str, AccountKey], Fraction]]:
        accounts: dict[AccountKey, Fraction] = {}
        owners: dict[tuple[str, AccountKey], Fraction] = {}
        if self._capital_policy is not None:
            accounts = {a.account: _exact(a.external_usage) for a in self._capital_policy.accounts}
        for exposure in exposures:
            account = AccountKey.from_route(exposure.route)
            key = exposure.strategy_id, account
            amount = exposure.maximum * exposure.unit_cost
            accounts[account] = accounts.get(account, Fraction()) + amount
            owners[key] = owners.get(key, Fraction()) + amount
        return accounts, owners

    def capital_deficits(self) -> tuple[CapitalDeficit, ...]:
        policy = self._capital_policy
        if policy is None:
            return ()
        accounts, owners = self._totals(self._exposures())
        result = [CapitalDeficit(a.account, _number(accounts.get(a.account, Fraction()) - _exact(a.capacity)))
                  for a in policy.accounts if accounts.get(a.account, Fraction()) > _exact(a.capacity)]
        budgets = {(b.strategy_id, b.account): _exact(b.capacity) for b in policy.budgets}
        result.extend(CapitalDeficit(account, _number(amount - budgets.get((owner, account), Fraction())), owner)
                      for (owner, account), amount in owners.items()
                      if amount > budgets.get((owner, account), Fraction()))
        return tuple(result)

    def _admission_error(self, owner: str, intent: OrderIntent, order: OrderSnapshot,
                         *, replacement: bool = False) -> str | None:
        policy = self._capital_policy
        if policy is None:
            return 'capital policy is required' if self._capital_required else None
        if not any(grant.strategy_id == owner and grant.route == intent.route for grant in policy.grants):
            return 'strategy has no grant for this route'
        # A market price has no bound. This slice admits only limit orders in
        # managed mode; ordinary offline backtests retain their explicit mode.
        if intent.limit_price is None:
            return 'capital admission requires a limit price'
        if abs(intent.limit_price) > policy.price(intent.route):
            return 'order price exceeds capital valuation; refresh the snapshot first'
        after = self._exposures(replacement=(order.order_id, intent)) if replacement else self._exposures(extra=order)
        before_accounts, before_owners = self._totals(self._exposures())
        after_accounts, after_owners = self._totals(after)
        account = AccountKey.from_route(intent.route)
        key = owner, account
        account_limit = next(a.capacity for a in policy.accounts if a.account == account)
        owner_limit = next(b.capacity for b in policy.budgets if (b.strategy_id, b.account) == key)
        exceeds = (after_accounts.get(account, Fraction()) > _exact(account_limit)
                   or after_owners.get(key, Fraction()) > _exact(owner_limit))
        stale = self.clock.now() >= policy.expires_at
        if exceeds or stale:
            exposure = next((e for e in after if e.strategy_id == owner and e.route == intent.route), None)
            bounded_exit = exposure is not None and exposure.bounded_exit(intent.side)
            increasing = (after_accounts.get(account, Fraction()) > before_accounts.get(account, Fraction())
                          or after_owners.get(key, Fraction()) > before_owners.get(key, Fraction()))
            if not bounded_exit or increasing:
                return 'capital limit exceeded or valuation expired'
        # Reject unreportable arithmetic while the candidate is still isolated.
        usage_from_exposure(after)
        return None

    def _intent_error(self, intent: OrderIntent) -> str | None:
        if intent.route.instrument.id.gateway_id not in self.gateway_ids:
            return 'gateway is not registered'
        if intent.route.instrument.currency != self.ledger.report_currency:
            return 'FX conversion is not implemented'
        if not intent.route.instrument.accepts_quantity(intent.quantity):
            return 'quantity does not match instrument step'
        return None

    async def _new_order(self, strategy_id: str, intent: OrderIntent,
                         replaces_order_id: str | None = None) -> OrderSnapshot:
        order = OrderSnapshot(self._id_factory(), strategy_id, intent, self.clock.now(),
                              replaces_order_id=replaces_order_id)
        if not order.order_id or order.order_id in self._orders:
            raise ValueError('order identifier must be unique and nonempty')
        reason = self._intent_error(intent)
        if reason is None:
            reason = self._admission_error(strategy_id, intent, order)
        if reason:
            self._local_rejections.add(order.order_id)
            order = replace(order, status=OrderStatus.REJECTED, rejection_reason=reason)
            await self._update(order)
            return order
        await self._update(order)
        await self.bus.publish(gateway_recipient(intent.route.instrument.id.gateway_id), GatewaySubmit(order))
        return order

    async def _update(self, order: OrderSnapshot) -> None:
        self._orders[order.order_id] = order
        await self.bus.publish(strategy_recipient(order.strategy_id), OrderUpdated(order))

    def _from_gateway(self, order_id: str, gateway_id: str) -> OrderSnapshot:
        order = self._orders[order_id]
        if gateway_id != order.intent.route.instrument.id.gateway_id:
            raise ValueError('gateway does not match order')
        return order

    async def _accepted(self, message: GatewayAccepted) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        pending = self._cancellations.get(order.order_id)
        if pending is not None and pending.prior_status is OrderStatus.SUBMITTED:
            pending.prior_status = OrderStatus.ACCEPTED
        if order.status is OrderStatus.SUBMITTED:
            await self._update(replace(order, status=OrderStatus.ACCEPTED))

    async def _request_cancel(self, message: CancelOrder | ReplaceOrder) -> None:
        order = self._orders[message.order_id]
        if order.strategy_id != message.strategy_id:
            raise ValueError('strategy does not own order')
        intent = message.intent if isinstance(message, ReplaceOrder) else None
        if intent is not None:
            if order.status.terminal:
                raise ValueError('terminal order cannot be replaced')
            if intent.route != order.intent.route or intent.side != order.intent.side:
                raise ValueError('replacement must keep route and side')
            reason = self._intent_error(intent)
            if reason:
                raise ValueError(reason)
        pending = self._cancellations.get(order.order_id)
        if pending is not None:
            if intent is None:
                pending.replacement = None
                await self._finish_cancel(order.order_id)
            elif intent != pending.replacement:
                raise ValueError('another cancellation is already pending')
            return
        if order.status.terminal:
            return
        if intent is not None:
            reason = self._admission_error(order.strategy_id, intent, order, replacement=True)
            if reason:
                raise ValueError(reason)
        request_id = self._id_factory()
        self._cancellations[order.order_id] = _Cancellation(request_id, order.status, intent)
        await self._update(replace(order, status=OrderStatus.CANCEL_PENDING,
                                   cancel_request_id=request_id, cancel_rejection_reason=None))
        gateway_id = order.intent.route.instrument.id.gateway_id
        await self.bus.publish(gateway_recipient(gateway_id), GatewayCancel(order.order_id, gateway_id, request_id))

    async def _cancel_response(self, message: GatewayCancelled | GatewayCancelRejected) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        key = message.gateway_id, message.request_id
        previous = self._cancel_outcomes.get(key)
        if previous is not None:
            if previous != message:
                raise ValueError('cancellation identifier has conflicting outcome')
            return
        pending = self._cancellations.get(order.order_id)
        if pending is None or pending.request_id != message.request_id:
            raise ValueError('cancellation response has no matching request')
        instrument = order.intent.route.instrument
        if isinstance(message, GatewayCancelled):
            total = message.filled_quantity
            if total != 0 and not instrument.accepts_quantity(total):
                raise ValueError('cancelled quantity does not match instrument step')
            if total < order.filled_quantity and not instrument.quantities_equal(total, order.filled_quantity):
                raise ValueError('cancelled quantity is below known executions')
            if total > order.intent.quantity and not instrument.quantities_equal(total, order.intent.quantity):
                raise ValueError('cancelled quantity exceeds order')
            pending.acknowledgement = message
            self._cancel_outcomes[key] = message
            await self._finish_cancel(order.order_id)
        else:
            del self._cancellations[order.order_id]
            self._cancel_outcomes[key] = message
            status = order.status if order.status.terminal else (
                OrderStatus.PARTIALLY_FILLED if order.filled_quantity > 0 else pending.prior_status
            )
            await self._update(replace(order, status=status, cancel_rejection_reason=message.reason))

    async def _finish_cancel(self, order_id: str) -> None:
        pending = self._cancellations.get(order_id)
        if pending is None or pending.acknowledgement is None:
            return
        order = self._orders[order_id]
        instrument = order.intent.route.instrument
        if not instrument.quantities_equal(order.filled_quantity, pending.acknowledgement.filled_quantity):
            return
        intent = pending.replacement
        needs_child = (intent is not None and intent.quantity > order.filled_quantity
                       and not instrument.quantities_equal(intent.quantity, order.filled_quantity))
        if needs_child and not self.ready:
            return
        del self._cancellations[order_id]
        status = OrderStatus.FILLED if instrument.quantities_equal(order.filled_quantity, order.intent.quantity) else OrderStatus.CANCELLED
        order = replace(order, status=status)
        await self._update(order)
        intent = pending.replacement
        if intent is None or intent.quantity <= order.filled_quantity or instrument.quantities_equal(intent.quantity, order.filled_quantity):
            return
        remaining = instrument.quantity_step * round((intent.quantity - order.filled_quantity) / instrument.quantity_step)
        child = await self._new_order(order.strategy_id, replace(intent, quantity=remaining), order_id)
        await self._update(replace(order, replacement_order_id=child.order_id))

    async def _rejected(self, message: GatewayRejected) -> None:
        order = self._from_gateway(message.order_id, message.gateway_id)
        if order.status is OrderStatus.REJECTED and order.rejection_reason == message.reason:
            return
        if order.status.terminal or order.filled_quantity > 0:
            raise ValueError('rejection contradicts existing order state')
        self._cancellations.pop(order.order_id, None)
        await self._update(replace(order, status=OrderStatus.REJECTED, rejection_reason=message.reason))

    async def _execute(self, execution: Execution) -> None:
        previous = self._executions.get(execution.key)
        if previous is not None:
            if previous != execution:
                raise ValueError('execution identifier has conflicting contents')
            return
        order = self._orders[execution.order_id]
        if order.status.terminal:
            raise ValueError('order cannot receive further executions')
        if execution.route != order.intent.route or execution.side != order.intent.side:
            raise ValueError('execution route or side does not match order')
        if execution.timestamp < order.submitted_at:
            raise ValueError('execution predates submission')
        instrument = execution.route.instrument
        if not instrument.accepts_quantity(execution.quantity):
            raise ValueError('execution quantity does not match instrument step')
        if not order.intent.accepts_price(execution.price):
            raise ValueError('execution price violates order limit')
        total = order.filled_quantity + execution.quantity
        full = instrument.quantities_equal(total, order.intent.quantity)
        if total > order.intent.quantity and not full:
            raise ValueError('execution exceeds remaining quantity')
        pending = self._cancellations.get(order.order_id)
        if pending is not None and pending.acknowledgement is not None:
            final_total = pending.acknowledgement.filled_quantity
            if total > final_total and not instrument.quantities_equal(total, final_total):
                raise ValueError('execution contradicts cancellation barrier')
        self.ledger.apply(order.strategy_id, execution)
        if self._capital_policy is not None:
            self._observed_prices[execution.route] = max(
                self._observed_prices.get(execution.route, 0), abs(execution.price))
        self._executions[execution.key] = execution
        average = (order.average_fill_price * order.filled_quantity + execution.price * execution.quantity) / total
        status = OrderStatus.FILLED if full else (
            OrderStatus.CANCEL_PENDING if pending is not None else OrderStatus.PARTIALLY_FILLED
        )
        await self._update(replace(
            order, filled_quantity=order.intent.quantity if full else total,
            average_fill_price=average, commission=order.commission + execution.commission,
            status=status,
        ))
        await self.bus.publish(strategy_recipient(order.strategy_id), ExecutionApplied(execution))
        await self._finish_cancel(order.order_id)

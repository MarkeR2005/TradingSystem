import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from fractions import Fraction
from math import fsum
from typing import Protocol
from uuid import uuid4

from .domain import (
    CancelOrder, Execution, ExecutionApplied, GatewayAccepted, GatewayCancel,
    GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewaySubmit,
    GatewayReconciled, Message, OrderIntent, OrderSnapshot, OrderStatus, OrderUpdated, ReplaceOrder,
    SubmitOrder, Route, Side, PositionChanged,
)
from . import codec
from .admission import CapitalDeficit, CapitalPolicy, ConfigureCapital, RouteExposure, RouteValuation, usage_from_exposure
from .capital import AccountKey, CapitalPlan, CapitalUsage, _exact, _number
from .rebalance import CapitalSnapshot, CapitalTarget, RebalanceCapital, allocate_policy, initial_policy
from .persistence import DurableJournal, JournalError
from .ledger import PositionLedger
from .inventory import MANUAL, CloseManual, InventoryCommand, InventoryReport, TransferPosition
from .lifecycle import decode_strategy_record
from .time import Clock, ManualClock
from .plan_execution import PlanBook
from .deficit_execution import DeficitBook
from .deficit import AcknowledgeDeficit, CheckDeficits, DeficitSnapshot, RetryDeficit, StartDeficit
from .plans import (AcknowledgePlan, CheckPlanDeadlines, ClosePlan, PlanMarks,
                    PlanSnapshot, RetryPlan, SubmitPlan, UpdatePlanProtection)


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
        self._revision = 0
        self._plans = PlanBook()
        self._deficits = DeficitBook()
        self._inventory_operations: dict[str, InventoryCommand] = {}
        self._inventory_gateways: dict[str, datetime] = {}
        self._inventory_cutoffs: dict[str, datetime] = {}
        self._capital_required = require_capital
        self._capital_refreshed = not require_capital
        self._capital_policy: CapitalPolicy | None = None
        self._capital_plan: CapitalPlan | None = None
        self._capital_snapshot: CapitalSnapshot | None = None
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
    def revision(self) -> int:
        return self._revision

    @property
    def ready(self) -> bool:
        return (self._capital_refreshed and not self._unreconciled
                and not self._storage_blocked and not self._failed_after_commit
                and (self.journal is None or (self.journal.writable and not self.journal.degraded)))

    def _copy_state(self, candidate: 'OrderManager') -> None:
        candidate._plans = self._plans.fork()
        candidate._deficits = self._deficits.fork()
        candidate._revision = self._revision
        candidate._inventory_operations = self._inventory_operations.copy()
        candidate._inventory_gateways = self._inventory_gateways.copy()
        candidate._inventory_cutoffs = self._inventory_cutoffs.copy()
        candidate._capital_required = self._capital_required
        candidate._capital_refreshed = self._capital_refreshed
        candidate._capital_policy = self._capital_policy
        candidate._capital_plan = self._capital_plan
        candidate._capital_snapshot = self._capital_snapshot
        candidate._observed_prices = self._observed_prices.copy()
        candidate.gateway_ids = self.gateway_ids.copy()
        candidate._orders = self._orders.copy()
        candidate._local_rejections = self._local_rejections.copy()
        candidate._executions = self._executions.copy()
        candidate._cancellations = {key: replace(value) for key, value in self._cancellations.items()}
        candidate._cancel_outcomes = self._cancel_outcomes.copy()
        candidate._unreconciled = self._unreconciled.copy()

    def _adopt(self, candidate: 'OrderManager') -> None:
        self._plans = candidate._plans
        self._deficits = candidate._deficits
        self._revision = candidate._revision
        self._inventory_operations = candidate._inventory_operations
        self._inventory_gateways = candidate._inventory_gateways
        self._inventory_cutoffs = candidate._inventory_cutoffs
        self._capital_required = candidate._capital_required
        self._capital_refreshed = candidate._capital_refreshed
        self._capital_policy = candidate._capital_policy
        self._capital_plan = candidate._capital_plan
        self._capital_snapshot = candidate._capital_snapshot
        self._observed_prices = candidate._observed_prices
        self._orders = candidate._orders
        self._local_rejections = candidate._local_rejections
        self._executions = candidate._executions
        self._cancellations = candidate._cancellations
        self._cancel_outcomes = candidate._cancel_outcomes
        self._unreconciled = candidate._unreconciled
        self.ledger.adopt(candidate.ledger)

    async def handle(self, message: Message | ConfigureCapital | RebalanceCapital | InventoryCommand) -> None:
        async with self._transition_lock:
            await self._transition(message)

    async def configure_capital(self, policy: CapitalPolicy) -> None:
        await self.handle(ConfigureCapital(policy))

    async def rebalance_capital(self, snapshot: CapitalSnapshot, targets: tuple[CapitalTarget, ...],
                                *, expected_revision: int) -> CapitalPlan:
        async with self._transition_lock:
            await self._transition(RebalanceCapital(snapshot, targets, expected_revision))
            assert self._capital_plan is not None
            return self._capital_plan

    async def _transition(self, message: Message | ConfigureCapital | RebalanceCapital | InventoryCommand) -> None:
        if self._failed_after_commit:
            raise JournalError('durable transition could not be delivered; restart and reconcile required')
        if self._plans.duplicate(message) or self._deficits.duplicate(message):
            return
        if isinstance(message, (InventoryReport, TransferPosition, CloseManual)):
            previous = self._inventory_operations.get(message.operation_id)
            if previous is not None:
                if previous != message:
                    raise ValueError('operation identifier has conflicting contents')
                return
        if isinstance(message, (SubmitOrder, ReplaceOrder, CloseManual, SubmitPlan, RetryPlan, AcknowledgePlan,
                                StartDeficit, RetryDeficit, AcknowledgeDeficit, UpdatePlanProtection)) and not self.ready:
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
        candidate._revision += 1
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
        if self._revision or self._orders:
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
                candidate._revision += 1
                if next(identifiers, None) is not None:
                    raise JournalError('journal has unused generated identifiers')
                latest = timestamp
            except (ValueError, TypeError, KeyError, StopIteration, AttributeError, RuntimeError) as error:
                raise JournalError('cannot replay order journal') from error
        candidate._capital_required |= self._capital_required
        self._adopt(candidate)
        self._capital_refreshed = not self._capital_required
        self._unreconciled = {o.intent.route.instrument.id.gateway_id for o in self._orders.values()
                             if o.order_id not in self._local_rejections} | set(self._inventory_gateways)
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

    def _invalidate_allocation(self) -> None:
        self._capital_plan = None
        self._capital_required = True
        self._capital_refreshed = False

    async def _inventory_report(self, command: InventoryReport) -> None:
        if self._deficits.active() is not None and (command.openings or command.imports):
            raise ValueError('new inventory must wait until deficit recovery is acknowledged')
        gateway = command.gateway_id
        previous_time = self._inventory_gateways.get(gateway)
        if (gateway not in self.gateway_ids or command.as_of > self.clock.now()
                or (previous_time is not None and command.as_of < previous_time)
                or any(o.submitted_at > command.as_of for o in command.observed_orders)
                or any(e.timestamp > command.as_of for e in self._executions.values()
                       if e.route.instrument.id.gateway_id == gateway)):
            raise ValueError('inventory gateway or cutover time is invalid')
        incoming = [p.route for p in command.openings] + [p.route for p in command.observed_positions]
        incoming += [item.order.intent.route for item in command.imports]
        incoming += [order.intent.route for order in command.observed_orders]
        if any(r.instrument.id.gateway_id != gateway or r.instrument.currency != self.ledger.report_currency
               for r in incoming):
            raise ValueError('inventory report has a foreign gateway or currency')
        known = [p.route for _, p in self.ledger.positions()] + [o.intent.route for o in self._orders.values()]
        if self._capital_policy is not None:
            known += [v.route for v in self._capital_policy.valuations]
        instruments = {r.instrument.id: r.instrument for r in known}
        for route in incoming:
            if instruments.setdefault(route.instrument.id, route.instrument) != route.instrument:
                raise ValueError('conflicting inventory instrument metadata')
        for item in command.imports:
            order = item.order
            if order.order_id in self._orders or order.submitted_at > command.as_of:
                raise ValueError('external order is already known or future-dated')
            for execution in item.executions:
                if execution.key in self._executions or execution.timestamp > command.as_of:
                    raise ValueError('external execution identifier is known or future-dated')
                # These fills establish a deduplication baseline; their P&L is
                # before this system's cutover and must never be applied again.
                self._executions[execution.key] = execution
            await self._update(replace(order, strategy_id=MANUAL))
        for opening in command.openings:
            previous = self.ledger.position(MANUAL, opening.route)
            self.ledger.import_balance(MANUAL, opening, command.as_of)
            await self.bus.publish(strategy_recipient(MANUAL), PositionChanged(
                command.operation_id, previous, self.ledger.position(MANUAL, opening.route)))
        self._validate_reconciliation(gateway, command.observed_orders)
        quantities: dict[Route, list[float]] = {}
        for _, position in self.ledger.positions():
            if position.route.instrument.id.gateway_id == gateway:
                quantities.setdefault(position.route, []).append(position.quantity)
        observed = {p.route: p.quantity for p in command.observed_positions}
        for route in quantities.keys() | observed.keys():
            if not route.instrument.quantities_equal(fsum(quantities.get(route, ())), observed.get(route, 0)):
                raise ValueError('broker net positions differ; reconcile executions before importing residuals')
        self._inventory_gateways[gateway] = command.as_of
        for order in self._orders.values():
            if order.intent.route.instrument.id.gateway_id == gateway:
                self._inventory_cutoffs[order.order_id] = command.as_of
        self._unreconciled.discard(gateway)
        if command.openings or command.imports:
            self._invalidate_allocation()

    async def _transfer_position(self, command: TransferPosition) -> None:
        if (self._deficits.paused(command.source_id) or self._deficits.paused(command.target_id)
                or self._deficits.owns_route(command.route)):
            raise ValueError('deficit recovery locks affected ownership')
        if self._plans.active(command.source_id) or self._plans.active(command.target_id):
            raise ValueError('finish active plans before transferring ownership')
        if self._unreconciled or self._storage_blocked:
            raise RuntimeError('ownership transfer requires reconciled inventory and healthy storage')
        policy = self._capital_policy
        if command.target_id != MANUAL and (policy is None or not any(
                g.strategy_id == command.target_id and g.route == command.route for g in policy.grants)):
            raise ValueError('target has no route grant')
        if any(o.strategy_id in (command.source_id, command.target_id) and o.intent.route == command.route
               and (not o.status.terminal or o.order_id in self._cancellations) for o in self._orders.values()):
            raise ValueError('finish all route orders and cancellation barriers before transferring')
        previous = {owner: self.ledger.position(owner, command.route)
                    for owner in (command.source_id, command.target_id)}
        self.ledger.transfer(command, self.clock.now())
        for owner, position in previous.items():
            await self.bus.publish(strategy_recipient(owner), PositionChanged(
                command.operation_id, position, self.ledger.position(owner, command.route)))
        self._invalidate_allocation()

    async def _close_manual(self, command: CloseManual) -> None:
        if self._deficits.owns_route(command.route):
            raise ValueError('deficit recovery owns affected manual exits')
        position = self.ledger.position(MANUAL, command.route)
        side = Side.SELL if position.quantity > 0 else Side.BUY
        active = [o for o in self.orders(MANUAL) if o.intent.route == command.route and not o.status.terminal]
        remaining = fsum(o.remaining_quantity for o in active)
        total = remaining + command.quantity
        if (position.quantity == 0 or any(o.intent.side != side for o in active)
                or (total > abs(position.quantity)
                    and not command.route.instrument.quantities_equal(total, abs(position.quantity)))):
            raise ValueError('manual close would exceed position or conflict with an opening order')
        intent = OrderIntent(command.route, side, command.quantity, command.limit_price)
        order = await self._new_order(MANUAL, intent)
        if order.status is OrderStatus.REJECTED:
            raise ValueError(order.rejection_reason)

    def deficit(self, case_id: str) -> DeficitSnapshot:
        return self._deficits.states[case_id]

    def plan(self, plan_id: str) -> PlanSnapshot:
        return self._plans.states[plan_id]

    def has_active_plan(self, owner: str) -> bool:
        return self._plans.active(owner)

    def strategy_paused(self, owner: str) -> bool:
        return self._plans.paused(owner) or self._deficits.paused(owner)

    def _batch_error(self, owner: str, intents: tuple[OrderIntent, ...]) -> str | None:
        previous = self._orders
        self._orders = previous.copy()
        try:
            for index, intent in enumerate(intents):
                key = f'__plan_preflight_{index}'
                while key in self._orders:
                    key += '_'
                order = OrderSnapshot(key, owner, intent, self.clock.now())
                reason = self._intent_error(intent) or self._admission_error(owner, intent, order)
                if reason:
                    return reason
                self._orders[key] = order
        finally:
            self._orders = previous
        return None

    async def _handle(self, message: Message | ConfigureCapital | RebalanceCapital | InventoryCommand) -> None:
        if isinstance(message, (StartDeficit, RetryDeficit, AcknowledgeDeficit, CheckDeficits)):
            await self._deficits.handle(self, message)
        elif isinstance(message, (SubmitPlan, ClosePlan, PlanMarks, CheckPlanDeadlines, RetryPlan,
                                  AcknowledgePlan, UpdatePlanProtection)):
            await self._plans.handle(self, message)
        elif isinstance(message, (InventoryReport, TransferPosition, CloseManual)):
            if message.expected_revision != self._revision:
                raise ValueError('inventory revision changed; refresh the operator view')
            if isinstance(message, InventoryReport):
                await self._inventory_report(message)
            elif isinstance(message, TransferPosition):
                await self._transfer_position(message)
            else:
                await self._close_manual(message)
            self._inventory_operations[message.operation_id] = message
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, (ConfigureCapital, RebalanceCapital)):
            if isinstance(message, RebalanceCapital):
                self._rebalance_capital(message)
            else:
                self._configure_capital(message.policy)
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, GatewayReconciled):
            self._validate_reconciliation(message.gateway_id, message.observed)
            if message.gateway_id not in self._inventory_gateways:
                self._unreconciled.discard(message.gateway_id)
            if self.ready:
                for order_id in tuple(self._cancellations):
                    await self._finish_cancel(order_id)
        elif isinstance(message, SubmitOrder):
            if self._plans.active(message.strategy_id) or self.strategy_paused(message.strategy_id):
                raise ValueError('plan owns trading actions until its cycle and alerts are resolved')
            if self._deficits.owns_route(message.intent.route):
                raise ValueError('deficit recovery locks new orders on affected routes')
            if message.strategy_id == MANUAL:
                raise ValueError('manual strategy accepts only explicit bounded close commands')
            await self._new_order(message.strategy_id, message.intent)
        elif isinstance(message, (CancelOrder, ReplaceOrder)):
            if isinstance(message, ReplaceOrder) and (self._plans.active(message.strategy_id)
                                                     or self.strategy_paused(message.strategy_id)
                                                     or self._deficits.owns_route(message.intent.route)):
                raise ValueError('individual plan orders cannot be replaced')
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
        await self._plans.advance(self, message)
        await self._deficits.advance(self, message)

    @property
    def capital_policy(self) -> CapitalPolicy | None:
        return self._capital_policy

    @property
    def capital_plan(self) -> CapitalPlan | None:
        return self._capital_plan

    @property
    def capital_snapshot(self) -> CapitalSnapshot | None:
        return self._capital_snapshot

    def _rebalance_capital(self, command: RebalanceCapital) -> None:
        revision = self._capital_policy.revision if self._capital_policy is not None else 0
        if command.expected_revision != revision:
            raise ValueError('capital revision changed during refresh')
        snapshot = command.snapshot
        previous = self._capital_snapshot
        if previous is not None and (snapshot.source_id != previous.source_id
                                     or snapshot.sequence <= previous.sequence
                                     or snapshot.as_of < previous.as_of):
            raise ValueError('capital source identity, sequence or timestamp is stale')
        self._configure_capital(initial_policy(snapshot, command.targets, revision + 1))
        # A source request may overlap fills. A snapshot must not erase later
        # executions, including those sharing its timestamp. Older fills may be
        # revalued by a genuinely newer source mark.
        for execution in self._executions.values():
            if execution.timestamp >= snapshot.as_of:
                self._observed_prices[execution.route] = max(
                    self._observed_prices.get(execution.route, 0), abs(execution.price))
        exposures = self._exposures()
        prices = {item.route: _number(item.price) for item in exposures}
        valuations = tuple(RouteValuation(item.route, max(item.price, prices.get(item.route, 0),
                                                          self._observed_prices.get(item.route, 0)))
                           for item in snapshot.valuations)
        self._capital_plan, self._capital_policy = allocate_policy(command, usage_from_exposure(exposures), valuations)
        self._capital_snapshot = snapshot

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
        self._capital_plan = None
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
        if isinstance(message, ReplaceOrder) and message.strategy_id == MANUAL:
            raise ValueError('manual orders must be cancelled before a new bounded close')
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
        if self._deficits.paused(order.strategy_id) or self._deficits.owns_route(order.intent.route):
            pending.replacement = None
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
        cutoff = self._inventory_cutoffs.get(order.order_id)
        if cutoff is not None and execution.timestamp <= cutoff:
            raise ValueError('unknown execution at or before inventory cutover')
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

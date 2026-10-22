"""Scoped QUIK observations and a pure difference report. No apply/OMS commands."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from typing import Any

from .domain import OrderStatus, Route, Side, utc
from .inventory import OmsObservation
from .persistence import canonical
from .quik_boundary import CallbackCursor
from .quik_contract import COMMISSION_FIELDS, MAX_TRANSACTION_ID, decimal, identifier, integer, timestamp
from .quik_limits import _account_identifier, _table
from .reconciliation import BROKER_LIMITATIONS


@dataclass(frozen=True)
class ObservedRoute:
    firmid: str
    route: Route
    holding_type: int
    price_step: Decimal
    commission_fields: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        identifier(self.firmid)
        _account_identifier(self.route.account_id)
        identifier(self.route.instrument.id.symbol)
        if (self.route.instrument.id.venue != 'SPBFUT' or self.route.instrument.quantity_step != 1
                or self.route.instrument.currency != 'RUB'):
            raise ValueError('comparison supports RUB SPBFUT routes in integer lots')
        if type(self.holding_type) is not int or self.holding_type not in (0, 1, 2, 4):
            raise ValueError('select an explicit documented holding type')
        if not isinstance(self.price_step, Decimal) or decimal(self.price_step) <= 0:
            raise ValueError('positive Decimal price step required')
        fields = self.commission_fields
        if fields is not None and (not isinstance(fields, tuple) or len(set(fields)) != len(fields)
                                   or any(f not in COMMISSION_FIELDS for f in fields)):
            raise ValueError('declare distinct known commission fields or leave unknown')

    @property
    def key(self) -> tuple[str, str, str]:
        return self.firmid, self.route.account_id, self.route.instrument.id.symbol

    def price(self, value: object) -> Decimal:
        result = decimal(value)
        if result % self.price_step:
            raise ValueError('price off tick grid')
        return result


@dataclass(frozen=True)
class ComparisonPolicy:
    profile: str
    quik_version: str
    gateway_id: str
    session_id: str
    trading_date: date
    timezone_offset: timedelta
    max_age: timedelta
    routes: tuple[ObservedRoute, ...]

    def __post_init__(self) -> None:
        for value in (self.profile, self.gateway_id, self.session_id):
            identifier(value)
        if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self.quik_version):
            raise ValueError('exact four-component terminal version required')
        if (type(self.trading_date) is not date or not isinstance(self.max_age, timedelta)
                or self.max_age <= timedelta() or not isinstance(self.timezone_offset, timedelta)
                or not -timedelta(hours=24) < self.timezone_offset < timedelta(hours=24)):
            raise ValueError('explicit trading date, timezone and freshness bound required')
        if (not isinstance(self.routes, tuple) or not self.routes
                or not all(isinstance(r, ObservedRoute) for r in self.routes)
                or len({r.key for r in self.routes}) != len(self.routes)
                or len({r.route for r in self.routes}) != len(self.routes)
                or any(r.route.instrument.id.gateway_id != self.gateway_id for r in self.routes)):
            raise ValueError('comparison routes must be nonempty and unambiguous within one gateway')


@dataclass(frozen=True)
class BrokerLink:
    route: Route
    broker_id: int
    local_id: str

    def __post_init__(self) -> None:
        integer(self.broker_id)
        if not isinstance(self.local_id, str) or not self.local_id:
            raise ValueError('explicit local identifier required')


@dataclass(frozen=True)
class QuikBindings:
    gateway_id: str
    session_id: str
    revision: int
    orders: tuple[BrokerLink, ...]
    executions: tuple[BrokerLink, ...]

    def __post_init__(self) -> None:
        identifier(self.gateway_id)
        identifier(self.session_id)
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError('gateway revision must be nonnegative')
        if any(not isinstance(group, tuple) or not all(isinstance(b, BrokerLink) for b in group)
               for group in (self.orders, self.executions)):
            raise ValueError('bindings require immutable typed tuples')


@dataclass(frozen=True)
class ObservedOrder:
    route: Route
    broker_id: int
    side: Side
    quantity: int
    balance: int
    price: Decimal | None
    status: OrderStatus


@dataclass(frozen=True)
class ObservedTrade:
    route: Route
    broker_id: int
    order_id: int
    side: Side
    quantity: int
    price: Decimal
    commission: Decimal | None
    occurred_at: datetime


@dataclass(frozen=True)
class ObservedPosition:
    route: Route
    quantity: int


@dataclass(frozen=True)
class QuikObservation:
    orders: tuple[ObservedOrder, ...]
    trades: tuple[ObservedTrade, ...]
    positions: tuple[ObservedPosition, ...]
    issues: tuple[str, ...]
    duplicate_trades: int
    excluded_rows: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class Difference:
    code: str
    route: Route | None = None
    broker_id: int | None = None
    local_id: str | None = None
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ComparisonReport:
    scope: ComparisonPolicy
    observation: QuikObservation
    oms_revision: int
    gateway_revision: int
    differences: tuple[Difference, ...]
    limitations: tuple[str, ...] = BROKER_LIMITATIONS

    @property
    def consistent(self) -> bool:
        """No differences within the declared observations; NOT reconciled."""
        return not self.observation.issues and not self.differences

    @property
    def reconciled(self) -> bool:
        return False

    @property
    def can_apply(self) -> bool:
        return False

    @property
    def eligible_for_trading(self) -> bool:
        return False

    @property
    def eligible_for_capital(self) -> bool:
        return False


def _envelope(raw: dict[str, Any], policy: ComparisonPolicy, now: datetime) -> list[str]:
    issues = []
    runtime = raw.get('runtime')
    if (type(raw.get('schema')) is not int or raw.get('schema') != 1 or raw.get('mode') != 'read_only'
            or not isinstance(runtime, dict) or runtime.get('profile') != policy.profile
            or runtime.get('quik_version') != policy.quik_version or runtime.get('lua_version') != 'Lua 5.4'
            or type(runtime.get('protocol')) is not int or runtime.get('protocol') != 1
            or runtime.get('mode') != 'read_only'):
        issues.append('snapshot.identity_unverified')
    try:
        started, finished = (utc(datetime.fromisoformat(raw[k])) for k in ('started_at', 'finished_at'))
        if not started <= finished <= utc(now) or utc(now) - started > policy.max_age:
            raise ValueError('stale or future observation')
    except (KeyError, TypeError, ValueError):
        issues.append('snapshot.time_unverified')
    expected_date = policy.trading_date.strftime('%d.%m.%Y')
    if any(raw.get(k) != expected_date for k in ('trading_date_before', 'trading_date_after')):
        issues.append('snapshot.trading_date_unverified')
    try:
        boundary = raw['callback_boundary']
        baseline = CallbackCursor.parse(raw['runtime']['callback_cursor'])
        begin, end = (CallbackCursor.parse(boundary[k]) for k in ('begin', 'end'))
        if (type(boundary.get('schema')) is not int or boundary.get('schema') != 1
                or boundary.get('kind') != 'transport_cursor' or boundary.get('scope') != 'after_connection_baseline'
                or boundary.get('delivery_verified') is not True or boundary.get('quiet') is not True
                or boundary.get('broker_atomic') is not False or begin != end
                or begin.bridge_session != baseline.bridge_session or begin.failed_sends != baseline.failed_sends
                or begin.sequence < baseline.sequence):
            raise ValueError('unverified transport boundary')
        before, after = (integer(raw.get(k), zero=True) for k in ('callbacks_before', 'callbacks_after'))
        if before != after:
            raise ValueError('callbacks during observation')
    except (KeyError, TypeError, AttributeError, ValueError):
        issues.append('snapshot.boundary_unverified')
    if raw.get('issues') != [] or raw.get('repeatable_observation') is not True or raw.get('reconciled') is not False:
        issues.append('snapshot.unverified')
    return issues


def normalize_quik_snapshot(raw: dict[str, Any], policy: ComparisonPolicy, *, now: datetime) -> QuikObservation:
    issues = _envelope(raw, policy, now)
    orders: dict[tuple[Route, int], ObservedOrder] = {}
    trades: dict[tuple[Route, int], ObservedTrade] = {}
    positions: dict[Route, ObservedPosition] = {}
    excluded = dict.fromkeys(('orders', 'trades', 'holdings'), 0)
    duplicates = 0
    try:
        tables, previous = raw['tables'], raw['previous_tables']
        for name in ('accounts', 'orders', 'trades', 'holdings', 'limits'):
            _table(tables[name]); _table(previous[name])
        if canonical(tables) != canonical(previous):
            issues.append('snapshot.tables_changed')
    except (KeyError, TypeError, ValueError):
        return QuikObservation((), (), (), tuple(issues + ['snapshot.invalid_tables']), 0, tuple(excluded.items()))
    accounts = {(s.firmid, s.route.account_id) for s in policy.routes}
    by_key = {s.key: s for s in policy.routes}
    account_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in tables['accounts']:
        try:
            account_key = identifier(row.get('firmid')), _account_identifier(row.get('trdaccid'))
            account_rows.setdefault(account_key, []).append(row)
        except ValueError:
            issues.append('accounts.invalid_identity')
    for firm, account in sorted(accounts):
        matches = account_rows.get((firm, account), [])
        if (len(matches) != 1 or not isinstance(matches[0].get('class_codes'), str)
                or 'SPBFUT' not in matches[0]['class_codes'].split('|')):
            issues.append('accounts.scope_unverified')
    for name in excluded:
        for row in tables[name]:
            try:
                firm = identifier(row.get('firmid'))
                account = _account_identifier(row.get('trdaccid' if name == 'holdings' else 'account'))
                symbol = identifier(row.get('sec_code'))
                venue = 'SPBFUT' if name == 'holdings' else identifier(row.get('class_code'))
                if (firm, account) not in accounts or venue != 'SPBFUT':
                    excluded[name] += 1
                    continue
                spec = by_key.get((firm, account, symbol))
                if spec is None:
                    issues.append(name + '.unconfigured_route')
                    continue
                route = spec.route
                if name == 'holdings':
                    kind = row.get('type')
                    if type(kind) is not int or kind not in (0, 1, 2, 4):
                        raise ValueError('invalid holding type')
                    if kind != spec.holding_type:
                        excluded[name] += 1
                        continue
                    quantity = row.get('totalnet')
                    if type(quantity) is not int or not -MAX_TRANSACTION_ID <= quantity <= MAX_TRANSACTION_ID:
                        raise ValueError('invalid signed lots')
                    if route in positions:
                        issues.append('holdings.duplicate')
                    else:
                        positions[route] = ObservedPosition(route, quantity)
                    continue
                flags = integer(row.get('flags'), zero=True)
                side = Side.SELL if flags & 4 else Side.BUY
                quantity = integer(row.get('qty'), maximum=MAX_TRANSACTION_ID)
                if name == 'orders':
                    number = integer(row.get('order_num'))
                    balance = integer(row.get('balance'), zero=True, maximum=quantity)
                    active, cancelled, rejected = bool(flags & 1), bool(flags & 2), bool(flags & 1024)
                    if ((active and (cancelled or rejected or balance == 0))
                            or (not active and not cancelled and not rejected and balance != 0)):
                        raise ValueError('contradictory order state')
                    status = (OrderStatus.REJECTED if rejected else OrderStatus.CANCELLED if cancelled
                              else OrderStatus.FILLED if not active else OrderStatus.PARTIALLY_FILLED
                              if balance < quantity else OrderStatus.ACCEPTED)
                    order = ObservedOrder(route, number, side, quantity, balance,
                                          spec.price(row.get('price')) if flags & 8 else None, status)
                    if (route, number) in orders:
                        issues.append('orders.duplicate')
                    else:
                        orders[route, number] = order
                else:
                    if flags & 16:
                        raise ValueError('cancelled trade needs explicit correction handling')
                    fields = spec.commission_fields
                    commission = None if fields is None else sum((decimal(row.get(f)) for f in fields), Decimal())
                    trade = ObservedTrade(route, integer(row.get('trade_num')), integer(row.get('order_num')),
                        side, quantity, spec.price(row.get('price')), commission,
                        timestamp(row.get('datetime'), policy.timezone_offset))
                    if trade.occurred_at > utc(datetime.fromisoformat(raw['finished_at'])):
                        raise ValueError('trade is later than observation')
                    prior = trades.get((route, trade.broker_id))
                    if prior is not None:
                        if prior != trade:
                            issues.append('trades.conflicting_duplicate')
                        else:
                            duplicates += 1
                    else:
                        trades[route, trade.broker_id] = trade
            except (KeyError, TypeError, ValueError, ArithmeticError):
                issues.append(name + '.invalid_row')
    return QuikObservation(tuple(orders.values()), tuple(trades.values()), tuple(positions.values()),
                           tuple(sorted(set(issues))), duplicates, tuple(excluded.items()))


def compare_quik(raw: dict[str, Any], policy: ComparisonPolicy, oms: OmsObservation, bindings: QuikBindings, *,
                 now: datetime, current_revision: int) -> ComparisonReport:
    """Compare immutable observations; session binding is explicit, never guessed.

    The caller supplies the current OMS revision again to detect a view that has
    changed during collection. This is diagnostic, not a compare-and-swap apply.
    """
    observation = normalize_quik_snapshot(raw, policy, now=now)
    differences: list[Difference] = []

    def result() -> ComparisonReport:
        return ComparisonReport(policy, observation, oms.revision, bindings.revision, tuple(differences))

    if type(current_revision) is not int or current_revision != oms.revision:
        differences.append(Difference('oms_revision_changed'))
    if oms.gateway_id != policy.gateway_id:
        differences.append(Difference('oms_scope_mismatch'))
    if not timedelta() <= utc(now) - oms.captured_at <= policy.max_age:
        differences.append(Difference('oms_time_unverified'))
    if (bindings.gateway_id, bindings.session_id) != (policy.gateway_id, policy.session_id):
        differences.append(Difference('binding_scope_mismatch'))
    if observation.issues:
        differences.append(Difference('observation_unverified'))
    else:
        end = utc(datetime.fromisoformat(raw['finished_at']))
        try:
            if any(utc(t) > min(end, oms.captured_at) for t in
                   [o.submitted_at for o in oms.orders] + [e.timestamp for e in oms.executions]):
                differences.append(Difference('local_event_after_observation'))
        except (TypeError, ValueError):
            differences.append(Difference('oms_time_unverified'))
    scope = {s.route for s in policy.routes}
    if any(route not in scope for route in [o.intent.route for o in oms.orders]
           + [e.route for e in oms.executions] + [p.route for _, p in oms.positions]):
        differences.append(Difference('local_route_outside_scope'))
    if (len({o.order_id for o in oms.orders}) != len(oms.orders)
            or len({e.key for e in oms.executions}) != len(oms.executions)
            or len({(owner, p.route) for owner, p in oms.positions}) != len(oms.positions)
            or len(set(oms.local_rejections)) != len(oms.local_rejections)):
        differences.append(Difference('oms_ambiguous'))
    for group in (bindings.orders, bindings.executions):
        if (len({(b.route, b.broker_id) for b in group}) != len(group)
                or len({b.local_id for b in group}) != len(group)):
            differences.append(Difference('binding_ambiguous'))
        if any(b.route not in scope for b in group):
            differences.append(Difference('binding_scope_mismatch'))
    if differences:
        return result()
    local_orders = {o.order_id: o for o in oms.orders}
    local_fills = {e.execution_id: e for e in oms.executions}
    order_links = {(b.route, b.broker_id): b.local_id for b in bindings.orders}
    execution_links = {(b.route, b.broker_id): b.local_id for b in bindings.executions}
    by_local_order = {b.local_id: (b.route, b.broker_id) for b in bindings.orders}
    by_local_fill = {b.local_id: (b.route, b.broker_id) for b in bindings.executions}
    observed_orders = {(o.route, o.broker_id): o for o in observation.orders}
    observed_trades = {(t.route, t.broker_id): t for t in observation.trades}
    trade_quantities: dict[tuple[Route, int], int] = {}
    for trade in observation.trades:
        trade_key = trade.route, trade.order_id
        trade_quantities[trade_key] = trade_quantities.get(trade_key, 0) + trade.quantity
    for link in bindings.orders:
        if link.local_id not in local_orders:
            differences.append(Difference('binding_target_missing', link.route, link.broker_id, link.local_id))
    for link in bindings.executions:
        if link.local_id not in local_fills and (link.route, link.broker_id) not in observed_trades:
            differences.append(Difference('binding_target_missing', link.route, link.broker_id, link.local_id))
    rejected = set(oms.local_rejections)
    if any(i not in local_orders or local_orders[i].status is not OrderStatus.REJECTED
           or local_orders[i].filled_quantity != 0 for i in rejected):
        differences.append(Difference('oms_invalid_local_rejection'))
        return result()
    try:
        for order in oms.orders:
            if order.order_id in rejected:
                if order.order_id in by_local_order:
                    differences.append(Difference('local_rejection_has_broker_binding', order.intent.route,
                                                  local_id=order.order_id))
                continue
            route = order.intent.route
            if order.status in (OrderStatus.SUBMITTED, OrderStatus.CANCEL_PENDING):
                differences.append(Difference('order_outcome_unresolved', route, local_id=order.order_id))
            key = by_local_order.get(order.order_id)
            if key is None:
                differences.append(Difference('order_unmapped', route, local_id=order.order_id))
                continue
            observed = observed_orders.get(key)
            if observed is None:
                differences.append(Difference('order_not_observed', route, key[1], order.order_id))
                continue
            fields = tuple(name for name, actual, expected in (
                ('route', observed.route, route), ('side', observed.side, order.intent.side),
                ('quantity', observed.quantity, decimal(order.intent.quantity)),
                ('filled_quantity', observed.quantity - observed.balance, decimal(order.filled_quantity)),
                ('price', observed.price, None if order.intent.limit_price is None else decimal(order.intent.limit_price)),
                ('status', observed.status, order.status)) if actual != expected)
            if fields:
                differences.append(Difference('order_mismatch', route, key[1], order.order_id, fields))
        for key, observed in observed_orders.items():
            local_id = order_links.get(key)
            if local_id is None:
                differences.append(Difference('external_order', observed.route, observed.broker_id))
            quantity = trade_quantities.get(key, 0)
            if quantity != observed.quantity - observed.balance:
                differences.append(Difference('order_execution_coverage_gap', observed.route, observed.broker_id, local_id))
        for key, trade in observed_trades.items():
            local_id = execution_links.get(key)
            expected_order = order_links.get((trade.route, trade.order_id))
            if (trade.route, trade.order_id) not in observed_orders:
                differences.append(Difference('execution_order_not_observed', trade.route, trade.broker_id, local_id))
            if trade.commission is None:
                differences.append(Difference('commission_unverified', trade.route, trade.broker_id, local_id))
            if local_id is None:
                differences.append(Difference('external_execution', trade.route, trade.broker_id))
                continue
            fill = local_fills.get(local_id)
            if fill is None:
                differences.append(Difference('execution_not_recorded', trade.route, trade.broker_id, local_id))
                continue
            fields = tuple(name for name, actual, expected in (
                ('route', trade.route, fill.route), ('order_id', expected_order, fill.order_id),
                ('side', trade.side, fill.side), ('quantity', trade.quantity, decimal(fill.quantity)),
                ('price', trade.price, decimal(fill.price)), ('timestamp', trade.occurred_at, fill.timestamp),
                ('commission', trade.commission, decimal(fill.commission))) if actual != expected)
            if fields:
                differences.append(Difference('execution_mismatch', trade.route, trade.broker_id, local_id, fields))
        for fill in oms.executions:
            key = by_local_fill.get(fill.execution_id)
            if key is None:
                differences.append(Difference('execution_unmapped', fill.route, local_id=fill.execution_id))
            elif key not in observed_trades:
                differences.append(Difference('execution_not_observed', fill.route, key[1], fill.execution_id))
        positions = {p.route: p.quantity for p in observation.positions}
        totals = dict.fromkeys(scope, Decimal())
        for _, position in oms.positions:
            local_quantity = decimal(position.quantity)
            if local_quantity != local_quantity.to_integral_value():
                raise ValueError('fractional local futures position')
            totals[position.route] += local_quantity
        for spec in policy.routes:
            route = spec.route
            if route not in positions:
                differences.append(Difference('position_not_observed', route))
            elif positions[route] != totals[route]:
                differences.append(Difference('position_mismatch', route))
    except (ValueError, ArithmeticError):
        differences.append(Difference('oms_invalid_values'))
    return result()

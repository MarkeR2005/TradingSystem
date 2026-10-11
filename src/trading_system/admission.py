"""Operator-owned admission snapshots and conservative quantity exposure.

Valuations are explicit notional marks, not broker margin guarantees. Financial
positions remain owned by PositionLedger; reservations are derived from OMS.
"""

from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction

from .capital import AccountKey, CapitalUsage, _exact, _name, _nonnegative, _number
from .domain import InstrumentId, Route, Side, utc


@dataclass(frozen=True)
class AccountLimit:
    account: AccountKey
    capacity: float
    external_usage: float = 0

    def __post_init__(self) -> None:
        _nonnegative(self.capacity, 'account capacity')
        _nonnegative(self.external_usage, 'external usage')


@dataclass(frozen=True)
class StrategyLimit:
    strategy_id: str
    account: AccountKey
    capacity: float

    def __post_init__(self) -> None:
        _name(self.strategy_id, 'strategy')
        _nonnegative(self.capacity, 'strategy capacity')


@dataclass(frozen=True)
class RouteValuation:
    route: Route
    price: float

    def __post_init__(self) -> None:
        _nonnegative(self.price, 'absolute valuation price', strictly=True)


@dataclass(frozen=True)
class RouteGrant:
    strategy_id: str
    route: Route

    def __post_init__(self) -> None:
        _name(self.strategy_id, 'strategy')


@dataclass(frozen=True)
class CapitalPolicy:
    revision: int
    as_of: datetime
    expires_at: datetime
    report_currency: str
    accounts: tuple[AccountLimit, ...]
    valuations: tuple[RouteValuation, ...]
    grants: tuple[RouteGrant, ...]
    budgets: tuple[StrategyLimit, ...]

    def __post_init__(self) -> None:
        if type(self.revision) is not int or self.revision < 1:
            raise ValueError('capital revision must be a positive integer')
        object.__setattr__(self, 'as_of', utc(self.as_of))
        object.__setattr__(self, 'expires_at', utc(self.expires_at))
        if self.expires_at <= self.as_of:
            raise ValueError('capital validity interval is empty')
        _name(self.report_currency, 'report currency')
        for items, cls in ((self.accounts, AccountLimit), (self.valuations, RouteValuation),
                           (self.grants, RouteGrant), (self.budgets, StrategyLimit)):
            if not isinstance(items, tuple) or not all(isinstance(item, cls) for item in items):
                raise ValueError('capital policy requires immutable typed tuples')
        accounts = {item.account for item in self.accounts}
        routes = {item.route for item in self.valuations}
        grants = {(item.strategy_id, item.route) for item in self.grants}
        budgets = {(item.strategy_id, item.account) for item in self.budgets}
        if (len(accounts) != len(self.accounts) or len(routes) != len(self.valuations)
                or len(grants) != len(self.grants) or len(budgets) != len(self.budgets)):
            raise ValueError('duplicate capital policy key')
        instruments: dict[InstrumentId, object] = {}
        for route in routes:
            if AccountKey.from_route(route) not in accounts:
                raise ValueError('valuation account is unknown')
            if route.instrument.currency != self.report_currency:
                raise ValueError('FX admission is not implemented')
            if instruments.setdefault(route.instrument.id, route.instrument) != route.instrument:
                raise ValueError('conflicting instrument metadata')
        for grant in self.grants:
            if (grant.route not in routes
                    or (grant.strategy_id, AccountKey.from_route(grant.route)) not in budgets):
                raise ValueError('route grant requires a valuation and strategy budget')
        if any(budget.account not in accounts for budget in self.budgets):
            raise ValueError('strategy budget account is unknown')

    def price(self, route: Route) -> float:
        for valuation in self.valuations:
            if valuation.route == route:
                return valuation.price
        raise ValueError('route has no capital valuation')


@dataclass(frozen=True)
class ConfigureCapital:
    policy: CapitalPolicy

    def __post_init__(self) -> None:
        if not isinstance(self.policy, CapitalPolicy):
            raise ValueError('invalid capital policy')


@dataclass(frozen=True)
class CapitalDeficit:
    account: AccountKey
    amount: float
    strategy_id: str | None = None


@dataclass(frozen=True)
class RouteExposure:
    strategy_id: str
    route: Route
    position: Fraction
    buys: Fraction
    sells: Fraction
    price: Fraction

    @property
    def maximum(self) -> Fraction:
        return max(abs(self.position), abs(self.position + self.buys), abs(self.position - self.sells))

    @property
    def unit_cost(self) -> Fraction:
        return self.price * _exact(self.route.instrument.contract_multiplier)

    def bounded_exit(self, side: Side) -> bool:
        if side is Side.SELL:
            return self.position > 0 and self.sells <= self.position and self.buys == 0
        return self.position < 0 and self.buys <= -self.position and self.sells == 0


def usage_from_exposure(exposures: tuple[RouteExposure, ...]) -> tuple[CapitalUsage, ...]:
    totals: dict[tuple[str, AccountKey], tuple[Fraction, Fraction]] = {}
    for item in exposures:
        key = item.strategy_id, AccountKey.from_route(item.route)
        positions, orders = totals.get(key, (Fraction(), Fraction()))
        positions += abs(item.position) * item.unit_cost
        orders += (item.maximum - abs(item.position)) * item.unit_cost
        totals[key] = positions, orders
    return tuple(CapitalUsage(owner, account, _number(positions), _number(orders))
                 for (owner, account), (positions, orders) in sorted(totals.items()))

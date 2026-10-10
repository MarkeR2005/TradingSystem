"""Pure capacity planning from explicit snapshots; never sends or closes orders."""

from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction
from math import fsum

from .domain import Instrument, InstrumentId, Route, finite, utc


def _name(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field} must not be empty')


def _nonnegative(value: float, field: str, *, strictly: bool = False) -> None:
    finite(value, field)
    if isinstance(value, bool) or value < 0 or (strictly and value == 0):
        raise ValueError(f'invalid {field}')


def _exact(value: float) -> Fraction:
    return Fraction(str(value))


def _number(value: Fraction) -> float:
    try:
        result = float(value)
    except OverflowError as error:
        raise ValueError('capital calculation exceeds finite reporting range') from error
    finite(result, 'capital calculation')
    return result


@dataclass(frozen=True, order=True)
class AccountKey:
    gateway_id: str
    account_id: str

    def __post_init__(self) -> None:
        _name(self.gateway_id, 'gateway')
        _name(self.account_id, 'account')

    @classmethod
    def from_route(cls, route: Route) -> 'AccountKey':
        return cls(route.instrument.id.gateway_id, route.account_id)


@dataclass(frozen=True)
class AccountCapital:
    account: AccountKey
    currency: str
    equity: float
    max_leverage: float = 1
    reserve: float = 0

    def __post_init__(self) -> None:
        _name(self.currency, 'currency')
        finite(self.equity, 'equity')  # Negative equity is a valid deficit snapshot.
        _nonnegative(self.max_leverage, 'account leverage', strictly=True)
        _nonnegative(self.reserve, 'cash reserve')


@dataclass(frozen=True)
class CapitalUsage:
    """Occupied notional in account currency, aggregated once per owner/account."""

    strategy_id: str
    account: AccountKey
    positions: float = 0
    orders: float = 0

    def __post_init__(self) -> None:
        _name(self.strategy_id, 'usage owner')
        _nonnegative(self.positions, 'position usage')
        _nonnegative(self.orders, 'order usage')


@dataclass(frozen=True)
class CapitalLeg:
    """One fixed route and a whole-lot quantity in the minimal basket unit."""

    route: Route
    quantity_per_unit: float
    price: float

    def __post_init__(self) -> None:
        _nonnegative(self.quantity_per_unit, 'quantity per unit', strictly=True)
        finite(self.price, 'valuation price')
        if self.price == 0 or not self.route.instrument.accepts_quantity(self.quantity_per_unit):
            raise ValueError('leg needs a nonzero valuation and a valid lot quantity')
        step = self.route.instrument.quantity_step
        object.__setattr__(self, 'quantity_per_unit', _number(_exact(step) * round(self.quantity_per_unit / step)))


@dataclass(frozen=True)
class StrategyCapital:
    strategy_id: str
    weight: float
    legs: tuple[CapitalLeg, ...]
    desired_leverage: float = 1

    def __post_init__(self) -> None:
        _name(self.strategy_id, 'strategy')
        _nonnegative(self.weight, 'weight')
        _nonnegative(self.desired_leverage, 'weight modifier', strictly=True)
        if (not isinstance(self.legs, tuple) or (self.weight > 0 and not self.legs)
                or len({leg.route for leg in self.legs}) != len(self.legs)):
            raise ValueError('strategy needs unique fixed routes; a positive weight requires legs')


@dataclass(frozen=True)
class FxRate:
    currency: str
    rate: float

    def __post_init__(self) -> None:
        _name(self.currency, 'FX currency')
        _nonnegative(self.rate, 'FX rate', strictly=True)


@dataclass(frozen=True)
class FxSnapshot:
    """One unit of currency costs rate units of report_currency, at as_of."""

    report_currency: str
    as_of: datetime
    rates: tuple[FxRate, ...]

    def __post_init__(self) -> None:
        _name(self.report_currency, 'report currency')
        object.__setattr__(self, 'as_of', utc(self.as_of))
        if (not isinstance(self.rates, tuple) or len({rate.currency for rate in self.rates}) != len(self.rates)
                or any(rate.currency == self.report_currency for rate in self.rates)):
            raise ValueError('FX currencies must be unique; report currency has implicit rate 1')

    def factor(self, currency: str) -> Fraction:
        if currency == self.report_currency:
            return Fraction(1)
        for rate in self.rates:
            if rate.currency == currency:
                return _exact(rate.rate)
        raise ValueError(f'missing FX rate for {currency}')


@dataclass(frozen=True)
class LegAllocation:
    route: Route
    quantity: float
    capital: float  # Report currency, additional allocation only.


@dataclass(frozen=True)
class StrategyBudget:
    strategy_id: str
    effective_weight: float  # Percent, normalized across all positive weights.
    occupied: float
    additional: float
    units: int
    legs: tuple[LegAllocation, ...]

    @property
    def total(self) -> float:
        return self.occupied + self.additional


@dataclass(frozen=True)
class AccountBudget:
    account: AccountKey
    capacity: float
    positions: float
    orders: float
    additional: float
    available: float
    deficit: float


@dataclass(frozen=True)
class CapitalPlan:
    as_of: datetime
    report_currency: str
    strategies: tuple[StrategyBudget, ...]
    accounts: tuple[AccountBudget, ...]

    def strategy(self, strategy_id: str) -> StrategyBudget:
        for item in self.strategies:
            if item.strategy_id == strategy_id:
                return item
        raise KeyError(strategy_id)

    def account(self, account: AccountKey) -> AccountBudget:
        for item in self.accounts:
            if item.account == account:
                return item
        raise KeyError(account)

    @property
    def total_additional(self) -> float:
        return fsum(item.additional for item in self.accounts)

    @property
    def total_available(self) -> float:
        return fsum(item.available for item in self.accounts)

    @property
    def total_deficit(self) -> float:
        return fsum(item.deficit for item in self.accounts)


def _topups(floors: dict[str, Fraction], weights: dict[str, Fraction], free: Fraction) -> dict[str, Fraction]:
    """Weighted account targets with already-occupied/granted amounts as floors."""
    pending = set(floors)
    pool = sum(floors.values(), Fraction()) + free
    result = dict.fromkeys(floors, Fraction())
    while pending:
        weight = sum((weights[name] for name in pending), Fraction())
        targets = {name: pool * weights[name] / weight for name in pending}
        fixed = {name for name in pending if targets[name] < floors[name]}
        if not fixed:
            for name in pending:
                result[name] = targets[name] - floors[name]
            break
        for name in fixed:
            pool -= floors[name]
        pending -= fixed
    return result


def allocate_capital(accounts: tuple[AccountCapital, ...], strategies: tuple[StrategyCapital, ...],
                     usage: tuple[CapitalUsage, ...], fx: FxSnapshot) -> CapitalPlan:
    """Deterministic weighted passes on fixed routes, followed by lot rounding.

    Each account divides free capacity among feasible strategies using occupied
    floors. A basket uses one common integer scale on all its legs. Residual
    units follow account-count, effective-weight, ID priority. This is a greedy
    fixed-route planner, not a global integer optimizer or an OMS reservation.
    """
    sources = {item.account: item for item in accounts}
    requests = {item.strategy_id: item for item in strategies}
    if len(sources) != len(accounts) or len(requests) != len(strategies):
        raise ValueError('duplicate account or strategy identifier')
    positions = dict.fromkeys(sources, Fraction())
    orders = dict.fromkeys(sources, Fraction())
    occupied: dict[tuple[str, AccountKey], Fraction] = {}
    for item in usage:
        if item.account not in sources or (item.strategy_id, item.account) in occupied:
            raise ValueError('usage has an unknown account or duplicate owner/account')
        rate = fx.factor(sources[item.account].currency)
        positions[item.account] += _exact(item.positions) * rate
        orders[item.account] += _exact(item.orders) * rate
        occupied[item.strategy_id, item.account] = (_exact(item.positions) + _exact(item.orders)) * rate
    capacity = {key: max(Fraction(), _exact(item.equity) - _exact(item.reserve))
                * _exact(item.max_leverage) * fx.factor(item.currency) for key, item in sources.items()}
    free = {key: max(Fraction(), capacity[key] - positions[key] - orders[key]) for key in sources}
    initial_free = free.copy()
    costs: dict[str, dict[AccountKey, Fraction]] = {}
    leg_costs: dict[str, tuple[Fraction, ...]] = {}
    instruments: dict[InstrumentId, Instrument] = {}
    for name, request in requests.items():
        costs[name] = {}
        values: list[Fraction] = []
        for leg in request.legs:
            key = AccountKey.from_route(leg.route)
            instrument = leg.route.instrument
            if key not in sources:
                raise ValueError('leg uses an unknown account')
            if instrument.id in instruments and instruments[instrument.id] != instrument:
                raise ValueError('conflicting instrument metadata')
            instruments[instrument.id] = instrument
            cost = (_exact(abs(leg.price)) * _exact(leg.quantity_per_unit)
                    * _exact(instrument.contract_multiplier) * fx.factor(instrument.currency))
            costs[name][key] = costs[name].get(key, Fraction()) + cost
            values.append(cost)
        leg_costs[name] = tuple(values)
    weights = {name: _exact(item.weight) * _exact(item.desired_leverage) for name, item in requests.items()}
    weight_sum = sum(weights.values(), Fraction())
    priority = sorted((name for name in requests if weights[name]),
                      key=lambda name: (-len(costs[name]), -weights[name], name))
    units = dict.fromkeys(requests, 0)

    def affordable(name: str) -> int:
        return min(int(free[key] // cost) for key, cost in costs[name].items())

    def grant(name: str, quantity: int) -> None:
        units[name] += quantity
        for key, cost in costs[name].items():
            free[key] -= cost * quantity

    while True:
        eligible = [name for name in priority if affordable(name) > 0]
        if not eligible:
            break
        budgets: dict[AccountKey, dict[str, Fraction]] = {}
        for key in sources:
            floors = {name: occupied.get((name, key), Fraction()) + units[name] * costs[name][key]
                      for name in eligible if key in costs[name]}
            budgets[key] = _topups(floors, weights, free[key])
        progress = False
        for name in eligible:
            count = min(int(budgets[key][name] // cost) for key, cost in costs[name].items())
            count = min(count, affordable(name))
            if count:
                grant(name, count)
                progress = True
        if not progress:
            grant(eligible[0], 1)

    account_results = tuple(AccountBudget(
        key, _number(capacity[key]), _number(positions[key]), _number(orders[key]),
        _number(initial_free[key] - free[key]), _number(free[key]),
        _number(max(Fraction(), positions[key] + orders[key] - capacity[key])),
    ) for key in sorted(sources))
    strategy_results = tuple(StrategyBudget(
        name, _number(100 * weights[name] / weight_sum) if weight_sum else 0,
        _number(sum((amount for (owner, _), amount in occupied.items() if owner == name), Fraction())),
        _number(units[name] * sum(costs[name].values(), Fraction())), units[name],
        tuple(LegAllocation(leg.route, _number(_exact(leg.quantity_per_unit) * units[name]),
                            _number(cost * units[name]))
              for leg, cost in zip(requests[name].legs, leg_costs[name])),
    ) for name in sorted(requests))
    # Reports must stay finite even when every individual input was finite.
    _number(sum(capacity.values(), Fraction()) + sum(positions.values(), Fraction()) + sum(orders.values(), Fraction()))
    return CapitalPlan(fx.as_of, fx.report_currency, strategy_results, account_results)

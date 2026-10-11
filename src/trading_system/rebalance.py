"""Coherent source bundles and planning inputs, independent of financial ownership."""

from dataclasses import dataclass, replace
from datetime import datetime
from fractions import Fraction
from typing import Protocol

from .admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from .capital import (
    AccountCapital, AccountKey, CapitalLeg, CapitalPlan, CapitalUsage, FxSnapshot,
    StrategyCapital, _exact, _name, _nonnegative, _number, allocate_capital,
)
from .domain import Route, utc
from .inventory import MANUAL


@dataclass(frozen=True)
class AllocationLeg:
    route: Route
    quantity_per_unit: float

    def __post_init__(self) -> None:
        # Reuse the planner's grid validation and normalization without taking a
        # price from strategy configuration. Prices belong to the source bundle.
        leg = CapitalLeg(self.route, self.quantity_per_unit, 1)
        object.__setattr__(self, 'quantity_per_unit', leg.quantity_per_unit)


@dataclass(frozen=True)
class CapitalTarget:
    strategy_id: str
    weight: float
    legs: tuple[AllocationLeg, ...]
    desired_leverage: float = 1

    def __post_init__(self) -> None:
        if self.strategy_id == MANUAL and self.weight != 0:
            raise ValueError('manual strategy weight must be zero')
        if not isinstance(self.legs, tuple) or not all(isinstance(leg, AllocationLeg) for leg in self.legs):
            raise ValueError('allocation legs must be an immutable typed tuple')
        StrategyCapital(self.strategy_id, self.weight,
                        tuple(CapitalLeg(leg.route, leg.quantity_per_unit, 1) for leg in self.legs),
                        self.desired_leverage)


@dataclass(frozen=True)
class ExternalCapital:
    """Only resource not already represented by this OMS/ledger, in RUB."""

    account: AccountKey
    positions: float = 0
    orders: float = 0

    def __post_init__(self) -> None:
        if not isinstance(self.account, AccountKey):
            raise ValueError('external usage requires an account key')
        _nonnegative(self.positions, 'external positions')
        _nonnegative(self.orders, 'external orders')


@dataclass(frozen=True)
class CapitalSnapshot:
    """Full atomic bundle supplied by one trusted source, never a partial patch."""

    source_id: str
    sequence: int
    as_of: datetime
    expires_at: datetime
    report_currency: str
    accounts: tuple[AccountCapital, ...]
    valuations: tuple[RouteValuation, ...]
    external: tuple[ExternalCapital, ...] = ()

    def __post_init__(self) -> None:
        _name(self.source_id, 'capital source')
        if type(self.sequence) is not int or self.sequence < 1:
            raise ValueError('source sequence must be a positive integer')
        object.__setattr__(self, 'as_of', utc(self.as_of))
        object.__setattr__(self, 'expires_at', utc(self.expires_at))
        for items, cls in ((self.accounts, AccountCapital), (self.valuations, RouteValuation),
                           (self.external, ExternalCapital)):
            if not isinstance(items, tuple) or not all(isinstance(item, cls) for item in items):
                raise ValueError('capital snapshot requires immutable typed tuples')
        keys = {account.account for account in self.accounts}
        if len(keys) != len(self.accounts):
            raise ValueError('duplicate account snapshot')
        if (len({item.account for item in self.external}) != len(self.external)
                or any(item.account not in keys for item in self.external)):
            raise ValueError('external usage has a duplicate or unknown account')
        if self.report_currency != 'RUB' or any(account.currency != 'RUB' for account in self.accounts):
            raise ValueError('rebalance currently requires RUB accounts and valuations')
        # Also validates metadata uniqueness, route accounts, dates and finite capacity.
        initial_policy(self, (), 1)


class CapitalSource(Protocol):
    async def load(self) -> CapitalSnapshot: ...


class InMemoryCapitalSource:
    """Explicit, replaceable fixture for offline refreshes; never fabricates a new sequence."""

    def __init__(self, snapshot: CapitalSnapshot) -> None:
        self.snapshot = snapshot

    async def load(self) -> CapitalSnapshot:
        return self.snapshot


@dataclass(frozen=True)
class RebalanceCapital:
    snapshot: CapitalSnapshot
    targets: tuple[CapitalTarget, ...]
    expected_revision: int

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, CapitalSnapshot):
            raise ValueError('rebalance requires a capital snapshot')
        if type(self.expected_revision) is not int or self.expected_revision < 0:
            raise ValueError('expected revision must be a nonnegative integer')
        if (not isinstance(self.targets, tuple)
                or not all(isinstance(target, CapitalTarget) for target in self.targets)
                or len({target.strategy_id for target in self.targets}) != len(self.targets)):
            raise ValueError('rebalance requires unique strategy targets')


def initial_policy(snapshot: CapitalSnapshot, targets: tuple[CapitalTarget, ...], revision: int) -> CapitalPolicy:
    external = {item.account: _number(_exact(item.positions) + _exact(item.orders)) for item in snapshot.external}
    accounts = tuple(AccountLimit(
        account.account,
        _number(max(Fraction(), _exact(account.equity) - _exact(account.reserve)) * _exact(account.max_leverage)),
        external.get(account.account, 0),
    ) for account in snapshot.accounts)
    grants = tuple(RouteGrant(target.strategy_id, leg.route) for target in targets for leg in target.legs)
    keys = {(grant.strategy_id, AccountKey.from_route(grant.route)) for grant in grants}
    return CapitalPolicy(revision, snapshot.as_of, snapshot.expires_at, snapshot.report_currency,
                         accounts, snapshot.valuations, grants,
                         tuple(StrategyLimit(owner, account, 0) for owner, account in sorted(keys)))


def allocate_policy(command: RebalanceCapital, usage: tuple[CapitalUsage, ...],
                    valuations: tuple[RouteValuation, ...]) -> tuple[CapitalPlan, CapitalPolicy]:
    """Turn a plan into absolute budgets, retaining occupied floors for every owner."""
    snapshot = command.snapshot
    policy = replace(initial_policy(snapshot, command.targets, command.expected_revision + 1), valuations=valuations)
    targets = tuple(StrategyCapital(target.strategy_id, target.weight,
                                   tuple(CapitalLeg(leg.route, leg.quantity_per_unit, policy.price(leg.route))
                                         for leg in target.legs), target.desired_leverage)
                    for target in command.targets)
    # External resource has no strategy permissions or financial owner in the
    # ledger yet. A collision-free planning-only key keeps it out of real budgets.
    names = {item.strategy_id for item in usage} | {target.strategy_id for target in targets}
    external_owner = 'external'
    while external_owner in names:
        external_owner += '_'
    all_usage = usage + tuple(CapitalUsage(external_owner, item.account, item.positions, item.orders)
                              for item in snapshot.external)
    plan = allocate_capital(snapshot.accounts, targets, all_usage,
                            FxSnapshot(snapshot.report_currency, snapshot.as_of, ()))
    budgets = {(item.strategy_id, item.account): _exact(item.positions) + _exact(item.orders) for item in usage}
    for result in plan.strategies:
        for leg in result.legs:
            key = result.strategy_id, AccountKey.from_route(leg.route)
            budgets[key] = budgets.get(key, Fraction()) + _exact(leg.capital)
    result_policy = replace(policy, budgets=tuple(StrategyLimit(owner, account, _number(amount))
                                                 for (owner, account), amount in sorted(budgets.items())))
    return plan, result_policy

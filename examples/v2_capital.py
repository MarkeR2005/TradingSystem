"""Snapshot-only capital plan: account reserve, FX, basket ratio and deficit."""

from dataclasses import replace
from datetime import UTC, datetime

from trading_system.capital import (
    AccountCapital, AccountKey, CapitalLeg, CapitalUsage, FxRate, FxSnapshot,
    StrategyCapital, allocate_capital,
)
from trading_system.domain import Instrument, InstrumentId, Route


def main() -> None:
    first, second = AccountKey('sim', 'rub-account'), AccountKey('sim', 'usd-account')
    rub = Instrument(InstrumentId('sim', 'TEST', 'RUB-ASSET'), 'RUB')
    usd = Instrument(InstrumentId('sim', 'TEST', 'USD-ASSET'), 'USD')
    accounts = (AccountCapital(first, 'RUB', 110, reserve=10), AccountCapital(second, 'USD', 3))
    usage = (CapitalUsage('manual', first, positions=20, orders=10),)
    strategies = (
        StrategyCapital('basket', 50, (CapitalLeg(Route(rub, first.account_id), 2, 10),
                                       CapitalLeg(Route(usd, second.account_id), 1, 0.3)), desired_leverage=2),
        StrategyCapital('single', 100, (CapitalLeg(Route(usd, second.account_id), 1, 0.1),)),
        StrategyCapital('manual', 0, ()),
    )
    fx = FxSnapshot('RUB', datetime(2026, 10, 11, tzinfo=UTC), (FxRate('USD', 100),))
    plan = allocate_capital(accounts, strategies, usage, fx)
    assert plan.strategy('basket').units == 3
    assert [item.quantity for item in plan.strategy('basket').legs] == [6, 3]
    assert plan.strategy('single').units == 21
    assert plan.strategy('manual').occupied == 30
    assert plan.total_additional == 360 and plan.total_available == 10
    for item in plan.strategies:
        print(f'{item.strategy_id}: weight={item.effective_weight:g}%, occupied={item.occupied:g}, '
              f'additional={item.additional:g} RUB, basket units={item.units}')
    print(f'Additional: {plan.total_additional:g} RUB; unused: {plan.total_available:g} RUB')
    deficit = allocate_capital((replace(accounts[0], equity=10, reserve=0), accounts[1]), strategies, usage, fx)
    assert deficit.total_deficit == 20
    assert deficit.strategy('manual').occupied == 30
    assert deficit.strategy('basket').units == 0
    print(f'After withdrawal: deficit={deficit.total_deficit:g} RUB; '
          f'manual occupation preserved={deficit.strategy("manual").occupied:g} RUB')


if __name__ == '__main__':
    main()

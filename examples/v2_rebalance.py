"""Source snapshot -> live OMS usage -> weighted budgets -> restart -> deficit."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import RouteValuation
from trading_system.capital import AccountCapital, AccountKey
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, OrderStatus, Route, Side
from trading_system.persistence import DurableJournal
from trading_system.rebalance import AllocationLeg, CapitalSnapshot, CapitalTarget, ExternalCapital, InMemoryCapitalSource
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 10, 11, 11, tzinfo=UTC)
INSTRUMENT = Instrument(InstrumentId('sim', 'TEST', 'DEMO'), 'RUB')
ROUTE = Route(INSTRUMENT, 'demo')
ACCOUNT = AccountKey.from_route(ROUTE)


class Probe(AutoStrategy):
    def __init__(self, name: str) -> None:
        super().__init__(name, (INSTRUMENT.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


def budget(runtime: TradingRuntime, owner: str) -> float:
    policy = runtime.orders.capital_policy
    assert policy is not None
    return next(item.capacity for item in policy.budgets if item.strategy_id == owner)


async def main() -> None:
    initial = CapitalSnapshot(
        'offline-fixture', 1, START, START + timedelta(hours=1), 'RUB',
        (AccountCapital(ACCOUNT, 'RUB', 1000),), (RouteValuation(ROUTE, 100),),
        (ExternalCapital(ACCOUNT, positions=100, orders=100),))
    source = InMemoryCapitalSource(initial)
    first = CapitalTarget('a', 1, (AllocationLeg(ROUTE, 1),), desired_leverage=3)
    second = CapitalTarget('b', 1, (AllocationLeg(ROUTE, 1),))
    changed = (replace(first, weight=0), second)
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(START), journal=journal, require_capital=True) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                runtime.add_gateway(gateway)
                a = await runtime.start_strategy(Probe('a'))
                b = await runtime.start_strategy(Probe('b'))
                await runtime.rebalance_capital(source, (first, second))
                assert (budget(runtime, 'a'), budget(runtime, 'b')) == (600, 200)
                await a.place_order(OrderIntent(ROUTE, Side.BUY, 5, 100))
                await runtime.drain()
                await gateway.fill(a.orders()[0].order_id, 100, quantity=2)
                await runtime.drain()
                source.snapshot = replace(initial, sequence=2)
                plan = await runtime.rebalance_capital(source, changed)
                assert (budget(runtime, 'a'), budget(runtime, 'b')) == (500, 300)
                assert plan.strategy('a').occupied == 500
                assert a.position(ROUTE).quantity == 2
                await b.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                await runtime.drain()
                assert b.orders()[0].status is OrderStatus.REJECTED
                observed = gateway.order_snapshots()
                print('Initial budgets: a=600, b=200 RUB; external usage=200 RUB.')
                print('After weight change: a=500 retained (position 200 + orders 300), b=300 RUB.')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                gateway.restore_orders(observed)
                runtime.add_gateway(gateway)
                a = await runtime.start_strategy(Probe('a'))
                await runtime.start_strategy(Probe('b'))
                assert runtime.orders.capital_plan == plan
                assert runtime.orders.capital_snapshot == source.snapshot
                assert not runtime.orders.ready
                await runtime.reconcile_gateway('sim', gateway.order_snapshots())
                assert not runtime.orders.ready
                source.snapshot = replace(initial, sequence=3, accounts=(AccountCapital(ACCOUNT, 'RUB', 600),))
                plan = await runtime.rebalance_capital(source, changed)
                assert runtime.orders.ready
                assert (budget(runtime, 'a'), budget(runtime, 'b')) == (500, 0)
                assert plan.total_deficit == 100
                assert a.position(ROUTE).quantity == 2
                assert a.orders()[0].remaining_quantity == 3
                print('Restart restored plan and source sequence; fresh snapshot reports deficit=100 RUB.')
                print('Position and active order preserved; new allocation to b=0 RUB.')


if __name__ == '__main__':
    asyncio.run(main())

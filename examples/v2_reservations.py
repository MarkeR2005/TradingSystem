"""Plan -> admission -> replace/fill -> recovery -> bounded exit in a deficit."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountCapital, AccountKey, CapitalLeg, FxSnapshot, StrategyCapital, allocate_capital
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, OrderStatus, Route, Side
from trading_system.persistence import DurableJournal
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 10, 11, 10, tzinfo=UTC)
INSTRUMENT = Instrument(InstrumentId('sim', 'TEST', 'DEMO'), 'RUB')
ROUTE = Route(INSTRUMENT, 'demo')
ACCOUNT = AccountKey.from_route(ROUTE)


class Probe(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('probe', (INSTRUMENT.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


async def main() -> None:
    plan = allocate_capital(
        (AccountCapital(ACCOUNT, 'RUB', 210, reserve=10),),
        (StrategyCapital('probe', 100, (CapitalLeg(ROUTE, 1, 100),), desired_leverage=3),),
        (), FxSnapshot('RUB', START, ()))
    policy = CapitalPolicy(
        1, START, START + timedelta(hours=1), 'RUB',
        (AccountLimit(ACCOUNT, plan.account(ACCOUNT).capacity),),
        (RouteValuation(ROUTE, 100),), (RouteGrant('probe', ROUTE),),
        (StrategyLimit('probe', ACCOUNT, plan.strategy('probe').total),))
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(START), journal=journal, require_capital=True) as runtime:
                gateway = SimulationGateway('sim', runtime.clock, auto_confirm_cancels=False)
                runtime.add_gateway(gateway)
                await runtime.configure_capital(policy)
                context = await runtime.start_strategy(Probe())
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))
                await runtime.drain()
                original = context.orders()[0]
                await context.replace_order(original.order_id, OrderIntent(ROUTE, Side.BUY, 2, 99))
                await runtime.drain()
                assert runtime.orders.capital_usage()[0].orders == 200
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 1, 100))
                await runtime.drain()
                assert context.orders()[-1].status is OrderStatus.REJECTED
                await gateway.fill(original.order_id, 100)
                await runtime.drain()
                await gateway.confirm_cancel(original.order_id)
                await runtime.drain()
                usage = runtime.orders.capital_usage()[0]
                assert (usage.positions, usage.orders) == (100, 100)
                observed = gateway.order_snapshots()  # Independent simulated broker state.
                print('Replace reserved 200 RUB; competing order rejected; fill moved 100 RUB to position.')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                gateway.restore_orders(observed)
                runtime.add_gateway(gateway)
                context = await runtime.start_strategy(Probe())
                assert not runtime.orders.ready
                assert runtime.orders.capital_usage()[0] == usage
                await runtime.reconcile_gateway('sim', gateway.order_snapshots())
                assert not runtime.orders.ready
                await runtime.configure_capital(replace(policy, revision=2))
                assert runtime.orders.ready
                child = context.orders()[-1]
                await gateway.fill(child.order_id, 99)
                await runtime.drain()
                await runtime.configure_capital(replace(policy, revision=3, accounts=(AccountLimit(ACCOUNT, 100),)))
                assert runtime.orders.capital_deficits()[0].amount == 100
                await context.place_order(OrderIntent(ROUTE, Side.SELL, 1, 100))
                await runtime.drain()
                await gateway.fill(context.orders()[-1].order_id, 100)
                await runtime.drain()
                assert runtime.orders.capital_deficits() == ()
                assert context.position(ROUTE).quantity == 1
                print('Recovery preserved reservations; reconciliation + fresh policy reopened admission.')
                print('Deficit: 100 -> 0 RUB after a bounded exit; remaining position: 1 lot.')


if __name__ == '__main__':
    asyncio.run(main())

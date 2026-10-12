"""Cross-account proportional reduction, an explicit retry, and durable replay."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.deficit import AcknowledgeDeficit, DeficitPhase, DeficitQuotes, RetryDeficit, StartDeficit
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal
from trading_system.plans import FailurePolicy, PlanLeg, PlanPhase, PlanPrice, TradePlan
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
LONG = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'LONG'), 'RUB'), 'main')
SHORT = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'SHORT'), 'RUB'), 'hedge')
MAIN = AccountKey.from_route(LONG)
HEDGE = AccountKey.from_route(SHORT)


class Basket(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('basket', (LONG.instrument.id, SHORT.instrument.id))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


def capital(revision: int, capacity: float) -> CapitalPolicy:
    return CapitalPolicy(revision, START, START + timedelta(hours=1), 'RUB',
                         (AccountLimit(MAIN, capacity), AccountLimit(HEDGE, 1000)),
                         (RouteValuation(LONG, 100), RouteValuation(SHORT, 100)),
                         (RouteGrant('basket', LONG), RouteGrant('basket', SHORT)),
                         (StrategyLimit('basket', MAIN, 1000), StrategyLimit('basket', HEDGE, 1000)))


def quotes(clock: ManualClock) -> DeficitQuotes:
    return DeficitQuotes(clock.now(), clock.now() + timedelta(minutes=5),
                         (PlanPrice(LONG, 100), PlanPrice(SHORT, 100)))


async def main() -> None:
    clock = ManualClock(START)
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                await runtime.configure_capital(capital(1, 1000))
                await context.place_plan(TradePlan(
                    'basket-plan', 'basket', (PlanLeg(OrderIntent(LONG, Side.BUY, 6, 100), 100),
                                              PlanLeg(OrderIntent(SHORT, Side.SELL, 4, 100), 100)),
                    FailurePolicy.COMPLETE, START + timedelta(seconds=30), 100))
                await runtime.drain()
                for order in context.orders():
                    await broker.fill(order.order_id, 100)
                await runtime.drain()
                await runtime.configure_capital(capital(2, 400))
                await runtime.start_deficit(StartDeficit('account-risk', runtime.orders.revision,
                                                        MAIN, 10, 30, quotes(clock)))
                assert context.status is StrategyStatus.PAUSED
                assert len(context.orders()) == 2
                print('Alert and pause: 200 RUB deficit, waiting 10 seconds')
                clock.advance_to(START + timedelta(seconds=10))
                await runtime.check_deficits()
                first, partner = context.orders()[2:]
                assert [o.intent.quantity for o in (first, partner)] == [3, 2]
                print('Smallest proportional exit: 3 LONG and 2 SHORT, relief 300 RUB')
                await broker.fill(first.order_id, 100)
                await broker.reject_order(partner.order_id, 'controlled exit refusal')
                await runtime.drain()
                assert runtime.orders.deficit('account-risk').phase is DeficitPhase.BLOCKED
                assert all(d.account != MAIN or d.strategy_id is not None for d in runtime.orders.capital_deficits())
                await runtime.retry_deficit(RetryDeficit('retry', runtime.orders.revision,
                                                        'account-risk', quotes(clock)))
                retry = context.orders()[-1]
                assert retry.intent == partner.intent
                await broker.fill(retry.order_id, 100)
                await runtime.drain()
                assert (context.position(LONG).quantity, context.position(SHORT).quantity) == (3, -2)
                assert context.plan('basket-plan').phase is PlanPhase.HELD
                await runtime.acknowledge_deficit(AcknowledgeDeficit('ack', runtime.orders.revision, 'account-risk'))
                assert context.status is StrategyStatus.PAUSED
                state = runtime.orders.deficit('account-risk')
                observed = broker.order_snapshots()
                print('Explicit retry finished only the missing hedge leg; remaining ratio 3:2, paused')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(observed)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                assert runtime.orders.deficit('account-risk') == state
                assert context.status is StrategyStatus.PAUSED
                await runtime.reconcile_gateway('sim', observed)
                await runtime.configure_capital(capital(3, 400))
                assert broker.order_snapshots() == observed
                assert len(context.orders()) == 5
                assert context.position(LONG).quantity == 3
                assert context.position(SHORT).quantity == -2
                print('Replay preserved all five orders and remaining positions without resubmission')


if __name__ == '__main__':
    asyncio.run(main())

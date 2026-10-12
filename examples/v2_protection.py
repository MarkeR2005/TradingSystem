"""Aggregate TP/trailing with parameter changes, net fees, and two restarts."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal
from trading_system.plans import (
    FailurePolicy, PlanLeg, PlanMarks, PlanPhase, PlanPrice, PlanProtection,
    ProtectionKind, TradePlan, TrailingStop, UpdatePlanProtection,
)
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
LONG = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'LONG'), 'RUB'), 'demo')
SHORT = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'SHORT'), 'RUB'), 'demo')
ACCOUNT = AccountKey.from_route(LONG)


class Basket(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('basket', (LONG.instrument.id, SHORT.instrument.id))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


def capital(revision: int) -> CapitalPolicy:
    return CapitalPolicy(revision, START, START + timedelta(hours=1), 'RUB',
                         (AccountLimit(ACCOUNT, 1000),),
                         (RouteValuation(LONG, 130), RouteValuation(SHORT, 130)),
                         (RouteGrant('basket', LONG), RouteGrant('basket', SHORT)),
                         (StrategyLimit('basket', ACCOUNT, 1000),))


def marks(sequence: int, price: float) -> PlanMarks:
    return PlanMarks('protected', sequence, START, START + timedelta(minutes=1),
                     (PlanPrice(LONG, price), PlanPrice(SHORT, 100)))


async def scenario(kind: ProtectionKind) -> None:
    clock = ManualClock(START)
    take_profit = kind is ProtectionKind.TAKE_PROFIT
    spec = TradePlan('protected', 'basket', (PlanLeg(OrderIntent(LONG, Side.BUY, 2, 100), 100),
                                            PlanLeg(OrderIntent(SHORT, Side.SELL, 2, 100), 100)),
                     FailurePolicy.PROTECT, START + timedelta(seconds=30), stop_loss=100,
                     take_profit=20 if take_profit else None,
                     trailing=None if take_profit else TrailingStop(20, activation=20))
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                await runtime.configure_capital(capital(1))
                await context.place_plan(spec)
                await runtime.drain()
                for order in context.orders():
                    await broker.fill(order.order_id, 100, commission=1)
                await runtime.drain()
                await runtime.mark_plan(marks(1, 110 if take_profit else 126))
                if not take_profit:
                    await runtime.update_plan_protection(UpdatePlanProtection(
                        'tighten', runtime.orders.revision, 'basket', 'protected',
                        PlanProtection(100, trailing=TrailingStop(15, activation=20))))
                prior = context.plan('protected')
                assert prior.peak_pnl == (18 if take_profit else 50)
                assert prior.phase is PlanPhase.OPEN
                observed = broker.order_snapshots()
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(observed)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                assert context.plan('protected') == prior
                await runtime.reconcile_gateway('sim', observed)
                await runtime.configure_capital(capital(2))
                assert broker.order_snapshots() == observed
                price = 111 if take_profit else 118.5
                await runtime.mark_plan(marks(2, price))
                state = context.plan('protected')
                trigger = state.protection_trigger
                assert trigger is not None and trigger.kind is kind
                assert trigger.threshold == (20 if take_profit else 35)
                assert context.status is StrategyStatus.PAUSED
                for order in context.orders()[2:]:
                    await broker.fill(order.order_id, price if order.intent.route == LONG else 100, commission=1)
                await runtime.drain()
                state = context.plan('protected')
                expected = 18 if take_profit else 33
                assert state.phase is PlanPhase.CLOSED
                assert context.trades()[-1].pnl == expected
                assert context.status is StrategyStatus.PAUSED
                observed = broker.order_snapshots()
                print(f'{kind.value}: peak={state.peak_pnl}, trigger={trigger.pnl}, final P&L={expected} RUB')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(observed)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                await runtime.reconcile_gateway('sim', observed)
                await runtime.configure_capital(capital(3))
                assert context.plan('protected') == state
                assert runtime.ledger.positions() == ()
                assert len(context.orders()) == 4
                assert broker.order_snapshots() == observed
                print(f'{kind.value}: both restarts preserved settings, peak and trigger without resubmission')


async def main() -> None:
    for kind in (ProtectionKind.TAKE_PROFIT, ProtectionKind.TRAILING):
        await scenario(kind)


if __name__ == '__main__':
    asyncio.run(main())

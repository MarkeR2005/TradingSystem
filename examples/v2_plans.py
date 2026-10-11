"""Three declared leg-failure policies on the same durable offline OMS."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.lifecycle import StrategyStatus
from trading_system.persistence import DurableJournal
from trading_system.plans import AcknowledgePlan, FailurePolicy, PlanLeg, PlanMarks, PlanPhase, PlanPrice, TradePlan
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
                         (AccountLimit(ACCOUNT, 1200),),
                         (RouteValuation(LONG, 120), RouteValuation(SHORT, 120)),
                         (RouteGrant('basket', LONG), RouteGrant('basket', SHORT)),
                         (StrategyLimit('basket', ACCOUNT, 1200),))


async def scenario(failure_policy: FailurePolicy) -> None:
    clock = ManualClock(START)
    spec = TradePlan('plan', 'basket', (PlanLeg(OrderIntent(LONG, Side.BUY, 2, 100), 80),
                                      PlanLeg(OrderIntent(SHORT, Side.SELL, 2, 100), 110)),
                     failure_policy, START + timedelta(seconds=10), stop_loss=20, recovery_timeout=10)
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock, auto_confirm_cancels=False)
                runtime.add_gateway(broker)
                context = await runtime.start_strategy(Basket())
                await runtime.configure_capital(capital(1))
                await context.place_plan(spec)
                await runtime.drain()
                first, second = context.orders()
                assert sum(u.orders for u in runtime.orders.capital_usage()) == 480
                await broker.fill(first.order_id, 100, quantity=1, commission=1)
                await broker.reject_order(second.order_id, 'controlled leg failure')
                await runtime.drain()
                assert context.status is StrategyStatus.PAUSED
                if failure_policy is FailurePolicy.CLOSE:
                    # A fill races with the cancellation. The exit must include it.
                    await broker.fill(first.order_id, 100, quantity=1, commission=1)
                await broker.confirm_cancel(first.order_id)
                await runtime.drain()
                if failure_policy is FailurePolicy.COMPLETE:
                    remaining = context.orders()[2:]
                    assert [o.intent.quantity for o in remaining] == [1, 2]
                    for order in remaining:
                        await broker.fill(order.order_id, 100)
                    await runtime.drain()
                    assert context.plan('plan').phase is PlanPhase.OPEN
                    assert context.status is StrategyStatus.PAUSED
                    await runtime.acknowledge_plan(AcknowledgePlan('ack', runtime.orders.revision, 'plan'))
                    await runtime.resume_strategy('basket')
                    await context.close_plan('plan')
                    await runtime.drain()
                elif failure_policy is FailurePolicy.PROTECT:
                    assert context.plan('plan').phase is PlanPhase.HELD
                    assert context.position(LONG).quantity == 1
                    await runtime.mark_plan(PlanMarks('plan', 1, clock.now(), START + timedelta(seconds=2),
                                                      (PlanPrice(LONG, 80), PlanPrice(SHORT, 100))))
                    assert context.plan('plan').pnl == -21
                state = context.plan('plan')
                exits = [o for o in context.orders() if o.order_id in state.exit_ids]
                for order in exits:
                    price = 110 if failure_policy is FailurePolicy.COMPLETE and order.intent.side is Side.SELL else (
                        95 if order.intent.side is Side.BUY else 80)
                    await broker.fill(order.order_id, price, commission=1)
                await runtime.drain()
                expected = {FailurePolicy.PROTECT: -22, FailurePolicy.COMPLETE: 27, FailurePolicy.CLOSE: -43}[failure_policy]
                assert runtime.ledger.positions() == ()
                assert context.trades()[-1].pnl == expected
                state = context.plan('plan')
                assert state.phase is PlanPhase.CLOSED
                observed = broker.order_snapshots()
                print(f'{failure_policy.value}: closed, P&L={expected} RUB, alerts={len(state.alerts)}')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as restored:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(observed)
                restored.add_gateway(broker)
                await restored.start_strategy(Basket())
                assert restored.orders.plan('plan') == state
                await restored.reconcile_gateway('sim', observed)
                await restored.configure_capital(capital(2))
                assert restored.ledger.positions() == ()
                assert restored.ledger.trades('basket')[-1].pnl == expected
                print(f'{failure_policy.value}: replay preserved orders, alerts and P&L')


async def main() -> None:
    for failure_policy in FailurePolicy:
        await scenario(failure_policy)


if __name__ == '__main__':
    asyncio.run(main())

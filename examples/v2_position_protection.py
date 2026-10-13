"""Manual protection, partial transfer, trailing exit and two durable restarts."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Candle, Instrument, InstrumentId, OrderSnapshot, Route
from trading_system.inventory import MANUAL, BrokerPosition, InventoryReport, OpeningBalance, TransferPosition
from trading_system.persistence import DurableJournal
from trading_system.plans import (
    PlanMarks, PlanPhase, PlanPrice, PlanProtection, ProtectPositions, ProtectionKind,
    ReleasePositionProtection, TrailingStop,
)
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
ROUTE = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB'), 'demo')
ACCOUNT = AccountKey.from_route(ROUTE)


class Receiver(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('receiver', (ROUTE.instrument.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


def capital(revision: int) -> CapitalPolicy:
    return CapitalPolicy(revision, START, START + timedelta(hours=1), 'RUB',
                         (AccountLimit(ACCOUNT, 1000),), (RouteValuation(ROUTE, 150),),
                         tuple(RouteGrant(owner, ROUTE) for owner in (MANUAL, 'receiver')),
                         tuple(StrategyLimit(owner, ACCOUNT, 1000) for owner in (MANUAL, 'receiver')))


def marks(identifier: str, sequence: int, price: float) -> PlanMarks:
    return PlanMarks(identifier, sequence, START, START + timedelta(minutes=1), (PlanPrice(ROUTE, price),))


def protection(runtime: TradingRuntime, owner: str, identifier: str, price: float) -> ProtectPositions:
    return ProtectPositions('attach-' + identifier, runtime.orders.revision, owner, marks(identifier, 1, price),
                            PlanProtection(100, trailing=TrailingStop(6, activation=10)),
                            (PlanPrice(ROUTE, 100),))


def reconcile(runtime: TradingRuntime, identifier: str, quantity: float,
              observed: tuple[OrderSnapshot, ...]) -> InventoryReport:
    return InventoryReport(identifier, runtime.orders.revision, 'sim', START, (), (),
                           (BrokerPosition(ROUTE, quantity),) if quantity else (), observed)


async def main() -> None:
    clock = ManualClock(START)
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                runtime.add_gateway(broker)
                await runtime.start_strategy(Receiver())
                await runtime.reconcile_inventory(InventoryReport(
                    'import', runtime.orders.revision, 'sim', START, (OpeningBalance(ROUTE, 4, 100),), (),
                    (BrokerPosition(ROUTE, 4),), ()))
                await runtime.configure_capital(capital(1))
                await runtime.protect_positions(protection(runtime, MANUAL, 'original', 110))
                assert not broker.order_snapshots()
                await runtime.release_position_protection(ReleasePositionProtection(
                    'release-original', runtime.orders.revision, MANUAL, 'original'))
                await runtime.pause_strategy('receiver')
                await runtime.transfer_position(TransferPosition(
                    'transfer-one', runtime.orders.revision, MANUAL, 'receiver', ROUTE, 1))
                await runtime.configure_capital(capital(2))
                await runtime.protect_positions(protection(runtime, MANUAL, 'remaining', 110))
                before = runtime.orders.plan('remaining')
                assert before.peak_pnl == 30 and before.opening_positions[0].quantity == 3
                assert not broker.order_snapshots()
                print('Imported 4 lots; released original protection, transferred 1 and protected remaining 3. No fills invented.')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                runtime.add_gateway(broker)
                await runtime.start_strategy(Receiver())
                assert runtime.orders.plan('remaining') == before
                assert not runtime.orders.ready
                await runtime.reconcile_inventory(reconcile(runtime, 'reconcile-first', 4, ()))
                await runtime.configure_capital(capital(3))
                await runtime.mark_plan(marks('remaining', 2, 108))
                state = runtime.orders.plan('remaining')
                assert state.protection_trigger is not None
                assert state.protection_trigger.kind is ProtectionKind.TRAILING
                assert state.protection_trigger.pnl == 24
                close = runtime.orders.orders(MANUAL)[0]
                assert close.intent.quantity == 3
                await broker.fill(close.order_id, 108, commission=1)
                await runtime.drain()
                assert runtime.ledger.position('receiver', ROUTE).quantity == 1
                await runtime.protect_positions(protection(runtime, 'receiver', 'received', 100))
                await runtime.close_plan('received')
                await broker.fill(runtime.orders.orders('receiver')[0].order_id, 106, commission=1)
                await runtime.drain()
                assert runtime.ledger.positions() == ()
                assert runtime.ledger.settlements(MANUAL)[0].pnl == 23
                assert runtime.ledger.settlements('receiver')[0].pnl == 5
                after = runtime.orders.plan('remaining')
                observed = broker.order_snapshots()
                print('Trailing: peak 30, trigger 24, manual settlement 23 RUB; receiver settlement 5 RUB.')
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(observed)
                runtime.add_gateway(broker)
                await runtime.start_strategy(Receiver())
                assert runtime.orders.plan('remaining') == after
                assert runtime.orders.plan('original').phase is PlanPhase.RELEASED
                await runtime.reconcile_inventory(reconcile(runtime, 'reconcile-second', 0, observed))
                await runtime.configure_capital(capital(4))
                assert broker.order_snapshots() == observed and len(observed) == 2
                assert runtime.ledger.positions() == ()
                assert sum(runtime.ledger.equity(owner)[-1].value for owner in (MANUAL, 'receiver')) == 28
                print('Second restart: ownership, protection, released state and 28 RUB equity preserved; no resubmission.')


if __name__ == '__main__':
    asyncio.run(main())

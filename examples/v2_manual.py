"""External inventory -> manual -> partial ownership transfer -> real closes -> restart."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.admission import RouteValuation
from trading_system.capital import AccountCapital, AccountKey
from trading_system.domain import Candle, Execution, Instrument, InstrumentId, OrderIntent, OrderSnapshot, OrderStatus, Route, Side
from trading_system.inventory import MANUAL, BrokerPosition, CloseManual, ExternalOrder, InventoryReport, OpeningBalance, TransferPosition
from trading_system.persistence import DurableJournal
from trading_system.rebalance import AllocationLeg, CapitalSnapshot, CapitalTarget, InMemoryCapitalSource
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock

START = datetime(2026, 1, 1, 10, tzinfo=UTC)
INSTRUMENT = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
ROUTE = Route(INSTRUMENT, 'demo')


class Receiver(AutoStrategy):
    def __init__(self) -> None:
        super().__init__('receiver', (INSTRUMENT.id,))

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


def source(sequence: int, as_of: datetime) -> InMemoryCapitalSource:
    return InMemoryCapitalSource(CapitalSnapshot(
        'demo', sequence, as_of, START + timedelta(hours=1), 'RUB',
        (AccountCapital(AccountKey.from_route(ROUTE), 'RUB', 1200),),
        (RouteValuation(ROUTE, 120),),
    ))


async def main() -> None:
    clock = ManualClock(START)
    targets = (CapitalTarget(MANUAL, 0, (AllocationLeg(ROUTE, 1),)),
               CapitalTarget('receiver', 100, (AllocationLeg(ROUTE, 1),)))
    outside = OrderSnapshot('outside', 'broker-user', OrderIntent(ROUTE, Side.BUY, 6, 100),
                            START - timedelta(minutes=1), OrderStatus.PARTIALLY_FILLED, 4, 100, 4)
    past = Execution('historical', outside.order_id, ROUTE, Side.BUY, 4, 100, 4, START)
    gateway = SimulationGateway('sim', clock)
    gateway.restore_orders((outside,))
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as runtime:
                runtime.add_gateway(gateway)
                receiver = await runtime.start_strategy(Receiver())
                await runtime.pause_strategy('receiver')
                await runtime.reconcile_inventory(InventoryReport(
                    'import', runtime.orders.revision, 'sim', clock.now(),
                    (OpeningBalance(ROUTE, 4, 100),), (ExternalOrder(outside, (past,)),),
                    (BrokerPosition(ROUTE, 4),), gateway.order_snapshots(),
                ))
                await runtime.orders.handle(past)
                assert runtime.ledger.position(MANUAL, ROUTE).quantity == 4
                await runtime.cancel_manual(outside.order_id)
                await runtime.rebalance_capital(source(1, clock.now()), targets)
                await runtime.transfer_position(TransferPosition(
                    'transfer-half', runtime.orders.revision, MANUAL, 'receiver', ROUTE, 2,
                ))
                assert runtime.ledger.position(MANUAL, ROUTE).quantity == receiver.position(ROUTE).quantity == 2
                print('Ownership after transfer: manual=2, receiver=2; total=4')
                await runtime.rebalance_capital(source(2, clock.now()), targets)
                await runtime.resume_strategy('receiver')
                await runtime.close_manual(CloseManual('manual-exit', runtime.orders.revision, ROUTE, 2, 110))
                await receiver.place_order(OrderIntent(ROUTE, Side.SELL, 2, 110))
                await runtime.bus.drain()
                clock.advance_to(START + timedelta(seconds=1))
                await gateway.fill(runtime.orders.orders(MANUAL)[-1].order_id, 110, commission=1)
                await gateway.fill(receiver.orders()[-1].order_id, 110, commission=1)
                await runtime.bus.drain()
                assert runtime.ledger.positions() == ()
                assert runtime.ledger.equity(MANUAL)[-1].value == receiver.equity()[-1].value == 19
                assert runtime.ledger.trades(MANUAL) == receiver.trades() == ()
                assert len(runtime.ledger.settlements(MANUAL)) == len(receiver.settlements()) == 1
                print('P&L since import: manual=19, receiver=19; total=38 RUB')
                print('Historical commission of 4 RUB belongs to the pre-import period')
                broker_orders = gateway.order_snapshots()
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(clock, journal=journal, require_capital=True) as restored:
                broker = SimulationGateway('sim', clock)
                broker.restore_orders(broker_orders)
                restored.add_gateway(broker)
                await restored.start_strategy(Receiver())
                await restored.rebalance_capital(source(3, clock.now()), targets)
                await restored.reconcile_gateway('sim', broker_orders)
                assert not restored.orders.ready
                await restored.reconcile_inventory(InventoryReport(
                    'restart', restored.orders.revision, 'sim', clock.now(), (), (),
                    (BrokerPosition(ROUTE, 0),), broker_orders,
                ))
                assert restored.orders.ready and restored.ledger.positions() == ()
                assert restored.ledger.equity(MANUAL)[-1].value == 19
                assert restored.ledger.equity('receiver')[-1].value == 19
                print('Restart: inventory reconciled, positions flat, P&L unchanged')


if __name__ == '__main__':
    asyncio.run(main())

"""Durable partial fill, restart, deduplication, verified resume and flat equity."""

import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from trading_system.domain import Candle, Instrument, InstrumentId, OrderIntent, Route, Side
from trading_system.persistence import DurableJournal
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.strategy import AutoStrategy, StrategyContext
from trading_system.time import ManualClock


class RecoveryDemo(AutoStrategy):
    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass


async def main() -> None:
    start = datetime(2026, 1, 1, 10, tzinfo=UTC)
    instrument = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
    route = Route(instrument, 'demo-account')
    with TemporaryDirectory() as directory:
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(start), journal=journal) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                runtime.add_gateway(gateway)
                context = await runtime.start_strategy(RecoveryDemo('recovery-demo', (instrument.id,)))
                await context.place_order(OrderIntent(route, Side.BUY, 2, 100))
                await runtime.drain()
                execution = await gateway.fill(context.orders()[0].order_id, 100, commission=1)
                await runtime.drain()
                # Broker state is a separate fixture, captured independently of OMS replay.
                broker_orders = gateway.order_snapshots()
                original_id = context.orders()[0].order_id
        with DurableJournal(Path(directory)) as journal:
            async with TradingRuntime(ManualClock(start), journal=journal) as runtime:
                gateway = SimulationGateway('sim', runtime.clock)
                gateway.restore_orders(broker_orders)
                runtime.add_gateway(gateway)
                context = await runtime.start_strategy(RecoveryDemo('recovery-demo', (instrument.id,)))
                blocked_before_reconcile = not runtime.orders.ready
                await runtime.bus.publish('orders', execution)
                await runtime.drain()
                restored_quantity = context.position(route).quantity
                await runtime.reconcile_gateway('sim', gateway.order_snapshots())
                await context.place_order(OrderIntent(route, Side.SELL, 2, 110))
                await runtime.drain()
                await gateway.fill(context.orders()[-1].order_id, 110, commission=1)
                await runtime.drain()
                print(json.dumps({
                    'same_order_id_after_restart': context.orders()[0].order_id == original_id,
                    'trading_blocked_before_reconcile': blocked_before_reconcile,
                    'restored_quantity_after_duplicate': restored_quantity,
                    'position': context.position(route).quantity,
                    'closed_pnl_rub': context.trades()[-1].pnl,
                    'closed_equity_rub': context.equity()[-1].value,
                }, indent=2))


if __name__ == '__main__':
    asyncio.run(main())

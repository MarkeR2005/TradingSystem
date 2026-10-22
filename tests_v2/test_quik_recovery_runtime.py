from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.admission import AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.domain import Execution, OrderIntent, OrderStatus, Side
from trading_system.persistence import DurableJournal
from trading_system.quik_durable import QuikDurableGateway
from trading_system.runtime import TradingRuntime
from trading_system.time import ManualClock
from tests_v2.helpers import START
from tests_v2.test_quik_contract import Quiet, ROUTE, SPEC, order_report, trade_report


async def attach(runtime, journal, clock):
    gateway = await QuikDurableGateway.open('quik', (SPEC,), clock, journal=journal,
        session_id='psb-fixture', timezone_offset=timedelta(hours=3))
    runtime.add_gateway(gateway)
    context = await runtime.start_strategy(Quiet())
    await gateway.recover(runtime.orders)
    await runtime.drain()
    return gateway, context


async def capital(runtime):
    account = AccountKey.from_route(ROUTE)
    await runtime.configure_capital(CapitalPolicy(1, START, START + timedelta(hours=1), 'RUB',
        (AccountLimit(account, 2000),), (RouteValuation(ROUTE, 150),),
        (RouteGrant('a', ROUTE),), (StrategyLimit('a', account, 2000),)))


class QuikRecoveryRuntimeTests(IsolatedAsyncioTestCase):
    async def test_repeated_recovery_delivers_missing_fill_and_never_books_duplicates(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            clock = ManualClock(START)
            for iteration in range(3):
                with DurableJournal(root / 'oms') as oms, DurableJournal(root / 'quik') as journal:
                    async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                        gateway, context = await attach(runtime, journal, clock)
                        if iteration == 0:
                            await capital(runtime)
                            await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                            await runtime.drain()
                            await gateway.feed('OnOrder', order_report(1))
                            await runtime.drain()
                            publish = runtime.bus.publish

                            async def interrupt_fill(recipient, message):
                                if isinstance(message, Execution):
                                    raise RuntimeError('process stopped before OMS delivery')
                                await publish(recipient, message)

                            with patch.object(runtime.bus, 'publish', side_effect=interrupt_fill):
                                with self.assertRaises(RuntimeError):
                                    await gateway.feed('OnTrade', trade_report())
                            await runtime.drain()
                            self.assertEqual(runtime.ledger.positions(), ())
                        else:
                            self.assertEqual(context.orders()[0].filled_quantity, 1)
                            position = runtime.ledger.position('a', ROUTE)
                            self.assertEqual((position.quantity, position.realized_pnl), (1, -1.25))
                            self.assertEqual(sum(u.orders for u in runtime.orders.capital_usage()), 450)
                            self.assertFalse(runtime.orders.ready)
                            self.assertEqual(gateway.prepared, ())
                            await gateway.feed('OnTrade', trade_report())
                            await runtime.drain()
                            self.assertEqual(runtime.ledger.position('a', ROUTE), position)
                            self.assertEqual(gateway.next_transaction_id, 2)

    async def test_late_fill_after_recovered_cancel_barrier_cannot_submit_replacement(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            clock = ManualClock(START)
            with DurableJournal(root / 'oms') as oms, DurableJournal(root / 'quik') as journal:
                async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                    gateway, context = await attach(runtime, journal, clock)
                    await capital(runtime)
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 4, 100))
                    await runtime.drain()
                    await gateway.feed('OnOrder', order_report(1))
                    await gateway.feed('OnTrade', trade_report())
                    await runtime.drain()
                    await context.replace_order(context.orders()[0].order_id, OrderIntent(ROUTE, Side.BUY, 4, 99))
                    await runtime.drain()
                    await gateway.feed('OnOrder', order_report(1, balance=2, flags=10))
                    await runtime.drain()
            with DurableJournal(root / 'oms') as oms, DurableJournal(root / 'quik') as journal:
                async with TradingRuntime(clock, journal=oms, require_capital=True) as runtime:
                    gateway, context = await attach(runtime, journal, clock)
                    await gateway.feed('OnTrade', trade_report(trade_num=9007199254740996))
                    await runtime.drain()
                    self.assertEqual(len(context.orders()), 1)
                    self.assertEqual(context.orders()[0].filled_quantity, 2)
                    self.assertEqual(context.orders()[0].status, OrderStatus.CANCEL_PENDING)
                    self.assertEqual(runtime.ledger.position('a', ROUTE).quantity, 2)
                    self.assertEqual(gateway.prepared, ())
                    self.assertEqual(len(gateway.transactions), 2)

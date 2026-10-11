import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.admission import RouteValuation
from trading_system.capital import AccountCapital, AccountKey
from trading_system.domain import CancelOrder, Execution, GatewayCancelled, OrderIntent, OrderStatus, ReplaceOrder, Side, SubmitOrder
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.rebalance import AllocationLeg, CapitalSnapshot, CapitalTarget, ExternalCapital
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_admission import Recorder

KEY = AccountKey.from_route(ROUTE)


def snapshot(equity=1000, *, sequence=1, price=100, external=(), as_of=START):
    return CapitalSnapshot('fixture', sequence, as_of, START + timedelta(hours=1), 'RUB',
                           (AccountCapital(KEY, 'RUB', equity),),
                           (RouteValuation(ROUTE, price),), external)


def target(name='a', weight=100, desired=1, route=ROUTE, quantity=1):
    return CapitalTarget(name, weight, (AllocationLeg(route, quantity),), desired)


class RebalanceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), require_capital=True)
        self.oms.gateway_ids.add('sim')

    async def rebalance(self, snap=None, targets=None, expected=None):
        snap = snapshot() if snap is None else snap
        targets = (target(), target('b')) if targets is None else targets
        if expected is None:
            expected = self.oms.capital_policy.revision if self.oms.capital_policy else 0
        return await self.oms.rebalance_capital(snap, targets, expected_revision=expected)

    def budget(self, owner, account=KEY):
        return next(b.capacity for b in self.oms.capital_policy.budgets
                    if b.strategy_id == owner and b.account == account)

    async def submit(self, owner='a', quantity=1, side=Side.BUY, price=100):
        await self.oms.handle(SubmitOrder(owner, OrderIntent(ROUTE, side, quantity, price)))
        return self.oms.orders(owner)[-1]

    async def test_weights_become_per_account_oms_budgets_not_additional_leverage(self):
        plan = await self.rebalance(targets=(target(desired=3), target('b')))
        self.assertEqual(plan.strategy('a').effective_weight, 75)
        self.assertEqual((self.budget('a'), self.budget('b')), (800, 200))
        self.assertEqual((await self.submit(quantity=8)).status, OrderStatus.SUBMITTED)
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)
        self.assertEqual((await self.submit('b', 2)).status, OrderStatus.SUBMITTED)
        self.assertEqual(self.oms.capital_plan, plan)

    async def test_changed_weights_keep_actual_occupied_floor(self):
        await self.rebalance(targets=(target(),))
        await self.submit(quantity=8)
        plan = await self.rebalance(snapshot(sequence=2), (target(weight=1), target('b', weight=99)))
        self.assertEqual((plan.strategy('a').occupied, plan.strategy('b').additional), (800, 200))
        self.assertEqual((self.budget('a'), self.budget('b')), (800, 200))
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)

    async def test_removed_owner_retains_usage_without_new_route_grants(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=3)
        await self.oms.handle(Execution('fill', order.order_id, ROUTE, Side.BUY, 1, 100, 0, START))
        await self.rebalance(snapshot(sequence=2), (target('b'),))
        self.assertEqual((self.budget('a'), self.budget('b')), (300, 700))
        self.assertEqual(self.oms.capital_deficits(), ())
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 1)

    async def test_pending_replace_is_counted_before_new_allocation(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=2)
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=6)))
        await self.rebalance(snapshot(sequence=2), (target(weight=0), target('b')))
        self.assertEqual((self.budget('a'), self.budget('b')), (600, 400))

    async def test_external_usage_reduces_free_capital_once(self):
        ext = (ExternalCapital(KEY, positions=200, orders=100),)
        plan = await self.rebalance(snapshot(external=ext), (target(),))
        self.assertEqual((plan.account(KEY).positions, plan.account(KEY).orders), (200, 100))
        self.assertEqual(self.budget('a'), 700)
        self.assertEqual(self.oms.capital_policy.accounts[0].external_usage, 300)
        self.assertEqual(self.oms.capital_usage(), ())
        self.assertEqual((await self.submit(quantity=8)).status, OrderStatus.REJECTED)

    async def test_withdrawal_reports_deficit_and_does_not_close_positions(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=3)
        await self.oms.handle(Execution('fill', order.order_id, ROUTE, Side.BUY, 3, 100, 0, START))
        before = tuple(self.oms.bus.events)
        plan = await self.rebalance(snapshot(100, sequence=2))
        self.assertEqual(plan.total_deficit, 200)
        self.assertEqual((self.budget('a'), self.budget('b')), (300, 0))
        self.assertEqual(tuple(self.oms.bus.events), before)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 3)
        self.assertEqual((await self.submit(side=Side.SELL)).status, OrderStatus.SUBMITTED)

    async def test_low_snapshot_price_does_not_undervalue_pending_limit_or_new_units(self):
        await self.rebalance(targets=(target(),))
        await self.submit(quantity=3)
        plan = await self.rebalance(snapshot(sequence=2, price=50), (target(),))
        self.assertEqual(plan.strategy('a').occupied, 300)
        self.assertEqual(plan.strategy('a').units, 7)
        self.assertEqual(self.oms.capital_policy.price(ROUTE), 100)
        self.assertEqual(plan.total_additional, 700)

    async def test_price_of_fill_newer_than_snapshot_is_preserved(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=2, side=Side.SELL)
        self.oms.clock.advance_to(START + timedelta(minutes=1))
        await self.oms.handle(Execution('fill', order.order_id, ROUTE, Side.SELL, 2, 200, 0, self.oms.clock.now()))
        plan = await self.rebalance(snapshot(sequence=2), (target(),))
        self.assertEqual(plan.strategy('a').occupied, 400)
        self.assertEqual(plan.strategy('a').units, 3)
        self.assertEqual(self.oms.capital_policy.price(ROUTE), 200)

    async def test_recent_execution_floor_also_prices_new_units_after_position_is_flat(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(side=Side.SELL)
        await self.oms.handle(Execution('open', order.order_id, ROUTE, Side.SELL, 1, 200, 0, START))
        close = await self.submit(side=Side.BUY)
        await self.oms.handle(Execution('close', close.order_id, ROUTE, Side.BUY, 1, 100, 0, START))
        plan = await self.rebalance(snapshot(sequence=2), (target(),))
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 0)
        self.assertEqual(plan.strategy('a').units, 5)
        self.assertEqual(self.oms.capital_policy.price(ROUTE), 200)
        self.assertEqual((await self.submit(quantity=5)).status, OrderStatus.SUBMITTED)

    async def test_snapshot_identity_sequence_time_and_revision_cannot_roll_back(self):
        await self.rebalance()
        before = self.oms.capital_plan, self.oms.capital_policy, self.oms.capital_snapshot
        invalid = (snapshot(), snapshot(sequence=2, as_of=START - timedelta(seconds=1)),
                   snapshot(sequence=2, as_of=START + timedelta(seconds=1)),
                   replace(snapshot(sequence=2), source_id='other'))
        for snap in invalid:
            with self.subTest(snapshot=snap), self.assertRaises(ValueError):
                await self.rebalance(snap)
            self.assertEqual((self.oms.capital_plan, self.oms.capital_policy, self.oms.capital_snapshot), before)
        with self.assertRaisesRegex(ValueError, 'revision'):
            await self.rebalance(snapshot(sequence=2), expected=0)

    async def test_expired_or_missing_active_route_does_not_replace_current_policy(self):
        await self.rebalance()
        await self.submit()
        before = self.oms.capital_policy
        with self.assertRaises(ValueError):
            await self.rebalance(replace(snapshot(sequence=2), valuations=()), ())
        self.oms.clock.advance_to(START + timedelta(hours=1))
        with self.assertRaises(ValueError):
            await self.rebalance(snapshot(sequence=2))
        self.assertEqual(self.oms.capital_policy, before)

    async def test_two_competing_rebalances_cannot_both_commit_the_same_revision(self):
        outcomes = await asyncio.gather(
            self.rebalance(snapshot(), expected=0),
            self.rebalance(snapshot(sequence=2), expected=0), return_exceptions=True)
        self.assertEqual(sum(isinstance(o, ValueError) for o in outcomes), 1)
        self.assertEqual(self.oms.capital_policy.revision, 1)

    async def test_basket_uses_same_integer_scale_and_each_account_budget(self):
        second = replace(ROUTE, account_id='second')
        key2 = AccountKey.from_route(second)
        snap = replace(snapshot(600), accounts=(AccountCapital(KEY, 'RUB', 600), AccountCapital(key2, 'RUB', 300)),
                       valuations=(RouteValuation(ROUTE, 100), RouteValuation(second, 100)))
        basket = CapitalTarget('a', 100, (AllocationLeg(ROUTE, 2), AllocationLeg(second, 1)))
        plan = await self.rebalance(snap, (basket,))
        self.assertEqual(plan.strategy('a').units, 3)
        self.assertEqual([leg.quantity for leg in plan.strategy('a').legs], [6, 3])
        self.assertEqual((self.budget('a'), self.budget('a', key2)), (600, 300))

    async def test_newer_mark_can_revalue_older_fills_without_changing_financial_history(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=2, side=Side.SELL)
        await self.oms.handle(Execution('open', order.order_id, ROUTE, Side.SELL, 2, 200, 4, START))
        before = self.oms.ledger.position('a', ROUTE)
        self.oms.clock.advance_to(START + timedelta(minutes=1))
        plan = await self.rebalance(snapshot(sequence=2, price=50, as_of=self.oms.clock.now()), (target(),))
        self.assertEqual(plan.strategy('a').occupied, 100)
        self.assertEqual(plan.strategy('a').units, 18)
        self.assertEqual(self.oms.ledger.position('a', ROUTE), before)

    async def test_account_cash_buffer_and_leverage_are_applied_once(self):
        snap = replace(snapshot(110, price=10), accounts=(AccountCapital(KEY, 'RUB', 110, 2, 10),),
                       external=(ExternalCapital(KEY, 10, 20),))
        plan = await self.rebalance(snap, (target(desired=10),))
        self.assertEqual((plan.account(KEY).capacity, plan.total_additional), (200, 170))
        self.assertEqual((self.budget('a'), plan.strategy('a').units), (170, 17))

    async def test_external_owner_name_cannot_collide_with_a_real_strategy(self):
        await self.rebalance(snapshot(external=(ExternalCapital(KEY, 100, 100),)), (target('external'),))
        self.assertEqual(self.budget('external'), 800)
        await self.submit('external', 2)
        plan = await self.rebalance(snapshot(sequence=2, external=(ExternalCapital(KEY, 100, 100),)), (target('external'),))
        self.assertEqual(plan.strategy('external').occupied, 200)
        self.assertEqual(plan.total_additional, 600)
        self.assertEqual(self.budget('external'), 800)

    async def test_cancelled_capacity_becomes_available_on_the_next_refresh(self):
        await self.rebalance(targets=(target(),))
        order = await self.submit(quantity=8)
        await self.rebalance(snapshot(sequence=2), (target(weight=0), target('b')))
        self.assertEqual(self.budget('b'), 200)
        await self.oms.handle(CancelOrder('a', order.order_id))
        request = self.oms.orders('a')[0].cancel_request_id
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', request, 0))
        await self.rebalance(snapshot(sequence=3), (target(weight=0), target('b')))
        self.assertEqual((self.budget('a'), self.budget('b')), (0, 1000))

    async def test_manual_policy_override_invalidates_plan_but_keeps_source_watermark(self):
        await self.rebalance()
        await self.oms.configure_capital(replace(self.oms.capital_policy, revision=2))
        self.assertIsNone(self.oms.capital_plan)
        self.assertEqual(self.oms.capital_snapshot.sequence, 1)
        with self.assertRaises(ValueError):
            await self.rebalance(snapshot(), expected=2)

    async def test_malformed_bundles_and_duplicate_targets_are_rejected(self):
        changes = ({'sequence': True}, {'sequence': 0}, {'source_id': ''}, {'expires_at': START},
                   {'accounts': [AccountCapital(KEY, 'RUB', 100)]},
                   {'accounts': (AccountCapital(KEY, 'USD', 100),)},
                   {'valuations': (RouteValuation(ROUTE, 100), RouteValuation(ROUTE, 200))},
                   {'external': (ExternalCapital(KEY, 1), ExternalCapital(KEY, 1))},
                   {'external': (ExternalCapital(AccountKey('sim', 'unknown'), 1),)})
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(snapshot(), **change)
        with self.assertRaises(ValueError):
            await self.rebalance(targets=(target(), target()))
        with self.assertRaises(ValueError):
            await self.rebalance(expected=False)
        self.assertIsNone(self.oms.capital_policy)


class GatedSource:
    def __init__(self, value):
        self.value = value
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def load(self):
        self.started.set()
        await self.release.wait()
        return self.value


class RebalanceSourceTests(IsolatedAsyncioTestCase):
    async def test_source_wait_does_not_block_orders_and_plan_uses_their_current_usage(self):
        from trading_system.rebalance import InMemoryCapitalSource
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        from tests_v2.helpers import PassiveStrategy
        async with TradingRuntime(ManualClock(START), require_capital=True) as runtime:
            gateway = SimulationGateway('sim', runtime.clock)
            runtime.add_gateway(gateway)
            context = await runtime.start_strategy(PassiveStrategy('a'))
            await runtime.rebalance_capital(InMemoryCapitalSource(snapshot()), (target(),))
            source = GatedSource(snapshot(sequence=2))
            refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(weight=1), target('b', weight=99))))
            await source.started.wait()
            try:
                await context.place_order(OrderIntent(ROUTE, Side.BUY, 8, 100))
                await asyncio.wait_for(runtime.drain(), 1)
                await gateway.fill(context.orders()[0].order_id, 100, quantity=3)
                await asyncio.wait_for(runtime.drain(), 1)
            finally:
                source.release.set()
            plan = await refresh
            self.assertEqual((plan.strategy('a').occupied, plan.strategy('b').additional), (800, 200))
            self.assertEqual(context.position(ROUTE).quantity, 3)

    async def test_late_source_response_cannot_overwrite_an_operator_change(self):
        from trading_system.rebalance import InMemoryCapitalSource
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        async with TradingRuntime(ManualClock(START), require_capital=True) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            await runtime.rebalance_capital(InMemoryCapitalSource(snapshot()), (target(),))
            source = GatedSource(snapshot(sequence=2))
            refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(),)))
            await source.started.wait()
            new = replace(runtime.orders.capital_policy, revision=2)
            await runtime.configure_capital(new)
            source.release.set()
            with self.assertRaisesRegex(ValueError, 'revision'):
                await refresh
            self.assertEqual(runtime.orders.capital_policy, new)
            self.assertEqual(runtime.orders.capital_snapshot.sequence, 1)

    async def test_expiration_during_load_is_checked_at_commit(self):
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        async with TradingRuntime(ManualClock(START), require_capital=True) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            source = GatedSource(snapshot())
            refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(),)))
            await source.started.wait()
            runtime.clock.advance_to(START + timedelta(hours=1))
            source.release.set()
            with self.assertRaises(ValueError):
                await refresh
            self.assertIsNone(runtime.orders.capital_policy)
            self.assertFalse(runtime.orders.ready)

    async def test_timeout_cancellation_and_source_error_leave_policy_unchanged(self):
        from trading_system.rebalance import InMemoryCapitalSource
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        async with TradingRuntime(ManualClock(START), require_capital=True) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            await runtime.rebalance_capital(InMemoryCapitalSource(snapshot()), (target(),))
            before = runtime.orders.capital_policy, runtime.orders.capital_plan
            with self.assertRaises(TimeoutError):
                await runtime.rebalance_capital(GatedSource(snapshot(sequence=2)), (target(),), timeout=.01)
            source = GatedSource(snapshot(sequence=2))
            refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(),)))
            await source.started.wait()
            refresh.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await refresh
            with patch.object(source, 'load', side_effect=OSError('source unavailable')):
                with self.assertRaises(OSError):
                    await runtime.rebalance_capital(source, (target(),))
            self.assertEqual((runtime.orders.capital_policy, runtime.orders.capital_plan), before)

    async def test_close_started_during_drain_prevents_late_refresh_commit(self):
        from trading_system.domain import CandleReceived
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        from tests_v2.helpers import candle
        runtime = await TradingRuntime(ManualClock(START), require_capital=True).__aenter__()
        runtime.add_gateway(SimulationGateway('sim', runtime.clock))
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow(message):
            entered.set()
            await release.wait()

        runtime.bus.subscribe('slow', slow)
        await runtime.bus.publish('slow', CandleReceived(candle()))
        await entered.wait()
        source = GatedSource(snapshot())
        source.release.set()
        refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(),)))
        await source.started.wait()
        closing = asyncio.create_task(runtime.__aexit__(None, None, None))
        await asyncio.sleep(0)
        release.set()
        await closing
        with self.assertRaises(RuntimeError):
            await refresh
        self.assertIsNone(runtime.orders.capital_policy)

    async def test_source_result_after_runtime_close_is_not_applied(self):
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        async with TradingRuntime(ManualClock(START), require_capital=True) as runtime:
            runtime.add_gateway(SimulationGateway('sim', runtime.clock))
            source = GatedSource(snapshot())
            refresh = asyncio.create_task(runtime.rebalance_capital(source, (target(),)))
            await source.started.wait()
        source.release.set()
        with self.assertRaises(RuntimeError):
            await refresh
        self.assertIsNone(runtime.orders.capital_policy)


class RebalanceRecoveryTests(IsolatedAsyncioTestCase):
    def manager(self, journal):
        oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal, require_capital=True)
        oms.gateway_ids.add('sim')
        return oms

    async def test_append_failure_keeps_plan_policy_source_watermark_and_finance(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            oms = self.manager(journal)
            await oms.rebalance_capital(snapshot(), (target(),), expected_revision=0)
            await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 2, 100)))
            before = oms.capital_plan, oms.capital_policy, oms.capital_snapshot, oms.capital_usage(), journal.records
            with patch.object(journal, 'append', side_effect=JournalError('disk unavailable')):
                with self.assertRaises(JournalError):
                    await oms.rebalance_capital(snapshot(100, sequence=2), (target('b'),), expected_revision=1)
            self.assertEqual((oms.capital_plan, oms.capital_policy, oms.capital_snapshot, oms.capital_usage(), journal.records), before)
            await oms.rebalance_capital(snapshot(100, sequence=2), (target('b'),), expected_revision=1)
            self.assertEqual(oms.capital_snapshot.sequence, 2)

    async def test_replay_restores_plan_source_and_floors_without_loading_source_or_sending_orders(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.rebalance_capital(snapshot(), (target(),), expected_revision=0)
                await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 4, 100)))
                order = oms.orders('a')[0]
                await oms.handle(Execution('fill', order.order_id, ROUTE, Side.BUY, 2, 100, 1, START))
                await oms.rebalance_capital(snapshot(sequence=2, external=(ExternalCapital(KEY, 100, 100),)),
                                            (target(weight=0), target('b')), expected_revision=1)
                before = oms.capital_plan, oms.capital_policy, oms.capital_snapshot, oms.capital_usage()
                observed = (replace(order, status=OrderStatus.PARTIALLY_FILLED,
                                    filled_quantity=2, average_fill_price=100, commission=1),)
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.restore()
                self.assertEqual((oms.capital_plan, oms.capital_policy, oms.capital_snapshot, oms.capital_usage()), before)
                self.assertFalse(oms.ready)
                self.assertEqual(oms.bus.events, [])
                await oms.reconcile_gateway('sim', observed)
                self.assertFalse(oms.ready)
                with self.assertRaises(ValueError):
                    await oms.rebalance_capital(snapshot(sequence=2), (target(),), expected_revision=2)
                await oms.rebalance_capital(snapshot(sequence=3), (target(),), expected_revision=2)
                self.assertTrue(oms.ready)
                self.assertEqual(oms.ledger.position('a', ROUTE).quantity, 2)
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.restore()
                self.assertEqual(oms.capital_snapshot.sequence, 3)
                self.assertEqual(oms.capital_plan.strategy('a').occupied, 400)
                self.assertFalse(oms.ready)

    async def test_rebalance_after_recovery_delivers_pending_child_and_is_itself_replayable(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.rebalance_capital(snapshot(200), (target(),), expected_revision=0)
                await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
                order = oms.orders('a')[0]
                await oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
                request = oms.orders('a')[0].cancel_request_id
                await oms.handle(GatewayCancelled(order.order_id, 'sim', request, 1))
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.restore()
                await oms.handle(Execution('late', order.order_id, ROUTE, Side.BUY, 1, 100, 0, START))
                await oms.reconcile_gateway('sim', (replace(order, status=OrderStatus.FILLED,
                                                           filled_quantity=1, average_fill_price=100),))
                self.assertEqual(len(oms.orders('a')), 1)
                plan = await oms.rebalance_capital(snapshot(200, sequence=2), (target(),), expected_revision=1)
                self.assertEqual(plan.strategy('a').occupied, 200)
                self.assertEqual(len(oms.orders('a')), 2)
                child = oms.orders('a')[1]
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.restore()
                self.assertEqual(oms.orders('a')[1], child)
                self.assertEqual(oms.capital_plan, plan)
                self.assertEqual(oms.bus.events, [])

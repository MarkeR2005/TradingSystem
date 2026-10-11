import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.admission import (
    AccountLimit, CapitalPolicy, RouteGrant, RouteValuation, StrategyLimit,
)
from trading_system.capital import AccountKey
from trading_system.domain import (
    CancelOrder, Execution, GatewayAccepted, GatewayCancelled, GatewayCancelRejected,
    GatewayRejected, GatewaySubmit, OrderIntent, OrderStatus, ReplaceOrder, Side, SubmitOrder,
)
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START

KEY = AccountKey.from_route(ROUTE)


def policy(capacity=200, *, revision=1, unit=100, grants=('a', 'b'), external=0):
    return CapitalPolicy(revision, START, START + timedelta(hours=1), 'RUB',
                         (AccountLimit(KEY, capacity, external),),
                         (RouteValuation(ROUTE, unit),),
                         tuple(RouteGrant(name, ROUTE) for name in grants),
                         tuple(StrategyLimit(name, KEY, capacity) for name in grants))


class Recorder:
    def __init__(self):
        self.events = []

    async def publish(self, recipient, message):
        # Force competing direct calls to overlap if OMS fails to serialize them.
        await asyncio.sleep(0)
        self.events.append((recipient, message))


class AdmissionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bus = Recorder()
        self.clock = ManualClock(START)
        self.ledger = PositionLedger()
        self.oms = OrderManager(self.bus, self.clock, self.ledger, require_capital=True)
        self.oms.gateway_ids.add('sim')
        await self.oms.configure_capital(policy())

    async def submit(self, quantity=1, side=Side.BUY, owner='a', route=ROUTE, price=100):
        await self.oms.handle(SubmitOrder(owner, OrderIntent(route, side, quantity, price)))
        return self.oms.orders(owner)[-1]

    async def fill(self, order, quantity=1, *, identifier='fill', price=100, commission=0):
        event = Execution(identifier, order.order_id, order.intent.route, order.intent.side,
                          quantity, price, commission, self.clock.now())
        await self.oms.handle(event)
        return event

    def usage(self):
        return self.oms.capital_usage()

    async def test_competing_orders_cannot_spend_the_same_account_capacity(self):
        await asyncio.gather(self.submit(2, owner='a'), self.submit(2, owner='b'))
        submitted = [m for _, m in self.bus.events if isinstance(m, GatewaySubmit)]
        self.assertEqual(len(submitted), 1)
        self.assertEqual(sum(u.orders for u in self.usage()), 200)
        self.assertEqual(sum(o.status is OrderStatus.REJECTED
                             for name in ('a', 'b') for o in self.oms.orders(name)), 1)

    async def test_fill_moves_reservation_to_position_and_duplicate_does_not_spend_twice(self):
        order = await self.submit(2)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (0, 200))
        fill = await self.fill(order)
        await self.oms.handle(fill)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 100))
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)

    async def test_cancel_waits_for_final_quantity_before_releasing_reserved_capital(self):
        order = await self.submit(2)
        await self.oms.handle(CancelOrder('a', order.order_id))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 1))
        self.assertEqual(self.usage()[0].orders, 200)
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)
        await self.fill(order)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 0))
        self.assertNotEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)

    async def test_replacement_reserves_increment_before_cancel_and_hands_it_to_child(self):
        order = await self.submit()
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
        self.assertEqual(self.usage()[0].orders, 200)
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 0))
        self.assertEqual(self.usage()[0].orders, 200)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)

    async def test_unaffordable_replace_leaves_old_order_and_cancel_channel_unchanged(self):
        order = await self.submit()
        events = tuple(self.bus.events)
        with self.assertRaisesRegex(ValueError, 'capital'):
            await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=3)))
        self.assertEqual(self.oms.orders('a'), (order,))
        self.assertEqual(tuple(self.bus.events), events)
        self.assertEqual(self.usage()[0].orders, 100)

    async def test_cancel_rejection_releases_only_replacement_increment(self):
        order = await self.submit()
        await self.oms.handle(GatewayAccepted(order.order_id, 'sim'))
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelRejected(order.order_id, 'sim', pending.cancel_request_id, 'busy'))
        self.assertEqual(self.usage()[0].orders, 100)
        self.assertEqual(self.oms.orders('a')[0].status, OrderStatus.ACCEPTED)

    async def test_unknown_owner_account_and_forged_instrument_are_rejected(self):
        for owner, route in [('other', ROUTE), ('a', replace(ROUTE, account_id='other')),
                             ('a', replace(ROUTE, instrument=replace(ROUTE.instrument, contract_multiplier=.01)))]:
            with self.subTest(owner=owner, route=route):
                self.assertEqual((await self.submit(owner=owner, route=route)).status, OrderStatus.REJECTED)
        self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))

    async def test_strategy_budget_and_external_usage_are_both_enforced(self):
        p = replace(policy(external=100, revision=2), budgets=(StrategyLimit('a', KEY, 100), StrategyLimit('b', KEY, 200)))
        await self.oms.configure_capital(p)
        self.assertEqual((await self.submit(2)).status, OrderStatus.REJECTED)
        self.assertNotEqual((await self.submit()).status, OrderStatus.REJECTED)
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)

    async def test_deficit_allows_bounded_exit_but_not_double_exit_or_new_risk(self):
        order = await self.submit()
        await self.fill(order)
        await self.oms.configure_capital(policy(0, revision=2))
        self.assertEqual(self.oms.capital_deficits()[0].amount, 100)
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)
        exit_order = await self.submit(side=Side.SELL)
        self.assertNotEqual(exit_order.status, OrderStatus.REJECTED)
        self.assertEqual((await self.submit(side=Side.SELL)).status, OrderStatus.REJECTED)
        await self.fill(exit_order, identifier='exit')
        self.assertEqual(self.oms.capital_deficits(), ())

    async def test_expired_policy_blocks_risk_but_keeps_fills_and_bounded_exit(self):
        order = await self.submit()
        self.clock.advance_to(START + timedelta(hours=1))
        await self.fill(order)
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)
        self.assertNotEqual((await self.submit(side=Side.SELL)).status, OrderStatus.REJECTED)

    async def test_reprice_above_valuation_is_rejected_before_cancelling(self):
        order = await self.submit()
        with self.assertRaisesRegex(ValueError, 'valuation'):
            await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, limit_price=101)))
        self.assertEqual(self.oms.orders('a'), (order,))

    async def test_broker_reject_releases_reserve(self):
        order = await self.submit(2)
        await self.oms.handle(GatewayRejected(order.order_id, 'sim', 'refused'))
        self.assertEqual(sum(u.orders for u in self.usage()), 0)
        self.assertNotEqual((await self.submit(2, owner='b')).status, OrderStatus.REJECTED)

    async def test_received_fill_above_valuation_is_booked_and_reports_deficit(self):
        order = await self.submit(2, side=Side.SELL)
        await self.fill(order, quantity=2, price=150, commission=3)
        self.assertEqual(self.ledger.position('a', ROUTE).quantity, -2)
        self.assertEqual(self.ledger.position('a', ROUTE).realized_pnl, -3)
        self.assertEqual(self.usage()[0].positions, 300)
        self.assertEqual(self.oms.capital_deficits()[0].amount, 100)
        self.assertEqual((await self.submit(side=Side.SELL)).status, OrderStatus.REJECTED)
        self.assertNotEqual((await self.submit(2, side=Side.BUY)).status, OrderStatus.REJECTED)

    async def test_fully_filled_old_order_keeps_unfinished_replacement_reserved(self):
        order = await self.submit()
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
        pending = self.oms.orders('a')[0]
        await self.fill(order)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 100))
        self.assertEqual((await self.submit(owner='b')).status, OrderStatus.REJECTED)
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 1))
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 1)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 100))

    async def test_cancel_can_discard_replacement_without_releasing_old_order(self):
        order = await self.submit()
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
        await self.oms.handle(CancelOrder('a', order.order_id))
        self.assertEqual(self.usage()[0].orders, 100)
        self.assertEqual(self.oms.orders('a')[0].status, OrderStatus.CANCEL_PENDING)

    async def test_expired_replacement_is_rechecked_before_child_is_sent(self):
        order = await self.submit()
        await self.oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
        pending = self.oms.orders('a')[0]
        self.clock.advance_to(START + timedelta(hours=1))
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 0))
        self.assertEqual(self.oms.orders('a')[-1].status, OrderStatus.REJECTED)
        self.assertEqual(sum(u.orders for u in self.usage()), 0)
        self.assertEqual(sum(isinstance(m, GatewaySubmit) for _, m in self.bus.events), 1)

    async def test_revoking_route_does_not_remove_usage_or_block_existing_fill_cancel(self):
        order = await self.submit(2)
        await self.oms.configure_capital(policy(revision=2, grants=('b',)))
        self.assertEqual(self.usage()[0].orders, 200)
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)
        await self.fill(order)
        await self.oms.handle(CancelOrder('a', order.order_id))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 1))
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 0))
        self.assertTrue(any(d.strategy_id == 'a' and d.amount == 100 for d in self.oms.capital_deficits()))

    async def test_policy_cannot_hide_active_routes_or_roll_back_revision(self):
        await self.submit()
        before = self.oms.capital_policy
        for p in (policy(), replace(policy(revision=2, grants=()), valuations=()),
                  replace(policy(revision=2), as_of=START + timedelta(minutes=1))):
            with self.subTest(policy=p), self.assertRaises(ValueError):
                await self.oms.configure_capital(p)
            self.assertEqual(self.oms.capital_policy, before)
            self.assertEqual(self.usage()[0].orders, 100)

    async def test_explicit_market_intent_is_rejected_in_managed_mode(self):
        self.assertEqual((await self.submit(price=None)).status, OrderStatus.REJECTED)
        self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))

    async def test_different_owners_do_not_net_their_opposite_orders(self):
        await self.submit(2)
        self.assertEqual((await self.submit(2, side=Side.SELL, owner='b')).status, OrderStatus.REJECTED)

    async def test_exposure_covers_every_subset_of_fills_without_double_reserving_exit(self):
        from itertools import product
        await self.oms.configure_capital(policy(400, revision=2))
        opening = await self.submit()
        await self.fill(opening)
        await self.submit(2)
        await self.submit(3, side=Side.SELL)
        expected = max(abs(1 + buys - sells) for buys, sells in product(range(3), range(4))) * 100
        self.assertEqual(expected, 300)
        self.assertEqual(self.usage()[0].positions + self.usage()[0].orders, expected)
        self.assertEqual((await self.submit(5, side=Side.SELL)).status, OrderStatus.REJECTED)

    async def test_fractional_grid_and_contract_multiplier_use_exact_reservations(self):
        route = replace(ROUTE, instrument=replace(ROUTE.instrument, quantity_step=.1, contract_multiplier=2))
        p = replace(policy(60, revision=2), valuations=(RouteValuation(route, 100),),
                    grants=(RouteGrant('a', route),))
        await self.oms.configure_capital(p)
        order = await self.submit(.1 + .2, route=route)
        self.assertNotEqual(order.status, OrderStatus.REJECTED)
        self.assertEqual(self.usage()[0].orders, 60)
        self.assertEqual((await self.submit(.1, route=route)).status, OrderStatus.REJECTED)
        await self.fill(order, quantity=.1)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (20, 40))

    async def test_reused_identifier_cannot_overwrite_an_existing_reservation(self):
        order = await self.submit()
        self.oms._id_factory = lambda: order.order_id
        with self.assertRaisesRegex(ValueError, 'identifier'):
            await self.submit(owner='b')
        self.assertEqual(self.oms.orders('a'), (order,))
        self.assertEqual(self.oms.orders('b'), ())

    async def test_lower_mark_cannot_release_capital_held_by_a_live_limit_price(self):
        order = await self.submit(2)
        await self.oms.configure_capital(policy(revision=2, unit=50))
        self.assertEqual(self.usage()[0].orders, 200)
        self.assertEqual((await self.submit(owner='b', price=50)).status, OrderStatus.REJECTED)
        await self.fill(order)
        self.assertEqual((self.usage()[0].positions, self.usage()[0].orders), (100, 100))

    async def test_separate_accounts_keep_separate_limits_and_route_permissions(self):
        other = replace(ROUTE, account_id='second')
        other_key = AccountKey.from_route(other)
        p = replace(policy(revision=2),
                    accounts=(AccountLimit(KEY, 100), AccountLimit(other_key, 200)),
                    valuations=(RouteValuation(ROUTE, 100), RouteValuation(other, 100)),
                    grants=(RouteGrant('a', ROUTE), RouteGrant('a', other)),
                    budgets=(StrategyLimit('a', KEY, 100), StrategyLimit('a', other_key, 200)))
        await self.oms.configure_capital(p)
        await asyncio.gather(self.submit(), self.submit(2, route=other))
        self.assertEqual({u.account: u.orders for u in self.usage()}, {KEY: 100, other_key: 200})
        self.assertEqual((await self.submit()).status, OrderStatus.REJECTED)
        self.assertEqual((await self.submit(route=other)).status, OrderStatus.REJECTED)

    async def test_invalid_policy_values_are_rejected(self):
        for change in ({'revision': True}, {'revision': 0}, {'expires_at': START},
                       {'accounts': (AccountLimit(KEY, 1), AccountLimit(KEY, 2))},
                       {'grants': [RouteGrant('a', ROUTE)]}, {'budgets': ()},
                       {'report_currency': 'USD'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(policy(), **change)
        for value in (0, -1, float('nan'), float('inf'), True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                RouteValuation(ROUTE, value)


class AdmissionRecoveryTests(IsolatedAsyncioTestCase):
    def manager(self, journal, *, required=True):
        manager = OrderManager(Recorder(), ManualClock(START), PositionLedger(),
                               journal=journal, require_capital=required)
        manager.gateway_ids.add('sim')
        return manager

    async def test_required_mode_blocks_startup_until_policy_is_available(self):
        oms = self.manager(None)
        self.assertFalse(oms.ready)
        with self.assertRaises(RuntimeError):
            await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
        await oms.configure_capital(policy())
        self.assertTrue(oms.ready)

    async def test_append_failure_rolls_back_config_submit_fill_and_replace(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            oms = self.manager(journal)
            await oms.configure_capital(policy())
            await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
            order = oms.orders('a')[0]
            before = oms.capital_usage(), oms.orders('a'), tuple(oms.bus.events), journal.records
            commands = (
                SubmitOrder('b', order.intent),
                ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)),
                Execution('fill', order.order_id, ROUTE, Side.BUY, 1, 100, 0, START),
            )
            for command in commands:
                with self.subTest(command=command), patch.object(journal, 'append', side_effect=JournalError('disk')):
                    with self.assertRaises(JournalError):
                        await oms.handle(command)
                self.assertEqual((oms.capital_usage(), oms.orders('a'), tuple(oms.bus.events), journal.records), before)
                self.assertEqual(oms.ledger.position('a', ROUTE).quantity, 0)
            with patch.object(journal, 'append', side_effect=JournalError('disk')):
                with self.assertRaises(JournalError):
                    await oms.configure_capital(policy(0, revision=2))
            self.assertEqual(oms.capital_policy, policy())

    async def test_restart_restores_usage_but_needs_both_fresh_policy_and_reconciliation(self):
        for refresh_first in (False, True):
            with self.subTest(refresh_first=refresh_first), TemporaryDirectory() as directory:
                with DurableJournal(Path(directory)) as journal:
                    oms = self.manager(journal)
                    await oms.configure_capital(policy())
                    await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 2, 100)))
                    order = oms.orders('a')[0]
                    await oms.handle(GatewayAccepted(order.order_id, 'sim'))
                    fill = Execution('fill', order.order_id, ROUTE, Side.BUY, 1, 100, 2, START)
                    await oms.handle(fill)
                    usage = oms.capital_usage()
                    observed = (replace(order, status=OrderStatus.PARTIALLY_FILLED,
                                        filled_quantity=1, average_fill_price=100, commission=2),)
                with DurableJournal(Path(directory)) as journal:
                    # Managed mode is sticky in the journal even if the default is used.
                    restored = self.manager(journal, required=False)
                    await restored.restore()
                    self.assertEqual(restored.capital_usage(), usage)
                    self.assertEqual(restored.bus.events, [])
                    self.assertFalse(restored.ready)
                    if refresh_first:
                        await restored.configure_capital(policy(revision=2))
                    else:
                        await restored.reconcile_gateway('sim', observed)
                    self.assertFalse(restored.ready)
                    if refresh_first:
                        await restored.reconcile_gateway('sim', observed)
                    else:
                        await restored.configure_capital(policy(revision=2))
                    self.assertTrue(restored.ready)
                    await restored.handle(fill)
                    self.assertEqual(restored.capital_usage(), usage)
                    await restored.handle(SubmitOrder('b', order.intent))
                    self.assertEqual(restored.orders('b')[0].status, OrderStatus.REJECTED)
                with DurableJournal(Path(directory)) as journal:
                    restored = self.manager(journal)
                    await restored.restore()
                    self.assertEqual(restored.capital_usage(), usage)
                    self.assertFalse(restored.ready)

    async def test_delayed_replace_fill_after_restart_waits_for_both_gates_then_replays_again(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.configure_capital(policy())
                await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
                order = oms.orders('a')[0]
                await oms.handle(ReplaceOrder('a', order.order_id, replace(order.intent, quantity=2)))
                pending = oms.orders('a')[0]
                await oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 1))
            with DurableJournal(Path(directory)) as journal:
                restored = self.manager(journal)
                await restored.restore()
                await restored.handle(Execution('late', order.order_id, ROUTE, Side.BUY, 1, 100, 0, START))
                observed = replace(order, status=OrderStatus.FILLED, filled_quantity=1, average_fill_price=100)
                await restored.reconcile_gateway('sim', (observed,))
                self.assertEqual(len(restored.orders('a')), 1)
                self.assertEqual((restored.capital_usage()[0].positions, restored.capital_usage()[0].orders), (100, 100))
                await restored.configure_capital(policy(revision=2))
                self.assertEqual(len(restored.orders('a')), 2)
                self.assertEqual(restored.orders('a')[1].intent.quantity, 1)
                usage, orders = restored.capital_usage(), restored.orders('a')
            with DurableJournal(Path(directory)) as journal:
                restored = self.manager(journal)
                await restored.restore()
                self.assertEqual(restored.capital_usage(), usage)
                self.assertEqual(restored.orders('a'), orders)
                self.assertEqual(restored.bus.events, [])

    async def test_post_commit_delivery_failure_retains_reserve_and_blocks_new_trades(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                oms = self.manager(journal)
                await oms.configure_capital(policy())
                with patch.object(oms.bus, 'publish', side_effect=RuntimeError('delivery')):
                    with self.assertRaises(RuntimeError):
                        await oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 2, 100)))
                self.assertFalse(oms.ready)
                self.assertEqual(oms.capital_usage()[0].orders, 200)
                with self.assertRaises(JournalError):
                    await oms.configure_capital(policy(revision=2))
            with DurableJournal(Path(directory)) as journal:
                restored = self.manager(journal)
                await restored.restore()
                self.assertEqual(restored.capital_usage()[0].orders, 200)
                self.assertFalse(restored.ready)
                self.assertEqual(restored.bus.events, [])

    async def test_legacy_v1_journal_can_gain_capital_policy_without_rewriting_history(self):
        from trading_system import codec
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            journal.append({'version': 1, 'time': START.isoformat(), 'ids': ['old-order'],
                            'gateways': ['sim'], 'unreconciled': [], 'storage_blocked': False,
                            'message': codec.encode(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))})
            restored = self.manager(journal)
            await restored.restore()
            observed = restored.orders('a')
            await restored.configure_capital(policy())
            await restored.reconcile_gateway('sim', observed)
            self.assertTrue(restored.ready)
            self.assertEqual(restored.capital_usage()[0].orders, 100)
            self.assertEqual(journal.records[0]['version'], 1)
            self.assertEqual(journal.records[1]['version'], 2)

    async def test_runtime_checkpoints_and_capital_replay_together(self):
        from trading_system.runtime import TradingRuntime
        from trading_system.simulation import SimulationGateway
        from tests_v2.helpers import PassiveStrategy
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal, require_capital=True) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    await runtime.configure_capital(policy(grants=('passive',)))
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2, 100))
                    await runtime.drain()
                    await gateway.fill(context.orders()[0].order_id, 100, quantity=1)
                    await runtime.drain()
                    observed = gateway.order_snapshots()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    gateway.restore_orders(observed)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    self.assertFalse(runtime.orders.ready)
                    await runtime.configure_capital(policy(revision=2, grants=('passive',)))
                    await runtime.reconcile_gateway('sim', gateway.order_snapshots())
                    self.assertTrue(runtime.orders.ready)
                    self.assertEqual(context.position(ROUTE).quantity, 1)
                    self.assertEqual(runtime.orders.capital_usage()[0].orders, 100)

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.admission import AccountLimit, RouteGrant, RouteValuation, StrategyLimit
from trading_system.capital import AccountKey
from trading_system.deficit import (
    AcknowledgeDeficit, CheckDeficits, DeficitPhase, DeficitQuotes, RetryDeficit, StartDeficit,
)
from trading_system.domain import (
    Execution, GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewaySubmit,
    OrderIntent, OrderStatus, ReplaceOrder, Side, SubmitOrder,
)
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.plans import FailurePolicy, PlanLeg, PlanMarks, PlanPhase, PlanPrice, SubmitPlan
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_admission import KEY, Recorder, policy
from tests_v2.test_plans import SECOND, capital, plan


class DeficitTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = ManualClock(START)
        self.bus = Recorder()
        self.oms = OrderManager(self.bus, self.clock, PositionLedger(), require_capital=True)
        self.oms.gateway_ids.add('sim')
        await self.oms.configure_capital(policy(10000))

    async def fill(self, order, quantity=None, price=100, identifier=None):
        event = Execution(identifier or f'fill-{self.oms.revision}', order.order_id,
                          order.intent.route, order.intent.side,
                          order.remaining_quantity if quantity is None else quantity, price, 0, self.clock.now())
        await self.oms.handle(event)
        return event

    async def position(self, owner='a', quantity=10, route=ROUTE):
        await self.oms.handle(SubmitOrder(owner, OrderIntent(route, Side.BUY, quantity, 100)))
        await self.fill(self.oms.orders(owner)[-1])

    def quotes(self, *routes, ttl=300):
        return DeficitQuotes(self.clock.now(), self.clock.now() + timedelta(seconds=ttl),
                             tuple(PlanPrice(route, 100) for route in (routes or (ROUTE,))))

    async def start(self, wait=10, quotes=None):
        command = StartDeficit('deficit', self.oms.revision, KEY, wait, 10, quotes or self.quotes())
        await self.oms.handle(command)
        return command

    async def tick(self, seconds=10):
        self.clock.advance_to(self.clock.now() + timedelta(seconds=seconds))
        await self.oms.handle(CheckDeficits())

    def state(self):
        return self.oms.deficit('deficit')

    async def ack_cancel(self, order, total=None):
        current = next(o for o in self.oms.orders(order.strategy_id) if o.order_id == order.order_id)
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', current.cancel_request_id,
                                               current.filled_quantity if total is None else total))

    async def test_wait_alert_largest_lots_and_only_missing_resource(self):
        await self.position('a', 10)
        await self.position('b', 3)
        await self.oms.configure_capital(policy(1150, revision=2))
        await self.start()
        self.assertEqual(self.state().phase, DeficitPhase.WAITING)
        self.assertTrue(self.state().alerts)
        self.assertTrue(self.oms.strategy_paused('a'))
        self.assertEqual(len(self.oms.orders('a')), 1)
        await self.tick(9)
        self.assertEqual(len(self.oms.orders('a')), 1)
        await self.tick(1)
        close = self.oms.orders('a')[-1]
        self.assertEqual((close.intent.side, close.intent.quantity), (Side.SELL, 2))
        self.assertEqual(len(self.oms.orders('b')), 1)
        await self.fill(close)
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)
        self.assertTrue(self.oms.strategy_paused('a'))
        await self.oms.handle(AcknowledgeDeficit('ack', self.oms.revision, 'deficit'))
        self.assertFalse(self.oms.strategy_paused('a'))
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 8)

    async def test_replenishment_during_wait_does_not_liquidate(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        await self.start()
        await self.oms.configure_capital(policy(1200, revision=3))
        await self.tick()
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)
        self.assertEqual(len(self.oms.orders('a')), 1)

    async def test_strategy_budget_deficit_is_not_an_account_deficit(self):
        await self.position()
        await self.oms.configure_capital(replace(policy(10000, revision=2, grants=('a',)), budgets=(StrategyLimit('a', KEY, 10),)))
        with self.assertRaises(ValueError):
            await self.start()
        self.assertEqual(len(self.oms.orders('a')), 1)

    async def test_cancel_barrier_counts_late_fills_and_drops_replacement(self):
        await self.position(quantity=4)
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 6, 100)))
        entry = self.oms.orders('a')[-1]
        await self.oms.handle(ReplaceOrder('a', entry.order_id, replace(entry.intent, quantity=8)))
        await self.oms.configure_capital(policy(500, revision=2))
        await self.start(wait=0)
        await self.ack_cancel(entry, 3)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.fill(entry, 3)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)
        self.assertEqual(len(self.oms.orders('a')), 3)

    async def basket(self, quantities=(6, 4), second=SECOND):
        key2 = AccountKey.from_route(second)
        accounts = tuple(AccountLimit(key, 10000) for key in sorted({KEY, key2}))
        cp = replace(capital(10000), accounts=accounts,
                     valuations=(RouteValuation(ROUTE, 100), RouteValuation(second, 100)),
                     grants=(RouteGrant('a', ROUTE), RouteGrant('a', second)),
                     budgets=tuple(StrategyLimit('a', key, 10000) for key in sorted({KEY, key2})))
        await self.oms.configure_capital(replace(cp, revision=2))
        spec = replace(plan(FailurePolicy.COMPLETE), legs=(
            PlanLeg(OrderIntent(ROUTE, Side.BUY, quantities[0], 100), 100),
            PlanLeg(OrderIntent(second, Side.SELL, quantities[1], 100), 100)))
        await self.oms.handle(SubmitPlan(spec))
        for order in self.oms.orders('a'):
            await self.fill(order)
        return cp

    async def test_cross_account_basket_preserves_ratio_and_waits_for_partner_fill(self):
        second = replace(SECOND, account_id='other')
        cp = await self.basket(second=second)
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 400), cp.accounts[1])))
        await self.start(wait=0, quotes=self.quotes(ROUTE, second))
        exits = self.oms.orders('a')[2:]
        self.assertEqual([o.intent.quantity for o in exits], [3, 2])
        await self.fill(exits[0])
        self.assertEqual(self.state().phase, DeficitPhase.REDUCING)
        self.assertEqual(len(self.oms.orders('a')), 4)
        await self.fill(exits[1])
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        self.assertEqual(self.oms.plan('basket').exit_ids, tuple(o.order_id for o in exits))
        await self.oms.handle(PlanMarks('basket', 1, self.clock.now(), self.clock.now() + timedelta(seconds=10),
                                       (PlanPrice(ROUTE, 101), PlanPrice(second, 100))))
        self.assertEqual(self.oms.plan('basket').pnl, 3)

    async def test_failed_exit_is_not_automatically_repeated(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        await self.start(wait=0)
        close = self.oms.orders('a')[-1]
        await self.oms.handle(GatewayRejected(close.order_id, 'sim', 'failed'))
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        await self.tick(20)
        self.assertEqual(len(self.oms.orders('a')), 2)
        command = RetryDeficit('retry', self.oms.revision, 'deficit', self.quotes())
        await self.oms.handle(command)
        await self.oms.handle(command)
        self.assertEqual(len(self.oms.orders('a')), 3)
        await self.fill(self.oms.orders('a')[-1])
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)

    async def test_stale_quotes_and_timeout_block_without_repeated_orders(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        await self.start(quotes=self.quotes(ttl=5))
        await self.tick()
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(len(self.oms.orders('a')), 1)
        await self.oms.handle(RetryDeficit('retry', self.oms.revision, 'deficit', self.quotes()))
        await self.tick(10)
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(self.oms.orders('a')[-1].status, OrderStatus.CANCEL_PENDING)
        await self.tick(10)
        self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_operation_identity_revision_and_concurrency(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        revision = self.oms.revision
        command = await self.start()
        await self.oms.handle(command)
        self.assertEqual(self.oms.revision, revision + 1)
        for bad in (replace(command, wait_seconds=20), replace(command, operation_id='other'),
                    replace(command, operation_id='other', expected_revision=self.oms.revision)):
            with self.assertRaises(ValueError):
                await self.oms.handle(bad)
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))
        await asyncio.gather(self.oms.handle(CheckDeficits()), self.oms.handle(CheckDeficits()))
        self.assertEqual(len(self.oms.orders('a')), 1)

    async def test_recovery_replays_wave_without_resend_and_waits_for_reconciliation(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.oms.configure_capital(policy(10000))
                await self.position()
                await self.oms.configure_capital(policy(800, revision=2))
                await self.start(wait=0)
                original = self.state()
                observed = self.oms.orders('a')
            self.bus.events.clear()
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                await self.oms.restore()
                self.oms.gateway_ids.add('sim')
                self.assertEqual(self.state(), original)
                self.assertEqual(self.bus.events, [])
                self.clock.advance_to(START + timedelta(seconds=11))
                await self.oms.handle(CheckDeficits())
                self.assertEqual(self.state().phase, DeficitPhase.REDUCING)
                await self.oms.reconcile_gateway('sim', observed)
                await self.oms.configure_capital(policy(800, revision=3))
                self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
                self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))
            with DurableJournal(Path(directory)) as journal:
                again = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal, require_capital=True)
                await again.restore()
                self.assertEqual(again.deficit('deficit'), self.state())

    async def test_retry_finishes_missing_basket_leg_even_when_account_is_no_longer_deficient(self):
        second = replace(SECOND, account_id='other')
        cp = await self.basket(second=second)
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 400), cp.accounts[1])))
        await self.start(wait=0, quotes=self.quotes(ROUTE, second))
        first, partner = self.oms.orders('a')[2:]
        await self.fill(first)
        await self.oms.handle(GatewayRejected(partner.order_id, 'sim', 'failed'))
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        with self.assertRaises(ValueError):
            await self.oms.handle(AcknowledgeDeficit('early', self.oms.revision, 'deficit'))
        await self.oms.handle(RetryDeficit('retry', self.oms.revision, 'deficit', self.quotes(ROUTE, second)))
        retry = self.oms.orders('a')[-1]
        self.assertNotEqual(retry.order_id, partner.order_id)
        self.assertEqual((retry.intent.route, retry.intent.quantity), (second, 2))
        await self.fill(retry)
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)
        self.assertEqual((self.oms.ledger.position('a', ROUTE).quantity,
                          self.oms.ledger.position('a', second).quantity), (3, -2))

    async def test_waiting_deficit_prevents_pending_replace_from_creating_a_child(self):
        await self.position(quantity=4)
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 6, 100)))
        entry = self.oms.orders('a')[-1]
        await self.oms.handle(ReplaceOrder('a', entry.order_id, replace(entry.intent, quantity=8)))
        await self.oms.configure_capital(policy(500, revision=2))
        await self.start(wait=10)
        await self.ack_cancel(entry, 0)
        self.assertEqual(len(self.oms.orders('a')), 2)
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)

    async def test_rejected_unfilled_wave_is_not_retried_after_replenishment(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        await self.start(wait=0)
        await self.oms.handle(GatewayRejected(self.oms.orders('a')[-1].order_id, 'sim', 'failed'))
        await self.oms.configure_capital(policy(1100, revision=3))
        await self.oms.handle(RetryDeficit('retry', self.oms.revision, 'deficit', self.quotes()))
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)
        self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_uses_lot_quantity_not_position_value_for_priority(self):
        expensive = replace(SECOND, instrument=replace(SECOND.instrument, contract_multiplier=10))
        cp = replace(policy(10000), valuations=(RouteValuation(ROUTE, 100), RouteValuation(expensive, 100)),
                     grants=(RouteGrant('a', ROUTE), RouteGrant('b', expensive)))
        await self.oms.configure_capital(replace(cp, revision=2))
        await self.position('a', 5)
        await self.position('b', 3, expensive)
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 3350),)))
        await self.start(wait=0, quotes=self.quotes(ROUTE, expensive))
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)
        self.assertEqual(len(self.oms.orders('b')), 1)

    async def test_sequential_groups_recompute_remaining_deficit(self):
        await self.position('a', 4)
        await self.position('b', 3)
        await self.oms.configure_capital(policy(150, revision=2))
        await self.start(wait=0)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 4)
        self.assertEqual(len(self.oms.orders('b')), 1)
        await self.fill(self.oms.orders('a')[-1])
        self.assertEqual(self.oms.orders('b')[-1].intent.quantity, 2)
        await self.fill(self.oms.orders('b')[-1])
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)

    async def test_cancel_rejection_and_late_duplicate_do_not_block_new_request(self):
        await self.position(quantity=4)
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 6, 100)))
        await self.oms.configure_capital(policy(300, revision=2))
        await self.start(wait=0)
        order = self.oms.orders('a')[1]
        rejection = GatewayCancelRejected(order.order_id, 'sim', order.cancel_request_id, 'busy')
        await self.oms.handle(rejection)
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        await self.oms.handle(RetryDeficit('retry', self.oms.revision, 'deficit', self.quotes()))
        await self.oms.handle(rejection)
        self.assertEqual(self.state().phase, DeficitPhase.CANCELLING)
        await self.ack_cancel(order)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 1)

    async def test_deficit_never_completes_an_entry_using_failure_policy(self):
        await self.oms.configure_capital(replace(capital(10000), revision=2))
        await self.oms.handle(SubmitPlan(plan(FailurePolicy.COMPLETE)))
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.configure_capital(capital(50, revision=3))
        await self.start(quotes=self.quotes(ROUTE, SECOND))
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'leg failure'))
        await self.ack_cancel(first)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.tick()
        self.assertEqual(self.oms.orders('a')[-1].intent.side, Side.SELL)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 1)
        await self.fill(self.oms.orders('a')[-1])
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSED)

    async def test_protective_stop_has_priority_over_partial_deficit_exit(self):
        cp = await self.basket()
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 800),)))
        await self.start(wait=0, quotes=self.quotes(ROUTE, SECOND))
        first, second = self.oms.orders('a')[2:]
        await self.fill(first)
        await self.oms.handle(PlanMarks('basket', 1, self.clock.now(), self.clock.now() + timedelta(seconds=10),
                                       (PlanPrice(ROUTE, 80), PlanPrice(SECOND, 100))))
        self.assertTrue(self.oms.plan('basket').close_requested)
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        await self.ack_cancel(second)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSING)
        for order in self.oms.orders('a')[4:]:
            await self.fill(order)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSED)
        await self.oms.handle(AcknowledgeDeficit('ack', self.oms.revision, 'deficit'))
        self.assertTrue(self.state().acknowledged)

    async def test_unavailable_grant_blocks_without_sending_rejected_partial_batch(self):
        cp = await self.basket()
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 800),), grants=()))
        await self.start(wait=0, quotes=self.quotes(ROUTE, SECOND))
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(len(self.oms.orders('a')), 2)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 6)

    async def test_decimal_steps_preserve_exact_ratio(self):
        fractional = replace(SECOND, instrument=replace(SECOND.instrument, quantity_step=0.1))
        cp = await self.basket(quantities=(3, 0.6), second=fractional)
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(AccountLimit(KEY, 310),)))
        await self.start(wait=0, quotes=self.quotes(ROUTE, fractional))
        self.assertEqual([o.intent.quantity for o in self.oms.orders('a')[2:]], [1, 0.2])

    async def test_deficit_beyond_owned_positions_blocks_after_flat(self):
        await self.position(quantity=2)
        await self.oms.configure_capital(policy(100, revision=2, external=300))
        await self.start(wait=0)
        await self.fill(self.oms.orders('a')[-1])
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 0)
        self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_journal_failure_does_not_adopt_case_or_deliver_exits(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
            self.oms.gateway_ids.add('sim')
            await self.oms.configure_capital(policy(10000))
            await self.position()
            await self.oms.configure_capital(policy(800, revision=2))
            before = (self.oms.revision, len(self.bus.events))
            with patch.object(journal, 'append', side_effect=JournalError('write failed')):
                with self.assertRaises(JournalError):
                    await self.start(wait=0)
            self.assertEqual((self.oms.revision, len(self.bus.events)), before)
            self.assertFalse(self.oms.strategy_paused('a'))
            self.assertEqual(len(self.oms.orders('a')), 1)

    async def test_manual_inventory_is_reduced_and_ownership_is_locked(self):
        from trading_system.inventory import CloseManual, TransferPosition
        from tests_v2.test_inventory import report
        await self.oms.handle(report(self.oms.revision, quantity=4))
        await self.oms.configure_capital(policy(200, revision=2, grants=('a', 'b', 'manual')))
        await self.start()
        for command in (TransferPosition('transfer', self.oms.revision, 'manual', 'a', ROUTE, 1),
                        CloseManual('close', self.oms.revision, ROUTE, 1, 100)):
            with self.assertRaises(ValueError):
                await self.oms.handle(command)
        await self.tick()
        exit_order = self.oms.orders('manual')[-1]
        self.assertEqual(exit_order.intent.quantity, 2)
        await self.fill(exit_order)
        self.assertEqual(self.oms.ledger.position('manual', ROUTE).quantity, 2)
        self.assertEqual(self.state().phase, DeficitPhase.RESOLVED)

    async def test_new_owner_on_affected_route_cannot_bypass_risk_pause(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        await self.start()
        await self.oms.configure_capital(policy(1100, revision=3))
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitOrder('b', OrderIntent(ROUTE, Side.BUY, 1, 100)))
        await self.oms.handle(AcknowledgeDeficit('ack', self.oms.revision, 'deficit'))
        await self.oms.handle(SubmitOrder('b', OrderIntent(ROUTE, Side.BUY, 1, 100)))
        self.assertNotEqual(self.oms.orders('b')[-1].status, OrderStatus.REJECTED)

    async def test_validation_rejects_bad_times_prices_and_stale_operator_revision(self):
        await self.position()
        await self.oms.configure_capital(policy(800, revision=2))
        request = StartDeficit('deficit', self.oms.revision, KEY, 10, 10, self.quotes())
        for changes in ({'wait_seconds': -1}, {'wait_seconds': True}, {'order_timeout': 0},
                        {'order_timeout': float('inf')}, {'order_timeout': 1e308}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(request, **changes)
        for changed in (replace(request, expected_revision=0),
                        replace(request, quotes=replace(request.quotes, prices=())),
                        replace(request, quotes=replace(request.quotes, as_of=START + timedelta(seconds=1)))):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                await self.oms.handle(changed)
        self.assertFalse(self.oms.strategy_paused('a'))

    async def test_snapshot_expiry_after_partial_fill_cannot_erase_fill(self):
        await self.position()
        await self.oms.configure_capital(replace(policy(800, revision=2), expires_at=START + timedelta(seconds=1)))
        await self.start(wait=0)
        close = self.oms.orders('a')[-1]
        self.clock.advance_to(START + timedelta(seconds=2))
        await self.fill(close)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 8)
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(self.oms.orders('a')[-1].status, OrderStatus.FILLED)

    async def test_post_commit_delivery_failure_replays_complete_risk_wave_without_resend(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.oms.configure_capital(policy(10000))
                await self.position()
                await self.oms.configure_capital(policy(800, revision=2))
                original_publish = self.bus.publish
                async def broken_publish(recipient, message):
                    if isinstance(message, GatewaySubmit):
                        raise RuntimeError('transport failed after commit')
                    await original_publish(recipient, message)
                with patch.object(self.bus, 'publish', broken_publish):
                    with self.assertRaises(RuntimeError):
                        await self.start(wait=0)
                state = self.state()
                self.assertEqual(state.phase, DeficitPhase.REDUCING)
                with self.assertRaises(JournalError):
                    await self.oms.handle(CheckDeficits())
            with DurableJournal(Path(directory)) as journal:
                bus = Recorder()
                restored = OrderManager(bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                await restored.restore()
                self.assertEqual(restored.deficit('deficit'), state)
                self.assertEqual(len(restored.orders('a')), 2)
                self.assertEqual(bus.events, [])

    async def test_unrepresentable_small_exit_from_huge_position_is_blocked_before_submit(self):
        await self.oms.configure_capital(policy(1e24, revision=2))
        await self.position(quantity=1e20)
        await self.oms.configure_capital(policy(1e22, revision=3, external=1))
        await self.start(wait=0)
        self.assertEqual(self.state().phase, DeficitPhase.BLOCKED)
        self.assertEqual(len(self.oms.orders('a')), 1)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 1e20)

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.admission import RouteGrant, RouteValuation
from trading_system.domain import Execution, GatewayCancelled, GatewayRejected, GatewaySubmit, OrderIntent, OrderStatus, Side, SubmitOrder
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.plans import (
    AcknowledgePlan, CheckPlanDeadlines, ClosePlan, FailurePolicy, PlanLeg,
    PlanMarks, PlanPhase, PlanPrice, RetryPlan, SubmitPlan, TradePlan,
)
from trading_system.time import ManualClock
from tests_v2.helpers import INSTRUMENT, ROUTE, START
from tests_v2.test_admission import Recorder, policy

SECOND = replace(ROUTE, instrument=replace(INSTRUMENT, id=replace(INSTRUMENT.id, symbol='SECOND')))


def capital(capacity=1000, revision=1):
    return replace(policy(capacity, revision=revision),
                   valuations=(RouteValuation(ROUTE, 120), RouteValuation(SECOND, 120)),
                   grants=tuple(RouteGrant(owner, route) for owner in ('a', 'b') for route in (ROUTE, SECOND)))


def plan(policy=FailurePolicy.PROTECT, identifier='basket', owner='a'):
    return TradePlan(identifier, owner,
                     (PlanLeg(OrderIntent(ROUTE, Side.BUY, 2, 100), 90),
                      PlanLeg(OrderIntent(SECOND, Side.SELL, 2, 100), 110)),
                     policy, START + timedelta(seconds=10), stop_loss=20, recovery_timeout=10)


class PlanTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = ManualClock(START)
        self.bus = Recorder()
        self.oms = OrderManager(self.bus, self.clock, PositionLedger(), require_capital=True)
        self.oms.gateway_ids.add('sim')
        await self.oms.configure_capital(capital())

    async def submit(self, policy=FailurePolicy.PROTECT):
        await self.oms.handle(SubmitPlan(plan(policy)))
        return self.oms.plan('basket')

    async def fill(self, order, quantity, price=100, commission=0, identifier=None):
        event = Execution(identifier or f'fill-{self.oms.revision}', order.order_id,
                          order.intent.route, order.intent.side, quantity, price, commission, self.clock.now())
        await self.oms.handle(event)
        return event

    async def cancel_ack(self, order, total=None):
        current = next(o for o in self.oms.orders(order.strategy_id) if o.order_id == order.order_id)
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', current.cancel_request_id,
                                              current.filled_quantity if total is None else total))

    async def marks(self, prices=(100, 100), sequence=1):
        await self.oms.handle(PlanMarks('basket', sequence, self.clock.now(),
                                        self.clock.now() + timedelta(seconds=2),
                                        (PlanPrice(ROUTE, prices[0]), PlanPrice(SECOND, prices[1]))))

    async def test_admission_is_all_or_nothing_and_reserves_both_legs(self):
        await self.oms.configure_capital(capital(300, 2))
        before = self.oms.revision
        with self.assertRaises(ValueError):
            await self.submit()
        self.assertEqual(self.oms.orders('a'), ())
        self.assertEqual(self.oms.revision, before)
        self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))
        await self.oms.configure_capital(capital(600, 3))
        await self.submit()
        self.assertEqual(sum(u.orders for u in self.oms.capital_usage()), 480)
        await self.oms.handle(SubmitOrder('b', OrderIntent(ROUTE, Side.BUY, 2, 100)))
        self.assertEqual(self.oms.orders('b')[-1].status, OrderStatus.REJECTED)

    async def test_normal_completion_tracks_real_fills_and_explicit_close(self):
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 1, commission=1)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.ENTERING)
        await self.fill(second, 2)
        await self.fill(first, 1)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.OPEN)
        await self.oms.handle(ClosePlan('a', 'basket'))
        closes = self.oms.orders('a')[2:]
        self.assertEqual([(o.intent.side, o.intent.quantity) for o in closes], [(Side.SELL, 2), (Side.BUY, 2)])
        await self.fill(closes[0], 2, price=110)
        await self.fill(closes[1], 2, price=95)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSED)
        self.assertFalse(self.oms.plan('basket').paused)
        self.assertEqual(self.oms.ledger.trades('a')[0].pnl, 29)

    async def test_protect_cancels_remainder_and_uses_only_filled_exposure(self):
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 1, commission=1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'leg failed'))
        self.assertTrue(self.oms.plan('basket').paused)
        self.assertTrue(self.oms.plan('basket').alerts)
        await self.cancel_ack(first)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        await self.marks((90, 120))
        self.assertEqual(self.oms.plan('basket').pnl, -11)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.marks((80, 120), sequence=2)
        close = self.oms.orders('a')[-1]
        self.assertEqual((close.intent.route, close.intent.side, close.intent.quantity), (ROUTE, Side.SELL, 1))
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSING)

    async def test_close_policy_waits_for_cancellation_barrier_and_late_fill(self):
        await self.submit(FailurePolicy.CLOSE)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        await self.cancel_ack(first, 2)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.fill(first, 1)
        close = self.oms.orders('a')[-1]
        self.assertEqual(close.intent.quantity, 2)
        await self.fill(close, 2, 90)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSED)
        self.assertTrue(self.oms.plan('basket').paused)
        await self.oms.handle(AcknowledgePlan('ack', self.oms.revision, 'basket'))
        self.assertFalse(self.oms.plan('basket').paused)

    async def test_complete_policy_builds_only_missing_lots_and_attempts_once(self):
        await self.submit(FailurePolicy.COMPLETE)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        await self.cancel_ack(first)
        recovery = self.oms.orders('a')[2:]
        self.assertEqual([o.intent.quantity for o in recovery], [1, 2])
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.COMPLETING)
        await self.oms.handle(GatewayRejected(recovery[1].order_id, 'sim', 'still failed'))
        await self.cancel_ack(recovery[0])
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        await self.oms.handle(CheckPlanDeadlines())
        self.assertEqual(len(self.oms.orders('a')), 4)

    async def test_completion_failure_to_reserve_keeps_fill_and_pause(self):
        await self.submit(FailurePolicy.COMPLETE)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        await self.oms.configure_capital(capital(120, 2))
        await self.cancel_ack(first)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 1)
        self.assertTrue(self.oms.plan('basket').paused)
        self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_stop_uses_combined_pnl_fees_and_partial_fills(self):
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 2, commission=1)
        await self.fill(second, 2, commission=1)
        await self.marks((80, 80))
        self.assertEqual(self.oms.plan('basket').pnl, -2)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.OPEN)
        await self.marks((80, 90), 2)
        self.assertEqual(self.oms.plan('basket').pnl, -22)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSING)

    async def test_timeout_and_duplicate_plan_do_not_submit_twice(self):
        await self.submit()
        count = self.oms.revision
        await self.oms.handle(SubmitPlan(plan()))
        self.assertEqual(self.oms.revision, count)
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitPlan(replace(plan(), stop_loss=30)))
        self.clock.advance_to(START + timedelta(seconds=10))
        await self.oms.handle(CheckPlanDeadlines())
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CANCELLING)
        self.assertTrue(self.oms.plan('basket').paused)
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 1, 100)))

    async def test_close_rejection_needs_explicit_retry_and_does_not_loop(self):
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 2)
        await self.fill(second, 2)
        await self.oms.handle(ClosePlan('a', 'basket'))
        closes = self.oms.orders('a')[2:]
        await self.fill(closes[1], 2)
        await self.oms.handle(GatewayRejected(closes[0].order_id, 'sim', 'exit refused'))
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.BLOCKED)
        await self.oms.handle(ClosePlan('a', 'basket'))
        self.assertEqual(len(self.oms.orders('a')), 4)
        retry = RetryPlan('retry', self.oms.revision, 'basket', (PlanPrice(ROUTE, 95), PlanPrice(SECOND, 105)))
        await self.oms.handle(retry)
        await self.oms.handle(retry)
        self.assertEqual(len(self.oms.orders('a')), 5)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)

    async def test_stale_incomplete_and_future_marks_cannot_trigger_protection(self):
        await self.submit()
        await self.fill(self.oms.orders('a')[0], 1)
        await self.marks()
        mark = PlanMarks('basket', 2, START, START + timedelta(seconds=1),
                         (PlanPrice(ROUTE, 70), PlanPrice(SECOND, 100)))
        for bad in (replace(mark, sequence=1), replace(mark, prices=mark.prices[:1]),
                    replace(mark, as_of=START + timedelta(seconds=1)), replace(mark, expires_at=START)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                await self.oms.handle(bad)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.ENTERING)

    async def test_append_failure_and_restart_preserve_atomic_plan_and_fault(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                await self.oms.configure_capital(capital(1000, 2))
                with patch.object(journal, 'append', side_effect=JournalError('disk')):
                    with self.assertRaises(JournalError):
                        await self.submit(FailurePolicy.CLOSE)
                self.assertEqual(self.oms.orders('a'), ())
                await self.submit(FailurePolicy.CLOSE)
                first, second = self.oms.orders('a')
                await self.fill(first, 1)
                await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
                await self.cancel_ack(first, 2)
                state = self.oms.plan('basket')
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                self.assertEqual(restored.plan('basket'), state)
                self.assertEqual(restored.bus.events, [])
                restored.gateway_ids.add('sim')
                late = Execution('late', first.order_id, ROUTE, Side.BUY, 1, 100, 0, START)
                await restored.handle(late)
                self.assertEqual(len(restored.orders('a')), 2)
                observed = tuple(replace(o, status=OrderStatus.FILLED) if o.order_id == first.order_id else o
                                 for o in restored.orders('a'))
                await restored.reconcile_gateway('sim', observed)
                await restored.configure_capital(capital(1000, 3))
                self.assertEqual(len(restored.orders('a')), 3)
                self.assertEqual(restored.orders('a')[-1].intent.quantity, 2)

    async def test_closed_plan_marks_cannot_value_or_close_a_new_plan_position(self):
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 2)
        await self.fill(second, 2)
        await self.oms.handle(ClosePlan('a', 'basket'))
        for order in self.oms.orders('a')[2:]:
            await self.fill(order, 2)
        await self.oms.handle(SubmitPlan(plan(identifier='next')))
        await self.fill(self.oms.orders('a')[-2], 1)
        await self.marks((120, 100))
        self.assertEqual(self.oms.plan('basket').pnl, 0)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CLOSED)
        self.assertEqual(len(self.oms.orders('a')), 6)

    async def test_stop_does_not_automatically_repeat_timed_out_exit(self):
        await self.submit()
        for order in self.oms.orders('a'):
            await self.fill(order, 2)
        await self.marks((80, 100))
        closes = self.oms.orders('a')[2:]
        self.clock.advance_to(START + timedelta(seconds=11))
        await self.oms.handle(CheckPlanDeadlines())
        for order in closes:
            await self.cancel_ack(order)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.HELD)
        await self.marks((70, 100), 2)
        self.assertEqual(len(self.oms.orders('a')), 4)
        self.assertTrue(self.oms.plan('basket').paused)

    async def test_bad_cached_mark_cannot_roll_back_a_real_fill(self):
        await self.submit()
        await self.marks((1e308, 100))
        first = self.oms.orders('a')[0]
        await self.fill(first, 2)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 2)
        self.assertTrue(self.oms.plan('basket').paused)
        self.assertIsNone(self.oms.plan('basket').pnl)

    async def test_duplicate_old_cancel_rejection_does_not_block_new_cancellation(self):
        from trading_system.domain import GatewayCancelRejected
        await self.submit(FailurePolicy.CLOSE)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        pending = self.oms.orders('a')[0]
        response = GatewayCancelRejected(first.order_id, 'sim', pending.cancel_request_id, 'busy')
        await self.oms.handle(response)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.BLOCKED)
        await self.oms.handle(RetryPlan('retry', self.oms.revision, 'basket',
                                        (PlanPrice(ROUTE, 90), PlanPrice(SECOND, 110))))
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CANCELLING)
        await self.oms.handle(response)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.CANCELLING)

    async def test_successful_completion_remains_paused_until_acknowledged(self):
        await self.submit(FailurePolicy.COMPLETE)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        with self.assertRaises(ValueError):
            await self.oms.handle(AcknowledgePlan('early', self.oms.revision, 'basket'))
        await self.cancel_ack(first)
        for order in self.oms.orders('a')[2:]:
            await self.fill(order, order.intent.quantity)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.OPEN)
        self.assertTrue(self.oms.plan('basket').paused)
        ack = AcknowledgePlan('ack', self.oms.revision, 'basket')
        await self.oms.handle(ack)
        await self.oms.handle(ack)
        self.assertFalse(self.oms.plan('basket').paused)
        self.assertEqual([p.quantity for _, p in self.oms.ledger.positions()], [2, -2])

    async def test_competing_plans_cannot_partially_spend_same_capacity(self):
        await self.oms.configure_capital(capital(600, 2))
        results = await asyncio.gather(*(self.oms.handle(SubmitPlan(plan(identifier=n, owner=n)))
                                         for n in ('a', 'b')), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, ValueError) for r in results), 1)
        self.assertEqual(sorted(len(self.oms.orders(owner)) for owner in ('a', 'b')), [0, 2])
        self.assertEqual(sum(u.orders for u in self.oms.capital_usage()), 480)

    async def test_active_plan_prevents_individual_replace_and_ownership_transfer(self):
        from trading_system.domain import ReplaceOrder
        from trading_system.inventory import MANUAL, TransferPosition
        await self.submit()
        first, second = self.oms.orders('a')
        await self.fill(first, 2)
        await self.fill(second, 2)
        with self.assertRaises(ValueError):
            await self.oms.handle(TransferPosition('t', self.oms.revision, 'a', MANUAL, ROUTE, 1))
        with self.assertRaises(ValueError):
            await self.oms.handle(ReplaceOrder('a', first.order_id, first.intent))
        with self.assertRaises(ValueError):
            await self.oms.handle(ClosePlan('b', 'basket'))
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.OPEN)

    async def test_cancellation_timeout_retains_reservations_and_late_fills(self):
        await self.submit(FailurePolicy.CLOSE)
        first, second = self.oms.orders('a')
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
        self.clock.advance_to(START + timedelta(seconds=11))
        await self.oms.handle(CheckPlanDeadlines())
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.BLOCKED)
        self.assertEqual(sum(u.orders for u in self.oms.capital_usage()), 240)
        await self.fill(first, 1)
        await self.cancel_ack(first)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 1)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.BLOCKED)
        await self.oms.handle(RetryPlan('retry', self.oms.revision, 'basket',
                                        (PlanPrice(ROUTE, 90), PlanPrice(SECOND, 110))))
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 1)

    async def test_expired_marks_are_not_reused_for_later_executions(self):
        await self.submit()
        await self.marks((80, 100))
        self.clock.advance_to(START + timedelta(seconds=3))
        await self.fill(self.oms.orders('a')[0], 2)
        self.assertIsNone(self.oms.plan('basket').pnl)
        self.assertEqual(self.oms.plan('basket').phase, PlanPhase.ENTERING)
        await self.marks((80, 100), 2)
        self.assertTrue(self.oms.plan('basket').paused)

    async def test_partial_gateway_delivery_failure_replays_no_submissions(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                await self.oms.configure_capital(capital(1000, 2))
                original = self.bus.publish
                deliveries = 0

                async def fail_after_one_submit(recipient, message):
                    nonlocal deliveries
                    if isinstance(message, GatewaySubmit):
                        deliveries += 1
                        if deliveries == 2:
                            raise RuntimeError('second gateway delivery failed')
                    await original(recipient, message)

                with patch.object(self.bus, 'publish', side_effect=fail_after_one_submit):
                    with self.assertRaises(RuntimeError):
                        await self.submit()
                self.assertFalse(self.oms.ready)
                expected = self.oms.plan('basket')
                self.assertEqual(len(self.oms.orders('a')), 2)
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                restored.gateway_ids.add('sim')
                self.assertEqual(restored.plan('basket'), expected)
                self.assertEqual(restored.bus.events, [])
                with self.assertRaises(ValueError):
                    await restored.reconcile_gateway('sim', self.oms.orders('a')[:1])
                self.assertFalse(restored.ready)

    async def test_closed_recovered_plan_and_ack_survive_second_restart(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms.journal = journal
                await self.oms.configure_capital(capital(1000, 2))
                await self.submit(FailurePolicy.CLOSE)
                first, second = self.oms.orders('a')
                await self.fill(first, 2)
                await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'failed'))
                observed = self.oms.orders('a')
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                restored.gateway_ids.add('sim')
                await restored.reconcile_gateway('sim', observed)
                await restored.configure_capital(capital(1000, 3))
                close = restored.orders('a')[-1]
                await restored.handle(Execution('exit', close.order_id, ROUTE, Side.SELL, 2, 90, 1, START))
                await restored.handle(AcknowledgePlan('ack', restored.revision, 'basket'))
                expected = restored.plan('basket')
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                self.assertEqual(restored.plan('basket'), expected)
                self.assertEqual(expected.phase, PlanPhase.CLOSED)
                self.assertFalse(expected.paused)
                self.assertEqual(restored.ledger.trades('a')[0].pnl, -21)
                self.assertEqual(restored.bus.events, [])

    async def test_two_gateways_reserve_separate_accounts_atomically(self):
        from trading_system.admission import AccountLimit, StrategyLimit
        from trading_system.capital import AccountKey
        route = replace(SECOND, instrument=replace(SECOND.instrument, id=replace(SECOND.instrument.id, gateway_id='other')),
                        account_id='other-account')
        key = AccountKey.from_route(route)
        self.oms.gateway_ids.add('other')
        p = capital(1000, 2)
        p = replace(p, accounts=p.accounts + (AccountLimit(key, 120),),
                    valuations=p.valuations + (RouteValuation(route, 120),),
                    grants=p.grants + (RouteGrant('a', route),),
                    budgets=p.budgets + (StrategyLimit('a', key, 240),))
        await self.oms.configure_capital(p)
        spec = plan()
        spec = replace(spec, legs=(spec.legs[0], replace(spec.legs[1], intent=replace(spec.legs[1].intent, route=route))))
        with self.assertRaises(ValueError):
            await self.oms.handle(SubmitPlan(spec))
        self.assertEqual(self.oms.orders('a'), ())
        await self.oms.configure_capital(replace(p, revision=3, accounts=p.accounts[:-1] + (AccountLimit(key, 240),)))
        await self.oms.handle(SubmitPlan(spec))
        self.assertEqual(sorted(u.orders for u in self.oms.capital_usage()), [240, 240])

    async def test_invalid_plan_inputs_fail_before_any_order(self):
        spec = plan()
        for changes in ({'legs': spec.legs[:1]}, {'legs': (spec.legs[0], spec.legs[0])},
                        {'failure_policy': 'close'}, {'stop_loss': float('nan')},
                        {'stop_loss': 0}, {'recovery_timeout': 1e308}, {'stop_loss': True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await self.oms.handle(SubmitPlan(replace(spec, **changes)))
        self.assertEqual(self.oms.orders('a'), ())

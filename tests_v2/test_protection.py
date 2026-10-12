import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from fractions import Fraction
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import Execution, GatewayCancelled, GatewayRejected, GatewaySubmit, OrderStatus
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.plans import (
    CheckPlanDeadlines, ClosePlan, PlanMarks, PlanPhase, PlanPrice, PlanProtection,
    ProtectionKind, RetryPlan, SubmitPlan, TrailingStop, UpdatePlanProtection,
)
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_admission import Recorder
from tests_v2.test_plans import SECOND, capital, plan


class ProtectionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = ManualClock(START)
        self.bus = Recorder()
        self.oms = OrderManager(self.bus, self.clock, PositionLedger(), require_capital=True)
        self.oms.gateway_ids.add('sim')
        await self.oms.configure_capital(capital())

    async def submit(self, *, filled=True, **settings):
        await self.oms.handle(SubmitPlan(replace(plan(), **settings)))
        if filled:
            for order in self.oms.orders('a'):
                await self.fill(order)
        return self.state()

    def state(self):
        return self.oms.plan('basket')

    async def fill(self, order, quantity=None, price=100, commission=0):
        event = Execution(f'exec-{self.oms.revision}', order.order_id, order.intent.route,
                          order.intent.side, order.remaining_quantity if quantity is None else quantity,
                          price, commission, self.clock.now())
        await self.oms.handle(event)
        return event

    async def marks(self, long=100, short=100, *, ttl=5):
        sequence = self.state().marks.sequence + 1 if self.state().marks else 1
        await self.oms.handle(PlanMarks('basket', sequence, self.clock.now(),
                                       self.clock.now() + timedelta(seconds=ttl),
                                       (PlanPrice(ROUTE, long), PlanPrice(SECOND, short))))

    async def update(self, protection, *, operation='update', **kwargs):
        command = UpdatePlanProtection(operation, self.oms.revision, 'a', 'basket', protection, **kwargs)
        await self.oms.handle(command)
        return command

    async def cancel_ack(self, order, total=None):
        current = next(o for o in self.oms.orders('a') if o.order_id == order.order_id)
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', current.cancel_request_id,
                                               current.filled_quantity if total is None else total))

    async def test_take_profit_uses_combined_net_pnl_and_inclusive_threshold(self):
        await self.submit(filled=False, take_profit=20)
        for order in self.oms.orders('a'):
            await self.fill(order, commission=1)
        await self.marks(120, 110)
        self.assertEqual(self.state().pnl, 18)
        self.assertEqual(self.state().phase, PlanPhase.OPEN)
        await self.marks(121, 110)
        self.assertEqual(self.state().phase, PlanPhase.CLOSING)
        trigger = self.state().protection_trigger
        self.assertEqual((trigger.kind, trigger.pnl, trigger.threshold), (ProtectionKind.TAKE_PROFIT, 20, 20))
        self.assertTrue(self.state().paused)
        self.assertEqual([o.intent.quantity for o in self.oms.orders('a')[2:]], [2, 2])

    async def test_trailing_activation_peak_and_drawdown(self):
        await self.submit(stop_loss=100, trailing=TrailingStop(10, activation=20))
        await self.marks(109)
        await self.marks(100)
        self.assertIsNone(self.state().protection_trigger)
        self.assertEqual(self.state().peak_pnl, 18)
        await self.marks(110)
        await self.marks(125)
        await self.marks(121)
        self.assertEqual(self.state().peak_pnl, 50)
        self.assertEqual(self.state().phase, PlanPhase.OPEN)
        await self.marks(120)
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.TRAILING)
        self.assertEqual(self.state().protection_trigger.threshold, 40)
        self.assertEqual(self.state().phase, PlanPhase.CLOSING)

    async def test_no_fill_cannot_create_peak_or_trigger(self):
        await self.submit(filled=False, take_profit=1, trailing=TrailingStop(1))
        await self.marks(1000, 1)
        self.assertIsNone(self.state().peak_pnl)
        self.assertIsNone(self.state().protection_trigger)
        self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_partial_entry_take_profit_waits_for_cancel_and_late_fill(self):
        await self.submit(filled=False, take_profit=10)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.marks(110)
        self.assertEqual(self.state().phase, PlanPhase.CANCELLING)
        await self.cancel_ack(first, 2)
        await self.cancel_ack(second, 0)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.fill(first, 1)
        self.assertEqual(len(self.oms.orders('a')), 3)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)
        self.assertEqual(self.state().protection_trigger.pnl, 10)

    async def test_update_is_immediate_atomic_and_preserves_original_plan(self):
        await self.submit()
        original = self.state().plan
        await self.marks(95)
        command = await self.update(PlanProtection(5, 30, None), exit_prices=(PlanPrice(ROUTE, 95), PlanPrice(SECOND, 105)))
        self.assertEqual(self.state().plan, original)
        self.assertEqual(self.state().protection, command.protection)
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.STOP_LOSS)
        self.assertEqual([o.intent.limit_price for o in self.oms.orders('a')[2:]], [95, 105])
        before = self.oms.revision
        await self.oms.handle(command)
        self.assertEqual(self.oms.revision, before)
        with self.assertRaises(ValueError):
            await self.update(PlanProtection(1000), operation='undo')
        self.assertEqual(len(self.oms.orders('a')), 4)

    async def test_updates_keep_peak_and_can_enable_trailing_against_existing_history(self):
        await self.submit(stop_loss=100)
        await self.marks(120)
        await self.marks(115)
        self.assertEqual(self.state().peak_pnl, 40)
        await self.update(PlanProtection(100, trailing=TrailingStop(10, 20)))
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.TRAILING)
        self.assertEqual(self.state().peak_pnl, 40)

    async def test_update_needs_fresh_marks_after_fills_and_checks_owner_revision(self):
        await self.submit(filled=False)
        await self.update(PlanProtection(30))
        await self.fill(self.oms.orders('a')[0], 1)
        command = UpdatePlanProtection('fresh', self.oms.revision, 'a', 'basket', PlanProtection(10))
        with self.assertRaises(ValueError):
            await self.oms.handle(command)
        await self.marks()
        for changes in ({'strategy_id': 'b'}, {'expected_revision': 0}, {'exit_prices': (PlanPrice(ROUTE, 90),)},
                        {'exit_prices': (PlanPrice(ROUTE, 1000), PlanPrice(SECOND, 100))}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                current = replace(command, expected_revision=self.oms.revision)
                await self.oms.handle(replace(current, **changes))
        self.clock.advance_to(START + timedelta(seconds=6))
        with self.assertRaises(ValueError):
            await self.update(PlanProtection(10), operation='expired')
        self.assertEqual(self.state().protection.stop_loss, 30)

    async def test_parameter_validation_and_operation_cas(self):
        for factory in (lambda: TrailingStop(0), lambda: TrailingStop(True), lambda: TrailingStop(1, -1),
                        lambda: TrailingStop(1, float('nan')), lambda: PlanProtection(0),
                        lambda: PlanProtection(1, take_profit=False), lambda: PlanProtection(1, trailing={}),
                        lambda: replace(plan(), take_profit=-1)):
            with self.assertRaises(ValueError):
                factory()
        await self.submit(filled=False)
        commands = [UpdatePlanProtection(name, self.oms.revision, 'a', 'basket', PlanProtection(limit))
                    for name, limit in (('one', 10), ('two', 30))]
        results = await asyncio.gather(*(self.oms.handle(c) for c in commands), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, ValueError) for r in results), 1)
        before = self.oms.revision
        await self.oms.handle(commands[0])
        self.assertEqual(self.oms.revision, before)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(commands[0], protection=PlanProtection(50)))

    async def test_legacy_plan_codec_keeps_stop_only_and_remains_closed_to_unknown_fields(self):
        old = codec.encode(plan())
        old['fields'].pop('take_profit')
        old['fields'].pop('trailing')
        decoded = codec.decode(old)
        self.assertEqual(decoded, plan())
        for field in ('stop_loss', 'recovery_timeout'):
            bad = deepcopy(old)
            bad['fields'].pop(field)
            with self.assertRaises(JournalError):
                codec.decode(bad)
        bad = deepcopy(old)
        bad['fields']['trailing_typo'] = 1
        with self.assertRaises(JournalError):
            codec.decode(bad)
        new = replace(plan(), take_profit=50, trailing=TrailingStop(5, 10))
        self.assertEqual(codec.decode(codec.encode(new)), new)

    async def test_peak_and_parameters_restore_before_new_marks_without_resend(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.oms.configure_capital(capital(revision=2))
                await self.submit(stop_loss=100, trailing=TrailingStop(20, 10))
                await self.marks(120)
                await self.update(PlanProtection(100, trailing=TrailingStop(15, 10)))
                state = self.state()
                observed = self.oms.orders('a')
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal, require_capital=True)
                await restored.restore()
                self.assertEqual(restored.plan('basket'), state)
                self.assertEqual(restored.bus.events, [])
                restored.gateway_ids.add('sim')
                await restored.reconcile_gateway('sim', observed)
                await restored.configure_capital(capital(revision=3))
                self.oms = restored
                await self.marks(112.5)
                self.assertEqual(self.state().protection_trigger.threshold, 25)
                self.assertEqual(self.state().phase, PlanPhase.CLOSING)

    async def test_stale_marks_and_newer_fill_do_not_move_peak(self):
        await self.submit(filled=False, trailing=TrailingStop(10))
        await self.marks(120, ttl=2)
        self.clock.advance_to(START + timedelta(seconds=1))
        await self.fill(self.oms.orders('a')[0], 1)
        self.assertIsNone(self.state().peak_pnl)
        self.assertIsNone(self.state().pnl)
        with self.assertRaises(ValueError):
            await self.update(PlanProtection(20), operation='late-fill')
        await self.marks(120, ttl=1)
        self.assertEqual(self.state().peak_pnl, 20)
        self.clock.advance_to(START + timedelta(seconds=3))
        await self.fill(self.oms.orders('a')[0], 1)
        self.assertEqual(self.state().peak_pnl, 20)
        self.assertIsNone(self.state().pnl)
        prior = self.state()
        with self.assertRaises(ValueError):
            await self.oms.handle(PlanMarks('basket', 1, START, START + timedelta(seconds=1),
                                           (PlanPrice(ROUTE, 1000), PlanPrice(SECOND, 1))))
        self.assertEqual(self.state(), prior)

    async def test_trigger_is_latched_through_timeout_new_marks_and_explicit_retry(self):
        await self.submit(trailing=TrailingStop(10))
        await self.marks(120)
        await self.marks(115)
        trigger = self.state().protection_trigger
        exits = self.oms.orders('a')[2:]
        self.clock.advance_to(START + timedelta(seconds=11))
        await self.oms.handle(CheckPlanDeadlines())
        for order in exits:
            await self.cancel_ack(order)
        self.assertEqual(self.state().phase, PlanPhase.HELD)
        await self.marks(140)
        await self.marks(100)
        self.assertEqual(self.state().protection_trigger, trigger)
        self.assertEqual(self.state().peak_pnl, 40)
        self.assertEqual(len(self.oms.orders('a')), 4)
        with self.assertRaises(ValueError):
            await self.update(PlanProtection(1000), operation='disable')
        await self.oms.handle(RetryPlan('retry', self.oms.revision, 'basket',
                                       (PlanPrice(ROUTE, 100), PlanPrice(SECOND, 100))))
        self.assertEqual(len(self.oms.orders('a')), 6)
        self.assertEqual(self.state().protection_trigger, trigger)

    async def test_deficit_partial_exit_keeps_realized_pnl_and_historical_peak(self):
        from trading_system.deficit import DeficitQuotes, StartDeficit
        from tests_v2.test_admission import KEY
        await self.submit(stop_loss=100, trailing=TrailingStop(15))
        await self.marks(120)
        await self.oms.configure_capital(capital(300, revision=2))
        await self.oms.handle(StartDeficit('deficit', self.oms.revision, KEY, 0, 10,
            DeficitQuotes(START, START + timedelta(minutes=1), (PlanPrice(ROUTE, 100), PlanPrice(SECOND, 100)))))
        first, second = self.oms.orders('a')[2:]
        self.assertEqual([o.intent.quantity for o in (first, second)], [1, 1])
        await self.fill(first, price=110)
        await self.fill(second)
        self.assertEqual(self.state().pnl, 30)
        self.assertEqual(self.state().peak_pnl, 40)
        self.assertEqual(self.state().phase, PlanPhase.HELD)
        await self.marks(115)
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.TRAILING)
        self.assertEqual(self.state().protection_trigger.pnl, 25)
        self.assertEqual([o.intent.quantity for o in self.oms.orders('a')[4:]], [1, 1])

    async def test_take_profit_supersedes_an_incomplete_deficit_wave(self):
        from trading_system.deficit import DeficitPhase, DeficitQuotes, StartDeficit
        from tests_v2.test_admission import KEY
        await self.submit(take_profit=30)
        await self.marks(110)
        await self.oms.configure_capital(capital(300, revision=2))
        await self.oms.handle(StartDeficit('deficit', self.oms.revision, KEY, 0, 10,
            DeficitQuotes(START, START + timedelta(minutes=1), (PlanPrice(ROUTE, 100), PlanPrice(SECOND, 100)))))
        first, second = self.oms.orders('a')[2:]
        await self.fill(first, price=120)
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.TAKE_PROFIT)
        self.assertEqual(self.oms.deficit('deficit').phase, DeficitPhase.BLOCKED)
        await self.cancel_ack(second)
        self.assertEqual([o.intent.quantity for o in self.oms.orders('a')[4:]], [1, 2])
        for order in self.oms.orders('a')[4:]:
            await self.fill(order)
        self.assertEqual(self.state().phase, PlanPhase.CLOSED)

    async def test_exact_decimal_threshold_and_stop_loss_priority(self):
        await self.submit(trailing=TrailingStop(0.1))
        await self.marks(100.1)
        await self.marks(100.05)
        self.assertEqual(self.state().protection_trigger.threshold, Fraction(1, 10))
        self.assertEqual(self.state().protection_trigger.pnl, Fraction(1, 10))
        from trading_system.protection import evaluate_protection
        peak, breach = evaluate_protection(PlanProtection(10, trailing=TrailingStop(20)), Fraction(-20), Fraction(30))
        self.assertEqual((peak, breach), (30, (ProtectionKind.STOP_LOSS, -10)))

    async def test_unreportable_peak_remains_exact_and_does_not_erase_fills(self):
        await self.submit(trailing=TrailingStop(1e308))
        await self.marks(1e308)
        self.assertIsNone(self.state().pnl)
        self.assertEqual(self.state().peak_pnl, 2 * Fraction('1e308') - 200)
        self.assertTrue(self.state().paused)
        await self.marks(4e307)
        self.assertEqual(self.state().protection_trigger.threshold, Fraction('1e308') - 200)
        self.assertEqual(self.state().phase, PlanPhase.CLOSING)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).quantity, 2)

    async def test_disable_and_reenable_trailing_preserves_peak_without_undoing_a_trigger(self):
        await self.submit(stop_loss=100, trailing=TrailingStop(100))
        await self.marks(120)
        await self.update(PlanProtection(100))
        await self.marks(110)
        self.assertEqual(self.state().peak_pnl, 40)
        await self.update(PlanProtection(100, trailing=TrailingStop(10, 50)), operation='reenable')
        self.assertIsNone(self.state().protection_trigger)
        await self.marks(125)
        await self.marks(120)
        self.assertEqual(self.state().protection_trigger.threshold, 40)

    async def test_closed_plan_does_not_change_peak_or_apply_protection_to_next_cycle(self):
        await self.submit(trailing=TrailingStop(50))
        await self.marks(120)
        await self.oms.handle(ClosePlan('a', 'basket'))
        for order in self.oms.orders('a')[2:]:
            await self.fill(order)
        await self.oms.handle(SubmitPlan(replace(plan(identifier='next'), trailing=TrailingStop(10))))
        for order in self.oms.orders('a')[4:]:
            await self.fill(order)
        await self.marks(200)
        self.assertEqual(self.state().peak_pnl, 40)
        self.assertIsNone(self.state().protection_trigger)
        self.assertIsNone(self.oms.plan('next').peak_pnl)
        self.assertEqual(len(self.oms.orders('a')), 6)

    async def test_failed_update_journal_write_preserves_settings_peak_and_orders(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
            self.oms.gateway_ids.add('sim')
            await self.oms.configure_capital(capital())
            await self.submit()
            await self.marks(95)
            previous = self.state(), self.oms.revision, len(self.bus.events)
            with patch.object(journal, 'append', side_effect=JournalError('write failed')):
                with self.assertRaises(JournalError):
                    await self.update(PlanProtection(5))
            self.assertEqual((self.state(), self.oms.revision, len(self.bus.events)), previous)
            self.assertEqual(len(self.oms.orders('a')), 2)

    async def test_legacy_journal_can_be_extended_with_new_protection_and_replayed_twice(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.oms.configure_capital(capital())
                encode = codec.encode
                def legacy_encode(value):
                    result = encode(value)
                    if isinstance(result, dict) and result.get('type') == 'TradePlan':
                        result['fields'].pop('take_profit')
                        result['fields'].pop('trailing')
                    return result
                with patch.object(codec, 'encode', side_effect=legacy_encode):
                    await self.submit(stop_loss=100)
                await self.marks(120)
                await self.update(PlanProtection(100, trailing=TrailingStop(10)))
                expected = self.state()
            for _ in range(2):
                with DurableJournal(Path(directory)) as journal:
                    restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                    await restored.restore()
                    self.assertEqual(restored.plan('basket'), expected)
                    self.assertEqual(restored.bus.events, [])
                    self.assertEqual(restored.plan('basket').protection.trailing.distance, 10)

    async def test_breach_while_cancellation_is_blocked_requires_explicit_retry(self):
        from trading_system.domain import GatewayCancelRejected
        await self.submit(filled=False, take_profit=10)
        first, second = self.oms.orders('a')
        await self.fill(first, 1)
        await self.oms.handle(GatewayRejected(second.order_id, 'sim', 'entry failed'))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelRejected(first.order_id, 'sim', pending.cancel_request_id, 'busy'))
        await self.marks(110)
        self.assertEqual(self.state().phase, PlanPhase.BLOCKED)
        self.assertEqual(self.state().protection_trigger.kind, ProtectionKind.TAKE_PROFIT)
        self.assertEqual(len(self.oms.orders('a')), 2)
        await self.oms.handle(RetryPlan('retry', self.oms.revision, 'basket',
                                       (PlanPrice(ROUTE, 100), PlanPrice(SECOND, 100))))
        await self.cancel_ack(first)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 1)

    async def test_post_commit_delivery_failure_keeps_trigger_and_complete_exit_wave_on_replay(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.oms.configure_capital(capital())
                await self.submit(take_profit=10)
                publish = self.bus.publish
                sent = 0
                async def fail_second_exit(recipient, message):
                    nonlocal sent
                    if isinstance(message, GatewaySubmit):
                        sent += 1
                        if sent == 2:
                            raise RuntimeError('transport failed after first exit')
                    await publish(recipient, message)
                with patch.object(self.bus, 'publish', side_effect=fail_second_exit):
                    with self.assertRaises(RuntimeError):
                        await self.marks(105)
                expected = self.state()
                self.assertEqual(expected.protection_trigger.kind, ProtectionKind.TAKE_PROFIT)
                self.assertEqual(len(expected.exit_ids), 2)
                with self.assertRaises(JournalError):
                    await self.marks(110)
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                self.assertEqual(restored.plan('basket'), expected)
                self.assertEqual(restored.bus.events, [])

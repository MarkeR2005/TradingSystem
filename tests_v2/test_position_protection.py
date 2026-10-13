import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.admission import RouteGrant, RouteValuation
from trading_system.deficit import DeficitPhase, DeficitQuotes, StartDeficit
from trading_system.domain import (
    CancelOrder, Execution, GatewayCancelled, GatewaySubmit, OrderIntent, Side, SubmitOrder,
)
from trading_system.inventory import MANUAL, BrokerPosition, CloseManual, OpeningBalance, TransferPosition
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.plans import (
    ClosePlan, PlanMarks, PlanPhase, PlanPrice, PlanProtection, ProtectPositions,
    ProtectionKind, ReleasePositionProtection, SubmitPlan, TrailingStop,
)
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START
from tests_v2.test_admission import KEY, Recorder, policy
from tests_v2.test_inventory import report
from tests_v2.test_plans import SECOND, plan


class PositionProtectionTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = ManualClock(START)
        self.bus = Recorder()
        self.oms = OrderManager(self.bus, self.clock, PositionLedger(), require_capital=True)
        self.oms.gateway_ids.add('sim')
        await self.refresh()

    async def refresh(self):
        revision = self.oms.capital_policy.revision + 1 if self.oms.capital_policy else 1
        await self.oms.configure_capital(policy(2000, revision=revision, unit=150, grants=('a', 'b', MANUAL)))

    async def fill(self, order, quantity=None, price=100, commission=0):
        event = Execution(f'fill-{self.oms.revision}', order.order_id, order.intent.route,
                          order.intent.side, order.remaining_quantity if quantity is None else quantity,
                          price, commission, self.clock.now())
        await self.oms.handle(event)
        return event

    async def ordinary(self, quantity=4, commission=2, side=Side.BUY):
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, side, quantity, 100)))
        return await self.fill(self.oms.orders('a')[-1], commission=commission)

    def marks(self, price=100, sequence=1, identifier='guard'):
        return PlanMarks(identifier, sequence, self.clock.now(), self.clock.now() + timedelta(seconds=30),
                         (PlanPrice(ROUTE, price),))

    def command(self, owner='a', price=100, identifier='guard', **settings):
        return ProtectPositions('attach-' + identifier, self.oms.revision, owner, self.marks(price, identifier=identifier),
                                PlanProtection(settings.pop('stop_loss', 100), **settings),
                                (PlanPrice(ROUTE, 100),), order_timeout=5)

    def release(self, owner='a', identifier='guard'):
        return ReleasePositionProtection('release-' + identifier, self.oms.revision, owner, identifier)

    async def test_existing_position_attaches_without_orders_or_financial_mutation(self):
        await self.ordinary()
        before = self.oms.ledger.positions(), self.oms.orders('a'), self.oms.ledger.equity('a')
        self.bus.events.clear()
        await self.oms.handle(self.command(price=105))
        self.assertEqual((self.oms.ledger.positions(), self.oms.orders('a'), self.oms.ledger.equity('a')), before)
        self.assertFalse(any(isinstance(m, GatewaySubmit) for _, m in self.bus.events))
        state = self.oms.plan('guard')
        self.assertEqual((state.phase, state.pnl, state.peak_pnl), (PlanPhase.OPEN, 18, 18))
        self.assertEqual(state.entry_ids, ())

    async def test_attachment_includes_current_cycle_realization_and_fees_only(self):
        await self.ordinary(quantity=1)
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.SELL, 1, 100)))
        await self.fill(self.oms.orders('a')[-1], price=120, commission=1)
        await self.ordinary()
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.SELL, 1, 100)))
        await self.fill(self.oms.orders('a')[-1], price=110, commission=1)
        await self.oms.handle(self.command(price=105, take_profit=30))
        self.assertEqual(self.oms.plan('guard').pnl, 22)  # 7 realized + 15 unrealized; old trade excluded.
        await self.oms.handle(self.marks(108, 2))
        state = self.oms.plan('guard')
        self.assertEqual(state.protection_trigger.pnl, 31)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 3)

    async def test_manual_short_stop_closes_only_owned_quantity(self):
        await self.oms.handle(report(self.oms.revision, quantity=-4))
        await self.refresh()
        await self.oms.handle(self.command(MANUAL, price=106, stop_loss=20))
        state = self.oms.plan('guard')
        self.assertEqual(state.protection_trigger.kind, ProtectionKind.STOP_LOSS)
        self.assertTrue(state.paused)
        close = self.oms.orders(MANUAL)[0]
        self.assertEqual((close.intent.side, close.intent.quantity), (Side.BUY, 4))
        await self.fill(close, price=99, commission=1)
        self.assertEqual(self.oms.plan('guard').phase, PlanPhase.CLOSED)
        self.assertEqual(self.oms.ledger.settlements(MANUAL)[0].pnl, 3)

    async def test_attach_requires_settled_orders_including_late_fill_barrier(self):
        await self.oms.handle(SubmitOrder('a', OrderIntent(ROUTE, Side.BUY, 4, 100)))
        order = self.oms.orders('a')[0]
        await self.fill(order, 1)
        with self.assertRaises(ValueError):
            await self.oms.handle(self.command())
        await self.oms.handle(CancelOrder('a', order.order_id))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 2))
        with self.assertRaises(ValueError):
            await self.oms.handle(self.command())
        await self.fill(order, 1)
        await self.oms.handle(self.command())
        self.assertEqual(self.oms.plan('guard').opening_positions[0].quantity, 2)

    async def test_attach_rejects_flat_stale_quotes_wrong_routes_and_revision_atomically(self):
        with self.assertRaises(ValueError):
            await self.oms.handle(self.command())
        await self.ordinary()
        command = self.command()
        before = self.oms.revision, self.oms.orders('a'), self.oms.ledger.positions(), tuple(self.bus.events)
        for changed in (
            replace(command, expected_revision=0), replace(command, exit_prices=()),
            replace(command, exit_prices=(PlanPrice(ROUTE, 151),)),
            replace(command, marks=replace(command.marks, expires_at=START)),
            replace(command, marks=replace(command.marks, as_of=START - timedelta(seconds=1))),
            replace(command, strategy_id='b'),
        ):
            with self.subTest(command=changed), self.assertRaises(ValueError):
                await self.oms.handle(changed)
            self.assertEqual((self.oms.revision, self.oms.orders('a'), self.oms.ledger.positions(), tuple(self.bus.events)), before)

    async def test_attach_and_release_are_idempotent_but_conflicts_and_reused_plan_id_fail(self):
        await self.ordinary()
        command = self.command()
        await self.oms.handle(command)
        revision = self.oms.revision
        await self.oms.handle(command)
        self.assertEqual(self.oms.revision, revision)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, protection=PlanProtection(10)))
        release = self.release()
        await self.oms.handle(release)
        revision = self.oms.revision
        await self.oms.handle(release)
        self.assertEqual(self.oms.revision, revision)
        with self.assertRaises(ValueError):
            await self.oms.handle(replace(command, operation_id='new', expected_revision=self.oms.revision))

    async def test_release_allows_partial_transfer_without_changing_financial_history(self):
        await self.oms.handle(report(self.oms.revision))
        await self.refresh()
        await self.oms.handle(self.command(MANUAL, trailing=TrailingStop(10)))
        with self.assertRaises(ValueError):
            await self.oms.handle(TransferPosition('blocked', self.oms.revision, MANUAL, 'a', ROUTE, 1))
        await self.oms.handle(self.release(MANUAL))
        released = self.oms.plan('guard')
        self.assertEqual(released.phase, PlanPhase.RELEASED)
        await self.oms.handle(TransferPosition('transfer', self.oms.revision, MANUAL, 'a', ROUTE, 1))
        await self.refresh()
        self.assertEqual(self.oms.ledger.position(MANUAL, ROUTE).quantity, 3)
        self.assertEqual(self.oms.ledger.position('a', ROUTE).average_price, 100)
        with self.assertRaises(ValueError):
            await self.oms.handle(self.marks(50, 2))
        self.assertEqual(self.oms.plan('guard'), released)
        await self.oms.handle(self.command(MANUAL, identifier='remaining'))
        self.assertEqual(self.oms.plan('remaining').opening_positions[0].quantity, 3)

    async def test_release_rejects_wrong_owner_stale_marks_and_triggered_exit(self):
        await self.ordinary()
        await self.oms.handle(self.command(stop_loss=10))
        with self.assertRaises(ValueError):
            await self.oms.handle(self.release('b'))
        self.clock.advance_to(START + timedelta(seconds=31))
        with self.assertRaises(ValueError):
            await self.oms.handle(self.release())
        await self.oms.handle(self.marks(90, 2))
        with self.assertRaises(ValueError):
            await self.oms.handle(self.release())
        self.assertEqual(self.oms.plan('guard').phase, PlanPhase.CLOSING)

    async def test_manual_close_and_import_cannot_bypass_active_protection(self):
        await self.oms.handle(report(self.oms.revision))
        await self.refresh()
        await self.oms.handle(self.command(MANUAL))
        with self.assertRaises(ValueError):
            await self.oms.handle(CloseManual('close', self.oms.revision, ROUTE, 1, 100))
        with self.assertRaises(ValueError):
            await self.oms.handle(report(self.oms.revision, quantity=5, operation='extra'))
        await self.oms.handle(ClosePlan(MANUAL, 'guard'))
        self.assertEqual(self.oms.orders(MANUAL)[0].intent.quantity, 4)

    async def test_new_single_leg_plan_protects_partial_entry_and_late_fill(self):
        spec = replace(plan(), legs=plan().legs[:1], stop_loss=5)
        await self.oms.handle(SubmitPlan(spec))
        order = self.oms.orders('a')[0]
        await self.fill(order, 1)
        await self.oms.handle(self.marks(90, identifier='basket'))
        pending = self.oms.orders('a')[0]
        await self.oms.handle(GatewayCancelled(order.order_id, 'sim', pending.cancel_request_id, 2))
        self.assertEqual(len(self.oms.orders('a')), 1)
        await self.fill(order, 1)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 2)
        with self.assertRaises(ValueError):
            await self.oms.handle(self.release(identifier='basket'))

    async def test_deficit_partial_close_preserves_baseline_and_protects_remaining_position(self):
        await self.ordinary()
        await self.oms.handle(self.command(price=110, trailing=TrailingStop(10)))
        await self.oms.configure_capital(policy(450, revision=2, unit=150))
        await self.oms.handle(StartDeficit('deficit', self.oms.revision, KEY, 0, 5,
            DeficitQuotes(START, START + timedelta(minutes=1), (PlanPrice(ROUTE, 100),))))
        close = self.oms.orders('a')[-1]
        self.assertEqual(close.intent.quantity, 1)
        await self.fill(close, price=110, commission=1)
        self.assertEqual(self.oms.deficit('deficit').phase, DeficitPhase.RESOLVED)
        state = self.oms.plan('guard')
        self.assertEqual((state.peak_pnl, state.pnl), (38, 37))
        with self.assertRaises(ValueError):
            await self.oms.handle(self.release())
        await self.oms.handle(self.marks(107, 2))
        self.assertEqual(self.oms.plan('guard').protection_trigger.pnl, 28)
        self.assertEqual(self.oms.orders('a')[-1].intent.quantity, 3)

    async def test_manual_group_does_not_turn_independent_positions_into_deficit_basket(self):
        initial = replace(report(self.oms.revision),
            openings=(OpeningBalance(ROUTE, 4, 100), OpeningBalance(SECOND, 2, 100)),
            observed_positions=(BrokerPosition(ROUTE, 4), BrokerPosition(SECOND, 2)))
        await self.oms.handle(initial)
        cp = replace(policy(2000, revision=2, grants=(MANUAL,)),
                     valuations=(RouteValuation(ROUTE, 100), RouteValuation(SECOND, 100)),
                     grants=(RouteGrant(MANUAL, ROUTE), RouteGrant(MANUAL, SECOND)))
        await self.oms.configure_capital(cp)
        command = self.command(MANUAL)
        quotes = (PlanPrice(ROUTE, 100), PlanPrice(SECOND, 100))
        await self.oms.handle(replace(command, marks=replace(command.marks, prices=quotes), exit_prices=quotes))
        await self.oms.configure_capital(replace(cp, revision=3, accounts=(replace(cp.accounts[0], capacity=500),)))
        await self.oms.handle(StartDeficit('deficit', self.oms.revision, KEY, 0, 5,
            DeficitQuotes(START, START + timedelta(minutes=1), quotes)))
        exits = self.oms.orders(MANUAL)
        self.assertEqual([(o.intent.route, o.intent.quantity) for o in exits], [(ROUTE, 1)])
        await self.fill(exits[0])
        self.assertEqual(self.oms.ledger.position(MANUAL, SECOND).quantity, 2)
        self.assertEqual(self.oms.deficit('deficit').phase, DeficitPhase.RESOLVED)

    async def test_concurrent_attachment_cas_allows_one_group_only(self):
        await self.ordinary()
        results = await asyncio.gather(self.oms.handle(self.command()),
                                       self.oms.handle(self.command(identifier='other')), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, ValueError) for result in results), 1)
        self.assertEqual(len(self.oms._plans.states), 1)
        self.assertEqual(len(self.oms.orders('a')), 1)

    async def test_codec_validation_is_closed_and_typed(self):
        for command in (self.command(), self.release()):
            encoded = codec.encode(command)
            self.assertEqual(codec.decode(encoded), command)
            missing = deepcopy(encoded)
            missing['fields'].pop('expected_revision')
            extra = deepcopy(encoded)
            extra['fields']['run'] = 'anything'
            for bad in (missing, extra):
                with self.assertRaises(JournalError):
                    codec.decode(bad)
        for changes in ({'order_timeout': True}, {'order_timeout': 0}, {'order_timeout': float('nan')},
                        {'marks': {}}, {'protection': {}}, {'exit_prices': []}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.command(), **changes)

    async def test_restart_preserves_baseline_peak_and_waits_for_reconciliation_before_exit(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.refresh()
                await self.ordinary()
                command = self.command(price=110, trailing=TrailingStop(10))
                await self.oms.handle(command)
                expected = self.oms.plan('guard')
                observed = self.oms.orders('a')
            with DurableJournal(Path(directory)) as journal:
                self.bus = Recorder()
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                await self.oms.restore()
                self.assertEqual(self.oms.plan('guard'), expected)
                self.assertEqual(self.bus.events, [])
                await self.oms.handle(command)  # Exact retry remains harmless before readiness.
                await self.oms.handle(self.marks(107.5, 2))
                self.assertEqual(self.oms.plan('guard').phase, PlanPhase.CANCELLING)
                self.assertEqual(len(self.oms.orders('a')), 1)
                self.oms.gateway_ids.add('sim')
                await self.oms.reconcile_gateway('sim', observed)
                await self.refresh()
                self.assertEqual(len(self.oms.orders('a')), 2)
                await self.fill(self.oms.orders('a')[-1], price=107.5, commission=1)
                expected = self.oms.plan('guard')
                self.assertEqual((expected.phase, expected.pnl), (PlanPhase.CLOSED, 27))
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                self.assertEqual(restored.plan('guard'), expected)
                self.assertEqual(restored.ledger.trades('a')[0].pnl, 27)
                self.assertEqual(restored.bus.events, [])

    async def test_failed_journal_append_cannot_attach_release_or_emit_exit(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
            self.oms.gateway_ids.add('sim')
            await self.refresh()
            await self.ordinary()
            before = self.oms.revision, self.oms.ledger.positions(), self.oms.orders('a'), tuple(self.bus.events)
            with patch.object(journal, 'append', side_effect=JournalError('disk failure')):
                with self.assertRaises(JournalError):
                    await self.oms.handle(self.command(price=90, stop_loss=10))
            self.assertEqual((self.oms.revision, self.oms.ledger.positions(), self.oms.orders('a'), tuple(self.bus.events)), before)
            self.assertFalse(self.oms.has_active_plan('a'))
            await self.oms.handle(self.command())
            state = self.oms.plan('guard')
            with patch.object(journal, 'append', side_effect=JournalError('disk failure')):
                with self.assertRaises(JournalError):
                    await self.oms.handle(self.release())
            self.assertEqual(self.oms.plan('guard'), state)

    async def test_released_plan_replay_stays_inactive_after_transfer_and_new_owner_exit(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                self.oms = OrderManager(self.bus, self.clock, PositionLedger(), journal=journal, require_capital=True)
                self.oms.gateway_ids.add('sim')
                await self.refresh()
                await self.ordinary()
                await self.oms.handle(self.command())
                await self.oms.handle(self.release())
                expected = self.oms.plan('guard')
                await self.oms.handle(TransferPosition('move', self.oms.revision, 'a', MANUAL, ROUTE, 4))
                await self.refresh()
                await self.oms.handle(CloseManual('exit', self.oms.revision, ROUTE, 4, 100))
                await self.fill(self.oms.orders(MANUAL)[0], price=110)
            with DurableJournal(Path(directory)) as journal:
                restored = OrderManager(Recorder(), self.clock, PositionLedger(), journal=journal)
                await restored.restore()
                self.assertEqual(restored.plan('guard'), expected)
                self.assertFalse(restored.has_active_plan('a'))
                self.assertEqual(restored.ledger.settlements('a')[0].pnl, -2)
                self.assertEqual(restored.ledger.settlements(MANUAL)[0].pnl, 40)

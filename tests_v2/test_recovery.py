import json
from contextlib import closing
from dataclasses import replace
import sqlite3
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from trading_system.domain import (
    Execution, GatewayCancelled, OrderIntent, OrderStatus, Side,
)
from trading_system.events import DeliveryError
from trading_system.persistence import DurableJournal, JournalError
from trading_system.runtime import TradingRuntime
from trading_system.simulation import SimulationGateway
from trading_system.time import ManualClock

from tests_v2.helpers import PassiveStrategy, ROUTE, START


class JournalTests(TestCase):
    def test_restart_preserves_order_and_sqlite_projection(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                journal.append({'name': 'первый'})
                journal.append({'name': 'second'})
            with DurableJournal(Path(directory)) as restored:
                self.assertEqual(restored.records, ({'name': 'первый'}, {'name': 'second'}))
                self.assertFalse(restored.degraded)
            with closing(sqlite3.connect(Path(directory) / 'events.sqlite3')) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 2)

    def test_database_failure_keeps_durable_events_and_rebuilds_projection(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                with patch.object(journal, '_project', side_effect=sqlite3.OperationalError('unavailable')):
                    journal.append({'fill': 1})
                    journal.append({'fill': 2})
                self.assertTrue(journal.degraded)
            with DurableJournal(Path(directory)) as restored:
                self.assertEqual(len(restored.records), 2)
                self.assertFalse(restored.degraded)

    def test_failed_fsync_stops_further_writes_until_restart(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                with patch('trading_system.persistence.os.fsync', side_effect=OSError('disk error')):
                    with self.assertRaises(JournalError):
                        journal.append({'order': 1})
                with self.assertRaises(JournalError):
                    journal.append({'order': 2})

    def test_partial_tail_is_reported_and_not_silently_discarded(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                journal.append({'fill': 1})
            path = Path(directory) / 'events.jsonl'
            with path.open('ab') as stream:
                stream.write(b'{"partial":')
            with self.assertRaises(JournalError):
                DurableJournal(Path(directory))
            self.assertTrue(path.read_bytes().endswith(b'{"partial":'))

    def test_content_change_and_event_reordering_are_detected(self):
        for reorder in (False, True):
            with self.subTest(reorder=reorder), TemporaryDirectory() as directory:
                with DurableJournal(Path(directory)) as journal:
                    journal.append({'fill': 1})
                    journal.append({'fill': 2})
                path = Path(directory) / 'events.jsonl'
                lines = path.read_text().splitlines()
                if reorder:
                    lines.reverse()
                else:
                    item = json.loads(lines[0])
                    item['payload']['fill'] = 99
                    lines[0] = json.dumps(item)
                path.write_text('\n'.join(lines) + '\n')
                with self.assertRaises(JournalError):
                    DurableJournal(Path(directory))

    def test_only_one_writer_can_open_a_directory(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)):
                with self.assertRaises(JournalError):
                    DurableJournal(Path(directory))
            with DurableJournal(Path(directory)) as journal:
                journal.append({'writer': 'next'})

    def test_abrupt_process_exit_keeps_events_and_releases_writer_lock(self):
        with TemporaryDirectory() as directory:
            code = ("from pathlib import Path; import os, sys; "
                    "from trading_system.persistence import DurableJournal; "
                    "journal = DurableJournal(Path(sys.argv[1])); "
                    "journal.append({'committed': True}); os._exit(0)")
            subprocess.run([sys.executable, '-c', code, directory], check=True, timeout=10)
            with DurableJournal(Path(directory)) as restored:
                self.assertEqual(restored.records, ({'committed': True},))

    def test_sqlite_conflict_is_reported_without_overwriting_either_history(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                journal.append({'fill': 1})
            path = Path(directory) / 'events.sqlite3'
            with closing(sqlite3.connect(path)) as db:
                db.execute("UPDATE events SET payload = '{}' WHERE sequence = 1")
                db.commit()
            with self.assertRaises(JournalError):
                DurableJournal(Path(directory))
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute('SELECT payload FROM events').fetchone()[0], '{}')


class RecoveryTests(IsolatedAsyncioTestCase):
    async def test_restart_restores_closed_equity_and_deduplicates_execution(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    buy = context.orders()[0]
                    await gateway.fill(buy.order_id, 100, commission=1)
                    await runtime.drain()
                    await context.place_order(OrderIntent(ROUTE, Side.SELL, 2))
                    await runtime.drain()
                    last = await gateway.fill(context.orders()[-1].order_id, 110, commission=1)
                    await runtime.drain()
                    before = context.equity()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    restored.add_gateway(gateway)
                    strategy = PassiveStrategy()
                    context = await restored.start_strategy(strategy)
                    self.assertEqual(context.equity(), before)
                    self.assertEqual(context.position(ROUTE).quantity, 0)
                    self.assertEqual(len(context.trades()), 1)
                    self.assertEqual(before[-1].value, 18)
                    self.assertEqual(strategy.fills, [])
                    await restored.bus.publish('orders', last)
                    await restored.drain()
                    self.assertEqual(context.equity(), before)
                    self.assertEqual(strategy.fills, [])
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
                    self.assertEqual(gateway.order_snapshots(), ())

    async def test_write_failure_does_not_send_order_or_apply_fill(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    with patch.object(journal, 'append', side_effect=JournalError('unavailable')):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                        with self.assertRaises(DeliveryError):
                            await runtime.drain()
                    self.assertEqual(context.orders(), ())
                    self.assertEqual(gateway.order_snapshots(), ())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    order = context.orders()[0]
                    fill = Execution('lost-delivery', order.order_id, ROUTE, Side.BUY, 1, 100, 1, START)
                    with patch.object(journal, 'append', side_effect=JournalError('unavailable')):
                        await runtime.bus.publish('orders', fill)
                        with self.assertRaises(DeliveryError):
                            await runtime.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 0)
                    self.assertEqual(context.orders()[0].filled_quantity, 0)
                    await runtime.bus.publish('orders', fill)
                    await runtime.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 1)

    async def test_degraded_database_blocks_new_trades_but_keeps_fills_and_cancel(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 3))
                    await runtime.drain()
                    order = context.orders()[0]
                    with patch.object(journal, '_project', side_effect=sqlite3.OperationalError('db down')):
                        await gateway.fill(order.order_id, 100, quantity=1)
                        await runtime.drain()
                        with self.assertRaises(RuntimeError):
                            await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
                        with self.assertRaises(RuntimeError):
                            await context.replace_order(order.order_id, OrderIntent(ROUTE, Side.BUY, 4))
                        await context.cancel_order(order.order_id)
                        await runtime.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 1)
                    self.assertEqual(context.orders()[0].status, OrderStatus.CANCELLED)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    self.assertEqual(restored.ledger.position('passive', ROUTE).quantity, 1)
                    self.assertEqual(restored.orders.orders('passive')[0].status, OrderStatus.CANCELLED)

    async def test_cancel_barrier_survives_restart_and_replacement_is_sent_once(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock, auto_confirm_cancels=False)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 5, 100))
                    await runtime.drain()
                    original = context.orders()[0]
                    await context.replace_order(original.order_id, OrderIntent(ROUTE, Side.BUY, 5, 99))
                    await runtime.drain()
                    request = context.orders()[0].cancel_request_id
                    response = GatewayCancelled(original.order_id, 'sim', request, 2)
                    await runtime.bus.publish('orders', response)
                    await runtime.drain()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    restored.add_gateway(gateway)
                    context = await restored.start_strategy(PassiveStrategy())
                    self.assertEqual(len(context.orders()), 1)
                    self.assertEqual(gateway.order_snapshots(), ())
                    fill = Execution('late', original.order_id, ROUTE, Side.BUY, 2, 100, 1, START)
                    await restored.bus.publish('orders', fill)
                    await restored.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 2)
                    self.assertEqual(len(context.orders()), 1)
                    self.assertEqual(gateway.order_snapshots(), ())
                    old = context.orders()[0]
                    verified = replace(old, status=OrderStatus.CANCELLED)
                    # Independent fixture models the broker's final cancellation report.
                    await restored.reconcile_gateway('sim', (verified,))
                    child = context.orders()[1]
                    self.assertEqual(child.intent.quantity, 3)
                    self.assertEqual(len(gateway.order_snapshots()), 1)
                    child_id = child.order_id
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    restored.add_gateway(gateway)
                    context = await restored.start_strategy(PassiveStrategy())
                    await restored.bus.publish('orders', fill)
                    await restored.bus.publish('orders', response)
                    await restored.drain()
                    self.assertEqual(context.orders()[1].order_id, child_id)
                    self.assertEqual(gateway.order_snapshots(), ())

    async def test_matching_gateway_state_allows_resume_and_mismatch_does_not(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    observed = gateway.order_snapshots()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    gateway.restore_orders(observed)
                    restored.add_gateway(gateway)
                    context = await restored.start_strategy(PassiveStrategy())
                    with self.assertRaises(ValueError):
                        await restored.reconcile_gateway('sim', ())
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
                    await restored.reconcile_gateway('sim', gateway.order_snapshots())
                    await gateway.fill(observed[0].order_id, 100)
                    await restored.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 2)
                    await context.place_order(OrderIntent(ROUTE, Side.SELL, 2))
                    await restored.drain()
                    self.assertEqual(len(gateway.order_snapshots()), 2)

    async def test_crash_after_commit_before_gateway_send_restores_same_id_without_resend(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    publish = runtime.bus.publish

                    async def crash_before_gateway(recipient, message):
                        if recipient == 'gateway:sim':
                            raise OSError('process failed before dispatch')
                        await publish(recipient, message)

                    with patch.object(runtime.bus, 'publish', side_effect=crash_before_gateway):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                        with self.assertRaises(DeliveryError):
                            await runtime.drain()
                    committed = context.orders()[0]
                    self.assertEqual(gateway.order_snapshots(), ())
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    restored.add_gateway(gateway)
                    context = await restored.start_strategy(PassiveStrategy())
                    self.assertEqual(context.orders()[0].order_id, committed.order_id)
                    self.assertEqual(context.orders()[0].status, OrderStatus.SUBMITTED)
                    self.assertEqual(gateway.order_snapshots(), ())
                    with self.assertRaises(ValueError):
                        await restored.reconcile_gateway('sim', ())

    async def test_conflicting_execution_after_restart_is_rejected_without_changing_history(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    fill = await gateway.fill(context.orders()[0].order_id, 100, quantity=1, commission=1)
                    await runtime.drain()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    restored.add_gateway(SimulationGateway('sim', restored.clock))
                    context = await restored.start_strategy(PassiveStrategy())
                    await restored.bus.publish('orders', replace(fill, commission=2))
                    with self.assertRaises(DeliveryError):
                        await restored.drain()
                    self.assertEqual(context.position(ROUTE).quantity, 1)
                    self.assertEqual(context.orders()[0].commission, 1)
                    self.assertEqual(len(journal.records), 3)

    async def test_pending_cancel_can_be_abandoned_before_reconciliation_without_new_order(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock, auto_confirm_cancels=False)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 5))
                    await runtime.drain()
                    original = context.orders()[0]
                    await context.replace_order(original.order_id, OrderIntent(ROUTE, Side.BUY, 5, 100))
                    await runtime.drain()
                    request = context.orders()[0].cancel_request_id
                    # Broker's final report declares a fill still missing locally.
                    await runtime.bus.publish('orders', GatewayCancelled(original.order_id, 'sim', request, 1))
                    await runtime.drain()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    gateway = SimulationGateway('sim', restored.clock)
                    restored.add_gateway(gateway)
                    context = await restored.start_strategy(PassiveStrategy())
                    await context.cancel_order(original.order_id)
                    await restored.drain()
                    await restored.bus.publish('orders', Execution('late-abandon', original.order_id, ROUTE,
                                                                  Side.BUY, 1, 100, 0, START))
                    await restored.drain()
                    self.assertEqual(context.orders()[0].status, OrderStatus.CANCELLED)
                    self.assertEqual(len(context.orders()), 1)
                    self.assertEqual(gateway.order_snapshots(), ())

    async def test_locally_rejected_intent_does_not_require_a_nonexistent_broker_order(self):
        bad_route = replace(ROUTE, instrument=replace(ROUTE.instrument, quantity_step=2))
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    runtime.add_gateway(SimulationGateway('sim', runtime.clock))
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(bad_route, Side.BUY, 1))
                    await runtime.drain()
                    self.assertEqual(context.orders()[0].status, OrderStatus.REJECTED)
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    restored.add_gateway(SimulationGateway('sim', restored.clock))
                    context = await restored.start_strategy(PassiveStrategy())
                    await restored.reconcile_gateway('sim', ())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))
                    await restored.drain()
                    self.assertEqual(len(context.orders()), 2)

    async def test_pending_cancel_requires_the_same_broker_request_identifier(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as runtime:
                    gateway = SimulationGateway('sim', runtime.clock, auto_confirm_cancels=False)
                    runtime.add_gateway(gateway)
                    context = await runtime.start_strategy(PassiveStrategy())
                    await context.place_order(OrderIntent(ROUTE, Side.BUY, 2))
                    await runtime.drain()
                    await context.cancel_order(context.orders()[0].order_id)
                    await runtime.drain()
                    observed = gateway.order_snapshots()
            with DurableJournal(Path(directory)) as journal:
                async with TradingRuntime(ManualClock(START), journal=journal) as restored:
                    restored.add_gateway(SimulationGateway('sim', restored.clock))
                    context = await restored.start_strategy(PassiveStrategy())
                    with self.assertRaises(ValueError):
                        await restored.reconcile_gateway('sim', (replace(observed[0], cancel_request_id='other'),))
                    with self.assertRaises(RuntimeError):
                        await context.place_order(OrderIntent(ROUTE, Side.BUY, 1))

    async def test_unknown_journal_schema_refuses_to_start_runtime(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                journal.append({'version': 99})
            with DurableJournal(Path(directory)) as journal:
                with self.assertRaises(JournalError):
                    async with TradingRuntime(ManualClock(START), journal=journal):
                        self.fail('invalid journal must not activate a runtime')

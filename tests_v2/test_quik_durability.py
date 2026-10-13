import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.domain import Execution, GatewayAccepted, GatewayCancel, GatewaySubmit, GatewayUncertain, OrderIntent, OrderSnapshot, Side
from trading_system.events import DeliveryError, EventBus
from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_durable import QuikDurableGateway
from trading_system.time import ManualClock
from tests_v2.helpers import START
from tests_v2.test_quik_contract import BROKER_ID, ROUTE, SPEC, order_report, trade_report


class QuikDurabilityTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.clock = ManualClock(START)
        self.received = []
        self.journal = DurableJournal(self.path)
        await self.start()

    async def start(self, **changes):
        self.gateway = await QuikDurableGateway.open('quik', (SPEC,), self.clock, journal=self.journal,
            session_id=changes.pop('session_id', 'psb-fixture'), timezone_offset=timedelta(hours=3), **changes)
        self.bus = EventBus()

        async def receive(message):
            # Every financial output must already be recoverable from disk.
            if isinstance(message, (Execution, GatewayAccepted)):
                self.assertEqual(self.journal.records[-1]['kind'], 'result')
            self.received.append(message)

        self.bus.subscribe('orders', receive)
        self.gateway.attach(self.bus)
        await self.gateway.recover()
        await self.bus.drain()

    async def asyncTearDown(self):
        await self.bus.close()
        self.journal.close()

    async def command(self, message):
        await self.bus.publish('gateway:quik', message)
        await self.bus.drain()

    async def submit(self):
        snapshot = OrderSnapshot('order-1', 'a', OrderIntent(ROUTE, Side.BUY, 4, 100), START)
        await self.command(GatewaySubmit(snapshot))
        return snapshot

    async def feed(self, name, data):
        await self.gateway.feed(name, data)
        await self.bus.drain()

    async def restart(self):
        await self.bus.close()
        self.journal.close()
        self.journal = DurableJournal(self.path)
        await self.start()

    async def test_prepared_transaction_is_durable_and_duplicate_command_does_not_allocate(self):
        original = await self.submit()
        await self.command(GatewaySubmit(original))
        self.assertEqual([t.transaction_id for t in self.gateway.transactions], [1])
        self.assertEqual(self.gateway.next_transaction_id, 2)
        self.assertEqual(len(self.gateway.prepared), 1)
        records = self.journal.records
        self.assertEqual([r['kind'] for r in records], ['session', 'input', 'result', 'input', 'result'])
        self.assertEqual(records[2]['transactions'][0]['fields']['TRANS_ID'], '1')

    async def test_restart_restores_large_broker_mapping_ids_and_dedup_without_resending(self):
        await self.submit()
        await self.feed('OnOrder', order_report(1))
        await self.feed('OnTrade', trade_report())
        await self.restart()
        self.assertEqual(self.gateway.broker_orders, ((BROKER_ID, 'order-1'),))
        self.assertEqual(self.gateway.next_transaction_id, 2)
        self.assertEqual(self.gateway.prepared, ())
        self.assertTrue(self.gateway.blocked)
        before = sum(isinstance(m, Execution) for m in self.received)
        await self.feed('OnTrade', trade_report())
        self.assertEqual(sum(isinstance(m, Execution) for m in self.received), before)
        await self.feed('OnTrade', trade_report(trade_num=9007199254740996))
        self.assertEqual(sum(isinstance(m, Execution) for m in self.received), before + 1)
        self.assertEqual(self.received[-1].execution_id, 'psb-fixture:9007199254740996')

    async def test_early_trade_and_original_deadline_survive_restart(self):
        await self.submit()
        await self.feed('OnTrade', trade_report())
        self.clock.advance_to(START + timedelta(seconds=80))
        await self.restart()
        self.assertEqual(len(self.gateway.unmatched), 1)
        self.clock.advance_to(START + timedelta(seconds=90))
        await self.gateway.check_deadlines()
        await self.bus.drain()
        self.assertIn('deadline expired', self.gateway.incidents[-1])
        await self.feed('OnOrder', order_report(1, balance=3))
        self.assertEqual(self.gateway.unmatched, ())
        self.assertTrue(any(isinstance(m, Execution) for m in self.received))

    async def test_cancel_mapping_and_late_trade_survive_restart(self):
        await self.submit()
        await self.feed('OnOrder', order_report(1))
        await self.command(GatewayCancel('order-1', 'quik', 'cancel-1'))
        await self.feed('OnOrder', order_report(1, balance=3, flags=10))
        await self.restart()
        self.assertEqual([t.transaction_id for t in self.gateway.transactions], [1, 2])
        self.assertEqual(dict(self.gateway.transactions[-1].fields)['ORDER_KEY'], str(BROKER_ID))
        await self.feed('OnTrade', trade_report())
        self.assertTrue(any(isinstance(m, Execution) for m in self.received))
        self.assertEqual(self.gateway.prepared, ())

    async def test_raw_invalid_callback_is_retained_and_cannot_partially_bind(self):
        await self.submit()
        raw = {**order_report(1), 'account': 'wrong', 'unused': {'exact_id': BROKER_ID}}
        with self.assertRaises(ValueError):
            await self.gateway.feed('OnOrder', raw)
        await self.bus.drain()
        self.assertEqual(self.journal.records[-2]['payload']['data'], raw)
        self.assertTrue(self.journal.records[-1]['error'])
        self.assertEqual(self.gateway.broker_orders, ())
        self.assertFalse(any(isinstance(m, GatewayAccepted) for m in self.received))
        await self.restart()
        self.assertTrue(self.gateway.blocked)

    async def test_failure_to_save_input_prevents_id_allocation(self):
        with patch.object(self.journal, 'append', side_effect=JournalError('disk failed')):
            with self.assertRaises(DeliveryError):
                await self.submit()
        await self.bus.drain()
        self.assertEqual(self.gateway.transactions, ())
        self.assertEqual(self.gateway.next_transaction_id, 1)
        self.assertTrue(self.gateway.blocked)
        with self.assertRaises(JournalError):
            await self.gateway.feed('OnConnected', {})

    async def test_crash_after_input_before_result_recovers_preparation_but_never_dispatches(self):
        append = self.journal.append

        def fail_result(record):
            if record['kind'] == 'result':
                raise JournalError('power loss before result')
            append(record)

        with patch.object(self.journal, 'append', side_effect=fail_result):
            with self.assertRaises(DeliveryError):
                await self.submit()
        await self.bus.drain()
        self.assertEqual(self.journal.records[-1]['kind'], 'input')
        self.assertEqual(self.gateway.transactions, ())
        await self.restart()
        self.assertEqual([t.transaction_id for t in self.gateway.transactions], [1])
        self.assertEqual(self.gateway.next_transaction_id, 2)
        self.assertEqual(self.gateway.prepared, ())
        self.assertTrue(self.gateway.blocked)

    async def test_committed_callback_lost_before_delivery_is_republished(self):
        await self.submit()
        await self.feed('OnOrder', order_report(1))
        with patch.object(self.bus, 'publish', side_effect=RuntimeError('delivery interrupted')):
            with self.assertRaises(RuntimeError):
                await self.gateway.feed('OnTrade', trade_report())
        self.assertFalse(any(isinstance(m, Execution) for m in self.received))
        await self.restart()
        fills = [m for m in self.received if isinstance(m, Execution)]
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].quantity, 1)

    async def test_session_and_route_metadata_cannot_change_on_restart(self):
        await self.submit()
        for specs, session in (((SPEC,), 'another-session'),
                               ((replace(SPEC, commission_fields=()),), 'psb-fixture')):
            with self.subTest(session=session), self.assertRaises(JournalError):
                await QuikDurableGateway.open('quik', specs, self.clock, journal=self.journal,
                    session_id=session, timezone_offset=timedelta(hours=3))

    async def test_valid_hash_chain_with_false_derived_result_is_rejected(self):
        await self.submit()
        records = list(self.journal.records)
        records[-1]['transactions'][0]['fields']['PRICE'] = '1'
        with TemporaryDirectory() as other, DurableJournal(Path(other)) as journal:
            for record in records:
                journal.append(record)
            with self.assertRaises(JournalError):
                await QuikDurableGateway.open('quik', (SPEC,), self.clock, journal=journal,
                    session_id='psb-fixture', timezone_offset=timedelta(hours=3))

    async def test_commands_are_blocked_after_restart_even_if_caller_bypasses_oms(self):
        await self.submit()
        await self.restart()
        await self.command(GatewaySubmit(OrderSnapshot('another', 'a', OrderIntent(ROUTE, Side.BUY, 1, 100), START)))
        self.assertEqual(len(self.gateway.transactions), 1)
        self.assertEqual(self.gateway.prepared, ())
        self.assertIsInstance(self.received[-1], GatewayUncertain)

    async def test_transport_response_and_disconnect_are_journaled(self):
        await self.submit()
        await self.gateway.transport_response(1, {'cmd': 'sendTransaction', 'data': True})
        await self.feed('OnDisconnected', {})
        await self.restart()
        self.assertTrue(self.gateway.blocked)
        kinds = [r['payload']['op'] for r in self.journal.records if r['kind'] == 'input']
        self.assertIn('response', kinds)
        self.assertIn('callback', kinds)

    async def test_invalid_envelope_blocks_without_leaving_an_unreplayable_input(self):
        for event, data in ((4, {}), ('OnOrder', None), ('OnTrade', {'price': float('nan')}),
                            ('OnTrade', {1: 'not a JSON key'})):
            with self.subTest(event=event, data=data), self.assertRaises(ValueError):
                await self.gateway.feed(event, data)
            await self.bus.drain()
            self.assertEqual(self.journal.records[-1]['kind'], 'result')
        await self.restart()
        self.assertTrue(self.gateway.blocked)

    async def test_original_callback_is_copied_and_not_mutated_by_the_caller(self):
        await self.submit()
        report = trade_report()
        await self.feed('OnTrade', report)
        report['datetime']['hour'] = 1
        report['price'] = 1
        raw = self.journal.records[-2]['payload']['data']
        self.assertEqual((raw['datetime']['hour'], raw['price']), (13, 100))
        await self.restart()
        self.assertEqual(len(self.gateway.unmatched), 1)

    async def test_concurrent_commands_have_distinct_durable_transaction_ids(self):
        await asyncio.gather(*(self.command(GatewaySubmit(OrderSnapshot(f'order-{i}', 'a',
            OrderIntent(ROUTE, Side.BUY, 1, 100), START))) for i in range(8)))
        self.assertEqual([t.transaction_id for t in self.gateway.transactions], list(range(1, 9)))
        await self.restart()
        self.assertEqual(self.gateway.next_transaction_id, 9)

    async def test_second_engine_cannot_silently_append_using_a_stale_journal_tail(self):
        await self.submit()
        await QuikDurableGateway.open('quik', (SPEC,), self.clock, journal=self.journal,
            session_id='psb-fixture', timezone_offset=timedelta(hours=3))
        with self.assertRaises(JournalError):
            await self.gateway.feed('OnConnected', {})
        await self.bus.drain()
        await self.restart()
        self.assertEqual(self.gateway.next_transaction_id, 2)

    async def test_backward_recovery_clock_is_rejected_before_appending(self):
        self.clock.advance_to(START + timedelta(seconds=10))
        await self.feed('OnConnected', {})
        count = len(self.journal.records)
        with self.assertRaises(JournalError):
            await QuikDurableGateway.open('quik', (SPEC,), ManualClock(START), journal=self.journal,
                session_id='psb-fixture', timezone_offset=timedelta(hours=3))
        self.assertEqual(len(self.journal.records), count)

    async def test_cancel_queued_before_registration_is_not_sent_during_recovery(self):
        await self.submit()
        await self.command(GatewayCancel('order-1', 'quik', 'cancel-1'))
        await self.restart()
        await self.feed('OnOrder', order_report(1))
        self.assertEqual(len(self.gateway.transactions), 1)
        self.assertEqual(self.gateway.prepared, ())

    async def test_crash_with_only_raw_trade_persisted_recovers_financial_output(self):
        await self.submit()
        await self.feed('OnOrder', order_report(1))
        append = self.journal.append

        def stop_before_result(record):
            if record['kind'] == 'result':
                raise JournalError('interrupted after raw trade')
            append(record)

        with patch.object(self.journal, 'append', side_effect=stop_before_result):
            with self.assertRaises(JournalError):
                await self.gateway.feed('OnTrade', trade_report())
        await self.bus.drain()
        self.assertFalse(any(isinstance(m, Execution) for m in self.received))
        await self.restart()
        self.assertEqual(sum(isinstance(m, Execution) for m in self.received), 1)
        self.assertEqual(self.gateway.prepared, ())

    async def test_uncertain_result_write_does_not_reuse_an_already_saved_id(self):
        append = self.journal.append

        def stop_after_result(record):
            append(record)
            if record['kind'] == 'result':
                raise JournalError('saved result but acknowledgement lost')

        with patch.object(self.journal, 'append', side_effect=stop_after_result):
            with self.assertRaises(DeliveryError):
                await self.submit()
        await self.bus.drain()
        self.assertEqual(self.gateway.prepared, ())
        await self.restart()
        self.assertEqual(self.gateway.next_transaction_id, 2)
        self.assertEqual(len(self.gateway.transactions), 1)

    async def test_closed_journal_blocks_and_notifies_oms_before_new_preparation(self):
        self.journal.close()
        with self.assertRaises(DeliveryError):
            await self.submit()
        self.assertTrue(self.gateway.blocked)
        self.assertIsInstance(self.received[-1], GatewayUncertain)
        self.assertEqual(self.gateway.transactions, ())

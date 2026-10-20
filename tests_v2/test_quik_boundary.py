import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_boundary import CallbackCursor, CallbackTracker
from trading_system.quik_readonly import ReadOnlyQuik, QuikTransportError
from trading_system.quik_snapshot import capture_snapshot, SnapshotError
from tests_v2.quik_server import QuikServer


SESSION = '0123456789abcdef0123456789abcdef'


def cursor(sequence=0, failed_sends=0, session=SESSION):
    return {'bridge_session': session, 'sequence': sequence, 'failed_sends': failed_sends}


def frame(sequence, command='OnOrder', **changes):
    return {'cmd': command, 'data': {'order_num': 501}, 'callback_cursor': cursor(sequence, **changes)}


class CursorTests(TestCase):
    def test_baseline_does_not_claim_delivery_of_earlier_connections(self):
        tracker = CallbackTracker(CallbackCursor.parse(cursor(100, 3)))
        self.assertEqual(tracker.received_sequence, 100)
        tracker.target(CallbackCursor.parse(cursor(100, 3)))
        tracker.observe(CallbackCursor.parse(cursor(101, 3)))
        self.assertEqual(tracker.received_sequence, 101)

    def test_pre_handshake_frames_are_validated_without_replaying_baseline(self):
        tracker = CallbackTracker(CallbackCursor.parse(cursor(10, 2)))
        tracker.observe(CallbackCursor.parse(cursor(9, 1)))
        tracker.observe(CallbackCursor.parse(cursor(10, 2)))
        tracker.observe(CallbackCursor.parse(cursor(11, 2)))
        self.assertEqual(tracker.received_sequence, 11)

    def test_gap_duplicate_and_reversed_callbacks_rejected(self):
        for sequence in (1, 3, 0):
            with self.subTest(sequence=sequence):
                tracker = CallbackTracker(CallbackCursor.parse(cursor()))
                tracker.observe(CallbackCursor.parse(cursor(1)))
                with self.assertRaises(ValueError):
                    tracker.observe(CallbackCursor.parse(cursor(sequence)))
                self.assertEqual(tracker.received_sequence, 1)

    def test_initial_callback_cannot_skip_a_post_baseline_sequence(self):
        tracker = CallbackTracker(CallbackCursor.parse(cursor(100)))
        with self.assertRaises(ValueError):
            tracker.observe(CallbackCursor.parse(cursor(102)))

    def test_delayed_cursor_response_can_precede_already_received_callback(self):
        tracker = CallbackTracker(CallbackCursor.parse(cursor()))
        tracker.observe(CallbackCursor.parse(cursor(1)))
        tracker.observe(CallbackCursor.parse(cursor(2)))
        tracker.target(CallbackCursor.parse(cursor(1)))
        self.assertEqual(tracker.received_sequence, 2)

    def test_cursor_regression_and_new_send_failure_are_rejected(self):
        for value in (cursor(0), cursor(1, 1)):
            tracker = CallbackTracker(CallbackCursor.parse(cursor()))
            tracker.target(CallbackCursor.parse(cursor(1)))
            with self.assertRaises(ValueError):
                tracker.target(CallbackCursor.parse(value))

    def test_new_failure_in_callback_blocks_even_when_sequence_is_contiguous(self):
        tracker = CallbackTracker(CallbackCursor.parse(cursor()))
        with self.assertRaises(ValueError):
            tracker.observe(CallbackCursor.parse(cursor(1, 1)))

    def test_foreign_bridge_is_rejected_on_both_channels(self):
        for method in ('observe', 'target'):
            tracker = CallbackTracker(CallbackCursor.parse(cursor()))
            with self.assertRaises(ValueError):
                getattr(tracker, method)(CallbackCursor.parse(cursor(1, session='f' * 32)))

    def test_exact_bounded_schema_rejects_bool_strings_overflow_and_extra_fields(self):
        cases = [None, {}, {**cursor(), 'extra': 1}, cursor(True), cursor('1'), cursor(-1), cursor(2**31),
                 cursor(0, True), cursor(0, -1), cursor(session=''), cursor(session='x' * 32)]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                CallbackCursor.parse(value)


class BoundaryTransportTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = DurableJournal(Path(self.temp.name))
        self.server = await QuikServer().start()
        info = self.server.responses['tsRuntimeInfo']
        info.update(capabilities=['callback_cursor_v1'], callback_cursor=cursor())
        self.server.responses['tsCallbackCursor'] = cursor()
        self.client = ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port, timeout=1)

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.close()
        self.journal.close()

    async def wait_until(self, predicate):
        async with asyncio.timeout(3):
            while not predicate():
                await asyncio.sleep(0.001)

    async def test_fence_waits_for_callback_channel_after_response_channel_replies(self):
        await self.client.connect()
        self.server.responses['tsCallbackCursor'] = cursor(1)
        task = asyncio.create_task(self.client.callback_fence())
        await self.wait_until(lambda: any(r['cmd'] == 'tsCallbackCursor' for r in self.server.requests))
        self.assertFalse(task.done())
        await self.server.emit(frame(1))
        result = await task
        self.assertEqual(result.sequence, 1)
        callbacks = self.client.drain_callbacks()
        self.assertEqual(callbacks[0].cursor, result)
        self.assertEqual(callbacks[0].data, {'order_num': 501})
        self.assertTrue(any(r['event'] == 'callback' for r in self.journal.records))
        self.assertEqual(self.journal.records[-1]['event'], 'callback_fence')
        self.assertEqual(self.journal.records[-1]['cursor'], cursor(1))

    async def test_callback_can_arrive_before_corresponding_cursor_response(self):
        await self.client.connect()
        await self.server.emit(frame(1))
        await self.wait_until(lambda: self.client.callback_count == 1)
        self.server.responses['tsCallbackCursor'] = cursor(1)
        self.assertEqual((await self.client.callback_fence()).sequence, 1)

    async def test_cursor_cannot_predate_callbacks_received_before_its_request(self):
        await self.client.connect()
        await self.server.emit(frame(1))
        await self.wait_until(lambda: self.client.callback_count == 1)
        # No earlier fence exists, but Lua must already have allocated sequence 1.
        self.server.responses['tsCallbackCursor'] = cursor(0)
        with self.assertRaises(QuikTransportError):
            await self.client.callback_fence()
        self.assertFalse(self.client.healthy)

    async def test_callback_allocated_during_cursor_request_may_overtake_response(self):
        await self.client.connect()
        async def overtake(request, raw):
            if request['cmd'] == 'tsCallbackCursor':
                await self.server.emit(frame(1))
                await self.wait_until(lambda: self.client.callback_count == 1)
            return raw
        self.server.override = overtake
        self.assertEqual((await self.client.callback_fence()).sequence, 0)
        self.assertTrue(self.client.healthy)

    async def test_zero_callback_window_is_fenced_without_trading_permission(self):
        await self.client.connect()
        result = await capture_snapshot(self.client, require_callback_fence=True)
        boundary = result['callback_boundary']
        self.assertTrue(boundary['delivery_verified'])
        self.assertTrue(boundary['quiet'])
        self.assertFalse(boundary['broker_atomic'])
        self.assertFalse(result['reconciled'])
        self.assertEqual(boundary['begin'], cursor())
        self.assertEqual(boundary['end'], cursor())
        self.assertEqual([r['cmd'] for r in self.server.requests].count('tsCallbackCursor'), 2)

    async def test_delayed_event_after_table_reads_invalidates_quiet_snapshot(self):
        await self.client.connect()
        end_requested = asyncio.Event()
        calls = 0
        async def override(request, raw):
            nonlocal calls
            if request['cmd'] == 'tsCallbackCursor':
                calls += 1
                if calls == 2:
                    response = json.loads(raw)
                    response['data'] = cursor(1)
                    end_requested.set()
                    return json.dumps(response).encode() + b'\n'
            return raw
        self.server.override = override
        task = asyncio.create_task(capture_snapshot(self.client, require_callback_fence=True))
        await end_requested.wait()
        self.assertFalse(task.done())
        await self.server.emit(frame(1, 'OnTrade'))
        result = await task
        self.assertFalse(result['repeatable_observation'])
        self.assertFalse(result['callback_boundary']['quiet'])
        self.assertIn('bridge_callbacks_during_scan', result['issues'])

    async def test_missing_callback_times_out_and_quarantines_both_channels(self):
        await self.client.connect()
        self.client.timeout = 0.05
        self.server.responses['tsCallbackCursor'] = cursor(1)
        with self.assertRaises(QuikTransportError):
            await self.client.callback_fence()
        self.assertFalse(self.client.healthy)
        self.assertIsNotNone(self.client.fault)
        self.assertEqual([r['cmd'] for r in self.server.requests].count('tsCallbackCursor'), 1)

    async def test_gap_is_a_transport_fault_not_financial_deduplication(self):
        await self.client.connect()
        await self.server.emit(frame(2))
        await self.wait_until(lambda: self.client.fault is not None)
        self.assertFalse(self.client.healthy)
        self.assertIn('sequence', self.client.fault)

    async def test_duplicate_transport_sequence_is_rejected(self):
        await self.client.connect()
        await self.server.emit(frame(1))
        await self.server.emit(frame(1))
        await self.wait_until(lambda: self.client.fault is not None)
        self.assertFalse(self.client.healthy)

    async def test_duplicate_trade_payload_with_distinct_transport_sequences_is_preserved(self):
        await self.client.connect()
        await self.server.emit(frame(1, 'OnTrade'))
        await self.server.emit(frame(2, 'OnTrade'))
        await self.wait_until(lambda: self.client.callback_count == 2)
        records = self.client.drain_callbacks()
        self.assertEqual(records[0].data, records[1].data)
        self.assertNotEqual(records[0].cursor, records[1].cursor)
        self.assertTrue(self.client.healthy)

    async def test_foreign_callback_bridge_is_rejected(self):
        await self.client.connect()
        await self.server.emit(frame(1, session='f' * 32))
        await self.wait_until(lambda: self.client.fault is not None)
        self.assertFalse(self.client.healthy)

    async def test_new_capability_requires_metadata_on_every_callback(self):
        await self.client.connect()
        await self.server.emit({'cmd': 'OnOrder', 'data': {}})
        await self.wait_until(lambda: self.client.fault is not None)
        self.assertFalse(self.client.healthy)

    async def test_fatal_callback_wakes_fence_waiter(self):
        await self.client.connect()
        self.server.responses['tsCallbackCursor'] = cursor(2)
        task = asyncio.create_task(self.client.callback_fence())
        await self.wait_until(lambda: any(r['cmd'] == 'tsCallbackCursor' for r in self.server.requests))
        await self.server.emit(frame(1, 'OnDisconnected'))
        with self.assertRaises(QuikTransportError):
            await asyncio.wait_for(task, 0.5)

    async def test_failed_send_counter_blocks_even_without_any_received_callback(self):
        await self.client.connect()
        self.server.responses['tsCallbackCursor'] = cursor(0, 1)
        with self.assertRaises(QuikTransportError):
            await self.client.callback_fence()
        self.assertFalse(self.client.healthy)

    async def test_cancellation_of_fence_does_not_allow_reuse(self):
        await self.client.connect()
        self.server.responses['tsCallbackCursor'] = cursor(1)
        task = asyncio.create_task(self.client.callback_fence())
        await self.wait_until(lambda: any(r['cmd'] == 'tsCallbackCursor' for r in self.server.requests))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.client.healthy)

    async def test_legacy_bridge_stays_usable_but_cannot_claim_a_fence(self):
        self.server.responses['tsRuntimeInfo'].pop('capabilities')
        self.server.responses['tsRuntimeInfo'].pop('callback_cursor')
        await self.client.connect()
        ordinary = await capture_snapshot(self.client)
        self.assertIsNone(ordinary['callback_boundary'])
        before = len(self.server.requests)
        with self.assertRaisesRegex(SnapshotError, 'update and restart'):
            await capture_snapshot(self.client, require_callback_fence=True)
        self.assertEqual(len(self.server.requests), before)
        self.assertTrue(self.client.healthy)

    async def test_invalid_runtime_cursor_rejected_before_ping(self):
        self.server.responses['tsRuntimeInfo']['callback_cursor'] = cursor(True)
        with self.assertRaises(QuikTransportError):
            await self.client.connect()
        self.assertEqual([r['cmd'] for r in self.server.requests], ['tsRuntimeInfo'])

    async def test_frames_received_before_runtime_reply_are_validated_after_handshake(self):
        async def early(request, raw):
            if request['cmd'] == 'tsRuntimeInfo':
                await self.server.emit(frame(1))
                await self.wait_until(lambda: self.client.callback_count == 1)
            return raw
        self.server.override = early
        await self.client.connect()
        self.server.responses['tsCallbackCursor'] = cursor(1)
        self.assertEqual((await self.client.callback_fence()).sequence, 1)

    async def test_fence_operations_serialize_without_consuming_callbacks(self):
        await self.client.connect()
        result = await asyncio.gather(self.client.callback_fence(), self.client.callback_fence())
        self.assertEqual(result[0], result[1])
        self.assertTrue(self.client.healthy)

    async def test_fence_journal_failure_prevents_a_successful_boundary(self):
        await self.client.connect()
        original = self.journal.append
        def append(record):
            if record.get('event') == 'callback_fence':
                raise JournalError('synthetic persistence failure')
            return original(record)
        with patch.object(self.journal, 'append', side_effect=append), self.assertRaises(QuikTransportError):
            await self.client.callback_fence()
        self.assertFalse(self.client.healthy)

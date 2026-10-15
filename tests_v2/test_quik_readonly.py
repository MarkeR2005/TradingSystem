import asyncio
import base64
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.persistence import DurableJournal, JournalError
from trading_system.quik_readonly import ReadOnlyQuik, QuikTransportError
from tests_v2.quik_server import QuikServer


class ReadOnlyQuikTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = DurableJournal(Path(self.temp.name))
        self.server = await QuikServer().start()
        self.client = self.make_client()

    def make_client(self, **changes):
        return ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port,
            timeout=changes.pop('timeout', 0.3), **changes)

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.close()
        self.journal.close()

    async def wait_until(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.005)

    async def test_two_socket_handshake_and_fragmented_cp1251_response(self):
        self.server.fragment = True
        self.server.responses['getTradeAccounts'][0]['description'] = 'Демо счёт'
        info = await self.client.connect()
        self.assertEqual(info['lua_version'], 'Lua 5.4')
        result = await self.client.request('getTradeAccounts')
        self.assertEqual(result[0]['description'], 'Демо счёт')
        self.assertTrue(self.client.healthy)
        ids = [r['id'] for r in self.server.requests]
        self.assertEqual(len(ids), len(set(ids)))

    async def test_trade_and_arbitrary_commands_rejected_before_any_write(self):
        await self.client.connect()
        before = len(self.server.requests)
        for command, data in (('sendTransaction', ''), ('message', 'hello'), ('eval', 'sendTransaction({})'),
                              ('getInfoParam', 'PASSWORD'), ('getSecurityInfo', 'SPBFUT|X|Y'),
                              ('get_orders', 'SPBFUT|TEST')):
            with self.subTest(command=command), self.assertRaises(ValueError):
                await self.client.request(command, data)
        self.assertEqual(len(self.server.requests), before)
        self.assertTrue(self.client.healthy)

    async def test_profile_or_version_mismatch_closes_session(self):
        self.server.responses['tsRuntimeInfo']['profile'] = 'psb-live'
        with self.assertRaises(QuikTransportError):
            await self.client.connect()
        self.assertFalse(self.client.healthy)
        self.assertEqual([r['cmd'] for r in self.server.requests], ['tsRuntimeInfo'])

    async def test_major_minor_prefix_does_not_accept_a_different_version(self):
        self.server.responses['tsRuntimeInfo']['quik_version'] = '13.20.0.1'
        with self.assertRaises(QuikTransportError):
            await self.client.connect()

    async def test_bridge_must_explicitly_confirm_read_only_mode(self):
        self.server.responses['tsRuntimeInfo']['mode'] = 'live'
        with self.assertRaises(QuikTransportError):
            await self.client.connect()

    async def test_timeout_is_terminal_and_does_not_retry_or_reuse_socket(self):
        await self.client.connect()
        async def silence(request, raw):
            return None
        self.server.override = silence
        before = len(self.server.requests)
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')
        self.assertEqual(len(self.server.requests), before + 1)

    async def test_wrong_response_identity_cannot_be_applied(self):
        await self.client.connect()
        async def mismatch(request, raw):
            obj = json.loads(raw)
            obj['id'] += 1
            obj['cmd'] = 'lua_error'
            obj['lua_error'] = 'Command not implemented in Lua qsfunctions module: tsRuntimeInfo'
            return json.dumps(obj).encode() + b'\n'
        self.server.override = mismatch
        with self.assertRaisesRegex(QuikTransportError, 'response request id mismatch'):
            await self.client.request('get_orders')
        self.assertFalse(self.client.healthy)

    async def test_lua_error_has_command_context_without_exposing_raw_details(self):
        await self.client.connect()
        async def lua_failure(request, raw):
            return (json.dumps({**request, 'cmd': 'lua_error', 'data': '',
                'lua_error': 'failure for private-account-123'}) + '\n').encode()
        self.server.override = lua_failure
        with self.assertRaisesRegex(QuikTransportError, 'Lua rejected read-only command get_orders') as error:
            await self.client.request('get_orders')
        self.assertNotIn('private-account-123', str(error.exception))
        self.assertFalse(self.client.healthy)
        response = next(r for r in reversed(self.journal.records) if r['event'] == 'response')
        self.assertIn(b'private-account-123', base64.b64decode(response['bytes_b64']))

    async def test_raw_malformed_json_is_saved_before_rejection(self):
        await self.client.connect()
        bad = b'{"id":1,"id":2,"cmd":"get_orders","data":[]}\n'
        async def duplicate(request, raw):
            return bad
        self.server.override = duplicate
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')
        responses = [r for r in self.journal.records if r['event'] == 'response']
        self.assertEqual(base64.b64decode(responses[-1]['bytes_b64']), bad)

    async def test_callbacks_preserve_large_ids_and_raw_bytes(self):
        await self.client.connect()
        await self.server.emit({'cmd': 'OnTrade', 'data': {'trade_num': 9007199254740993}, 't': 0})
        await self.wait_until(lambda: self.client.callback_count == 1)
        callback = self.client.drain_callbacks()[0]
        self.assertEqual(callback.data['trade_num'], 9007199254740993)
        self.assertEqual(self.client.drain_callbacks(), ())
        self.assertEqual(self.client.callback_count, 1)
        self.assertEqual(self.journal.records[-1]['event'], 'callback')

    async def test_callback_overflow_stops_instead_of_silently_dropping(self):
        await self.client.close()
        self.client = self.make_client(max_callbacks=1)
        await self.client.connect()
        await self.server.emit(b'{"cmd":"OnOrder","data":{}}\n{"cmd":"OnTrade","data":{}}\n')
        await self.wait_until(lambda: not self.client.healthy)
        self.assertIn('overflow', self.client.fault)
        self.assertEqual(len([r for r in self.journal.records if r['event'] == 'callback']), 2)

    async def test_terminal_disconnect_interrupts_readiness(self):
        await self.client.connect()
        await self.server.emit({'cmd': 'OnDisconnected', 'data': ''})
        await self.wait_until(lambda: not self.client.healthy)
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')

    async def test_callback_eof_requires_a_new_explicit_connection(self):
        await self.client.connect()
        self.server.callback.close()
        await self.wait_until(lambda: not self.client.healthy)
        with self.assertRaises(QuikTransportError):
            await self.client.connect()

    async def test_journal_failure_prevents_request_from_reaching_terminal(self):
        await self.client.connect()
        before = len(self.server.requests)
        with patch.object(self.journal, 'append', side_effect=JournalError('disk failure')):
            with self.assertRaises(QuikTransportError):
                await self.client.request('get_orders')
        self.assertEqual(len(self.server.requests), before)
        self.assertFalse(self.client.healthy)

    async def test_oversized_frame_is_rejected_with_a_bounded_reader(self):
        await self.client.close()
        self.client = self.make_client(max_frame_bytes=1024)
        await self.client.connect()
        async def huge(request, raw):
            return b'x' * 2048 + b'\n'
        self.server.override = huge
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')

    async def test_cancelled_request_closes_connection_without_retry(self):
        await self.client.connect()
        async def silence(request, raw):
            return None
        self.server.override = silence
        pending = asyncio.create_task(self.client.request('get_orders'))
        await self.wait_until(lambda: self.server.requests[-1]['cmd'] == 'get_orders')
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertFalse(self.client.healthy)

    async def test_only_numeric_loopback_and_distinct_ports_are_allowed(self):
        for options in ({'host': 'localhost'}, {'host': '0.0.0.0'}, {'host': 'example.com'},
                        {'request_port': 0}, {'request_port': 34131, 'callback_port': 34131}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2', **options)

    async def test_lost_broker_connection_invalidates_readiness_even_without_callback(self):
        await self.client.connect()
        self.server.responses['isConnected'] = 0
        with self.assertRaises(QuikTransportError):
            await self.client.request('isConnected')
        self.assertFalse(self.client.healthy)

    async def test_partial_eof_frame_is_preserved_and_never_retried(self):
        await self.client.connect()
        async def partial(request, raw):
            writer = next(w for w in self.server.writers if w is not self.server.callback and not w.is_closing())
            writer.write(b'{"cmd":"get_orders"')
            await writer.drain()
            writer.close()
            return None
        self.server.override = partial
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')
        responses = [r for r in self.journal.records if r['event'] == 'response']
        self.assertEqual(base64.b64decode(responses[-1]['bytes_b64']), b'{"cmd":"get_orders"')
        self.assertEqual(sum(r['cmd'] == 'get_orders' for r in self.server.requests), 1)

    async def test_json_numeric_overflow_is_rejected(self):
        await self.client.connect()
        async def overflow(request, raw):
            return ('{"id":' + str(request['id']) + ',"cmd":"get_orders","data":[1e999]}\n').encode()
        self.server.override = overflow
        with self.assertRaises(QuikTransportError):
            await self.client.request('get_orders')

    async def test_callback_queue_has_a_total_byte_limit(self):
        await self.client.close()
        self.client = self.make_client(max_callback_bytes=16)
        await self.client.connect()
        await self.server.emit({'cmd': 'OnTrade', 'data': {'text': 'x' * 30}})
        await self.wait_until(lambda: not self.client.healthy)
        self.assertIn('overflow', self.client.fault)
        self.assertEqual(self.client.drain_callbacks(), ())

    async def test_wire_capture_has_a_total_budget_before_disk_growth(self):
        await self.client.close()
        self.client = self.make_client(max_capture_bytes=1024)
        await self.client.connect()
        await self.server.emit({'cmd': 'OnTrade', 'data': {'text': 'x' * 900}})
        await self.wait_until(lambda: not self.client.healthy)
        self.assertIn('budget', self.client.fault)
        self.assertLess(sum(len(base64.b64decode(r.get('bytes_b64', ''))) for r in self.journal.records), 1025)

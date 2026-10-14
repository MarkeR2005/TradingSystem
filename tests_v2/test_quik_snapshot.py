import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.persistence import DurableJournal
from trading_system.quik_readonly import ReadOnlyQuik
from trading_system.quik_snapshot import capture_snapshot, SnapshotError
from tests_v2.quik_server import QuikServer


class SnapshotTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = DurableJournal(Path(self.temp.name))
        self.server = await QuikServer().start()
        self.client = ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port, timeout=0.3)
        await self.client.connect()

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.close()
        self.journal.close()

    async def test_repeatable_empty_tables_are_observation_not_financial_reconciliation(self):
        snapshot = await capture_snapshot(self.client, class_code='SPBFUT', symbol='TEST')
        self.assertTrue(snapshot['repeatable_observation'])
        self.assertFalse(snapshot['reconciled'])
        self.assertEqual(snapshot['security']['min_price_step'], 0.5)
        self.assertEqual(snapshot['tables']['orders'], [])
        self.assertEqual(snapshot['classes'], ['SPBFUT', 'QJSIM'])
        self.assertEqual(sum(r['cmd'] == 'get_orders' for r in self.server.requests), 2)

    async def test_changed_table_cannot_be_reported_as_repeatable(self):
        calls = 0
        async def changed(request, raw):
            nonlocal calls
            if request['cmd'] == 'get_orders':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data'] = [{'order_num': 9007199254740993, 'trans_id': 1}]
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = changed
        snapshot = await capture_snapshot(self.client)
        self.assertFalse(snapshot['repeatable_observation'])
        self.assertIn('tables_changed', snapshot['issues'])

    async def test_null_table_is_not_treated_as_an_empty_account(self):
        self.server.responses['getFuturesClientHoldings'] = None
        with self.assertRaises(SnapshotError):
            await capture_snapshot(self.client)

    async def test_duplicate_or_float_broker_identifiers_are_rejected(self):
        for rows in ([{'order_num': float(9007199254740993), 'trans_id': 1}],
                     [{'order_num': 42, 'trans_id': 1}, {'order_num': 42, 'trans_id': 1}]):
            self.server.responses['get_orders'] = rows
            with self.subTest(rows=rows), self.assertRaises(SnapshotError):
                await capture_snapshot(self.client)

    async def test_date_rollover_invalidates_repeatability(self):
        calls = 0
        async def rollover(request, raw):
            nonlocal calls
            if request['cmd'] == 'getInfoParam' and request['data'] == 'TRADEDATE':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data'] = '15.10.2026'
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = rollover
        result = await capture_snapshot(self.client)
        self.assertFalse(result['repeatable_observation'])
        self.assertIn('trading_date_changed', result['issues'])

    async def test_missing_demo_class_is_reported_without_inventing_spbfut(self):
        self.server.responses['getClassesList'] = 'QJSIM,'
        with self.assertRaises(SnapshotError):
            await capture_snapshot(self.client, class_code='SPBFUT')
        self.assertNotIn('getSecurityInfo', [r['cmd'] for r in self.server.requests])

    async def test_table_row_order_does_not_create_a_false_difference(self):
        rows = [{'trade_num': 1, 'order_num': 42}, {'trade_num': 2, 'order_num': 42}]
        self.server.responses['get_trades'] = rows
        calls = 0
        async def reverse(request, raw):
            nonlocal calls
            if request['cmd'] == 'get_trades':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data'].reverse()
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = reverse
        self.assertTrue((await capture_snapshot(self.client))['repeatable_observation'])

    async def test_callback_during_scan_prevents_repeatable_observation(self):
        emitted = False
        async def callback(request, raw):
            nonlocal emitted
            if request['cmd'] == 'get_orders' and not emitted:
                emitted = True
                await self.server.emit({'cmd': 'OnTrade', 'data': {'trade_num': 42}})
                async with asyncio.timeout(2):
                    while self.client.callback_count == 0:
                        await asyncio.sleep(0.001)
            return raw
        self.server.override = callback
        snapshot = await capture_snapshot(self.client)
        self.assertFalse(snapshot['repeatable_observation'])
        self.assertIn('callbacks_during_scan', snapshot['issues'])

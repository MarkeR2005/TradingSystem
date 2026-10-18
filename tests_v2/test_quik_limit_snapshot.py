import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.persistence import DurableJournal
from trading_system.quik_limits import FuturesLimitKey
from trading_system.quik_readonly import ReadOnlyQuik
from trading_system.quik_snapshot import capture_snapshot, SnapshotError
from tests_v2.quik_limits_fixture import accounts, limit_rows
from tests_v2.quik_parameters_fixture import parameters, security
from tests_v2.quik_server import QuikServer


class LimitSnapshotTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = DurableJournal(Path(self.temp.name))
        self.server = await QuikServer().start()
        self.server.responses['getTradeAccounts'] = accounts()
        self.server.responses['getFuturesClientLimits'] = limit_rows()
        self.key = FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR')
        self.client = ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port, timeout=3)
        await self.client.connect()

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.close()
        self.journal.close()

    async def test_limits_reuse_existing_two_reads_without_new_lua_commands(self):
        result = await capture_snapshot(self.client, include_limits=True, limit_selection=self.key)
        self.assertTrue(result['limits']['selection_verified'])
        self.assertTrue(result['repeatable_observation'])
        self.assertFalse(result['reconciled'])
        commands = [r['cmd'] for r in self.server.requests]
        self.assertEqual(commands.count('getFuturesClientLimits'), 2)
        self.assertNotIn('getFuturesLimit', commands)
        self.assertNotIn('getParamEx', commands)

    async def test_default_snapshot_is_compatible_and_has_no_limit_analysis(self):
        result = await capture_snapshot(self.client)
        self.assertIsNone(result['limits'])
        self.assertTrue(result['repeatable_observation'])

    async def test_selection_without_limits_mode_is_rejected_before_capture(self):
        with self.assertRaisesRegex(SnapshotError, 'selection requires'):
            await capture_snapshot(self.client, limit_selection=self.key)
        self.assertNotIn('getTradeAccounts', [r['cmd'] for r in self.server.requests])

    async def test_unavailable_cash_row_leaves_a_captured_unverified_selection(self):
        self.server.responses['getFuturesClientLimits'] = limit_rows()[1:]
        result = await capture_snapshot(self.client, include_limits=True, limit_selection=self.key)
        self.assertFalse(result['repeatable_observation'])
        self.assertIn('limits_unverified', result['issues'])
        self.assertIn('selection.not_unique', result['limits']['issues'])

    async def test_changes_elsewhere_in_snapshot_withhold_selection(self):
        calls = 0
        async def change(request, raw):
            nonlocal calls
            if request['cmd'] == 'getFuturesClientHoldings':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data'] = [{'totalnet': 1}]
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = change
        result = await capture_snapshot(self.client, include_limits=True, limit_selection=self.key)
        self.assertIn('tables_changed', result['issues'])
        self.assertFalse(result['limits']['selection_verified'])

    async def test_limit_callback_during_scan_withholds_selection(self):
        sent = False
        async def emit(request, raw):
            nonlocal sent
            if request['cmd'] == 'getFuturesClientLimits' and not sent:
                sent = True
                await self.server.emit({'cmd': 'OnFuturesLimitChange', 'data': limit_rows()[0]})
                async with asyncio.timeout(2):
                    while self.client.callback_count == 0:
                        await asyncio.sleep(0.001)
            return raw
        self.server.override = emit
        result = await capture_snapshot(self.client, include_limits=True, limit_selection=self.key)
        self.assertIn('callbacks_during_scan', result['issues'])
        self.assertFalse(result['limits']['selection_verified'])

    async def enable_financial(self):
        self.server.responses['tsRuntimeInfo']['capabilities'] = ['instrument_parameters_v1']
        # Runtime info is captured at connect; reconnect a new client for this contract.
        await self.client.close()
        self.client = ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port, timeout=3)
        await self.client.connect()
        self.server.responses['getClassSecurities'] = 'SiZ6,'
        self.server.responses['getSecurityInfo'] = security()
        self.server.parameters = parameters()

    async def test_combined_limit_failure_withholds_point_value_candidate(self):
        await self.enable_financial()
        self.server.responses['getFuturesClientLimits'][0].pop('cbplimit')
        result = await capture_snapshot(self.client, class_code='SPBFUT', symbol='SiZ6', include_financial=True,
                                        include_limits=True, limit_selection=self.key)
        self.assertFalse(result['repeatable_observation'])
        self.assertFalse(result['financial']['repeatable_observation'])
        self.assertIsNone(result['financial']['point_value_candidate'])
        self.assertEqual(result['financial']['issues'].count('snapshot_unstable'), 1)

    async def test_combined_financial_failure_prevents_limit_selection_verification(self):
        await self.enable_financial()
        self.server.parameters['BUYDEPO']['result'] = '0'
        result = await capture_snapshot(self.client, class_code='SPBFUT', symbol='SiZ6', include_financial=True,
                                        include_limits=True, limit_selection=self.key)
        self.assertFalse(result['limits']['selection_verified'])
        self.assertIn('financial_parameters_incomplete', result['limits']['snapshot_issues'])
        self.assertNotIn('snapshot_unstable', result['financial']['issues'])

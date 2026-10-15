import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.persistence import DurableJournal
from trading_system.quik_readonly import ReadOnlyQuik
from trading_system.quik_snapshot import capture_snapshot, SnapshotError
from tests_v2.quik_parameters_fixture import parameters, security
from tests_v2.quik_server import QuikServer


class FinancialSnapshotTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = DurableJournal(Path(self.temp.name))
        self.server = await QuikServer().start()
        self.server.responses['tsRuntimeInfo']['capabilities'] = ['instrument_parameters_v1']
        self.server.responses['getClassSecurities'] = 'SiZ6,'
        self.server.responses['getSecurityInfo'] = security()
        self.server.parameters = parameters()
        self.client = ReadOnlyQuik(journal=self.journal, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port, timeout=3)

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.close()
        self.journal.close()

    async def capture(self):
        await self.client.connect()
        return await capture_snapshot(self.client, class_code='SPBFUT', symbol='SiZ6', include_financial=True)

    async def test_two_complete_parameter_reads_and_security_checks(self):
        snapshot = await self.capture()
        self.assertTrue(snapshot['repeatable_observation'])
        self.assertEqual(snapshot['financial']['point_value_candidate'], '1')
        self.assertEqual(snapshot['financial']['values']['lot_size'], '1000')
        commands = [r['cmd'] for r in self.server.requests]
        self.assertEqual(commands.count('getParamEx'), 10)
        self.assertEqual(commands.count('getSecurityInfo'), 2)
        self.assertFalse(snapshot['reconciled'])

    async def test_old_bridge_gets_an_upgrade_message_before_financial_requests(self):
        del self.server.responses['tsRuntimeInfo']['capabilities']
        await self.client.connect()
        with self.assertRaisesRegex(SnapshotError, 'update and restart TradingSystemReadOnly.lua'):
            await capture_snapshot(self.client, class_code='SPBFUT', symbol='SiZ6', include_financial=True)
        self.assertNotIn('getParamEx', [r['cmd'] for r in self.server.requests])

    async def test_default_probe_still_works_without_capabilities(self):
        del self.server.responses['tsRuntimeInfo']['capabilities']
        await self.client.connect()
        snapshot = await capture_snapshot(self.client, class_code='SPBFUT', symbol='SiZ6')
        self.assertIsNone(snapshot.get('financial'))
        self.assertNotIn('getParamEx', [r['cmd'] for r in self.server.requests])

    async def test_financial_probe_requires_explicit_spbfut_instrument(self):
        await self.client.connect()
        for args in ({}, {'class_code': 'SPBFUT'}, {'class_code': 'QJSIM', 'symbol': 'X'}):
            with self.subTest(args=args), self.assertRaises(SnapshotError):
                await capture_snapshot(self.client, include_financial=True, **args)
        self.assertNotIn('getParamEx', [r['cmd'] for r in self.server.requests])

    async def test_unavailable_parameter_stays_in_report_without_zero_substitution(self):
        self.server.parameters['BUYDEPO'] = {'result': '0', 'param_type': '0', 'param_value': '0', 'param_image': ''}
        snapshot = await self.capture()
        self.assertFalse(snapshot['repeatable_observation'])
        self.assertIsNone(snapshot['financial']['values']['buy_depo'])
        self.assertIsNone(snapshot['financial']['point_value_candidate'])
        self.assertEqual(snapshot['financial']['parameters']['BUYDEPO']['result'], '0')

    async def test_parameter_change_is_reported_and_candidate_withheld(self):
        calls = 0
        async def change(request, raw):
            nonlocal calls
            if request['cmd'] == 'getParamEx' and request['data'].endswith('|STEPPRICE'):
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data']['param_value'] = '2'
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = change
        result = await self.capture()
        self.assertIn('parameters_changed', result['financial']['issues'])
        self.assertIsNone(result['financial']['point_value_candidate'])

    async def test_other_snapshot_changes_also_withhold_financial_candidate(self):
        calls = 0
        async def change(request, raw):
            nonlocal calls
            if request['cmd'] == 'get_orders':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data'] = [{'order_num': 42, 'trans_id': 0}]
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = change
        result = await self.capture()
        self.assertIn('tables_changed', result['issues'])
        self.assertIsNone(result['financial']['point_value_candidate'])
        self.assertFalse(result['financial']['repeatable_observation'])

    async def test_second_security_identity_mismatch_stops_the_capture(self):
        calls = 0
        async def change(request, raw):
            nonlocal calls
            if request['cmd'] == 'getSecurityInfo':
                calls += 1
                if calls == 2:
                    obj = json.loads(raw)
                    obj['data']['code'] = 'OTHER'
                    return json.dumps(obj).encode() + b'\n'
            return raw
        self.server.override = change
        with self.assertRaises(SnapshotError):
            await self.capture()

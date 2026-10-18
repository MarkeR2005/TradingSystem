import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.quik_probe import run_probe
from tests_v2.quik_server import QuikServer
from tests_v2.quik_parameters_fixture import demo_fractional_margin_parameters, parameters, security


class ProbeTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'run'
        self.server = await QuikServer().start()

    async def asyncTearDown(self):
        await self.server.close()

    async def probe(self, **options):
        return await run_probe(self.path, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port,
            timeout=3, observe_seconds=0, **options)

    def financial_terminal(self):
        self.server.responses['tsRuntimeInfo']['capabilities'] = ['instrument_parameters_v1']
        self.server.responses['getClassSecurities'] = 'SiZ6,'
        self.server.responses['getSecurityInfo'] = security()
        self.server.parameters = parameters()

    async def test_financial_export_excludes_account_tables(self):
        self.financial_terminal()
        report, code = await self.probe(class_code='SPBFUT', symbol='SiZ6', include_financial=True)
        self.assertEqual(code, 0)
        instrument = json.loads((self.path / 'instrument.json').read_text(encoding='utf-8'))
        self.assertEqual(instrument['symbol'], 'SiZ6')
        self.assertEqual(instrument['financial']['point_value_candidate'], '1')
        self.assertFalse(instrument['reconciled'])
        self.assertNotIn('tables', instrument)
        self.assertNotIn('trdaccid', json.dumps(instrument))
        self.assertIn('tables', report['snapshot'])

    async def test_incomplete_parameters_export_raw_failure_and_exit_two(self):
        self.financial_terminal()
        self.server.parameters['CURSTEPPRICE'] = None
        report, code = await self.probe(class_code='SPBFUT', symbol='SiZ6', include_financial=True)
        self.assertEqual(code, 2, report.get('error'))
        self.assertEqual(report['status'], 'captured')
        instrument = json.loads((self.path / 'instrument.json').read_text(encoding='utf-8'))
        self.assertIsNone(instrument['financial']['parameters']['CURSTEPPRICE'])
        self.assertIsNone(instrument['financial']['point_value_candidate'])
        self.assertIn('CURSTEPPRICE.unavailable', instrument['financial']['issues'])

    async def test_demo_fractional_margin_exports_complete_without_trading_permission(self):
        self.financial_terminal()
        self.server.parameters = demo_fractional_margin_parameters()
        report, code = await self.probe(class_code='SPBFUT', symbol='SiZ6', include_financial=True)
        self.assertEqual(code, 0, report.get('error', report.get('snapshot', {}).get('issues')))
        instrument = json.loads((self.path / 'instrument.json').read_text(encoding='utf-8'))
        financial = instrument['financial']
        self.assertEqual(financial['values']['buy_depo'], '7876.05')
        self.assertEqual(financial['values']['sell_depo'], '7999.56')
        self.assertEqual(financial['point_value_candidate'], '1')
        self.assertEqual(financial['issues'], [])
        self.assertTrue(instrument['repeatable_observation'])
        self.assertEqual(financial['parameters'], self.server.parameters)
        self.assertEqual(financial['previous_parameters'], self.server.parameters)
        self.assertFalse(financial['eligible_for_trading'])
        self.assertFalse(instrument['reconciled'])
        self.assertNotIn('tables', instrument)
        commands = [r['cmd'] for r in self.server.requests]
        self.assertEqual(commands.count('getParamEx'), 10)
        self.assertNotIn('sendTransaction', commands)

    async def test_report_and_separate_wire_journal_are_saved(self):
        report, code = await self.probe()
        self.assertEqual(code, 0)
        self.assertEqual(report, json.loads((self.path / 'report.json').read_text(encoding='utf-8')))
        self.assertTrue((self.path / 'wire/events.jsonl').exists())
        self.assertFalse(report['snapshot']['reconciled'])
        self.assertNotIn('sendTransaction', [r['cmd'] for r in self.server.requests])

    async def test_wrong_terminal_leaves_failure_report_and_no_snapshot(self):
        self.server.responses['tsRuntimeInfo']['profile'] = 'psb-live'
        report, code = await self.probe()
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('snapshot', report)
        self.assertIn('profile expected="demo-quik" observed="psb-live"', report['error'])
        self.assertEqual(report, json.loads((self.path / 'report.json').read_text(encoding='utf-8')))
        self.assertFalse(report['reconciled'])
        self.assertEqual([r['cmd'] for r in self.server.requests], ['tsRuntimeInfo'])

    async def test_stock_bridge_reports_missing_guard_and_stops_at_first_request(self):
        async def stock_bridge(request, raw):
            return (json.dumps({**request, 'cmd': 'lua_error', 'lua_error':
                'Command not implemented in Lua qsfunctions module: tsRuntimeInfo'}) + '\n').encode()
        self.server.override = stock_bridge
        report, code = await self.probe()
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'failed')
        self.assertIn('tsRuntimeInfo is not supported by the connected bridge', report['error'])
        self.assertIn('TradingSystemReadOnly.lua', report['error'])
        self.assertNotIn('snapshot', report)
        self.assertFalse(report['reconciled'])
        self.assertEqual([r['cmd'] for r in self.server.requests], ['tsRuntimeInfo'])
        self.assertEqual(report, json.loads((self.path / 'report.json').read_text(encoding='utf-8')))

    async def test_existing_report_is_never_overwritten(self):
        self.path.mkdir()
        file = self.path / 'report.json'
        file.write_text('existing')
        with self.assertRaises(FileExistsError):
            await self.probe()
        self.assertEqual(file.read_text(), 'existing')
        self.assertEqual(self.server.requests, [])

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system.quik_limits import FuturesLimitKey
from trading_system.quik_probe import main, run_probe
from tests_v2.quik_limits_fixture import accounts, limit_rows
from tests_v2.quik_parameters_fixture import parameters, security
from tests_v2.quik_server import QuikServer


class LimitProbeTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'run'
        self.server = await QuikServer().start()
        self.server.responses['getTradeAccounts'] = accounts()
        self.server.responses['getFuturesClientLimits'] = limit_rows()

    async def asyncTearDown(self):
        await self.server.close()

    async def probe(self, **options):
        return await run_probe(self.path, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port,
            timeout=3, observe_seconds=0, **options)

    def saved(self, name):
        return json.loads((self.path / name).read_text(encoding='utf-8'))

    async def test_discovery_exports_aliases_and_keeps_keys_in_separate_local_file(self):
        self.server.responses['getFuturesClientLimits'][0]['client_code'] = 'SENSITIVE_EXTRA'
        report, code = await self.probe(include_limits=True)
        self.assertEqual(code, 0, report.get('error'))
        exported = self.saved('limits.json')
        local = self.saved('limit-keys.local.json')
        self.assertEqual(len(exported['limits']['rows']), 4)
        self.assertEqual(len(local['keys']), 4)
        for private in ('FUTURES', 'DEMO', 'SENSITIVE_EXTRA', 'firmid', 'trdaccid', 'client_code'):
            self.assertNotIn(private, json.dumps(exported))
        self.assertEqual({r['key']['account_ref'] for r in exported['limits']['rows']}, {'account-1'})
        self.assertEqual(local['keys'][0]['firmid'], 'DEMO')
        self.assertEqual(local['keys'][0]['account_ref'], 'account-1')
        self.assertFalse(exported['limits']['selection_verified'])
        self.assertFalse(exported['reconciled'])
        self.assertNotIn('tables', exported)
        self.assertFalse((self.path / 'instrument.json').exists())

    async def test_selected_key_and_both_passes_are_redacted_consistently(self):
        report, code = await self.probe(include_limits=True,
            limit_selection=FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR'))
        self.assertEqual(code, 0, report.get('error'))
        limits = self.saved('limits.json')['limits']
        selected = limits['selected_observation']
        self.assertTrue(limits['selection_verified'])
        self.assertEqual(selected['key'], limits['selection'])
        self.assertEqual(limits['rows'], limits['previous_rows'])
        self.assertEqual(selected['values']['cbplimit'], '100000.25')
        self.assertFalse(limits['eligible_for_capital'])
        self.assertFalse(limits['eligible_for_trading'])

    async def test_missing_selection_is_exported_with_exit_two_without_fallback(self):
        report, code = await self.probe(include_limits=True,
            limit_selection=FuturesLimitKey('DEMO', 'OTHER', 0, 'SUR'))
        self.assertEqual(code, 2, report.get('error'))
        limits = self.saved('limits.json')['limits']
        self.assertIsNone(limits['selected_observation'])
        self.assertIn('selection.not_unique', limits['issues'])
        self.assertNotIn('OTHER', json.dumps(limits))

    async def test_same_account_name_at_different_firms_gets_distinct_aliases(self):
        account = {**accounts()[0], 'firmid': 'SECOND'}
        row = {**limit_rows()[0], 'firmid': 'SECOND'}
        self.server.responses['getTradeAccounts'].append(account)
        self.server.responses['getFuturesClientLimits'].append(row)
        report, code = await self.probe(include_limits=True)
        self.assertEqual(code, 0, report.get('error'))
        limits = self.saved('limits.json')['limits']
        cash = [r for r in limits['rows'] if r['key']['limit_type'] == 0]
        self.assertEqual({r['key']['account_ref'] for r in cash}, {'account-1', 'account-2'})
        keys = self.saved('limit-keys.local.json')['keys']
        self.assertEqual({k['account_ref']: k['firmid'] for k in keys},
                         {'account-1': 'DEMO', 'account-2': 'SECOND'})
        self.assertNotIn('SECOND', json.dumps(limits))

    async def test_selection_requires_limits_before_output_or_network_side_effects(self):
        with self.assertRaisesRegex(ValueError, 'selection requires'):
            await self.probe(limit_selection=FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR'))
        self.assertFalse(self.path.exists())
        self.assertEqual(self.server.requests, [])

    async def test_default_probe_does_not_create_limit_files(self):
        report, code = await self.probe()
        self.assertEqual(code, 0, report.get('error'))
        self.assertFalse((self.path / 'limits.json').exists())
        self.assertFalse((self.path / 'limit-keys.local.json').exists())

    async def test_live_profile_with_plus_stock_account_keeps_futures_observation_complete(self):
        self.server.responses['getTradeAccounts'][1]['trdaccid'] = 'STOCK+SETTLEMENT'
        self.server.responses['tsRuntimeInfo'].update(profile='psb-live', quik_version='13.0.2.20')
        self.server.responses['getFuturesClientLimits'] = [r for r in limit_rows() if r['limit_type'] != 3]
        report, code = await run_probe(self.path, profile='psb-live', expected_version='13.0.2.20',
            request_port=self.server.request_port, callback_port=self.server.callback_port,
            timeout=3, observe_seconds=0, include_limits=True)
        self.assertEqual(code, 0, report.get('snapshot', {}).get('issues'))
        result = self.saved('limits.json')
        self.assertEqual(result['runtime']['profile'], 'psb-live')
        self.assertTrue(result['limits']['complete'])
        self.assertEqual(result['limits']['issues'], [])
        self.assertEqual(len(result['limits']['rows']), 3)
        self.assertNotIn('STOCK+SETTLEMENT', json.dumps(result))
        self.assertFalse(result['limits']['eligible_for_capital'])

    async def test_plus_account_fix_does_not_hide_zero_buy_margin(self):
        self.server.responses['getTradeAccounts'][1]['trdaccid'] = 'STOCK+SETTLEMENT'
        self.server.responses['tsRuntimeInfo']['capabilities'] = ['instrument_parameters_v1']
        self.server.responses['getClassSecurities'] = 'SiZ6,'
        self.server.responses['getSecurityInfo'] = security()
        self.server.parameters = parameters()
        self.server.parameters['BUYDEPO'].update(result='1', param_type='2', param_value='0.000000', param_image='')
        report, code = await self.probe(class_code='SPBFUT', symbol='SiZ6',
                                        include_financial=True, include_limits=True)
        self.assertEqual(code, 2, report.get('error'))
        result = self.saved('limits.json')['limits']
        self.assertTrue(result['complete'])
        self.assertEqual(result['issues'], ['snapshot_unverified'])
        self.assertFalse(result['repeatable_observation'])
        financial = self.saved('instrument.json')['financial']
        self.assertEqual(financial['issues'], ['BUYDEPO.nonpositive_value'])
        self.assertIsNone(financial['values']['buy_depo'])
        self.assertIsNone(financial['point_value_candidate'])

    async def test_invalid_identity_is_not_leaked_by_export(self):
        self.server.responses['getFuturesClientLimits'][0]['firmid'] = {'private': 'SECRET'}
        report, code = await self.probe(include_limits=True)
        self.assertEqual(code, 2, report.get('error'))
        self.assertNotIn('SECRET', json.dumps(self.saved('limits.json')))
        self.assertTrue(any(r['key'] is None for r in self.saved('limits.json')['limits']['rows']))

    async def test_cli_rejects_partial_selection_before_async_runner(self):
        argv = ['probe', '--profile', 'demo-quik', '--expect-version', '13.2', '--output', str(self.path),
                '--limits', '--limit-type', '0']
        stderr = io.StringIO()
        with patch('sys.argv', argv), patch('sys.stderr', stderr), \
                patch('trading_system.quik_probe.asyncio.run') as runner:
            with self.assertRaises(SystemExit) as error:
                main()
        self.assertEqual(error.exception.code, 2)
        self.assertIn('all four limit key options', stderr.getvalue())
        runner.assert_not_called()

    async def test_cli_passes_exact_complete_key_including_zero_type(self):
        argv = ['probe', '--profile', 'demo-quik', '--expect-version', '13.2', '--output', str(self.path),
                '--limits', '--limit-firm', 'DEMO', '--limit-account', 'FUTURES',
                '--limit-type', '0', '--limit-currency', 'SUR']
        report = {'status': 'failed', 'error': 'synthetic offline diagnostic'}
        # Consume the coroutine without opening a terminal inside this running event loop.
        def close_coroutine(coroutine):
            coroutine.close()
            return report, 1
        with patch('sys.argv', argv), patch('sys.stdout', io.StringIO()), patch('sys.stderr', io.StringIO()), \
                patch('trading_system.quik_probe.run_probe') as probe, \
                patch('trading_system.quik_probe.asyncio.run', side_effect=close_coroutine):
            self.assertEqual(main(), 1)
        self.assertEqual(probe.call_args.kwargs['limit_selection'], FuturesLimitKey('DEMO', 'FUTURES', 0, 'SUR'))
        self.assertTrue(probe.call_args.kwargs['include_limits'])

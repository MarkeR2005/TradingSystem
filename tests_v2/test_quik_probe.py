import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase

from trading_system.quik_probe import run_probe
from tests_v2.quik_server import QuikServer


class ProbeTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'run'
        self.server = await QuikServer().start()

    async def asyncTearDown(self):
        await self.server.close()

    async def probe(self):
        return await run_probe(self.path, profile='demo-quik', expected_version='13.2',
            request_port=self.server.request_port, callback_port=self.server.callback_port,
            timeout=0.3, observe_seconds=0)

    async def test_report_and_separate_wire_journal_are_saved(self):
        report, code = await self.probe()
        self.assertEqual(code, 0)
        self.assertEqual(report, json.loads((self.path / 'report.json').read_text()))
        self.assertTrue((self.path / 'wire/events.jsonl').exists())
        self.assertFalse(report['snapshot']['reconciled'])
        self.assertNotIn('sendTransaction', [r['cmd'] for r in self.server.requests])

    async def test_wrong_terminal_leaves_failure_report_and_no_snapshot(self):
        self.server.responses['tsRuntimeInfo']['profile'] = 'psb-live'
        report, code = await self.probe()
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'failed')
        self.assertNotIn('snapshot', report)
        self.assertTrue((self.path / 'report.json').exists())

    async def test_existing_report_is_never_overwritten(self):
        self.path.mkdir()
        file = self.path / 'report.json'
        file.write_text('existing')
        with self.assertRaises(FileExistsError):
            await self.probe()
        self.assertEqual(file.read_text(), 'existing')
        self.assertEqual(self.server.requests, [])

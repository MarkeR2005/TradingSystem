from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from trading_system import codec
from trading_system.domain import GatewayUncertain
from trading_system.ledger import PositionLedger
from trading_system.orders import OrderManager
from trading_system.persistence import DurableJournal, JournalError
from trading_system.time import ManualClock
from tests_v2.helpers import START
from tests_v2.test_admission import Recorder, policy


class GatewayUncertaintyTests(IsolatedAsyncioTestCase):
    async def test_disconnection_without_orders_survives_restart_and_requires_fresh_capital(self):
        with TemporaryDirectory() as directory:
            with DurableJournal(Path(directory)) as journal:
                oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
                oms.gateway_ids.add('sim')
                await oms.handle(GatewayUncertain('sim', 'callback stream lost'))
                self.assertFalse(oms.ready)
            with DurableJournal(Path(directory)) as journal:
                oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
                await oms.restore()
                self.assertFalse(oms.ready)
                oms.gateway_ids.add('sim')
                await oms.configure_capital(policy())
                self.assertFalse(oms.ready)
                await oms.reconcile_gateway('sim', ())
                self.assertTrue(oms.ready)
                await oms.handle(GatewayUncertain('sim', 'lost again'))
                await oms.reconcile_gateway('sim', ())
                self.assertFalse(oms.ready)
            with DurableJournal(Path(directory)) as journal:
                oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
                await oms.restore()
                self.assertFalse(oms.ready)

    async def test_uncertainty_and_codec_reject_unknown_gateway_without_mutation(self):
        oms = OrderManager(Recorder(), ManualClock(START), PositionLedger())
        command = GatewayUncertain('unknown', 'gap')
        self.assertEqual(codec.decode(codec.encode(command)), command)
        with self.assertRaises(ValueError):
            await oms.handle(command)
        self.assertEqual(oms.revision, 0)
        self.assertTrue(oms.ready)

    async def test_uncertainty_journal_failure_is_atomic(self):
        with TemporaryDirectory() as directory, DurableJournal(Path(directory)) as journal:
            oms = OrderManager(Recorder(), ManualClock(START), PositionLedger(), journal=journal)
            oms.gateway_ids.add('sim')
            with patch.object(journal, 'append', side_effect=JournalError('disk failure')):
                with self.assertRaises(JournalError):
                    await oms.handle(GatewayUncertain('sim', 'gap'))
            self.assertEqual(oms.revision, 0)
            self.assertEqual(oms.bus.events, [])

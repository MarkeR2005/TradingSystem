from dataclasses import replace
import unittest

from trading_system.domain import Instrument, InstrumentId, Route, Side
from trading_system.ledger import PositionLedger
from tests_v2.helpers import INSTRUMENT, ROUTE, execution


class LedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = PositionLedger()

    def test_average_cost_partial_close_and_cumulative_equity(self) -> None:
        for fill in (execution('1', Side.BUY, 2, 100, 2),
                     execution('2', Side.BUY, 2, 120, 2)):
            self.ledger.apply('s', fill)
        self.assertEqual(self.ledger.position('s', ROUTE).average_price, 110)
        self.ledger.apply('s', execution('3', Side.SELL, 1, 130, 1))
        self.assertEqual(self.ledger.position('s', ROUTE).quantity, 3)
        self.assertEqual(self.ledger.position('s', ROUTE).average_price, 110)
        self.assertEqual(self.ledger.equity('s'), ())
        self.ledger.apply('s', execution('4', Side.SELL, 3, 90, 3))
        self.assertEqual(self.ledger.trades('s')[0].pnl, -48)
        self.ledger.apply('s', execution('5', Side.SELL, 1, 100))
        self.ledger.apply('s', execution('6', Side.BUY, 1, 90))
        self.assertEqual([point.value for point in self.ledger.equity('s')], [-48, -38])

    def test_equity_waits_until_every_leg_is_flat(self) -> None:
        second = Route(Instrument(InstrumentId('sim', 'SPBFUT', 'SECOND'), 'RUB'), 'demo')
        self.ledger.apply('basket', execution('1', Side.BUY, 2, 100, route=ROUTE))
        self.ledger.apply('basket', execution('2', Side.SELL, 1, 200, route=second))
        self.ledger.apply('basket', execution('3', Side.SELL, 2, 110, route=ROUTE))
        self.assertEqual(self.ledger.equity('basket'), ())
        self.ledger.apply('basket', execution('4', Side.BUY, 1, 205, route=second))
        self.assertEqual(self.ledger.trades('basket')[0].pnl, 15)

    def test_reversal_splits_commission_between_closed_and_new_cycles(self) -> None:
        self.ledger.apply('s', execution('1', Side.BUY, 2, 100, 2))
        self.ledger.apply('s', execution('2', Side.SELL, 3, 110, 3))
        self.assertEqual(self.ledger.position('s', ROUTE).quantity, -1)
        self.assertEqual(self.ledger.trades('s')[0].pnl, 16)
        self.ledger.apply('s', execution('3', Side.BUY, 1, 105, 1))
        self.assertEqual([trade.pnl for trade in self.ledger.trades('s')], [16, 3])
        self.assertEqual(self.ledger.equity('s')[-1].value, 19)

    def test_reversal_keeps_basket_cycle_open_if_another_leg_is_open(self) -> None:
        second = Route(INSTRUMENT, 'second-account')
        self.ledger.apply('s', execution('1', Side.BUY, 1, 100))
        self.ledger.apply('s', execution('2', Side.BUY, 1, 100, route=second))
        self.ledger.apply('s', execution('3', Side.SELL, 2, 110))
        self.assertEqual(self.ledger.trades('s'), ())
        self.ledger.apply('s', execution('4', Side.SELL, 1, 110, route=second))
        self.ledger.apply('s', execution('5', Side.BUY, 1, 105))
        self.assertEqual(self.ledger.trades('s')[0].pnl, 25)

    def test_opposing_strategies_keep_independent_virtual_positions(self) -> None:
        self.ledger.apply('long', execution('1', Side.BUY, 1, 100))
        self.ledger.apply('short', execution('2', Side.SELL, 1, 100))
        self.assertEqual(self.ledger.position('long', ROUTE).quantity, 1)
        self.assertEqual(self.ledger.position('short', ROUTE).quantity, -1)
        self.assertEqual(self.ledger.equity('long'), ())

    def test_duplicate_is_idempotent_but_conflicting_identifier_fails(self) -> None:
        fill = execution('1', Side.BUY, 1, 100)
        self.assertTrue(self.ledger.apply('s', fill))
        self.assertFalse(self.ledger.apply('s', fill))
        with self.assertRaises(ValueError):
            self.ledger.apply('s', replace(fill, price=101))
        with self.assertRaises(ValueError):
            self.ledger.apply('other', fill)
        self.assertEqual(self.ledger.position('s', ROUTE).quantity, 1)

    def test_contract_multiplier_applies_to_pnl(self) -> None:
        route = Route(replace(INSTRUMENT, contract_multiplier=10), 'demo')
        self.ledger.apply('s', execution('1', Side.SELL, 2, 100, route=route))
        self.ledger.apply('s', execution('2', Side.BUY, 2, 95, route=route))
        self.assertEqual(self.ledger.trades('s')[0].pnl, 100)

    def test_foreign_currency_rejected_before_position_changes(self) -> None:
        route = Route(replace(INSTRUMENT, currency='USD'), 'demo')
        with self.assertRaises(ValueError):
            self.ledger.apply('s', execution('1', Side.BUY, 1, 100, route=route))
        self.assertEqual(self.ledger.position('s', route).quantity, 0)

    def test_decimal_lots_close_cycle_without_float_residue(self) -> None:
        route = Route(replace(INSTRUMENT, quantity_step=0.1), 'demo')
        self.ledger.apply('s', execution('1', Side.BUY, 0.1, 100, route=route))
        self.ledger.apply('s', execution('2', Side.BUY, 0.2, 100, route=route))
        self.ledger.apply('s', execution('3', Side.SELL, 0.3, 110, route=route))
        self.assertEqual(self.ledger.position('s', route).quantity, 0)
        self.assertEqual(len(self.ledger.trades('s')), 1)
        self.assertAlmostEqual(self.ledger.equity('s')[0].value, 3)

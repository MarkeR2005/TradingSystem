from datetime import datetime, timedelta, timezone
import unittest

from trading_system.domain import OrderIntent, Side
from trading_system.time import ManualClock
from tests_v2.helpers import ROUTE, START, candle
from dataclasses import replace


class TimeDomainTests(unittest.TestCase):
    def test_clock_rejects_naive_time_and_backwards_movement(self) -> None:
        with self.assertRaises(ValueError):
            ManualClock(datetime(2026, 1, 1))
        clock = ManualClock(START)
        with self.assertRaises(ValueError):
            clock.advance_to(START - timedelta(seconds=1))
        self.assertEqual(clock.now(), START)

    def test_candle_timestamp_normalized_to_utc_and_marks_open(self) -> None:
        bar = replace(candle(), opened_at=START.astimezone(timezone(timedelta(hours=3))))
        self.assertEqual(bar.opened_at, START)
        self.assertEqual(bar.closed_at, START + timedelta(minutes=1))
        self.assertEqual(bar.opened_at.utcoffset(), timedelta(0))

    def test_inconsistent_or_nonfinite_candle_rejected(self) -> None:
        for changes in ({'high': 98}, {'low': 102}, {'volume': -1},
                        {'close': float('nan')}, {'timeframe': timedelta(0)}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(candle(), **changes)

    def test_nonpositive_or_nonfinite_order_quantity_rejected(self) -> None:
        for quantity in (0, -1, float('nan'), float('inf')):
            with self.subTest(quantity=quantity), self.assertRaises(ValueError):
                OrderIntent(ROUTE, Side.BUY, quantity)

"""Shared completed bars, explicit corrections and calendar-supplied gaps."""

from datetime import datetime, timedelta

from .domain import Candle, CandleCorrected, CandleReceived, InstrumentId, finite, utc
from .optimization import HistoryRequest
from .time import Clock


type CandleKey = tuple[InstrumentId, timedelta, datetime]


def candle_key(bar: Candle) -> CandleKey:
    return bar.instrument_id, bar.timeframe, bar.opened_at


def candle_sort_key(bar: Candle) -> tuple[datetime, str, str, str]:
    instrument = bar.instrument_id
    return bar.opened_at, instrument.gateway_id, instrument.venue, instrument.symbol


class CandleBook:
    """One immutable candle per key; only data already known to the clock."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self._bars: dict[CandleKey, Candle] = {}
        self._last_close: dict[tuple[InstrumentId, timedelta], datetime] = {}

    def get(self, bar: Candle) -> Candle | None:
        return self._bars.get(candle_key(bar))

    def validate(self, bar: Candle, *, as_of: datetime | None = None) -> None:
        if bar.closed_at > (self.clock.now() if as_of is None else utc(as_of)):
            raise ValueError('cannot store future data')
        if candle_key(bar) in self._bars:
            return
        series = bar.instrument_id, bar.timeframe
        last = self._last_close.get(series)
        if last is not None and bar.opened_at < last:
            raise ValueError('unknown candle is out of order or overlaps an existing bar')

    def put(self, bar: Candle) -> CandleReceived | CandleCorrected | None:
        self.validate(bar)
        key = candle_key(bar)
        previous = self._bars.get(key)
        if previous == bar:
            return None
        self._bars[key] = bar
        if previous is not None:
            return CandleCorrected(previous, bar)
        series = bar.instrument_id, bar.timeframe
        self._last_close[series] = bar.closed_at
        return CandleReceived(bar)

    async def load(self, request: HistoryRequest) -> tuple[Candle, ...]:
        if request.end > self.clock.now():
            raise ValueError('history cannot include future data')
        return tuple(sorted((bar for bar in self._bars.values()
                             if bar.instrument_id in request.instruments and bar.timeframe == request.timeframe
                             and bar.opened_at >= request.start and bar.closed_at <= request.end),
                            key=candle_sort_key))


def fill_gaps(instrument: InstrumentId, timeframe: timedelta,
              expected_openings: tuple[datetime, ...], candles: tuple[Candle, ...], *,
              seed: float | None = None) -> tuple[Candle, ...]:
    """Expected openings come from an explicit calendar; never infer sessions."""
    if timeframe <= timedelta(0):
        raise ValueError('timeframe must be positive')
    if seed is not None:
        finite(seed, 'seed')
    openings = tuple(utc(item) for item in expected_openings)
    if any(right < left + timeframe for left, right in zip(openings, openings[1:])):
        raise ValueError('expected openings must be ordered, unique and nonoverlapping')
    expected = set(openings)
    actual: dict[datetime, Candle] = {}
    for existing in candles:
        if (existing.instrument_id != instrument or existing.timeframe != timeframe
                or existing.opened_at not in expected or existing.opened_at in actual):
            raise ValueError('history does not match expected openings')
        actual[existing.opened_at] = existing
    result: list[Candle] = []
    previous = seed
    for opened_at in openings:
        bar = actual.get(opened_at)
        if bar is None:
            if previous is None:
                raise ValueError('initial missing bar requires a seed price')
            bar = Candle(instrument, opened_at, timeframe, previous, previous, previous, previous, 0,
                         synthetic=True)
        previous = bar.close
        result.append(bar)
    return tuple(result)

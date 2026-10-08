from datetime import UTC, datetime, timedelta

from trading_system.domain import Candle, Execution, Instrument, InstrumentId, Route, Side
from trading_system.strategy import AutoStrategy, StrategyContext


START = datetime(2026, 1, 1, 10, tzinfo=UTC)
INSTRUMENT = Instrument(InstrumentId('sim', 'SPBFUT', 'TEST'), 'RUB')
ROUTE = Route(INSTRUMENT, 'demo')


def execution(identifier: str, side: Side, quantity: float, price: float,
              commission: float = 0, route: Route = ROUTE) -> Execution:
    return Execution(identifier, 'order-' + identifier, route, side, quantity,
                     price, commission, START)


def candle(opened_at: datetime = START) -> Candle:
    return Candle(INSTRUMENT.id, opened_at, timedelta(minutes=1), 100, 101, 99, 100, 10)


class PassiveStrategy(AutoStrategy):
    def __init__(self, name: str = 'passive') -> None:
        super().__init__(name, (INSTRUMENT.id,))
        self.fills: list[Execution] = []

    async def on_candle(self, context: StrategyContext, candle: Candle) -> None:
        pass

    async def on_fill(self, context: StrategyContext, execution: Execution) -> None:
        self.fills.append(execution)

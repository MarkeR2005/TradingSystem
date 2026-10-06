from abc import ABC, abstractmethod
from collections.abc import Callable

from .domain import (
    Candle, CancelOrder, EquityPoint, Execution, InstrumentId, OrderIntent,
    OrderSnapshot, Position, ReplaceOrder, Route, SubmitOrder, Trade,
)
from .events import EventBus
from .ledger import PositionLedger
from .orders import OrderManager
from .time import Clock


class StrategyContext:
    def __init__(self, strategy_id: str, bus: EventBus, clock: Clock,
                 ledger: PositionLedger, orders: OrderManager, can_trade: Callable[[], bool],
                 can_cancel: Callable[[], bool] | None = None) -> None:
        self.strategy_id = strategy_id
        self.clock = clock
        self._bus = bus
        self._ledger = ledger
        self._orders = orders
        self._can_trade = can_trade
        self._can_cancel = can_cancel if can_cancel is not None else can_trade

    async def place_order(self, intent: OrderIntent) -> None:
        if not self._can_trade():
            raise RuntimeError('strategy is not running')
        await self._bus.publish('orders', SubmitOrder(self.strategy_id, intent))

    async def cancel_order(self, order_id: str) -> None:
        if not self._can_cancel():
            raise RuntimeError('runtime is not active')
        await self._bus.publish('orders', CancelOrder(self.strategy_id, order_id))

    async def replace_order(self, order_id: str, intent: OrderIntent) -> None:
        """Cancel first; intent.quantity is the target total for the old order."""
        if not self._can_trade():
            raise RuntimeError('strategy is not running')
        await self._bus.publish('orders', ReplaceOrder(self.strategy_id, order_id, intent))

    def position(self, route: Route) -> Position:
        return self._ledger.position(self.strategy_id, route)

    def orders(self) -> tuple[OrderSnapshot, ...]:
        return self._orders.orders(self.strategy_id)

    def trades(self) -> tuple[Trade, ...]:
        return self._ledger.trades(self.strategy_id)

    def equity(self) -> tuple[EquityPoint, ...]:
        return self._ledger.equity(self.strategy_id)


class AutoStrategy(ABC):
    def __init__(self, name: str, instruments: tuple[InstrumentId, ...]) -> None:
        if not name:
            raise ValueError('strategy name must not be empty')
        self.name = name
        self.instruments = instruments

    @abstractmethod
    async def on_candle(self, context: StrategyContext, candle: Candle) -> None: ...

    async def on_start(self, context: StrategyContext) -> None:
        pass

    async def on_fill(self, context: StrategyContext, execution: Execution) -> None:
        pass

    async def on_order(self, context: StrategyContext, order: OrderSnapshot) -> None:
        pass

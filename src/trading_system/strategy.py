from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from .domain import (
    Candle, CancelOrder, EquityPoint, Execution, InstrumentId, OrderIntent,
    OrderSnapshot, Position, ReplaceOrder, Route, SubmitOrder, Trade,
)
from .events import EventBus
from .ledger import PositionLedger
from .lifecycle import StrategyStatus
from .optimization import HistoryRequest, OptimizationFunction, OptimizationHandle
from .state import JsonObject, json_object
from .orders import OrderManager
from .time import Clock


class StrategyContext:
    def __init__(self, strategy_id: str, bus: EventBus, clock: Clock,
                 ledger: PositionLedger, orders: OrderManager, can_trade: Callable[[], bool],
                 can_cancel: Callable[[], bool] | None = None, *,
                 get_status: Callable[[], StrategyStatus] | None = None,
                 begin_optimization: Callable[[HistoryRequest, OptimizationFunction], Awaitable[OptimizationHandle]] | None = None) -> None:
        self.strategy_id = strategy_id
        self.clock = clock
        self._bus = bus
        self._ledger = ledger
        self._orders = orders
        self._can_trade = can_trade
        self._get_status = get_status
        self._begin_optimization = begin_optimization
        self._can_cancel = can_cancel if can_cancel is not None else can_trade

    @property
    def status(self) -> StrategyStatus:
        return self._get_status() if self._get_status is not None else StrategyStatus.RUNNING

    async def start_optimization(self, request: HistoryRequest,
                                 worker: OptimizationFunction) -> OptimizationHandle:
        if self._begin_optimization is None:
            raise RuntimeError('optimization service is not configured')
        return await self._begin_optimization(request, worker)

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
    state_version = 1

    def __init__(self, name: str, instruments: tuple[InstrumentId, ...], *,
                 parameters: JsonObject | None = None) -> None:
        if not name:
            raise ValueError('strategy name must not be empty')
        self.name = name
        self.instruments = instruments
        self._parameters = json_object(parameters if parameters is not None else {})

    @property
    def parameters(self) -> JsonObject:
        return json_object(self._parameters)

    def apply_parameters(self, parameters: JsonObject) -> None:
        self._parameters = json_object(parameters)

    def save_state(self) -> JsonObject:
        return {}

    def restore_state(self, state: JsonObject) -> None:
        if state:
            raise ValueError('strategy must implement restore_state for nonempty state')

    async def on_restore(self, context: StrategyContext) -> None:
        """Rebuild derived objects; trading stays disabled during this callback."""
        pass

    @abstractmethod
    async def on_candle(self, context: StrategyContext, candle: Candle) -> None: ...

    async def on_start(self, context: StrategyContext) -> None:
        pass

    async def on_fill(self, context: StrategyContext, execution: Execution) -> None:
        pass

    async def on_order(self, context: StrategyContext, order: OrderSnapshot) -> None:
        pass

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from .domain import (
    Candle, CancelOrder, EquityPoint, Execution, InstrumentId, OrderIntent,
    OrderSnapshot, OwnershipSettlement, DeficitUpdated, PlanUpdated, Position, PositionChanged, ReplaceOrder, Route, SubmitOrder, Trade,
)
from .events import EventBus
from .ledger import PositionLedger
from .lifecycle import StrategyStatus
from .optimization import HistoryRequest, OptimizationFunction, OptimizationHandle
from .state import JsonObject, json_object
from .orders import OrderManager
from .time import Clock
from .plans import ClosePlan, PlanSnapshot, SubmitPlan, TradePlan


class StrategyContext:
    def __init__(self, strategy_id: str, bus: EventBus, clock: Clock,
                 ledger: PositionLedger, orders: OrderManager, can_trade: Callable[[], bool],
                 can_cancel: Callable[[], bool] | None = None, *,
                 get_status: Callable[[], StrategyStatus] | None = None,
                 instruments: tuple[InstrumentId, ...] = (),
                 begin_optimization: Callable[[HistoryRequest, OptimizationFunction], Awaitable[OptimizationHandle]] | None = None,
                 load_history: Callable[[HistoryRequest], Awaitable[tuple[Candle, ...]]] | None = None) -> None:
        self.strategy_id = strategy_id
        self.clock = clock
        self._bus = bus
        self._ledger = ledger
        self._orders = orders
        self._can_trade = can_trade
        self._get_status = get_status
        self._instruments = instruments
        self._begin_optimization = begin_optimization
        self._load_history = load_history
        self._can_cancel = can_cancel if can_cancel is not None else can_trade

    @property
    def status(self) -> StrategyStatus:
        return self._get_status() if self._get_status is not None else StrategyStatus.RUNNING

    async def start_optimization(self, request: HistoryRequest,
                                 worker: OptimizationFunction) -> OptimizationHandle:
        if self._begin_optimization is None:
            raise RuntimeError('optimization service is not configured')
        return await self._begin_optimization(request, worker)

    async def load_history(self, request: HistoryRequest) -> tuple[Candle, ...]:
        if self._load_history is None:
            raise RuntimeError('history service is not configured')
        return await self._load_history(request)

    async def place_plan(self, plan: TradePlan) -> None:
        if not self._can_trade():
            raise RuntimeError('strategy is not running')
        if plan.strategy_id != self.strategy_id or any(
                leg.intent.route.instrument.id not in self._instruments for leg in plan.legs):
            raise ValueError('plan must belong to this strategy and its declared instruments')
        await self._bus.publish('orders', SubmitPlan(plan))

    async def close_plan(self, plan_id: str) -> None:
        if not self._can_cancel():
            raise RuntimeError('runtime is not active')
        await self._bus.publish('orders', ClosePlan(self.strategy_id, plan_id))

    def plan(self, plan_id: str) -> PlanSnapshot:
        result = self._orders.plan(plan_id)
        if result.plan.strategy_id != self.strategy_id:
            raise ValueError('strategy does not own plan')
        return result

    async def place_order(self, intent: OrderIntent) -> None:
        if not self._can_trade() or self._orders.has_active_plan(self.strategy_id):
            raise RuntimeError('strategy is not running')
        await self._bus.publish('orders', SubmitOrder(self.strategy_id, intent))

    async def cancel_order(self, order_id: str) -> None:
        if not self._can_cancel():
            raise RuntimeError('runtime is not active')
        await self._bus.publish('orders', CancelOrder(self.strategy_id, order_id))

    async def replace_order(self, order_id: str, intent: OrderIntent) -> None:
        """Cancel first; intent.quantity is the target total for the old order."""
        if not self._can_trade() or self._orders.has_active_plan(self.strategy_id):
            raise RuntimeError('strategy is not running or has an active plan')
        await self._bus.publish('orders', ReplaceOrder(self.strategy_id, order_id, intent))

    def position(self, route: Route) -> Position:
        return self._ledger.position(self.strategy_id, route)

    def orders(self) -> tuple[OrderSnapshot, ...]:
        return self._orders.orders(self.strategy_id)

    def trades(self) -> tuple[Trade, ...]:
        return self._ledger.trades(self.strategy_id)

    def settlements(self) -> tuple[OwnershipSettlement, ...]:
        return self._ledger.settlements(self.strategy_id)

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

    def accepts_candle(self, candle: Candle) -> bool:
        """Override to filter timeframes; instrument membership remains mandatory."""
        return candle.instrument_id in self.instruments

    async def on_correction(self, context: StrategyContext, previous: Candle, corrected: Candle) -> None:
        """Rebuild derived data separately from the normal trading signal callback."""
        pass

    @abstractmethod
    async def on_candle(self, context: StrategyContext, candle: Candle) -> None: ...

    async def on_start(self, context: StrategyContext) -> None:
        pass

    async def on_fill(self, context: StrategyContext, execution: Execution) -> None:
        pass

    async def on_order(self, context: StrategyContext, order: OrderSnapshot) -> None:
        pass

    async def on_position_change(self, context: StrategyContext, change: PositionChanged) -> None:
        """Rebuild position-derived state after ownership changes, including while paused."""
        pass

    async def on_plan(self, context: StrategyContext, update: PlanUpdated) -> None:
        """Progress and durable alerts; plan execution does not depend on this callback."""
        pass

    async def on_deficit(self, context: StrategyContext, update: DeficitUpdated) -> None:
        """Durable account-risk status; reduction does not depend on this callback."""
        pass

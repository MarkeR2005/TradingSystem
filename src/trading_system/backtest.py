"""A deterministic single-strategy OHLC run using the ordinary OMS and ledger."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from itertools import count

from .domain import (
    Candle, EquityPoint, Execution, Instrument, InstrumentId, OrderSnapshot, OrderStatus,
    Position, Side, Trade, finite, utc,
)
from .events import EventBus
from .market_data import candle_sort_key
from .persistence import canonical
from .runtime import TradingRuntime
from .simulation import SimulationGateway
from .strategy import AutoStrategy
from .time import ManualClock


class HistoricalGateway:
    """Full-quantity bar model, with no intrabar path or liquidity assumptions."""

    def __init__(self, gateway_id: str, clock: ManualClock, timeframe: timedelta, *,
                 commission_per_lot: float = 0) -> None:
        finite(commission_per_lot, 'commission per lot')
        if not gateway_id or timeframe <= timedelta(0) or commission_per_lot < 0:
            raise ValueError('invalid historical gateway configuration')
        self.gateway_id = gateway_id
        self.clock = clock
        self.timeframe = timeframe
        self.commission_per_lot = commission_per_lot
        identifiers = count(1)
        self._simulator = SimulationGateway(
            gateway_id, clock, id_factory=lambda: f'execution-{next(identifiers):08d}')
        self._attached = False
        self._last_close: dict[InstrumentId, datetime] = {}
        self._executions: list[Execution] = []

    def attach(self, bus: EventBus) -> None:
        self._simulator.attach(bus)
        self._attached = True

    def order_snapshots(self) -> tuple[OrderSnapshot, ...]:
        return self._simulator.order_snapshots()

    @property
    def executions(self) -> tuple[Execution, ...]:
        return tuple(self._executions)

    async def process_bar(self, bar: Candle) -> None:
        if not self._attached:
            raise RuntimeError('gateway is not attached')
        if (bar.instrument_id.gateway_id != self.gateway_id or bar.timeframe != self.timeframe
                or self.clock.now() != bar.closed_at):
            raise ValueError('bar does not match gateway, timeframe or model clock')
        previous = self._last_close.get(bar.instrument_id)
        if previous is not None and bar.opened_at < previous:
            raise ValueError('historical execution bars must be ordered and nonoverlapping')
        self._last_close[bar.instrument_id] = bar.closed_at
        if bar.synthetic:
            return
        # Snapshot once: orders created by fills are never eligible on this bar.
        for order in self._simulator.order_snapshots():
            if (order.intent.route.instrument.id != bar.instrument_id
                    or order.status not in (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED)
                    or order.submitted_at > bar.opened_at):
                continue
            side, limit = order.intent.side, order.intent.limit_price
            if limit is None:
                price = bar.high if side is Side.BUY else bar.low
            elif (side is Side.BUY and bar.low <= limit) or (side is Side.SELL and bar.high >= limit):
                price = limit
            else:
                continue
            execution = await self._simulator.fill(
                order.order_id, price, commission=self.commission_per_lot * order.remaining_quantity)
            self._executions.append(execution)


@dataclass(frozen=True)
class BacktestConfig:
    instrument: Instrument
    start: datetime
    end: datetime
    timeframe: timedelta
    commission_per_lot: float = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, 'start', utc(self.start))
        object.__setattr__(self, 'end', utc(self.end))
        finite(self.commission_per_lot, 'commission per lot')
        if self.start >= self.end or self.timeframe <= timedelta(0) or self.commission_per_lot < 0:
            raise ValueError('invalid backtest configuration')
        if self.instrument.currency != 'RUB':
            raise ValueError('the current ledger reports RUB; FX conversion is not implemented')


@dataclass(frozen=True)
class BacktestResult:
    config: BacktestConfig
    strategy_name: str
    data_digest: str
    processed_bars: int
    synthetic_bars: int
    orders: tuple[OrderSnapshot, ...]
    executions: tuple[Execution, ...]
    positions: tuple[Position, ...]
    trades: tuple[Trade, ...]
    equity: tuple[EquityPoint, ...]
    unrealized_pnl: float
    marked_at: datetime
    mark_price: float

    @property
    def closed_pnl(self) -> float:
        return self.equity[-1].value if self.equity else 0

    @property
    def realized_pnl(self) -> float:
        """All realized movements, including fees on the still-open cycle."""
        return sum(position.realized_pnl for position in self.positions)

    @property
    def commission(self) -> float:
        return sum(execution.commission for execution in self.executions)

    @property
    def total_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def pending_orders(self) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self.orders if not order.status.terminal)


def _data_digest(bars: tuple[Candle, ...]) -> str:
    payload = [{'instrument': [bar.instrument_id.gateway_id, bar.instrument_id.venue, bar.instrument_id.symbol],
                'opened_at': bar.opened_at.isoformat(), 'timeframe_us': bar.timeframe // timedelta(microseconds=1),
                'ohlcv': [bar.open, bar.high, bar.low, bar.close, bar.volume], 'synthetic': bar.synthetic}
               for bar in bars]
    return sha256(canonical(payload).encode()).hexdigest()


class SingleBacktest:
    """Owned clock/gateway/history; factory must create a fresh trusted strategy."""

    def __init__(self, config: BacktestConfig, candles: tuple[Candle, ...]) -> None:
        self.config = config
        self._bars = tuple(sorted(candles, key=candle_sort_key))
        last: datetime | None = None
        for bar in self._bars:
            if (bar.instrument_id != config.instrument.id or bar.timeframe != config.timeframe
                    or (last is not None and bar.opened_at < last)
                    or bar.opened_at < config.start < bar.closed_at
                    or bar.opened_at < config.end < bar.closed_at):
                raise ValueError('ambiguous history: wrong instrument/timeframe, duplicate, overlap or partial boundary bar')
            last = bar.closed_at
        self._warmup = tuple(bar for bar in self._bars if bar.closed_at <= config.start)
        self._trading = tuple(bar for bar in self._bars
                              if bar.opened_at >= config.start and bar.closed_at <= config.end)
        if not self._trading:
            raise ValueError('no complete bars in the backtest range')
        self._used: set[AutoStrategy] = set()

    async def run(self, factory: Callable[[], AutoStrategy]) -> BacktestResult:
        strategy = factory()
        if strategy in self._used:
            raise ValueError('backtest factory must create a fresh strategy')
        self._used.add(strategy)
        if strategy.instruments != (self.config.instrument.id,):
            raise ValueError('single backtest requires exactly the configured instrument')
        clock = ManualClock(self.config.start)
        identifiers = count(1)
        async with TradingRuntime(clock, order_id_factory=lambda: f'order-{next(identifiers):08d}',
                                  optimization_enabled=False) as runtime:
            gateway = HistoricalGateway(self.config.instrument.id.gateway_id, clock, self.config.timeframe,
                                        commission_per_lot=self.config.commission_per_lot)
            runtime.add_gateway(gateway)
            for bar in self._warmup:
                runtime.market.put(bar)
            context = await runtime.start_strategy(strategy)
            await runtime.drain()
            self._check_orders(context.orders())
            for bar in self._trading:
                clock.advance_to(bar.closed_at)
                await gateway.process_bar(bar)
                await runtime.drain()  # Fills/order updates precede this bar's signal.
                await runtime.feed_candle(bar)
                await runtime.drain()
                self._check_orders(context.orders())
            orders = context.orders()
            # Include all accounts used by this strategy, with no force-close or lost fees.
            routes = tuple(dict.fromkeys(order.intent.route for order in orders))
            positions = tuple(context.position(route) for route in routes)
            mark = self._trading[-1].close
            unrealized = sum(position.quantity * (mark - position.average_price)
                             * position.route.instrument.contract_multiplier for position in positions)
            return BacktestResult(self.config, strategy.name, _data_digest(self._warmup + self._trading),
                                  len(self._trading), sum(bar.synthetic for bar in self._trading),
                                  orders, gateway.executions, positions, context.trades(), context.equity(), unrealized,
                                  self._trading[-1].closed_at, mark)

    def _check_orders(self, orders: tuple[OrderSnapshot, ...]) -> None:
        if any(order.intent.route.instrument != self.config.instrument for order in orders):
            raise ValueError('orders must use the configured instrument and contract metadata')

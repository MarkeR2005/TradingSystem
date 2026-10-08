"""Single owner of per-strategy positions, completed cycles and closed equity."""

from dataclasses import dataclass, field
from datetime import datetime

from .domain import EquityPoint, Execution, Position, Route, Trade


@dataclass
class _Account:
    positions: dict[Route, Position] = field(default_factory=dict)
    opened_at: datetime | None = None
    cycle_pnl: float = 0.0
    closed_pnl: float = 0.0
    trades: list[Trade] = field(default_factory=list)
    equity: list[EquityPoint] = field(default_factory=list)


class PositionLedger:
    def __init__(self, report_currency: str = 'RUB') -> None:
        self.report_currency = report_currency
        self._accounts: dict[str, _Account] = {}
        self._executions: dict[tuple[str, str], tuple[str, Execution]] = {}

    def position(self, strategy_id: str, route: Route) -> Position:
        account = self._accounts.get(strategy_id)
        return account.positions.get(route, Position(route)) if account else Position(route)

    def trades(self, strategy_id: str) -> tuple[Trade, ...]:
        account = self._accounts.get(strategy_id)
        return tuple(account.trades) if account else ()

    def equity(self, strategy_id: str) -> tuple[EquityPoint, ...]:
        account = self._accounts.get(strategy_id)
        return tuple(account.equity) if account else ()

    def apply(self, strategy_id: str, execution: Execution) -> bool:
        previous = self._executions.get(execution.key)
        if previous is not None:
            if previous != (strategy_id, execution):
                raise ValueError('execution identifier has conflicting contents or owner')
            return False
        if execution.route.instrument.currency != self.report_currency:
            raise ValueError('FX conversion is required before combining different currencies')

        account = self._accounts.setdefault(strategy_id, _Account())
        position = self.position(strategy_id, execution.route)
        old = position.quantity
        delta = execution.side.sign * execution.quantity
        opposite = old * delta < 0
        closing = min(abs(old), execution.quantity) if opposite else 0.0
        if opposite and execution.route.instrument.quantities_equal(abs(old), execution.quantity):
            closing = execution.quantity
            quantity = 0.0
        else:
            quantity = old + delta
        opening = execution.quantity - closing
        close_fee = execution.commission * closing / execution.quantity
        gross = (execution.price - position.average_price) * closing
        gross *= (1 if old > 0 else -1) * execution.route.instrument.contract_multiplier

        if account.opened_at is None:
            account.opened_at = execution.timestamp
        if opposite:
            account.cycle_pnl += gross - close_fee
            account.positions[execution.route] = Position(execution.route)
            if (quantity == 0 or opening > 0) and all(p.quantity == 0 for p in account.positions.values()):
                self._complete(account, execution.timestamp)
            if opening > 0:
                if account.opened_at is None:
                    account.opened_at = execution.timestamp
                account.cycle_pnl -= execution.commission - close_fee
            average = execution.price if opening > 0 else position.average_price
        else:
            account.cycle_pnl -= execution.commission
            average = (abs(old) * position.average_price + execution.quantity * execution.price) / abs(quantity)

        if quantity == 0:
            average = 0.0
        account.positions[execution.route] = Position(
            execution.route, quantity, average,
            position.realized_pnl + gross - execution.commission,
        )
        self._executions[execution.key] = strategy_id, execution
        return True

    @staticmethod
    def _complete(account: _Account, timestamp: datetime) -> None:
        if account.opened_at is None:
            raise RuntimeError('trade cycle has no opening time')
        account.trades.append(Trade(account.opened_at, timestamp, account.cycle_pnl))
        account.closed_pnl += account.cycle_pnl
        account.equity.append(EquityPoint(timestamp, account.closed_pnl))
        account.opened_at = None
        account.cycle_pnl = 0.0

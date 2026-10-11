"""Single owner of per-strategy positions, completed cycles and closed equity."""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime

from .domain import EquityPoint, Execution, OwnershipSettlement, Position, Route, Trade, finite
from .inventory import OpeningBalance, TransferPosition


@dataclass
class _Account:
    positions: dict[Route, Position] = field(default_factory=dict)
    opened_at: datetime | None = None
    cycle_pnl: float = 0.0
    closed_pnl: float = 0.0
    trades: list[Trade] = field(default_factory=list)
    ownership_changed: bool = False
    settlements: list[OwnershipSettlement] = field(default_factory=list)
    equity: list[EquityPoint] = field(default_factory=list)


class PositionLedger:
    def __init__(self, report_currency: str = 'RUB') -> None:
        self.report_currency = report_currency
        self._accounts: dict[str, _Account] = {}
        self._executions: dict[tuple[str, str], tuple[str, Execution]] = {}

    def fork(self) -> 'PositionLedger':
        """Isolated candidate for a durable order transition."""
        return deepcopy(self)

    def adopt(self, candidate: 'PositionLedger') -> None:
        if candidate.report_currency != self.report_currency:
            raise ValueError('ledger currencies differ')
        self._accounts = candidate._accounts
        self._executions = candidate._executions

    def position(self, strategy_id: str, route: Route) -> Position:
        account = self._accounts.get(strategy_id)
        return account.positions.get(route, Position(route)) if account else Position(route)

    def positions(self) -> tuple[tuple[str, Position], ...]:
        """Immutable financial snapshot, including owners without route grants."""
        return tuple((owner, position) for owner, account in self._accounts.items()
                     for position in account.positions.values() if position.quantity != 0)

    def trades(self, strategy_id: str) -> tuple[Trade, ...]:
        account = self._accounts.get(strategy_id)
        return tuple(account.trades) if account else ()

    def equity(self, strategy_id: str) -> tuple[EquityPoint, ...]:
        account = self._accounts.get(strategy_id)
        return tuple(account.equity) if account else ()

    def settlements(self, strategy_id: str) -> tuple[OwnershipSettlement, ...]:
        account = self._accounts.get(strategy_id)
        return tuple(account.settlements) if account else ()

    def import_balance(self, strategy_id: str, opening: OpeningBalance, timestamp: datetime) -> None:
        self._receive(strategy_id, opening.route, opening.quantity, opening.average_price, timestamp)

    def _receive(self, owner: str, route: Route, quantity: float, price: float, timestamp: datetime) -> None:
        if route.instrument.currency != self.report_currency:
            raise ValueError('inventory currency differs from ledger')
        previous = self.position(owner, route)
        if previous.quantity * quantity < 0:
            raise ValueError('ownership changes cannot net opposing positions')
        total = previous.quantity + quantity
        if (not route.instrument.accepts_quantity(abs(total))
                or not route.instrument.quantities_equal(total - previous.quantity, quantity)):
            raise ValueError('resulting inventory quantity is invalid')
        average = previous.average_price * (abs(previous.quantity) / abs(total)) + price * (abs(quantity) / abs(total))
        finite(average, 'inventory average price')
        account = self._accounts.setdefault(owner, _Account())
        account.positions[route] = Position(route, total, average, previous.realized_pnl)
        account.ownership_changed = True
        if account.opened_at is None:
            account.opened_at = timestamp

    def transfer(self, command: TransferPosition, timestamp: datetime) -> None:
        previous = self.position(command.source_id, command.route)
        instrument = command.route.instrument
        full = instrument.quantities_equal(abs(previous.quantity), command.quantity)
        if previous.quantity == 0 or (command.quantity > abs(previous.quantity) and not full):
            raise ValueError('transfer exceeds owned position')
        quantity = abs(previous.quantity) if full else command.quantity
        delta = quantity if previous.quantity > 0 else -quantity
        remainder = 0.0 if full else previous.quantity - delta
        if (not instrument.quantities_equal(previous.quantity - remainder, delta)
                or (remainder != 0 and not instrument.accepts_quantity(abs(remainder)))):
            raise ValueError('transfer quantity cannot be represented without loss')
        self._receive(command.target_id, command.route, delta, previous.average_price, timestamp)
        account = self._accounts[command.source_id]
        account.positions[command.route] = Position(command.route, remainder,
                                                    previous.average_price if remainder else 0,
                                                    previous.realized_pnl)
        account.ownership_changed = True
        if all(p.quantity == 0 for p in account.positions.values()):
            self._complete(account, timestamp)

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
        if account.ownership_changed:
            account.settlements.append(OwnershipSettlement(account.opened_at, timestamp, account.cycle_pnl))
        else:
            account.trades.append(Trade(account.opened_at, timestamp, account.cycle_pnl))
        account.ownership_changed = False
        account.closed_pnl += account.cycle_pnl
        account.equity.append(EquityPoint(timestamp, account.closed_pnl))
        account.opened_at = None
        account.cycle_pnl = 0.0

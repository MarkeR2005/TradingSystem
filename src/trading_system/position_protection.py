"""Attach settled inventory to the existing plan executor without synthetic fills."""

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

from .domain import OrderIntent, Side, finite
from .plans import (
    FailurePolicy, PlanLeg, PlanPhase, PlanSnapshot, ProtectPositions,
    ReleasePositionProtection, TradePlan,
)

if TYPE_CHECKING:
    from .orders import OrderManager
    from .plan_execution import PlanBook


async def attach(book: 'PlanBook', oms: 'OrderManager', command: ProtectPositions) -> None:
    owner = command.strategy_id
    marks = command.marks
    positions = tuple(p for name, p in oms.ledger.positions() if name == owner)
    routes = {p.route for p in positions}
    if command.expected_revision != oms.revision or marks.plan_id in book.states:
        raise ValueError('protection revision changed or identifier already used')
    if (not positions or book.active(owner) or oms.strategy_paused(owner)
            or any(oms._deficits.owns_route(route) for route in routes)
            or any(not o.status.terminal or o.order_id in oms._cancellations for o in oms.orders(owner))):
        raise ValueError('attachment requires settled owned positions without active risk recovery or orders')
    if routes != {p.route for p in marks.prices} or routes != {p.route for p in command.exit_prices}:
        raise ValueError('attachment prices must cover all exact owned routes')
    # Include executions of previous owners and broker inventory cutovers. Marks
    # may not precede financial information already known for these routes.
    latest = max([e.timestamp for e in oms._executions.values() if e.route in routes]
                 + [time for gateway, time in oms._inventory_gateways.items()
                    if any(r.instrument.id.gateway_id == gateway for r in routes)], default=marks.as_of)
    if not marks.as_of <= oms.clock.now() < marks.expires_at or marks.as_of < latest:
        raise ValueError('attachment marks are stale, future-dated or precede known inventory')
    exits = {p.route: p.price for p in command.exit_prices}
    reason = oms._batch_error(owner, tuple(OrderIntent(
        p.route, Side.SELL if p.quantity > 0 else Side.BUY, abs(p.quantity), exits[p.route]) for p in positions))
    if oms.capital_policy is None or reason:
        raise ValueError(reason or 'position protection requires a capital policy')
    opening_pnl = oms.ledger.open_cycle_pnl(owner)
    finite(opening_pnl, 'open cycle P&L')
    try:
        deadline = oms.clock.now() + timedelta(seconds=command.order_timeout)
    except OverflowError as error:
        raise ValueError('protection timeout exceeds the supported clock range') from error
    policy = command.protection
    spec = TradePlan(marks.plan_id, owner, tuple(PlanLeg(OrderIntent(
        p.route, Side.BUY if p.quantity > 0 else Side.SELL, abs(p.quantity), p.average_price), exits[p.route])
        for p in positions), FailurePolicy.PROTECT, deadline, policy.stop_loss,
        command.order_timeout, policy.take_profit, policy.trailing)
    state = PlanSnapshot(spec, PlanPhase.OPEN, deadline, exit_prices=command.exit_prices,
                         marks=marks, protection=policy, opening_positions=positions, opening_pnl=opening_pnl)
    await book._save(oms, state)
    await book._evaluate(oms, state)
    book.operations[command.operation_id] = command


async def release(book: 'PlanBook', oms: 'OrderManager', command: ReleasePositionProtection) -> None:
    state = book.states[command.plan_id]
    if command.expected_revision != oms.revision or state.plan.strategy_id != command.strategy_id:
        raise ValueError('protection revision or owner changed')
    if (not state.opening_positions or state.phase is not PlanPhase.OPEN
            or state.close_requested or state.protection_trigger is not None
            or oms.strategy_paused(command.strategy_id) or book._orders(oms, state)
            or any(oms._deficits.owns_route(p.route) for p in state.opening_positions)
            or not book._fresh_marks(oms, state)):
        raise ValueError('release requires fresh marks and quiet, untriggered attached protection')
    await book._save(oms, replace(state, phase=PlanPhase.RELEASED))
    book.operations[command.operation_id] = command

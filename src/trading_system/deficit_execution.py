"""Serialized risk recovery: cancel barriers, then one proportional exit wave.

Derived risk errors block recovery, never roll back an accepted broker fill.
All state and outgoing commands belong to the existing OMS journal transaction.
"""

from dataclasses import replace
from datetime import timedelta
from fractions import Fraction
from math import ceil, gcd
from typing import TYPE_CHECKING

from .capital import AccountKey, _exact, _number
from .deficit import (
    AcknowledgeDeficit, CheckDeficits, DeficitCommand, DeficitGroup, DeficitPhase,
    DeficitQuotes, DeficitSnapshot, ReductionTarget, RetryDeficit, StartDeficit,
)
from .domain import CancelOrder, DeficitUpdated, GatewayCancelRejected, OrderIntent, OrderSnapshot, OrderStatus, Route, Side
from .plans import FailurePolicy, PlanAlert, PlanPhase

if TYPE_CHECKING:
    from .orders import OrderManager


def route_key(route: Route) -> tuple[str, str, str, str]:
    return (route.instrument.id.gateway_id, route.account_id, route.instrument.id.venue, route.instrument.id.symbol)


class DeficitBook:
    def __init__(self) -> None:
        self.states: dict[str, DeficitSnapshot] = {}
        self.operations: dict[str, StartDeficit | RetryDeficit | AcknowledgeDeficit] = {}

    def fork(self) -> 'DeficitBook':
        candidate = DeficitBook()
        candidate.states = self.states.copy()
        candidate.operations = self.operations.copy()
        return candidate

    def active(self) -> DeficitSnapshot | None:
        return next((s for s in self.states.values() if not s.acknowledged), None)

    def paused(self, owner: str) -> bool:
        state = self.active()
        return state is not None and any(g.strategy_id == owner for g in state.groups)

    def owns_route(self, route: Route) -> bool:
        state = self.active()
        return state is not None and (AccountKey.from_route(route) == state.request.account
                                      or any(route in g.routes for g in state.groups))

    def duplicate(self, message: object) -> bool:
        if isinstance(message, (StartDeficit, RetryDeficit, AcknowledgeDeficit)):
            old = self.operations.get(message.operation_id)
            if old is not None:
                if old != message:
                    raise ValueError('deficit operation identifier has conflicting contents')
                return True
        return False

    @staticmethod
    def _groups(oms: 'OrderManager', account: AccountKey) -> tuple[DeficitGroup, ...]:
        pairs = {(owner, pos.route) for owner, pos in oms.ledger.positions()
                 if AccountKey.from_route(pos.route) == account}
        pairs.update((o.strategy_id, o.intent.route) for o in oms._orders.values()
                     if AccountKey.from_route(o.intent.route) == account
                     and (not o.status.terminal or o.order_id in oms._cancellations))
        groups = []
        for state in sorted(oms._plans.states.values(), key=lambda s: s.plan.plan_id):
            if state.phase is PlanPhase.CLOSED:
                continue
            routes = tuple(leg.intent.route for leg in state.plan.legs)
            if any((state.plan.strategy_id, route) in pairs for route in routes):
                groups.append(DeficitGroup(state.plan.strategy_id, routes, state.plan.plan_id))
                pairs.difference_update((state.plan.strategy_id, route) for route in routes)
        groups.extend(DeficitGroup(owner, (route,))
                      for owner, route in sorted(pairs, key=lambda p: (p[0], route_key(p[1]))))
        return tuple(groups)

    @staticmethod
    def _orders(oms: 'OrderManager', state: DeficitSnapshot) -> tuple[OrderSnapshot, ...]:
        pairs = {(g.strategy_id, route) for g in state.groups for route in g.routes}
        return tuple(o for o in oms._orders.values() if (o.strategy_id, o.intent.route) in pairs
                     and (not o.status.terminal or o.order_id in oms._cancellations))

    @staticmethod
    def _amount(oms: 'OrderManager', account: AccountKey) -> Fraction:
        policy = oms.capital_policy
        if policy is None or not policy.as_of <= oms.clock.now() < policy.expires_at:
            raise ValueError('a fresh capital policy is required for deficit sizing')
        limit = next((a for a in policy.accounts if a.account == account), None)
        if limit is None:
            raise ValueError('deficit account is absent from capital policy')
        accounts, _ = oms._totals(oms._exposures())
        return max(Fraction(), accounts.get(account, Fraction()) - _exact(limit.capacity))

    @staticmethod
    def _quotes(oms: 'OrderManager', quotes: DeficitQuotes, groups: tuple[DeficitGroup, ...]) -> None:
        if not quotes.as_of <= oms.clock.now() < quotes.expires_at:
            raise ValueError('deficit exit quotes are stale or future-dated')
        if {q.route for q in quotes.prices} != {route for g in groups for route in g.routes}:
            raise ValueError('deficit quotes must cover the exact affected routes')

    async def _save(self, oms: 'OrderManager', state: DeficitSnapshot) -> None:
        if self.states.get(state.request.operation_id) == state:
            return
        self.states[state.request.operation_id] = state
        for owner in sorted({g.strategy_id for g in state.groups}):
            await oms.bus.publish(f'strategy:{owner}', DeficitUpdated(
                state.request.operation_id, owner, state.phase.value, not state.acknowledged,
                state.alerts[-1].reason))

    async def _block(self, oms: 'OrderManager', state: DeficitSnapshot, reason: str) -> None:
        alerts = state.alerts
        if alerts[-1].reason != reason:
            alerts += (PlanAlert(oms.clock.now(), reason),)
        await self._save(oms, replace(state, phase=DeficitPhase.BLOCKED, alerts=alerts))

    async def handle(self, oms: 'OrderManager', message: DeficitCommand) -> None:
        if isinstance(message, CheckDeficits):
            return
        if message.expected_revision != oms.revision:
            raise ValueError('deficit operator revision changed')
        if isinstance(message, StartDeficit):
            if self.active() is not None:
                raise ValueError('finish and acknowledge the previous deficit case first')
            if self._amount(oms, message.account) <= 0:
                raise ValueError('account has no deficit; strategy budgets are not account deficits')
            groups = self._groups(oms, message.account)
            self._quotes(oms, message.quotes, groups)
            await self._save(oms, DeficitSnapshot(
                message, DeficitPhase.WAITING, oms.clock.now() + timedelta(seconds=message.wait_seconds),
                groups, message.quotes, (PlanAlert(oms.clock.now(), 'account deficit detected; recovery waiting'),)))
        else:
            state = self.states[message.case_id]
            if state.acknowledged:
                raise ValueError('deficit case was already acknowledged')
            if isinstance(message, RetryDeficit):
                if state.phase not in (DeficitPhase.BLOCKED, DeficitPhase.RESOLVED):
                    raise ValueError('retry requires a blocked or resolved deficit case')
                self._amount(oms, state.request.account)
                self._quotes(oms, message.quotes, state.groups)
                await self._save(oms, replace(state, phase=DeficitPhase.WAITING, deadline=oms.clock.now(),
                                              quotes=message.quotes, wave=()))
            else:
                if (self._amount(oms, state.request.account) > 0 or self._orders(oms, state)
                        or self._remaining(oms, state)):
                    raise ValueError('acknowledgement requires no account deficit or outstanding affected orders')
                await self._save(oms, replace(state, phase=DeficitPhase.RESOLVED, acknowledged=True, target=None))
        self.operations[message.operation_id] = message

    async def _cancel(self, oms: 'OrderManager', state: DeficitSnapshot) -> DeficitSnapshot:
        state = replace(state, phase=DeficitPhase.CANCELLING,
                        deadline=oms.clock.now() + timedelta(seconds=state.request.order_timeout))
        await self._save(oms, state)
        closing: set[str] = set()
        for group in state.groups:
            if group.plan_id is not None:
                plan = oms.plan(group.plan_id)
                # A protective full exit has priority over partial deficit sizing.
                if plan.close_requested:
                    closing.update(plan.entry_ids + plan.exit_ids)
                elif plan.phase is not PlanPhase.CLOSED:
                    await oms._plans._cancel(oms, plan, FailurePolicy.PROTECT, 'external account deficit recovery')
        for order in self._orders(oms, state):
            if order.order_id not in closing:
                await oms._request_cancel(CancelOrder(order.strategy_id, order.order_id))
        await oms._plans.advance(oms, None)
        return state

    def _select(self, oms: 'OrderManager', state: DeficitSnapshot,
                amount: Fraction) -> tuple[DeficitGroup, tuple[OrderIntent, ...]]:
        self._quotes(oms, state.quotes, state.groups)
        exposures = {(e.strategy_id, e.route): e for e in oms._exposures()}
        candidates = []
        for group in state.groups:
            if group.plan_id is not None:
                plan = oms.plan(group.plan_id)
                if plan.close_requested and plan.phase is not PlanPhase.CLOSED:
                    raise ValueError('protective plan exit must finish before deficit reduction')
            owned = [exposures[(group.strategy_id, route)] for route in group.routes
                     if (group.strategy_id, route) in exposures and exposures[(group.strategy_id, route)].position]
            affected = [e for e in owned if AccountKey.from_route(e.route) == state.request.account]
            if not affected:
                continue
            # Integer grid units give the smallest exact proportional basket.
            divisor = 0
            for e in owned:
                units = abs(e.position) / _exact(e.route.instrument.quantity_step)
                if units.denominator != 1:
                    raise ValueError('owned position is off the lot grid')
                divisor = gcd(divisor, units.numerator)
            relief = sum((abs(e.position) * e.unit_cost for e in affected), Fraction()) / divisor
            units_to_close = min(divisor, ceil(amount / relief))
            limits = {q.route: q.price for q in state.quotes.prices}
            intents = tuple(OrderIntent(e.route, Side.SELL if e.position > 0 else Side.BUY,
                                        _number(abs(e.position) * units_to_close / divisor), limits[e.route])
                            for e in owned)
            priority = max(abs(e.position) for e in affected)
            candidates.append((-priority, group.strategy_id, tuple(route_key(r) for r in group.routes), group, intents))
        if not candidates:
            raise ValueError('no owned positions can relieve the remaining account deficit')
        _, _, _, group, intents = min(candidates, key=lambda item: item[:3])
        return group, intents

    @staticmethod
    def _remaining(oms: 'OrderManager', state: DeficitSnapshot) -> tuple[OrderIntent, ...]:
        target = state.target
        if target is None:
            return ()
        if target.group.plan_id is not None and oms.plan(target.group.plan_id).phase is PlanPhase.CLOSED:
            return ()  # A completed protective full exit supersedes the partial target.
        if all(route.instrument.quantities_equal(oms.ledger.position(target.group.strategy_id, route).quantity,
                                                  _number(initial)) for route, initial in target.initial):
            return ()  # No fill: a retry must size from the current deficit, not an obsolete target.
        limits = {q.route: q.price for q in state.quotes.prices}
        intents = []
        for route, desired in target.quantities:
            position = oms.ledger.position(target.group.strategy_id, route).quantity
            step = _exact(route.instrument.quantity_step)
            current = step * round(position / route.instrument.quantity_step)
            delta = current - desired
            if delta == 0:
                continue
            if delta * current <= 0 or abs(delta) > abs(current):
                raise ValueError('partial exit target changed; reopening positions is prohibited')
            intents.append(OrderIntent(route, Side.SELL if delta > 0 else Side.BUY,
                                       _number(abs(delta)), limits[route]))
        return tuple(intents)

    async def advance(self, oms: 'OrderManager', message: object) -> None:
        state = self.active()
        if state is None or state.phase in (DeficitPhase.RESOLVED, DeficitPhase.BLOCKED) or not oms.ready:
            return
        if state.phase is DeficitPhase.REDUCING:
            wave = [oms._orders[key] for key in state.wave]
            failed = any(o.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.CANCEL_PENDING) for o in wave)
            if failed or (oms.clock.now() >= state.deadline and not all(o.status is OrderStatus.FILLED for o in wave)):
                for order in wave:
                    await oms._request_cancel(CancelOrder(order.strategy_id, order.order_id))
                await self._block(oms, state, 'deficit exit failed or timed out; explicit retry required')
                return
            if not all(o.status is OrderStatus.FILLED for o in wave):
                return
            state = replace(state, phase=DeficitPhase.CANCELLING, wave=(), target=None)
        if state.phase is DeficitPhase.CANCELLING:
            pending = self._orders(oms, state)
            if (isinstance(message, GatewayCancelRejected)
                    and any(o.order_id == message.order_id and o.cancel_request_id == message.request_id
                            and not o.status.terminal for o in pending)):
                await self._block(oms, state, 'deficit cancellation rejected; explicit retry required')
                return
            if pending:
                if oms.clock.now() >= state.deadline:
                    await self._block(oms, state, 'deficit cancellation barrier timed out; explicit retry required')
                return
        try:
            amount = self._amount(oms, state.request.account)
            if amount == 0 and state.target is None:
                await self._save(oms, replace(state, phase=DeficitPhase.RESOLVED))
                return
            if state.phase is DeficitPhase.WAITING:
                if oms.clock.now() < state.deadline:
                    return
                state = await self._cancel(oms, state)
                if self._orders(oms, state):
                    return
                amount = self._amount(oms, state.request.account)
                if amount == 0 and state.target is None:
                    await self._save(oms, replace(state, phase=DeficitPhase.RESOLVED))
                    return
            intents = self._remaining(oms, state)
            if state.target is not None and intents:
                group = state.target.group
                self._quotes(oms, state.quotes, state.groups)
                if group.plan_id is not None and oms.plan(group.plan_id).close_requested:
                    raise ValueError('protective plan exit must finish before deficit reduction')
            else:
                state = replace(state, target=None)
                if amount == 0:
                    await self._save(oms, replace(state, phase=DeficitPhase.RESOLVED))
                    return
                group, intents = self._select(oms, state, amount)
                quantities = []
                initial = []
                for intent in intents:
                    step = _exact(intent.route.instrument.quantity_step)
                    position = oms.ledger.position(group.strategy_id, intent.route).quantity
                    current = step * round(position / intent.route.instrument.quantity_step)
                    quantities.append((intent.route, current + intent.side.sign * _exact(intent.quantity)))
                    initial.append((intent.route, current))
                state = replace(state, target=ReductionTarget(group, tuple(quantities), tuple(initial)))
            for intent in intents:
                position = oms.ledger.position(group.strategy_id, intent.route).quantity
                delta = intent.side.sign * intent.quantity
                if not intent.route.instrument.quantities_equal((position + delta) - position, delta):
                    raise ValueError('deficit exit quantity cannot be represented without loss')
            reason = oms._batch_error(group.strategy_id, intents)
            if reason:
                raise ValueError(reason)
            next_deadline = oms.clock.now() + timedelta(seconds=state.request.order_timeout)
        except (ValueError, OverflowError) as error:
            await self._block(oms, state, str(error))
            return
        generated = []
        for intent in intents:
            generated.append((await oms._new_order(group.strategy_id, intent)).order_id)
        ids = tuple(generated)
        if group.plan_id is not None:
            plan = oms.plan(group.plan_id)
            await oms._plans._save(oms, replace(plan, exit_ids=plan.exit_ids + ids))
        await self._save(oms, replace(state, phase=DeficitPhase.REDUCING, wave=ids,
                                      exit_ids=state.exit_ids + ids,
                                      deadline=next_deadline))

"""Plan state belongs to the OMS transaction; no independent financial writer."""

from dataclasses import replace
from datetime import timedelta
from fractions import Fraction
from typing import TYPE_CHECKING

from .domain import CancelOrder, Execution, GatewayCancelRejected, Message, OrderIntent, OrderStatus, PlanUpdated, Side
from .capital import _exact, _number
from .inventory import MANUAL
from .plans import (
    AcknowledgePlan, CheckPlanDeadlines, ClosePlan, FailurePolicy, PlanAlert,
    PlanMarks, PlanPhase, PlanPrice, PlanSnapshot, RetryPlan, SubmitPlan,
)

if TYPE_CHECKING:
    from .orders import OrderManager


class PlanBook:
    def __init__(self) -> None:
        self.states: dict[str, PlanSnapshot] = {}
        self.operations: dict[str, RetryPlan | AcknowledgePlan] = {}
        self.waves: dict[str, tuple[str, ...]] = {}

    def fork(self) -> 'PlanBook':
        candidate = PlanBook()
        candidate.states = self.states.copy()
        candidate.operations = self.operations.copy()
        candidate.waves = self.waves.copy()
        return candidate

    def active(self, owner: str) -> bool:
        return any(s.plan.strategy_id == owner and s.phase is not PlanPhase.CLOSED for s in self.states.values())

    def paused(self, owner: str) -> bool:
        return any(s.plan.strategy_id == owner and s.paused for s in self.states.values())

    def duplicate(self, message: object) -> bool:
        if isinstance(message, SubmitPlan):
            previous = self.states.get(message.plan.plan_id)
            if previous is not None:
                if previous.plan != message.plan:
                    raise ValueError('plan identifier has conflicting contents')
                return True
        if isinstance(message, (RetryPlan, AcknowledgePlan)):
            old = self.operations.get(message.operation_id)
            if old is not None:
                if old != message:
                    raise ValueError('plan operation identifier has conflicting contents')
                return True
        if isinstance(message, ClosePlan):
            state = self.states[message.plan_id]
            if state.plan.strategy_id != message.strategy_id:
                raise ValueError('strategy does not own plan')
            return state.close_requested or state.phase is PlanPhase.CLOSED
        return False

    async def _save(self, oms: 'OrderManager', state: PlanSnapshot) -> None:
        if self.states.get(state.plan.plan_id) == state:
            return
        self.states[state.plan.plan_id] = state
        await oms.bus.publish(f'strategy:{state.plan.strategy_id}', PlanUpdated(
            state.plan.plan_id, state.plan.strategy_id, state.phase.value, state.paused,
            len(state.alerts), state.alerts[-1].reason if state.alerts else None))

    def _alert(self, oms: 'OrderManager', state: PlanSnapshot, reason: str) -> PlanSnapshot:
        alerts = state.alerts
        if not alerts or alerts[-1].reason != reason:
            alerts += (PlanAlert(oms.clock.now(), reason),)
        return replace(state, paused=True, alerts=alerts)

    def _deadline(self, oms: 'OrderManager', state: PlanSnapshot) -> PlanSnapshot:
        return replace(state, deadline=oms.clock.now() + timedelta(seconds=state.plan.recovery_timeout))

    def _orders(self, oms: 'OrderManager', state: PlanSnapshot) -> tuple[str, ...]:
        return tuple(key for key in state.entry_ids + state.exit_ids
                     if not oms._orders[key].status.terminal or key in oms._cancellations)

    def _flat(self, oms: 'OrderManager', state: PlanSnapshot) -> bool:
        return all(oms.ledger.position(state.plan.strategy_id, leg.intent.route).quantity == 0
                   for leg in state.plan.legs)

    async def _cancel(self, oms: 'OrderManager', state: PlanSnapshot, goal: FailurePolicy,
                      reason: str | None = None) -> None:
        if reason is not None:
            state = self._alert(oms, state, reason)
        state = self._deadline(oms, replace(state, phase=PlanPhase.CANCELLING, goal=goal,
                                            close_requested=state.close_requested or goal is FailurePolicy.CLOSE))
        await self._save(oms, state)
        for key in self._orders(oms, state):
            await oms._request_cancel(CancelOrder(state.plan.strategy_id, key))

    async def handle(self, oms: 'OrderManager', message: Message) -> None:
        if isinstance(message, SubmitPlan):
            spec = message.plan
            if (spec.strategy_id == MANUAL or self.active(spec.strategy_id) or self.paused(spec.strategy_id)
                    or any(owner == spec.strategy_id for owner, _ in oms.ledger.positions())
                    or any(not o.status.terminal or o.order_id in oms._cancellations
                           for o in oms.orders(spec.strategy_id))):
                raise ValueError('a plan requires a flat, unpaused strategy without outstanding orders')
            if spec.expires_at <= oms.clock.now() or oms.capital_policy is None:
                raise ValueError('a plan requires a future deadline and a capital policy')
            reason = oms._batch_error(spec.strategy_id, tuple(leg.intent for leg in spec.legs))
            if reason:
                raise ValueError(reason)
            ids = []
            for leg in spec.legs:
                order = await oms._new_order(spec.strategy_id, leg.intent)
                ids.append(order.order_id)
            self.waves[spec.plan_id] = tuple(ids)
            await self._save(oms, PlanSnapshot(spec, PlanPhase.ENTERING, spec.expires_at,
                                              entry_ids=tuple(ids),
                                              exit_prices=tuple(PlanPrice(l.intent.route, l.exit_limit) for l in spec.legs)))
        elif isinstance(message, ClosePlan):
            state = self.states[message.plan_id]
            if state.plan.strategy_id != message.strategy_id:
                raise ValueError('strategy does not own plan')
            await self._cancel(oms, replace(state, close_requested=True), FailurePolicy.CLOSE)
        elif isinstance(message, PlanMarks):
            state = self.states[message.plan_id]
            self._validate_prices(state, message.prices)
            latest = max((e.timestamp for e in oms._executions.values()
                          if e.order_id in state.entry_ids + state.exit_ids), default=message.as_of)
            if (not message.as_of <= oms.clock.now() < message.expires_at or message.as_of < latest
                    or (state.marks is not None and (message.sequence <= state.marks.sequence
                                                     or message.as_of < state.marks.as_of))):
                raise ValueError('plan marks are stale, future-dated or precede executions')
            await self._save(oms, replace(state, marks=message))
            await self._evaluate(oms, self.states[message.plan_id])
        elif isinstance(message, (RetryPlan, AcknowledgePlan)):
            if message.expected_revision != oms.revision:
                raise ValueError('plan operator revision changed')
            state = self.states[message.plan_id]
            if isinstance(message, RetryPlan):
                if state.phase not in (PlanPhase.BLOCKED, PlanPhase.HELD):
                    raise ValueError('explicit exit retry requires a blocked or held plan')
                self._validate_prices(state, message.exit_prices)
                await self._cancel(oms, replace(state, exit_prices=message.exit_prices, close_requested=True),
                                   FailurePolicy.CLOSE)
            else:
                if state.phase not in (PlanPhase.OPEN, PlanPhase.CLOSED) or self._orders(oms, state):
                    raise ValueError('acknowledge only a completed entry or a flat closed plan')
                await self._save(oms, replace(state, paused=False))
            self.operations[message.operation_id] = message
        elif isinstance(message, CheckPlanDeadlines):
            for state in tuple(self.states.values()):
                if oms.clock.now() < state.deadline:
                    continue
                if state.phase is PlanPhase.ENTERING:
                    await self._cancel(oms, state, state.plan.failure_policy, 'entry deadline expired')
                elif state.phase in (PlanPhase.COMPLETING, PlanPhase.CLOSING):
                    await self._cancel(oms, state, FailurePolicy.PROTECT, 'recovery deadline expired')
                elif state.phase is PlanPhase.CANCELLING:
                    await self._save(oms, replace(self._alert(oms, state, 'cancellation deadline expired'),
                                                  phase=PlanPhase.BLOCKED))
        else:
            raise ValueError('unexpected plan command')

    @staticmethod
    def _validate_prices(state: PlanSnapshot, marks: tuple[PlanPrice, ...]) -> None:
        if {item.route for item in marks} != {leg.intent.route for leg in state.plan.legs}:
            raise ValueError('prices must cover every exact plan route')

    async def _evaluate(self, oms: 'OrderManager', state: PlanSnapshot) -> None:
        marks = state.marks
        if marks is None:
            return
        fills = [e for e in oms._executions.values() if e.order_id in state.entry_ids + state.exit_ids]
        if oms.clock.now() >= marks.expires_at or any(e.timestamp > marks.as_of for e in fills):
            await self._save(oms, replace(state, pnl=None))
            return
        cash = sum((-e.side.sign * _exact(e.quantity) * _exact(e.price)
                    * _exact(e.route.instrument.contract_multiplier) - _exact(e.commission) for e in fills), Fraction())
        quantities = {m.route: sum((e.side.sign * _exact(e.quantity) for e in fills if e.route == m.route), Fraction())
                      for m in marks.prices}
        value = sum((quantities[m.route] * _exact(m.price) * _exact(m.route.instrument.contract_multiplier)
                     for m in marks.prices), Fraction())
        total = cash + value
        try:
            state = replace(state, pnl=_number(total))
        except ValueError:
            state = replace(self._alert(oms, state, 'plan P&L exceeds reporting range'), pnl=None)
            # A derived risk calculation must never roll back a valid broker fill.
            # Stop growth, preserve the fill and wait for usable marks/operator action.
            if state.phase in (PlanPhase.ENTERING, PlanPhase.COMPLETING):
                await self._cancel(oms, state, FailurePolicy.PROTECT)
                state = self.states[state.plan.plan_id]
        await self._save(oms, state)
        if total <= -_exact(state.plan.stop_loss) and any(quantities.values()) and state.phase is not PlanPhase.CLOSED:
            if state.phase is PlanPhase.BLOCKED or (state.close_requested and state.phase is PlanPhase.HELD):
                await self._save(oms, self._alert(oms, state, 'stop loss breached while recovery is blocked'))
            elif not state.close_requested:
                await self._cancel(oms, replace(state, close_requested=True), FailurePolicy.CLOSE,
                                   'aggregate executed P&L reached stop loss')

    async def advance(self, oms: 'OrderManager', message: object) -> None:
        for plan_id in tuple(self.states):
            state = self.states[plan_id]
            if isinstance(message, Execution) and message.order_id in state.entry_ids + state.exit_ids:
                await self._evaluate(oms, state)
                state = self.states[plan_id]
            if (isinstance(message, GatewayCancelRejected) and message.order_id in state.entry_ids + state.exit_ids
                    and state.phase is PlanPhase.CANCELLING
                    and oms._orders[message.order_id].cancel_request_id == message.request_id
                    and not oms._orders[message.order_id].status.terminal):
                await self._save(oms, replace(self._alert(oms, state, 'gateway rejected plan cancellation'),
                                              phase=PlanPhase.BLOCKED))
                continue
            wave = [oms._orders[key] for key in self.waves.get(plan_id, ())]
            failed = any(o.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.CANCEL_PENDING)
                         for o in wave)
            if state.phase in (PlanPhase.ENTERING, PlanPhase.COMPLETING):
                if failed:
                    goal = state.plan.failure_policy if state.phase is PlanPhase.ENTERING else FailurePolicy.PROTECT
                    await self._cancel(oms, state, goal, 'entry leg failed' if state.phase is PlanPhase.ENTERING
                                       else 'completion attempt failed; retaining executed exposure')
                elif all(o.status is OrderStatus.FILLED for o in wave):
                    await self._save(oms, replace(state, phase=PlanPhase.OPEN, goal=None))
            elif state.phase is PlanPhase.CLOSING:
                if self._flat(oms, state) and not self._orders(oms, state):
                    await self._save(oms, replace(state, phase=PlanPhase.CLOSED))
                elif failed:
                    await self._save(oms, replace(self._alert(oms, state, 'exit leg failed; explicit retry required'),
                                                  phase=PlanPhase.BLOCKED))
            elif state.phase is PlanPhase.BLOCKED and self._flat(oms, state) and not self._orders(oms, state):
                await self._save(oms, replace(state, phase=PlanPhase.CLOSED))
            state = self.states[plan_id]
            if state.phase is PlanPhase.CANCELLING and not self._orders(oms, state):
                await self._recover(oms, state)

    async def _recover(self, oms: 'OrderManager', state: PlanSnapshot) -> None:
        if state.goal is FailurePolicy.PROTECT:
            await self._save(oms, replace(state, phase=PlanPhase.CLOSED if self._flat(oms, state) else PlanPhase.HELD))
            return
        if state.goal is FailurePolicy.CLOSE and self._flat(oms, state):
            await self._save(oms, replace(state, phase=PlanPhase.CLOSED))
            return
        if not oms.ready:
            return
        owner = state.plan.strategy_id
        intents: list[OrderIntent] = []
        exits = {p.route: p.price for p in state.exit_prices}
        for leg in state.plan.legs:
            intent = leg.intent
            position = oms.ledger.position(owner, intent.route)
            if state.goal is FailurePolicy.COMPLETE:
                remaining = intent.quantity - position.quantity * intent.side.sign
                if intent.route.instrument.quantities_equal(remaining, 0):
                    continue
                if remaining < 0 or remaining > intent.quantity or state.recovery_attempted:
                    raise ValueError('plan completion quantity contradicts owned exposure')
                quantity = intent.route.instrument.quantity_step * round(remaining / intent.route.instrument.quantity_step)
                intents.append(replace(intent, quantity=quantity))
            elif position.quantity:
                intents.append(OrderIntent(intent.route, Side.SELL if position.quantity > 0 else Side.BUY,
                                           abs(position.quantity), exits[intent.route]))
        if not intents:
            await self._save(oms, replace(state, phase=PlanPhase.OPEN, goal=None))
            return
        reason = oms._batch_error(owner, tuple(intents))
        if reason:
            phase = PlanPhase.HELD if state.goal is FailurePolicy.COMPLETE else PlanPhase.BLOCKED
            await self._save(oms, replace(self._alert(oms, state, f'recovery admission failed: {reason}'),
                                          phase=phase, recovery_attempted=True))
            return
        ids = []
        for intent in intents:
            ids.append((await oms._new_order(owner, intent)).order_id)
        self.waves[state.plan.plan_id] = tuple(ids)
        if state.goal is FailurePolicy.COMPLETE:
            state = replace(state, phase=PlanPhase.COMPLETING, entry_ids=state.entry_ids + tuple(ids),
                            recovery_attempted=True)
        else:
            state = replace(state, phase=PlanPhase.CLOSING, exit_ids=state.exit_ids + tuple(ids))
        await self._save(oms, self._deadline(oms, state))

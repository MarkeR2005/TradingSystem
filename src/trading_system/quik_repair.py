"""Plan a missing committed execution suffix. No broker cutover or trading resume."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from . import codec
from .domain import Execution, OrderIntent, utc
from .inventory import BrokerPosition, OmsObservation, RepairExecutions, RepairOrderTarget
from .persistence import canonical
from .quik_compare import ComparisonPolicy, QuikBindings, compare_quik
from .quik_contract import decimal


@dataclass(frozen=True)
class ExecutionEvidence:
    """Committed payloads in protocol insertion order, NOT sorted broker IDs."""

    bindings: QuikBindings
    executions: tuple[Execution, ...]

    def __post_init__(self) -> None:
        if (not isinstance(self.bindings, QuikBindings) or not isinstance(self.executions, tuple)
                or not all(isinstance(e, Execution) for e in self.executions)):
            raise ValueError('execution evidence requires immutable typed records')
        if (len({e.key for e in self.executions}) != len(self.executions)
                or any(e.route.instrument.id.gateway_id != self.bindings.gateway_id for e in self.executions)):
            raise ValueError('execution evidence identity is ambiguous')

    @property
    def digest(self) -> str:
        bindings = self.bindings
        value = {'gateway': bindings.gateway_id, 'session': bindings.session_id, 'revision': bindings.revision,
                 'orders': [[codec.encode(b.route), b.broker_id, b.local_id] for b in bindings.orders],
                 'executions': [[codec.encode(b.route), b.broker_id, b.local_id] for b in bindings.executions],
                 'payloads': codec.encode(self.executions)}
        return sha256(canonical(value).encode()).hexdigest()


def missing_suffix(evidence: ExecutionEvidence, oms: OmsObservation) -> tuple[Execution, ...]:
    """Do not append a historical hole after an already booked later execution."""
    local = {e.key: e for e in oms.executions}
    saved = {e.key: e for e in evidence.executions}
    if any(saved.get(key) != execution for key, execution in local.items()):
        raise ValueError('OMS history differs from committed execution evidence')
    missing: list[Execution] = []
    for execution in evidence.executions:
        if execution.key not in local:
            missing.append(execution)
        elif missing:
            raise ValueError('only a missing execution suffix can be repaired')
    if not missing:
        raise ValueError('no missing execution suffix')
    if local and min(e.timestamp for e in missing) < max(e.timestamp for e in local.values()):
        raise ValueError('execution suffix predates already booked history')
    return tuple(missing)


def _float(value: Decimal) -> float:
    result = float(value)
    if decimal(result) != value:
        raise ValueError('observed value cannot round-trip through the OMS numeric domain')
    return result


def build_execution_repair(operation_id: str, raw: dict[str, Any], policy: ComparisonPolicy,
                           oms: OmsObservation, evidence: ExecutionEvidence, *, now: datetime,
                           current_revision: int) -> RepairExecutions:
    """Positive evidence only; unresolved cancellation remains unresolved."""
    report = compare_quik(raw, policy, oms, evidence.bindings, now=now, current_revision=current_revision)
    allowed = {'execution_not_recorded', 'position_mismatch', 'order_outcome_unresolved'}
    if report.observation.issues or any(
            d.code not in allowed and not (d.code == 'order_mismatch'
                                          and set(d.fields) <= {'filled_quantity', 'status'})
            for d in report.differences):
        raise ValueError('observation contains differences outside execution repair')
    missing = missing_suffix(evidence, oms)
    expected_missing = {d.local_id for d in report.differences if d.code == 'execution_not_recorded'}
    if expected_missing != {e.execution_id for e in missing}:
        raise ValueError('snapshot does not cover the missing execution suffix')
    bindings = evidence.bindings
    order_ids = {(b.route, b.broker_id): b.local_id for b in bindings.orders}
    execution_ids = {(b.route, b.broker_id): b.local_id for b in bindings.executions}
    saved = {e.execution_id: e for e in evidence.executions}
    if set(saved) != {b.local_id for b in bindings.executions}:
        raise ValueError('bindings need complete saved execution payloads')
    for trade in report.observation.trades:
        execution = saved.get(execution_ids.get((trade.route, trade.broker_id), ''))
        if execution is None or (
                execution.route, execution.order_id, execution.side, decimal(execution.quantity),
                decimal(execution.price), decimal(execution.commission), execution.timestamp) != (
                    trade.route, order_ids.get((trade.route, trade.order_id)), trade.side,
                    Decimal(trade.quantity), trade.price, trade.commission, trade.occurred_at):
            raise ValueError('saved execution payload differs from broker observation')
    targets = tuple(RepairOrderTarget(order_ids[o.route, o.broker_id],
        OrderIntent(o.route, o.side, o.quantity, None if o.price is None else _float(o.price)),
        o.quantity - o.balance) for o in report.observation.orders)
    return RepairExecutions(operation_id, oms.revision, policy.gateway_id, policy.session_id,
        bindings.revision, evidence.digest, sha256(canonical(raw).encode()).hexdigest(),
        utc(datetime.fromisoformat(raw['finished_at'])), utc(now),
        utc(datetime.fromisoformat(raw['started_at'])) + policy.max_age,
        missing, targets, tuple(BrokerPosition(p.route, p.quantity) for p in report.observation.positions))

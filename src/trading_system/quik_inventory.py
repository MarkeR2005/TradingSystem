"""Plan external inventory from explicit provenance; never import or infer a cost basis."""

from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from math import fsum
from typing import Any

from .domain import Execution, GatewayRejected, OrderIntent, OrderSnapshot, OrderStatus, utc
from .inventory import (MANUAL, BrokerPosition, ExternalOrder, ExternalOrderOrigin, ExternalPositionBasis,
                        OmsObservation, StageExternalInventory)
from .persistence import canonical
from .quik_compare import ComparisonPolicy, compare_quik
from .quik_contract import decimal
from .quik_outcomes import OrderEvidence
from .quik_repair import _float


def build_external_inventory_plan(operation_id: str, raw: dict[str, Any], policy: ComparisonPolicy,
        oms: OmsObservation, evidence: OrderEvidence, *, position_basis: tuple[ExternalPositionBasis, ...],
        order_origins: tuple[ExternalOrderOrigin, ...], now: datetime, current_revision: int) -> StageExternalInventory:
    """All unknown inventory goes to manual in the proposal; adoption is deliberately separate."""
    if (not isinstance(position_basis, tuple) or not all(isinstance(p, ExternalPositionBasis) for p in position_basis)
            or not isinstance(order_origins, tuple) or not all(isinstance(o, ExternalOrderOrigin) for o in order_origins)):
        raise ValueError('explicit immutable provenance declarations required')
    bindings = evidence.executions.bindings
    if {e.key: e for e in oms.executions} != {e.key: e for e in evidence.executions.executions}:
        raise ValueError('resolve known execution history before planning external inventory')
    # A saved registration rejection is positive evidence; absence alone is not.
    rejected = {m.order_id: m for m in evidence.outcomes if isinstance(m, GatewayRejected)}
    proven = tuple(o.order_id for o in oms.orders if o.status is OrderStatus.REJECTED and o.filled_quantity == 0
                   and o.order_id in rejected and o.rejection_reason == rejected[o.order_id].reason)
    view = replace(oms, local_rejections=tuple(dict.fromkeys(oms.local_rejections + proven)))
    report = compare_quik(raw, policy, view, bindings, now=now, current_revision=current_revision)
    if report.observation.issues or any(d.code not in {'external_order', 'external_execution', 'position_mismatch'}
                                       for d in report.differences):
        raise ValueError('resolve local discrepancies before planning external inventory')
    observation = report.observation
    if (len({o.broker_id for o in observation.orders}) != len(observation.orders)
            or len({t.broker_id for t in observation.trades}) != len(observation.trades)):
        raise ValueError('broker identifiers must be unique within the gateway session')
    bound_orders = {(b.route, b.broker_id) for b in bindings.orders}
    bound_trades = {(b.route, b.broker_id) for b in bindings.executions}
    external_orders = {(o.route, o.broker_id): o for o in observation.orders if (o.route, o.broker_id) not in bound_orders}
    external_trades = tuple(t for t in observation.trades if (t.route, t.broker_id) not in bound_trades)
    if any((t.route, t.order_id) not in external_orders for t in external_trades):
        raise ValueError('untracked execution of a known order is not external inventory')
    origins = {(o.route, o.broker_order_id): o for o in order_origins}
    if len(origins) != len(order_origins) or set(origins) != set(external_orders):
        raise ValueError('declare exactly the observed external order origins')
    totals = {p.route: Decimal() for p in observation.positions}
    for _, position in oms.positions:
        totals[position.route] += decimal(position.quantity)
    residuals = {p.route: Decimal(p.quantity) - totals[p.route] for p in observation.positions
                 if Decimal(p.quantity) != totals[p.route]}
    basis = {p.opening.route: p for p in position_basis}
    if len(basis) != len(position_basis) or set(basis) != set(residuals):
        raise ValueError('declare cost basis for exactly the nonzero external residuals')
    for route, quantity in residuals.items():
        if decimal(basis[route].opening.quantity) != quantity:
            raise ValueError('declared residual quantity differs from broker net minus all known owners')
        if any(owner == MANUAL and p.route == route and decimal(p.quantity) * quantity < 0 for owner, p in oms.positions):
            raise ValueError('opposing manual residual requires real execution recovery, not inventory netting')
    end = utc(datetime.fromisoformat(raw['finished_at']))
    if any(p.provenance.as_of != end for p in position_basis) or any(o.provenance.as_of != end for o in order_origins):
        raise ValueError('provenance must describe inventory at this exact observation boundary')
    imported: list[ExternalOrder] = []
    ordered_origins: list[ExternalOrderOrigin] = []
    local_ids = {o.order_id for o in oms.orders}
    local_execution_ids = {e.execution_id for e in oms.executions}
    for key, observed in external_orders.items():
        origin = origins[key]
        local_id = f'{policy.session_id}:external-order:{observed.broker_id}'
        if local_id in local_ids:
            raise ValueError('proposed external order ID is already in use')
        trades = tuple(t for t in external_trades if (t.route, t.order_id) == key)
        executions = []
        for trade in trades:
            if trade.commission is None or f'{policy.session_id}:{trade.broker_id}' in local_execution_ids:
                raise ValueError('external execution is unpriced or already known')
            executions.append(Execution(f'{policy.session_id}:{trade.broker_id}', local_id, trade.route,
                trade.side, trade.quantity, _float(trade.price), _float(trade.commission), trade.occurred_at))
        filled = observed.quantity - observed.balance
        average = fsum(e.price * (e.quantity / filled) for e in executions) if filled else 0
        commission = _float(sum((t.commission for t in trades if t.commission is not None), Decimal()))
        order = OrderSnapshot(local_id, MANUAL, OrderIntent(observed.route, observed.side, observed.quantity,
            None if observed.price is None else _float(observed.price)), origin.submitted_at,
            observed.status, filled, average, commission)
        imported.append(ExternalOrder(order, tuple(executions)))
        ordered_origins.append(origin)
    return StageExternalInventory(operation_id, oms.revision, policy.gateway_id, policy.session_id, bindings.revision,
        evidence.digest, sha256(canonical(raw).encode()).hexdigest(), end, utc(now),
        utc(datetime.fromisoformat(raw['started_at'])) + policy.max_age, position_basis, tuple(ordered_origins),
        tuple(imported), tuple(BrokerPosition(p.route, p.quantity) for p in observation.positions))

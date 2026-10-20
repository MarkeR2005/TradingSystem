"""Versioned, closed JSON vocabulary; no pickle or dynamic class imports."""

from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from .domain import (
    CancelOrder, Execution, GatewayAccepted, GatewayCancel, GatewayCancelled, GatewayCancelRejected, GatewaySubmit,
    GatewayRejected, GatewayReconciled, GatewayUncertain, Instrument, InstrumentId, OrderIntent,
    OrderSnapshot, OrderStatus, ReplaceOrder, Route, Side, SubmitOrder,
)
from .persistence import JournalError
from .deficit import AcknowledgeDeficit, CheckDeficits, DeficitQuotes, RetryDeficit, StartDeficit
from .plans import (AcknowledgePlan, CheckPlanDeadlines, ClosePlan, FailurePolicy,
                    PlanLeg, PlanMarks, PlanPrice, PlanProtection, RetryPlan, SubmitPlan, TradePlan,
                    TrailingStop, UpdatePlanProtection, ProtectPositions, ReleasePositionProtection)
from .inventory import BrokerPosition, CloseManual, ExternalOrder, InventoryReport, OpeningBalance, TransferPosition
from .inventory import RepairExecutions, RepairOrderTarget
from .capital import AccountCapital, AccountKey
from .admission import (
    AccountLimit, CapitalPolicy, ConfigureCapital, RouteGrant, RouteValuation, StrategyLimit,
)
from .rebalance import AllocationLeg, CapitalSnapshot, CapitalTarget, ExternalCapital, RebalanceCapital

_TYPES = {cls.__name__: cls for cls in (
    RepairExecutions, RepairOrderTarget,
    PlanProtection, TrailingStop, UpdatePlanProtection, ProtectPositions, ReleasePositionProtection,
    AcknowledgeDeficit, CheckDeficits, DeficitQuotes, RetryDeficit, StartDeficit,
    AcknowledgePlan, CheckPlanDeadlines, ClosePlan, PlanLeg, PlanMarks, PlanPrice, RetryPlan, SubmitPlan, TradePlan,
    BrokerPosition, CloseManual, ExternalOrder, InventoryReport, OpeningBalance, TransferPosition,
    AccountCapital, AllocationLeg, CapitalSnapshot, CapitalTarget, ExternalCapital, RebalanceCapital,
    AccountKey, AccountLimit, CapitalPolicy, ConfigureCapital, RouteGrant, RouteValuation, StrategyLimit,
    InstrumentId, Instrument, Route, OrderIntent, OrderSnapshot, SubmitOrder, CancelOrder, ReplaceOrder,
    GatewaySubmit, GatewayCancel, GatewayAccepted, GatewayCancelled, GatewayCancelRejected, GatewayRejected, GatewayReconciled, GatewayUncertain, Execution,
)}


def encode(value: object) -> Any:
    if isinstance(value, datetime):
        return {'datetime': value.isoformat()}
    if isinstance(value, tuple):
        return {'tuple': [encode(item) for item in value]}
    if isinstance(value, Enum):
        return {'enum': type(value).__name__, 'value': value.value}
    if is_dataclass(value) and not isinstance(value, type):
        if type(value).__name__ not in _TYPES:
            raise JournalError('unsupported durable message')
        return {'type': type(value).__name__,
                'fields': {field.name: encode(getattr(value, field.name)) for field in fields(value)}}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise JournalError('unsupported durable value')


def decode(value: Any) -> Any:
    if not isinstance(value, dict):
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise JournalError('invalid durable value')
    try:
        if set(value) == {'datetime'}:
            result = datetime.fromisoformat(value['datetime'])
            if result.utcoffset() is None:
                raise ValueError('naive durable time')
            return result
        if set(value) == {'tuple'}:
            return tuple(decode(item) for item in value['tuple'])
        if set(value) == {'enum', 'value'}:
            enum = {'Side': Side, 'OrderStatus': OrderStatus, 'FailurePolicy': FailurePolicy}[value['enum']]
            return enum(value['value'])
        if set(value) == {'type', 'fields'}:
            cls = _TYPES[value['type']]
            arguments = value['fields']
            expected = {field.name for field in fields(cls)}
            # Exact historical TradePlan shape from iterations 10/11 only. Keep
            # required fields and all other journal types strict.
            if cls is TradePlan and set(arguments) == expected - {'take_profit', 'trailing'}:
                arguments = {**arguments, 'take_profit': None, 'trailing': None}
            if set(arguments) != expected:
                raise ValueError('invalid durable fields')
            return cls(**{key: decode(item) for key, item in arguments.items()})
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise JournalError('invalid durable value') from error
    raise JournalError('unknown durable type')

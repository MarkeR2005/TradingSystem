"""Pure aggregate-P&L protection. Thresholds and peak use exact arithmetic."""

from fractions import Fraction

from .capital import _exact
from .plans import PlanProtection, ProtectionKind


def evaluate_protection(
    policy: PlanProtection, pnl: Fraction, previous_peak: Fraction | None,
) -> tuple[Fraction, tuple[ProtectionKind, Fraction] | None]:
    peak = pnl if previous_peak is None else max(previous_peak, pnl)
    stop = -_exact(policy.stop_loss)
    if pnl <= stop:
        return peak, (ProtectionKind.STOP_LOSS, stop)
    if policy.take_profit is not None and pnl >= _exact(policy.take_profit):
        return peak, (ProtectionKind.TAKE_PROFIT, _exact(policy.take_profit))
    trailing = policy.trailing
    if trailing is not None and peak >= _exact(trailing.activation):
        floor = peak - _exact(trailing.distance)
        if pnl <= floor:
            return peak, (ProtectionKind.TRAILING, floor)
    return peak, None

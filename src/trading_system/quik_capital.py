"""Pure, read-only futures resource assessment; not an OMS capital source.

CBPLPLANNED is broker-reported free funds (QUIK support, topic4158, reply 3).
Only explicitly unsubmitted local margin may be deducted. Once submission is
possible, an independent cutover is required; two matching reads do not supply it.
"""

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Context, Decimal, localcontext
from enum import Enum
import re
from typing import Any

from .domain import utc
from .persistence import canonical
from .quik_contract import identifier
from .quik_limits import FuturesLimitKey, build_limits_report
from .quik_snapshot import TABLES, rows


@dataclass(frozen=True)
class FuturesResourcePolicy:
    profile: str
    quik_version: str
    selection: FuturesLimitKey
    trading_date: date
    max_age: timedelta
    version: str = field(default='quik.cbplplanned.v1', init=False)
    currency: str = field(default='RUB', init=False)

    def __post_init__(self) -> None:
        identifier(self.profile)
        if not isinstance(self.quik_version, str) or not re.fullmatch(r'[0-9]{1,6}(?:\.[0-9]{1,6}){3}', self.quik_version):
            raise ValueError('resource policy requires an exact four-component QUIK version')
        if (not isinstance(self.selection, FuturesLimitKey) or self.selection.limit_type != 0
                or self.selection.currcode not in ('SUR', 'RUB')):
            raise ValueError('resource policy requires an exact type-0 SUR or RUB key')
        if type(self.trading_date) is not date:
            raise ValueError('resource policy requires an explicit trading date')
        if not isinstance(self.max_age, timedelta) or self.max_age <= timedelta(0):
            raise ValueError('resource policy requires a positive observation lifetime')


class ReservationState(str, Enum):
    LOCAL_ONLY = 'local_only'
    SUBMITTED_UNRECONCILED = 'submitted_unreconciled'


@dataclass(frozen=True)
class LocalMarginReservation:
    """Explicit monetary margin, never CapitalUsage notional or instrument GO.

    LOCAL_ONLY asserts the obligation could not have reached the broker. Any
    submitted, partially filled, cancelled-but-unconfirmed or recovered unknown
    obligation remains SUBMITTED_UNRECONCILED. This API cannot release it.
    """

    reservation_id: str
    selection: FuturesLimitKey
    currency: str
    amount: Decimal
    state: ReservationState

    def __post_init__(self) -> None:
        identifier(self.reservation_id)
        if not isinstance(self.selection, FuturesLimitKey):
            raise ValueError('reservation requires an exact account/limit key')
        if not isinstance(self.currency, str) or not re.fullmatch(r'[A-Z]{3}', self.currency):
            raise ValueError('reservation requires an explicit currency')
        if not isinstance(self.state, ReservationState):
            raise ValueError('reservation requires a typed submission state')
        # Bound before fixed-point formatting (huge Decimal exponents must not
        # allocate enormous strings). Input and sum remain exact, even if the
        # caller changes the process-wide Decimal precision or rounding mode.
        if (not isinstance(self.amount, Decimal) or not self.amount.is_finite()
                or self.amount < 0 or abs(self.amount.adjusted()) > 80
                or len(self.amount.as_tuple().digits) > 80
                or len(format(self.amount, 'f')) > 80):
            raise ValueError('reservation margin must be a bounded nonnegative Decimal')


@dataclass(frozen=True)
class FuturesResourceAssessment:
    policy_version: str
    selection: FuturesLimitKey
    currency: str
    evaluated_at: datetime
    connection_id: str | None
    observed_at: datetime | None
    broker_free: Decimal | None
    local_reserved: Decimal | None
    candidate_free: Decimal | None
    issues: tuple[str, ...]
    eligible_for_capital: bool = field(default=False, init=False)
    eligible_for_trading: bool = field(default=False, init=False)
    reconciled: bool = field(default=False, init=False)

    @property
    def complete(self) -> bool:
        """A complete calculation is not reconciliation or trading permission."""
        return not self.issues


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError('snapshot.invalid_time')
    try:
        return utc(datetime.fromisoformat(value))
    except (ValueError, OverflowError) as error:
        raise ValueError('snapshot.invalid_time') from error


def _same_json(left: Any, right: Any) -> bool:
    try:
        return canonical(left) == canonical(right)
    except (ValueError, TypeError, OverflowError):
        return False


def _observe(snapshot: dict[str, Any], policy: FuturesResourcePolicy, now: datetime) -> tuple[Decimal, datetime, str]:
    if (not isinstance(snapshot, dict) or type(snapshot.get('schema')) is not int or snapshot['schema'] != 1
            or snapshot.get('mode') != 'read_only' or snapshot.get('issues') != []
            or snapshot.get('repeatable_observation') is not True or snapshot.get('reconciled') is not False):
        raise ValueError('snapshot.unverified')
    runtime = snapshot.get('runtime')
    expected = {'protocol': 1, 'mode': 'read_only', 'profile': policy.profile,
                'quik_version': policy.quik_version, 'lua_version': 'Lua 5.4'}
    if (not isinstance(runtime, dict) or type(runtime.get('protocol')) is not int
            or any(runtime.get(key) != value for key, value in expected.items())):
        raise ValueError('snapshot.runtime_mismatch')
    try:
        connection = identifier(snapshot.get('connection_id'))
    except ValueError as error:
        raise ValueError('snapshot.invalid_connection') from error
    started, finished = _timestamp(snapshot.get('started_at')), _timestamp(snapshot.get('finished_at'))
    if not started <= finished <= now:
        raise ValueError('snapshot.invalid_time')
    if now - started >= policy.max_age:
        raise ValueError('snapshot.expired')
    expected_date = policy.trading_date.strftime('%d.%m.%Y')
    if any(snapshot.get(key) != expected_date for key in ('trading_date_before', 'trading_date_after')):
        raise ValueError('snapshot.trading_date_mismatch')
    classes = snapshot.get('classes')
    if (not isinstance(classes, list) or any(not isinstance(c, str) for c in classes)
            or 'SPBFUT' not in classes or len(classes) != len(set(classes))):
        raise ValueError('snapshot.no_spbfut')
    counters = (snapshot.get('callbacks_before'), snapshot.get('callbacks_after'))
    if any(type(value) is not int or not 0 <= value <= 2**63 - 1 for value in counters):
        raise ValueError('snapshot.invalid_callbacks')
    if counters[0] != counters[1]:
        raise ValueError('snapshot.callbacks_changed')
    financial = snapshot.get('financial')
    if financial is not None and (not isinstance(financial, dict) or financial.get('complete') is not True
                                 or financial.get('repeatable_observation') is not True or financial.get('issues') != []):
        raise ValueError('snapshot.financial_unverified')
    passes: list[dict[str, list[dict[str, Any]]]] = []
    try:
        for name in ('previous_tables', 'tables'):
            table_set = snapshot.get(name)
            if not isinstance(table_set, dict):
                raise ValueError('missing tables')
            passes.append({key: rows(table_set.get(key), key) for key in TABLES})
    except (ValueError, TypeError, OverflowError) as error:
        raise ValueError('snapshot.invalid_tables') from error
    if not _same_json(passes[0], passes[1]):
        raise ValueError('snapshot.tables_changed')
    report = snapshot.get('limits')
    if not isinstance(report, dict) or not _same_json(report.get('selection'), asdict(policy.selection)):
        raise ValueError('limits.selection_mismatch')
    before, after = passes
    rebuilt = build_limits_report(before['accounts'], before['limits'], after['accounts'], after['limits'],
                                  selection=policy.selection)
    if not rebuilt['selection_verified']:
        raise ValueError('limits.unverified')
    if (not _same_json(report, rebuilt) or report.get('selection_verified') is not True
            or report.get('complete') is not True or report.get('repeatable_observation') is not True
            or any(report.get(key) is not False for key in ('reconciled', 'eligible_for_capital', 'eligible_for_trading'))):
        raise ValueError('limits.report_mismatch')
    return Decimal(rebuilt['selected_observation']['values']['cbplplanned']), started, connection


def assess_futures_resource(snapshot: dict[str, Any], policy: FuturesResourcePolicy, *, now: datetime,
                            reservations: tuple[LocalMarginReservation, ...]) -> FuturesResourceAssessment:
    """Assess trusted capture data at an explicit instant; performs no I/O.

    The caller must supply a complete local reservation set for the selected
    resource. Empty is explicit, never inferred from empty broker tables. The
    caller's local-only declarations are assumptions, not evidence of cutover.
    Invalid configuration raises ValueError; unusable observations return issues.
    """
    if not isinstance(policy, FuturesResourcePolicy) or not isinstance(now, datetime):
        raise ValueError('assessment requires a typed policy and aware evaluation time')
    now = utc(now)
    if (not isinstance(reservations, tuple) or len(reservations) > 50000
            or any(not isinstance(item, LocalMarginReservation) for item in reservations)):
        raise ValueError('assessment requires a bounded immutable tuple of typed reservations')
    broker_free: Decimal | None = None
    local_reserved: Decimal | None = None
    candidate: Decimal | None = None
    observed_at: datetime | None = None
    connection: str | None = None
    issues: list[str] = []
    try:
        broker_free, observed_at, connection = _observe(snapshot, policy, now)
    except ValueError as error:
        issues.append(str(error))
    if len({item.reservation_id for item in reservations}) != len(reservations):
        issues.append('reservations.duplicate_id')
    if any(item.selection != policy.selection for item in reservations):
        issues.append('reservations.selection_mismatch')
    if any(item.currency != policy.currency for item in reservations):
        issues.append('reservations.currency_mismatch')
    if any(item.state is ReservationState.SUBMITTED_UNRECONCILED for item in reservations):
        issues.append('reservations.cutover_required')
    if not issues:
        assert broker_free is not None
        # At most 50,000 fixed-point inputs of <=80 characters. 200 significant
        # digits cover the entire sum/subtraction without rounding or float casts.
        with localcontext(Context(prec=200)):
            local_reserved = sum((item.amount for item in reservations), Decimal(0))
            candidate = broker_free - local_reserved
    return FuturesResourceAssessment(policy.version, policy.selection, policy.currency, now,
                                     connection, observed_at, broker_free, local_reserved, candidate, tuple(issues))

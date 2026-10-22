"""Durable local comparison receipts. None of these values grants trading permission."""

from dataclasses import dataclass
from datetime import datetime
import re

from .domain import Route, utc

BROKER_LIMITATIONS = ('broker_atomicity_unverified', 'history_coverage_unverified',
                      'position_cost_basis_unverified', 'gateway_cutover_unverified')


@dataclass(frozen=True)
class QuikReconciliationCheckpoint:
    operation_id: str
    adoption_id: str
    gateway_id: str
    session_id: str
    oms_revision: int
    oms_record_count: int
    oms_journal_digest: str
    source_revision: int
    gateway_journal_digest: str
    source_digest: str
    snapshot_digest: str
    policy_digest: str
    observed_at: datetime
    created_at: datetime
    expires_at: datetime
    routes: tuple[Route, ...]
    order_count: int
    execution_count: int

    def __post_init__(self) -> None:
        if any(not isinstance(s, str) or not s.strip() for s in
               (self.operation_id, self.adoption_id, self.gateway_id, self.session_id)):
            raise ValueError('checkpoint identifiers must be nonempty')
        if self.operation_id == self.adoption_id:
            raise ValueError('checkpoint and adoption must have different identifiers')
        if any(type(n) is not int or n < 0 for n in (self.oms_revision, self.oms_record_count,
                self.source_revision, self.order_count, self.execution_count)):
            raise ValueError('checkpoint revisions and counts must be nonnegative integers')
        if self.oms_revision > self.oms_record_count:
            raise ValueError('checkpoint requires a fully journaled OMS revision')
        for digest in (self.oms_journal_digest, self.gateway_journal_digest, self.source_digest,
                       self.snapshot_digest, self.policy_digest):
            if not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None:
                raise ValueError('checkpoint requires SHA-256 digests')
        for name in ('observed_at', 'created_at', 'expires_at'):
            object.__setattr__(self, name, utc(getattr(self, name)))
        if not self.observed_at <= self.created_at <= self.expires_at or self.observed_at == self.expires_at:
            raise ValueError('invalid checkpoint observation window')
        if (not isinstance(self.routes, tuple) or not self.routes
                or not all(isinstance(r, Route) for r in self.routes)
                or len(set(self.routes)) != len(self.routes)
                or any(r.instrument.id.gateway_id != self.gateway_id for r in self.routes)):
            raise ValueError('checkpoint requires distinct routes of one gateway')


@dataclass(frozen=True)
class ReconciliationStatus:
    """Current means unchanged local evidence, not broker reconciliation or permission."""

    checkpoint: QuikReconciliationCheckpoint
    invalidated_by: tuple[str, ...]

    @property
    def current(self) -> bool:
        return not self.invalidated_by

    @property
    def blockers(self) -> tuple[str, ...]:
        return self.invalidated_by + BROKER_LIMITATIONS

    @property
    def reconciled(self) -> bool:
        return False

    @property
    def eligible_for_trading(self) -> bool:
        return False

    @property
    def eligible_for_capital(self) -> bool:
        return False

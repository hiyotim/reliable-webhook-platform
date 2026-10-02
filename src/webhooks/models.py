"""Database model for the webhook platform.

Three tables carry the whole delivery state machine:

``webhook_endpoints``  where events are delivered (url + HMAC secret)
``events``             durable record of a submitted event (payload, status)
``delivery_attempts``  one row per delivery try; the job queue itself

Job coordination lives entirely in ``delivery_attempts``: a claim is an
``UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)`` that flips
``state`` to ``in_progress`` and stamps a lease. No external broker is needed and
crash recovery falls out of lease expiry.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class EndpointState(enum.StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DELETED = "deleted"


class EventStatus(enum.StrEnum):
    """Lifecycle of a submitted event."""

    PENDING = "pending"  # queued or waiting for its next backoff slot
    IN_PROGRESS = "in_progress"  # an attempt is currently claimed
    DELIVERED = "delivered"  # terminal success
    DEAD_LETTER = "dead_letter"  # terminal failure, retries exhausted


class AttemptState(enum.StrEnum):
    """Lifecycle of a single delivery try."""

    PENDING = "pending"  # scheduled, not yet claimed
    IN_PROGRESS = "in_progress"  # claimed by a worker, lease held
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _enum_values(enum_cls: type[enum.Enum]) -> str:
    return ", ".join(f"'{member.value}'" for member in enum_cls)


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"
    __table_args__ = (
        CheckConstraint("length(name) > 0", name="ck_endpoints_name_not_empty"),
        CheckConstraint("url ~ '^https?://'", name="ck_endpoints_url_scheme"),
        CheckConstraint(
            "state IN ('active', 'disabled', 'deleted')",
            name="ck_endpoints_state",
        ),
        Index("ix_endpoints_state", "state"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    # Plaintext secret: it is required to compute outbound HMAC signatures.
    # The API never serialises it after creation and logs never include it.
    secret: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Application-managed (not a server-side ON UPDATE default): the service layer
    # stamps it from the database clock, so a mutated row never leaves an expired
    # attribute that would need lazy IO later.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    events: Mapped[list[Event]] = relationship(back_populates="endpoint")

    @property
    def is_deliverable(self) -> bool:
        return self.state == EndpointState.ACTIVE.value


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        # Idempotent submission: one event per (endpoint, idempotency key).
        # NULL keys are always distinct in PostgreSQL, so keyless submits are
        # never deduplicated against each other.
        UniqueConstraint("endpoint_id", "idempotency_key", name="uq_events_endpoint_idempotency"),
        CheckConstraint(
            f"status IN ({_enum_values(EventStatus)})",
            name="ck_events_status",
        ),
        CheckConstraint("max_attempts >= 1", name="ck_events_max_attempts"),
        CheckConstraint("attempt_count >= 0", name="ck_events_attempt_count"),
        CheckConstraint("length(event_type) > 0", name="ck_events_event_type_not_empty"),
        Index("ix_events_status_created", "status", "created_at"),
        Index("ix_events_endpoint_created", "endpoint_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("webhook_endpoints.id", ondelete="RESTRICT"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text(f"'{EventStatus.PENDING.value}'")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dead_lettered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    endpoint: Mapped[WebhookEndpoint] = relationship(back_populates="events")
    attempts: Mapped[list[DeliveryAttempt]] = relationship(
        back_populates="event", order_by="DeliveryAttempt.attempt_number", lazy="selectin"
    )


class DeliveryAttempt(Base):
    """One delivery try: row per attempt, and the queue row workers claim."""

    __tablename__ = "delivery_attempts"
    __table_args__ = (
        UniqueConstraint("event_id", "attempt_number", name="uq_attempts_event_number"),
        # At most one claimable attempt per event: submitting or scheduling the
        # next attempt concurrently can never fork an event into two live jobs.
        Index(
            "uq_attempts_active_per_event",
            "event_id",
            unique=True,
            postgresql_where=text("state IN ('pending', 'in_progress')"),
        ),
        # Claim/backoff scan: pending work ordered by schedule.
        Index(
            "ix_attempts_claimable",
            "scheduled_at",
            postgresql_where=text("state = 'pending'"),
        ),
        # Lease expiry scan for crashed workers.
        Index(
            "ix_attempts_lease",
            "lease_expires_at",
            postgresql_where=text("state = 'in_progress'"),
        ),
        CheckConstraint(f"state IN ({_enum_values(AttemptState)})", name="ck_attempts_state"),
        CheckConstraint("attempt_number >= 1", name="ck_attempts_number"),
        CheckConstraint(
            "state <> 'in_progress' OR (lease_expires_at IS NOT NULL AND worker_id IS NOT NULL)",
            name="ck_attempts_lease_required",
        ),
        CheckConstraint(
            "state NOT IN ('succeeded', 'failed') OR finished_at IS NOT NULL",
            name="ck_attempts_finished_required",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text(f"'{AttemptState.PENDING.value}'")
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Number of times the lease expired before this attempt finished.
    recoveries: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_snippet: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    event: Mapped[Event] = relationship(back_populates="attempts")

    @property
    def is_terminal(self) -> bool:
        return self.state in (AttemptState.SUCCEEDED.value, AttemptState.FAILED.value)


__all__ = [
    "AttemptState",
    "Base",
    "DeliveryAttempt",
    "EndpointState",
    "Event",
    "EventStatus",
    "WebhookEndpoint",
]

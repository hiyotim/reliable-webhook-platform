"""Business logic: endpoints, events, and the delivery-attempt queue.

The claim path is the heart of the system. It is a single SQL statement so that
two workers racing for the same attempt cannot both win it:

* ``FOR UPDATE SKIP LOCKED`` makes concurrent claims pick disjoint rows,
* the row only becomes claimable again through lease expiry, which is what makes
  a crashed worker recoverable,
* every completion is fenced on ``worker_id`` + ``state`` so a worker whose lease
  was reclaimed can never overwrite the newer owner's result.
"""

from __future__ import annotations

import logging
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from sqlalchemy import bindparam, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from webhooks.backoff import BackoffPolicy
from webhooks.models import (
    AttemptState,
    DeliveryAttempt,
    EndpointState,
    Event,
    EventStatus,
    WebhookEndpoint,
)

logger = logging.getLogger(__name__)

SECRET_BYTES = 32
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200


def generate_secret() -> str:
    """Create a per-endpoint HMAC secret (URL-safe, 256 bits of entropy)."""
    return secrets.token_urlsafe(SECRET_BYTES)


class ServiceError(Exception):
    """Base class for domain errors surfaced as HTTP errors by the API."""


class NotFoundError(ServiceError):
    pass


class ConflictError(ServiceError):
    pass


class ValidationError(ServiceError):
    pass


async def database_now(session: AsyncSession) -> datetime:
    """Current database timestamp.

    Scheduling timestamps come from the database clock so that lease expiry,
    backoff and ``now()``-based claim predicates all agree even when workers run
    on hosts with drifting clocks.
    """
    value = await session.scalar(text("SELECT now()"))
    assert isinstance(value, datetime)
    return value


@dataclass(frozen=True, slots=True)
class ClaimedAttempt:
    """Everything a worker needs to perform one delivery."""

    attempt_id: int
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    attempt_number: int
    event_type: str
    payload: dict[str, Any]
    url: str
    secret: str
    endpoint_state: str
    recovered: bool


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
async def create_endpoint(session: AsyncSession, *, name: str, url: str) -> WebhookEndpoint:
    endpoint = WebhookEndpoint(name=name.strip(), url=url, secret=generate_secret())
    session.add(endpoint)
    await session.flush()
    logger.info("endpoint created", extra={"endpoint_id": str(endpoint.id)})
    return endpoint


async def get_endpoint(session: AsyncSession, endpoint_id: uuid.UUID) -> WebhookEndpoint:
    endpoint = await session.get(WebhookEndpoint, endpoint_id)
    if endpoint is None:
        raise NotFoundError("endpoint not found")
    return endpoint


async def list_endpoints(
    session: AsyncSession,
    *,
    include_deleted: bool = False,
    limit: int = DEFAULT_LIST_LIMIT,
    offset: int = 0,
) -> tuple[list[WebhookEndpoint], int]:
    statement = select(WebhookEndpoint).order_by(WebhookEndpoint.created_at.desc())
    count_statement = select(text("count(*)")).select_from(WebhookEndpoint)
    if not include_deleted:
        statement = statement.where(WebhookEndpoint.state != EndpointState.DELETED.value)
        count_statement = count_statement.where(
            WebhookEndpoint.state != EndpointState.DELETED.value
        )
    result = await session.scalars(statement.limit(limit).offset(offset))
    total = await session.scalar(count_statement)
    return list(result), int(total or 0)


async def update_endpoint(
    session: AsyncSession,
    endpoint_id: uuid.UUID,
    *,
    name: str | None = None,
    url: str | None = None,
    is_active: bool | None = None,
) -> WebhookEndpoint:
    endpoint = await get_endpoint(session, endpoint_id)
    if endpoint.state == EndpointState.DELETED.value:
        raise ConflictError("endpoint is deleted")
    if name is not None:
        endpoint.name = name.strip()
    if url is not None:
        endpoint.url = url
    if is_active is not None:
        endpoint.state = EndpointState.ACTIVE.value if is_active else EndpointState.DISABLED.value
    endpoint.updated_at = await database_now(session)
    await session.flush()
    return endpoint


async def delete_endpoint(session: AsyncSession, endpoint_id: uuid.UUID) -> None:
    """Soft-delete: delivery history stays inspectable, new events are rejected."""
    endpoint = await get_endpoint(session, endpoint_id)
    if endpoint.state == EndpointState.DELETED.value:
        return
    now = await database_now(session)
    endpoint.state = EndpointState.DELETED.value
    endpoint.deleted_at = now
    endpoint.updated_at = now
    await session.flush()
    logger.info("endpoint deleted", extra={"endpoint_id": str(endpoint_id)})


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #
async def submit_event(
    session: AsyncSession,
    *,
    endpoint_id: uuid.UUID,
    event_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
    default_max_attempts: int,
    max_attempts: int | None = None,
) -> tuple[Event, bool]:
    """Store an event and schedule its first attempt.

    Returns ``(event, created)``; ``created=False`` means the idempotency key was
    already used and the original event is returned unchanged.
    """
    endpoint = await get_endpoint(session, endpoint_id)
    if endpoint.state == EndpointState.DELETED.value:
        raise ConflictError("endpoint is deleted")
    if endpoint.state == EndpointState.DISABLED.value:
        raise ConflictError("endpoint is disabled")

    if idempotency_key is not None:
        existing = await session.scalar(
            select(Event).where(
                Event.endpoint_id == endpoint_id,
                Event.idempotency_key == idempotency_key,
            )
        )
        if existing is not None:
            return existing, False

    event = Event(
        endpoint_id=endpoint_id,
        event_type=event_type,
        payload=payload,
        idempotency_key=idempotency_key,
        status=EventStatus.PENDING.value,
        attempt_count=1,
        max_attempts=max_attempts or default_max_attempts,
    )
    now = await database_now(session)
    event.next_attempt_at = now
    try:
        async with session.begin_nested():
            session.add(event)
            await session.flush()
            session.add(
                DeliveryAttempt(
                    event_id=event.id,
                    attempt_number=1,
                    state=AttemptState.PENDING.value,
                    scheduled_at=now,
                )
            )
            await session.flush()
    except IntegrityError:
        # Lost an idempotency race with a concurrent submit: the unique
        # constraint is what makes the submission idempotent, not the pre-read.
        if idempotency_key is None:
            raise
        existing = await session.scalar(
            select(Event).where(
                Event.endpoint_id == endpoint_id,
                Event.idempotency_key == idempotency_key,
            )
        )
        if existing is None:
            raise
        return existing, False

    await session.flush()
    logger.info(
        "event submitted",
        extra={
            "event_id": str(event.id),
            "endpoint_id": str(endpoint_id),
            "event_type": event_type,
            "attempt_number": 1,
            "idempotency_key": idempotency_key,
        },
    )
    return event, True


async def get_event(session: AsyncSession, event_id: uuid.UUID) -> Event:
    event = await session.get(Event, event_id)
    if event is None:
        raise NotFoundError("event not found")
    return event


async def list_events(
    session: AsyncSession,
    *,
    status: str | None = None,
    endpoint_id: uuid.UUID | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    offset: int = 0,
) -> tuple[list[Event], int]:
    statement = select(Event).order_by(Event.created_at.desc())
    count_statement = select(text("count(*)")).select_from(Event)
    if status is not None:
        statement = statement.where(Event.status == status)
        count_statement = count_statement.where(Event.status == status)
    if endpoint_id is not None:
        statement = statement.where(Event.endpoint_id == endpoint_id)
        count_statement = count_statement.where(Event.endpoint_id == endpoint_id)

    events = list(await session.scalars(statement.limit(limit).offset(offset)))
    total = await session.scalar(count_statement)
    return events, int(total or 0)


async def list_attempts(session: AsyncSession, event_id: uuid.UUID) -> list[DeliveryAttempt]:
    result = await session.scalars(
        select(DeliveryAttempt)
        .where(DeliveryAttempt.event_id == event_id)
        .order_by(DeliveryAttempt.attempt_number)
    )
    return list(result)


async def retry_event(
    session: AsyncSession,
    event_id: uuid.UUID,
    *,
    default_max_attempts: int,
    max_attempts: int | None = None,
) -> Event:
    """Requeue a dead-lettered (or otherwise stuck) event for a fresh attempt.

    A manual retry grants a fresh retry budget counted from the new attempt, so a
    destination that was fixed after the event dead-lettered gets the normal
    policy again rather than a single shot.
    """
    event = await session.scalar(select(Event).where(Event.id == event_id).with_for_update())
    if event is None:
        raise NotFoundError("event not found")
    if event.status == EventStatus.DELIVERED.value:
        raise ConflictError("event is already delivered")
    if event.status == EventStatus.IN_PROGRESS.value:
        raise ConflictError("event has an attempt in flight")

    active = await session.scalar(
        select(DeliveryAttempt.id)
        .where(
            DeliveryAttempt.event_id == event_id,
            DeliveryAttempt.state.in_([AttemptState.PENDING.value, AttemptState.IN_PROGRESS.value]),
        )
        .limit(1)
    )
    if active is not None:
        raise ConflictError("event already has a queued attempt")

    last_number = await session.scalar(
        select(text("coalesce(max(attempt_number), 0)"))
        .select_from(DeliveryAttempt)
        .where(DeliveryAttempt.event_id == event_id)
    )
    new_number = int(last_number or 0) + 1
    budget = max_attempts or default_max_attempts
    now = await database_now(session)
    event.status = EventStatus.PENDING.value
    event.dead_lettered_at = None
    event.delivered_at = None
    event.last_error = None
    event.attempt_count = event.attempt_count + 1
    event.max_attempts = new_number + budget - 1
    event.next_attempt_at = now
    event.updated_at = now
    session.add(
        DeliveryAttempt(
            event_id=event.id,
            attempt_number=new_number,
            state=AttemptState.PENDING.value,
            scheduled_at=now,
        )
    )
    await session.flush()
    logger.info(
        "event manually requeued",
        extra={"event_id": str(event_id), "attempt_number": new_number},
    )
    return event


# --------------------------------------------------------------------------- #
# Delivery queue
# --------------------------------------------------------------------------- #
_CLAIM_SQL = text(
    """
    UPDATE delivery_attempts AS a
       SET state = 'in_progress',
           worker_id = :worker_id,
           lease_expires_at = now() + make_interval(secs => :lease_seconds),
           started_at = now(),
           recoveries = a.recoveries + CASE WHEN c.previous_state = 'in_progress' THEN 1 ELSE 0 END
      FROM (
            SELECT id, state AS previous_state
              FROM delivery_attempts
             WHERE (state = 'pending' AND scheduled_at <= now())
                OR (state = 'in_progress' AND lease_expires_at <= now())
             ORDER BY scheduled_at, id
             FOR UPDATE SKIP LOCKED
             LIMIT :batch_size
           ) AS c
     WHERE a.id = c.id
    RETURNING a.id AS attempt_id,
              a.event_id,
              a.attempt_number,
              a.recoveries,
              (c.previous_state = 'in_progress') AS recovered
    """
)

_FETCH_CLAIMED_SQL = text(
    """
    SELECT a.id AS attempt_id,
           a.event_id,
           a.attempt_number,
           e.event_type,
           e.payload,
           ep.id AS endpoint_id,
           ep.url,
           ep.secret,
           ep.state AS endpoint_state
      FROM delivery_attempts a
      JOIN events e ON e.id = a.event_id
      JOIN webhook_endpoints ep ON ep.id = e.endpoint_id
     WHERE a.id IN :attempt_ids
     ORDER BY a.id
    """
).bindparams(bindparam("attempt_ids", expanding=True))

_MARK_EVENTS_IN_PROGRESS_SQL = text(
    """
    UPDATE events
       SET status = 'in_progress', next_attempt_at = NULL, updated_at = now()
     WHERE id IN :event_ids AND status = 'pending'
    """
).bindparams(bindparam("event_ids", expanding=True))

_SUCCESS_SQL = text(
    """
    UPDATE delivery_attempts
       SET state = 'succeeded',
           finished_at = now(),
           response_status = :response_status,
           response_snippet = :response_snippet,
           error = NULL,
           duration_ms = :duration_ms,
           lease_expires_at = NULL
     WHERE id = :attempt_id AND state = 'in_progress' AND worker_id = :worker_id
    RETURNING event_id
    """
)

_FAILURE_SQL = text(
    """
    UPDATE delivery_attempts
       SET state = 'failed',
           finished_at = now(),
           response_status = :response_status,
           response_snippet = :response_snippet,
           error = :error,
           duration_ms = :duration_ms,
           lease_expires_at = NULL
     WHERE id = :attempt_id AND state = 'in_progress' AND worker_id = :worker_id
    RETURNING event_id, attempt_number
    """
)

_READ_EVENT_FOR_UPDATE_SQL = text(
    "SELECT id, status, max_attempts FROM events WHERE id = :id FOR UPDATE"
)

_DEAD_LETTER_SQL = text(
    """
    UPDATE events
       SET status = 'dead_letter',
           dead_lettered_at = now(),
           next_attempt_at = NULL,
           last_error = :error,
           updated_at = now()
     WHERE id = :id
    """
)

_SCHEDULE_RETRY_SQL = text(
    """
    UPDATE events
       SET status = 'pending',
           next_attempt_at = now() + make_interval(secs => :delay_seconds),
           last_error = :error,
           attempt_count = attempt_count + 1,
           updated_at = now()
     WHERE id = :id
    """
)

_INSERT_NEXT_ATTEMPT_SQL = text(
    """
    INSERT INTO delivery_attempts (event_id, attempt_number, state, scheduled_at)
    VALUES (:event_id, :attempt_number, 'pending', now() + make_interval(secs => :delay_seconds))
    """
)


@dataclass(frozen=True, slots=True)
class FailureOutcome:
    """What happened after a failed attempt."""

    action: Literal["retried", "dead_letter", "lease_lost"]
    next_attempt_number: int | None = None
    retry_in_seconds: float | None = None


async def claim_attempts(
    session: AsyncSession,
    *,
    worker_id: str,
    batch_size: int,
    lease_seconds: float,
) -> list[ClaimedAttempt]:
    """Atomically claim up to ``batch_size`` due or lease-expired attempts."""
    rows = (
        await session.execute(
            _CLAIM_SQL,
            {
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
                "batch_size": batch_size,
            },
        )
    ).mappings()
    claims = [dict(row) for row in rows]
    if not claims:
        return []

    recovered = [claim for claim in claims if claim["recovered"]]
    if recovered:
        logger.warning(
            "recovered attempts after lease expiry",
            extra={"worker_id": worker_id, "attempt_ids": [c["attempt_id"] for c in recovered]},
        )

    event_ids = sorted({claim["event_id"] for claim in claims})
    attempt_ids = [claim["attempt_id"] for claim in claims]
    await session.execute(_MARK_EVENTS_IN_PROGRESS_SQL, {"event_ids": event_ids})

    # The claim statement already told us which rows were recovered; carry that
    # through rather than re-reading per-row state after the update.
    detail_rows = (
        await session.execute(_FETCH_CLAIMED_SQL, {"attempt_ids": attempt_ids})
    ).mappings()
    recovered_ids = {claim["attempt_id"] for claim in claims if claim["recovered"]}

    claimed: list[ClaimedAttempt] = []
    for row in detail_rows:
        claimed.append(
            ClaimedAttempt(
                attempt_id=row["attempt_id"],
                event_id=row["event_id"],
                endpoint_id=row["endpoint_id"],
                attempt_number=row["attempt_number"],
                event_type=row["event_type"],
                payload=row["payload"],
                url=row["url"],
                secret=row["secret"],
                endpoint_state=row["endpoint_state"],
                recovered=row["attempt_id"] in recovered_ids,
            )
        )
    return claimed


async def mark_attempt_succeeded(
    session: AsyncSession,
    *,
    attempt_id: int,
    worker_id: str,
    response_status: int | None,
    response_snippet: str | None,
    duration_ms: int,
) -> bool:
    """Mark the attempt delivered.

    Returns ``False`` when the lease was lost (another worker now owns the
    attempt and this result is discarded).
    """
    row = (
        await session.execute(
            _SUCCESS_SQL,
            {
                "attempt_id": attempt_id,
                "worker_id": worker_id,
                "response_status": response_status,
                "response_snippet": response_snippet,
                "duration_ms": duration_ms,
            },
        )
    ).first()
    if row is None:
        logger.warning(
            "discarded success for attempt no longer owned",
            extra={"attempt_id": attempt_id, "worker_id": worker_id},
        )
        return False

    event_id = row[0]
    await session.execute(
        text(
            """
            UPDATE events
               SET status = 'delivered',
                   delivered_at = now(),
                   next_attempt_at = NULL,
                   last_error = NULL,
                   updated_at = now()
             WHERE id = :id AND status <> 'delivered'
            """
        ),
        {"id": event_id},
    )
    logger.info(
        "delivery succeeded",
        extra={
            "event_id": str(event_id),
            "attempt_id": attempt_id,
            "worker_id": worker_id,
            "response_status": response_status,
            "duration_ms": duration_ms,
        },
    )
    return True


async def mark_attempt_failed(
    session: AsyncSession,
    *,
    attempt_id: int,
    worker_id: str,
    error: str,
    response_status: int | None,
    response_snippet: str | None,
    duration_ms: int,
    backoff: BackoffPolicy,
    terminal: bool = False,
) -> FailureOutcome:
    """Record a failed attempt and schedule the retry or dead-letter the event.

    ``terminal=True`` is used when retrying cannot help (for example the endpoint
    was disabled or deleted while the attempt was in flight): the event goes
    straight to dead-letter instead of burning the remaining budget.
    """
    row = (
        await session.execute(
            _FAILURE_SQL,
            {
                "attempt_id": attempt_id,
                "worker_id": worker_id,
                "error": error,
                "response_status": response_status,
                "response_snippet": response_snippet,
                "duration_ms": duration_ms,
            },
        )
    ).first()
    if row is None:
        logger.warning(
            "discarded failure for attempt no longer owned",
            extra={"attempt_id": attempt_id, "worker_id": worker_id},
        )
        return FailureOutcome(action="lease_lost")

    event_id = row[0]
    attempt_number = int(row[1])
    event_row = (
        (await session.execute(_READ_EVENT_FOR_UPDATE_SQL, {"id": event_id})).mappings().first()
    )
    if event_row is None or event_row["status"] == EventStatus.DELIVERED.value:
        return FailureOutcome(action="lease_lost")

    max_attempts = int(event_row["max_attempts"])
    if terminal or attempt_number >= max_attempts:
        await session.execute(_DEAD_LETTER_SQL, {"id": event_id, "error": error})
        logger.warning(
            "event moved to dead letter",
            extra={
                "event_id": str(event_id),
                "attempt_id": attempt_id,
                "attempt_number": attempt_number,
                "worker_id": worker_id,
                "response_status": response_status,
                "error": error,
            },
        )
        return FailureOutcome(action="dead_letter")

    delay = backoff.delay_seconds(attempt_number)
    next_number = attempt_number + 1
    await session.execute(
        _INSERT_NEXT_ATTEMPT_SQL,
        {"event_id": event_id, "attempt_number": next_number, "delay_seconds": delay},
    )
    await session.execute(
        _SCHEDULE_RETRY_SQL, {"id": event_id, "delay_seconds": delay, "error": error}
    )
    logger.warning(
        "delivery failed, retry scheduled",
        extra={
            "event_id": str(event_id),
            "attempt_id": attempt_id,
            "attempt_number": attempt_number,
            "next_attempt_number": next_number,
            "retry_in_seconds": round(delay, 3),
            "worker_id": worker_id,
            "response_status": response_status,
            "error": error,
        },
    )
    return FailureOutcome(action="retried", next_attempt_number=next_number, retry_in_seconds=delay)


def validate_status_filter(status: str | None) -> str | None:
    if status is None:
        return None
    if status not in {member.value for member in EventStatus}:
        allowed = ", ".join(sorted(member.value for member in EventStatus))
        raise ValidationError(f"invalid status filter; expected one of: {allowed}")
    return status

"""Request/response models for the HTTP API.

Endpoint secrets appear exactly once, in the create response; every other
representation is built explicitly through the ``from_*`` builders so a secret
can never leak into a response by accident.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from webhooks.models import DeliveryAttempt, Event, WebhookEndpoint

NameField = Annotated[str, Field(min_length=1, max_length=200)]
EventTypeField = Annotated[str, Field(min_length=1, max_length=100)]
IdempotencyKeyField = Annotated[str, Field(min_length=1, max_length=255)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
class EndpointCreate(_StrictModel):
    name: NameField
    url: HttpUrl


class EndpointUpdate(_StrictModel):
    name: NameField | None = None
    url: HttpUrl | None = None
    is_active: bool | None = None


class EndpointOut(BaseModel):
    id: uuid.UUID
    name: str
    url: str
    state: str
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_endpoint(cls, endpoint: WebhookEndpoint) -> EndpointOut:
        return cls(
            id=endpoint.id,
            name=endpoint.name,
            url=endpoint.url,
            state=endpoint.state,
            is_active=endpoint.state == "active",
            created_at=endpoint.created_at,
            updated_at=endpoint.updated_at,
        )


class EndpointCreated(EndpointOut):
    """Create response: the only place the HMAC secret is ever returned."""

    secret: str


class EndpointListResponse(BaseModel):
    items: list[EndpointOut]
    total: int
    limit: int
    offset: int


# --------------------------------------------------------------------------- #
# Events / attempts
# --------------------------------------------------------------------------- #
class EventCreate(_StrictModel):
    endpoint_id: uuid.UUID
    event_type: EventTypeField
    payload: dict[str, Any]
    idempotency_key: IdempotencyKeyField | None = None
    max_attempts: Annotated[int | None, Field(default=None, ge=1, le=100)] = None


class RetryRequest(_StrictModel):
    max_attempts: Annotated[int | None, Field(default=None, ge=1, le=100)] = None


class AttemptOut(BaseModel):
    attempt_number: int
    state: str
    scheduled_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    worker_id: str | None
    recoveries: int
    response_status: int | None
    error: str | None
    duration_ms: int | None

    @classmethod
    def from_attempt(cls, attempt: DeliveryAttempt) -> AttemptOut:
        return cls(
            attempt_number=attempt.attempt_number,
            state=attempt.state,
            scheduled_at=attempt.scheduled_at,
            started_at=attempt.started_at,
            finished_at=attempt.finished_at,
            worker_id=attempt.worker_id,
            recoveries=attempt.recoveries,
            response_status=attempt.response_status,
            error=attempt.error,
            duration_ms=attempt.duration_ms,
        )


class EventOut(BaseModel):
    id: uuid.UUID
    endpoint_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    last_error: str | None
    delivered_at: datetime | None
    dead_lettered_at: datetime | None
    created_at: datetime
    updated_at: datetime
    attempts: list[AttemptOut] = Field(default_factory=list)

    @classmethod
    def from_event(cls, event: Event, attempts: list[DeliveryAttempt] | None = None) -> EventOut:
        source = event.attempts if attempts is None else attempts
        return cls(
            id=event.id,
            endpoint_id=event.endpoint_id,
            event_type=event.event_type,
            payload=event.payload,
            status=event.status,
            attempt_count=event.attempt_count,
            max_attempts=event.max_attempts,
            next_attempt_at=event.next_attempt_at,
            last_error=event.last_error,
            delivered_at=event.delivered_at,
            dead_lettered_at=event.dead_lettered_at,
            created_at=event.created_at,
            updated_at=event.updated_at,
            attempts=[AttemptOut.from_attempt(attempt) for attempt in source],
        )


class EventListResponse(BaseModel):
    items: list[EventOut]
    total: int
    limit: int
    offset: int

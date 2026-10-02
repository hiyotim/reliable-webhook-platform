"""Event submission, inspection, dead-letter listing and manual retry."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, Query, Response, status

from webhooks import service
from webhooks.api.deps import SessionDep, SettingsDep
from webhooks.api.errors import ApiError
from webhooks.api.schemas import (
    EventCreate,
    EventListResponse,
    EventOut,
    RetryRequest,
)

router = APIRouter(prefix="/v1/events", tags=["events"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=EventOut,
    summary="Submit an event for asynchronous delivery",
)
async def submit_event(
    payload: EventCreate,
    session: SessionDep,
    settings: SettingsDep,
    response: Response,
    idempotency_key_header: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> EventOut:
    """Store an event durably and queue its first delivery attempt.

    Supplying an idempotency key (body field or ``Idempotency-Key`` header) makes
    the submission idempotent: the same key returns the original event with
    ``200`` and ``Idempotent-Replay: true`` instead of creating a second one.
    """
    idempotency_key = payload.idempotency_key or idempotency_key_header
    if (
        payload.idempotency_key
        and idempotency_key_header
        and payload.idempotency_key != idempotency_key_header
    ):
        raise ApiError(
            status.HTTP_400_BAD_REQUEST,
            "invalid_request",
            "idempotency_key in body and Idempotency-Key header disagree",
        )

    event, created = await service.submit_event(
        session,
        endpoint_id=payload.endpoint_id,
        event_type=payload.event_type,
        payload=payload.payload,
        idempotency_key=idempotency_key,
        default_max_attempts=settings.delivery_max_attempts,
        max_attempts=payload.max_attempts,
    )
    if not created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replay"] = "true"
    else:
        response.headers["Location"] = f"/v1/events/{event.id}"
    return EventOut.from_event(event, attempts=await service.list_attempts(session, event.id))


@router.get("", response_model=EventListResponse, summary="List events (dead-letter queue view)")
async def list_events(
    session: SessionDep,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
    endpoint_id: Annotated[uuid.UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=service.MAX_LIST_LIMIT)] = service.DEFAULT_LIST_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> EventListResponse:
    """List events, newest first. Filter with ``status=dead_letter`` for the DLQ."""
    validated_status = service.validate_status_filter(status_filter)
    events, total = await service.list_events(
        session,
        status=validated_status,
        endpoint_id=endpoint_id,
        limit=limit,
        offset=offset,
    )
    return EventListResponse(
        items=[EventOut.from_event(event) for event in events],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{event_id}", response_model=EventOut, summary="Inspect an event and its attempts")
async def get_event(event_id: uuid.UUID, session: SessionDep) -> EventOut:
    event = await service.get_event(session, event_id)
    return EventOut.from_event(event, attempts=await service.list_attempts(session, event.id))


@router.post(
    "/{event_id}/retry",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventOut,
    summary="Manually retry a failed / dead-lettered event",
)
async def retry_event(
    event_id: uuid.UUID,
    session: SessionDep,
    settings: SettingsDep,
    payload: RetryRequest | None = None,
) -> EventOut:
    """Queue a new attempt for an event that is not currently in flight.

    A manual retry grants a fresh retry budget of ``DELIVERY_MAX_ATTEMPTS``
    (or the request's ``max_attempts``) counted from the new attempt number.
    """
    event = await service.retry_event(
        session,
        event_id,
        default_max_attempts=settings.delivery_max_attempts,
        max_attempts=payload.max_attempts if payload else None,
    )
    return EventOut.from_event(event, attempts=await service.list_attempts(session, event.id))

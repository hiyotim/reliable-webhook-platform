"""Endpoint registration and lifecycle endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, status
from starlette.responses import Response

from webhooks import service
from webhooks.api.deps import SessionDep
from webhooks.api.schemas import (
    EndpointCreate,
    EndpointCreated,
    EndpointListResponse,
    EndpointOut,
    EndpointUpdate,
)

router = APIRouter(prefix="/v1/endpoints", tags=["endpoints"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=EndpointCreated,
    summary="Register a webhook endpoint",
)
async def create_endpoint(payload: EndpointCreate, session: SessionDep) -> EndpointCreated:
    """Register a destination. The response contains the HMAC secret exactly once."""
    endpoint = await service.create_endpoint(session, name=payload.name, url=str(payload.url))
    base = EndpointOut.from_endpoint(endpoint)
    return EndpointCreated(**base.model_dump(), secret=endpoint.secret)


@router.get("", response_model=EndpointListResponse, summary="List webhook endpoints")
async def list_endpoints(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=service.MAX_LIST_LIMIT)] = service.DEFAULT_LIST_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
    include_deleted: Annotated[bool, Query()] = False,
) -> EndpointListResponse:
    endpoints, total = await service.list_endpoints(
        session, include_deleted=include_deleted, limit=limit, offset=offset
    )
    return EndpointListResponse(
        items=[EndpointOut.from_endpoint(endpoint) for endpoint in endpoints],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{endpoint_id}", response_model=EndpointOut, summary="Inspect an endpoint")
async def get_endpoint(endpoint_id: uuid.UUID, session: SessionDep) -> EndpointOut:
    endpoint = await service.get_endpoint(session, endpoint_id)
    return EndpointOut.from_endpoint(endpoint)


@router.patch("/{endpoint_id}", response_model=EndpointOut, summary="Update or disable an endpoint")
async def update_endpoint(
    endpoint_id: uuid.UUID, payload: EndpointUpdate, session: SessionDep
) -> EndpointOut:
    endpoint = await service.update_endpoint(
        session,
        endpoint_id,
        name=payload.name,
        url=str(payload.url) if payload.url is not None else None,
        is_active=payload.is_active,
    )
    return EndpointOut.from_endpoint(endpoint)


@router.delete(
    "/{endpoint_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an endpoint (soft delete)",
)
async def delete_endpoint(endpoint_id: uuid.UUID, session: SessionDep) -> Response:
    await service.delete_endpoint(session, endpoint_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

"""Shared helpers for API-level tests."""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient

from webhooks.testing.client import ReceiverClient


async def register_endpoint(
    client: AsyncClient,
    *,
    url: str,
    name: str = "test endpoint",
    expected_status: int = 201,
) -> dict[str, Any]:
    response = await client.post("/v1/endpoints", json={"name": name, "url": url})
    assert response.status_code == expected_status, response.text
    return dict(response.json())


async def register_hook_endpoint(
    client: AsyncClient,
    receiver: ReceiverClient,
    hook_id: str,
    *,
    name: str | None = None,
) -> dict[str, Any]:
    """Register an endpoint pointing at the receiver and configure the hook.

    The receiver is given the secret the API generated, so it verifies
    signatures with exactly the value the platform signs with.
    """
    created = await register_endpoint(client, url=receiver.hook_url(hook_id), name=name or hook_id)
    await receiver.configure(hook_id, secret=created["secret"])
    return created


async def submit_event(
    client: AsyncClient,
    endpoint_id: str,
    *,
    payload: dict[str, Any] | None = None,
    event_type: str = "order.created",
    idempotency_key: str | None = None,
    max_attempts: int | None = None,
    expected_status: int = 201,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "endpoint_id": endpoint_id,
        "event_type": event_type,
        "payload": payload if payload is not None else {"id": 1},
    }
    if idempotency_key is not None:
        body["idempotency_key"] = idempotency_key
    if max_attempts is not None:
        body["max_attempts"] = max_attempts
    response = await client.post("/v1/events", json=body)
    assert response.status_code == expected_status, response.text
    return dict(response.json())


async def get_event(client: AsyncClient, event_id: str) -> dict[str, Any]:
    response = await client.get(f"/v1/events/{event_id}")
    assert response.status_code == 200, response.text
    return dict(response.json())

"""Event submission: validation, idempotency, inspection, listing, manual retry."""

from __future__ import annotations

import uuid

from httpx import AsyncClient

from tests.helpers import register_endpoint, submit_event


async def test_submit_event_queues_first_attempt(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/submit")
    response = await client.post(
        "/v1/events",
        json={
            "endpoint_id": endpoint["id"],
            "event_type": "order.created",
            "payload": {"order_id": 42},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert response.headers["location"] == f"/v1/events/{body['id']}"
    assert body["status"] == "pending"
    assert body["attempt_count"] == 1
    assert body["max_attempts"] == 3
    assert body["next_attempt_at"] is not None
    assert [attempt["attempt_number"] for attempt in body["attempts"]] == [1]
    assert body["attempts"][0]["state"] == "pending"


async def test_submit_event_validates_request(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/validate")
    cases = [
        {"endpoint_id": endpoint["id"], "event_type": "", "payload": {"a": 1}},
        {"endpoint_id": endpoint["id"], "event_type": "x", "payload": [1, 2]},
        {"endpoint_id": endpoint["id"], "event_type": "x"},
        {"endpoint_id": endpoint["id"], "event_type": "x", "payload": {}, "max_attempts": 0},
        {"event_type": "x", "payload": {}},
    ]
    for body in cases:
        response = await client.post("/v1/events", json=body)
        assert response.status_code == 422, body

    unknown = await client.post(
        "/v1/events",
        json={"endpoint_id": str(uuid.uuid4()), "event_type": "x", "payload": {}},
    )
    assert unknown.status_code == 404


async def test_duplicate_submission_is_idempotent(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/idem")

    first = await submit_event(client, endpoint["id"], idempotency_key="order-42")
    second = await client.post(
        "/v1/events",
        json={
            "endpoint_id": endpoint["id"],
            "event_type": "order.created",
            "payload": {"order_id": 999},
            "idempotency_key": "order-42",
        },
    )
    assert second.status_code == 200
    assert second.headers["idempotent-replay"] == "true"
    assert second.json()["id"] == first["id"]
    # The original payload wins; the duplicate does not overwrite anything.
    assert second.json()["payload"] == {"id": 1}
    assert second.json()["attempt_count"] == 1

    # A different key on the same endpoint is a different event.
    third = await submit_event(client, endpoint["id"], idempotency_key="order-43")
    assert third["id"] != first["id"]

    # Same key on a different endpoint is also a different event.
    other = await register_endpoint(client, url="https://example.test/hooks/idem-2")
    fourth = await submit_event(client, other["id"], idempotency_key="order-42")
    assert fourth["id"] != first["id"]


async def test_idempotency_key_header_and_conflict(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/idem-header")

    response = await client.post(
        "/v1/events",
        json={"endpoint_id": endpoint["id"], "event_type": "order.created", "payload": {}},
        headers={"Idempotency-Key": "header-key"},
    )
    assert response.status_code == 201

    replay = await client.post(
        "/v1/events",
        json={"endpoint_id": endpoint["id"], "event_type": "order.created", "payload": {}},
        headers={"Idempotency-Key": "header-key"},
    )
    assert replay.status_code == 200
    assert replay.json()["id"] == response.json()["id"]

    conflicting = await client.post(
        "/v1/events",
        json={
            "endpoint_id": endpoint["id"],
            "event_type": "order.created",
            "payload": {},
            "idempotency_key": "body-key",
        },
        headers={"Idempotency-Key": "header-key"},
    )
    assert conflicting.status_code == 400
    assert conflicting.json()["error"]["code"] == "invalid_request"


async def test_get_event_includes_attempts_and_404s(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/inspect")
    event = await submit_event(client, endpoint["id"])

    response = await client.get(f"/v1/events/{event['id']}")
    assert response.status_code == 200
    assert response.json()["payload"] == {"id": 1}
    assert len(response.json()["attempts"]) == 1

    assert (await client.get(f"/v1/events/{uuid.uuid4()}")).status_code == 404
    assert (await client.get("/v1/events/nope")).status_code == 422


async def test_list_events_filters_by_status_and_endpoint(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/list")
    other = await register_endpoint(client, url="https://example.test/hooks/list-2")
    first = await submit_event(client, endpoint["id"])
    await submit_event(client, other["id"])

    everything = await client.get("/v1/events")
    assert everything.status_code == 200
    assert everything.json()["total"] == 2
    # Newest first.
    assert everything.json()["items"][0]["endpoint_id"] == other["id"]

    pending = await client.get("/v1/events", params={"status": "pending"})
    assert pending.json()["total"] == 2

    empty = await client.get("/v1/events", params={"status": "dead_letter"})
    assert empty.json()["total"] == 0

    scoped = await client.get("/v1/events", params={"endpoint_id": endpoint["id"]})
    assert scoped.json()["total"] == 1
    assert scoped.json()["items"][0]["id"] == first["id"]

    paged = await client.get("/v1/events", params={"limit": 1, "offset": 1})
    assert len(paged.json()["items"]) == 1
    assert paged.json()["total"] == 2

    invalid = await client.get("/v1/events", params={"status": "nonsense"})
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "invalid_request"

    assert (await client.get("/v1/events", params={"limit": 0})).status_code == 422


async def test_manual_retry_of_queued_event_conflicts(client: AsyncClient) -> None:
    """A pending event already has an attempt queued; retrying it is a conflict."""
    endpoint = await register_endpoint(client, url="https://example.test/hooks/retry-conflict")
    event = await submit_event(client, endpoint["id"])

    response = await client.post(f"/v1/events/{event['id']}/retry")
    assert response.status_code == 409

    assert (await client.post(f"/v1/events/{uuid.uuid4()}/retry")).status_code == 404

"""End-to-end delivery tests: real PostgreSQL, real worker, real HTTP receiver."""

from __future__ import annotations

import json
from typing import Any

from httpx import AsyncClient

from tests.helpers import get_event, register_hook_endpoint, submit_event
from webhooks.config import Settings
from webhooks.db import Database
from webhooks.worker import Worker


async def status_is(client: AsyncClient, event_id: str, expected: str) -> bool:
    """Awaitable predicate for the drain fixture."""
    return (await get_event(client, event_id))["status"] == expected


async def test_successful_delivery_is_signed(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    endpoint = await register_hook_endpoint(client, receiver.client, "success")
    event = await submit_event(client, endpoint["id"], payload={"order_id": 7, "note": "hello"})

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain([worker], lambda: status_is(client, event["id"], "delivered"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "delivered"
    assert detail["delivered_at"] is not None
    assert detail["last_error"] is None
    assert [a["state"] for a in detail["attempts"]] == ["succeeded"]
    assert detail["attempts"][0]["response_status"] == 200
    assert detail["attempts"][0]["worker_id"] == "worker-a"
    assert detail["attempts"][0]["duration_ms"] is not None

    deliveries = await receiver.client.deliveries("success")
    assert deliveries["total"] == 1
    assert deliveries["duplicates"] == []
    received = deliveries["items"][0]
    assert received["event_id"] == event["id"]
    assert received["attempt"] == 1
    assert received["signature_valid"] is True
    assert received["content_type"] == "application/json"
    # The exact bytes the signature covers are the bytes on the wire.
    assert json.loads(received["body"]) == {"note": "hello", "order_id": 7}


async def test_duplicate_submission_is_delivered_once(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    endpoint = await register_hook_endpoint(client, receiver.client, "idem")
    first = await submit_event(client, endpoint["id"], idempotency_key="key-1")
    replay = await submit_event(
        client, endpoint["id"], idempotency_key="key-1", expected_status=200
    )
    assert replay["id"] == first["id"]

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain([worker], lambda: status_is(client, first["id"], "delivered"))
    finally:
        await worker.aclose()

    deliveries = await receiver.client.deliveries("idem")
    assert deliveries["total"] == 1
    assert deliveries["duplicates"] == []


async def test_retry_after_failing_receiver_then_success(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    await receiver.client.configure("flaky", fail_first_n=2, failure_status=500)
    endpoint = await register_hook_endpoint(client, receiver.client, "flaky")
    event = await submit_event(client, endpoint["id"], payload={"n": 1})

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain([worker], lambda: status_is(client, event["id"], "delivered"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    attempts = detail["attempts"]
    assert [a["attempt_number"] for a in attempts] == [1, 2, 3]
    assert [a["state"] for a in attempts] == ["failed", "failed", "succeeded"]
    assert [a["response_status"] for a in attempts] == [500, 500, 200]
    assert attempts[0]["error"] == "unexpected status 500"
    assert detail["attempt_count"] == 3
    assert detail["last_error"] is None  # cleared on success

    # Backoff: each retry is scheduled after the previous attempt had started.
    assert attempts[1]["scheduled_at"] > attempts[0]["started_at"]
    assert attempts[2]["scheduled_at"] > attempts[1]["started_at"]
    assert attempts[0]["scheduled_at"] <= attempts[0]["started_at"]

    deliveries = await receiver.client.deliveries("flaky")
    assert [item["attempt"] for item in deliveries["items"]] == [1, 2, 3]
    assert all(item["signature_valid"] is True for item in deliveries["items"])
    assert deliveries["duplicates"] == []


async def test_exhausted_retries_dead_letter_then_manual_retry_succeeds(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    await receiver.client.configure("always-fails", always_fail=True)
    endpoint = await register_hook_endpoint(client, receiver.client, "always-fails")
    event = await submit_event(client, endpoint["id"], max_attempts=2)

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain([worker], lambda: status_is(client, event["id"], "dead_letter"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "dead_letter"
    assert detail["dead_lettered_at"] is not None
    assert detail["last_error"] == "unexpected status 500"
    assert detail["next_attempt_at"] is None
    assert [a["state"] for a in detail["attempts"]] == ["failed", "failed"]

    # Dead-letter events are listed for operators.
    dead_letters = await client.get("/v1/events", params={"status": "dead_letter"})
    assert [item["id"] for item in dead_letters.json()["items"]] == [event["id"]]

    # The destination is fixed; a manual retry delivers the event.
    await receiver.client.configure("always-fails", always_fail=False, fail_first_n=0)
    retried = await client.post(f"/v1/events/{event['id']}/retry")
    assert retried.status_code == 202
    assert retried.json()["status"] == "pending"
    assert retried.json()["attempt_count"] == 3
    assert retried.json()["dead_lettered_at"] is None

    worker = Worker(settings, database, worker_id="worker-b")
    try:
        await drain([worker], lambda: status_is(client, event["id"], "delivered"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert [a["attempt_number"] for a in detail["attempts"]] == [1, 2, 3]
    assert detail["attempts"][-1]["state"] == "succeeded"
    assert (await receiver.client.deliveries("always-fails"))["total"] == 3

    delivered = await client.get("/v1/events", params={"status": "delivered"})
    assert [item["id"] for item in delivered.json()["items"]] == [event["id"]]


async def test_timeout_is_recorded_as_failure(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    # The receiver stalls for 2s while the worker's timeout is 1s.
    await receiver.client.configure("slow", delay_seconds=2.0)
    endpoint = await register_hook_endpoint(client, receiver.client, "slow")
    event = await submit_event(client, endpoint["id"], max_attempts=1)

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain(
            [worker],
            lambda: status_is(client, event["id"], "dead_letter"),
            timeout=20.0,
        )
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "dead_letter"
    assert detail["attempts"][0]["state"] == "failed"
    assert detail["attempts"][0]["response_status"] is None
    assert "timeout after 1s" in (detail["attempts"][0]["error"] or "")
    # The stalled request did arrive; the client just stopped waiting for it.
    # It is recorded only after the receiver's 2s sleep finishes.
    deliveries = await receiver.client.wait_for_count("slow", 1, within_seconds=10.0)
    assert deliveries["total"] == 1


async def test_disabled_endpoint_dead_letters_without_http_call(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    endpoint = await register_hook_endpoint(client, receiver.client, "disabled")
    event = await submit_event(client, endpoint["id"], max_attempts=3)
    await client.patch(f"/v1/endpoints/{endpoint['id']}", json={"is_active": False})

    worker = Worker(settings, database, worker_id="worker-a")
    try:
        await drain([worker], lambda: status_is(client, event["id"], "dead_letter"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "dead_letter"
    # Retrying cannot help, so the event does not burn its remaining budget.
    assert len(detail["attempts"]) == 1
    assert detail["attempts"][0]["state"] == "failed"
    assert detail["attempts"][0]["error"] == "endpoint is disabled"
    assert (await receiver.client.deliveries("disabled"))["total"] == 0

"""Worker-failure recovery: lease expiry, re-claim, and result fencing."""

from __future__ import annotations

import asyncio
from typing import Any

from httpx import AsyncClient

from tests.helpers import get_event, register_hook_endpoint, submit_event
from webhooks.config import Settings
from webhooks.db import Database
from webhooks.service import claim_attempts, mark_attempt_succeeded
from webhooks.worker import Worker


async def test_expired_lease_is_recovered_by_another_worker(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    endpoint = await register_hook_endpoint(client, receiver.client, "recovery")
    event = await submit_event(client, endpoint["id"])

    # A worker claims the attempt and then dies without recording anything.
    async with database.session() as session:
        claimed = await claim_attempts(
            session, worker_id="crashed-worker", batch_size=1, lease_seconds=0.3
        )
    assert len(claimed) == 1
    attempt_id = claimed[0].attempt_id

    alive = Worker(settings, database, worker_id="recovered-worker")
    try:
        # While the lease is valid nobody else may take the attempt.
        assert await alive.run_once() == 0
        before = await get_event(client, event["id"])
        assert before["status"] == "in_progress"
        assert before["attempts"][0]["state"] == "in_progress"

        # After the lease expires the attempt comes back to the queue.
        await asyncio.sleep(0.35)
        await drain([alive], lambda: _status(client, event["id"], "delivered"))
    finally:
        await alive.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "delivered"
    attempt = detail["attempts"][0]
    assert attempt["attempt_number"] == 1
    assert attempt["state"] == "succeeded"
    assert attempt["worker_id"] == "recovered-worker"
    assert attempt["recoveries"] == 1
    assert (await receiver.client.deliveries("recovery"))["total"] == 1

    # The dead worker's late result must be rejected: it no longer owns the lease.
    async with database.session() as session:
        accepted = await mark_attempt_succeeded(
            session,
            attempt_id=attempt_id,
            worker_id="crashed-worker",
            response_status=200,
            response_snippet=None,
            duration_ms=1,
        )
    assert accepted is False

    after = await get_event(client, event["id"])
    assert after["attempts"][0]["worker_id"] == "recovered-worker"
    assert after["status"] == "delivered"
    assert after["attempt_count"] == 1


async def _status(client: AsyncClient, event_id: str, expected: str) -> bool:
    return (await get_event(client, event_id))["status"] == expected


async def test_crash_between_claim_and_completion_retries_within_budget(
    client: AsyncClient, receiver: Any, settings: Settings, database: Database, drain: Any
) -> None:
    """A recovered attempt keeps its attempt number, so the budget is not burnt twice."""
    endpoint = await register_hook_endpoint(client, receiver.client, "recovery-budget")
    event = await submit_event(client, endpoint["id"], max_attempts=3)

    async with database.session() as session:
        claimed = await claim_attempts(
            session, worker_id="crashed-worker", batch_size=1, lease_seconds=0.2
        )
    assert len(claimed) == 1

    await asyncio.sleep(0.25)
    worker = Worker(settings, database, worker_id="worker-after-crash")
    try:
        await drain([worker], lambda: _status(client, event["id"], "delivered"))
    finally:
        await worker.aclose()

    detail = await get_event(client, event["id"])
    assert detail["status"] == "delivered"
    assert len(detail["attempts"]) == 1
    assert detail["attempts"][0]["recoveries"] == 1
    assert detail["attempt_count"] == 1

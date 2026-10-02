"""Concurrency: two workers, one queue, no double delivery."""

from __future__ import annotations

import asyncio
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text

from tests.helpers import register_hook_endpoint, submit_event
from webhooks.config import Settings
from webhooks.db import Database
from webhooks.service import claim_attempts
from webhooks.worker import Worker


async def _all_delivered(client: AsyncClient, event_ids: list[str]) -> bool:
    response = await client.get("/v1/events", params={"status": "delivered", "limit": 200})
    return {item["id"] for item in response.json()["items"]} >= set(event_ids)


async def _worker_histogram(database: Database) -> dict[str, int]:
    async with database.engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    "SELECT worker_id, count(*) FROM delivery_attempts "
                    "WHERE state = 'succeeded' GROUP BY worker_id"
                )
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}


async def test_two_concurrent_workers_do_not_double_deliver(
    client: AsyncClient,
    receiver: Any,
    settings: Settings,
    database: Database,
    drain: Any,
) -> None:
    """Two workers draining one queue deliver every attempt exactly once."""
    # A small per-request delay keeps both workers busy at the same time.
    await receiver.client.configure("concurrent", delay_seconds=0.05)
    endpoint = await register_hook_endpoint(client, receiver.client, "concurrent")

    total = 20
    event_ids = [
        (await submit_event(client, endpoint["id"], payload={"i": index}))["id"]
        for index in range(total)
    ]

    # Small batches so each worker claims its own slice instead of one worker
    # grabbing the whole queue in a single claim.
    tuned = settings.model_copy(update={"worker_batch_size": 4})
    workers = [Worker(tuned, database, worker_id=f"worker-{n}") for n in (1, 2)]
    try:
        await drain(
            workers,
            lambda: _all_delivered(client, event_ids),
            timeout=60.0,
            tick=0.01,
        )
    finally:
        for worker in workers:
            await worker.aclose()

    deliveries = await receiver.client.deliveries("concurrent")
    assert deliveries["total"] == total
    assert deliveries["duplicates"] == [], "an attempt was delivered twice"
    assert len({item["event_id"] for item in deliveries["items"]}) == total

    histogram = await _worker_histogram(database)
    assert sum(histogram.values()) == total
    # Both workers really participated, which is what makes the uniqueness
    # assertion above meaningful.
    assert set(histogram) == {"worker-1", "worker-2"}, histogram

    async with database.engine.connect() as connection:
        attempts = (
            await connection.execute(
                text(
                    "SELECT count(*), count(DISTINCT (event_id, attempt_number)) "
                    "FROM delivery_attempts"
                )
            )
        ).one()
    assert attempts[0] == total
    assert attempts[1] == total


async def test_concurrent_claims_are_disjoint(
    client: AsyncClient, settings: Settings, database: Database
) -> None:
    """Racing claim statements must never hand the same attempt to two workers."""
    endpoint_response = await client.post(
        "/v1/endpoints", json={"name": "claims", "url": "https://example.test/hooks/claims"}
    )
    assert endpoint_response.status_code == 201
    endpoint_id = endpoint_response.json()["id"]
    for index in range(20):
        await submit_event(client, endpoint_id, payload={"i": index})

    async def claim(worker_id: str) -> list[int]:
        async with database.session() as session:
            claims = await claim_attempts(
                session, worker_id=worker_id, batch_size=5, lease_seconds=30.0
            )
            return [claim.attempt_id for claim in claims]

    results = await asyncio.gather(*(claim(f"worker-{index}") for index in range(8)))
    claimed = [attempt_id for group in results for attempt_id in group]

    assert len(claimed) == 20, "every queued attempt should be claimed exactly once"
    assert len(set(claimed)) == 20, "an attempt was claimed by two workers"

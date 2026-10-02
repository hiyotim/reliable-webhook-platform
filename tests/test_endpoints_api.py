"""Endpoint registration, disabling, deletion and secret handling."""

from __future__ import annotations

import uuid

from httpx import AsyncClient

from tests.helpers import register_endpoint, submit_event


async def test_create_endpoint_returns_secret_once(client: AsyncClient) -> None:
    created = await register_endpoint(client, url="https://example.test/hooks/a", name="orders")
    assert created["secret"]
    assert created["is_active"] is True
    assert created["state"] == "active"
    assert uuid.UUID(created["id"])

    # The secret must never be readable again through any normal response.
    single = await client.get(f"/v1/endpoints/{created['id']}")
    listed = await client.get("/v1/endpoints")
    assert single.status_code == 200 and "secret" not in single.json()
    assert listed.status_code == 200
    assert all("secret" not in item for item in listed.json()["items"])


async def test_create_endpoint_validates_request(client: AsyncClient) -> None:
    cases = [
        {"name": "", "url": "https://example.test/hook"},
        {"name": "x", "url": "not-a-url"},
        {"name": "x", "url": "ftp://example.test/hook"},
        {"name": "x"},
        {"url": "https://example.test/hook", "unexpected": 1},
    ]
    for body in cases:
        response = await client.post("/v1/endpoints", json=body)
        assert response.status_code == 422, body
        assert response.json()["error"]["code"] == "validation_error"


async def test_update_disable_and_reject_submissions(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/disabled")
    await submit_event(client, endpoint["id"])

    disabled = await client.patch(f"/v1/endpoints/{endpoint['id']}", json={"is_active": False})
    assert disabled.status_code == 200
    assert disabled.json()["state"] == "disabled"
    assert disabled.json()["is_active"] is False

    rejected = await client.post(
        "/v1/events",
        json={
            "endpoint_id": endpoint["id"],
            "event_type": "order.created",
            "payload": {"id": 2},
        },
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "conflict"

    enabled = await client.patch(f"/v1/endpoints/{endpoint['id']}", json={"is_active": True})
    assert enabled.json()["state"] == "active"


async def test_rename_and_url_update(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/one", name="one")
    response = await client.patch(
        f"/v1/endpoints/{endpoint['id']}",
        json={"name": "two", "url": "https://example.test/hooks/two"},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "two"
    assert response.json()["url"] == "https://example.test/hooks/two"


async def test_delete_endpoint_is_soft_and_blocks_new_events(client: AsyncClient) -> None:
    endpoint = await register_endpoint(client, url="https://example.test/hooks/gone")
    event = await submit_event(client, endpoint["id"])

    deleted = await client.delete(f"/v1/endpoints/{endpoint['id']}")
    assert deleted.status_code == 204

    detail = await client.get(f"/v1/endpoints/{endpoint['id']}")
    assert detail.json()["state"] == "deleted"

    listed = await client.get("/v1/endpoints")
    assert listed.json()["items"] == []
    with_deleted = await client.get("/v1/endpoints", params={"include_deleted": True})
    assert len(with_deleted.json()["items"]) == 1

    rejected = await client.post(
        "/v1/events",
        json={
            "endpoint_id": endpoint["id"],
            "event_type": "order.created",
            "payload": {"id": 3},
        },
    )
    assert rejected.status_code == 409

    # Delivery history stays inspectable after the endpoint is deleted.
    assert (await client.get(f"/v1/events/{event['id']}")).status_code == 200

    # Deleting twice is idempotent.
    assert (await client.delete(f"/v1/endpoints/{endpoint['id']}")).status_code == 204


async def test_unknown_endpoint_returns_404(client: AsyncClient) -> None:
    missing = uuid.uuid4()
    assert (await client.get(f"/v1/endpoints/{missing}")).status_code == 404
    assert (await client.delete(f"/v1/endpoints/{missing}")).status_code == 404
    assert (await client.get("/v1/endpoints/not-a-uuid")).status_code == 422


async def test_list_endpoints_pagination_and_total(client: AsyncClient) -> None:
    created = [
        await register_endpoint(
            client, url=f"https://example.test/hooks/list-{index}", name=f"endpoint-{index}"
        )
        for index in range(3)
    ]
    assert (await client.delete(f"/v1/endpoints/{created[0]['id']}")).status_code == 204

    page = await client.get("/v1/endpoints", params={"limit": 1, "offset": 1})
    assert page.status_code == 200
    assert len(page.json()["items"]) == 1
    # total counts every live endpoint, not just the returned page.
    assert page.json()["total"] == 2

    with_deleted = await client.get("/v1/endpoints", params={"include_deleted": True})
    assert with_deleted.json()["total"] == 3
    assert (await client.get("/v1/endpoints", params={"limit": 0})).status_code == 422

"""Test client for the reference receiver."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx


class ReceiverClient:
    """Thin async wrapper over the reference receiver's control endpoints."""

    def __init__(self, base_url: str, client: httpx.AsyncClient | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def hook_url(self, hook_id: str) -> str:
        return f"{self.base_url}/hooks/{hook_id}"

    async def configure(self, hook_id: str, **settings: Any) -> dict[str, Any]:
        response = await self._client.put(f"/hooks/{hook_id}/config", json=settings)
        response.raise_for_status()
        return dict(response.json())

    async def reset(self) -> None:
        response = await self._client.delete("/deliveries")
        response.raise_for_status()

    async def deliveries(self, hook_id: str | None = None) -> dict[str, Any]:
        params = {"hook_id": hook_id} if hook_id else None
        response = await self._client.get("/deliveries", params=params)
        response.raise_for_status()
        return dict(response.json())

    async def wait_for_count(
        self, hook_id: str, count: int, *, within_seconds: float = 5.0, interval: float = 0.01
    ) -> dict[str, Any]:
        """Poll until the hook has received ``count`` requests; returns the log."""
        deadline = asyncio.get_running_loop().time() + within_seconds
        while True:
            payload = await self.deliveries(hook_id)
            if payload["total"] >= count:
                return payload
            if asyncio.get_running_loop().time() > deadline:
                return payload
            await asyncio.sleep(interval)

"""Worker process entrypoint: ``webhooks-worker``."""

from __future__ import annotations

import asyncio
import contextlib
import signal

from webhooks.config import get_settings
from webhooks.db import Database
from webhooks.logging_setup import configure_logging
from webhooks.worker.main import Worker


async def _amain() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    database = Database.from_settings(settings)
    worker = Worker(settings, database)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop_event.set)

    try:
        await worker.run(stop_event)
    finally:
        await database.dispose()


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):  # pragma: no cover - interactive Ctrl-C
        asyncio.run(_amain())


if __name__ == "__main__":
    main()

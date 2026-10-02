"""API process entrypoint: ``webhooks-api``."""

from __future__ import annotations

import uvicorn

from webhooks.config import get_settings
from webhooks.logging_setup import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    uvicorn.run(
        "webhooks.api.app:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,
        access_log=True,
    )


if __name__ == "__main__":
    main()

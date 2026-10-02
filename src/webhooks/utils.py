"""Small helpers shared across the platform."""

from __future__ import annotations

import json
from typing import Any

__all__ = ["json_dumps_compact"]


def json_dumps_compact(value: Any) -> str:
    """Canonical JSON encoding used for both signing and transport.

    Signatures are computed over these exact bytes, so the payload is serialised
    once and the same bytes are sent on the wire.
    """
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)

"""HMAC signing and verification for outbound webhook requests.

Wire format (also documented in the README so consumers can implement it):

    X-Webhook-Id:        <event uuid>
    X-Webhook-Timestamp: <unix seconds, integer>
    X-Webhook-Attempt:   <attempt number>
    X-Webhook-Event-Type:<event type>
    X-Webhook-Signature: sha256=<hex hmac_sha256(secret, "{timestamp}.{body}")>

The timestamp is inside the signed string, which lets receivers reject replays
with a freshness window instead of trusting the header.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass

SIGNATURE_HEADER = "X-Webhook-Signature"
EVENT_ID_HEADER = "X-Webhook-Id"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
ATTEMPT_HEADER = "X-Webhook-Attempt"
EVENT_TYPE_HEADER = "X-Webhook-Event-Type"

SIGNATURE_PREFIX = "sha256="

# Default freshness window used by the reference verifier / test receiver.
DEFAULT_TOLERANCE_SECONDS = 300


def sign_payload(secret: str, timestamp: int, body: bytes) -> str:
    """Return the hex HMAC-SHA256 of ``"{timestamp}.{body}"``."""
    mac = hmac.new(secret.encode("utf-8"), digestmod=hashlib.sha256)
    mac.update(f"{timestamp}.".encode())
    mac.update(body)
    return mac.hexdigest()


def signature_header_value(secret: str, timestamp: int, body: bytes) -> str:
    return f"{SIGNATURE_PREFIX}{sign_payload(secret, timestamp, body)}"


def verify_signature(
    secret: str,
    timestamp: int,
    body: bytes,
    signature: str,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: int | None = None,
) -> bool:
    """Constant-time verification of a received signature.

    Returns ``False`` for a malformed header, a stale/future timestamp, or a
    mismatched digest.
    """
    if not signature.startswith(SIGNATURE_PREFIX):
        return False
    provided = signature[len(SIGNATURE_PREFIX) :]
    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        return False
    expected = sign_payload(secret, timestamp, body)
    return hmac.compare_digest(expected, provided)


@dataclass(frozen=True, slots=True)
class SignatureHeaders:
    """The signed headers attached to one delivery attempt."""

    event_id: str
    timestamp: int
    attempt_number: int
    event_type: str
    signature: str

    def as_dict(self) -> dict[str, str]:
        return {
            EVENT_ID_HEADER: self.event_id,
            TIMESTAMP_HEADER: str(self.timestamp),
            ATTEMPT_HEADER: str(self.attempt_number),
            EVENT_TYPE_HEADER: self.event_type,
            SIGNATURE_HEADER: self.signature,
        }


def build_signature_headers(
    secret: str,
    *,
    event_id: str,
    event_type: str,
    attempt_number: int,
    body: bytes,
    timestamp: int | None = None,
) -> SignatureHeaders:
    """Compute the header set for one attempt (timestamp defaults to now)."""
    issued_at = int(time.time()) if timestamp is None else timestamp
    return SignatureHeaders(
        event_id=event_id,
        timestamp=issued_at,
        attempt_number=attempt_number,
        event_type=event_type,
        signature=signature_header_value(secret, issued_at, body),
    )

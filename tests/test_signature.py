"""HMAC signing / verification unit tests."""

from __future__ import annotations

import time

from webhooks.security import (
    ATTEMPT_HEADER,
    EVENT_ID_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    build_signature_headers,
    sign_payload,
    signature_header_value,
    verify_signature,
)

SECRET = "top-secret"
BODY = b'{"a":1}'


def test_signature_is_stable_and_prefixed() -> None:
    header = signature_header_value(SECRET, 1_700_000_000, BODY)
    assert header.startswith("sha256=")
    assert header == f"sha256={sign_payload(SECRET, 1_700_000_000, BODY)}"
    # Same inputs -> same signature; the timestamp is part of the signed string.
    assert header == signature_header_value(SECRET, 1_700_000_000, BODY)
    assert header != signature_header_value(SECRET, 1_700_000_001, BODY)


def test_verify_accepts_valid_signature() -> None:
    now = int(time.time())
    header = signature_header_value(SECRET, now, BODY)
    assert verify_signature(SECRET, now, BODY, header, now=now)


def test_verify_rejects_tampering() -> None:
    now = int(time.time())
    header = signature_header_value(SECRET, now, BODY)
    assert not verify_signature(SECRET, now, b'{"a":2}', header, now=now)
    assert not verify_signature("other-secret", now, BODY, header, now=now)
    assert not verify_signature(SECRET, now + 1, BODY, header, now=now)


def test_verify_rejects_stale_and_malformed_headers() -> None:
    now = int(time.time())
    header = signature_header_value(SECRET, now, BODY)
    assert not verify_signature(SECRET, now, BODY, header, tolerance_seconds=0, now=now + 30)
    assert not verify_signature(SECRET, now, BODY, "deadbeef", now=now)
    assert not verify_signature(SECRET, now, BODY, "sha256=zzzz", now=now)


def test_build_signature_headers_wire_format() -> None:
    headers = build_signature_headers(
        SECRET,
        event_id="11111111-2222-3333-4444-555555555555",
        event_type="order.created",
        attempt_number=2,
        body=BODY,
        timestamp=1_700_000_000,
    ).as_dict()

    assert headers[EVENT_ID_HEADER] == "11111111-2222-3333-4444-555555555555"
    assert headers[TIMESTAMP_HEADER] == "1700000000"
    assert headers[ATTEMPT_HEADER] == "2"
    assert headers[SIGNATURE_HEADER] == signature_header_value(SECRET, 1_700_000_000, BODY)
    assert (
        verify_signature(SECRET, 1_700_000_000, BODY, headers[SIGNATURE_HEADER], now=1_700_000_000)
        is True
    )

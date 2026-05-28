"""Tests for the Roark HTTP client.

Uses ``httpx.MockTransport`` so we exercise the real ``httpx.AsyncClient`` paths
without any network. Verifies headers, payload shape, and the swallow-failures
contract.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from roark_analytics_python_livekit.client import (
    API_KEY_HEADER,
    CHUNK_UPLOAD_URL_ENDPOINT,
    WEBHOOK_URL,
    RoarkClient,
)


def _client_with_mock(handler: Any) -> RoarkClient:
    client = RoarkClient(api_key="rk_test")
    client._client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(handler),
        headers={API_KEY_HEADER: "rk_test"},
    )
    client._s3_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(handler),
    )
    return client


@pytest.mark.asyncio
async def test_post_call_started_sends_api_key_and_payload() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["api_key"] = request.headers.get(API_KEY_HEADER)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _client_with_mock(handler)
    ok = await client.post_call_started(
        {
            "event": "call-started",
            "livekitCallId": "abc",
            "eventTimestamp": "t",
            "agentId": "a",
        }
    )
    await client.aclose()

    assert ok is True
    assert seen["url"] == WEBHOOK_URL
    assert seen["api_key"] == "rk_test"
    assert seen["body"]["event"] == "call-started"
    assert seen["body"]["livekitCallId"] == "abc"


@pytest.mark.asyncio
async def test_post_returns_false_on_5xx() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="boom")

    client = _client_with_mock(handler)
    ok = await client.post_call_ended(
        {
            "event": "call-ended",
            "livekitCallId": "abc",
            "eventTimestamp": "t",
            "callEndedReason": "x",
        }
    )
    await client.aclose()
    assert ok is False


@pytest.mark.asyncio
async def test_request_chunk_upload_url_unwraps_data_envelope() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "uploadUrl": "https://s3/x",
                    "s3Key": "calls/p/abc/chunks/000000.pcm",
                    "chunkIndex": 0,
                    "expiresInSeconds": 900,
                    "method": "PUT",
                    "contentType": "audio/pcm",
                }
            },
        )

    client = _client_with_mock(handler)
    out = await client.request_chunk_upload_url(livekit_call_id="abc", chunk_index=0)
    await client.aclose()
    assert out is not None
    assert out["uploadUrl"] == "https://s3/x"
    assert out["s3Key"].endswith("000000.pcm")


@pytest.mark.asyncio
async def test_upload_chunk_returns_true_on_2xx() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content
        seen["content_type"] = request.headers.get("content-type")
        return httpx.Response(200)

    client = _client_with_mock(handler)
    ok = await client.upload_chunk(upload_url="https://s3/x", body=b"abc")
    await client.aclose()
    assert ok is True
    assert seen["body"] == b"abc"
    assert seen["content_type"] == "audio/pcm"


def test_endpoints_are_built_in() -> None:
    c = RoarkClient(api_key="k")
    assert c._webhook_url == WEBHOOK_URL
    assert c._chunk_upload_url_endpoint == CHUNK_UPLOAD_URL_ENDPOINT

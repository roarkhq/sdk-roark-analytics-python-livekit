"""HTTP client for the Roark webhook + chunk-upload-url endpoints.

All methods are best-effort: failures are logged and surfaced as return values,
never raised. The helpers in ``session.py`` must never break the call.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from ._types import CallEndedPayload, CallStartedPayload, ChunkUploadUrlResponse

API_KEY_HEADER = "x-roark-api-key"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_CHUNK_TIMEOUT_SECONDS = 10.0

# Roark service endpoints. The integration contract: callers only supply an API key,
# never a URL. Follows the same /v1/integrations/<provider> shape as Roark's other
# integrations (vapi, retell, pipecat).
WEBHOOK_URL = "https://api.roark.ai/v1/integrations/livekit-sdk"
CHUNK_UPLOAD_URL_ENDPOINT = "https://api.roark.ai/v1/integrations/livekit-sdk/chunk-upload-url"

log = logging.getLogger("roark_analytics_python_livekit.client")


class RoarkClient:
    """Async HTTP client for the LiveKit-Agents observer endpoints on Roark.

    All methods are best-effort: failures are logged and surfaced via return
    values, never raised. The session helpers must never break the surrounding
    call.
    """

    def __init__(self, *, api_key: str) -> None:
        """Initialise the client.

        Args:
            api_key: Roark API key (e.g. ``rk_live_...``). Sent on every Roark
                request as ``x-roark-api-key`` *and* ``Authorization: Bearer``
                so both the webhook and the customer-api router accept it.
        """
        self._api_key = api_key
        self._webhook_url: str = WEBHOOK_URL
        self._chunk_upload_url_endpoint: str = CHUNK_UPLOAD_URL_ENDPOINT
        self._client: httpx.AsyncClient | None = None
        self._s3_client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=DEFAULT_TIMEOUT_SECONDS,
                headers={
                    API_KEY_HEADER: self._api_key,
                    "authorization": f"Bearer {self._api_key}",
                },
            )
        return self._client

    def _ensure_s3_client(self) -> httpx.AsyncClient:
        # S3 rejects extra Authorization headers that weren't part of the signature,
        # so the auth client can't be reused for presigned PUTs.
        if self._s3_client is None:
            self._s3_client = httpx.AsyncClient(timeout=DEFAULT_CHUNK_TIMEOUT_SECONDS)
        return self._s3_client

    async def aclose(self) -> None:
        """Close both underlying ``httpx.AsyncClient`` pools."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._s3_client is not None:
            await self._s3_client.aclose()
            self._s3_client = None

    async def post_call_started(self, payload: CallStartedPayload) -> bool:
        """POST a ``call-started`` event to the Roark webhook."""
        return await self._post_event(dict(payload))

    async def post_call_ended(self, payload: CallEndedPayload) -> bool:
        """POST a ``call-ended`` event to the Roark webhook."""
        return await self._post_event(dict(payload))

    async def request_chunk_upload_url(
        self,
        *,
        livekit_call_id: str,
        chunk_index: int,
        content_type: str = "audio/pcm",
    ) -> ChunkUploadUrlResponse | None:
        """Ask Roark for a one-shot presigned PUT URL for a single audio chunk."""
        client = self._ensure_client()
        body: dict[str, Any] = {
            "livekitCallId": livekit_call_id,
            "chunkIndex": chunk_index,
            "contentType": content_type,
        }
        try:
            resp = await client.post(self._chunk_upload_url_endpoint, json=body)
        except httpx.HTTPError as err:
            log.warning("chunk-upload-url request failed: %r", err)
            return None
        if resp.status_code >= 400:
            log.warning("chunk-upload-url returned HTTP %s: %s", resp.status_code, resp.text[:300])
            return None
        try:
            parsed = resp.json()
        except ValueError:
            return None
        # customer-api router wraps responses in {data: ...}; tolerate either shape.
        data = parsed.get("data") if isinstance(parsed, dict) and "data" in parsed else parsed
        if not isinstance(data, dict) or "uploadUrl" not in data:
            return None
        return data  # type: ignore[return-value]

    async def upload_chunk(
        self, *, upload_url: str, body: bytes, content_type: str = "audio/pcm"
    ) -> bool:
        """PUT a single audio chunk to the presigned S3 URL."""
        s3 = self._ensure_s3_client()
        try:
            resp = await s3.put(upload_url, content=body, headers={"content-type": content_type})
        except httpx.HTTPError as err:
            log.warning("chunk PUT failed: %r", err)
            return False
        if resp.status_code >= 400:
            log.warning("chunk PUT returned HTTP %s: %s", resp.status_code, resp.text[:300])
            return False
        return True

    async def _post_event(self, body: dict[str, Any]) -> bool:
        client = self._ensure_client()
        event = body.get("event", "?")
        call_id = body.get("livekitCallId", "?")
        try:
            resp = await client.post(self._webhook_url, json=body)
        except httpx.HTTPError as err:
            log.warning("webhook %s (call=%s) failed: %r", event, call_id, err)
            return False
        if resp.status_code >= 400:
            log.warning(
                "webhook %s (call=%s) returned HTTP %s: %s",
                event,
                call_id,
                resp.status_code,
                resp.text[:300],
            )
            return False
        log.info("webhook %s ok: call=%s", event, call_id)
        return True

"""Wire-format types for the Roark LiveKit-Agents webhook contract.

These shapes are what the helpers in ``session.py`` POST to
``/v1/integrations/livekit-sdk`` on call-started / call-ended. The Roark
backend's ``@roarkanalytics/integrations/livekit`` package owns the mapping
into Roark's internal ``TranscriptEntry`` / ``ExecutedToolInvocation`` shapes
— this SDK stays dumb and forwards livekit-native shapes verbatim.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class TranscriptMessage(TypedDict, total=False):
    """One turn in the call transcript, either user-spoken or assistant-spoken."""

    role: Literal["assistant", "user", "system"]
    content: str
    timestamp: str  # ISO 8601 UTC — turn start (speech onset)
    endTimestamp: str  # ISO 8601 UTC — turn end (speech offset)
    audioOffsetMs: int  # ms from recording start to speech onset
    endAudioOffsetMs: int  # ms from recording start to speech offset
    userId: str
    language: str  # BCP-47


class ToolCallMessage(TypedDict, total=False):
    """Tool invocation emitted by the LLM (paired by ``toolCallId`` with a result)."""

    kind: Literal["tool_call"]
    toolCallId: str
    name: str
    arguments: str  # JSON string — Roark side JSON.parses it
    timestamp: str
    audioOffsetMs: int


class ToolResultMessage(TypedDict, total=False):
    """Tool execution result returned to the LLM (paired by ``toolCallId``)."""

    kind: Literal["tool_result"]
    toolCallId: str
    content: str  # stringified result (json.dumps for objects, str() for scalars)
    timestamp: str
    audioOffsetMs: int


# Metrics ---------------------------------------------------------------------
# Forwarded verbatim from livekit.agents.metrics. The Roark backend persists
# these on the call row so latency budgets (EOU / STT / LLM / TTS) are queryable
# alongside transcript and tool data.


class MetricMessage(TypedDict, total=False):
    """A single metric sample emitted by LiveKit Agents.

    ``kind`` distinguishes the source (eou, stt, llm, tts, agent). The remaining
    fields are forwarded verbatim from the corresponding livekit metric class —
    the Roark backend reads the ones it knows about and stores the rest as JSON.
    """

    kind: Literal["eou", "stt", "llm", "tts", "agent"]
    timestamp: str  # ISO 8601 UTC — when the metric was observed
    audioOffsetMs: int
    # Latency fields (seconds, mirrored from livekit metric classes).
    # Not every kind populates every field; absent fields are omitted.
    endOfUtteranceDelay: float
    transcriptionDelay: float
    onConversationItemAddedDelay: float
    ttft: float  # time-to-first-token (LLM)
    duration: float  # total stream duration
    ttfb: float  # time-to-first-byte (TTS)
    audioDuration: float  # TTS audio length
    # Token usage (LLM).
    promptTokens: int
    completionTokens: int
    totalTokens: int
    cachedTokens: int
    tokensPerSecond: float
    # STT extras.
    streamed: bool
    # Free-form passthrough for fields we don't recognise yet.
    extra: dict[str, Any]


class CallStartedPayload(TypedDict, total=False):
    """Webhook body POSTed when the agent session starts."""

    event: Literal["call-started"]
    livekitCallId: str
    eventTimestamp: str  # ISO 8601 UTC

    agentId: str
    agentName: str
    agentPrompt: str

    # Test-vs-production classification. ``observe_session`` defaults to
    # ``False`` (production); set ``is_test=True`` to flag a test call. The
    # Roark backend uses this to file the call under the right bucket on the
    # dashboard.
    isTest: bool

    # Room / job context (purely informational; used for debugging).
    roomSid: str
    roomName: str
    participantIdentity: str
    jobId: str


class CallEndedPayload(TypedDict, total=False):
    """Webhook body POSTed when the agent session ends.

    Contains the full transcript, tool-call timeline, metrics, and recording
    metadata. Roark stitches previously-uploaded PCM chunks into a WAV using
    ``recordingSampleRate`` / ``recordingNumChannels``.
    """

    event: Literal["call-ended"]
    livekitCallId: str
    eventTimestamp: str

    callStartedAt: str | None
    callEndedAt: str | None
    callEndedReason: str
    agentSpokeFirst: bool
    recordingSampleRate: int
    recordingNumChannels: int
    transcript: list[TranscriptMessage]
    toolCalls: list[ToolCallMessage | ToolResultMessage]
    metrics: list[MetricMessage]


class ChunkUploadUrlResponse(TypedDict, total=False):
    """Response from the Roark chunk-upload endpoint — a one-shot presigned S3 PUT."""

    uploadUrl: str
    s3Key: str
    chunkIndex: int
    expiresInSeconds: int
    method: Literal["PUT"]
    contentType: str

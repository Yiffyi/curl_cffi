"""Per-request retry policy and terminal diagnostics."""

from __future__ import annotations

import math
import random
import time
import warnings
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal
from collections.abc import Callable

from ..curl import SendState
from ..utils import CurlCffiWarning
from .exceptions import HTTPError, RequestException, Timeout
from .streams import _rewind_body

if TYPE_CHECKING:
    from .models import Request, Response

RetryBackoff = Literal["linear", "exponential"]
RetryReason = Literal[
    "attempts_exhausted",
    "budget_exhausted",
    "replay_forbidden",
    "deterministic_error",
    "body_not_replayable",
    "stream_interrupted",
]


@dataclass(frozen=True)
class RetryStrategy:
    """``count`` counts additional attempts; time budgets are seconds.

    An opened stream has no completion deadline. Its establishment is subject to
    the attempt and total budgets; ``idle_timeout`` governs subsequent reception.
    """

    count: int = 0
    delay: float = 0.0
    jitter: float = 0.0
    backoff: RetryBackoff = "linear"
    total_timeout: float | None = None
    attempt_timeout: float | None = None
    connect_timeout: float | None = None


@dataclass(frozen=True)
class RetryInfo:
    attempts: int
    elapsed: float
    send_state: SendState
    reason: RetryReason


@dataclass(frozen=True)
class RequestContext:
    method: str
    url: str
    stream: bool
    context: Any = None


@dataclass(frozen=True)
class AttemptInfo:
    attempt: int
    elapsed: float
    request_context: RequestContext
    response: Response | None = None
    error: BaseException | None = None
    request: Request | None = None


@dataclass(frozen=True)
class CompleteInfo:
    request_context: RequestContext
    attempts: int
    elapsed: float
    response: Response | None = None
    error: BaseException | None = None
    request: Request | None = None
    retry_info: RetryInfo | None = None


AttemptCallback = Callable[[AttemptInfo], None]
CompleteCallback = Callable[[CompleteInfo], None]


def _notify(callback, event):
    if callback is not None:
        try:
            callback(event)
        except BaseException as exc:
            name = "on_attempt" if isinstance(event, AttemptInfo) else "on_complete"
            with suppress(Exception):
                warnings.warn(
                    f"{name} callback failed: {exc}",
                    CurlCffiWarning,
                    stacklevel=3,
                )


def normalize_retry(retry: int | RetryStrategy | None) -> RetryStrategy:
    if retry is None:
        retry = 0
    if isinstance(retry, int):
        retry = RetryStrategy(count=retry)
    if not isinstance(retry, RetryStrategy):
        raise TypeError("retry must be an int or RetryStrategy")
    if not isinstance(retry.count, int) or retry.count < 0:
        raise ValueError("retry.count must be a nonnegative integer")
    for name in ("delay", "jitter"):
        value = getattr(retry, name)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"retry.{name} must be finite and >= 0")
    for name in ("total_timeout", "attempt_timeout", "connect_timeout"):
        value = getattr(retry, name)
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError(f"retry.{name} must be finite and > 0")
    if retry.backoff not in ("linear", "exponential"):
        raise ValueError("retry.backoff must be 'linear' or 'exponential'")
    return retry


# A completed HTTP response, certificate failures, malformed URLs and credentials
# are deliberately absent. TLS handshake failures are ambiguous, not automatically
# transient. No decision is made from timing/getinfo/debug output.
_TRANSIENT_CODES = frozenset({5, 6, 7, 16, 18, 28, 52, 55, 56, 81, 89, 92, 95, 96})


class _RetryController:
    def __init__(
        self, strategy, allow_replay, on_attempt, request_context, on_complete=None
    ):
        self.strategy = strategy
        self.allow_replay = allow_replay
        self.on_attempt = on_attempt
        self.on_complete = on_complete
        self.request_context = request_context
        self.request = None
        self.started = time.monotonic()
        self.attempts = 0

    @property
    def elapsed(self):
        return time.monotonic() - self.started

    @property
    def remaining(self):
        budget = self.strategy.total_timeout
        return None if budget is None else max(0.0, budget - self.elapsed)

    def timeout(self, fallback=None):
        per_attempt = self.strategy.attempt_timeout
        if per_attempt is None:
            per_attempt = sum(fallback) if isinstance(fallback, tuple) else fallback
            if per_attempt == 0:
                per_attempt = None
        limits = [v for v in (self.remaining, per_attempt) if v is not None]
        return min(limits) if limits else None

    def notify(self, response=None, error=None):
        _notify(
            self.on_attempt,
            AttemptInfo(
                self.attempts,
                self.elapsed,
                self.request_context,
                response,
                error,
                self.request,
            ),
        )

    def complete(self, response=None, error=None):
        _notify(
            self.on_complete,
            CompleteInfo(
                self.request_context,
                self.attempts,
                self.elapsed,
                response,
                error,
                self.request,
                getattr(error, "retry_info", None),
            ),
        )

    def finish(self, error, reason):
        state = getattr(error, "send_state", SendState.UNKNOWN)
        error.retry_info = RetryInfo(self.attempts, self.elapsed, state, reason)
        return error

    def budget_error(self, state=SendState.NOT_STARTED):
        return self.finish(
            Timeout("Request budget exhausted", code=28, send_state=state),
            "budget_exhausted",
        )

    def retry_delay(self, error, body, position, multipart):
        if not isinstance(error, RequestException) or (
            isinstance(error, HTTPError) and error.code in (0, 22)
        ):
            self.finish(error, "deterministic_error")
            return None
        if error.code not in _TRANSIENT_CODES:
            self.finish(error, "deterministic_error")
            return None
        remaining = self.remaining
        if remaining is not None and remaining <= 0:
            self.finish(error, "budget_exhausted")
            return None
        if error.send_state is not SendState.NOT_STARTED and not self.allow_replay:
            self.finish(error, "replay_forbidden")
            return None
        if self.attempts > self.strategy.count:
            self.finish(error, "attempts_exhausted")
            return None
        try:
            if multipart is not None:
                raise ValueError("Multipart request body cannot be safely rewound")
            _rewind_body(body, position)
        except Exception:
            self.finish(error, "body_not_replayable")
            return None
        strategy = self.strategy
        factor = (
            2 ** (self.attempts - 1)
            if strategy.backoff == "exponential"
            else self.attempts
        )
        delay = strategy.delay * factor + random.uniform(0.0, strategy.jitter)
        if remaining is not None and delay >= remaining:
            self.finish(error, "budget_exhausted")
            return None
        return delay

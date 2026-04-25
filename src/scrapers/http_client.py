"""Shared HTTP client base for scrapers.

Provides RetryingHttpClient: a thin wrapper around httpx.AsyncClient that
handles the common retry/backoff/throttle pattern every scraper needs.
Scraper-specific quirks (auth, custom rate-limit headers, response shape)
stay in the individual scraper modules.

Why not put this in retry.py?
  retry.py wraps arbitrary callables in retry_async() — it's caller-driven.
  RetryingHttpClient is the inverse: HTTP-specific, knows about status codes
  and Retry-After, throttles internally so callers don't need to.

Usage:
    client = RetryingHttpClient(
        base_url="https://api.example.com",
        delay=1.0,
        source="example",
    )
    try:
        resp = await client.get("/things/42")
    finally:
        await client.aclose()
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import ClassVar

import httpx
import structlog
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    RetryError,
    retry_any,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
)

log = structlog.get_logger()


def _wait_from_state(initial_backoff: float):
    """Return a tenacity wait callable that respects Retry-After / X-RateLimit-Reset."""

    def _wait(retry_state: RetryCallState) -> float:
        n = retry_state.attempt_number
        if retry_state.outcome.failed:
            # network / transient exception → exponential backoff
            return min(initial_backoff * (2 ** (n - 1)), 120.0)
        # bad HTTP status — try Retry-After header first
        resp: httpx.Response = retry_state.outcome.result()
        raw = resp.headers.get("Retry-After", "")
        try:
            return float(max(int(raw), 5))
        except (ValueError, TypeError):
            return float(min(initial_backoff * 2 * (2 ** (n - 1)), 300.0))

    return _wait


def _before_sleep_log(source: str, on_rate_limit=None):
    """Return a tenacity before_sleep callable with structlog + optional callback."""

    def _before_sleep(retry_state: RetryCallState) -> None:
        wait = retry_state.next_action.sleep  # type: ignore[union-attr]
        if retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            log.warning(
                "http_request_error",
                source=source,
                error_type=type(exc).__name__,
                error=str(exc),
                attempt=retry_state.attempt_number,
                wait_s=wait,
            )
        else:
            resp: httpx.Response = retry_state.outcome.result()
            log.warning(
                "http_rate_limited",
                source=source,
                status=resp.status_code,
                wait_s=wait,
                attempt=retry_state.attempt_number,
            )
            if on_rate_limit is not None:
                on_rate_limit(int(wait))

    return _before_sleep


def _retry_error_callback(retry_state: RetryCallState):
    """Called when tenacity exhausts retries; raise_for_status on bad responses."""
    outcome = retry_state.outcome
    if outcome.failed:
        raise outcome.exception()
    # outcome is a bad-status response — raise it
    outcome.result().raise_for_status()


class RetryingHttpClient:
    """Async HTTP client with throttle + retry + structured logging.

    Retries on:
      - HTTP 429 (rate limit, respects Retry-After header)
      - HTTP 500-504, 522, 524 (transient server / Cloudflare)
      - httpx.TimeoutException, ConnectError, ReadError,
        RemoteProtocolError, PoolTimeout (network layer)

    Does NOT retry on 4xx other than 429 — those are caller's problem.
    """

    DEFAULT_RETRYABLE_STATUS: ClassVar[frozenset[int]] = frozenset(
        {429, 500, 502, 503, 504, 522, 524}
    )
    DEFAULT_RETRYABLE_EXC: ClassVar[tuple[type[Exception], ...]] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.RemoteProtocolError,
        httpx.PoolTimeout,
    )

    def __init__(
        self,
        *,
        source: str,
        delay: float = 1.0,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        base_url: str = "",
        max_attempts: int = 8,
        initial_backoff: float = 4.0,
        retryable_status: frozenset[int] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.source = source
        self._delay = delay
        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff
        self._retryable_status = retryable_status or self.DEFAULT_RETRYABLE_STATUS
        self._last_request = 0.0
        client_kwargs: dict = {
            "timeout": timeout,
            "follow_redirects": True,
        }
        if headers:
            client_kwargs["headers"] = headers
        if base_url:
            client_kwargs["base_url"] = base_url
        if transport is not None:
            client_kwargs["transport"] = transport
        self._client = httpx.AsyncClient(**client_kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()

    # Backwards-compatible alias used by some scrapers
    async def close(self) -> None:
        await self.aclose()

    async def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        wait = self._delay - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request = time.monotonic()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json: object | None = None,
        headers: dict | None = None,
        max_attempts: int | None = None,
        # AniList-specific: rate limit header capture + callback
        rate_limit_context: dict | None = None,
        on_rate_limit: object | None = None,
    ) -> httpx.Response:
        _max = max_attempts or self._max_attempts
        _retryable_status = self._retryable_status

        async def _once() -> httpx.Response:
            await self._throttle()
            resp = await self._client.request(method, url, params=params, json=json, headers=headers)
            if rate_limit_context is not None:
                self._update_rate_limit_context(resp, rate_limit_context)
            return resp

        try:
            return await AsyncRetrying(
                retry=retry_any(
                    retry_if_exception_type(self.DEFAULT_RETRYABLE_EXC),
                    retry_if_result(lambda r: r.status_code in _retryable_status),
                ),
                wait=_wait_from_state(self._initial_backoff),
                stop=stop_after_attempt(_max),
                before_sleep=_before_sleep_log(self.source, on_rate_limit),
                retry_error_callback=_retry_error_callback,
            )(_once)
        except RetryError:
            raise  # pragma: no cover

    @staticmethod
    def _update_rate_limit_context(resp: httpx.Response, ctx: dict) -> None:
        """Extract X-RateLimit-* headers into ctx (AniList-specific)."""
        if "X-RateLimit-Remaining" in resp.headers:
            ctx["remaining"] = int(resp.headers["X-RateLimit-Remaining"])
            ctx["reset_at"] = int(resp.headers.get("X-RateLimit-Reset", 0))
            ctx["limit"] = int(resp.headers.get("X-RateLimit-Limit", 0))

    async def get(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        max_attempts: int | None = None,
    ) -> httpx.Response:
        return await self.request(
            "GET", url, params=params, headers=headers, max_attempts=max_attempts
        )

    async def post(
        self,
        url: str,
        *,
        json: object | None = None,
        headers: dict | None = None,
        max_attempts: int | None = None,
        # AniList-specific options: rate limit header capture + callback
        rate_limit_context: dict | None = None,
        on_rate_limit: object | None = None,
    ) -> httpx.Response:
        """POST request with retries.

        Args:
            rate_limit_context: Optional dict; if provided and the response carries
                                X-RateLimit-* headers, they are stored under keys
                                ``remaining``, ``reset_at``, ``limit``.
                                This feature exists for AniList which exposes these
                                headers — other sources ignore it.
            on_rate_limit:      Optional callable(remaining_secs: int | None) called
                                during rate-limit waits (AniList-specific).
        """
        return await self.request(
            "POST",
            url,
            json=json,
            headers=headers,
            max_attempts=max_attempts,
            rate_limit_context=rate_limit_context,
            on_rate_limit=on_rate_limit,
        )


class DualWindowRateLimiter:
    """Sliding-window rate limiter with independent per-second and per-minute budgets.

    Jikan v4 official limits: per_second=3, per_minute=60.
    Cache hits bypass acquire() — call acquire() only directly before HTTP requests.
    """

    def __init__(self, per_second: int, per_minute: int) -> None:
        self.per_second = per_second
        self.per_minute = per_minute
        self._sec_window: deque[float] = deque()
        self._min_window: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._sec_window and now - self._sec_window[0] >= 1.0:
                    self._sec_window.popleft()
                while self._min_window and now - self._min_window[0] >= 60.0:
                    self._min_window.popleft()
                wait = 0.0
                if len(self._sec_window) >= self.per_second:
                    wait = max(wait, 1.0 - (now - self._sec_window[0]))
                if len(self._min_window) >= self.per_minute:
                    wait = max(wait, 60.0 - (now - self._min_window[0]))
                if wait <= 0:
                    self._sec_window.append(now)
                    self._min_window.append(now)
                    return
                await asyncio.sleep(wait + 0.01)

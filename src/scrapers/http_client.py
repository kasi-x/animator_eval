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
from typing import ClassVar

import httpx
import structlog

log = structlog.get_logger()


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
    ) -> httpx.Response:
        max_attempts = max_attempts or self._max_attempts
        backoff = self._initial_backoff
        attempt = 0
        while True:
            attempt += 1
            await self._throttle()
            try:
                resp = await self._client.request(
                    method, url, params=params, json=json, headers=headers
                )
            except self.DEFAULT_RETRYABLE_EXC as exc:
                if attempt >= max_attempts:
                    log.error(
                        "http_request_giveup",
                        source=self.source,
                        url=url,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        attempts=attempt,
                    )
                    raise
                wait = min(backoff, 120)
                log.warning(
                    "http_request_error",
                    source=self.source,
                    url=url,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    attempt=attempt,
                    max_attempts=max_attempts,
                    wait_s=wait,
                )
                await asyncio.sleep(wait)
                backoff *= 2
                continue

            if resp.status_code in self._retryable_status:
                retry_after = self._parse_retry_after(resp, backoff)
                if attempt >= max_attempts:
                    log.error(
                        "http_rate_giveup",
                        source=self.source,
                        url=url,
                        status=resp.status_code,
                        attempts=attempt,
                    )
                    resp.raise_for_status()
                    return resp
                log.warning(
                    "http_rate_limited",
                    source=self.source,
                    url=url,
                    status=resp.status_code,
                    wait_s=retry_after,
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                await asyncio.sleep(retry_after)
                backoff = min(max(backoff * 2, retry_after), 300)
                continue

            return resp

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
    ) -> httpx.Response:
        return await self.request(
            "POST", url, json=json, headers=headers, max_attempts=max_attempts
        )

    @staticmethod
    def _parse_retry_after(resp: httpx.Response, backoff: float) -> int:
        raw = resp.headers.get("Retry-After", "")
        try:
            return max(int(raw), 5)
        except (ValueError, TypeError):
            return int(min(backoff * 2, 300))

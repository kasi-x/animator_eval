"""bangumi /v0 REST API async client.

DEPRECATED (2026-04-25): Use BangumiGraphQLClient (bangumi_graphql_scraper.py) for new code.
This v0 REST client is retained as a fallback when --client v0 is specified.
Prefer the GraphQL client for batched fetches (~100x throughput on initial backfill).

Provides ``BangumiClient`` — an async context manager wrapping httpx with:
- 1 req/sec floor rate limiter (shared with GraphQL client via module-level ``_HOST_RATE_LIMITER``)
- Exponential backoff retry (429 / 5xx, max 5 attempts)
- Retry-After header support: honors RFC 7231 integer-seconds or HTTP-date,
  capped at _RETRY_AFTER_CAP to prevent runaway stalls
- 404 → returns None without retry (logged and checkpointed by caller)

Endpoints implemented:
    fetch_subject_persons(subject_id)   → list[dict] | None
    fetch_subject_characters(subject_id) → list[dict] | None
    fetch_person(person_id)             → dict | None        (used by Card 04)
    fetch_character(character_id)       → dict | None        (used by Card 05)

Rate limiter:
    Both this module and ``bangumi_graphql_scraper`` import the module-level
    ``_HOST_RATE_LIMITER`` defined here.  Both clients hit the same host
    (api.bgm.tv) and therefore share one lock, ensuring total throughput across
    both clients never exceeds 1 req/sec.
"""

from __future__ import annotations

import asyncio
import email.utils
import time
from typing import Any

import httpx
import structlog

from src.scrapers.exceptions import RateLimitError, ScraperError
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.queries.bangumi import (
    DEFAULT_USER_AGENT,
    character_url,
    person_url,
    subject_characters_url,
    subject_persons_url,
)

log = structlog.get_logger()

_SOURCE = "bangumi"
_MAX_ATTEMPTS = 5
_BASE_DELAY = 2.0  # seconds; doubles each retry
_RETRY_AFTER_CAP = 120.0  # seconds; upper bound for any Retry-After value

# ---------------------------------------------------------------------------
# Shared host-level rate limiter
# ---------------------------------------------------------------------------
# Both BangumiClient (v0 REST) and BangumiGraphQLClient import this object.
# They hit the same host (api.bgm.tv) so they must share one asyncio.Lock
# to ensure combined throughput across both clients stays <= 1 req/sec.
#
# Usage in each client:
#   await _HOST_RATE_LIMITER.throttle()   # replaces the old _throttle() method


class _HostRateLimiter:
    """Module-level async rate limiter keyed to a single host.

    Enforces a minimum interval between successive outgoing requests using
    a single asyncio.Lock + monotonic timestamp.  Both v0 REST and GraphQL
    clients import and share the same instance so they never race each other
    past the 1 req/sec floor for api.bgm.tv.

    Args:
        min_interval_sec: minimum seconds between any two calls to ``throttle()``.
    """

    def __init__(self, min_interval_sec: float = 1.0) -> None:
        self._min_interval = min_interval_sec
        self._lock: asyncio.Lock | None = None  # created lazily (event-loop-safe)
        self._last_request_at: float = 0.0

    def _get_lock(self) -> asyncio.Lock:
        """Return (or lazily create) the asyncio.Lock bound to the running event loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def throttle(self) -> None:
        """Sleep until >= min_interval_sec has elapsed since the last call."""
        async with self._get_lock():
            now = time.monotonic()
            gap = self._min_interval - (now - self._last_request_at)
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_request_at = time.monotonic()

    def reset_for_test(self) -> None:
        """Reset state for unit tests that swap event loops between runs."""
        self._lock = None
        self._last_request_at = 0.0


# Singleton shared by all bangumi clients in this process.
_HOST_RATE_LIMITER = _HostRateLimiter(min_interval_sec=1.0)


def _compute_backoff_sleep(
    attempt: int,
    status: int,  # noqa: ARG001  (kept for test-friendly seam / future use)
    retry_after: str | None,
) -> float:
    """Return how many seconds to sleep before the next retry attempt.

    Exponential backoff: ``_BASE_DELAY * 2^(attempt-1)`` (attempt is 1-based).
    If ``retry_after`` is present (RFC 7231: integer seconds or HTTP-date),
    the sleep is ``max(parsed_retry_after, exp_backoff)`` capped at
    ``_RETRY_AFTER_CAP``.

    Args:
        attempt: current attempt number (1 = first try).
        status: HTTP status code that triggered the retry (for future use).
        retry_after: raw value of the Retry-After response header, or None.

    Returns:
        Sleep duration in seconds (float, > 0).
    """
    exp_backoff = _BASE_DELAY * (2 ** (attempt - 1))

    if retry_after is None:
        return exp_backoff

    # RFC 7231 §7.1.3: Retry-After is either an integer (delay-seconds)
    # or an HTTP-date string.
    parsed: float | None = None
    stripped = retry_after.strip()
    if stripped.isdigit():
        parsed = float(stripped)
    else:
        try:
            # email.utils.parsedate_to_datetime handles RFC 5322 / HTTP-date.
            ts = email.utils.parsedate_to_datetime(stripped)
            parsed = max(0.0, ts.timestamp() - time.time())
        except Exception:
            parsed = None

    if parsed is not None and parsed > 0:
        chosen = min(max(parsed, exp_backoff), _RETRY_AFTER_CAP)
    else:
        chosen = min(exp_backoff, _RETRY_AFTER_CAP)

    return chosen


class BangumiClient(RateLimitedHttpClient):
    """Async httpx client for the bangumi /v0 REST API.

    Rate limiter: delegates to the module-level ``_HOST_RATE_LIMITER`` so that
    this client and ``BangumiGraphQLClient`` share a single asyncio.Lock and
    never collectively exceed 1 req/sec on api.bgm.tv.

    The ``rate_limit_per_sec`` constructor argument still exists so that tests
    can pass ``rate_limit_per_sec=100.0`` to speed up the per-instance limiter
    without touching the shared singleton.  When rate_limit_per_sec != 1.0 the
    client uses a *local* ``_HostRateLimiter`` instance instead of the shared
    one, so high-rate test runs don't interfere with the global limit.

    Usage::

        async with BangumiClient() as client:
            persons = await client.fetch_subject_persons(50)
            chars   = await client.fetch_subject_characters(50)
    """

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        rate_limit_per_sec: float = 1.0,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(delay=1.0 / rate_limit_per_sec)
        self._user_agent = user_agent
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        # Use the shared singleton at the default rate; local instance for tests.
        if rate_limit_per_sec == 1.0:
            self._limiter = _HOST_RATE_LIMITER
        else:
            self._limiter = _HostRateLimiter(min_interval_sec=1.0 / rate_limit_per_sec)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> BangumiClient:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent, "Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public fetch methods
    # ------------------------------------------------------------------

    async def fetch_subject_persons(self, subject_id: int) -> list[dict[str, Any]] | None:
        """Fetch GET /v0/subjects/{id}/persons.

        Returns:
            list of person-relation dicts, or None on 404.

        Each dict keys observed from live API:
            id, name, type, relation, career (list), eps, images
        """
        url = subject_persons_url(subject_id)
        raw = await self._get_with_retry(url, context=f"subject={subject_id}/persons")
        if raw is None:
            return None
        result = raw if isinstance(raw, list) else []
        log.info(
            "bangumi_api_fetch_done",
            subject_id=subject_id,
            endpoint="persons",
            count=len(result),
        )
        return result

    async def fetch_subject_characters(self, subject_id: int) -> list[dict[str, Any]] | None:
        """Fetch GET /v0/subjects/{id}/characters.

        Returns:
            list of character-relation dicts, or None on 404.

        Each dict keys observed from live API:
            id, name, type, relation, actors (list of person dicts), images, summary
        """
        url = subject_characters_url(subject_id)
        raw = await self._get_with_retry(url, context=f"subject={subject_id}/characters")
        if raw is None:
            return None
        result = raw if isinstance(raw, list) else []
        log.info(
            "bangumi_api_fetch_done",
            subject_id=subject_id,
            endpoint="characters",
            count=len(result),
        )
        return result

    async def fetch_person(self, person_id: int) -> dict[str, Any] | None:
        """Fetch GET /v0/persons/{id}.

        Returns:
            person detail dict, or None on 404.
            Used by Card 04 (person_detail).
        """
        url = person_url(person_id)
        raw = await self._get_with_retry(url, context=f"person={person_id}")
        if raw is None:
            return None
        log.info("bangumi_api_fetch_done", person_id=person_id, endpoint="person")
        return raw if isinstance(raw, dict) else None

    async def fetch_character(self, character_id: int) -> dict[str, Any] | None:
        """Fetch GET /v0/characters/{id}.

        Returns:
            character detail dict, or None on 404.
            Used by Card 05 (character_detail).
        """
        url = character_url(character_id)
        raw = await self._get_with_retry(url, context=f"character={character_id}")
        if raw is None:
            return None
        log.info("bangumi_api_fetch_done", character_id=character_id, endpoint="character")
        return raw if isinstance(raw, dict) else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Sleep until >= min_interval has elapsed since last request.

        Delegates to the shared ``_HostRateLimiter`` (or a local one for tests).
        """
        await self._limiter.throttle()

    async def _get_with_retry(
        self, url: str, context: str = ""
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        """GET url with rate-limiting and exponential backoff retry.

        Returns:
            Parsed JSON body (list or dict), or None on 404.

        Raises:
            ScraperError: when all retry attempts are exhausted.
        """
        assert self._client is not None, "BangumiClient must be used as async context manager"

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._throttle()

            try:
                resp = await self._client.get(url)
            except httpx.TransportError as exc:
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"Transport error after {_MAX_ATTEMPTS} attempts: {exc}",
                        source=_SOURCE,
                        url=url,
                    ) from exc
                wait = _compute_backoff_sleep(attempt, 0, None)
                log.warning(
                    "bangumi_transport_error",
                    url=url,
                    context=context,
                    attempt=attempt,
                    wait_seconds=wait,
                    error=str(exc),
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 404:
                log.info("bangumi_not_found", url=url, context=context)
                return None

            if resp.status_code == 429:
                raw_retry_after = resp.headers.get("Retry-After")
                wait = _compute_backoff_sleep(attempt, 429, raw_retry_after)
                log.warning(
                    "bangumi_rate_limited",
                    url=url,
                    context=context,
                    attempt=attempt,
                    retry_after_header=raw_retry_after,
                    wait_seconds=wait,
                )
                if raw_retry_after is not None:
                    log.info(
                        "bangumi_retry_after_honored",
                        attempt=attempt,
                        seconds=wait,
                    )
                if attempt >= _MAX_ATTEMPTS:
                    raise RateLimitError(
                        "bangumi rate limit exceeded after max attempts",
                        source=_SOURCE,
                        url=url,
                        retry_after=wait,
                    )
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"Server error {resp.status_code} after {_MAX_ATTEMPTS} attempts",
                        source=_SOURCE,
                        url=url,
                    )
                raw_retry_after = resp.headers.get("Retry-After")
                wait = _compute_backoff_sleep(attempt, resp.status_code, raw_retry_after)
                log.warning(
                    "bangumi_server_error",
                    url=url,
                    context=context,
                    status=resp.status_code,
                    attempt=attempt,
                    retry_after_header=raw_retry_after,
                    wait_seconds=wait,
                )
                if raw_retry_after is not None:
                    log.info(
                        "bangumi_retry_after_honored",
                        attempt=attempt,
                        seconds=wait,
                    )
                await asyncio.sleep(wait)
                continue

            if not (200 <= resp.status_code < 300):
                raise ScraperError(
                    f"Unexpected HTTP {resp.status_code}",
                    source=_SOURCE,
                    url=url,
                )

            return resp.json()

        raise ScraperError(
            f"Failed to GET {url} after {_MAX_ATTEMPTS} attempts",
            source=_SOURCE,
            url=url,
        )

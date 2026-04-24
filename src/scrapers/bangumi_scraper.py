"""bangumi /v0 API async client.

Provides ``BangumiClient`` — an async context manager wrapping httpx with:
- 1 req/sec floor rate limiter (shared across all fetch methods)
- Exponential backoff retry (429 / 5xx, max 5 attempts)
- 404 → returns None without retry (logged and checkpointed by caller)

Endpoints implemented:
    fetch_subject_persons(subject_id)   → list[dict] | None
    fetch_subject_characters(subject_id) → list[dict] | None
    fetch_person(person_id)             → dict | None        (used by Card 04)
    fetch_character(character_id)       → dict | None        (used by Card 05)
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import structlog

from src.scrapers.exceptions import RateLimitError, ScraperError
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


class BangumiClient:
    """Async httpx client for the bangumi /v0 REST API.

    Rate limiter: a single asyncio.Lock + ``_last_request_at`` timestamp
    ensures >= 1.0 s between *any* two outgoing requests.  All four fetch
    methods share the same lock, so running them sequentially per subject
    automatically serialises through the limiter at the right cadence.

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
        self._user_agent = user_agent
        self._min_interval = 1.0 / rate_limit_per_sec  # seconds
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        self._rate_lock = asyncio.Lock()
        self._last_request_at: float = 0.0

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
        """Sleep until >= _min_interval has elapsed since last request."""
        async with self._rate_lock:
            now = time.monotonic()
            gap = self._min_interval - (now - self._last_request_at)
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_request_at = time.monotonic()

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
                wait = _BASE_DELAY * (2 ** (attempt - 1))
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
                retry_after = float(resp.headers.get("Retry-After", 60))
                log.warning(
                    "bangumi_rate_limited",
                    url=url,
                    context=context,
                    attempt=attempt,
                    retry_after=retry_after,
                )
                if attempt >= _MAX_ATTEMPTS:
                    raise RateLimitError(
                        "bangumi rate limit exceeded after max attempts",
                        source=_SOURCE,
                        url=url,
                        retry_after=retry_after,
                    )
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"Server error {resp.status_code} after {_MAX_ATTEMPTS} attempts",
                        source=_SOURCE,
                        url=url,
                    )
                wait = _BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "bangumi_server_error",
                    url=url,
                    context=context,
                    status=resp.status_code,
                    attempt=attempt,
                    wait_seconds=wait,
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

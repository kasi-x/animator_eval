"""bangumi GraphQL API async client.

Endpoint: https://api.bgm.tv/v0/graphql (POST, Content-Type: application/json)
Server: bangumi/server-private (Fastify + mercurius).  Altair UI: /v0/altair/
Confirmed: 2026-04-25.

Rate limit: 1 req/sec via ``_HOST_RATE_LIMITER`` (module-level asyncio.Lock + timestamp).

Error semantics:
    - GraphQL ``errors[0].extensions.code == "NOT_FOUND"``  → returns None (like 404).
    - Any other non-empty ``errors`` array → raises ``ScraperError``.
    - HTTP 4xx (non-429) → raises ``ScraperError``.
    - HTTP 429 / 5xx     → exponential backoff retry.
    - Transport error    → same retry logic.

Logging (structlog):
    ``bangumi_graphql_fetch_done  query=<kind>  batch_size=<N>  duration_ms=<ms>``
"""

from __future__ import annotations

import asyncio
import email.utils
import json
import time
from typing import Any

import httpx
import structlog

from src.scrapers.exceptions import RateLimitError, ScraperError
from src.scrapers.queries.bangumi_graphql import (
    BANGUMI_GRAPHQL_URL,
    DEFAULT_USER_AGENT,
    SUBJECT_BATCH_QUERY,
    SUBJECT_FULL_QUERY,
)

_SOURCE = "bangumi"
_MAX_ATTEMPTS = 5
_BASE_DELAY = 2.0
_RETRY_AFTER_CAP = 120.0


class _HostRateLimiter:
    """Async rate limiter for api.bgm.tv (1 req/sec floor)."""

    def __init__(self, min_interval_sec: float = 1.0) -> None:
        self._min_interval = min_interval_sec
        self._lock: asyncio.Lock | None = None
        self._last_request_at: float = 0.0

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def throttle(self) -> None:
        async with self._get_lock():
            now = time.monotonic()
            gap = self._min_interval - (now - self._last_request_at)
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_request_at = time.monotonic()

    def reset_for_test(self) -> None:
        self._lock = None
        self._last_request_at = 0.0


_HOST_RATE_LIMITER = _HostRateLimiter(min_interval_sec=1.0)


def _compute_backoff_sleep(attempt: int, status: int, retry_after: str | None) -> float:  # noqa: ARG001
    exp_backoff = _BASE_DELAY * (2 ** (attempt - 1))
    if retry_after is None:
        return exp_backoff
    parsed: float | None = None
    stripped = retry_after.strip()
    if stripped.isdigit():
        parsed = float(stripped)
    else:
        try:
            ts = email.utils.parsedate_to_datetime(stripped)
            parsed = max(0.0, ts.timestamp() - time.time())
        except Exception:
            parsed = None
    if parsed is not None and parsed > 0:
        return min(max(parsed, exp_backoff), _RETRY_AFTER_CAP)
    return min(exp_backoff, _RETRY_AFTER_CAP)


log = structlog.get_logger()

# Maximum subjects per batched POST.  Keeping this low avoids enormous
# response payloads; 25 subjects × ~50 persons + chars each ≈ 200-400 KB.
DEFAULT_BATCH_SIZE = 25


class BangumiGraphQLClient:
    """Async httpx client for the bangumi GraphQL API.

    Uses a single POST to the GraphQL endpoint.  The main
    throughput win is ``fetch_subjects_batched`` which sends N aliased queries
    in one request, amortising the 1-req/sec floor across N subjects.

    Usage::

        async with BangumiGraphQLClient() as client:
            # Single-subject (for verification)
            subject = await client.fetch_subject_full(328)

            # Batched (production backfill)
            batch = await client.fetch_subjects_batched([100, 200, 300])
            # batch == {100: {...}, 200: {...}, 300: {...}}

            person = await client.fetch_person_rest(9527)
            char   = await client.fetch_character_rest(4321)
    """

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 60.0,
    ) -> None:
        self._user_agent = user_agent
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None
        # Always use the shared limiter — GraphQL client is never called
        # from tests with a custom rate, so no local-override branch needed.
        self._limiter = _HOST_RATE_LIMITER

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> BangumiGraphQLClient:
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=True,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public fetch methods
    # ------------------------------------------------------------------

    async def fetch_subject_full(self, subject_id: int) -> dict[str, Any] | None:
        """Fetch one subject with nested persons, characters, and relations.

        Used for single-subject verification.  For production backfills,
        prefer ``fetch_subjects_batched`` to amortise the rate-limit floor.

        Args:
            subject_id: bangumi subject integer ID.

        Returns:
            Parsed ``data.subject`` dict, or None if the subject is not found.

        Raises:
            ScraperError: on permanent errors (non-NOT_FOUND GraphQL error, HTTP 4xx/5xx
                after max retries).
        """
        t0 = time.monotonic()
        query = SUBJECT_FULL_QUERY(subject_id)
        raw = await self._post_with_retry(query, context=f"subject_full id={subject_id}")
        if raw is None:
            return None
        result: dict[str, Any] | None = raw.get("data", {}).get("subject")
        log.info(
            "bangumi_graphql_fetch_done",
            query="subject_full",
            batch_size=1,
            subject_id=subject_id,
            duration_ms=round((time.monotonic() - t0) * 1000),
        )
        return result

    async def fetch_subjects_batched(
        self,
        subject_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """Fetch multiple subjects in a single POST using aliased GraphQL queries.

        Each subject is aliased as ``s{id}`` in the query document and parsed
        back to an integer key in the returned dict.  Subjects not found in the
        response (or returned as ``null`` by the server) are omitted from the
        result — callers should treat missing keys as 404.

        Args:
            subject_ids: non-empty list of bangumi subject IDs.  Recommended
                batch size is <= 25 (``DEFAULT_BATCH_SIZE``).

        Returns:
            Mapping from subject_id (int) → parsed subject dict.

        Raises:
            ValueError: if subject_ids is empty.
            ScraperError: on permanent HTTP/GraphQL errors.
        """
        if not subject_ids:
            raise ValueError("subject_ids must be non-empty")

        t0 = time.monotonic()
        query = SUBJECT_BATCH_QUERY(subject_ids)
        raw = await self._post_with_retry(query, context=f"subject_batch n={len(subject_ids)}")
        if raw is None:
            return {}

        data = raw.get("data") or {}
        result: dict[int, dict[str, Any]] = {}
        for sid in subject_ids:
            alias = f"s{sid}"
            subject_data = data.get(alias)
            if subject_data is not None:
                result[sid] = subject_data

        log.info(
            "bangumi_graphql_fetch_done",
            query="subject_batch",
            batch_size=len(subject_ids),
            returned=len(result),
            duration_ms=round((time.monotonic() - t0) * 1000),
        )
        return result

    async def fetch_person_rest(self, person_id: int) -> dict[str, Any] | None:
        """Fetch person via REST GET /v0/persons/{id}.

        Returns snake_case dict compatible with ``_build_person_row()`` directly —
        no adaptation step needed.  Fields include: gender, blood_type, birth_year,
        birth_mon, birth_day, last_modified, career, stat, images, infobox.

        Args:
            person_id: bangumi person integer ID.

        Returns:
            Person dict or None on 404.

        Raises:
            ScraperError: on non-404 HTTP errors after max retries.
        """
        from src.scrapers.queries.bangumi import person_url

        return await self._get_dict_with_retry(
            person_url(person_id), context=f"person_rest id={person_id}"
        )

    async def fetch_character_rest(self, character_id: int) -> dict[str, Any] | None:
        """Fetch character via REST GET /v0/characters/{id}.

        Returns snake_case dict compatible with ``_build_character_row()`` directly —
        no adaptation step needed.  Fields include: gender, blood_type, birth_year,
        birth_mon, birth_day, stat, nsfw, images, infobox.

        Args:
            character_id: bangumi character integer ID.

        Returns:
            Character dict or None on 404.

        Raises:
            ScraperError: on non-404 HTTP errors after max retries.
        """
        from src.scrapers.queries.bangumi import character_url

        return await self._get_dict_with_retry(
            character_url(character_id), context=f"character_rest id={character_id}"
        )

    async def fetch_subject_characters_rest(self, subject_id: int) -> list[dict[str, Any]]:
        """Fetch subject characters via REST GET /v0/subjects/{id}/characters.

        Returns the full character list with nested ``actors`` array per character.
        Returns [] on 404.

        Each character entry contains: id, name, relation (str label e.g. "主角"),
        type, images, actors[{id, name, type, career, images}].

        Args:
            subject_id: bangumi subject integer ID.

        Returns:
            List of character dicts, each with a nested actors list.

        Raises:
            ScraperError: on non-404 HTTP errors after max retries.
        """
        from src.scrapers.queries.bangumi import subject_characters_url

        url = subject_characters_url(subject_id)
        return await self._get_list_with_retry(url, context=f"subject_characters_rest id={subject_id}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_dict_with_retry(self, url: str, *, context: str = "") -> dict[str, Any] | None:
        """GET a REST endpoint that returns a JSON object. None on 404."""
        return await self._get_rest_retry(url, context=context, not_found=None)

    async def _get_list_with_retry(self, url: str, *, context: str = "") -> list[dict[str, Any]]:
        """GET a REST endpoint that returns a JSON array. [] on 404."""
        result = await self._get_rest_retry(url, context=context, not_found=[])
        return result if isinstance(result, list) else []

    async def _get_rest_retry(self, url: str, *, context: str = "", not_found: Any) -> Any:
        """GET with rate-limiting + exponential backoff retry.

        Args:
            url: target URL.
            context: label for log/error messages.
            not_found: value returned on HTTP 404.

        Returns:
            Parsed JSON body, or ``not_found`` on 404.

        Raises:
            RateLimitError: on 429 after max retries.
            ScraperError: on other permanent failures.
        """
        assert self._client is not None, (
            "BangumiGraphQLClient must be used as an async context manager"
        )
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._limiter.throttle()
            try:
                resp = await self._client.get(url)
            except httpx.TransportError as exc:
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"Transport error after {_MAX_ATTEMPTS} attempts: {exc}",
                        source=_SOURCE,
                        url=url,
                    ) from exc
                await asyncio.sleep(_compute_backoff_sleep(attempt, 0, None))
                continue
            if resp.status_code == 404:
                return not_found
            if resp.status_code == 429:
                wait = _compute_backoff_sleep(attempt, 429, resp.headers.get("Retry-After"))
                if attempt >= _MAX_ATTEMPTS:
                    raise RateLimitError(
                        "bangumi REST rate limit exceeded after max attempts",
                        source=_SOURCE,
                        url=url,
                        retry_after=wait,
                    )
                await asyncio.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = _compute_backoff_sleep(attempt, resp.status_code, resp.headers.get("Retry-After"))
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"REST server error {resp.status_code} after {_MAX_ATTEMPTS} attempts",
                        source=_SOURCE,
                        url=url,
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
            f"Failed REST GET after {_MAX_ATTEMPTS} attempts ({context})",
            source=_SOURCE,
            url=url,
        )

    async def _post_with_retry(
        self,
        query: str,
        context: str = "",
    ) -> dict[str, Any] | None:
        """POST a GraphQL query with rate-limiting and exponential backoff retry.

        Args:
            query: GraphQL query document string.
            context: human-readable label for log messages.

        Returns:
            Parsed JSON response body (full dict including ``data`` and ``errors``),
            or None when the server indicates NOT_FOUND.

        Raises:
            ScraperError: when retries are exhausted or a non-recoverable error occurs.
        """
        assert self._client is not None, (
            "BangumiGraphQLClient must be used as an async context manager"
        )

        body = json.dumps({"query": query}).encode()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._limiter.throttle()

            try:
                resp = await self._client.post(
                    BANGUMI_GRAPHQL_URL,
                    content=body,
                )
            except httpx.TransportError as exc:
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"Transport error after {_MAX_ATTEMPTS} attempts: {exc}",
                        source=_SOURCE,
                        url=BANGUMI_GRAPHQL_URL,
                    ) from exc
                wait = _compute_backoff_sleep(attempt, 0, None)
                log.warning(
                    "bangumi_graphql_transport_error",
                    context=context,
                    attempt=attempt,
                    wait_seconds=wait,
                    error=str(exc),
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 429:
                raw_retry_after = resp.headers.get("Retry-After")
                wait = _compute_backoff_sleep(attempt, 429, raw_retry_after)
                log.warning(
                    "bangumi_graphql_rate_limited",
                    context=context,
                    attempt=attempt,
                    retry_after_header=raw_retry_after,
                    wait_seconds=wait,
                )
                if raw_retry_after is not None:
                    log.info(
                        "bangumi_graphql_retry_after_honored",
                        attempt=attempt,
                        seconds=wait,
                    )
                if attempt >= _MAX_ATTEMPTS:
                    raise RateLimitError(
                        "bangumi GraphQL rate limit exceeded after max attempts",
                        source=_SOURCE,
                        url=BANGUMI_GRAPHQL_URL,
                        retry_after=wait,
                    )
                await asyncio.sleep(wait)
                continue

            if resp.status_code >= 500:
                raw_retry_after = resp.headers.get("Retry-After")
                wait = _compute_backoff_sleep(attempt, resp.status_code, raw_retry_after)
                log.warning(
                    "bangumi_graphql_server_error",
                    context=context,
                    status=resp.status_code,
                    attempt=attempt,
                    wait_seconds=wait,
                )
                if attempt >= _MAX_ATTEMPTS:
                    raise ScraperError(
                        f"GraphQL server error {resp.status_code} after {_MAX_ATTEMPTS} attempts",
                        source=_SOURCE,
                        url=BANGUMI_GRAPHQL_URL,
                    )
                await asyncio.sleep(wait)
                continue

            if not (200 <= resp.status_code < 300):
                raise ScraperError(
                    f"Unexpected HTTP {resp.status_code}",
                    source=_SOURCE,
                    url=BANGUMI_GRAPHQL_URL,
                )

            # Parse the GraphQL envelope.
            parsed: dict[str, Any] = resp.json()

            # GraphQL errors array handling:
            #   NOT_FOUND → return None (same contract as v0 404)
            #   other     → raise ScraperError
            errors = parsed.get("errors")
            if errors:
                first = errors[0] if isinstance(errors, list) and errors else {}
                code = (first.get("extensions") or {}).get("code", "")
                if code == "NOT_FOUND":
                    log.info(
                        "bangumi_graphql_not_found",
                        context=context,
                    )
                    return None
                log.error(
                    "bangumi_graphql_error",
                    context=context,
                    errors=errors,
                )
                raise ScraperError(
                    f"GraphQL errors: {errors}",
                    source=_SOURCE,
                    url=BANGUMI_GRAPHQL_URL,
                )

            return parsed

        raise ScraperError(
            f"Failed GraphQL POST after {_MAX_ATTEMPTS} attempts ({context})",
            source=_SOURCE,
            url=BANGUMI_GRAPHQL_URL,
        )


def adapt_subject_persons_gql(
    subject_id: int,
    subject_gql: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract the persons list from a GraphQL subject node into v0 REST shape.

    Converts the GraphQL ``persons`` sub-list to the same shape returned by
    ``GET /v0/subjects/{id}/persons``, compatible with ``_build_person_rows()``.

    GraphQL person node (in subject context):
        id, name, type, career, images, eps, relation

    v0 REST shape (expected by ``_build_person_rows``):
        id, name, type, relation, career, eps, images

    Args:
        subject_id: bangumi subject ID (used for context; not mutated into rows here).
        subject_gql: raw ``data.s{id}`` or ``data.subject`` dict.

    Returns:
        List of person dicts in v0-compatible shape.
    """
    persons = subject_gql.get("persons") or []
    result = []
    for entry in persons:
        p = entry.get("person") or {}
        result.append(
            {
                "id": p.get("id"),
                "name": p.get("name"),
                "type": p.get("type"),
                "relation": "" if entry.get("position") is None else str(entry["position"]),
                "career": p.get("career") or [],
                "eps": "",
                "images": _flatten_images(p.get("images")),
            }
        )
    return result


def adapt_subject_characters_rest(
    rest_chars: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Normalize REST GET /v0/subjects/{id}/characters response for _build_character_and_actor_rows.

    REST format per entry: {id, name, relation (str e.g. "主角"), type, images,
    actors[{id, name, type, career, images}]}.

    Args:
        rest_chars: raw list from REST GET /v0/subjects/{id}/characters.

    Returns:
        List of character dicts compatible with ``_build_character_and_actor_rows()``.
    """
    result = []
    for c in rest_chars:
        result.append(
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "type": c.get("type"),
                "relation": str(c.get("relation") or ""),
                "images": _flatten_images(c.get("images")),
                "summary": c.get("summary") or "",
                "actors": c.get("actors") or [],
            }
        )
    return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _flatten_images(images: dict[str, Any] | None) -> dict[str, Any]:
    """Return images dict (or empty dict if None).

    GraphQL returns images as a flat object ``{large, medium, small, grid}``,
    which is the same shape as the v0 REST API.  This helper is a no-op pass-
    through for clarity.

    Args:
        images: raw GraphQL images object or None.

    Returns:
        Images dict with at least empty string values for standard keys.
    """
    if not images:
        return {}
    return {
        "large": images.get("large") or "",
        "medium": images.get("medium") or "",
        "small": images.get("small") or "",
        "grid": images.get("grid") or "",
    }

"""bangumi GraphQL API async client.

This is the MAIN PATH for new bangumi scraping.  The v0 REST client
(``bangumi_scraper.BangumiClient``) is retained as a --client v0 fallback.

Endpoint: https://api.bgm.tv/v0/graphql (POST, Content-Type: application/json)
Server: bangumi/server-private (Fastify + mercurius).  Altair UI: /v0/altair/
Confirmed: 2026-04-25.

Rate limit:
    Shares ``_HOST_RATE_LIMITER`` from ``bangumi_scraper`` so that GraphQL and
    v0 REST requests never collectively exceed 1 req/sec on api.bgm.tv.

Error semantics:
    - GraphQL ``errors[0].extensions.code == "NOT_FOUND"``  → returns None (like 404).
    - Any other non-empty ``errors`` array → raises ``ScraperError``.
    - HTTP 4xx (non-429) → raises ``ScraperError``.
    - HTTP 429 / 5xx     → exponential backoff retry (same as v0 client).
    - Transport error    → same retry logic.

Logging (structlog):
    ``bangumi_graphql_fetch_done  query=<kind>  batch_size=<N>  duration_ms=<ms>``
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import structlog

from src.scrapers.bangumi_scraper import (
    _HOST_RATE_LIMITER,
    _MAX_ATTEMPTS,
    _SOURCE,
    _compute_backoff_sleep,
)
from src.scrapers.exceptions import RateLimitError, ScraperError
from src.scrapers.queries.bangumi_graphql import (
    BANGUMI_GRAPHQL_URL,
    CHARACTER_QUERY,
    DEFAULT_USER_AGENT,
    PERSON_QUERY,
    SUBJECT_BATCH_QUERY,
    SUBJECT_FULL_QUERY,
)

log = structlog.get_logger()

# Maximum subjects per batched POST.  Keeping this low avoids enormous
# response payloads; 25 subjects × ~50 persons + chars each ≈ 200-400 KB.
DEFAULT_BATCH_SIZE = 25


class BangumiGraphQLClient:
    """Async httpx client for the bangumi GraphQL API.

    Implements the same retry / rate-limit discipline as ``BangumiClient``
    (v0 REST) but uses a single POST to the GraphQL endpoint.  The main
    throughput win is ``fetch_subjects_batched`` which sends N aliased queries
    in one request, amortising the 1-req/sec floor across N subjects.

    Usage::

        async with BangumiGraphQLClient() as client:
            # Single-subject (for verification)
            subject = await client.fetch_subject_full(328)

            # Batched (production backfill)
            batch = await client.fetch_subjects_batched([100, 200, 300])
            # batch == {100: {...}, 200: {...}, 300: {...}}

            person = await client.fetch_person(9527)
            char   = await client.fetch_character(4321)
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

    async def fetch_person(self, person_id: int) -> dict[str, Any] | None:
        """Fetch one person's full detail.

        Returns the parsed ``data.person`` dict, or None if not found.

        The returned dict uses camelCase field names from the GraphQL schema
        (e.g. ``bloodType``, ``birthYear``).  Use ``adapt_person_gql_to_v0()``
        to normalise to the v0-compatible shape expected by ``_build_person_row``.

        Args:
            person_id: bangumi person integer ID.

        Returns:
            Parsed person dict, or None on NOT_FOUND.
        """
        t0 = time.monotonic()
        query = PERSON_QUERY(person_id)
        raw = await self._post_with_retry(query, context=f"person id={person_id}")
        if raw is None:
            return None
        result: dict[str, Any] | None = raw.get("data", {}).get("person")
        log.info(
            "bangumi_graphql_fetch_done",
            query="person",
            batch_size=1,
            person_id=person_id,
            duration_ms=round((time.monotonic() - t0) * 1000),
        )
        return result

    async def fetch_character(self, character_id: int) -> dict[str, Any] | None:
        """Fetch one character's full detail.

        Returns the parsed ``data.character`` dict, or None if not found.

        The returned dict uses camelCase field names.  Use
        ``adapt_character_gql_to_v0()`` to normalise to the v0-compatible shape.

        Args:
            character_id: bangumi character integer ID.

        Returns:
            Parsed character dict, or None on NOT_FOUND.
        """
        t0 = time.monotonic()
        query = CHARACTER_QUERY(character_id)
        raw = await self._post_with_retry(query, context=f"character id={character_id}")
        if raw is None:
            return None
        result: dict[str, Any] | None = raw.get("data", {}).get("character")
        log.info(
            "bangumi_graphql_fetch_done",
            query="character",
            batch_size=1,
            character_id=character_id,
            duration_ms=round((time.monotonic() - t0) * 1000),
        )
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Response adapters
# ---------------------------------------------------------------------------
# These functions convert the GraphQL camelCase response shape to the
# snake_case / nested shape that the v0-based BRONZE row builders expect.
# They are intentionally thin — the row builders are NOT rewritten.
#
# GraphQL field → v0 REST field mapping (person):
#   bloodType   → blood_type
#   birthYear   → birth_year
#   birthMon    → birth_mon
#   birthDay    → birth_day
#   lastModified → last_modified
#   stat.comments / stat.collects → stat.comments / stat.collects  (unchanged)
#   infobox[].values[].k / .v → infobox[].key / .value  (re-mapped; see note)
#
# GraphQL infobox shape:
#   { key: "...", values: [{ k: "...", v: "..." }] }
# v0 REST infobox shape (same, just nested differently):
#   { key: "...", value: [{ k: "...", v: "..." }] }  ← "value" not "values"
# → adapter renames "values" → "value" for compatibility.


def adapt_person_gql_to_v0(gql: dict[str, Any]) -> dict[str, Any]:
    """Convert a GraphQL ``person`` node to the v0 REST response shape.

    The output dict is compatible with ``_build_person_row()`` in
    ``scrape_bangumi_persons.py`` without any changes to that function.

    GraphQL → v0 field mapping:
        bloodType    → blood_type
        birthYear    → birth_year
        birthMon     → birth_mon
        birthDay     → birth_day
        lastModified → last_modified
        infobox[].values → infobox[].value

    Args:
        gql: raw ``data.person`` dict from the GraphQL response.

    Returns:
        Dict compatible with ``_build_person_row``.
    """
    out = dict(gql)
    out["blood_type"] = out.pop("bloodType", None)
    out["birth_year"] = out.pop("birthYear", None)
    out["birth_mon"] = out.pop("birthMon", None)
    out["birth_day"] = out.pop("birthDay", None)
    out["last_modified"] = out.pop("lastModified", None) or ""
    out["infobox"] = _adapt_infobox(out.get("infobox") or [])
    # images: GraphQL returns a flat dict {large, medium, small, grid} — same shape as v0.
    return out


def adapt_character_gql_to_v0(gql: dict[str, Any]) -> dict[str, Any]:
    """Convert a GraphQL ``character`` node to the v0 REST response shape.

    The output dict is compatible with ``_build_character_row()`` in
    ``scrape_bangumi_characters.py`` without any changes to that function.

    Note: ``last_modified`` is absent from character responses in both v0 REST
    and GraphQL — the adapter does not add it.

    GraphQL → v0 field mapping:
        bloodType → blood_type
        birthYear → birth_year
        birthMon  → birth_mon
        birthDay  → birth_day
        infobox[].values → infobox[].value

    Args:
        gql: raw ``data.character`` dict from the GraphQL response.

    Returns:
        Dict compatible with ``_build_character_row``.
    """
    out = dict(gql)
    out["blood_type"] = out.pop("bloodType", None)
    out["birth_year"] = out.pop("birthYear", None)
    out["birth_mon"] = out.pop("birthMon", None)
    out["birth_day"] = out.pop("birthDay", None)
    out["infobox"] = _adapt_infobox(out.get("infobox") or [])
    return out


def adapt_subject_persons_gql(
    subject_id: int,
    subject_gql: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract the persons list from a GraphQL subject node into v0 REST shape.

    Converts the GraphQL ``persons`` sub-list to the same shape returned by
    ``GET /v0/subjects/{id}/persons``, so that ``_build_person_rows()`` in
    ``scrape_bangumi_relations.py`` can consume it unchanged.

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
    for p in persons:
        result.append(
            {
                "id": p.get("id"),
                "name": p.get("name"),
                "type": p.get("type"),
                "relation": p.get("relation"),
                "career": p.get("career") or [],
                "eps": p.get("eps") or "",
                "images": _flatten_images(p.get("images")),
            }
        )
    return result


def adapt_subject_characters_gql(
    subject_id: int,
    subject_gql: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract the characters list from a GraphQL subject node into v0 REST shape.

    Converts the GraphQL ``characters`` sub-list (including nested ``actors``)
    to the same shape returned by ``GET /v0/subjects/{id}/characters``, so that
    ``_build_character_and_actor_rows()`` in ``scrape_bangumi_relations.py`` can
    consume it unchanged.

    GraphQL character node (in subject context):
        id, name, type, relation, images, actors[{id, name, type, career, images}]

    v0 REST shape (expected by ``_build_character_and_actor_rows``):
        id, name, type, relation, images, summary, actors[{id, name, type, career}]

    Args:
        subject_id: bangumi subject ID (unused; present for API symmetry).
        subject_gql: raw ``data.s{id}`` or ``data.subject`` dict.

    Returns:
        List of character dicts in v0-compatible shape (actors list nested inside).
    """
    characters = subject_gql.get("characters") or []
    result = []
    for c in characters:
        actors = []
        for a in c.get("actors") or []:
            actors.append(
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "type": a.get("type"),
                    "career": a.get("career") or [],
                }
            )
        result.append(
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "type": c.get("type"),
                "relation": c.get("relation"),
                "images": _flatten_images(c.get("images")),
                "summary": c.get("summary") or "",
                "actors": actors,
            }
        )
    return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _adapt_infobox(infobox: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rename ``values`` → ``value`` in each infobox entry for v0 compatibility.

    GraphQL schema uses ``values`` (plural); the v0 REST API uses ``value``
    (singular) for the nested list.  Row builders call ``json.dumps(infobox)``
    directly, so the field name must match what they expect.

    Args:
        infobox: raw GraphQL infobox list.

    Returns:
        Infobox list with each entry's ``values`` key renamed to ``value``.
    """
    result = []
    for entry in infobox:
        adapted = dict(entry)
        if "values" in adapted:
            adapted["value"] = adapted.pop("values")
        result.append(adapted)
    return result


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

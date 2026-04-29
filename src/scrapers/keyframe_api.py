"""KeyFrame Staff List — non-public API client.

Endpoints covered:
  GET /api/data/roles.php                              — role master (1924 items)
  GET /api/data/translate.v4.php?ja=<name>&uuid=      — name→AniList ID (ja or en param)
  GET /api/person/show.php?id=<id>&type=person         — person detail + credits
  GET /api/person/get_by_id.php?id=<id>&studio=<0|1>  — lightweight name lookup
  GET /api/stafflists/preview.php                      — top-page snapshot
  GET /api/search/?q=<q>&type=staff&offset=<N>         — staff search
  GET /api/search/?q=<q>&type=all&offset=<N>           — staff + stafflists search
"""

from __future__ import annotations

from urllib.parse import quote

import structlog

from src.scrapers.http_client import RetryingHttpClient

log = structlog.get_logger()

BASE = "https://keyframe-staff-list.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36 "
        "(animetor-eval research scraper; contact: akizora.biz@gmail.com)"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ja,en;q=0.9",
}

_HTML_HEADERS = {
    **_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_DELAY = 5.0

# keyframe-staff-list.com enforces a hard daily quota (~300 req/day per IP).
# Once exhausted, 429 persists until UTC rollover — retrying within the same
# day burns time without recovery, so we exclude 429 from the retry set and
# raise KeyframeQuotaExceeded for callers to detect and gracefully halt.
_RETRYABLE_STATUS_NO_429 = frozenset({500, 502, 503, 504, 522, 524})


class KeyframeQuotaExceeded(Exception):
    """Daily request quota (~300/day) exhausted. Resume after UTC rollover."""


class KeyframeApiClient:
    """Async HTTP client for keyframe-staff-list.com API endpoints.

    Thin wrapper around RetryingHttpClient: adds JSON parsing, 404→None
    conversion, and named endpoint methods. Retry/throttle/Retry-After
    handling is delegated to RetryingHttpClient (429 excluded — see
    KeyframeQuotaExceeded).
    """

    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        self._client = RetryingHttpClient(
            source="keyframe",
            delay=delay,
            timeout=60.0,
            headers=_HEADERS,
            base_url=BASE,
            retryable_status=_RETRYABLE_STATUS_NO_429,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "KeyframeApiClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> "object | None":
        """GET url with shared 404→None / 429→KeyframeQuotaExceeded handling.

        Returns the raw httpx.Response when the call succeeded (caller decides
        whether to read .json() or .text), or None on 404.
        """
        resp = await self._client.request("GET", url, headers=headers)
        if resp.status_code == 404:
            log.debug("keyframe_not_found", url=url)
            return None
        if resp.status_code == 429:
            log.warning("keyframe_quota_exceeded", url=url)
            raise KeyframeQuotaExceeded(f"429 from {url}")
        resp.raise_for_status()
        return resp

    async def _get_json(self, url: str) -> dict | list | None:
        """GET url and return parsed JSON. 404→None. 429→KeyframeQuotaExceeded."""
        resp = await self._get(url)
        return resp.json() if resp is not None else None

    async def _get_html(self, url: str) -> str | None:
        """GET url with HTML Accept header. 404→None. 429→KeyframeQuotaExceeded."""
        resp = await self._get(url, headers=_HTML_HEADERS)
        return resp.text if resp is not None else None

    # ------------------------------------------------------------------
    # Public endpoint methods
    # ------------------------------------------------------------------

    async def get_roles_master(self) -> list[dict] | None:
        """Fetch /api/data/roles.php — complete role master list (~1924 items)."""
        return await self._get_json(f"{BASE}/api/data/roles.php")  # type: ignore[return-value]

    async def get_person_show(self, person_id: int) -> dict | None:
        """Fetch /api/person/show.php?id=<id>&type=person — person detail + all credits."""
        return await self._get_json(f"{BASE}/api/person/show.php?id={person_id}&type=person")  # type: ignore[return-value]

    async def get_preview(self) -> dict | None:
        """Fetch /api/stafflists/preview.php — recent/airing/data top-page snapshot."""
        return await self._get_json(f"{BASE}/api/stafflists/preview.php")  # type: ignore[return-value]

    async def search_staff(self, query: str, offset: int = 0) -> dict | None:
        """Fetch /api/search/?q=<q>&type=staff — staff search results (50/page)."""
        return await self._get_json(f"{BASE}/api/search/?q={quote(query)}&type=staff&offset={offset}")  # type: ignore[return-value]

    async def search_all(self, query: str, offset: int = 0) -> dict | None:
        """Fetch /api/search/?q=<q>&type=all — staff + stafflists search (50/page)."""
        return await self._get_json(f"{BASE}/api/search/?q={quote(query)}&type=all&offset={offset}")  # type: ignore[return-value]

    async def translate_name(
        self,
        name: str,
        lang: str = "ja",
        category: str | None = None,
    ) -> list[dict] | None:
        """Fetch /api/data/translate.v4.php — name→AniList ID mapping.

        Args:
            name: Person name to look up.
            lang: 'ja' or 'en' — which param name to use.
            category: Optional category filter.

        Returns:
            List of match dicts (empty = no match, multiple = ambiguous).
            None on network/server error.
        """
        cat_param = f"&category={quote(category)}" if category else ""
        url = f"{BASE}/api/data/translate.v4.php?{lang}={quote(name)}&uuid={cat_param}"
        return await self._get_json(url)  # type: ignore[return-value]

    async def get_person_by_id(self, person_id: int | str, is_studio: bool = False) -> dict | None:
        """Fetch /api/person/get_by_id.php — lightweight name lookup by keyframe ID."""
        studio_flag = 1 if is_studio else 0
        url = f"{BASE}/api/person/get_by_id.php?id={quote(str(person_id))}&studio={studio_flag}"
        return await self._get_json(url)  # type: ignore[return-value]

    async def get_sitemap(self) -> str | None:
        """Fetch /sitemap.xml — returns raw XML text."""
        return await self._get_html(f"{BASE}/sitemap.xml")

    async def get_anime_page(self, slug: str) -> str | None:
        """Fetch /staff/<slug> — returns raw HTML text containing preloadData."""
        return await self._get_html(f"{BASE}/staff/{slug}")

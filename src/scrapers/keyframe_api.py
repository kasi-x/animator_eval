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


class KeyframeApiClient:
    """Async HTTP client for keyframe-staff-list.com API endpoints.

    Thin wrapper around RetryingHttpClient: adds JSON parsing, 404→None
    conversion, and named endpoint methods. Retry/throttle/Retry-After
    handling is delegated to RetryingHttpClient.
    """

    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        self._client = RetryingHttpClient(
            source="keyframe",
            delay=delay,
            timeout=60.0,
            headers=_HEADERS,
            base_url=BASE,
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

    async def _get_json(self, url: str) -> dict | list | None:
        """GET url and return parsed JSON, or None on 404."""
        resp = await self._client.request("GET", url)
        if resp.status_code == 404:
            log.debug("keyframe_api_not_found", url=url)
            return None
        resp.raise_for_status()
        return resp.json()

    async def _get_html(self, url: str) -> str | None:
        """GET url with HTML Accept header and return text, or None on 404."""
        resp = await self._client.request("GET", url, headers=_HTML_HEADERS)
        if resp.status_code == 404:
            log.debug("keyframe_html_not_found", url=url)
            return None
        resp.raise_for_status()
        return resp.text

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

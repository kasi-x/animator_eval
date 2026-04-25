"""KeyFrame Staff List — non-public API client.

Wraps the discovered JSON endpoints with retry, rate-limit, and Retry-After
handling. All requests are serialised through _get_with_retry which enforces
a minimum inter-request delay to protect the server.

Endpoints covered:
  GET /api/data/roles.php                        — role master (1924 items)
  GET /api/person/show.php?id=<id>&type=person   — person detail + credits
  GET /api/stafflists/preview.php                — top-page snapshot
  GET /api/search/?q=<q>&type=staff&offset=<N>   — staff search
"""

from __future__ import annotations

import asyncio
import time
from urllib.parse import quote

import httpx
import structlog

log = structlog.get_logger()

BASE = "https://keyframe-staff-list.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36 "
        "(animetor-eval research scraper; contact: akizora.biz@gmail.com)"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ja,en;q=0.9",
}

HTML_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}

DEFAULT_DELAY = 5.0
MAX_RETRIES = 5


class KeyframeApiClient:
    """Async HTTP client for keyframe-staff-list.com API endpoints.

    Enforces a minimum inter-request delay and handles 429 Retry-After
    responses. All methods return parsed JSON or None on permanent failure.
    """

    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        self.delay = delay
        self._client = httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            headers=HEADERS,
        )
        self._last_request_at: float = 0.0

    async def close(self) -> None:
        """Close the underlying httpx client."""
        await self._client.aclose()

    async def __aenter__(self) -> "KeyframeApiClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Sleep if the last request was too recent."""
        now = time.monotonic()
        wait = self.delay - (now - self._last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request_at = time.monotonic()

    async def _get_with_retry(self, url: str) -> dict | list | None:
        """GET url with retry, rate-limit, and server-error handling.

        Returns: parsed JSON (dict or list) or None on permanent failure.
        """
        await self._throttle()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await self._client.get(url)
            except httpx.RequestError as exc:
                wait_s = max(self.delay * 2, 10 * attempt)
                log.warning(
                    "keyframe_api_request_error",
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                    err=str(exc)[:120],
                )
                await asyncio.sleep(wait_s)
                continue

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404:
                log.debug("keyframe_api_not_found", url=url)
                return None

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                wait_s = max(retry_after, 30, 10 * attempt)
                log.warning(
                    "keyframe_api_rate_limited",
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                )
                await asyncio.sleep(wait_s)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait_s = max(self.delay * 2, 15 * attempt)
                log.warning(
                    "keyframe_api_server_error",
                    status=resp.status_code,
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                )
                await asyncio.sleep(wait_s)
                continue

            log.warning(
                "keyframe_api_unhandled_status",
                status=resp.status_code,
                url=url,
            )
            return None

        log.warning("keyframe_api_max_retries", url=url)
        return None

    async def _get_html_with_retry(self, url: str) -> str | None:
        """GET url expecting HTML response with retry handling.

        Returns: response text or None on permanent failure.
        """
        await self._throttle()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await self._client.get(url, headers=HTML_HEADERS)
            except httpx.RequestError as exc:
                wait_s = max(self.delay * 2, 10 * attempt)
                log.warning(
                    "keyframe_html_request_error",
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                    err=str(exc)[:120],
                )
                await asyncio.sleep(wait_s)
                continue

            if resp.status_code == 200:
                return resp.text

            if resp.status_code == 404:
                log.debug("keyframe_html_not_found", url=url)
                return None

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                wait_s = max(retry_after, 30, 10 * attempt)
                log.warning(
                    "keyframe_html_rate_limited",
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                )
                await asyncio.sleep(wait_s)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait_s = max(self.delay * 2, 15 * attempt)
                log.warning(
                    "keyframe_html_server_error",
                    status=resp.status_code,
                    url=url,
                    attempt=attempt,
                    wait_s=wait_s,
                )
                await asyncio.sleep(wait_s)
                continue

            log.warning(
                "keyframe_html_unhandled_status",
                status=resp.status_code,
                url=url,
            )
            return None

        log.warning("keyframe_html_max_retries", url=url)
        return None

    # ------------------------------------------------------------------
    # Public endpoint methods
    # ------------------------------------------------------------------

    async def get_roles_master(self) -> list[dict] | None:
        """Fetch /api/data/roles.php — complete role master list (~1924 items)."""
        return await self._get_with_retry(f"{BASE}/api/data/roles.php")  # type: ignore[return-value]

    async def get_person_show(self, person_id: int) -> dict | None:
        """Fetch /api/person/show.php?id=<id>&type=person — person detail + all credits."""
        url = f"{BASE}/api/person/show.php?id={person_id}&type=person"
        return await self._get_with_retry(url)  # type: ignore[return-value]

    async def get_preview(self) -> dict | None:
        """Fetch /api/stafflists/preview.php — recent/airing/data top-page snapshot."""
        return await self._get_with_retry(f"{BASE}/api/stafflists/preview.php")  # type: ignore[return-value]

    async def search_staff(self, query: str, offset: int = 0) -> dict | None:
        """Fetch /api/search/?q=<q>&type=staff — staff search results (50/page)."""
        url = f"{BASE}/api/search/?q={quote(query)}&type=staff&offset={offset}"
        return await self._get_with_retry(url)  # type: ignore[return-value]

    async def get_sitemap(self) -> str | None:
        """Fetch /sitemap.xml — returns raw XML text."""
        return await self._get_html_with_retry(f"{BASE}/sitemap.xml")

    async def get_anime_page(self, slug: str) -> str | None:
        """Fetch /staff/<slug> — returns raw HTML text containing preloadData."""
        return await self._get_html_with_retry(f"{BASE}/staff/{slug}")

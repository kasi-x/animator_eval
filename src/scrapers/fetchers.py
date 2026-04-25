"""Fetcher helpers for ScrapeRunner.

Each fetcher is a callable: async (id) → raw | None.
  None = skip (404 / permanent failure / not-found).
  Raises = transient error (will be counted as error in Stats).

Available fetchers:
  HtmlFetcher       — URL template → HTML text
  XmlBatchFetcher   — slash-delimited batch XML endpoint (ANN-style)
  JsonFetcher       — callable endpoint → JSON dict/list

GraphQL (AniList) is not covered here; its pagination and auth
are too special-cased for a generic wrapper.
"""

from __future__ import annotations

from collections.abc import Callable

import structlog

from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.http_client import RetryingHttpClient

log = structlog.get_logger()


class HtmlFetcher:
    """Fetch a single HTML page by ID.

    Args:
        client:       RetryingHttpClient instance (shared by caller).
        url_pattern:  URL template with ``{id}`` placeholder.
                      e.g. ``"https://example.com/item/{id}"``
        namespace:    cache_store namespace; None = disable caching.
        source:       label for log events.
    """

    def __init__(
        self,
        client: RetryingHttpClient,
        url_pattern: str,
        *,
        namespace: str | None = None,
        source: str = "html",
    ) -> None:
        self._client = client
        self._url_pattern = url_pattern
        self._namespace = namespace
        self._source = source

    async def __call__(self, item_id) -> str | None:
        """Fetch URL for item_id; return HTML text or None for 404."""
        return await self._fetch_html(item_id)

    async def _fetch_html(self, item_id) -> str | None:
        """Build URL, check cache, GET, return text or None."""
        url = self._url_pattern.format(id=item_id)

        if self._namespace is not None:
            cached = load_cached_json(self._namespace, {"url": url})
            if cached is not None:
                return cached

        resp = await self._client.get(url)
        if resp.status_code == 404:
            log.debug(f"{self._source}_not_found", url=url, item_id=item_id)
            return None
        resp.raise_for_status()

        if self._namespace is not None:
            save_cached_json(self._namespace, {"url": url}, resp.text)

        return resp.text


class XmlBatchFetcher:
    """Fetch multiple IDs in one slash-delimited GET (ANN-style XML API).

    The ANN API accepts up to ``batch_size`` IDs like:
      ``https://host/api.xml?anime=1/2/3/4``

    Since ScrapeRunner iterates one ID at a time, ``__call__`` accepts
    either a single ID or a list/tuple of IDs.  The caller is responsible
    for grouping IDs into batches *before* passing them to ScrapeRunner
    (see ann_scraper._run_scrape_anime for the pattern).

    Args:
        client:         RetryingHttpClient instance.
        endpoint:       Base URL of the XML API (without query string).
        batch_size:     Max IDs per request (default 50).
        id_param_name:  Query parameter name (e.g. ``"anime"`` or ``"people"``).
        namespace:      cache_store namespace; None = disable caching.
        source:         label for log events.
    """

    def __init__(
        self,
        client: RetryingHttpClient,
        endpoint: str,
        *,
        batch_size: int = 50,
        id_param_name: str = "anime",
        namespace: str | None = None,
        source: str = "xml_batch",
    ) -> None:
        self._client = client
        self._endpoint = endpoint
        self._batch_size = batch_size
        self._id_param_name = id_param_name
        self._namespace = namespace
        self._source = source

    async def __call__(self, ids) -> str | None:
        """Fetch XML for one or more IDs; return raw XML text or None."""
        return await self._fetch_batch(ids)

    async def _fetch_batch(self, ids) -> str | None:
        """Build slash-delimited URL, fetch, return XML text."""
        if isinstance(ids, (list, tuple)):
            ids_str = "/".join(str(i) for i in ids)
        else:
            ids_str = str(ids)

        url = f"{self._endpoint}?{self._id_param_name}={ids_str}"

        if self._namespace is not None:
            cached = load_cached_json(self._namespace, {"url": url})
            if cached is not None:
                return cached

        resp = await self._client.get(url)
        if resp.status_code == 404:
            log.debug(f"{self._source}_not_found", url=url)
            return None
        resp.raise_for_status()

        text = resp.text.lstrip()
        if self._namespace is not None:
            save_cached_json(self._namespace, {"url": url}, text)

        return text


class JsonFetcher:
    """Fetch a JSON endpoint where the URL is computed per ID.

    Args:
        client:       RetryingHttpClient instance.
        endpoint_fn:  Callable ID → URL string.
        namespace:    cache_store namespace; None = disable caching.
        source:       label for log events.
    """

    def __init__(
        self,
        client: RetryingHttpClient,
        endpoint_fn: Callable[[object], str],
        *,
        namespace: str | None = None,
        source: str = "json",
    ) -> None:
        self._client = client
        self._endpoint_fn = endpoint_fn
        self._namespace = namespace
        self._source = source

    async def __call__(self, item_id) -> dict | list | None:
        """Fetch JSON for item_id; return parsed object or None for 404."""
        return await self._fetch_json(item_id)

    async def _fetch_json(self, item_id) -> dict | list | None:
        """Compute URL, check cache, GET, return parsed JSON or None."""
        url = self._endpoint_fn(item_id)

        if self._namespace is not None:
            cached = load_cached_json(self._namespace, {"url": url})
            if cached is not None:
                return cached

        resp = await self._client.get(url)
        if resp.status_code == 404:
            log.debug(f"{self._source}_not_found", url=url, item_id=item_id)
            return None
        resp.raise_for_status()

        data = resp.json()
        if self._namespace is not None:
            save_cached_json(self._namespace, {"url": url}, data)

        return data

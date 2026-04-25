"""Common base class for rate-limited async HTTP scrapers.

All scraper clients inherit from RateLimitedHttpClient to get a shared
_throttle() implementation. Subclasses override _throttle() when they use
a different throttling mechanism (e.g. BangumiClient uses a module-level
singleton _HostRateLimiter shared across client instances).

Usage::

    class MyClient(RateLimitedHttpClient):
        DEFAULT_DELAY = 1.5

        def __init__(self) -> None:
            super().__init__(delay=self.DEFAULT_DELAY)
            self._http = httpx.AsyncClient(...)

        async def close(self) -> None:
            await self._http.aclose()
"""
from __future__ import annotations

import asyncio
import time


class RateLimitedHttpClient:
    """Base class for scrapers that need per-request rate throttling.

    Provides _throttle() based on minimum interval between requests.
    Subclasses that use an external rate limiter (e.g. BangumiClient's
    module-level _HostRateLimiter) should override _throttle().
    """

    DEFAULT_DELAY: float = 1.0

    def __init__(self, *, delay: float | None = None) -> None:
        self._delay = delay if delay is not None else self.DEFAULT_DELAY
        self._last_request: float = 0.0

    async def _throttle(self) -> None:
        """Sleep until >= _delay seconds have elapsed since the last call."""
        elapsed = time.monotonic() - self._last_request
        wait = self._delay - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request = time.monotonic()

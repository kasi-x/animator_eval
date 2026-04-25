"""Common base class for rate-limited async HTTP scrapers.

All scraper clients inherit from RateLimitedHttpClient to get a shared
_gate() / _throttle() implementation.

Usage — legacy fixed-delay::

    class MyClient(RateLimitedHttpClient):
        def __init__(self) -> None:
            super().__init__(delay=1.0)

Usage — dual sliding-window (Jikan v4)::

    LIMITER = DualWindowRateLimiter(per_second=3, per_minute=60)

    class JikanClient(RateLimitedHttpClient):
        def __init__(self) -> None:
            super().__init__(limiter=LIMITER)
"""
from __future__ import annotations

import asyncio
import time
from collections import deque


class DualWindowRateLimiter:
    """Sliding-window rate limiter with independent per-second and per-minute budgets.

    Jikan v4 official limits: per_second=3, per_minute=60.
    Cache hits bypass acquire() — call acquire() only directly before HTTP requests.
    """

    def __init__(self, per_second: int, per_minute: int) -> None:
        self.per_second = per_second
        self.per_minute = per_minute
        self._sec_window: deque[float] = deque()
        self._min_window: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._sec_window and now - self._sec_window[0] >= 1.0:
                    self._sec_window.popleft()
                while self._min_window and now - self._min_window[0] >= 60.0:
                    self._min_window.popleft()
                wait = 0.0
                if len(self._sec_window) >= self.per_second:
                    wait = max(wait, 1.0 - (now - self._sec_window[0]))
                if len(self._min_window) >= self.per_minute:
                    wait = max(wait, 60.0 - (now - self._min_window[0]))
                if wait <= 0:
                    self._sec_window.append(now)
                    self._min_window.append(now)
                    return
                await asyncio.sleep(wait + 0.01)


class RateLimitedHttpClient:
    """Base class for scrapers that need per-request rate throttling.

    Two modes:
    - ``delay=`` (float): fixed minimum interval between requests (legacy).
    - ``limiter=`` (DualWindowRateLimiter): sliding-window rate limiter.

    ``limiter`` takes priority when both are provided.
    Subclasses call ``await self._gate()`` before each HTTP request.
    """

    DEFAULT_DELAY: float = 1.0

    def __init__(
        self,
        *,
        delay: float | None = None,
        limiter: DualWindowRateLimiter | None = None,
    ) -> None:
        self._delay = delay if delay is not None else self.DEFAULT_DELAY
        self._limiter = limiter
        self._last_request: float = 0.0
        self._lock = asyncio.Lock()

    async def _gate(self) -> None:
        """Acquire rate limit slot before issuing an HTTP request."""
        if self._limiter is not None:
            await self._limiter.acquire()
            return
        async with self._lock:
            elapsed = time.monotonic() - self._last_request
            wait = self._delay - elapsed
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()

    # Legacy alias used by older subclasses
    async def _throttle(self) -> None:
        await self._gate()

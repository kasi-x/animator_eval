"""DualWindowRateLimiter unit tests.

No pytest-asyncio — async tests use asyncio.run() wrappers.
"""

from __future__ import annotations

import asyncio
import time

from src.scrapers.http_base import DualWindowRateLimiter, RateLimitedHttpClient


def _run(coro):
    return asyncio.run(coro)


def test_per_second_limit():
    """4 requests with per_second=3 → 4th delayed ~1s."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=3, per_minute=600)
        t0 = time.monotonic()
        for _ in range(4):
            await lim.acquire()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert 0.9 < elapsed < 2.0, f"expected ~1s delay, got {elapsed:.2f}s"


def test_per_second_within_budget():
    """3 requests with per_second=3 → no delay."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=3, per_minute=600)
        t0 = time.monotonic()
        for _ in range(3):
            await lim.acquire()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert elapsed < 0.5, f"expected no delay, got {elapsed:.2f}s"


def test_acquire_records_both_windows():
    """Each acquire() stamps both sec and min windows."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=10, per_minute=100)
        await lim.acquire()
        assert len(lim._sec_window) == 1
        assert len(lim._min_window) == 1
        await lim.acquire()
        assert len(lim._sec_window) == 2
        assert len(lim._min_window) == 2
    _run(_inner())


def test_minute_budget_enforced_at_third():
    """With per_minute=3, acquiring 3 fills the bucket; 4th is tracked."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=100, per_minute=3)
        for _ in range(3):
            await lim.acquire()
        assert len(lim._min_window) == 3
    _run(_inner())


def test_window_prune_after_interval():
    """Entries older than 1s are pruned from sec_window on next acquire."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=2, per_minute=600)
        await lim.acquire()
        await lim.acquire()
        assert len(lim._sec_window) == 2
        await asyncio.sleep(1.05)
        t0 = time.monotonic()
        await lim.acquire()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert elapsed < 0.3, f"should not block after window expired, got {elapsed:.2f}s"


def test_backward_compat_delay_mode():
    """Legacy delay= path in RateLimitedHttpClient._gate() still works."""
    async def _inner():
        client = RateLimitedHttpClient(delay=0.1)
        assert client._limiter is None
        t0 = time.monotonic()
        await client._gate()
        await client._gate()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert elapsed >= 0.08, f"delay= path should throttle, got {elapsed:.2f}s"


def test_limiter_mode_ignores_delay():
    """When limiter= is set, delay= path is bypassed."""
    async def _inner():
        lim = DualWindowRateLimiter(per_second=100, per_minute=600)
        client = RateLimitedHttpClient(delay=999.0, limiter=lim)
        t0 = time.monotonic()
        await client._gate()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert elapsed < 0.5, f"limiter mode should not sleep 999s, got {elapsed:.2f}s"


def test_throttle_alias():
    """_throttle() is an alias for _gate() — backward compat."""
    async def _inner():
        client = RateLimitedHttpClient(delay=0.05)
        t0 = time.monotonic()
        await client._throttle()
        await client._throttle()
        return time.monotonic() - t0
    elapsed = _run(_inner())
    assert elapsed >= 0.04

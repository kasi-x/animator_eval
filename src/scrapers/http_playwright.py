"""Playwright-based async HTTP fetcher for CF-protected sites.

Usage:
    async with PlaywrightFetcher() as f:
        html = await f.fetch("https://example.com/page")
        # caller controls rate: await asyncio.sleep(delay)
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, Page, Playwright

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
_CF_TITLES = frozenset(["just a moment", "attention required"])
_CF_WAIT_S = 30.0


def _is_cf_challenge(title: str) -> bool:
    return any(t in title.lower() for t in _CF_TITLES)


class PlaywrightFetcher:
    def __init__(
        self,
        *,
        headless: bool = True,
        profile_dir: Path | None = None,
        cdp_url: str | None = None,
    ) -> None:
        self._headless = False if os.getenv("HEADFUL") == "1" else headless
        self._profile_dir = profile_dir or Path("data/playwright_profile")
        self._cdp_url = cdp_url or os.getenv("CHROME_CDP_URL")
        self._pw: Playwright | None = None
        self._context: BrowserContext | None = None
        self._browser = None  # only used for CDP mode

    async def __aenter__(self) -> "PlaywrightFetcher":
        from playwright.async_api import async_playwright

        self._pw = await async_playwright().start()
        if self._cdp_url:
            # Connect to existing Chrome via CDP (inherits cookies/CF clearance)
            self._browser = await self._pw.chromium.connect_over_cdp(self._cdp_url)
            self._context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
        else:
            self._profile_dir.mkdir(parents=True, exist_ok=True)
            self._context = await self._pw.chromium.launch_persistent_context(
                user_data_dir=str(self._profile_dir),
                headless=self._headless,
                user_agent=_UA,
            )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._context and not self._cdp_url:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()

    async def fetch(
        self,
        url: str,
        *,
        wait_selector: str | None = None,
        wait_until: str = "domcontentloaded",
        timeout_ms: int = 30_000,
    ) -> str:
        from playwright_stealth import Stealth

        assert self._context is not None, "Use as async context manager"
        page: Page = await self._context.new_page()
        try:
            await Stealth().apply_stealth_async(page)
            await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            await _wait_cf_challenge(page)
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=timeout_ms)
            html = await page.content()
            if _is_cf_challenge(await page.title()):
                raise RuntimeError(f"Cloudflare challenge not resolved: {url}")
            return html
        finally:
            await page.close()


async def _wait_cf_challenge(page: Page) -> None:
    title = (await page.title()).lower()
    if not any(t in title for t in _CF_TITLES):
        return
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _CF_WAIT_S
    while loop.time() < deadline:
        await asyncio.sleep(1.0)
        title = (await page.title()).lower()
        if not any(t in title for t in _CF_TITLES):
            return

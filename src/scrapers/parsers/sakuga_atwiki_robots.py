"""robots.txt disallow checker for www18.atwiki.jp."""
from __future__ import annotations

from fnmatch import fnmatch

import httpx
import structlog

log = structlog.get_logger()

_ROBOTS_URL = "https://www18.atwiki.jp/robots.txt"


async def fetch_disallow_patterns() -> list[str]:
    """Fetch Disallow paths once; caller caches the result."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(_ROBOTS_URL)
        resp.raise_for_status()
    patterns: list[str] = []
    for line in resp.text.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("disallow:"):
            path = stripped[len("disallow:"):].strip()
            if path:
                patterns.append(path)
    log.info("robots_fetched", disallow_count=len(patterns))
    return patterns


def is_allowed(url_path: str, disallow_patterns: list[str]) -> bool:
    for pat in disallow_patterns:
        if "*" in pat or "?" in pat:
            if fnmatch(url_path, pat):
                return False
        elif url_path.startswith(pat):
            return False
    return True

"""Local JSON cache for scraper network responses.

Default cache directory: data/scraper_cache
Disable with: SCRAPER_CACHE_DISABLE=1
Override dir with: SCRAPER_CACHE_DIR=/path/to/cache
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


def _cache_root() -> Path | None:
    # Tests expect real client control-flow (retries/errors), so disable cache.
    if os.getenv("PYTEST_CURRENT_TEST"):
        return None
    if os.getenv("SCRAPER_CACHE_DISABLE") == "1":
        return None
    return Path(os.getenv("SCRAPER_CACHE_DIR", "data/scraper_cache"))


def _cache_file(namespace: str, key_payload: dict[str, Any]) -> Path | None:
    root = _cache_root()
    if root is None:
        return None
    raw = json.dumps(key_payload, sort_keys=True, ensure_ascii=False, default=str)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return root / namespace / f"{digest}.json"


def load_cached_json(namespace: str, key_payload: dict[str, Any]) -> Any | None:
    path = _cache_file(namespace, key_payload)
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save_cached_json(namespace: str, key_payload: dict[str, Any], payload: Any) -> None:
    path = _cache_file(namespace, key_payload)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
    except OSError:
        return

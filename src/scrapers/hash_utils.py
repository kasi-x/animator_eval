"""Content hash utilities for Parquet-based BRONZE diff detection.

Computes stable SHA256 hashes for scraper data (anime, persons, etc.)
for change detection when integrating BRONZE parquet → SILVER DuckDB.
"""

import hashlib
import json
from typing import Any


def hash_anime_data(anime_dict: dict[str, Any]) -> str:
    """Compute SHA256 hash of anime data for diff detection.

    Stable hash excluding fetched_at/content_hash fields.
    """
    sanitized = {
        k: v
        for k, v in anime_dict.items()
        if k not in ("fetched_at", "content_hash", "scraped_at")
    }
    json_str = json.dumps(sanitized, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()

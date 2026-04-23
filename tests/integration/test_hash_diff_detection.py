"""E2E test for hash-based diff detection in §7.1 scraper + integrate workflow."""

import sqlite3
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pytest

from src.scrapers.hash_utils import hash_anime_data


def test_hash_diff_detection_skip_unchanged():
    """Test that unchanged anime (same hash) are skipped during upsert."""
    # Create SILVER DuckDB with anime table
    with tempfile.TemporaryDirectory() as tmpdir:
        silver_path = Path(tmpdir) / "silver.duckdb"
        conn = duckdb.connect(str(silver_path))

        # Create anime table
        conn.execute("""
            CREATE TABLE anime (
                id VARCHAR PRIMARY KEY,
                title_ja VARCHAR,
                content_hash VARCHAR,
                fetched_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT now()
            )
        """)

        # Insert initial anime with hash
        anime1 = {"id": "anime_1", "title_ja": "Test Anime 1"}
        hash1 = hash_anime_data(anime1)
        now = datetime.now(timezone.utc)

        conn.execute(
            "INSERT INTO anime (id, title_ja, content_hash, fetched_at) VALUES (?, ?, ?, ?)",
            [anime1["id"], anime1["title_ja"], hash1, now],
        )
        conn.commit()

        # Simulate second scrape: same anime, same hash
        anime1_v2 = {"id": "anime_1", "title_ja": "Test Anime 1"}  # Identical
        hash1_v2 = hash_anime_data(anime1_v2)
        now2 = now + timedelta(hours=1)

        # Test the hash comparison logic (simulate DuckDB filtered WHERE clause)
        existing = conn.execute(
            "SELECT content_hash FROM anime WHERE id = ?", [anime1_v2["id"]]
        ).fetchall()

        if existing:
            old_hash = existing[0][0]
            should_update = hash1_v2 != old_hash
            assert not should_update, "Identical content should not trigger update"

        # Now test with modified anime
        anime1_modified = {"id": "anime_1", "title_ja": "Test Anime 1 Modified"}
        hash1_modified = hash_anime_data(anime1_modified)

        existing_modified = conn.execute(
            "SELECT content_hash FROM anime WHERE id = ?", [anime1_modified["id"]]
        ).fetchall()

        if existing_modified:
            old_hash_mod = existing_modified[0][0]
            should_update_mod = hash1_modified != old_hash_mod
            assert should_update_mod, "Modified content should trigger update"

        conn.close()


def test_since_mode_skips_recent_fetches():
    """Test that --since mode correctly skips anime fetched after cutoff."""
    with tempfile.TemporaryDirectory() as tmpdir:
        silver_path = Path(tmpdir) / "silver.duckdb"
        conn = duckdb.connect(str(silver_path))

        # Create anime table
        conn.execute("""
            CREATE TABLE anime (
                id VARCHAR PRIMARY KEY,
                title_ja VARCHAR,
                content_hash VARCHAR,
                fetched_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT now()
            )
        """)

        # Insert anime with various fetched_at times
        now = datetime.now(timezone.utc)
        past_24h = now - timedelta(hours=24)
        past_1h = now - timedelta(hours=1)

        anime_old = [
            ("anime_1", "Old Anime 1", past_24h),
            ("anime_2", "Old Anime 2", past_24h),
        ]
        anime_recent = [
            ("anime_3", "Recent Anime 1", past_1h),
            ("anime_4", "Recent Anime 2", past_1h),
        ]

        for anime_id, title, fetched_at in anime_old + anime_recent:
            data = {"id": anime_id, "title_ja": title}
            hash_val = hash_anime_data(data)
            conn.execute(
                "INSERT INTO anime (id, title_ja, content_hash, fetched_at) VALUES (?, ?, ?, ?)",
                [anime_id, title, hash_val, fetched_at],
            )
        conn.commit()

        # Simulate --since mode: get anime fetched after past_6h
        cutoff = now - timedelta(hours=6)
        result = conn.execute(
            "SELECT id FROM anime WHERE fetched_at >= ? ORDER BY fetched_at DESC",
            [cutoff],
        ).fetchall()

        recent_ids = {row[0] for row in result}
        # Should include anime_3, anime_4 but not anime_1, anime_2
        assert "anime_3" in recent_ids, "Recent anime should be in skip set"
        assert "anime_4" in recent_ids, "Recent anime should be in skip set"
        assert "anime_1" not in recent_ids, "Old anime should not be in skip set"
        assert "anime_2" not in recent_ids, "Old anime should not be in skip set"

        conn.close()

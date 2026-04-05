#!/usr/bin/env python3
"""One-time migration: backfill episode data from raw_role strings.

Reads credits with episode annotations in raw_role (e.g., "Key Animation (ep 10)"),
parses episode numbers, and either updates the episode column (single episode) or
inserts additional rows (multi-episode credits, one per episode).

Usage:
    python scripts/backfill_episodes.py [--db-path PATH] [--dry-run]
"""

import argparse
import sys
from pathlib import Path

import structlog

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import get_connection, init_db
from src.utils.episode_parser import parse_episodes

logger = structlog.get_logger()


def backfill_episodes(db_path: Path | None = None, dry_run: bool = False) -> dict[str, int]:
    """Parse episode info from raw_role and update/insert credit rows.

    Args:
        db_path: Path to SQLite database (None for default)
        dry_run: If True, show what would be done without modifying DB

    Returns:
        Stats dict with counts of updated/inserted/skipped rows
    """
    conn = get_connection(db_path)
    init_db(conn)

    # Find credits with episode annotations in raw_role
    rows = conn.execute(
        "SELECT id, person_id, anime_id, role, raw_role, episode, source "
        "FROM credits WHERE raw_role LIKE '%%(ep%%'"
    ).fetchall()

    stats = {"total_candidates": len(rows), "updated": 0, "inserted": 0, "skipped": 0}
    logger.info("backfill_start", candidates=len(rows), dry_run=dry_run)

    for row in rows:
        credit_id = row["id"]
        raw_role = row["raw_role"]
        current_episode = row["episode"]

        episodes = parse_episodes(raw_role)
        if not episodes:
            stats["skipped"] += 1
            continue

        # Already has episode data set (not default -1)
        if current_episode is not None and current_episode != -1:
            stats["skipped"] += 1
            continue

        episodes_sorted = sorted(episodes)

        if len(episodes_sorted) == 1:
            # Single episode: update existing row
            if not dry_run:
                conn.execute(
                    "UPDATE credits SET episode = ? WHERE id = ?",
                    (episodes_sorted[0], credit_id),
                )
            stats["updated"] += 1
        else:
            # Multi-episode: update first row, insert additional rows
            if not dry_run:
                # Update existing row with first episode
                conn.execute(
                    "UPDATE credits SET episode = ? WHERE id = ?",
                    (episodes_sorted[0], credit_id),
                )
                # Insert additional rows for remaining episodes
                for ep in episodes_sorted[1:]:
                    conn.execute(
                        "INSERT OR IGNORE INTO credits "
                        "(person_id, anime_id, role, raw_role, episode, source) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            row["person_id"],
                            row["anime_id"],
                            row["role"],
                            raw_role,
                            ep,
                            row["source"],
                        ),
                    )
            stats["updated"] += 1
            stats["inserted"] += len(episodes_sorted) - 1

    if not dry_run:
        conn.commit()

    conn.close()

    logger.info(
        "backfill_complete",
        **stats,
        dry_run=dry_run,
    )
    return stats


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Backfill episode data from raw_role strings")
    parser.add_argument("--db-path", type=Path, default=None, help="Path to SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without modifying DB")
    args = parser.parse_args()

    stats = backfill_episodes(db_path=args.db_path, dry_run=args.dry_run)

    print(f"\nBackfill {'(DRY RUN) ' if args.dry_run else ''}complete:")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()

"""MADB データ統合スクリプト.

MADB (Media Arts Database) のクレジットデータを AniList データと統合し、
効果を測定する統計レポートを出力する。

Usage:
    pixi run python scripts/integrate_madb.py [--dry-run] [--max-anime N]
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import structlog
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.anime_title_matching import AnimeMatch, match_anime_titles
from src.database import get_connection, init_db
from src.scrapers.mediaarts_scraper import scrape_madb

log = structlog.get_logger()
console = Console()


@dataclass
class DBSnapshot:
    """DB 統計のスナップショット."""

    total_anime: int = 0
    total_persons: int = 0
    total_credits: int = 0
    anilist_anime: int = 0
    madb_anime: int = 0
    anilist_persons: int = 0
    madb_persons: int = 0
    anilist_credits: int = 0
    madb_credits: int = 0
    anime_without_credits: int = 0
    year_distribution: dict[str, int] = field(default_factory=dict)
    role_distribution: dict[str, int] = field(default_factory=dict)


def take_snapshot(conn) -> DBSnapshot:
    """DB の現在の統計をキャプチャする."""
    snap = DBSnapshot()

    snap.total_anime = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    snap.total_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    snap.total_credits = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]

    snap.anilist_anime = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'anilist:%'"
    ).fetchone()[0]
    snap.madb_anime = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'madb:%'"
    ).fetchone()[0]

    snap.anilist_persons = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'anilist:%'"
    ).fetchone()[0]
    snap.madb_persons = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'madb:%'"
    ).fetchone()[0]

    snap.anilist_credits = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE source != 'mediaarts'"
    ).fetchone()[0]
    snap.madb_credits = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE source = 'mediaarts'"
    ).fetchone()[0]

    # Anime without any credits (N/A placeholders)
    snap.anime_without_credits = conn.execute(
        """SELECT COUNT(*) FROM anime a
           WHERE a.id LIKE 'anilist:%'
             AND NOT EXISTS (SELECT 1 FROM credits c WHERE c.anime_id = a.id)"""
    ).fetchone()[0]

    # Year distribution (MADB anime by decade)
    rows = conn.execute(
        """SELECT (year / 10) * 10 AS decade, COUNT(*) AS cnt
           FROM anime WHERE id LIKE 'madb:%' AND year IS NOT NULL
           GROUP BY decade ORDER BY decade"""
    ).fetchall()
    snap.year_distribution = {f"{r[0]}s": r[1] for r in rows}

    # Role distribution (MADB credits)
    rows = conn.execute(
        """SELECT role, COUNT(*) AS cnt FROM credits
           WHERE source = 'mediaarts'
           GROUP BY role ORDER BY cnt DESC LIMIT 15"""
    ).fetchall()
    snap.role_distribution = {r[0]: r[1] for r in rows}

    return snap


def load_madb_anime(conn) -> list[dict]:
    """DB から MADB anime をロードする."""
    rows = conn.execute(
        "SELECT id, title_ja, title_en, year FROM anime WHERE id LIKE 'madb:%'"
    ).fetchall()
    return [
        {
            "id": r["id"],
            "title": r["title_ja"] or r["title_en"] or "",
            "year": r["year"],
        }
        for r in rows
    ]


def load_anilist_anime(conn) -> list[dict]:
    """DB から AniList anime をロードする."""
    rows = conn.execute(
        "SELECT id, title_ja, title_en, year, synonyms FROM anime WHERE id LIKE 'anilist:%'"
    ).fetchall()
    import json

    result = []
    for r in rows:
        synonyms_raw = r["synonyms"] or "[]"
        try:
            synonyms = json.loads(synonyms_raw)
        except (json.JSONDecodeError, TypeError):
            synonyms = []
        result.append(
            {
                "id": r["id"],
                "title_ja": r["title_ja"] or "",
                "title_en": r["title_en"] or "",
                "year": r["year"],
                "synonyms": synonyms if isinstance(synonyms, list) else [],
            }
        )
    return result


def reassign_credits(
    conn,
    matches: list[AnimeMatch],
    dry_run: bool = False,
) -> dict[str, int]:
    """マッチした MADB credits の anime_id を AniList anime ID に UPDATE する.

    Returns:
        統計 dict (reassigned, skipped_duplicate, by_role)
    """
    stats: dict[str, int] = {
        "reassigned": 0,
        "skipped_duplicate": 0,
    }
    role_counts: Counter[str] = Counter()

    for match in matches:
        if dry_run:
            # Count how many credits would be reassigned
            rows = conn.execute(
                "SELECT role FROM credits WHERE anime_id = ?",
                (match.madb_anime_id,),
            ).fetchall()
            stats["reassigned"] += len(rows)
            for r in rows:
                role_counts[r[0]] += 1
            continue

        # Get credits for this MADB anime
        credits = conn.execute(
            """SELECT id, person_id, role, raw_role, episode, source
               FROM credits WHERE anime_id = ?""",
            (match.madb_anime_id,),
        ).fetchall()

        for credit in credits:
            # Try to update anime_id; UNIQUE constraint may conflict
            try:
                conn.execute(
                    """UPDATE credits SET anime_id = ?
                       WHERE id = ?
                       AND NOT EXISTS (
                           SELECT 1 FROM credits c2
                           WHERE c2.person_id = ?
                             AND c2.anime_id = ?
                             AND c2.raw_role = ?
                             AND c2.episode = ?
                             AND c2.id != ?
                       )""",
                    (
                        match.anilist_anime_id,
                        credit["id"],
                        credit["person_id"],
                        match.anilist_anime_id,
                        credit["raw_role"],
                        credit["episode"],
                        credit["id"],
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    stats["reassigned"] += 1
                    role_counts[credit["role"]] += 1
                else:
                    stats["skipped_duplicate"] += 1
            except Exception:
                stats["skipped_duplicate"] += 1

        # Store MADB provenance on the AniList anime record
        conn.execute(
            "UPDATE anime SET madb_id = ? WHERE id = ?",
            (match.madb_anime_id.removeprefix("madb:"), match.anilist_anime_id),
        )

    stats["by_role"] = dict(role_counts.most_common())
    return stats


def count_na_resolved(conn, before_na: int) -> int:
    """N/A (クレジットなし) anime のうち、統合でクレジットが入った数."""
    after_na = conn.execute(
        """SELECT COUNT(*) FROM anime a
           WHERE a.id LIKE 'anilist:%'
             AND NOT EXISTS (SELECT 1 FROM credits c WHERE c.anime_id = a.id)"""
    ).fetchone()[0]
    return before_na - after_na


def print_report(
    before: DBSnapshot,
    after: DBSnapshot,
    scrape_stats: dict,
    matches: list[AnimeMatch],
    reassign_stats: dict[str, int],
    na_resolved: int,
    dry_run: bool,
) -> None:
    """Rich テーブルで統計レポートを出力する."""
    mode = "[bold yellow]DRY RUN[/]" if dry_run else "[bold green]LIVE[/]"
    console.rule(f"[bold]MADB Integration Report[/] {mode}")

    # --- Scrape Results ---
    t = Table(title="Scrape Results", show_header=True)
    t.add_column("Metric", style="cyan")
    t.add_column("Value", justify="right", style="green")
    t.add_row("Anime fetched", f"{scrape_stats.get('anime_fetched', 0):,}")
    t.add_row(
        "Anime with contributors",
        f"{scrape_stats.get('anime_with_contributors', 0):,}",
    )
    t.add_row("Credits created", f"{scrape_stats.get('credits_created', 0):,}")
    t.add_row("Persons created", f"{scrape_stats.get('persons_created', 0):,}")
    console.print(t)
    console.print()

    # --- Title Matching ---
    exact = sum(1 for m in matches if m.strategy == "exact")
    fuzzy = sum(1 for m in matches if m.strategy == "fuzzy")
    madb_total = scrape_stats.get("anime_with_contributors", 0)
    unmatched = madb_total - len(matches)

    t = Table(title="Title Matching", show_header=True)
    t.add_column("Metric", style="cyan")
    t.add_column("Value", justify="right", style="green")
    t.add_row("Exact matches", f"{exact:,}")
    t.add_row("Fuzzy matches", f"{fuzzy:,}")
    t.add_row("Unmatched", f"{unmatched:,}")
    t.add_row(
        "Match rate",
        f"{100 * len(matches) / max(1, madb_total):.1f}%",
    )
    console.print(t)
    console.print()

    # --- Credit Reassignment ---
    t = Table(title="Credit Reassignment", show_header=True)
    t.add_column("Metric", style="cyan")
    t.add_column("Value", justify="right", style="green")
    t.add_row("Credits reassigned", f"{reassign_stats.get('reassigned', 0):,}")
    t.add_row("Skipped (duplicate)", f"{reassign_stats.get('skipped_duplicate', 0):,}")
    console.print(t)
    console.print()

    # --- Role distribution ---
    by_role = reassign_stats.get("by_role", {})
    if by_role:
        t = Table(title="Reassigned Credits by Role", show_header=True)
        t.add_column("Role", style="cyan")
        t.add_column("Count", justify="right", style="green")
        for role, cnt in sorted(by_role.items(), key=lambda x: -x[1])[:10]:
            t.add_row(role, f"{cnt:,}")
        console.print(t)
        console.print()

    # --- N/A Resolution ---
    t = Table(title="N/A Resolution (AniList anime without credits)", show_header=True)
    t.add_column("Metric", style="cyan")
    t.add_column("Value", justify="right", style="green")
    t.add_row("Before", f"{before.anime_without_credits:,}")
    if not dry_run:
        t.add_row("Resolved", f"{na_resolved:,}")
        t.add_row("After", f"{after.anime_without_credits:,}")
    else:
        t.add_row("Estimated resolved", f"{na_resolved:,} (dry-run)")
    console.print(t)
    console.print()

    # --- Before / After ---
    t = Table(title="Before / After Comparison", show_header=True)
    t.add_column("Metric", style="cyan")
    t.add_column("Before", justify="right", style="yellow")
    t.add_column("After", justify="right", style="green")
    t.add_column("Delta", justify="right", style="magenta")

    def _row(label: str, b: int, a: int) -> None:
        delta = a - b
        sign = "+" if delta > 0 else ""
        t.add_row(label, f"{b:,}", f"{a:,}", f"{sign}{delta:,}")

    _row("Total anime", before.total_anime, after.total_anime)
    _row("Total persons", before.total_persons, after.total_persons)
    _row("Total credits", before.total_credits, after.total_credits)
    _row("AniList anime", before.anilist_anime, after.anilist_anime)
    _row("MADB anime", before.madb_anime, after.madb_anime)
    _row("AniList credits", before.anilist_credits, after.anilist_credits)
    _row("MADB credits", before.madb_credits, after.madb_credits)
    console.print(t)
    console.print()

    # --- Year distribution ---
    if after.year_distribution:
        t = Table(title="MADB Anime by Decade", show_header=True)
        t.add_column("Decade", style="cyan")
        t.add_column("Count", justify="right", style="green")
        for decade, cnt in sorted(after.year_distribution.items()):
            t.add_row(decade, f"{cnt:,}")
        console.print(t)
        console.print()

    # --- Top matches (sample) ---
    if matches:
        t = Table(title="Sample Matches (first 10)", show_header=True)
        t.add_column("MADB Title", style="cyan", max_width=30)
        t.add_column("AniList Title", style="green", max_width=30)
        t.add_column("Score", justify="right")
        t.add_column("Strategy")
        for m in matches[:10]:
            t.add_row(m.madb_title, m.anilist_title, f"{m.score:.0f}", m.strategy)
        console.print(t)

    console.rule("[bold]Done[/]")


def main() -> None:
    """統合スクリプトのエントリポイント."""
    parser = argparse.ArgumentParser(description="MADB data integration")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze without modifying the database",
    )
    parser.add_argument(
        "--max-anime",
        type=int,
        default=0,
        help="Maximum MADB anime to process (0 = all)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=90,
        help="Fuzzy match threshold (0-100)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/madb"),
        help="MADB download directory",
    )
    args = parser.parse_args()

    # Setup structlog
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # In dry-run mode, work on a copy of the DB to avoid lock contention
    # with other running processes (e.g. backfill scrapers)
    from src.database import DEFAULT_DB_PATH

    db_path = DEFAULT_DB_PATH
    tmp_dir = None

    if args.dry_run and db_path.exists():
        tmp_dir = tempfile.mkdtemp(prefix="madb_dryrun_")
        tmp_db = Path(tmp_dir) / "animetor_eval.db"
        console.print(f"[dim]Dry-run: copying DB to {tmp_db}...[/]")
        shutil.copy2(db_path, tmp_db)
        # Also copy WAL/SHM if present (for consistent state)
        for suffix in ("-wal", "-shm"):
            src = db_path.parent / (db_path.name + suffix)
            if src.exists():
                shutil.copy2(src, Path(tmp_dir) / (tmp_db.name + suffix))
        conn = get_connection(tmp_db)
    else:
        conn = get_connection()

    conn.execute("PRAGMA busy_timeout = 30000")  # Wait up to 30s for locks
    try:
        init_db(conn)

        # Step 1: Before snapshot
        console.print("[bold]Step 1:[/] Taking before snapshot...")
        before = take_snapshot(conn)
        log.info(
            "before_snapshot",
            anime=before.total_anime,
            persons=before.total_persons,
            credits=before.total_credits,
        )

        # Step 2: Scrape MADB
        console.print("[bold]Step 2:[/] Scraping MADB data...")
        scrape_stats = asyncio.run(
            scrape_madb(
                conn,
                data_dir=args.data_dir,
                max_anime=args.max_anime or 999_999,
            )
        )
        log.info("scrape_complete", **scrape_stats)

        # Step 3: Title matching
        console.print("[bold]Step 3:[/] Matching anime titles...")
        madb_anime_list = load_madb_anime(conn)
        anilist_anime_list = load_anilist_anime(conn)
        matches = match_anime_titles(
            madb_anime_list,
            anilist_anime_list,
            threshold=args.threshold,
        )

        # Step 4: Credit reassignment
        console.print("[bold]Step 4:[/] Reassigning credits...")
        reassign_stats = reassign_credits(conn, matches, dry_run=args.dry_run)
        if not args.dry_run:
            conn.commit()
        log.info(
            "reassignment_complete",
            **{k: v for k, v in reassign_stats.items() if k != "by_role"},
        )

        # Step 5: After snapshot
        console.print("[bold]Step 5:[/] Taking after snapshot...")
        after = take_snapshot(conn)

        # N/A resolution count
        na_resolved = count_na_resolved(conn, before.anime_without_credits)

        # Step 6: Report
        print_report(
            before=before,
            after=after,
            scrape_stats=scrape_stats,
            matches=matches,
            reassign_stats=reassign_stats,
            na_resolved=na_resolved,
            dry_run=args.dry_run,
        )

    finally:
        conn.close()
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()

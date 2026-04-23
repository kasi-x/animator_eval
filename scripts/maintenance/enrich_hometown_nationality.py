"""LLM-assisted hometown → nationality enrichment.

Finds persons with a hometown but unknown/empty nationality, infers the
nationality via local Ollama, persists results to hometown_tokens.json cache,
and (unless --dry-run) writes the nationality back to the DB.

Usage:
  pixi run python scripts/maintenance/enrich_hometown_nationality.py
  pixi run python scripts/maintenance/enrich_hometown_nationality.py --limit 50
  pixi run python scripts/maintenance/enrich_hometown_nationality.py --dry-run
  pixi run python scripts/maintenance/enrich_hometown_nationality.py --show-cached
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Literal

# Project root on path
sys.path.insert(0, str(Path(__file__).parents[2]))

from src.database import get_connection, init_db
from src.utils.name_utils import (
    detect_name_script,
    infer_nationalities,
    _HOMETOWN_CACHE,
    _llm_infer_nationality,
    _save_hometown_cache,
)

_BATCH_DELAY_S = 0.5  # polite pause between LLM calls


def _query_candidates(conn, limit: int) -> list[dict]:
    """Persons with a hometown but no inferred nationality."""
    rows = conn.execute(
        """
        SELECT id, name_en, name_ja, hometown, nationality
        FROM persons
        WHERE hometown IS NOT NULL
          AND hometown != ''
          AND (nationality IS NULL OR nationality = '[]' OR nationality = '')
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    cols = ["id", "name_en", "name_ja", "hometown", "nationality"]
    return [dict(zip(cols, r)) for r in rows]


def _update_nationality(conn, person_id: str, code: str) -> None:
    conn.execute(
        "UPDATE persons SET nationality = ? WHERE id = ?",
        (json.dumps([code], ensure_ascii=False), person_id),
    )


def _check_llm() -> bool:
    try:
        import httpx
        from src.utils.config import LLM_BASE_URL, LLM_TIMEOUT
        base = LLM_BASE_URL.replace("/v1", "")
        return httpx.get(f"{base}/api/tags", timeout=LLM_TIMEOUT).status_code == 200
    except Exception:
        return False


def _print_cache() -> None:
    """Print all cached LLM decisions sorted by hometown and exit."""
    print(f"LLM cache entries ({len(_HOMETOWN_CACHE)}):")
    for ht, code in sorted(_HOMETOWN_CACHE.items()):
        print(f"  {code or 'NULL':6s}  {ht}")


def _ensure_llm_reachable_or_exit() -> None:
    """Exit with error if Ollama is not reachable."""
    if not _check_llm():
        print("ERROR: Ollama not reachable. Start it with: ollama serve", file=sys.stderr)
        sys.exit(1)


def _classify_candidate(row: dict) -> Literal["script_covered", "cache_hit", "llm_needed"]:
    """Decide how to process this candidate without performing any action.

    script_covered: ja/ko/th script — already handled upstream by script-based inference.
    cache_hit: hometown is already in _HOMETOWN_CACHE.
    llm_needed: requires an LLM call.
    """
    # Only name_ja is checked: it gives the native script hint.
    # name_ko / name_zh go through different routing paths upstream.
    native = row["name_ja"] or ""
    script = detect_name_script(native) if native else "en"
    if script not in ("zh_or_ja", "en"):
        return "script_covered"
    cache_key = row["hometown"].strip()
    if cache_key in _HOMETOWN_CACHE:
        return "cache_hit"
    return "llm_needed"


def _process_row(
    conn, row: dict, *, dry_run: bool, script_only: bool
) -> tuple[str, str | None]:
    """Process a single candidate row.

    Returns (bucket, country_code) where bucket is one of:
      "skipped"  — script_covered or script_only with no cache hit
      "cached"   — resolved from _HOMETOWN_CACHE without an LLM call
      "updated"  — resolved (via LLM or cache) and written to DB
      "would_update" — resolved but dry_run prevented the DB write
    """
    classification = _classify_candidate(row)

    if classification == "script_covered":
        return "skipped", None

    hometown = row["hometown"]
    cache_key = hometown.strip()

    if classification == "cache_hit":
        code = _HOMETOWN_CACHE[cache_key]
        tag = f"[cache] → {code or 'NULL'}"
    else:  # llm_needed
        if script_only:
            return "skipped", None
        code = _llm_infer_nationality(hometown)
        _save_hometown_cache(cache_key, code)
        tag = f"[llm]   → {code or 'NULL'}"
        time.sleep(_BATCH_DELAY_S)

    native = row["name_ja"] or ""
    name_display = native or row["name_en"] or row["id"]
    print(f"  {name_display:30s}  {hometown:40s}  {tag}")

    if not code:
        return "cached" if classification == "cache_hit" else "skipped", None

    if dry_run:
        return "would_update", code

    _update_nationality(conn, row["id"], code)
    return "updated", code


def _print_summary(counts: dict, dry_run: bool) -> None:
    verb = "Would update" if dry_run else "Updated"
    update_key = "would_update" if dry_run else "updated"
    print(
        f"\n{verb}: {counts.get(update_key, 0)}"
        f"  |  cached: {counts.get('cached', 0)}"
        f"  |  skipped: {counts.get('skipped', 0)}"
    )
    if dry_run:
        print("(dry-run: no DB writes performed)")


def run(limit: int, dry_run: bool, show_cached: bool, script_only: bool) -> None:
    if show_cached:
        _print_cache()
        return

    conn = get_connection()
    init_db(conn)
    candidates = _query_candidates(conn, limit)
    print(f"Candidates: {len(candidates)} persons with hometown but no nationality")

    if not candidates:
        conn.close()
        return

    if not script_only:
        _ensure_llm_reachable_or_exit()

    counts: dict[str, int] = {}
    for row in candidates:
        bucket, _ = _process_row(conn, row, dry_run=dry_run, script_only=script_only)
        counts[bucket] = counts.get(bucket, 0) + 1
        # Batch commit every 50 real writes to reduce I/O overhead
        if not dry_run and counts.get("updated", 0) % 50 == 0 and counts.get("updated", 0) > 0:
            conn.commit()

    if not dry_run:
        conn.commit()
    conn.close()
    _print_summary(counts, dry_run)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=200,
                   help="Max persons to process (default 200)")
    p.add_argument("--dry-run", action="store_true",
                   help="Infer and cache but do not write nationality to DB")
    p.add_argument("--show-cached", action="store_true",
                   help="Print all cached LLM decisions and exit")
    p.add_argument("--script-only", action="store_true",
                   help="Only apply cached LLM results; do not call LLM for new hometowns")
    args = p.parse_args()
    run(
        limit=args.limit,
        dry_run=args.dry_run,
        show_cached=args.show_cached,
        script_only=args.script_only,
    )


if __name__ == "__main__":
    main()

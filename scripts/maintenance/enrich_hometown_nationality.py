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


def _native_name(row: dict) -> str:
    return row["name_ja"] or ""


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


def run(limit: int, dry_run: bool, show_cached: bool, script_only: bool) -> None:
    if show_cached:
        print(f"LLM cache entries ({len(_HOMETOWN_CACHE)}):")
        for ht, code in sorted(_HOMETOWN_CACHE.items()):
            print(f"  {code or 'NULL':6s}  {ht}")
        return

    conn = get_connection()
    init_db(conn)
    candidates = _query_candidates(conn, limit)
    print(f"Candidates: {len(candidates)} persons with hometown but no nationality")

    if not candidates:
        conn.close()
        return

    if not script_only and not _check_llm():
        print("ERROR: Ollama not reachable. Start it with: ollama serve", file=sys.stderr)
        conn.close()
        sys.exit(1)

    updated = skipped = cached = failed = 0

    for row in candidates:
        native = _native_name(row)
        hometown = row["hometown"]

        script = detect_name_script(native) if native else "en"
        if script not in ("zh_or_ja", "en"):
            # Script-based inference already covers ja/ko/th; skip
            skipped += 1
            continue

        # Check cache first
        cache_key = hometown.strip()
        if cache_key in _HOMETOWN_CACHE:
            code = _HOMETOWN_CACHE[cache_key]
            cached += 1
            tag = f"[cache] → {code or 'NULL'}"
        else:
            if script_only:
                skipped += 1
                continue
            code = _llm_infer_nationality(hometown)
            _save_hometown_cache(cache_key, code)
            tag = f"[llm]   → {code or 'NULL'}"
            time.sleep(_BATCH_DELAY_S)

        name_display = native or row["name_en"] or row["id"]
        print(f"  {name_display:30s}  {hometown:40s}  {tag}")

        if code and not dry_run:
            _update_nationality(conn, row["id"], code)
            conn.commit()
            updated += 1
        elif code:
            updated += 1  # count as "would update"

    conn.close()
    verb = "Would update" if dry_run else "Updated"
    print(
        f"\n{verb}: {updated}  |  cached: {cached}  |  skipped: {skipped}  |  failed: {failed}"
    )
    if dry_run:
        print("(dry-run: no DB writes performed)")


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

"""KeyFrame Staff List — post-scrape translate enrichment.

Separate from the main scraper because:
  • Runs on demand after entity resolution (not part of the daily Phase 0-4 loop).
  • Reads existing Bronze parquet to plan targets, instead of crawling the site.
  • Uses a different endpoint (/api/data/translate.v4.php) than the main scraper.

CLI: `pixi run scrape-keyframe-enrich`
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import structlog
import typer

from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.cli_common import DataDirOpt, DelayOpt, make_scraper_app
from src.scrapers.keyframe_api import (
    DEFAULT_DELAY,
    KeyframeApiClient,
    KeyframeQuotaExceeded,
)
from src.scrapers.parsers import keyframe_api as api_parser

log = structlog.get_logger()

app = make_scraper_app("keyframe_enrich")

CHECKPOINT_INTERVAL = 10  # progress log every N items


def _pending_translate_targets(
    bronze_root: Path,
    ids: list[int] | None,
) -> dict[int, tuple[str | None, str | None]]:
    """Return person_id → (name_ja, name_en) for persons still needing translate.

    If ids is given, restrict to those IDs (re-translate allowed).
    Otherwise, exclude persons already present in person_translate Bronze.
    """
    import pyarrow.parquet as pq

    def _read_parquet_columns(table_name: str, columns: list[str]) -> list[dict]:
        path = bronze_root / "source=keyframe" / f"table={table_name}"
        rows: list[dict] = []
        if not path.exists():
            return rows
        for f in path.rglob("*.parquet"):
            try:
                rows.append(pq.read_table(f, columns=columns).to_pydict())
            except Exception as exc:
                log.warning("keyframe_bronze_read_error", file=str(f), err=str(exc)[:80])
        return rows

    persons: dict[int, tuple[str | None, str | None]] = {}
    for d in _read_parquet_columns("person_profile", ["person_id", "name_ja", "name_en"]):
        for pid, ja, en in zip(d["person_id"], d["name_ja"], d["name_en"]):
            try:
                persons[int(pid)] = (ja or None, en or None)
            except (TypeError, ValueError):
                pass

    if ids is not None:
        return {pid: persons[pid] for pid in ids if pid in persons}

    already_done: set[int] = set()
    for d in _read_parquet_columns("person_translate", ["person_id"]):
        for pid in d["person_id"]:
            try:
                already_done.add(int(pid))
            except (TypeError, ValueError):
                pass

    return {pid: names for pid, names in persons.items() if pid not in already_done}


async def _translate_one(
    client: KeyframeApiClient,
    pid: int,
    name_ja: str | None,
    name_en: str | None,
    bw: BronzeWriter,
    stats: dict,
) -> None:
    """Call translate.v4.php for one person and append all candidates to Bronze."""
    name = name_ja or name_en
    lang = "ja" if name_ja else "en"
    if not name:
        return

    raw_matches = await client.translate_name(name, lang=lang)
    if raw_matches is None:
        return

    matches = api_parser.parse_translate_result(raw_matches)
    match_count = len(matches)

    for m in matches:
        bw.append({
            "person_id": pid,
            "query_lang": lang,
            "query_name": name,
            "match_count": match_count,
            **{
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                for k, v in m.items()
            },
        })

    if match_count == 1:
        stats["translate_matched"] += 1
    elif match_count == 0:
        stats["translate_no_match"] += 1
    else:
        stats["translate_ambiguous"] += 1


async def run_enrich_translate(
    *,
    bronze_root: Path,
    delay: float,
    ids: list[int] | None,
) -> dict:
    """Fetch translate.v4.php for keyframe persons not yet in person_translate Bronze."""
    targets = _pending_translate_targets(bronze_root, ids)
    stats = {
        "total": len(targets),
        "translate_matched": 0,
        "translate_ambiguous": 0,
        "translate_no_match": 0,
    }

    if not targets:
        log.info("keyframe_enrich_translate_nothing_to_do", **stats)
        return stats

    log.info("keyframe_enrich_translate_start", **stats)
    target_list = list(targets.items())

    client = KeyframeApiClient(delay=delay)
    try:
        with BronzeWriter("keyframe", table="person_translate") as bw:
            for i, (pid, (name_ja, name_en)) in enumerate(target_list):
                try:
                    await _translate_one(client, pid, name_ja, name_en, bw, stats)
                except KeyframeQuotaExceeded:
                    log.warning(
                        "keyframe_enrich_quota_exhausted",
                        processed=i,
                        remaining=len(target_list) - i,
                        note="resume tomorrow with `pixi run scrape-keyframe-enrich`",
                        **stats,
                    )
                    break
                if (i + 1) % CHECKPOINT_INTERVAL == 0:
                    log.info(
                        "keyframe_enrich_translate_progress",
                        progress=f"{i + 1}/{len(target_list)}",
                        matched=stats["translate_matched"],
                    )
    finally:
        await client.close()

    log.info("keyframe_enrich_translate_done", **stats)
    return stats


@app.command("run")
def cmd_enrich_translate(
    ids: str = typer.Option(
        "",
        help="Comma-separated keyframe person_ids to enrich (empty = all unmatched)",
    ),
    delay: DelayOpt = DEFAULT_DELAY,
    bronze_root: DataDirOpt = Path("result/bronze"),
) -> None:
    """Enrich keyframe persons with AniList IDs via translate.v4.php.

    Run after entity resolution to fill in anilist_id for persons that
    failed to match other sources by name. Results written to Bronze
    person_translate table; re-run ETL + entity resolution to apply.

    By default enriches all persons not yet in person_translate.
    Pass --ids to restrict to specific person_ids (e.g. after identifying
    unmatched persons from entity resolution output).
    """
    parsed_ids: list[int] | None = None
    if ids.strip():
        try:
            parsed_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
        except ValueError as exc:
            log.error("keyframe_enrich_invalid_ids", err=str(exc))
            raise typer.Exit(1) from exc

    stats = asyncio.run(
        run_enrich_translate(
            bronze_root=bronze_root,
            delay=delay,
            ids=parsed_ids,
        )
    )
    log.info("keyframe_enrich_done", **stats)


if __name__ == "__main__":
    app()

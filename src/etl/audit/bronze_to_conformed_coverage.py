"""BRONZE → Conformed table-level coverage audit tool (Card 14/10).

For every BRONZE table (source × table), determines whether it is consumed
by a conformed loader and reports its integration status.

Complements silver_column_coverage.py (column-level gaps within integrated
tables) and silver_completeness.py (row-level completeness for integrated
tables).  This audit operates one level higher: *which tables* have been
integrated at all.

Usage (CLI):
    python -m src.etl.audit.bronze_to_conformed_coverage \
        --bronze-root result/bronze \
        --out         result/audit/bronze_conformed_coverage.md

Public API:
    discover_bronze_tables(bronze_root) -> list[BronzeTableInfo]
    build_coverage_table(bronze_tables) -> list[CoverageEntry]
    generate_report(entries, out_path)
"""
from __future__ import annotations

import datetime
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb
import structlog

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# INTEGRATION_MAP
#
# Declares whether each (source, bronze_table) is consumed by a conformed
# loader and maps it to the SILVER destination.
#
# status values:
#   INTEGRATED  — loader reads this table and writes to SILVER
#   FOLDED      — data merged into another table (no dedicated SILVER table)
#   UNUSED      — no loader reads this table
#   DISPLAY_ONLY— loader reads but the data is intentionally excluded from
#                 scoring paths (display_* columns, subjective stats)
# ---------------------------------------------------------------------------

_IntegrationEntry = tuple[str, str, str]  # (status, silver_target, notes)

INTEGRATION_MAP: dict[tuple[str, str], _IntegrationEntry] = {
    # ── AniList ─────────────────────────────────────────────────────────────
    ("anilist", "anime"):                  ("INTEGRATED", "anime", "id LIKE 'anilist:%'; display_* extras via anilist loader"),
    ("anilist", "persons"):               ("INTEGRATED", "persons", "id LIKE 'anilist:%'"),
    ("anilist", "credits"):               ("INTEGRATED", "credits", "evidence_source='anilist'"),
    ("anilist", "characters"):            ("INTEGRATED", "characters", "id LIKE 'anilist:%'"),
    ("anilist", "character_voice_actors"): ("INTEGRATED", "character_voice_actors", "source='anilist'"),
    ("anilist", "studios"):               ("INTEGRATED", "studios", "id LIKE 'anilist:%'"),
    ("anilist", "anime_studios"):         ("INTEGRATED", "anime_studios", "source='anilist'; merged from table + array"),
    ("anilist", "relations"):             ("FOLDED", "anime.relations_json", "folded into anime.relations_json then parsed into anime_relations"),

    # ── ANN ──────────────────────────────────────────────────────────────────
    ("ann", "anime"):                     ("INTEGRATED", "anime", "UPDATE only: ANN extra columns (plot_summary, themes, running_time_raw…)"),
    ("ann", "persons"):                   ("INTEGRATED", "persons", "UPDATE only: ANN extra columns (gender, height_raw, image_url_ann…)"),
    ("ann", "credits"):                   ("INTEGRATED", "credits", "evidence_source='ann'"),
    ("ann", "cast"):                      ("INTEGRATED", "character_voice_actors", "source='ann'; character casting relationships"),
    ("ann", "company"):                   ("INTEGRATED", "studios + anime_studios", "Animation Production rows → studios/anime_studios"),
    ("ann", "episodes"):                  ("INTEGRATED", "anime_episodes", "episode list per anime"),
    ("ann", "news"):                      ("INTEGRATED", "anime_news", "news items per anime"),
    ("ann", "related"):                   ("INTEGRATED", "anime_relations", "related anime relationships source='ann'"),
    ("ann", "releases"):                  ("INTEGRATED", "anime_releases", "home-video/BD/DVD release records"),

    # ── Bangumi ──────────────────────────────────────────────────────────────
    ("bangumi", "subjects"):              ("INTEGRATED", "anime", "id='bgm:s<id>'; type=2 rows only"),
    ("bangumi", "persons"):               ("INTEGRATED", "persons", "id='bgm:p<id>'"),
    ("bangumi", "subject_persons"):       ("INTEGRATED", "credits", "evidence_source='bangumi'"),
    ("bangumi", "characters"):            ("INTEGRATED", "characters", "id='bgm:c<id>'"),
    ("bangumi", "subject_characters"):    ("FOLDED", "characters", "character-anime link; enriches characters (DOB update)"),
    ("bangumi", "person_characters"):     ("INTEGRATED", "character_voice_actors", "source='bangumi'; joined with subject_characters"),

    # ── Keyframe ─────────────────────────────────────────────────────────────
    ("keyframe", "anime"):                ("INTEGRATED", "anime", "UPDATE: kf_uuid + kf_* columns"),
    ("keyframe", "persons"):              ("INTEGRATED", "persons", "id='keyframe:p<id>'"),
    ("keyframe", "credits"):              ("INTEGRATED", "credits", "evidence_source='keyframe'"),
    ("keyframe", "person_credits"):       ("FOLDED", "credits", "alternative credits path; merged into credits via integrate_duckdb"),
    ("keyframe", "anime_studios"):        ("INTEGRATED", "anime_studios", "source='keyframe'"),
    ("keyframe", "studios_master"):       ("INTEGRATED", "studios", "id='kf:s<studio_id>'"),
    ("keyframe", "person_jobs"):          ("INTEGRATED", "person_jobs", "source='keyframe'"),
    ("keyframe", "person_studios"):       ("INTEGRATED", "person_studio_affiliations", "source='keyframe'"),
    ("keyframe", "person_profile"):       ("INTEGRATED", "persons", "UPDATE only: description + image_large (COALESCE: existing wins)"),
    ("keyframe", "roles_master"):         ("UNUSED", "", "role lookup table; not stored in SILVER (used only for role mapping at scrape time)"),
    ("keyframe", "settings_categories"):  ("INTEGRATED", "anime_settings_categories", "production setting categories"),
    ("keyframe", "preview"):              ("UNUSED", "", "preview/teaser metadata; not loaded into SILVER"),

    # ── MAL ──────────────────────────────────────────────────────────────────
    ("mal", "anime"):                     ("INTEGRATED", "anime", "id='mal:a<id>'; display_*_mal extras"),
    ("mal", "staff_credits"):             ("INTEGRATED", "credits", "evidence_source='mal'"),
    ("mal", "va_credits"):                ("INTEGRATED", "character_voice_actors", "source='mal'"),
    ("mal", "anime_studios"):             ("INTEGRATED", "studios + anime_studios", "name-based id 'mal:n:' || name"),
    ("mal", "anime_genres"):              ("INTEGRATED", "anime_genres", "genre tags per anime"),
    ("mal", "anime_characters"):          ("INTEGRATED", "characters", "id='mal:c<id>'; name + URL only"),
    ("mal", "anime_episodes"):            ("INTEGRATED", "anime_episodes", "episode list per anime (MAL source)"),
    ("mal", "anime_external"):            ("UNUSED", "", "external streaming/site links; not loaded into SILVER"),
    ("mal", "anime_moreinfo"):            ("UNUSED", "", "free-text trivia; no structural value for scoring"),
    ("mal", "anime_news"):                ("INTEGRATED", "anime_news", "news items per anime (MAL source)"),
    ("mal", "anime_pictures"):            ("UNUSED", "", "cover/promo image URLs; no scoring value; media blob handling out of scope"),
    ("mal", "anime_recommendations"):     ("INTEGRATED", "anime_recommendations", "crowd-sourced recommendations with vote counts"),
    ("mal", "anime_relations"):           ("INTEGRATED", "anime_relations", "related anime (prequel/sequel/etc.) source='mal'"),
    ("mal", "anime_statistics"):          ("DISPLAY_ONLY", "anime.display_*_mal", "member/score statistics; excluded from scoring (H1); not yet loaded"),
    ("mal", "anime_streaming"):           ("UNUSED", "", "streaming platform links; display only, not loaded"),
    ("mal", "anime_themes"):              ("UNUSED", "", "OP/ED theme songs per anime; overlaps with seesaawiki theme_songs"),
    ("mal", "anime_videos_ep"):           ("UNUSED", "", "episode video embeds (YouTube/etc.); media metadata, no scoring value"),
    ("mal", "anime_videos_promo"):        ("UNUSED", "", "promotional video embeds; media metadata, no scoring value"),

    # ── MediaArts DB ─────────────────────────────────────────────────────────
    ("mediaarts", "anime"):               ("INTEGRATED", "anime", "id='madb:<id>'"),
    ("mediaarts", "persons"):             ("INTEGRATED", "persons", "id='madb:<id>'"),
    ("mediaarts", "credits"):             ("INTEGRATED", "credits", "evidence_source='mediaarts'"),
    ("mediaarts", "broadcasters"):        ("INTEGRATED", "anime_broadcasters", "broadcast station linkage"),
    ("mediaarts", "broadcast_schedule"):  ("INTEGRATED", "anime_broadcast_schedule", "raw schedule text per anime"),
    ("mediaarts", "original_work_links"): ("INTEGRATED", "anime_original_work_links", "source material URLs"),
    ("mediaarts", "production_committee"): ("INTEGRATED", "anime_production_committee", "production committee members"),
    ("mediaarts", "production_companies"): ("INTEGRATED", "anime_production_companies + studios + anime_studios", "companies + studio linkage"),
    ("mediaarts", "video_releases"):      ("INTEGRATED", "anime_video_releases", "BD/DVD/VHS release metadata"),

    # ── Sakuga@wiki ──────────────────────────────────────────────────────────
    ("sakuga_atwiki", "credits"):         ("INTEGRATED", "credits", "evidence_source='sakuga_atwiki'; anime_id resolved via sakuga_work_title_resolution"),
    ("sakuga_atwiki", "pages"):           ("UNUSED", "", "raw wiki page text; used only as scraper input, not stored in SILVER"),
    ("sakuga_atwiki", "persons"):         ("INTEGRATED", "persons", "id='sakuga:p<page_id>'"),
    ("sakuga_atwiki", "work_staff"):      ("FOLDED", "credits", "denormalized credits; merged into credits table via credits BRONZE"),

    # ── SeesaaWiki ───────────────────────────────────────────────────────────
    ("seesaawiki", "anime"):              ("INTEGRATED", "anime", "id='seesaa:<id>'"),
    ("seesaawiki", "persons"):            ("INTEGRATED", "persons", "id='seesaa:<id>'"),
    ("seesaawiki", "credits"):            ("INTEGRATED", "credits", "evidence_source='seesaawiki'"),
    ("seesaawiki", "studios"):            ("INTEGRATED", "studios", "id='seesaa:<id>'"),
    ("seesaawiki", "anime_studios"):      ("INTEGRATED", "anime_studios", "source='seesaawiki'"),
    ("seesaawiki", "episode_titles"):     ("INTEGRATED", "anime_episode_titles", "episode title list per anime"),
    ("seesaawiki", "theme_songs"):        ("INTEGRATED", "anime_theme_songs", "OP/ED/insert theme song credits"),
    ("seesaawiki", "gross_studios"):      ("INTEGRATED", "anime_gross_studios", "gross-credit studio attribution (seesaawiki-specific)"),
    ("seesaawiki", "original_work_info"): ("INTEGRATED", "anime_original_work_info", "source material metadata"),
    ("seesaawiki", "production_committee"): ("INTEGRATED", "anime_production_committee", "production committee (shared table with madb)"),

    # ── TMDb ─────────────────────────────────────────────────────────────────
    # TMDb is a 14/09 parallel card (conformed loader not yet implemented).
    ("tmdb", "anime"):                    ("UNUSED", "", "TMDb anime metadata; 14/09 loader in progress"),
    ("tmdb", "credits"):                  ("UNUSED", "", "TMDb crew credits; 14/09 loader in progress"),
    ("tmdb", "persons"):                  ("UNUSED", "", "TMDb person metadata; 14/09 loader in progress"),
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class BronzeTableInfo:
    """Metadata for one BRONZE table partition."""

    source: str
    table: str
    row_count: int
    error: Optional[str] = field(default=None)


@dataclass
class CoverageEntry:
    """Integration status for one (source, bronze_table) pair."""

    source: str
    bronze_table: str
    bronze_rows: int
    status: str          # INTEGRATED / FOLDED / UNUSED / DISPLAY_ONLY / UNKNOWN
    silver_target: str   # SILVER table(s) written, or '' if none
    notes: str
    in_map: bool         # True if entry exists in INTEGRATION_MAP


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_bronze_tables(bronze_root: Path) -> list[BronzeTableInfo]:
    """Enumerate all (source, table) pairs in the BRONZE hive store.

    Scans ``bronze_root/source=<src>/table=<tbl>/date=*/*.parquet`` and counts
    rows using DuckDB (latest-snapshot snapshot; all date partitions are scanned
    together; counting is approximate for large tables but sufficient for audit).

    Args:
        bronze_root: Root of the hive-partitioned BRONZE parquet store.

    Returns:
        List of BronzeTableInfo, one per (source, table) pair found.
        Sources named ``*.bak-*`` are skipped (archive/defunct partitions).
    """
    results: list[BronzeTableInfo] = []
    conn = duckdb.connect(":memory:")

    try:
        source_dirs = sorted(
            d for d in bronze_root.iterdir()
            if d.is_dir() and "bak" not in d.name
        )

        for source_dir in source_dirs:
            source = source_dir.name.replace("source=", "")
            table_dirs = sorted(
                d for d in source_dir.iterdir()
                if d.is_dir()
            )

            for table_dir in table_dirs:
                table = table_dir.name.replace("table=", "")
                glob = str(table_dir / "date=*" / "*.parquet")

                try:
                    row_count = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{glob}', "
                        f"hive_partitioning=true, union_by_name=true)"
                    ).fetchone()[0]
                    results.append(BronzeTableInfo(source=source, table=table, row_count=row_count))
                except Exception as exc:
                    results.append(
                        BronzeTableInfo(source=source, table=table, row_count=0, error=str(exc))
                    )
                    logger.warning(
                        "bronze_coverage: row count failed",
                        source=source,
                        table=table,
                        error=str(exc),
                    )
    finally:
        conn.close()

    return results


# ---------------------------------------------------------------------------
# Coverage table builder
# ---------------------------------------------------------------------------


def build_coverage_table(bronze_tables: list[BronzeTableInfo]) -> list[CoverageEntry]:
    """Join BRONZE table list with INTEGRATION_MAP to produce coverage entries.

    Tables present in BRONZE but absent from INTEGRATION_MAP are marked UNKNOWN.
    Tables in INTEGRATION_MAP but absent from the actual BRONZE store are not
    included in the output (they have no data to worry about).

    Args:
        bronze_tables: Output of discover_bronze_tables().

    Returns:
        List of CoverageEntry, one per discovered BRONZE table.
    """
    entries: list[CoverageEntry] = []

    for info in bronze_tables:
        key = (info.source, info.table)
        mapping = INTEGRATION_MAP.get(key)

        if mapping is not None:
            status, silver_target, notes = mapping
            in_map = True
        else:
            status = "UNKNOWN"
            silver_target = ""
            notes = "not declared in INTEGRATION_MAP — audit required"
            in_map = False
            logger.warning(
                "bronze_coverage: table not in INTEGRATION_MAP",
                source=info.source,
                table=info.table,
            )

        entries.append(CoverageEntry(
            source=info.source,
            bronze_table=info.table,
            bronze_rows=info.row_count,
            status=status,
            silver_target=silver_target,
            notes=notes,
            in_map=in_map,
        ))

    return entries


# ---------------------------------------------------------------------------
# Priority classification helpers
# ---------------------------------------------------------------------------

_PRIORITY_NOTES: dict[tuple[str, str], str] = {
    ("mal", "anime_external"):     "P1-HIGH: external streaming site links → enriches anime.external_links_json; complements anilist",
    ("mal", "anime_themes"):       "P2-MEDIUM: OP/ED themes → unique data vs seesaawiki (EN titles); deduplicate before loading",
    ("mal", "anime_streaming"):    "P2-MEDIUM: streaming platform availability — useful for display_* context",
    ("mal", "anime_statistics"):   "P3-LOW: member/score stats → display_*_mal columns; H1 prohibits scoring use; consider lazy load",
    ("mal", "anime_moreinfo"):     "P4-LOW: free-text trivia; low structural value; load only if search-index is planned",
    ("mal", "anime_pictures"):     "P5-DEFER: image blob URLs; requires CDN/media storage strategy first",
    ("mal", "anime_videos_ep"):    "P5-DEFER: episode video embeds; media metadata only",
    ("mal", "anime_videos_promo"): "P5-DEFER: promo video embeds; media metadata only",
    ("keyframe", "roles_master"):  "P3-LOW: role taxonomy; useful for enriching role_groups.py normalization; read-only reference",
    ("keyframe", "preview"):       "P5-DEFER: preview/teaser metadata; no scoring value identified",
    ("sakuga_atwiki", "pages"):    "P4-LOW: raw wiki page HTML; already mined for credits/persons; no additional SILVER value",
    ("tmdb", "anime"):             "P1-HIGH: TMDb anime metadata (14/09 card in parallel); 79K rows, global coverage",
    ("tmdb", "credits"):           "P1-HIGH: TMDb crew credits (14/09 card in parallel); 1.17M rows",
    ("tmdb", "persons"):           "P1-HIGH: TMDb person metadata (14/09 card in parallel); 293K rows",
}


def _priority_label(source: str, table: str) -> str:
    """Return a priority annotation for UNUSED/DISPLAY_ONLY tables."""
    return _PRIORITY_NOTES.get((source, table), "")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

_STATUS_SORT = {"UNKNOWN": 0, "UNUSED": 1, "DISPLAY_ONLY": 2, "FOLDED": 3, "INTEGRATED": 4}


def _fmt_rows(n: int) -> str:
    """Format row count with thousands separators or '?' for errors."""
    if n < 0:
        return "?"
    return f"{n:,}"


def generate_report(entries: list[CoverageEntry], out_path: Path) -> None:
    """Write a Markdown BRONZE → Conformed coverage audit report.

    Sections:
    1. Summary counts by status
    2. Full coverage table (sorted by status priority, then source, then table)
    3. Unused / Display-only table analysis
    4. Priority list for un-integrated tables
    5. Sub-card recommendations
    6. Disclaimers

    Args:
        entries: Output of build_coverage_table().
        out_path: Destination .md file path.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    integrated   = [e for e in entries if e.status == "INTEGRATED"]
    folded       = [e for e in entries if e.status == "FOLDED"]
    display_only = [e for e in entries if e.status == "DISPLAY_ONLY"]
    unused       = [e for e in entries if e.status == "UNUSED"]
    unknown      = [e for e in entries if e.status == "UNKNOWN"]

    sorted_entries = sorted(
        entries,
        key=lambda e: (_STATUS_SORT.get(e.status, 99), e.source, e.bronze_table),
    )

    sections: list[str] = []

    # ── Header ───────────────────────────────────────────────────────────────
    sections.append(f"# BRONZE → Conformed Table-Level Coverage Audit\n\nGenerated: {now}\n")
    sections.append(textwrap.dedent("""\
        Audit scope: all BRONZE tables (excluding `*.bak-*` archive partitions).
        Each table's integration status is classified as:

        | Status | Meaning |
        |--------|---------|
        | INTEGRATED | A conformed loader reads this table and writes to SILVER |
        | FOLDED | Data merged into another BRONZE or SILVER table; no dedicated SILVER table |
        | DISPLAY_ONLY | Loader reads, but data goes to `display_*` columns only (excluded from scoring per H1) |
        | UNUSED | No loader reads this table; potential new sub-card |
        | UNKNOWN | Not declared in INTEGRATION_MAP; needs audit |

    """))

    # ── Summary ──────────────────────────────────────────────────────────────
    total_rows_integrated = sum(e.bronze_rows for e in integrated)
    total_rows_unused = sum(e.bronze_rows for e in unused + display_only)

    sections.append(textwrap.dedent(f"""\
        ## Summary

        | Status | Tables | Total BRONZE rows |
        |--------|--------|-------------------|
        | INTEGRATED | {len(integrated)} | {total_rows_integrated:,} |
        | FOLDED | {len(folded)} | {sum(e.bronze_rows for e in folded):,} |
        | DISPLAY_ONLY | {len(display_only)} | {sum(e.bronze_rows for e in display_only):,} |
        | UNUSED | {len(unused)} | {sum(e.bronze_rows for e in unused):,} |
        | UNKNOWN | {len(unknown)} | {sum(e.bronze_rows for e in unknown):,} |
        | **Total** | **{len(entries)}** | **{sum(e.bronze_rows for e in entries):,}** |

        BRONZE rows reachable by conformed loaders: **{total_rows_integrated:,}**
        BRONZE rows not yet loaded into SILVER scoring paths: **{total_rows_unused:,}**

    """))

    # ── Full Coverage Table ───────────────────────────────────────────────────
    sections.append("## Full Coverage Table\n")
    sections.append(
        "| source | bronze_table | bronze_rows | status | silver_target |\n"
        "|--------|--------------|-------------|--------|---------------|\n"
    )
    for e in sorted_entries:
        target_str = e.silver_target if e.silver_target else "—"
        sections.append(
            f"| {e.source} | {e.bronze_table} | {_fmt_rows(e.bronze_rows)} "
            f"| {e.status} | {target_str} |"
        )
    sections.append("")

    # ── Unused Table Analysis ─────────────────────────────────────────────────
    actionable = [e for e in unused + display_only + unknown]
    if actionable:
        sections.append("## Un-integrated Tables — Detail\n")
        sections.append(
            "| source | bronze_table | bronze_rows | status | notes |\n"
            "|--------|--------------|-------------|--------|-------|\n"
        )
        for e in sorted(actionable, key=lambda e: -e.bronze_rows):
            priority = _priority_label(e.source, e.bronze_table)
            note_str = priority if priority else e.notes
            sections.append(
                f"| {e.source} | {e.bronze_table} | {_fmt_rows(e.bronze_rows)} "
                f"| {e.status} | {note_str} |"
            )
        sections.append("")

    # ── Priority List ─────────────────────────────────────────────────────────
    sections.append("## Priority List — Recommended Sub-Cards\n")
    sections.append(textwrap.dedent("""\
        Tables below are ranked by integration priority.
        Criteria: structural value for scoring, data volume, implementation complexity.

    """))

    priority_tables = [
        ("P1 — HIGH (scoring / network value)",
         [
             ("tmdb", "anime",            79658,  "TMDb anime metadata — global coverage complement; 14/09 loader in parallel"),
             ("tmdb", "credits",         1174486, "TMDb crew credits — 1.17M rows; non-Japanese staff coverage"),
             ("tmdb", "persons",          293115, "TMDb person metadata — 293K rows; DOB/gender/image enrichment"),
             ("mal",  "anime_external",   105508, "External streaming links — structural link graph (anilist complement)"),
         ]),
        ("P2 — MEDIUM (display enrichment)",
         [
             ("mal",  "anime_themes",     20088, "OP/ED theme song credits — EN title variant vs seesaawiki"),
             ("mal",  "anime_streaming",   8313, "Streaming availability links — display context"),
         ]),
        ("P3 — LOW (taxonomy / reference)",
         [
             ("keyframe", "roles_master", 3848,  "Role taxonomy — enriches role_groups.py; read-only reference; no SILVER table needed"),
             ("mal", "anime_statistics",  19122, "Member/score statistics — display_*_mal only; load into display columns if analytics needed"),
             ("mal", "anime_moreinfo",    19122, "Free-text trivia — low structural value; useful only if full-text search planned"),
         ]),
        ("P4 — DEFER (media / raw)",
         [
             ("mal",           "anime_pictures",    46370, "Cover image URLs — requires CDN/media strategy"),
             ("mal",           "anime_videos_ep",   33465, "Episode video embeds — media metadata only"),
             ("mal",           "anime_videos_promo", 9482, "Promo video embeds — media metadata only"),
             ("sakuga_atwiki", "pages",              2500,  "Raw wiki page HTML — already fully mined"),
             ("keyframe",      "preview",              54,  "Preview metadata — no identified scoring value"),
         ]),
    ]

    for section_title, table_list in priority_tables:
        sections.append(f"### {section_title}\n")
        sections.append(
            "| source | bronze_table | bronze_rows | recommendation |\n"
            "|--------|--------------|-------------|----------------|\n"
        )
        for src, tbl, rows, rec in table_list:
            sections.append(f"| {src} | {tbl} | {rows:,} | {rec} |")
        sections.append("")

    # ── Sub-card Recommendations ───────────────────────────────────────────────
    sections.append("## Sub-Card Recommendations\n")
    sections.append(textwrap.dedent("""\
        New sub-cards to file under `TASK_CARDS/14_silver_extend/`:

        | Sub-card ID | Source | Bronze table(s) | SILVER target | Priority |
        |-------------|--------|-----------------|---------------|----------|
        | 14/11 | tmdb | anime + credits + persons | anime / credits / persons + display_* | P1 |
        | 14/12 | mal | anime_external | anime.external_links_json enrichment | P1 |
        | 14/13 | mal | anime_themes | anime_theme_songs (seesaawiki table) or new | P2 |
        | 14/14 | mal | anime_streaming | anime.streaming_json display column | P2 |
        | 14/15 | mal | anime_statistics | anime.display_members_mal, display_scored_by_mal (already partial in loader) | P3 |

        Note: 14/09 (TMDb) is a parallel card already in progress — 14/11 should coordinate
        with that card to avoid duplication.

    """))

    # ── Disclaimer ───────────────────────────────────────────────────────────
    sections.append(textwrap.dedent("""\
        ---

        **Disclaimer (JA)**: 本レポートは公開クレジットデータの BRONZE table 統合状況を示す
        構造的監査であり、個人の評価や能力判断には一切使用しない。
        row_count はスコアリングには使用せず、取込計画の優先度付けのみに使う。

        **Disclaimer (EN)**: This report presents a structural table-level audit of
        BRONZE → Conformed integration status derived solely from public credit records.
        Row counts are used for prioritisation only and do not enter any scoring formula.
    """))

    report = "\n".join(sections)
    out_path.write_text(report, encoding="utf-8")
    logger.info("bronze_coverage: report written", path=str(out_path))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(
    bronze_root: Path = Path("result/bronze"),
    out: Path = Path("result/audit/bronze_conformed_coverage.md"),
) -> list[CoverageEntry]:
    """Run the BRONZE → Conformed coverage audit and write a Markdown report.

    Args:
        bronze_root: Root of the hive-partitioned BRONZE parquet store.
        out:         Output path for the Markdown report.

    Returns:
        List of CoverageEntry for programmatic use.
    """
    bronze_root = Path(bronze_root)
    logger.info("bronze_coverage: discovering tables", bronze_root=str(bronze_root))

    bronze_tables = discover_bronze_tables(bronze_root)
    logger.info("bronze_coverage: tables discovered", count=len(bronze_tables))

    entries = build_coverage_table(bronze_tables)
    generate_report(entries, out)

    integrated_count = sum(1 for e in entries if e.status == "INTEGRATED")
    unused_count = sum(1 for e in entries if e.status in ("UNUSED", "UNKNOWN"))
    logger.info(
        "bronze_coverage: audit complete",
        total=len(entries),
        integrated=integrated_count,
        unused=unused_count,
        report=str(out),
    )
    return entries


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="BRONZE → Conformed table-level coverage audit"
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("result/bronze"),
        help="Root of BRONZE hive-partitioned parquet store",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("result/audit/bronze_conformed_coverage.md"),
        help="Output Markdown report path",
    )
    args = parser.parse_args()
    main(bronze_root=args.bronze_root, out=args.out)

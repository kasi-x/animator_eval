"""One-shot data migration: legacy animetor.db → new schema (init_db_v2).

Usage:
    pixi run python scripts/migrate_to_v2.py \\
        --src result/animetor.db \\
        --dst result/animetor_v2.db

Safety:
    - Source DB is opened read-only.
    - Dest DB is created fresh (fails if file exists unless --force).
    - Any error aborts; no partial commit.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.database_v2 import init_db_v2


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
    )


def copy_table_direct(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    name: str,
    columns: list[str],
) -> int:
    col_list = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    rows = src.execute(f"SELECT {col_list} FROM {name}").fetchall()
    if rows:
        dst.executemany(
            f"INSERT OR IGNORE INTO {name} ({col_list}) VALUES ({placeholders})",
            rows,
        )
    return len(rows)


def copy_anime(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    src_cols = set(_columns(src, "anime"))
    # Build SELECT: rename source → original_work_type, skip bronze-only columns
    skip = {"score", "popularity", "description", "genres", "tags", "studios",
             "cover_large", "cover_extra_large", "cover_medium", "banner",
             "cover_large_path", "banner_path", "popularity_rank", "favourites",
             "synonyms", "mean_score", "country_of_origin", "is_licensed",
             "is_adult", "hashtag", "site_url", "trailer_url", "trailer_site",
             "relations_json", "external_links_json", "rankings_json",
             "ann_id", "allcinema_id"}
    dst_cols = _columns(dst, "anime")

    select_parts = []
    insert_cols = []
    for col in dst_cols:
        if col == "original_work_type":
            if "source" in src_cols:
                select_parts.append("source AS original_work_type")
                insert_cols.append("original_work_type")
        elif col in src_cols and col not in skip:
            select_parts.append(col)
            insert_cols.append(col)

    if not select_parts:
        return 0

    col_list = ", ".join(select_parts)
    insert_list = ", ".join(insert_cols)
    placeholders = ", ".join("?" for _ in insert_cols)

    rows = src.execute(f"SELECT {col_list} FROM anime").fetchall()
    if rows:
        dst.executemany(
            f"INSERT OR IGNORE INTO anime ({insert_list}) VALUES ({placeholders})",
            rows,
        )
    return len(rows)


def copy_persons(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    src_cols = set(_columns(src, "persons"))
    dst_cols = _columns(dst, "persons")
    common = [c for c in dst_cols if c in src_cols]
    return copy_table_direct(src, dst, "persons", common)


def copy_credits(src: sqlite3.Connection, dst: sqlite3.Connection) -> int:
    src_cols = set(_columns(src, "credits"))
    dst_cols = _columns(dst, "credits")

    select_parts = []
    insert_cols = []
    for col in dst_cols:
        if col == "evidence_source":
            src_col = "evidence_source" if "evidence_source" in src_cols else "source"
            if src_col in src_cols:
                select_parts.append(f"{src_col} AS evidence_source")
                insert_cols.append("evidence_source")
        elif col == "episode":
            if "episode" in src_cols:
                # Convert sentinel -1 to NULL
                select_parts.append("CASE WHEN episode = -1 THEN NULL ELSE episode END AS episode")
                insert_cols.append("episode")
        elif col in src_cols:
            select_parts.append(col)
            insert_cols.append(col)

    if not select_parts:
        return 0

    col_list = ", ".join(select_parts)
    insert_list = ", ".join(insert_cols)
    placeholders = ", ".join("?" for _ in insert_cols)

    rows = src.execute(f"SELECT {col_list} FROM credits").fetchall()
    if rows:
        dst.executemany(
            f"INSERT OR IGNORE INTO credits ({insert_list}) VALUES ({placeholders})",
            rows,
        )
    return len(rows)


def copy_renamed(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    src_table: str,
    dst_table: str,
) -> int:
    if not _table_exists(src, src_table):
        return 0
    src_cols = set(_columns(src, src_table))
    dst_cols = _columns(dst, dst_table)
    common = [c for c in dst_cols if c in src_cols]
    if not common:
        return 0
    col_list = ", ".join(common)
    placeholders = ", ".join("?" for _ in common)
    rows = src.execute(f"SELECT {col_list} FROM {src_table}").fetchall()
    if rows:
        dst.executemany(
            f"INSERT OR IGNORE INTO {dst_table} ({col_list}) VALUES ({placeholders})",
            rows,
        )
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, required=True, help="Source DB (read-only)")
    parser.add_argument("--dst", type=Path, required=True, help="Destination DB (new)")
    parser.add_argument("--force", action="store_true", help="Overwrite dst if exists")
    args = parser.parse_args()

    if not args.src.exists():
        print(f"Source DB not found: {args.src}", file=sys.stderr)
        return 2

    if args.dst.exists() and not args.force:
        print(f"Dest exists: {args.dst} (use --force to overwrite)", file=sys.stderr)
        return 2

    if args.dst.exists():
        args.dst.unlink()

    src = sqlite3.connect(f"file:{args.src}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    dst = sqlite3.connect(args.dst)

    try:
        init_db_v2(dst)
        dst.execute("BEGIN")

        stats: dict[str, int] = {}

        # Canonical tables
        stats["anime"]   = copy_anime(src, dst)
        stats["persons"] = copy_persons(src, dst)
        stats["credits"] = copy_credits(src, dst)

        # Direct copies (same name, same columns)
        for t in ["studios", "anime_studios", "anime_external_ids", "person_external_ids",
                  "person_aliases", "anime_genres", "anime_tags", "anime_relations",
                  "characters", "character_voice_actors", "person_affiliations"]:
            if _table_exists(src, t):
                src_cols = set(_columns(src, t))
                dst_cols = _columns(dst, t)
                common = [c for c in dst_cols if c in src_cols]
                stats[t] = copy_table_direct(src, dst, t, common)

        # Renamed tables
        stats["person_scores"] = copy_renamed(src, dst, "scores", "person_scores")
        stats["voice_actor_scores"] = copy_renamed(src, dst, "va_scores", "voice_actor_scores")
        stats["ops_source_scrape_status"] = copy_renamed(
            src, dst, "source_scrape_status", "ops_source_scrape_status"
        )
        stats["ops_lineage"] = copy_renamed(src, dst, "meta_lineage", "ops_lineage")
        stats["ops_entity_resolution_audit"] = copy_renamed(
            src, dst, "meta_entity_resolution_audit", "ops_entity_resolution_audit"
        )
        stats["ops_quality_snapshot"] = copy_renamed(
            src, dst, "meta_quality_snapshot", "ops_quality_snapshot"
        )

        # feat_* tables
        for t in ["feat_person_scores", "feat_network", "feat_career", "feat_genre_affinity",
                  "feat_contribution", "feat_credit_activity", "feat_career_annual",
                  "feat_birank_annual", "birank_compute_state", "feat_studio_affiliation",
                  "feat_credit_contribution", "feat_person_work_summary", "feat_work_context",
                  "feat_person_role_progression", "feat_causal_estimates",
                  "feat_cluster_membership", "feat_mentorships", "feat_career_gaps"]:
            if _table_exists(src, t):
                src_cols = set(_columns(src, t))
                dst_cols = _columns(dst, t)
                common = [c for c in dst_cols if c in src_cols]
                stats[t] = copy_table_direct(src, dst, t, common)

        # agg_* tables
        for t in ["agg_milestones", "agg_director_circles"]:
            if _table_exists(src, t):
                src_cols = set(_columns(src, t))
                dst_cols = _columns(dst, t)
                common = [c for c in dst_cols if c in src_cols]
                stats[t] = copy_table_direct(src, dst, t, common)

        # meta_* report tables
        for t in ["meta_common_person_parameters",
                  "meta_policy_attrition", "meta_policy_monopsony",
                  "meta_policy_gender", "meta_policy_generation",
                  "meta_hr_studio_benchmark", "meta_hr_mentor_card",
                  "meta_hr_attrition_risk", "meta_hr_succession",
                  "meta_biz_whitespace", "meta_biz_undervalued",
                  "meta_biz_trust_entry", "meta_biz_team_template",
                  "meta_biz_independent_unit"]:
            if _table_exists(src, t):
                src_cols = set(_columns(src, t))
                dst_cols = _columns(dst, t)
                common = [c for c in dst_cols if c in src_cols]
                stats[t] = copy_table_direct(src, dst, t, common)

        # src_* bronze tables
        for t in ["src_anilist_anime", "src_anilist_persons", "src_anilist_credits",
                  "src_ann_anime", "src_ann_persons", "src_ann_credits",
                  "src_allcinema_anime", "src_allcinema_persons", "src_allcinema_credits",
                  "src_seesaawiki_anime", "src_seesaawiki_credits",
                  "src_keyframe_anime", "src_keyframe_credits"]:
            if _table_exists(src, t):
                src_cols = set(_columns(src, t))
                dst_cols = _columns(dst, t)
                common = [c for c in dst_cols if c in src_cols]
                stats[t] = copy_table_direct(src, dst, t, common)

        dst.commit()

        total = sum(stats.values())
        for table, n in sorted(stats.items()):
            if n:
                print(f"  {table}: {n} rows")
        print(f"\nTotal: {total} rows copied to {args.dst}")
        return 0

    except Exception as e:
        dst.rollback()
        print(f"FAIL: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    raise SystemExit(main())

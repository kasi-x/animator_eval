"""keyframe vs 他 source の入力差分 CSV ダンプ.

2 系統で比較する:

1. **cluster path** (`match_type=cluster`)
   resolved.duckdb の cluster で keyframe member を含む canonical を取り、
   同 canonical 内の keyframe row vs 他 source row を field 比較.

2. **orphan path** (`match_type=orphan_unique`)
   Resolved に乗らなかった (cluster 化されなかった) keyframe row を救う.
   silver 内で natural key (anime: title + year, person: name, studio: 正規化 name)
   が **両側で一意** (kf 側も other 側もその key で COUNT=1) のペアのみ採用.
   multi:multi マッチは ambiguous として除外し誤マッチ爆発を防ぐ.

両 path とも、両値 NOT NULL かつ値が異なる場合のみ 1 row per (entity, field, other) 出力.

keyframe prefix は anime/persons は `keyframe:`、studios は `kf:` で BRONZE writer が混在.
両 prefix を受ける.

出力列:
    entity_type, match_type, canonical_id, label, field,
    kf_id, kf_value, other_src, other_id, other_value
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
# 5層 architecture: Conformed 層 = animetor.duckdb の `conformed` schema.
# 旧 silver.duckdb (main schema) は legacy で参照しない.
CONFORMED_DB = REPO_ROOT / "result" / "animetor.duckdb"
RESOLVED_DB = REPO_ROOT / "result" / "resolved.duckdb"
OUTPUT_CSV = REPO_ROOT / "result" / "keyframe_vs_others_diff.csv"

# Keyframe prefix は entity で異なる. 両方受ける.
KF_PREFIX_PATTERN = "(id LIKE 'keyframe:%' OR id LIKE 'kf:%')"

ANIME_FIELDS = [
    "title_ja",
    "title_en",
    "year",
    "episodes",
    "format",
    "duration",
    "source_mat",
    "season",
    "start_date",
    "end_date",
    "status",
    "country_of_origin",
]

PERSON_FIELDS = [
    "name_ja",
    "name_en",
    "name_ko",
    "name_zh",
    "gender",
    "birth_date",
    "death_date",
    "nationality",
]

STUDIO_FIELDS = [
    "name",
    "is_animation_studio",
    "country_of_origin",
]


LABEL_EXPR = {
    "anime": "COALESCE(title_ja, title_en)",
    "person": "COALESCE(name_ja, name_en)",
    "studio": "name",
}

RESOLVED_TABLE = {"anime": "anime", "person": "persons", "studio": "studios"}
SILVER_TABLE = {"anime": "anime", "person": "persons", "studio": "studios"}

def _build_member_view(con: duckdb.DuckDBPyConnection, entity: str) -> None:
    """canonical_id ↔ member_id 展開 view (keyframe member 含む cluster のみ)."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_members AS
        SELECT
          canonical_id,
          {LABEL_EXPR[entity]} AS label,
          UNNEST(CAST(source_ids_json AS JSON)::VARCHAR[]) AS member_id
        FROM resolved.{RESOLVED_TABLE[entity]}
        WHERE source_ids_json LIKE '%keyframe:%' OR source_ids_json LIKE '%kf:%'
        """
    )


def _cluster_diff_sql(entity: str, table: str, fields: list[str]) -> str:
    """cluster path: 同 canonical で keyframe vs 他 source 比較."""
    union_blocks = []
    for f in fields:
        union_blocks.append(
            f"""
            SELECT
              '{entity}' AS entity_type,
              'cluster' AS match_type,
              kf.canonical_id,
              kf.label,
              '{f}' AS field,
              kf.member_id AS kf_id,
              CAST(kf_row.{f} AS VARCHAR) AS kf_value,
              split_part(oth.member_id, ':', 1) AS other_src,
              oth.member_id AS other_id,
              CAST(oth_row.{f} AS VARCHAR) AS other_value
            FROM {entity}_kf kf
            JOIN animetor.conformed.{table} kf_row ON kf_row.id = kf.member_id
            JOIN {entity}_other oth ON oth.canonical_id = kf.canonical_id
            JOIN animetor.conformed.{table} oth_row ON oth_row.id = oth.member_id
            WHERE kf_row.{f} IS NOT NULL
              AND oth_row.{f} IS NOT NULL
              AND CAST(kf_row.{f} AS VARCHAR) <> CAST(oth_row.{f} AS VARCHAR)
            """
        )
    return "\nUNION ALL\n".join(union_blocks)


_NORM_NAME = (
    "LOWER(TRIM(regexp_replace({col}, "
    "'株式会社|㈱|有限会社|\\(株\\)|\\(有\\)|Inc\\.?|Ltd\\.?|Co\\.?, ?Ltd\\.?', "
    "'', 'g')))"
)

# entity 別 natural key 式 (kf/oth どちらの prefix も入れた式に format される).
_NATKEY_TEMPLATE = {
    "anime": "COALESCE({t}.title_ja, {t}.title_en) || '|' || COALESCE(CAST({t}.year AS VARCHAR), '')",
    "person": "COALESCE({t}.name_ja, {t}.name_en)",
    "studio": _NORM_NAME.format(col="{t}.name"),
}


def _orphan_label_expr(entity: str) -> str:
    if entity == "anime":
        return "COALESCE(kf.title_ja, kf.title_en)"
    if entity == "person":
        return "COALESCE(kf.name_ja, kf.name_en)"
    return "kf.name"


def _natkey_not_null(entity: str, alias: str) -> str:
    """natural key の最低限欠損弾き (label 部分 NOT NULL)."""
    if entity == "anime":
        return f"COALESCE({alias}.title_ja, {alias}.title_en) IS NOT NULL"
    if entity == "person":
        return f"COALESCE({alias}.name_ja, {alias}.name_en) IS NOT NULL"
    return f"{alias}.name IS NOT NULL"


def _build_orphan_unique_views(
    con: duckdb.DuckDBPyConnection, entity: str, table: str
) -> None:
    """kf 側/oth 側それぞれで natural_key が一意 (COUNT=1) の row のみ抽出 view.

    orphan 対象 = 「cluster path で oth とペアにならなかった kf row」.
    cluster の `source_ids_json` に kf 単独で入っているだけの canonical (Phase 2a
    single-source studio で発生) は orphan に含める必要があるため、`{entity}_kf`
    全体ではなく、`{entity}_other` と同 canonical を持つ kf member のみ除外する.
    """
    natkey_kf = _NATKEY_TEMPLATE[entity].format(t="t")
    notnull_kf = _natkey_not_null(entity, "t")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_kf_paired AS
        SELECT DISTINCT kf.member_id
        FROM {entity}_kf kf
        JOIN {entity}_other oth ON oth.canonical_id = kf.canonical_id
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_orphan_kf AS
        WITH base AS (
          SELECT t.*, {natkey_kf} AS natkey
          FROM animetor.conformed.{table} t
          WHERE (t.id LIKE 'keyframe:%' OR t.id LIKE 'kf:%')
            AND {notnull_kf}
            AND NOT EXISTS (
              SELECT 1 FROM {entity}_kf_paired p WHERE p.member_id = t.id
            )
        )
        SELECT * FROM base
        WHERE natkey IN (
          SELECT natkey FROM base GROUP BY natkey HAVING COUNT(*) = 1
        )
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_orphan_oth AS
        WITH base AS (
          SELECT t.*, {natkey_kf} AS natkey
          FROM animetor.conformed.{table} t
          WHERE t.id NOT LIKE 'keyframe:%' AND t.id NOT LIKE 'kf:%'
            AND {notnull_kf}
        )
        SELECT * FROM base
        WHERE natkey IN (
          SELECT natkey FROM base GROUP BY natkey HAVING COUNT(*) = 1
        )
        """
    )


def _orphan_diff_sql(entity: str, table: str, fields: list[str]) -> str:
    """orphan path: 一意 natural key の kf vs other のみ比較."""
    label = _orphan_label_expr(entity)
    union_blocks = []
    for f in fields:
        union_blocks.append(
            f"""
            SELECT
              '{entity}' AS entity_type,
              'orphan_unique' AS match_type,
              'orphan:' || kf.id AS canonical_id,
              {label} AS label,
              '{f}' AS field,
              kf.id AS kf_id,
              CAST(kf.{f} AS VARCHAR) AS kf_value,
              split_part(oth.id, ':', 1) AS other_src,
              oth.id AS other_id,
              CAST(oth.{f} AS VARCHAR) AS other_value
            FROM {entity}_orphan_kf kf
            JOIN {entity}_orphan_oth oth ON kf.natkey = oth.natkey
            WHERE kf.{f} IS NOT NULL
              AND oth.{f} IS NOT NULL
              AND CAST(kf.{f} AS VARCHAR) <> CAST(oth.{f} AS VARCHAR)
            """
        )
    return "\nUNION ALL\n".join(union_blocks)


def _dump_entity(
    con: duckdb.DuckDBPyConnection,
    entity: str,
    table: str,
    fields: list[str],
    writer: csv.writer,
) -> tuple[int, int]:
    """returns (cluster_rows, orphan_rows)."""
    _build_member_view(con, entity)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_kf AS
        SELECT * FROM {entity}_members
        WHERE member_id LIKE 'keyframe:%' OR member_id LIKE 'kf:%'
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {entity}_other AS
        SELECT * FROM {entity}_members
        WHERE member_id NOT LIKE 'keyframe:%' AND member_id NOT LIKE 'kf:%'
        """
    )

    cluster_rows = con.execute(_cluster_diff_sql(entity, table, fields)).fetchall()
    for r in cluster_rows:
        writer.writerow(r)
    logger.info("cluster_diff_written", entity=entity, rows=len(cluster_rows))

    _build_orphan_unique_views(con, entity, table)
    orphan_rows = con.execute(_orphan_diff_sql(entity, table, fields)).fetchall()
    for r in orphan_rows:
        writer.writerow(r)
    logger.info("orphan_diff_written", entity=entity, rows=len(orphan_rows))

    return len(cluster_rows), len(orphan_rows)


def _dump_credits(con: duckdb.DuckDBPyConnection, writer: csv.writer) -> int:
    """credits.role: 同 (person_canonical, anime_canonical) で keyframe vs 他 source."""
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW person_map AS
        SELECT
          canonical_id AS person_canonical,
          UNNEST(CAST(source_ids_json AS JSON)::VARCHAR[]) AS person_member,
          COALESCE(name_ja, name_en) AS person_label
        FROM resolved.persons
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW anime_map AS
        SELECT
          canonical_id AS anime_canonical,
          UNNEST(CAST(source_ids_json AS JSON)::VARCHAR[]) AS anime_member,
          COALESCE(title_ja, title_en) AS anime_label
        FROM resolved.anime
        """
    )
    sql = """
        WITH credit_resolved AS (
          SELECT
            pm.person_canonical,
            am.anime_canonical,
            pm.person_label,
            am.anime_label,
            c.person_id,
            c.anime_id,
            c.role,
            split_part(c.person_id, ':', 1) AS src
          FROM animetor.conformed.credits c
          JOIN person_map pm ON pm.person_member = c.person_id
          JOIN anime_map  am ON am.anime_member  = c.anime_id
          WHERE c.role IS NOT NULL
        ),
        kf AS (
          SELECT * FROM credit_resolved WHERE src IN ('keyframe', 'kf')
        ),
        oth AS (
          SELECT * FROM credit_resolved WHERE src NOT IN ('keyframe', 'kf')
        )
        SELECT
          'credit_role' AS entity_type,
          'cluster' AS match_type,
          kf.person_canonical || '|' || kf.anime_canonical AS canonical_id,
          kf.person_label || ' @ ' || kf.anime_label AS label,
          'role' AS field,
          kf.person_id || '|' || kf.anime_id AS kf_id,
          kf.role AS kf_value,
          oth.src AS other_src,
          oth.person_id || '|' || oth.anime_id AS other_id,
          oth.role AS other_value
        FROM kf
        JOIN oth USING (person_canonical, anime_canonical)
        WHERE kf.role <> oth.role
    """
    rows = con.execute(sql).fetchall()
    for r in rows:
        writer.writerow(r)
    logger.info("credits_diff_written", rows=len(rows))
    return len(rows)


def main() -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{CONFORMED_DB}' AS animetor (READ_ONLY)")
    con.execute(f"ATTACH '{RESOLVED_DB}' AS resolved (READ_ONLY)")

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "entity_type",
                "match_type",
                "canonical_id",
                "label",
                "field",
                "kf_id",
                "kf_value",
                "other_src",
                "other_id",
                "other_value",
            ]
        )
        summary = {}
        for entity, fields in [
            ("anime", ANIME_FIELDS),
            ("person", PERSON_FIELDS),
            ("studio", STUDIO_FIELDS),
        ]:
            cluster, orphan = _dump_entity(
                con, entity, SILVER_TABLE[entity], fields, writer
            )
            summary[entity] = {"cluster": cluster, "orphan": orphan}

        credits_rows = _dump_credits(con, writer)
        summary["credit_role"] = {"cluster": credits_rows, "orphan": 0}

    total = sum(v["cluster"] + v["orphan"] for v in summary.values())
    print(f"output: {OUTPUT_CSV}")
    print(f"total rows: {total}")
    for ent, counts in summary.items():
        print(f"  {ent}: cluster={counts['cluster']} orphan={counts['orphan']}")


if __name__ == "__main__":
    main()

"""keyframe-source row の ID 形式分布 audit (read-only).

`TASK_CARDS/19_resolved_cluster_fix/05_keyframe_id_dedup.md` の Investigation
Step 1. silver.{anime,persons,studios,credits} を read-only で開き:

1. **prefix 分布**: `keyframe:` vs `kf:` 件数
2. **ID 形式分布**: SHA hex 64 / SHA hex 32 / slug / 短 ID (`s\\d+`) etc を
   正規表現で分類して件数
3. **natural-key 並存 (重複候補)**: 同 natural_key を持つ kf row が複数あるケースを
   サンプリング. natkey = anime: title_ja/en+year, person: name, studio: 正規化 name

出力:
- 標準出力に集計
- `result/keyframe_id_audit_dup_<entity>.csv` に並存サンプル (各 entity 最大 50 件)
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DB = REPO_ROOT / "result" / "silver.duckdb"
OUTPUT_DIR = REPO_ROOT / "result"

KF_PREFIX_FILTER = "(id LIKE 'keyframe:%' OR id LIKE 'kf:%')"


# entity -> (table, natkey_sql, label_sql)
ENTITY_SPEC: dict[str, tuple[str, str, str]] = {
    "anime": (
        "anime",
        "COALESCE(title_ja, title_en) || '|' || COALESCE(CAST(year AS VARCHAR), '')",
        "COALESCE(title_ja, title_en)",
    ),
    "person": (
        "persons",
        "COALESCE(name_ja, name_en)",
        "COALESCE(name_ja, name_en)",
    ),
    "studio": (
        "studios",
        "LOWER(TRIM(regexp_replace(name, "
        "'株式会社|㈱|有限会社|\\(株\\)|\\(有\\)|Inc\\.?|Ltd\\.?|Co\\.?, ?Ltd\\.?', "
        "'', 'g')))",
        "name",
    ),
}

# ID 形式分類用 regex (prefix 除いた suffix 部分でマッチ).
ID_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("sha256_64hex", re.compile(r"^[0-9a-f]{64}$")),
    ("sha1_40hex", re.compile(r"^[0-9a-f]{40}$")),
    ("md5_32hex", re.compile(r"^[0-9a-f]{32}$")),
    ("short_hex_8_16", re.compile(r"^[0-9a-f]{8,16}$")),
    ("s_numeric", re.compile(r"^s\d+$")),  # 例: kf:s164
    ("a_numeric", re.compile(r"^a\d+$")),  # 例: keyframe:a123
    ("p_numeric", re.compile(r"^p\d+$")),
    ("numeric_only", re.compile(r"^\d+$")),
    ("slug_kebab", re.compile(r"^[a-z0-9]+(-[a-z0-9]+)+$")),
    ("slug_snake", re.compile(r"^[a-z0-9]+(_[a-z0-9]+)+$")),
]


def _classify_id_format(suffix: str) -> str:
    """ID suffix を ID_PATTERNS で分類. マッチしなければ 'other'."""
    for name, pat in ID_PATTERNS:
        if pat.match(suffix):
            return name
    return "other"


def _audit_prefix(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, int]:
    """prefix 別件数."""
    r = con.execute(
        f"""
        SELECT
          SUM(CASE WHEN id LIKE 'keyframe:%' THEN 1 ELSE 0 END) AS keyframe_,
          SUM(CASE WHEN id LIKE 'kf:%' THEN 1 ELSE 0 END) AS kf_
        FROM silver.{table}
        """
    ).fetchone()
    return {"keyframe:": r[0] or 0, "kf:": r[1] or 0}


def _audit_id_format(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, int]:
    """ID suffix を Python で分類して集計."""
    rows = con.execute(
        f"SELECT id FROM silver.{table} WHERE {KF_PREFIX_FILTER}"
    ).fetchall()
    buckets: dict[str, int] = {}
    for (id_,) in rows:
        suffix = id_.split(":", 1)[1] if ":" in id_ else id_
        cls = _classify_id_format(suffix)
        buckets[cls] = buckets.get(cls, 0) + 1
    return buckets


def _audit_natkey_dups(
    con: duckdb.DuckDBPyConnection,
    entity: str,
    table: str,
    natkey_sql: str,
    label_sql: str,
    max_samples: int = 50,
) -> tuple[int, list[tuple]]:
    """同 natkey で kf row が複数並存する件数 + サンプル."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW kf_natkey AS
        SELECT
          id,
          {natkey_sql} AS natkey,
          {label_sql} AS label
        FROM silver.{table}
        WHERE {KF_PREFIX_FILTER}
        """
    )
    dup_keys = con.execute(
        "SELECT natkey, COUNT(*) AS n FROM kf_natkey "
        "WHERE natkey IS NOT NULL AND natkey <> '' "
        "GROUP BY natkey HAVING COUNT(*) > 1"
    ).fetchall()
    total_dup_rows = sum(n for _, n in dup_keys)

    samples: list[tuple] = []
    if dup_keys:
        sample_keys = [k for k, _ in dup_keys[:max_samples]]
        placeholders = ", ".join(["?"] * len(sample_keys))
        samples = con.execute(
            f"""
            SELECT natkey, label, id
            FROM kf_natkey
            WHERE natkey IN ({placeholders})
            ORDER BY natkey, id
            """,
            sample_keys,
        ).fetchall()
    return total_dup_rows, samples


def _write_dup_csv(entity: str, samples: list[tuple]) -> Path | None:
    if not samples:
        return None
    out = OUTPUT_DIR / f"keyframe_id_audit_dup_{entity}.csv"
    with out.open("w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp)
        w.writerow(["entity_type", "natkey", "label", "kf_id"])
        for natkey, label, kf_id in samples:
            w.writerow([entity, natkey, label, kf_id])
    return out


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{SILVER_DB}' AS silver (READ_ONLY)")

    print("=" * 70)
    print("keyframe ID audit — silver layer")
    print("=" * 70)

    for entity, (table, natkey_sql, label_sql) in ENTITY_SPEC.items():
        print(f"\n## {entity}  (silver.{table})")
        prefix = _audit_prefix(con, table)
        print(f"  prefix:  keyframe:={prefix['keyframe:']}  kf:={prefix['kf:']}")

        fmt = _audit_id_format(con, table)
        if fmt:
            print("  id_format:")
            for k in sorted(fmt, key=lambda x: -fmt[x]):
                print(f"    {k:<20} {fmt[k]:>8}")

        dup_total, samples = _audit_natkey_dups(con, entity, table, natkey_sql, label_sql)
        print(f"  natkey dup rows: {dup_total}  (kf row 同 natkey 並存)")
        out = _write_dup_csv(entity, samples)
        if out:
            print(f"  dup samples → {out}")

    # credits は person_id / anime_id の prefix も見る
    print("\n## credits (kf prefix 参照)")
    r = con.execute(
        """
        SELECT
          SUM(CASE WHEN person_id LIKE 'keyframe:%' OR person_id LIKE 'kf:%' THEN 1 ELSE 0 END) AS kf_person,
          SUM(CASE WHEN anime_id LIKE 'keyframe:%' OR anime_id LIKE 'kf:%' THEN 1 ELSE 0 END) AS kf_anime,
          COUNT(*) AS total
        FROM silver.credits
        """
    ).fetchone()
    print(f"  total={r[2]}  kf_person={r[0]}  kf_anime={r[1]}")


if __name__ == "__main__":
    main()

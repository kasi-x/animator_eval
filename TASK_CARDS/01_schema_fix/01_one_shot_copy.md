# Task: 旧 DB → 新 schema への一発データコピー

**ID**: `01_schema_fix/01_one_shot_copy`
**Priority**: 🔴 Critical
**Estimated changes**: 新スクリプト 1 本 (~150-300 lines)
**Blocks**: `02`, `03`, `04`
**Blocked by**: `00_target_schema`

---

## Goal

旧 `result/animetor.db` (v54 相当) から、新 schema の DB へ **1 回限りのデータコピー**を行うスクリプトを書く。各テーブル単位で SELECT → INSERT、命名変更箇所(`_naming_decisions/` 反映)を吸収する。

---

## 方針(重要)

- **漸進 migration を書かない**。`ALTER TABLE` / `DROP TABLE` / `RENAME` の連鎖は不要
- 旧 DB は**読み取り専用**で扱う。書き込みは新 DB のみ
- エラーは無視せず早期終了。**失敗したら最初からやり直す** 前提
- 「正しく動いたら OK」なので、データ不整合を途中で検出したら raise して止める
- テーブルごとに独立したコピー関数にすれば、個別再実行もしやすい

---

## Files to create

| File | 内容 |
|---|---|
| `scripts/migrate_to_v2.py` | 旧 → 新のデータコピースクリプト |

---

## 命名変換マッピング (`_naming_decisions/` から抽出)

| 旧テーブル | 新テーブル | カラム変換 |
|---|---|---|
| `scores` | `person_scores` | そのまま |
| `va_scores` | `voice_actor_scores` | そのまま |
| `data_sources` | `ops_source_scrape_status` | そのまま |
| `meta_lineage` | `ops_lineage` | そのまま |
| `meta_entity_resolution_audit` | `ops_entity_resolution_audit` | そのまま |
| `meta_quality_snapshot` | `ops_quality_snapshot` | そのまま |
| `anime` 列 `source` | `anime` 列 `original_work_type` | リネームのみ |
| `credits` 列 `episode=-1` | `credits` 列 `episode=NULL` | sentinel 変換 |

**廃止(コピーしない)**: `anime_display`, `anime_analysis`, `ensure_phase1_schema` が作る一時テーブル, `_archive_v*`, `credits_new`

---

## スクリプト骨格

```python
"""One-shot data migration: old animetor.db → new schema (init_db_v2).

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
import argparse, sqlite3, sys
from pathlib import Path

from src.database_v2 import init_db_v2


def copy_table_direct(src: sqlite3.Connection, dst: sqlite3.Connection,
                       name: str, columns: list[str]) -> int:
    """Straight column-for-column copy."""
    col_list = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    rows = src.execute(f"SELECT {col_list} FROM {name}").fetchall()
    dst.executemany(
        f"INSERT INTO {name} ({col_list}) VALUES ({placeholders})",
        rows,
    )
    return len(rows)


def copy_scores_to_person_scores(src, dst) -> int:
    """scores → person_scores (table renamed)"""
    rows = src.execute("SELECT * FROM scores").fetchall()
    # ... column list and INSERT
    return len(rows)


def copy_anime(src, dst) -> int:
    """anime: rename 'source' column → 'original_work_type',
    skip score/popularity/description/cover_*/genres JSON (these live in bronze only)."""
    rows = src.execute("""
        SELECT id, title_ja, title_en, year, season, quarter, episodes, format,
               duration, start_date, end_date, status,
               source AS original_work_type,
               work_type, scale_class, updated_at
        FROM anime
    """).fetchall()
    # ... INSERT into new anime table
    return len(rows)


def copy_credits(src, dst) -> int:
    """credits: evidence_source column used, episode=-1 → NULL."""
    rows = src.execute("""
        SELECT id, person_id, anime_id, role, raw_role,
               CASE WHEN episode = -1 THEN NULL ELSE episode END AS episode,
               evidence_source, updated_at
        FROM credits
    """).fetchall()
    # ... INSERT
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=Path, required=True)
    parser.add_argument("--dst", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if args.dst.exists() and not args.force:
        print(f"Dest exists: {args.dst} (use --force to overwrite)", file=sys.stderr)
        return 2

    if args.dst.exists():
        args.dst.unlink()

    src = sqlite3.connect(f"file:{args.src}?mode=ro", uri=True)
    dst = sqlite3.connect(args.dst)
    try:
        init_db_v2(dst)
        dst.execute("BEGIN")

        stats = {}
        stats["anime"] = copy_anime(src, dst)
        stats["persons"] = copy_table_direct(src, dst, "persons", [...])
        stats["credits"] = copy_credits(src, dst)
        stats["person_scores"] = copy_scores_to_person_scores(src, dst)
        stats["voice_actor_scores"] = copy_va_scores(src, dst)
        # ... 全テーブル分
        stats["ops_lineage"] = copy_table_renamed(src, dst, "meta_lineage", "ops_lineage")
        # ...

        dst.commit()
        for t, n in stats.items():
            print(f"  {t}: {n} rows")
        print(f"Total: {sum(stats.values())} rows copied")
        return 0
    except Exception as e:
        dst.rollback()
        print(f"FAIL: {e}", file=sys.stderr)
        return 1
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    raise SystemExit(main())
```

---

## Steps

1. `scripts/migrate_to_v2.py` を骨格通り書く (全テーブル分の copy 関数)
2. **small fixture でテスト**: 合成 DB で試して期待行数が移るか確認
3. 実 DB でコピー実行、stats を記録
4. (次のカード `03` / `04` で動作検証する)

---

## Verification (最小限)

```bash
# 1. スクリプトが実行できる
pixi run python scripts/migrate_to_v2.py --src result/animetor.db --dst /tmp/v2.db --force

# 2. 行数の sanity check (旧 DB と新 DB で比較)
# コピー対象外の anime_display/anime_analysis は 0 件でよい
sqlite3 /tmp/v2.db "SELECT COUNT(*) FROM anime"
sqlite3 result/animetor.db "SELECT COUNT(*) FROM anime"
# 期待: ほぼ同数 (anime_analysis 由来分)

sqlite3 /tmp/v2.db "SELECT COUNT(*) FROM person_scores"
sqlite3 result/animetor.db "SELECT COUNT(*) FROM scores"
# 期待: 同数

# 3. 廃止テーブルが存在しない
sqlite3 /tmp/v2.db "SELECT name FROM sqlite_master WHERE name IN ('anime_display','anime_analysis','va_scores','data_sources')"
# 期待: 空

# 4. 原作種別列の rename
sqlite3 /tmp/v2.db "PRAGMA table_info(anime)" | grep -E 'source|original_work_type'
# 期待: original_work_type のみ
```

---

## Completion signal

- [ ] `scripts/migrate_to_v2.py` が成功終了 (exit 0)
- [ ] 行数比較が sane
- [ ] コミット: `Add one-shot migration script to target schema`

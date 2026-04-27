# Task: madb (mediaarts) SILVER 統合 (6 新表)

**ID**: `14_silver_extend/02_madb_silver`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: yes (新表 PK 設計、anime_id mapping)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`TODO.md §10.2` で BRONZE 化済の madb 6 テーブルを SILVER に統合する:
- `broadcasters` (332,492 行) / `broadcast_schedule` (4,626) / `production_committee` (15,352) / `production_companies` (29,434) / `video_releases` (292,148) / `original_work_links` (3,576)

---

## Hard constraints

- **H1**: madb は score 系列なし → 該当なし、ただし新規 SILVER 列に display 系列追加禁止
- **H3**: entity_resolution 不変
- **H4**: credits は既存 source='mediaarts' で integrate 済 (touch しない)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE 確認: `find result/bronze/source=mediaarts/table=*/date=2026-04-27/ -name "*.parquet" | wc -l` → 6
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/madb.py` | `integrate(conn, bronze_root)` 関数 |
| `tests/test_etl/test_silver_madb.py` | 単体テスト |

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | 末尾に `-- ===== madb extension =====` セクション追加、6 新表 DDL |

## Files to NOT touch

- `src/etl/integrate_duckdb.py`

---

## SILVER スキーマ設計 (新規 6 表)

### `anime_broadcasters`
```sql
CREATE TABLE IF NOT EXISTS anime_broadcasters (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id            TEXT NOT NULL,             -- madb_id を SILVER anime.id へマップ
    broadcaster_name    TEXT NOT NULL,
    is_network_station  INTEGER,                   -- 0/1
    UNIQUE(anime_id, broadcaster_name)
);
CREATE INDEX IF NOT EXISTS idx_anime_broadcasters_anime ON anime_broadcasters(anime_id);
```
| BRONZE | SILVER | 備考 |
|--------|--------|------|
| `madb_id` | `anime_id` | madb_id がそのまま anime.id (mediaarts source) |
| `name` | `broadcaster_name` | |
| `is_network_station` | `is_network_station` | |

### `anime_broadcast_schedule`
```sql
CREATE TABLE IF NOT EXISTS anime_broadcast_schedule (
    anime_id  TEXT PRIMARY KEY,
    raw_text  TEXT NOT NULL
);
```

### `anime_production_committee`
```sql
CREATE TABLE IF NOT EXISTS anime_production_committee (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id      TEXT NOT NULL,
    company_name  TEXT NOT NULL,
    role_label    TEXT,
    UNIQUE(anime_id, company_name, role_label)
);
CREATE INDEX IF NOT EXISTS idx_apc_anime ON anime_production_committee(anime_id);
```

### `anime_production_companies`
```sql
CREATE TABLE IF NOT EXISTS anime_production_companies (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id      TEXT NOT NULL,
    company_name  TEXT NOT NULL,
    role_label    TEXT,
    is_main       INTEGER NOT NULL DEFAULT 0,
    UNIQUE(anime_id, company_name, role_label)
);
CREATE INDEX IF NOT EXISTS idx_apco_anime ON anime_production_companies(anime_id);
```

### `anime_video_releases`
```sql
CREATE TABLE IF NOT EXISTS anime_video_releases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    release_madb_id TEXT NOT NULL UNIQUE,    -- 各 release が一意 ID
    anime_id        TEXT,                    -- series_madb_id (NULL あり)
    media_format    TEXT,                    -- DVD / BD / etc.
    date_published  TEXT,
    publisher       TEXT,
    product_id      TEXT,
    gtin            TEXT,
    runtime_min     INTEGER,
    volume_number   TEXT,
    release_title   TEXT
);
CREATE INDEX IF NOT EXISTS idx_avr_anime ON anime_video_releases(anime_id);
```
注意: BRONZE `madb_id` = release 自身の ID、`series_madb_id` = anime ID。

### `anime_original_work_links`
```sql
CREATE TABLE IF NOT EXISTS anime_original_work_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id        TEXT NOT NULL,
    work_name       TEXT,
    creator_text    TEXT,
    series_link_id  TEXT,
    UNIQUE(anime_id, work_name)
);
CREATE INDEX IF NOT EXISTS idx_aow_anime ON anime_original_work_links(anime_id);
```

---

## Steps

### Step 1: schema.py 拡張
末尾の `_DDL` 文字列内に `-- ===== madb extension =====` コメント + 上記 6 DDL を追加。

```bash
# DDL 追加位置確認
grep -n "person_affiliations\|END OF DDL" src/db/schema.py | tail -5
```

### Step 2: `silver_loaders/madb.py` 実装

各 BRONZE → 各 SILVER に INSERT。重複は ON CONFLICT DO NOTHING で吸収。

```python
"""Mediaarts (madb) BRONZE → SILVER loaders.

Tables: anime_broadcasters / anime_broadcast_schedule /
anime_production_committee / anime_production_companies /
anime_video_releases / anime_original_work_links.
"""
from __future__ import annotations
from pathlib import Path
import duckdb


_BROADCASTERS_SQL = """
INSERT INTO anime_broadcasters (anime_id, broadcaster_name, is_network_station)
SELECT DISTINCT
    madb_id, name, TRY_CAST(is_network_station AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND name IS NOT NULL
ON CONFLICT (anime_id, broadcaster_name) DO NOTHING
"""

_BROADCAST_SCHEDULE_SQL = """
INSERT INTO anime_broadcast_schedule (anime_id, raw_text)
SELECT DISTINCT madb_id, raw_text
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
ON CONFLICT (anime_id) DO NOTHING
"""

_PROD_COMMITTEE_SQL = """
INSERT INTO anime_production_committee (anime_id, company_name, role_label)
SELECT DISTINCT madb_id, company_name, role_label
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND company_name IS NOT NULL
ON CONFLICT (anime_id, company_name, role_label) DO NOTHING
"""

_PROD_COMPANIES_SQL = """
INSERT INTO anime_production_companies (anime_id, company_name, role_label, is_main)
SELECT DISTINCT
    madb_id, company_name, role_label,
    COALESCE(TRY_CAST(is_main AS INTEGER), 0)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND company_name IS NOT NULL
ON CONFLICT (anime_id, company_name, role_label) DO NOTHING
"""

_VIDEO_RELEASES_SQL = """
INSERT INTO anime_video_releases
    (release_madb_id, anime_id, media_format, date_published, publisher,
     product_id, gtin, runtime_min, volume_number, release_title)
SELECT DISTINCT
    madb_id, series_madb_id, media_format, date_published, publisher,
    product_id, gtin,
    TRY_CAST(runtime_min AS INTEGER), volume_number, release_title
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
ON CONFLICT (release_madb_id) DO NOTHING
"""

_ORIGINAL_WORK_LINKS_SQL = """
INSERT INTO anime_original_work_links (anime_id, work_name, creator_text, series_link_id)
SELECT DISTINCT madb_id, work_name, creator_text, series_link_id
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
ON CONFLICT (anime_id, work_name) DO NOTHING
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    bronze_root = Path(bronze_root)
    def _g(table: str) -> str:
        return str(bronze_root / "source=mediaarts" / f"table={table}" / "date=*" / "*.parquet")

    counts: dict[str, int] = {}
    for label, sql, table in [
        ("broadcasters",         _BROADCASTERS_SQL,         "broadcasters"),
        ("broadcast_schedule",   _BROADCAST_SCHEDULE_SQL,   "broadcast_schedule"),
        ("production_committee", _PROD_COMMITTEE_SQL,       "production_committee"),
        ("production_companies", _PROD_COMPANIES_SQL,       "production_companies"),
        ("video_releases",       _VIDEO_RELEASES_SQL,       "video_releases"),
        ("original_work_links",  _ORIGINAL_WORK_LINKS_SQL,  "original_work_links"),
    ]:
        try:
            conn.execute(sql, [_g(table)])
        except Exception as exc:
            counts[f"{label}_error"] = str(exc)

    for silver_table in [
        "anime_broadcasters", "anime_broadcast_schedule",
        "anime_production_committee", "anime_production_companies",
        "anime_video_releases", "anime_original_work_links",
    ]:
        counts[silver_table] = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"
        ).fetchone()[0]
    return counts
```

### Step 3: Test
`tests/test_etl/test_silver_madb.py` — 合成 parquet で 6 表 INSERT 確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_madb.py

# 実 BRONZE で:
pixi run python -c "
import duckdb
from pathlib import Path
from src.etl.silver_loaders import madb
conn = duckdb.connect(':memory:')
# schema.py の DDL を流用
# ...
print(madb.integrate(conn, Path('result/bronze')))
"
# 期待: broadcasters / video_releases が大量、original_work_links 数千
```

---

## Stop-if

- [ ] BRONZE 6 parquet いずれか欠落
- [ ] 既存テスト失敗
- [ ] anime テーブル変更 (本カードでは触らない)

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/madb.py
rm tests/test_etl/test_silver_madb.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/02_madb_silver`

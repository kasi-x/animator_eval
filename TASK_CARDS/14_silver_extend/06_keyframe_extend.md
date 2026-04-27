# Task: keyframe SILVER 統合拡張

**ID**: `14_silver_extend/06_keyframe_extend`
**Priority**: 🟡
**Estimated changes**: 約 +300 / -0 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

keyframe BRONZE で SILVER 未統合な補助テーブルを統合:
- 既存統合済: `credits` (person credits) / `persons`
- 本カード対象: `person_studios` / `person_jobs` / `studios_master` / `anime_studios` / `settings_categories` / `preview` (display)

---

## Hard constraints

- **H1**: keyframe display 系列なし
- **H4**: credits は既存 source='keyframe' 統合済、touch しない

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=keyframe/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 10
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/keyframe.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_keyframe.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== keyframe extension =====` 追加

## Files to NOT touch

- `src/etl/integrate_duckdb.py`

---

## SILVER 設計

### `person_jobs` (新表)
```sql
CREATE TABLE IF NOT EXISTS person_jobs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    job       TEXT NOT NULL,
    source    TEXT NOT NULL DEFAULT 'keyframe',
    UNIQUE(person_id, job, source)
);
CREATE INDEX IF NOT EXISTS idx_person_jobs_person ON person_jobs(person_id);
```

### `person_affiliations` (既存 schema.py:249) を keyframe で拡張
keyframe `person_studios` → SILVER `person_affiliations` へ INSERT。
注意: 既存 schema は `(person_id, anime_id, studio_name)` PK = anime_id 必須。keyframe `person_studios` には anime_id 無し → **新表が必要**。

新表追加:
```sql
CREATE TABLE IF NOT EXISTS person_studio_affiliations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id   TEXT NOT NULL,
    studio_name TEXT NOT NULL,
    alt_names   TEXT,                 -- JSON array
    source      TEXT NOT NULL DEFAULT 'keyframe',
    UNIQUE(person_id, studio_name, source)
);
CREATE INDEX IF NOT EXISTS idx_psa_person ON person_studio_affiliations(person_id);
```

### `studios` 表に keyframe master データ追加
keyframe `studios_master (studio_id, name_ja, name_en)` → SILVER `studios` (id, name)。
ID prefix: `'kf:s' || studio_id`。

### `anime_studios` 既存表に keyframe 追加
keyframe `anime_studios (anime_id, studio_name, is_main)` → SILVER `anime_studios`。
注意: `studio_id` を name から逆引き or `'kf:n:' || studio_name` (name-based ID)。シンプル化のため name-based ID 採用:
```sql
INSERT INTO studios (id, name)
SELECT DISTINCT 'kf:n:' || studio_name, studio_name
FROM keyframe_anime_studios
ON CONFLICT (id) DO NOTHING;

INSERT INTO anime_studios (anime_id, studio_id, is_main)
SELECT anime_id, 'kf:n:' || studio_name, ...
```

### `anime_settings_categories` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_settings_categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id        TEXT NOT NULL,
    category_name   TEXT NOT NULL,
    category_order  INTEGER,
    UNIQUE(anime_id, category_name)
);
```

### `anime` ALTER 列 (keyframe 由来)
```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_uuid TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_status TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_slug TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_delimiters TEXT;        -- JSON
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_episode_delimiters TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_role_delimiters TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_staff_delimiters TEXT;
```
keyframe anime ID は既存 SILVER anime と一致 (anilist_id ベース) → ALTER で UPDATE。

### `persons` ALTER 列 (Card 04 と衝突注意)
keyframe `person_profile` の `bio` / `avatar` → 既存 SILVER `description` / `image_large` を UPDATE のみ (ALTER は Card 04)。

---

## Steps

### Step 1: schema.py 拡張

### Step 2: `silver_loaders/keyframe.py` 実装

```python
"""Keyframe BRONZE → SILVER extras."""
from __future__ import annotations
from pathlib import Path
import duckdb


def _g(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=keyframe" / f"table={table}" / "date=*" / "*.parquet")


_PERSON_JOBS_SQL = """
INSERT INTO person_jobs (person_id, job, source)
SELECT DISTINCT person_id, job, 'keyframe'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE person_id IS NOT NULL AND job IS NOT NULL
ON CONFLICT (person_id, job, source) DO NOTHING
"""

_PERSON_STUDIO_AFFILIATIONS_SQL = """
INSERT INTO person_studio_affiliations (person_id, studio_name, alt_names, source)
SELECT DISTINCT person_id, studio_name, alt_names, 'keyframe'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE person_id IS NOT NULL AND studio_name IS NOT NULL
ON CONFLICT (person_id, studio_name, source) DO NOTHING
"""

_STUDIOS_MASTER_SQL = """
INSERT INTO studios (id, name)
SELECT DISTINCT
    'kf:s' || CAST(studio_id AS VARCHAR),
    COALESCE(name_ja, name_en, '')
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE studio_id IS NOT NULL
ON CONFLICT (id) DO NOTHING
"""

_ANIME_STUDIOS_NAME_BASED_SQL_INSERT_STUDIO = """
INSERT INTO studios (id, name)
SELECT DISTINCT
    'kf:n:' || studio_name, studio_name
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE studio_name IS NOT NULL
ON CONFLICT (id) DO NOTHING
"""

_ANIME_STUDIOS_NAME_BASED_SQL_LINK = """
INSERT INTO anime_studios (anime_id, studio_id, is_main)
SELECT DISTINCT
    anime_id,
    'kf:n:' || studio_name,
    COALESCE(TRY_CAST(is_main AS INTEGER), 0)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_name IS NOT NULL
ON CONFLICT (anime_id, studio_id) DO NOTHING
"""

_SETTINGS_CATEGORIES_SQL = """
INSERT INTO anime_settings_categories (anime_id, category_name, category_order)
SELECT DISTINCT
    anime_id, category_name, TRY_CAST(category_order AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND category_name IS NOT NULL
ON CONFLICT (anime_id, category_name) DO NOTHING
"""

_ANIME_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
UPDATE anime SET
    kf_uuid                = bronze.kf_uuid,
    kf_status              = bronze.kf_status,
    kf_slug                = bronze.slug,
    kf_delimiters          = bronze.delimiters,
    kf_episode_delimiters  = bronze.episode_delimiters,
    kf_role_delimiters     = bronze.role_delimiters,
    kf_staff_delimiters    = bronze.staff_delimiters
FROM bronze
WHERE anime.id = bronze.id AND bronze._rn = 1
"""

_PERSONS_PROFILE_UPDATE_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE person_id IS NOT NULL AND COALESCE(is_studio, FALSE) = FALSE
)
UPDATE persons SET
    description = COALESCE(persons.description, bronze.bio),
    image_large = COALESCE(persons.image_large, bronze.avatar)
FROM bronze
WHERE persons.id = bronze.person_id AND bronze._rn = 1
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}
    pairs = [
        ("person_jobs",          _PERSON_JOBS_SQL),
        ("person_studios",       _PERSON_STUDIO_AFFILIATIONS_SQL),
        ("studios_master",       _STUDIOS_MASTER_SQL),
        ("anime_studios",        _ANIME_STUDIOS_NAME_BASED_SQL_INSERT_STUDIO),
        ("anime_studios",        _ANIME_STUDIOS_NAME_BASED_SQL_LINK),
        ("settings_categories",  _SETTINGS_CATEGORIES_SQL),
        ("anime",                _ANIME_EXTRAS_SQL),
        ("person_profile",       _PERSONS_PROFILE_UPDATE_SQL),
    ]
    for table, sql in pairs:
        try:
            conn.execute(sql, [_g(bronze_root, table)])
        except Exception as exc:
            counts[f"{table}_error"] = str(exc)
    counts["person_jobs"] = conn.execute("SELECT COUNT(*) FROM person_jobs").fetchone()[0]
    counts["person_studio_affiliations"] = conn.execute(
        "SELECT COUNT(*) FROM person_studio_affiliations"
    ).fetchone()[0]
    counts["anime_settings_categories"] = conn.execute(
        "SELECT COUNT(*) FROM anime_settings_categories"
    ).fetchone()[0]
    return counts
```

### Step 3: Test

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_keyframe.py
```

---

## Stop-if

- [ ] BRONZE 欠落
- [ ] anime_studios で uniqueness violation (既存 anilist データと name-based ID が衝突 — kf prefix 必須)

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/keyframe.py
rm tests/test_etl/test_silver_keyframe.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/06_keyframe_extend`

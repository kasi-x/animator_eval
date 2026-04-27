# Task: seesaawiki SILVER 統合拡張

**ID**: `14_silver_extend/04_seesaawiki_extend`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: yes (anime_studios の seesaawiki/anilist 衝突解決、gross_studios 表設計)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

seesaawiki BRONZE 9 表のうち SILVER 未統合な属性を統合:
- **既存統合済**: anime / credits / persons (基礎)
- **本カード対象**: studios / anime_studios / theme_songs / episode_titles / gross_studios / production_committee / original_work_info / persons (拡張列)

---

## Hard constraints

- **H1**: seesaawiki に rating 系なし、特になし
- **H4**: credits.evidence_source 維持 (既存 source='seesaawiki' 統合済、touch しない)
- **既存 anime_studios**: anilist + seesaawiki が同表に書き込む → ON CONFLICT で吸収

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=seesaawiki/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 9
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/seesaawiki.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_seesaawiki.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== seesaawiki extension =====` 追加

## Files to NOT touch

- `src/etl/integrate_duckdb.py`
- `src/scrapers/parsers/seesaawiki.py` (parser 不変)

---

## SILVER 設計

### `anime_theme_songs` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_theme_songs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id    TEXT NOT NULL,
    song_type   TEXT,           -- 'OP' / 'ED' / 'IN' (BRONZE: song_type)
    song_title  TEXT,
    role        TEXT,           -- 作詞 / 作曲 / 編曲 / 歌
    name        TEXT,           -- アーティスト名等
    UNIQUE(anime_id, song_type, song_title, role, name)
);
CREATE INDEX IF NOT EXISTS idx_ats_anime ON anime_theme_songs(anime_id);
```

### `anime_episode_titles` (新表) — Card 03 の `anime_episodes` とは別
```sql
CREATE TABLE IF NOT EXISTS anime_episode_titles (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id  TEXT NOT NULL,
    episode   INTEGER,
    title     TEXT,
    source    TEXT NOT NULL DEFAULT 'seesaawiki',
    UNIQUE(anime_id, episode, source)
);
```

### `anime_gross_studios` (新表) — gross 請けスタジオ専用
```sql
CREATE TABLE IF NOT EXISTS anime_gross_studios (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id     TEXT NOT NULL,
    studio_name  TEXT NOT NULL,
    episode      INTEGER,
    UNIQUE(anime_id, studio_name, episode)
);
CREATE INDEX IF NOT EXISTS idx_ags_anime ON anime_gross_studios(anime_id);
```

### `anime_production_committee` (Card 02 で作成済 → 共有)
seesaawiki BRONZE `production_committee (anime_id, member_name)` は Card 02 と同表へ。`role_label` は NULL。
重複時は ON CONFLICT で吸収。

```sql
-- 既存 (Card 02 で定義) を再利用
INSERT INTO anime_production_committee (anime_id, company_name, role_label)
SELECT DISTINCT anime_id, member_name, NULL
FROM ... WHERE source='seesaawiki' ...
```

注意: Card 02 と同名表 → schema.py DDL は **Card 02 が定義、本カードは DDL 追加しない**。Card 02/04 のどちらが先に走ってもこの表に行追加できるように。Card 02 と本カード並列走行のため、本カードは `CREATE TABLE IF NOT EXISTS` で重複定義しても OK。

### `anime_original_work_info` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_original_work_info (
    anime_id            TEXT PRIMARY KEY,
    author              TEXT,
    publisher           TEXT,
    label               TEXT,
    magazine            TEXT,
    serialization_type  TEXT
);
```

### `studios` / `anime_studios` (既存 schema.py、INSERT のみ)
seesaawiki `studios` BRONZE: `id, name, anilist_id, is_animation_studio, country_of_origin, favourites, site_url`
→ 既存 SILVER `studios` 表と完全一致 = 既存 `_STUDIOS_SQL` パターンで INSERT。ON CONFLICT (id) DO NOTHING。

`anime_studios` も既存 SILVER と一致。

### `persons` ALTER 列 (Card 03 と衝突に注意)
seesaawiki persons BRONZE 列で SILVER 不在のもの:
- `name_native_raw`、`aliases`、`nationality`、`primary_occupations`、`years_active`、`hometown`、`description`、`image_large` / `image_medium`

`hometown` は Card 03 と衝突 → **本カードは ALTER せず、UPDATE のみ**。Card 03 が ALTER。
他の列は本カードで ALTER:

```sql
ALTER TABLE persons ADD COLUMN IF NOT EXISTS name_native_raw TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS aliases TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS nationality TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS primary_occupations TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS years_active TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_large TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_medium TEXT;
```

注意: `description` / `image_large` / `image_medium` は他 source でも入りうる → 本カードで ALTER、他カードは UPDATE。

---

## Steps

### Step 1: schema.py 拡張
`-- ===== seesaawiki extension =====` セクション追加 + 上記 DDL。

### Step 2: `silver_loaders/seesaawiki.py` 実装

```python
"""SeesaaWiki BRONZE → SILVER extras."""
from __future__ import annotations
from pathlib import Path
import duckdb


def _g(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=seesaawiki" / f"table={table}" / "date=*" / "*.parquet")


_STUDIOS_SQL = """
INSERT INTO studios (id, name, anilist_id, is_animation_studio, country_of_origin, favourites, site_url)
SELECT DISTINCT
    id, COALESCE(name, ''),
    TRY_CAST(anilist_id AS INTEGER),
    TRY_CAST(is_animation_studio AS INTEGER),
    country_of_origin,
    TRY_CAST(favourites AS INTEGER),
    site_url
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE id IS NOT NULL
ON CONFLICT (id) DO NOTHING
"""

_ANIME_STUDIOS_SQL = """
INSERT INTO anime_studios (anime_id, studio_id, is_main)
SELECT DISTINCT
    anime_id, studio_id,
    COALESCE(TRY_CAST(is_main AS INTEGER), 0)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_id IS NOT NULL
ON CONFLICT (anime_id, studio_id) DO NOTHING
"""

_THEME_SONGS_SQL = """
INSERT INTO anime_theme_songs (anime_id, song_type, song_title, role, name)
SELECT DISTINCT anime_id, song_type, song_title, role, name
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL
ON CONFLICT (anime_id, song_type, song_title, role, name) DO NOTHING
"""

_EPISODE_TITLES_SQL = """
INSERT INTO anime_episode_titles (anime_id, episode, title, source)
SELECT DISTINCT anime_id, TRY_CAST(episode AS INTEGER), title, 'seesaawiki'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL
ON CONFLICT (anime_id, episode, source) DO NOTHING
"""

_GROSS_STUDIOS_SQL = """
INSERT INTO anime_gross_studios (anime_id, studio_name, episode)
SELECT DISTINCT anime_id, studio_name, TRY_CAST(episode AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_name IS NOT NULL
ON CONFLICT (anime_id, studio_name, episode) DO NOTHING
"""

_PRODUCTION_COMMITTEE_SQL = """
INSERT INTO anime_production_committee (anime_id, company_name, role_label)
SELECT DISTINCT anime_id, member_name, NULL
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND member_name IS NOT NULL
ON CONFLICT (anime_id, company_name, role_label) DO NOTHING
"""

_ORIGINAL_WORK_INFO_SQL = """
INSERT INTO anime_original_work_info (anime_id, author, publisher, label, magazine, serialization_type)
SELECT DISTINCT anime_id, author, publisher, label, magazine, serialization_type
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL
ON CONFLICT (anime_id) DO NOTHING
"""

_PERSONS_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
UPDATE persons SET
    name_native_raw     = COALESCE(persons.name_native_raw, bronze.name_native_raw),
    aliases             = COALESCE(persons.aliases, bronze.aliases),
    nationality         = COALESCE(persons.nationality, bronze.nationality),
    primary_occupations = COALESCE(persons.primary_occupations, bronze.primary_occupations),
    years_active        = COALESCE(persons.years_active, bronze.years_active),
    hometown            = COALESCE(persons.hometown, bronze.hometown),
    description         = COALESCE(persons.description, bronze.description),
    image_large         = COALESCE(persons.image_large, bronze.image_large),
    image_medium        = COALESCE(persons.image_medium, bronze.image_medium)
FROM bronze
WHERE persons.id = bronze.id AND bronze._rn = 1
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}
    pairs = [
        ("studios",              _STUDIOS_SQL),
        ("anime_studios",        _ANIME_STUDIOS_SQL),
        ("theme_songs",          _THEME_SONGS_SQL),
        ("episode_titles",       _EPISODE_TITLES_SQL),
        ("gross_studios",        _GROSS_STUDIOS_SQL),
        ("production_committee", _PRODUCTION_COMMITTEE_SQL),
        ("original_work_info",   _ORIGINAL_WORK_INFO_SQL),
        ("persons",              _PERSONS_EXTRAS_SQL),
    ]
    for table, sql in pairs:
        try:
            conn.execute(sql, [_g(bronze_root, table)])
        except Exception as exc:
            counts[f"{table}_error"] = str(exc)
    for st in ["anime_theme_songs", "anime_episode_titles", "anime_gross_studios",
               "anime_original_work_info"]:
        counts[st] = conn.execute(f"SELECT COUNT(*) FROM {st}").fetchone()[0]
    return counts
```

### Step 3: Test
合成 seesaawiki parquet で 8 関数確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_seesaawiki.py
```

---

## Stop-if

- [ ] BRONZE 9 parquet 欠落
- [ ] 既存 anime_studios / studios で uniqueness violation (既存 anilist データと衝突)

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/seesaawiki.py
rm tests/test_etl/test_silver_seesaawiki.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/04_seesaawiki_extend`

# Task: AniList SILVER 統合拡張 (characters / CVA / anime 拡張列)

**ID**: `14_silver_extend/01_anilist_extend`
**Priority**: 🟠
**Estimated changes**: 約 +250 / -0 lines, 3 files (新規 loader / 新規 test / schema.py 追加)
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

BRONZE `result/bronze/source=anilist/table={characters,character_voice_actors,anime}` を SILVER に統合し、現行 SILVER で取りこぼしている属性を最大限に保持する。

---

## Hard constraints

- **H1**: `score` / `popularity` / `favourites` / `mean_score` / `popularity_rank` は SILVER scoring 列に **入れない**。display 用は `display_*` prefix で隔離
- **H3**: entity_resolution ロジック不変
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE 存在確認: `ls result/bronze/source=anilist/table=characters/date=*/`
- [ ] BRONZE 存在確認: `ls result/bronze/source=anilist/table=character_voice_actors/date=*/`
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/__init__.py` | 空 (他カードと共有、競合時は merge) |
| `src/etl/silver_loaders/anilist.py` | `integrate_anilist_extras(conn, bronze_root)` 関数 |
| `tests/test_etl/test_silver_anilist.py` | 単体テスト |

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | 末尾 `-- ===== anilist extension =====` セクションに anime 拡張列追加 (display_*, structural) |

## Files to NOT touch

- `src/etl/integrate_duckdb.py` (dispatcher 統合は別タスク)
- 他カードの schema.py セクション

---

## SILVER スキーマ設計

### `characters` テーブル (DDL は `schema.py:213` 既存、loader 未実装)
BRONZE 列 → SILVER 列マッピング:

| BRONZE | SILVER | 備考 |
|--------|--------|------|
| `id` | `id` | PK |
| `name_ja` | `name_ja` | |
| `name_en` | `name_en` | |
| `aliases` | `aliases` | JSON 配列 |
| `anilist_id` | `anilist_id` | int、UNIQUE |
| `image_large` / `image_medium` | 同名 | display 用 |
| `description` | `description` | |
| `gender` / `date_of_birth` / `age` / `blood_type` | 同名 | |
| `favourites` | `favourites` | display 用 (H1: scoring 不参入) |
| `site_url` | `site_url` | |

### `character_voice_actors` テーブル (DDL `schema.py:232` 既存)
| BRONZE | SILVER | 備考 |
|--------|--------|------|
| `character_id` | `character_id` | FK characters.id |
| `person_id` | `person_id` | FK persons.id |
| `anime_id` | `anime_id` | FK anime.id |
| `character_role` | `character_role` | "MAIN" / "SUPPORTING" |
| `source` | `source` | = 'anilist' |

### `anime` 拡張列 (`schema.py` 末尾 `-- ===== anilist extension =====` 追加)
新規列:

| 列 | 型 | BRONZE 出典 | 備考 |
|----|----|-----------|------|
| `synonyms` | TEXT | `synonyms` | JSON 配列 |
| `country_of_origin` | TEXT | `country_of_origin` | "JP" 等 |
| `is_licensed` | INTEGER | `is_licensed` | 0/1 |
| `is_adult` | INTEGER | `is_adult` | 0/1 |
| `hashtag` | TEXT | `hashtag` | |
| `site_url` | TEXT | `site_url` | |
| `trailer_url` | TEXT | `trailer_url` | |
| `trailer_site` | TEXT | `trailer_site` | |
| `description` | TEXT | `description` | display 用 |
| `cover_large` | TEXT | `cover_large` | display |
| `cover_extra_large` | TEXT | `cover_extra_large` | display |
| `cover_medium` | TEXT | `cover_medium` | display |
| `banner` | TEXT | `banner` | display |
| `external_links_json` | TEXT | `external_links_json` | JSON |
| `airing_schedule_json` | TEXT | `airing_schedule_json` | JSON |
| `relations_json` | TEXT | `relations_json` | JSON |
| `display_score` | REAL | `score` | H1: display のみ |
| `display_mean_score` | REAL | `mean_score` | H1: display のみ |
| `display_favourites` | INTEGER | `favourites` | H1: display のみ |
| `display_popularity_rank` | INTEGER | `popularity_rank` | H1: display のみ |
| `display_rankings_json` | TEXT | `rankings_json` | H1: display のみ |

注意: **`score` / `popularity` / `favourites` を bare 名で SILVER に入れない**。`display_` prefix 必須。

---

## Steps

### Step 1: schema.py 末尾追加

```bash
grep -n "CREATE TABLE IF NOT EXISTS character_voice_actors" src/db/schema.py
# 既存 DDL 確認
tail -50 src/db/schema.py
# 末尾に追記する位置を特定
```

`schema.py` 末尾に以下を追加 (ALTER TABLE で列追加):

```python
        -- ===== anilist extension (Card 14/01) =====
        -- anime 拡張列 (display 系は display_* prefix で H1 隔離)
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS synonyms TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS country_of_origin TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS is_licensed INTEGER;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS is_adult INTEGER;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS hashtag TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS site_url TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS trailer_url TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS trailer_site TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS description TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_large TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_extra_large TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_medium TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS banner TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS external_links_json TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS airing_schedule_json TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS relations_json TEXT;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score REAL;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_mean_score REAL;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favourites INTEGER;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_popularity_rank INTEGER;
        ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rankings_json TEXT;
```

注意: SQLite/DuckDB の `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 構文は両方とも DuckDB は `ADD COLUMN IF NOT EXISTS`、SQLite は `IF NOT EXISTS` 不可。`schema.py` がどちら向け DDL を生成するか確認すること:

```bash
grep -n "duckdb\|sqlite" src/db/schema.py | head -10
```

DuckDB-only なら現行 DDL のまま、両対応なら `try/except` で囲む既存パターンを踏襲。

### Step 2: `silver_loaders/__init__.py` 作成

```python
"""SILVER loader modules — one per source.

Each loader exposes `integrate(conn, bronze_root)` that adds rows to
existing SILVER tables. Composed by integrate_duckdb.py at the top level
(integration is out of scope for Card 14).
"""
```

### Step 3: `silver_loaders/anilist.py` 実装

`src/etl/silver_loaders/anilist.py` 新規:

```python
"""AniList BRONZE → SILVER extra loaders.

Tables loaded:
- characters    (BRONZE: source=anilist/table=characters)
- character_voice_actors (BRONZE: source=anilist/table=character_voice_actors)
- anime extras  (ALTER 列群: synonyms / country_of_origin / display_*)

DDL is in src/db/schema.py "anilist extension" section.
"""
from __future__ import annotations

from pathlib import Path
import duckdb


_CHARACTERS_SQL = """
INSERT INTO characters
SELECT
    id,
    COALESCE(name_ja, '')   AS name_ja,
    COALESCE(name_en, '')   AS name_en,
    COALESCE(aliases, '[]') AS aliases,
    anilist_id,
    image_large,
    image_medium,
    description,
    gender,
    date_of_birth,
    age,
    blood_type,
    favourites,
    site_url,
    now()                   AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

_CVA_SQL = """
INSERT INTO character_voice_actors (character_id, person_id, anime_id, character_role, source)
SELECT DISTINCT
    character_id,
    person_id,
    anime_id,
    COALESCE(character_role, '') AS character_role,
    'anilist'                    AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE character_id IS NOT NULL
  AND person_id IS NOT NULL
  AND anime_id IS NOT NULL
ON CONFLICT (character_id, person_id, anime_id) DO NOTHING
"""

_ANIME_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
UPDATE anime SET
    synonyms                = bronze.synonyms,
    country_of_origin       = bronze.country_of_origin,
    is_licensed             = TRY_CAST(bronze.is_licensed AS INTEGER),
    is_adult                = TRY_CAST(bronze.is_adult AS INTEGER),
    hashtag                 = bronze.hashtag,
    site_url                = bronze.site_url,
    trailer_url             = bronze.trailer_url,
    trailer_site            = bronze.trailer_site,
    description             = bronze.description,
    cover_large             = bronze.cover_large,
    cover_extra_large       = bronze.cover_extra_large,
    cover_medium            = bronze.cover_medium,
    banner                  = bronze.banner,
    external_links_json     = bronze.external_links_json,
    airing_schedule_json    = bronze.airing_schedule_json,
    relations_json          = bronze.relations_json,
    display_score           = TRY_CAST(bronze.score AS REAL),
    display_mean_score      = TRY_CAST(bronze.mean_score AS REAL),
    display_favourites      = TRY_CAST(bronze.favourites AS INTEGER),
    display_popularity_rank = TRY_CAST(bronze.popularity_rank AS INTEGER),
    display_rankings_json   = bronze.rankings_json
FROM bronze
WHERE anime.id = bronze.id AND bronze._rn = 1
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    """Load AniList characters / CVA / anime extras into SILVER."""
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}

    chars_glob = str(bronze_root / "source=anilist" / "table=characters" / "date=*" / "*.parquet")
    cva_glob   = str(bronze_root / "source=anilist" / "table=character_voice_actors" / "date=*" / "*.parquet")
    anime_glob = str(bronze_root / "source=anilist" / "table=anime" / "date=*" / "*.parquet")

    conn.execute(_CHARACTERS_SQL, [chars_glob])
    counts["characters"] = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]

    conn.execute(_CVA_SQL, [cva_glob])
    counts["character_voice_actors"] = conn.execute(
        "SELECT COUNT(*) FROM character_voice_actors"
    ).fetchone()[0]

    conn.execute(_ANIME_EXTRAS_SQL, [anime_glob])
    counts["anime_extras_updated"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE description IS NOT NULL"
    ).fetchone()[0]

    return counts
```

### Step 4: 単体テスト

`tests/test_etl/test_silver_anilist.py` 新規 — 合成 parquet を temp dir に作って loader 走らせ、row count 確認。既存 `tests/test_integrate_duckdb.py` のパターン踏襲。

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. テスト (本カード)
pixi run test-scoped tests/test_etl/test_silver_anilist.py

# 3. 動作確認 (実 BRONZE で)
pixi run python -c "
import duckdb
from pathlib import Path
from src.etl.silver_loaders import anilist
conn = duckdb.connect(':memory:')
# DDL 適用 (integrate_duckdb._DDL を流用 or schema.py から取得)
# ...
print(anilist.integrate(conn, Path('result/bronze')))
"

# 4. invariant
rg 'anime\.score\b' src/etl/silver_loaders/anilist.py   # 0 件
rg '\bscore\b' src/etl/silver_loaders/anilist.py | rg -v 'display_'   # 0 件
```

期待出力:
- `characters`: > 100,000 rows
- `character_voice_actors`: > 200,000 rows
- `anime_extras_updated`: anime テーブル全行更新

---

## Stop-if conditions

- [ ] BRONZE parquet 存在しない → 中断
- [ ] `pixi run test` 既存テスト失敗
- [ ] schema.py の anime テーブル DDL に `score` / `popularity` / `favourites` が **prefix なし**で混入

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/anilist.py
rm tests/test_etl/test_silver_anilist.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] DONE: `14_silver_extend/01_anilist_extend`

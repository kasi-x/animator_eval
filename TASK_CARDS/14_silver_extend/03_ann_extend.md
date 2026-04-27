# Task: ANN SILVER 統合拡張 (cast / company / episodes / releases / news / related / anime 拡張列)

**ID**: `14_silver_extend/03_ann_extend`
**Priority**: 🟠
**Estimated changes**: 約 +500 / -0 lines, 3 files
**Requires senior judgment**: yes (ANN ID prefix 設計、display rating の隔離)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

ANN BRONZE 9 表のうち SILVER 未統合な 6 表を統合 + anime / persons の拡張列回収。
- 既存統合済: anime / credits / persons (基礎列のみ)
- 本カード対象: `cast` (250,048) / `company` (33,857) / `episodes` (169,115) / `releases` (22,603) / `news` (90,234) / `related` (15,198) + anime / persons の漏れ列

---

## Hard constraints

- **H1**: `display_rating_votes` / `display_rating_weighted` / `display_rating_bayesian` は **scoring 不参入**。SILVER 列名も `display_rating_*` prefix で隔離
- **H3**: entity_resolution 不変
- **ANN ID prefix**: persons = `'ann:p<id>'`、anime = `'ann:a<id>'`、character = `'ann:c<id>'` (既存規約踏襲: `integrate_duckdb._build_credits_insert_ann`)

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=ann/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 9
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/ann.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_ann.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== ann extension =====` section、新表 + ALTER 列追加

## Files to NOT touch

- `src/etl/integrate_duckdb.py`

---

## SILVER 設計

### `anime` ALTER 列追加 (display rating 系は H1 隔離)

```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS themes TEXT;            -- ; 区切り
ALTER TABLE anime ADD COLUMN IF NOT EXISTS plot_summary TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS running_time_raw TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS objectionable_content TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS opening_themes_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS ending_themes_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS insert_songs_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS official_websites_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS vintage_raw TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_votes INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_weighted REAL;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_bayesian REAL;
```

注意: 14/01 (anilist) と列名衝突の可能性 = `themes` / `plot_summary` 等は他カードに無いので OK。**列名重複は禁止**、衝突したら本カード優先で merge。

### `persons` ALTER 列追加
```sql
ALTER TABLE persons ADD COLUMN IF NOT EXISTS gender TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS height_raw TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS family_name_ja TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS given_name_ja TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS hometown TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_url_ann TEXT;
```
注意: `hometown` は他 source (anilist) でも追加候補 = 列名重複時は **`hometown` を本カード追加とし、他カードは UPDATE のみ**。

### `anime_episodes` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_episodes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id    TEXT NOT NULL,
    episode_num INTEGER,
    lang        TEXT,
    title       TEXT,
    aired_date  TEXT,
    UNIQUE(anime_id, episode_num, lang)
);
CREATE INDEX IF NOT EXISTS idx_anime_episodes_anime ON anime_episodes(anime_id);
```

### `anime_companies` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_companies (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id      TEXT NOT NULL,
    company_name  TEXT NOT NULL,
    task          TEXT,
    company_id    TEXT,         -- ann company_id (string)
    source        TEXT NOT NULL DEFAULT 'ann',
    UNIQUE(anime_id, company_name, task)
);
CREATE INDEX IF NOT EXISTS idx_anime_companies_anime ON anime_companies(anime_id);
```

### `anime_releases` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_releases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id        TEXT NOT NULL,
    product_title   TEXT,
    release_date    TEXT,
    href            TEXT,
    region          TEXT,
    source          TEXT NOT NULL DEFAULT 'ann',
    UNIQUE(anime_id, product_title, release_date)
);
CREATE INDEX IF NOT EXISTS idx_anime_releases_anime ON anime_releases(anime_id);
```

### `anime_news` (新表)
```sql
CREATE TABLE IF NOT EXISTS anime_news (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id  TEXT NOT NULL,
    datetime  TEXT,
    title     TEXT,
    href      TEXT,
    source    TEXT NOT NULL DEFAULT 'ann',
    UNIQUE(anime_id, href)
);
CREATE INDEX IF NOT EXISTS idx_anime_news_anime ON anime_news(anime_id);
```

### `anime_relations` (既存 schema.py:199、ANN data で拡張)
既存 SILVER `anime_relations` に ANN `related` を INSERT。`relation_type` = `rel`、`related_anime_id` = `'ann:a' || target_ann_id`。

### `character_voice_actors` (既存 schema.py:232、ANN cast で追加)
- `character_id` = `'ann:c' || character_id` (BRONZE の character_id)
- `person_id` = `'ann:p' || ann_person_id`
- `anime_id` = `'ann:a' || ann_anime_id`
- `character_role` = `cast_role`
- `source` = `'ann'`

---

## Steps

### Step 1: schema.py 拡張
末尾に `-- ===== ann extension =====` セクション追加、ALTER 列 + 5 新表 DDL 追加。`anime_relations` / `character_voice_actors` は既存 = ALTER 不要。

### Step 2: `silver_loaders/ann.py` 実装

各 BRONZE → SILVER 関数を 8 個 (anime_extras, persons_extras, episodes, companies, releases, news, related, cast)。

雛形:
```python
"""ANN BRONZE → SILVER extras."""
from __future__ import annotations
from pathlib import Path
import duckdb


def _g(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=ann" / f"table={table}" / "date=*" / "*.parquet")


_ANIME_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY 'ann:a' || CAST(ann_id AS VARCHAR) ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
)
UPDATE anime SET
    themes                  = bronze.themes,
    plot_summary            = bronze.plot_summary,
    running_time_raw        = bronze.running_time_raw,
    objectionable_content   = bronze.objectionable_content,
    opening_themes_json     = bronze.opening_themes_json,
    ending_themes_json      = bronze.ending_themes_json,
    insert_songs_json       = bronze.insert_songs_json,
    official_websites_json  = bronze.official_websites_json,
    vintage_raw             = bronze.vintage_raw,
    image_url               = bronze.image_url,
    display_rating_votes    = TRY_CAST(bronze.display_rating_votes AS INTEGER),
    display_rating_weighted = TRY_CAST(bronze.display_rating_weighted AS REAL),
    display_rating_bayesian = TRY_CAST(bronze.display_rating_bayesian AS REAL)
FROM bronze
WHERE anime.id = ('ann:a' || CAST(bronze.ann_id AS VARCHAR)) AND bronze._rn = 1
"""

_EPISODES_SQL = """
INSERT INTO anime_episodes (anime_id, episode_num, lang, title, aired_date)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR), episode_num, lang, title, aired_date
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
ON CONFLICT (anime_id, episode_num, lang) DO NOTHING
"""

_COMPANIES_SQL = """
INSERT INTO anime_companies (anime_id, company_name, task, company_id, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    company_name, task,
    CAST(company_id AS VARCHAR), 'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL AND company_name IS NOT NULL
ON CONFLICT (anime_id, company_name, task) DO NOTHING
"""

_RELEASES_SQL = """
INSERT INTO anime_releases (anime_id, product_title, release_date, href, region, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    product_title, release_date, href, region, 'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
ON CONFLICT (anime_id, product_title, release_date) DO NOTHING
"""

_NEWS_SQL = """
INSERT INTO anime_news (anime_id, datetime, title, href, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    datetime, title, href, 'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL AND href IS NOT NULL
ON CONFLICT (anime_id, href) DO NOTHING
"""

_RELATED_SQL = """
INSERT INTO anime_relations (anime_id, related_anime_id, relation_type, related_title, related_format)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    'ann:a' || CAST(target_ann_id AS VARCHAR),
    COALESCE(rel, ''), '', NULL
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL AND target_ann_id IS NOT NULL
ON CONFLICT (anime_id, related_anime_id, relation_type) DO NOTHING
"""

_CAST_SQL = """
INSERT INTO character_voice_actors (character_id, person_id, anime_id, character_role, source)
SELECT DISTINCT
    'ann:c' || CAST(character_id AS VARCHAR),
    'ann:p' || CAST(ann_person_id AS VARCHAR),
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    COALESCE(cast_role, ''), 'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_person_id IS NOT NULL
  AND ann_anime_id IS NOT NULL
  AND character_id IS NOT NULL
ON CONFLICT (character_id, person_id, anime_id) DO NOTHING
"""

_PERSONS_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY 'ann:p' || CAST(ann_id AS VARCHAR) ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
)
UPDATE persons SET
    gender         = bronze.gender,
    height_raw     = bronze.height_raw,
    family_name_ja = bronze.family_name_ja,
    given_name_ja  = bronze.given_name_ja,
    hometown       = bronze.hometown,
    image_url_ann  = bronze.image_url
FROM bronze
WHERE persons.id = ('ann:p' || CAST(bronze.ann_id AS VARCHAR)) AND bronze._rn = 1
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}
    pairs = [
        ("anime",     _ANIME_EXTRAS_SQL),
        ("persons",   _PERSONS_EXTRAS_SQL),
        ("episodes",  _EPISODES_SQL),
        ("company",   _COMPANIES_SQL),
        ("releases",  _RELEASES_SQL),
        ("news",      _NEWS_SQL),
        ("related",   _RELATED_SQL),
        ("cast",      _CAST_SQL),
    ]
    for table, sql in pairs:
        try:
            conn.execute(sql, [_g(bronze_root, table)])
        except Exception as exc:
            counts[f"{table}_error"] = str(exc)
    for st in ["anime_episodes", "anime_companies", "anime_releases", "anime_news"]:
        counts[st] = conn.execute(f"SELECT COUNT(*) FROM {st}").fetchone()[0]
    return counts
```

### Step 3: Test
合成 ANN parquet で 8 関数の動作確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_ann.py

# Hard rule check:
rg 'rating_votes\b|rating_weighted\b|rating_bayesian\b' src/etl/silver_loaders/ann.py | rg -v 'display_'   # 0 件
```

---

## Stop-if

- [ ] BRONZE 9 parquet いずれか欠落
- [ ] 列名衝突 (他カードと同名で異定義) 発生 → 中断、ユーザに報告

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/ann.py
rm tests/test_etl/test_silver_ann.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/03_ann_extend`

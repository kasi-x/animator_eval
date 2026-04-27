# Task: bangumi.tv SILVER 統合 (6 BRONZE → SILVER)

**ID**: `14_silver_extend/05_bangumi_silver`
**Priority**: 🟠
**Estimated changes**: 約 +450 / -0 lines, 3 files
**Requires senior judgment**: yes (bangumi subject_id ↔ silver anime_id マッピング戦略、cross-source ID design)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

bangumi.tv BRONZE 6 表を SILVER に統合。subject (anime) / persons / characters / 関係 3 表。

---

## Hard constraints

- **H1**: bangumi `score` / `score_details` / `rank` は **scoring 不参入**。SILVER 列名 `display_*` prefix
- **H3**: entity_resolution 不変
- **H4**: `subject_persons` → SILVER credits は `evidence_source='bangumi'`
- **bangumi ID prefix**: `'bgm:s<id>'` (subject), `'bgm:p<id>'` (person), `'bgm:c<id>'` (character)

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=bangumi/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 6
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/bangumi.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_bangumi.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== bangumi extension =====` セクション追加

## Files to NOT touch

- `src/etl/integrate_duckdb.py`

---

## SILVER 設計

### `anime` テーブルへの bangumi subject 追加

bangumi `subjects` (type=2 anime のみ) → SILVER `anime` に挿入。`id = 'bgm:s' || subject_id`。

ALTER 列追加 (BRONZE bangumi が持つ追加情報):
```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS infobox_json TEXT;        -- bangumi infobox raw
ALTER TABLE anime ADD COLUMN IF NOT EXISTS platform TEXT;            -- TV/OVA/Movie 等
ALTER TABLE anime ADD COLUMN IF NOT EXISTS meta_tags_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_flag INTEGER;      -- 0/1
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_bgm REAL;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_details_json TEXT;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rank_bgm INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favorite_bgm INTEGER;
```

注意: `name_cn` は title_alt JSON へ統合 (既存 `titles_alt` 列 ON 列追加なし)。

### `persons` テーブルへの bangumi person 追加

bangumi `persons` → SILVER `persons` に挿入。`id = 'bgm:p' || person_id`。

ALTER 列 (Card 04 と衝突注意):
```sql
ALTER TABLE persons ADD COLUMN IF NOT EXISTS career_json TEXT;       -- bangumi career array
ALTER TABLE persons ADD COLUMN IF NOT EXISTS infobox_json TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS summary_bgm TEXT;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS bgm_id INTEGER;
ALTER TABLE persons ADD COLUMN IF NOT EXISTS person_type INTEGER;    -- 1=person, 2=company etc.
```

注意: `gender` / `blood_type` は Card 03 (ann) で ALTER。本カードは UPDATE のみ。
注意: `birth_year` / `birth_mon` / `birth_day` を結合して `birth_date` 既存列に UPDATE (NULL 時のみ)。

### `characters` テーブルへの bangumi character 追加

bangumi `characters` → SILVER `characters` (既存 schema.py:213) に挿入。`id = 'bgm:c' || character_id`。

ALTER 列:
```sql
ALTER TABLE characters ADD COLUMN IF NOT EXISTS infobox_json TEXT;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS summary_bgm TEXT;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS bgm_id INTEGER;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS character_type INTEGER;
ALTER TABLE characters ADD COLUMN IF NOT EXISTS images_json TEXT;
```

### `subject_persons` → SILVER `credits` に挿入
- person_id = `'bgm:p<id>'`
- anime_id = `'bgm:s<id>'`
- role = `position` を `map_role("bangumi", position)` で正規化 (要 src/etl/role_mappers/bangumi.py 既存確認)
- raw_role = `position`
- evidence_source = `'bangumi'`
- episode = `eps` (range 文字列なら NULL、単一なら int)

### `subject_characters` + `person_characters` → `character_voice_actors`
- character_id = `'bgm:c<id>'`
- person_id = `'bgm:p<id>'`
- anime_id = `'bgm:s<id>'`
- character_role = `character.relation` (主役/配役) — `subject_characters` から取得、`person_characters` で actor 補完
- source = `'bangumi'`

実装は `subject_characters` で character の relation を取り、`person_characters` で actor person を取得して JOIN。

---

## Steps

### Step 1: schema.py 拡張
`-- ===== bangumi extension =====` セクションに ALTER 列群追加。

### Step 2: role_mapper 確認

```bash
ls src/etl/role_mappers/
grep -rn "bangumi" src/etl/role_mappers/ | head
```

存在しない場合は最小実装 = `position` (整数 or 文字列) → `Role.value` のマッピング辞書。bangumi 公式 50+ codes は `src/utils/role_groups.py` の `ANIME_POSITION_LABELS` 参照。

### Step 3: `silver_loaders/bangumi.py` 実装

雛形:
```python
"""bangumi.tv BRONZE → SILVER loaders.

Tables:
- anime          (subject_id → 'bgm:s<id>')
- persons        (person_id  → 'bgm:p<id>')
- characters     (character_id → 'bgm:c<id>')
- credits        (subject_persons → 'bangumi' source)
- character_voice_actors (subject_characters JOIN person_characters)
"""
from __future__ import annotations
from pathlib import Path
import duckdb


def _g(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=bangumi" / f"table={table}" / "date=*" / "*.parquet")


_SUBJECTS_SQL = """
INSERT INTO anime (
    id, title_ja, title_en,
    infobox_json, platform, meta_tags_json, series_flag,
    display_score_bgm, display_score_details_json, display_rank_bgm, display_favorite_bgm
)
SELECT DISTINCT
    'bgm:s' || CAST(id AS VARCHAR),
    COALESCE(name, ''), COALESCE(name_cn, ''),
    infobox, platform, meta_tags,
    TRY_CAST(series AS INTEGER),
    TRY_CAST(score AS REAL),
    score_details,
    TRY_CAST(rank AS INTEGER),
    TRY_CAST(favorite AS INTEGER)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL AND type = 2
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

_PERSONS_INSERT_SQL = """
INSERT INTO persons (id, name_ja, name_en, career_json, infobox_json, summary_bgm, bgm_id, person_type)
SELECT DISTINCT
    'bgm:p' || CAST(id AS VARCHAR),
    COALESCE(name, ''), '',
    career, infobox, summary,
    TRY_CAST(id AS INTEGER),
    TRY_CAST(type AS INTEGER)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

_PERSONS_UPDATE_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
UPDATE persons SET
    gender     = COALESCE(persons.gender, bronze.gender),
    blood_type = COALESCE(persons.blood_type, bronze.blood_type),
    birth_date = COALESCE(persons.birth_date,
                  CASE WHEN bronze.birth_year IS NOT NULL
                       THEN CAST(bronze.birth_year AS VARCHAR) || '-' ||
                            LPAD(COALESCE(CAST(bronze.birth_mon AS VARCHAR),'01'), 2, '0') || '-' ||
                            LPAD(COALESCE(CAST(bronze.birth_day AS VARCHAR),'01'), 2, '0')
                  END)
FROM bronze
WHERE persons.id = ('bgm:p' || CAST(bronze.id AS VARCHAR)) AND bronze._rn = 1
"""

_CHARACTERS_SQL = """
INSERT INTO characters (
    id, name_ja, name_en, gender, blood_type,
    infobox_json, summary_bgm, bgm_id, character_type, images_json
)
SELECT DISTINCT
    'bgm:c' || CAST(id AS VARCHAR),
    COALESCE(name, ''), '',
    gender, blood_type,
    infobox, summary,
    TRY_CAST(id AS INTEGER),
    TRY_CAST(type AS INTEGER),
    images
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

# subject_persons → credits
# role mapping: src/etl/role_mappers/bangumi.py を実装/利用
_CREDITS_SQL = """
INSERT INTO credits (person_id, anime_id, role, raw_role, episode, evidence_source, affiliation, position)
SELECT DISTINCT
    'bgm:p' || CAST(person_id AS VARCHAR),
    'bgm:s' || CAST(subject_id AS VARCHAR),
    map_role_bangumi(CAST(position AS VARCHAR)),
    CAST(position AS VARCHAR),
    NULL,
    'bangumi',
    NULL,
    NULL
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE person_id IS NOT NULL AND subject_id IS NOT NULL
"""

_CVA_SQL = """
INSERT INTO character_voice_actors (character_id, person_id, anime_id, character_role, source)
SELECT DISTINCT
    'bgm:c' || CAST(pc.character_id AS VARCHAR),
    'bgm:p' || CAST(pc.person_id AS VARCHAR),
    'bgm:s' || CAST(pc.subject_id AS VARCHAR),
    COALESCE(sc.relation, ''),
    'bangumi'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true) pc
LEFT JOIN read_parquet(?, hive_partitioning=true, union_by_name=true) sc
    ON pc.subject_id = sc.subject_id AND pc.character_id = sc.character_id
WHERE pc.character_id IS NOT NULL
  AND pc.person_id IS NOT NULL
  AND pc.subject_id IS NOT NULL
ON CONFLICT (character_id, person_id, anime_id) DO NOTHING
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    from src.etl.role_mappers import map_role
    conn.create_function(
        "map_role_bangumi",
        lambda r: map_role("bangumi", r) if r is not None else "other",
        ["VARCHAR"], "VARCHAR",
    )
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}
    conn.execute(_SUBJECTS_SQL,        [_g(bronze_root, "subjects")])
    conn.execute(_PERSONS_INSERT_SQL,  [_g(bronze_root, "persons")])
    conn.execute(_PERSONS_UPDATE_SQL,  [_g(bronze_root, "persons")])
    conn.execute(_CHARACTERS_SQL,      [_g(bronze_root, "characters")])
    conn.execute(_CREDITS_SQL,         [_g(bronze_root, "subject_persons")])
    conn.execute(_CVA_SQL, [
        _g(bronze_root, "person_characters"),
        _g(bronze_root, "subject_characters"),
    ])
    counts["bgm_anime"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'bgm:s%'"
    ).fetchone()[0]
    counts["bgm_persons"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'bgm:p%'"
    ).fetchone()[0]
    counts["bgm_characters"] = conn.execute(
        "SELECT COUNT(*) FROM characters WHERE id LIKE 'bgm:c%'"
    ).fetchone()[0]
    counts["bgm_credits"] = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE evidence_source = 'bangumi'"
    ).fetchone()[0]
    counts["bgm_cva"] = conn.execute(
        "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'bangumi'"
    ).fetchone()[0]
    return counts
```

### Step 4: bangumi role_mapper 確認/実装

`src/etl/role_mappers/bangumi.py` が無い場合は最小実装。`src/utils/role_groups.py` の `ANIME_POSITION_LABELS` を参照して position code → Role.value 辞書化。

### Step 5: Test

合成 BRONZE で 6 SQL 確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_bangumi.py

# H1 invariant:
rg '\bscore\b' src/etl/silver_loaders/bangumi.py | rg -v 'display_'   # 0 件
rg '\brank\b' src/etl/silver_loaders/bangumi.py | rg -v 'display_'    # 0 件
```

---

## Stop-if

- [ ] BRONZE 6 parquet 欠落
- [ ] role_mappers/bangumi.py が無く、新規実装で他 mapper の整合性破壊
- [ ] `score` / `rank` を bare 名で SILVER 列に追加してしまった

---

## Rollback

```bash
git checkout src/db/schema.py src/etl/role_mappers/
rm src/etl/silver_loaders/bangumi.py
rm tests/test_etl/test_silver_bangumi.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/05_bangumi_silver`

# Task: sakuga@wiki anime_id 解決 + persons 拡張

**ID**: `14_silver_extend/07_sakuga_atwiki_resolution`
**Priority**: 🟡
**Estimated changes**: 約 +250 / -0 lines, 3 files
**Requires senior judgment**: yes (title-matching 設計、entity_resolution 連携)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

sakuga@wiki BRONZE で SILVER 未解決な部分を補完:
1. **work_title → silver anime_id matching ETL**: 既存 SILVER credits の `anime_id IS NULL` (sakuga_atwiki source) を埋める
2. `persons` BRONZE → SILVER persons (sakuga prefix) 統合
3. `pages` / `work_staff` 表は補助情報、必要分のみ抽出

---

## Hard constraints

- **H3**: entity_resolution **ロジック不変**。本カードでは title fuzzy matching を独立 module として実装し、既存 entity_resolution には touch しない
- **H4**: credits.evidence_source = 'sakuga_atwiki' 維持

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=sakuga_atwiki/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 4
- [ ] 既存 SILVER credits に `anime_id IS NULL AND evidence_source='sakuga_atwiki'` 行が存在 (要事前確認)
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/sakuga_atwiki.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_sakuga_atwiki.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== sakuga_atwiki extension =====` 追加 (該当時)

## Files to NOT touch

- `src/analysis/entity_resolution.py`
- `src/etl/integrate_duckdb.py`

---

## SILVER 設計

### `persons` テーブルへの sakuga person 追加

`persons` BRONZE (sakuga) → SILVER `persons`. ID: `'sakuga:p' || page_id`。
- name → name_ja
- aliases_json → aliases (Card 04 で ALTER 済 = UPDATE)
- active_since_year → 既存 `years_active` (Card 04 ALTER) に START 年として書く

### work_title → anime_id matching テーブル

新表:
```sql
CREATE TABLE IF NOT EXISTS sakuga_work_title_resolution (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    work_title      TEXT NOT NULL,
    work_year       INTEGER,
    work_format     TEXT,
    resolved_anime_id TEXT,        -- マッチ結果。NULL = 未解決
    match_method    TEXT,          -- 'exact_title' / 'normalized' / 'unresolved'
    match_score     REAL,          -- 類似度 (0.0-1.0)
    UNIQUE(work_title, work_year, work_format)
);
CREATE INDEX IF NOT EXISTS idx_swtr_anime ON sakuga_work_title_resolution(resolved_anime_id);
```

### マッチング戦略 (5 段階のうち単純 3 段階のみ)

新規 module `src/etl/sakuga_title_matcher.py` を本カード内で作成:
1. **exact title match**: `silver.anime` の `title_ja` / `title_en` と完全一致
2. **normalized match**: 全角/半角・スペース・記号除去後一致
3. **giveup**: 上記 fail なら NULL (将来 entity_resolution で再解決)

`src/analysis/entity_resolution.py` の **5 段階フル実装は使わない**。理由: H3 で当該 module 不変、本カードは独立。

### credits の anime_id UPDATE

```sql
UPDATE credits
SET anime_id = swtr.resolved_anime_id
FROM sakuga_work_title_resolution swtr
JOIN read_parquet('...sakuga_atwiki/credits/...', ...) bronze
    ON swtr.work_title = bronze.work_title
   AND COALESCE(swtr.work_year, -1) = COALESCE(bronze.work_year, -1)
WHERE credits.evidence_source = 'sakuga_atwiki'
  AND credits.person_id = ('sakuga:p' || CAST(bronze.person_page_id AS VARCHAR))
  AND credits.raw_role = bronze.role_raw
  AND credits.anime_id IS NULL
  AND swtr.resolved_anime_id IS NOT NULL
```

---

## Steps

### Step 1: schema.py 拡張
`sakuga_work_title_resolution` 表追加。

### Step 2: title matcher 実装

`src/etl/sakuga_title_matcher.py`:
```python
"""sakuga@wiki work_title → silver anime_id matcher.

Conservative: only exact and normalized matches. Falls back to NULL
for ambiguous cases. Full entity resolution is left to a downstream
ETL step (out of scope for Card 14).
"""
import re
import unicodedata


def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[!！?？・･、，,.。〜~：:；;]", "", s)
    return s.lower()


def match_title(work_title, work_year, anime_rows):
    """Returns (anime_id, method, score) or (None, 'unresolved', 0.0).

    anime_rows: iterable of (id, title_ja, title_en, year)
    """
    if not work_title:
        return None, "unresolved", 0.0

    norm_target = _normalize(work_title)
    exact = []
    normalized = []
    for aid, ja, en, year in anime_rows:
        if work_year and year and abs(year - work_year) > 1:
            continue
        for cand in (ja, en):
            if not cand:
                continue
            if cand == work_title:
                exact.append(aid)
            elif _normalize(cand) == norm_target:
                normalized.append(aid)
    if len(exact) == 1:
        return exact[0], "exact_title", 1.0
    if len(normalized) == 1:
        return normalized[0], "normalized", 0.95
    return None, "unresolved", 0.0
```

### Step 3: `silver_loaders/sakuga_atwiki.py` 実装

```python
"""sakuga@wiki BRONZE → SILVER (work_title resolution + persons)."""
from __future__ import annotations
from pathlib import Path
import duckdb

from src.etl.sakuga_title_matcher import match_title


def _g(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=sakuga_atwiki" / f"table={table}" / "date=*" / "*.parquet")


_PERSONS_INSERT_SQL = """
INSERT INTO persons (id, name_ja, aliases, years_active)
SELECT DISTINCT
    'sakuga:p' || CAST(page_id AS VARCHAR),
    COALESCE(name, ''),
    COALESCE(aliases_json, '[]'),
    CASE WHEN active_since_year IS NOT NULL
         THEN CAST(active_since_year AS VARCHAR) || '-'
    END
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE page_id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""


def _resolve_work_titles(conn, bronze_root):
    """Build sakuga_work_title_resolution table."""
    bronze = conn.execute(f"""
        SELECT DISTINCT work_title, work_year, work_format
        FROM read_parquet('{_g(bronze_root, "credits")}', hive_partitioning=true, union_by_name=true)
        WHERE work_title IS NOT NULL
    """).fetchall()
    anime_rows = conn.execute(
        "SELECT id, title_ja, title_en, year FROM anime"
    ).fetchall()

    rows = []
    for work_title, work_year, work_format in bronze:
        aid, method, score = match_title(work_title, work_year, anime_rows)
        rows.append((work_title, work_year, work_format, aid, method, score))

    conn.executemany(
        """INSERT INTO sakuga_work_title_resolution
           (work_title, work_year, work_format, resolved_anime_id, match_method, match_score)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT DO NOTHING""",
        rows,
    )
    return len(rows)


_CREDITS_UPDATE_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY person_page_id, work_title, role_raw, episode_num
                              ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE person_page_id IS NOT NULL
)
UPDATE credits
SET anime_id = swtr.resolved_anime_id
FROM bronze
JOIN sakuga_work_title_resolution swtr
    ON swtr.work_title = bronze.work_title
   AND COALESCE(swtr.work_year, -1) = COALESCE(bronze.work_year, -1)
WHERE credits.evidence_source = 'sakuga_atwiki'
  AND credits.person_id = ('sakuga:p' || CAST(bronze.person_page_id AS VARCHAR))
  AND credits.raw_role = bronze.role_raw
  AND credits.anime_id IS NULL
  AND swtr.resolved_anime_id IS NOT NULL
  AND bronze._rn = 1
"""


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}

    conn.execute(_PERSONS_INSERT_SQL, [_g(bronze_root, "persons")])
    counts["sakuga_persons"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'sakuga:p%'"
    ).fetchone()[0]

    counts["resolution_rows"] = _resolve_work_titles(conn, bronze_root)
    counts["resolved_anime_ids"] = conn.execute(
        "SELECT COUNT(*) FROM sakuga_work_title_resolution WHERE resolved_anime_id IS NOT NULL"
    ).fetchone()[0]

    conn.execute(_CREDITS_UPDATE_SQL, [_g(bronze_root, "credits")])
    counts["credits_resolved"] = conn.execute(
        """SELECT COUNT(*) FROM credits
           WHERE evidence_source = 'sakuga_atwiki' AND anime_id IS NOT NULL"""
    ).fetchone()[0]
    return counts
```

### Step 4: Test
- title matcher の unit test (exact / normalized / unresolved)
- 合成 BRONZE での integrate 動作確認

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_sakuga_atwiki.py

# H3 check: entity_resolution 不変
git diff src/analysis/entity_resolution.py   # 変更なし
```

---

## Stop-if

- [ ] BRONZE 欠落
- [ ] entity_resolution.py を編集してしまった
- [ ] resolved_anime_id が 0 件 = matcher が壊れている可能性 → ロジック確認

---

## Rollback

```bash
git checkout src/db/schema.py
rm src/etl/silver_loaders/sakuga_atwiki.py src/etl/sakuga_title_matcher.py
rm tests/test_etl/test_silver_sakuga_atwiki.py
```

---

## Completion signal

- [ ] Verification pass
- [ ] DONE: `14_silver_extend/07_sakuga_atwiki_resolution`

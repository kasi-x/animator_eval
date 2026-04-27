# Task: MAL/Jikan SILVER 統合

**ID**: `14_silver_extend/08_mal_silver`
**Priority**: 🟡
**Estimated changes**: 約 +500 / -0 lines, 3 files
**Requires senior judgment**: yes (大量 BRONZE の取捨選択、display 隔離の徹底)
**Blocks**: なし
**Blocked by**: なし (現 BRONZE 7,667 parquet 既存。Card 12/05 全件 scrape は別)

---

## Goal

MAL/Jikan BRONZE 28 表のうち SILVER 統合価値のあるものを取り込む。MAL は **score / popularity / favourites がコア** = H1 隔離が厳格に必要。

---

## Hard constraints

- **H1 (重要)**: MAL `score` / `popularity` / `favourites` / `members` / `rank` は SILVER に **bare 名で入れない**。display_* prefix 必須
- **H3**: entity_resolution 不変
- **H4**: credits は `evidence_source='mal'` で挿入
- **MAL ID prefix**: `'mal:a<id>'` (anime), `'mal:p<id>'` (person), `'mal:c<id>'` (character)

---

## Pre-conditions

- [ ] BRONZE: `find result/bronze/source=mal/table=*/date=*/ -name "*.parquet" | wc -l` ≥ 28
- [ ] `pixi run test` baseline pass

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/mal.py` | `integrate(conn, bronze_root)` |
| `tests/test_etl/test_silver_mal.py` | 単体テスト |

## Files to modify

- `src/db/schema.py`: `-- ===== mal extension =====` 追加

## Files to NOT touch

- `src/etl/integrate_duckdb.py`
- `src/scrapers/mal_scraper.py` (scrape は別タスク Card 12/05)

---

## SILVER 設計 (取り込み対象選別)

MAL 28 表すべては多すぎる → 高価値テーブルに絞る:

### 必須統合 (8 表)
| BRONZE table | SILVER 先 | 備考 |
|--------------|-----------|------|
| `anime` | `anime` (mal:a prefix) + ALTER 列 | display_score_mal / display_popularity_mal 等 |
| `persons` | `persons` (mal:p prefix) | name / images / birth_date |
| `staff_credits` | `credits` | evidence_source='mal' |
| `va_credits` | `character_voice_actors` | + characters 連携 |
| `anime_characters` | `characters` (mal:c prefix) + `character_voice_actors` | role/main flag |
| `anime_genres` | `anime_genres` (既存表) | |
| `anime_studios` | `anime_studios` + `studios` | |
| `anime_relations` | `anime_relations` (既存表) | |

### Optional (display/補助、必要なら)
- `anime_themes`: theme songs → SILVER `anime_theme_songs` (Card 04 表)
- `anime_episodes`: → SILVER `anime_episodes` (Card 03 表)
- `anime_news`: → SILVER `anime_news` (Card 03 表)
- `anime_external`: external links JSON → anime.external_links_json (Card 01 列)
- `anime_streaming`: streaming providers (display)
- `anime_pictures` / `anime_videos_promo` / `anime_videos_ep`: 画像/動画 URL (display)
- `anime_statistics`: scoring/watching counts (**全部 display_***)
- `anime_recommendations`: → 新表 `anime_recommendations`
- `anime_moreinfo`: 雑多 → anime.description fallback

### 不要 (本カードでは見送り)
- `anime_themes` BRONZE にすでに専用カードあり → スキップ可

## ALTER 列追加 (anime)

```sql
ALTER TABLE anime ADD COLUMN IF NOT EXISTS mal_id_int INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_mal REAL;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_popularity_mal INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_members_mal INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favorites_mal INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rank_mal INTEGER;
ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_scored_by_mal INTEGER;
```

## 新表 (1 つ)

```sql
CREATE TABLE IF NOT EXISTS anime_recommendations (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id             TEXT NOT NULL,
    recommended_anime_id TEXT NOT NULL,
    votes                INTEGER,
    source               TEXT NOT NULL DEFAULT 'mal',
    UNIQUE(anime_id, recommended_anime_id, source)
);
CREATE INDEX IF NOT EXISTS idx_arec_anime ON anime_recommendations(anime_id);
```

---

## Steps

### Step 1: schema.py 拡張

### Step 2: BRONZE schema 確認

```bash
# 各 MAL table の列を把握
for t in anime persons staff_credits va_credits anime_characters \
         anime_genres anime_studios anime_relations anime_recommendations; do
    f=$(find "result/bronze/source=mal/table=$t" -name "*.parquet" | head -1)
    [ -n "$f" ] && echo "=== $t ===" && pixi run python -c "
import duckdb
print(','.join(r[0] for r in duckdb.connect().execute(\"DESCRIBE SELECT * FROM read_parquet('$f') LIMIT 0\").fetchall()))
"
done
```

実装時に正確な列名を確認すること。

### Step 3: `silver_loaders/mal.py` 実装

8 必須 + 1 新表 (anime_recommendations) の loader を作る。雛形は他カード参照。各 SQL は以下の方針:

- `anime`: ID prefix `'mal:a' || mal_id`、INSERT 後に display_* 列を UPDATE
- `persons`: ID prefix `'mal:p' || mal_id`
- `staff_credits`: `INSERT INTO credits ... evidence_source='mal'`、role mapping は `map_role("mal", raw)` 必要 (`src/etl/role_mappers/` 確認、無ければ最小実装)
- `va_credits` + `anime_characters`: characters 表 INSERT → CVA 表 INSERT (JOIN)
- `anime_genres`: そのまま (mal anime_id prefix)
- `anime_studios`: studio name → `'mal:n:' || studio_name` ID で studios 表追加 → anime_studios 連携
- `anime_relations`: source/target を mal:a prefix 化
- `anime_recommendations`: 新表へ

### Step 4: role_mapper

`src/etl/role_mappers/mal.py` 不在なら最小実装:
```python
"""MAL/Jikan staff role → canonical Role mapping."""
from src.utils.role_groups import Role

ROLE_MAP: dict[str, str] = {
    "Director": Role.DIRECTOR.value,
    "Series Composition": Role.SERIES_COMPOSITION.value,
    "Character Design": Role.CHARACTER_DESIGN.value,
    "Animation Director": Role.ANIMATION_DIRECTOR.value,
    "Key Animation": Role.KEY_ANIMATION.value,
    # ... 他は existing role_mappers/ann.py 等を参考
}

def map_role(raw: str | None) -> str:
    if not raw:
        return "other"
    return ROLE_MAP.get(raw.strip(), "other")
```

`src/etl/role_mappers/__init__.py` の dispatcher に登録。

### Step 5: Test

合成 MAL parquet で各 loader 動作 + H1 invariant 確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_mal.py

# H1 check (重要):
rg '\bscore\b|\bpopularity\b|\bfavourites\b|\bmembers\b|\brank\b' \
   src/etl/silver_loaders/mal.py | rg -v 'display_' | rg -v '^\s*#'   # 0 件
```

---

## Stop-if

- [ ] BRONZE 28 parquet 大半欠落 (必須 8 のいずれか欠落)
- [ ] H1 違反: `score` 等を bare 名で SILVER に書き込み
- [ ] role mapping が完全に欠落して 100% "other" になる

---

## Rollback

```bash
git checkout src/db/schema.py src/etl/role_mappers/
rm src/etl/silver_loaders/mal.py
rm tests/test_etl/test_silver_mal.py
```

---

## Completion signal

- [ ] Verification pass + H1 violation 0
- [ ] DONE: `14_silver_extend/08_mal_silver`

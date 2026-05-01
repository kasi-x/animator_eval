# Task: AniList anime 拡張列 SILVER 取込 (TODO §12.1 残務)

**ID**: `18_data_integrity/05_anilist_extras_silver`
**Priority**: 🟠
**Estimated changes**: 約 +200 / -0 lines, 2-3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

AniList BRONZE `anime` 表の拡張列 (external_links_json / airing_schedule_json / trailer_url / trailer_site / rankings_json) を SILVER `anime` 表に取り込む。

---

## Hard constraints

- **H1**: `score` / `popularity` 系列は **display_*** prefix 維持 (既存)、新規列で違反禁止
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- 並列衝突: schema.py は **末尾** anilist extension セクションを既存と統合 (中央部編集禁止)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE 確認:
```bash
duckdb result/silver.duckdb -c "
SELECT column_name FROM (
  SELECT * FROM read_parquet('result/bronze/source=anilist/table=anime/date=*/*.parquet', union_by_name=true) LIMIT 0
)"
```
- [ ] SILVER `anime` 表の現在の列確認 (Card 14/01 で `external_links_json` 列追加済か?)
- [ ] `pixi run test` baseline pass

---

## 設計

### 対象拡張列

| BRONZE | SILVER | 備考 |
|--------|--------|------|
| `external_links_json` | `external_links_json` | TEXT (JSON)、配信プラットフォームリンク (O8 で活用) |
| `airing_schedule_json` | `airing_schedule_json` | TEXT (JSON)、放送スケジュール |
| `trailer_url` | `trailer_url` | TEXT |
| `trailer_site` | `trailer_site` | TEXT (youtube/dailymotion 等) |
| `rankings_json` | `display_rankings_json` | TEXT (JSON)、display only (H1) |

### 既存 schema 確認

Card 14/01 で `_ANILIST_EXTENSION_COLUMNS` が以下を含むか:
```bash
grep -A 30 "_ANILIST_EXTENSION_COLUMNS" src/db/schema.py
```

不足列があれば追加 (ALTER TABLE IF NOT EXISTS)、既にあるなら loader 側のみ修正。

### loader 修正

`src/etl/silver_loaders/anilist.py` の `_ANIME_EXTRAS_SQL` (or 同等の UPDATE 文) に新列マッピング追加:

```sql
UPDATE anime SET
    ...,
    external_links_json     = bronze.external_links_json,
    airing_schedule_json    = bronze.airing_schedule_json,
    trailer_url             = bronze.trailer_url,
    trailer_site            = bronze.trailer_site,
    display_rankings_json   = bronze.rankings_json
FROM bronze
WHERE anime.id = bronze.id
```

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | 不足なら `_ANILIST_EXTENSION_COLUMNS` に列追加 |
| `src/etl/silver_loaders/anilist.py` | `_ANIME_EXTRAS_SQL` に新列マッピング追加 |
| `tests/test_etl/test_silver_anilist.py` | 新列が SILVER に反映される回帰テスト追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| 他 source の silver_loaders | 本カード anilist 限定 |
| `src/etl/integrate_duckdb.py` | dispatcher 既統合 |

---

## Steps

### Step 1: BRONZE / SILVER 列確認

```bash
duckdb result/silver.duckdb -c "DESCRIBE anime" | grep -E "external_links|airing_schedule|trailer|rankings"
```

### Step 2: schema.py 必要なら追加

不足列のみ ALTER TABLE IF NOT EXISTS 追加。

### Step 3: loader SQL 修正

`anilist.py` の UPDATE 文に列マッピング追加。

### Step 4: 回帰テスト追加

合成 BRONZE parquet で新列が SILVER 反映確認。

### Step 5: 実 BRONZE で動作確認

```bash
pixi run python -m src.etl.integrate_duckdb --rebuild  # or specific path
duckdb result/silver.duckdb -c "
SELECT COUNT(*) FILTER (WHERE external_links_json IS NOT NULL) AS with_links,
       COUNT(*) FILTER (WHERE airing_schedule_json IS NOT NULL) AS with_schedule,
       COUNT(*) FILTER (WHERE trailer_url IS NOT NULL) AS with_trailer
FROM anime
"
```

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_anilist.py
rg 'rankings_json|external_links_json|airing_schedule_json' src/etl/silver_loaders/anilist.py
```

---

## Stop-if conditions

- [ ] BRONZE に該当列が存在しない (parser 未対応) → scrape 系タスク (本カード Stop)
- [ ] `display_rankings_json` でなく `rankings_json` で SILVER 入れる誤り (H1 違反)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout src/etl/silver_loaders/anilist.py src/db/schema.py tests/test_etl/test_silver_anilist.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] SILVER `anime` 表の新列 row count > 0
- [ ] DONE: `18_data_integrity/05_anilist_extras_silver`

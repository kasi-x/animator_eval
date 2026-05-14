# Task: madb parent_madb_id 復元 + cluster ロジック修正

**ID**: `19_resolved_cluster_fix/01_madb_parent_link`
**Priority**: 🟠 High
**Estimated changes**: ~+250 / -30 lines, 5 files
**Requires senior judgment**: yes (resolved.episodes 新表 PK 設計 / cluster 起点切替)
**Blocks**: AKM / scoring 全般 (anime cluster size 異常正常化)
**Blocked by**: なし

---

## Goal

MADB JSON-LD の `schema:isPartOf` (M-manifestation → 親 C-series link) を **bronze 列として復元**、conformed.anime に伝播、resolved.anime cluster を **C-prefix 起点 + M-row は子情報として保持**する形に切替。

**情報量原則**: M-row は捨てない。話数別 broadcast_date / 個別 staff credit は episode 粒度で `resolved.episodes` (新) または `resolved.credits` の episode_id 列に流す。

---

## Hard constraints

- **H1**: anime.score 系列触らない
- **H3**: entity_resolution interface 不変 (cluster 入力フォーマットのみ変更)
- **H4**: credits 行数減らさない (M-row credit を捨てない)
- **再 scrape 不要**: raw JSON-LD zip は `data/madb/` に手元保持。parser 改修 + 再 parse のみ
- **逐次的に進める**: parser → bronze re-write → conformed → resolved の順、各段階で row count diff 検証

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `data/madb/` に metadata20x_json.zip 一式存在 (`ls data/madb/*.zip | wc -l` → 5 以上)
- [ ] `result/resolved.duckdb` バックアップ取得 (`cp result/resolved.duckdb result/resolved.duckdb.bak.before-madb-parent`)
- [ ] baseline cluster 統計記録 (audit 用):
  ```sql
  SELECT source_count, COUNT(*) FROM anime GROUP BY source_count ORDER BY source_count DESC LIMIT 20;
  ```

---

## Files to modify

| File | 変更内容 |
|------|----------|
| `src/scrapers/parsers/mediaarts.py` | `parse_jsonld_dump` 改修: `parent_madb_id` (= isPartOf @id 抜粋) + `record_type` (= `@type` 末尾、e.g. `AnimationTVRegularSeries` / `AnimationVideoPackage`) を anime row dict に追加 |
| `src/scrapers/mediaarts_scraper.py` | parquet 書き出し schema に 2 列追加 |
| `src/etl/conformed_loaders/madb.py` | conformed.anime に `parent_madb_id` / `record_type` 列伝播 |
| `src/db/schema.py` | conformed.anime に列追加 (SQLModel single source) |
| `src/etl/resolved/resolve_anime.py` | cluster ロジック修正: madb 系 row は parent_madb_id があれば parent C-id へ集約、なければ自身 C-id を起点 |

## Files to create

| File | 内容 |
|------|------|
| `tests/test_scrapers/test_mediaarts_parent_link.py` | サザエさん M-row sample fixture で parent 復元検証 |
| `tests/test_etl/test_resolved_madb_cluster.py` | C-prefix 起点 cluster 検証 (cluster size > 100 を許さない) |

---

## Implementation outline

### Step 1: parser 改修
`parse_jsonld_dump` 内で各 item から:
```python
parent_link = item.get("schema:isPartOf", {})
parent_madb_id = ""
if isinstance(parent_link, dict):
    m = re.search(r"/id/([^/]+)$", parent_link.get("@id", ""))
    parent_madb_id = m.group(1) if m else ""

record_type = item.get("@type", "")  # e.g. "class:AnimationTVRegularSeries"
record_type = record_type.split(":")[-1] if record_type else ""
```
出力 dict に `parent_madb_id`, `record_type` 追加。

### Step 2: bronze 再書き出し
```bash
pixi run python -m src.scrapers.mediaarts_scraper --reparse-only
```
新 parquet には 2 列追加。既存 row は再書き出し (raw JSON-LD は不変)。

### Step 3: conformed 再 build
```bash
pixi run python -m src.etl.conformed_loaders.madb
```
conformed.anime row に `parent_madb_id` 列伝播。

### Step 4: resolved cluster ロジック
`resolve_anime.py` 内 madb 系 row を以下で前処理:
```python
# M-row は parent C-row へ吸収。C-row なし (orphan M) は自身を起点
def _madb_canonical_key(row):
    if row.parent_madb_id and row.parent_madb_id.startswith("C"):
        return f"madb:{row.parent_madb_id}"
    return row.id  # 既存 logic
```
title+year fallback の前段に挿入。

### Step 5: M-row 固有情報の保持先
2 案検討:

**案 a (推奨)**: `resolved.episodes` 新 table
- PK: `episode_id` (= `madb:M{n}`)
- 列: `parent_anime_id` (= resolved canonical anime_id) / `episode_number` / `broadcast_date` / `subtitle` / `runtime_min`
- credits は episode_id 経由 join 可能

**案 b**: `resolved.credits` に `episode_id` 列追加 (既存 schema 拡張、新 table 不要)
- 簡素だが episode-level metadata (放送日/サブタイトル) の置き場ない

→ **案 a 採用**。`docs/REPORT_INVENTORY.md` の継続性分析 (季節別 credit 密度) で利点大きい。

---

## Audit / verification

### Step 6: cluster size 分布検証
```sql
-- BEFORE: source_count 1089 / 323 / 307 / 255 / 252... の異常 cluster
-- AFTER:  source_count > 100 の cluster ゼロを目標
SELECT source_count, COUNT(*) FROM anime GROUP BY source_count ORDER BY source_count DESC LIMIT 10;
```

### Step 7: row count 不変検証 (情報損失なし)
```sql
-- BEFORE / AFTER 両方:
SELECT COUNT(*) FROM credits WHERE source_id LIKE 'madb:%';
-- → 完全一致すべき (M-row credit が捨てられていないこと)
```

### Step 8: サザエさん specific
```sql
SELECT canonical_id, title_ja, year, source_count, json_array_length(source_ids_json) AS n
FROM anime WHERE title_ja = 'サザエさん';
-- AFTER: cluster は 1969年放送開始 TV シリーズ 1 件 + 劇場版数件 + 等。
-- M-row 1937 件は resolved.episodes 経由で別保持。
```

---

## Open questions

- **C-row なしの orphan M-row**: 一部 M-row の親 C が同 zip 内に存在しない場合あり (ex: 古い metadata 系)。orphan は自身を起点に独立 cluster?
- **record_type フィルタの厳密化**: `AnimationTVRegularSeries` / `AnimationVideoPackage` 以外の type (BroadcastEvent 等) が anime parquet に流入してた場合、別 table へ分離?
- **conformed.anime PK 衝突**: 同 madb_id が複数 zip (metadata 207 + 201) で重複 scrape されている可能性 → 既 conformed loader で dedupe 済か再確認

---

## Rollback

```bash
mv result/resolved.duckdb.bak.before-madb-parent result/resolved.duckdb
git revert HEAD~N..HEAD  # parser / loader / cluster commits
```

---

## Done criteria

- [ ] サザエさん cluster source_count = 1 (TV series, 1969-) + 派生映画作品数件
- [ ] あおきいろ 2024 / 2025 / シナぷしゅ 2022 / 2025: source_count < 10
- [ ] credits 行数 = before fix (loss ゼロ)
- [ ] `resolved.episodes` table 存在、行数 ≥ 旧巨大 cluster 内 M-row 合計
- [ ] `pixi run test-scoped tests/test_scrapers/test_mediaarts_parent_link.py tests/test_etl/test_resolved_madb_cluster.py` pass

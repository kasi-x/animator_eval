# Task: meta_lineage 充実 (28 SILVER 表 post-hoc 集約)

**ID**: `18_data_integrity/03_meta_lineage_extend`
**Priority**: 🟡
**Estimated changes**: 約 +300 / -0 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

各 SILVER 表の出処 (BRONZE source × table × date partition) を `meta_lineage` 表に集約。既存 silver_loaders を**触らず**、post-hoc 集約 ETL で実現する。

---

## Hard constraints

- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- 各 silver_loaders/*.py を **触らない** (並列衝突回避)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] SILVER 28 表存在
- [ ] `meta_lineage` 表の DDL 確認 (`schema.py`)
- [ ] `pixi run test` baseline pass

---

## 設計

### 既存 `meta_lineage` スキーマ確認

```bash
grep -A 20 "CREATE TABLE.*meta_lineage" src/db/schema.py
```

### 集約ロジック

```python
def collect(conn, bronze_root) -> list[LineageRow]:
    """Walks each silver table; for each, identifies the BRONZE
    parquet partitions consumed (via row sampling or evidence_source
    aggregation), then writes one meta_lineage row per
    (silver_table, source, bronze_table, partition_date)."""
```

各 SILVER 表に対し:
- `evidence_source` 列 or ID prefix から source 推定
- `MAX(updated_at)` から partition date 推定
- 集計行を `meta_lineage` に INSERT (UNIQUE 制約あれば UPSERT)

### post-hoc 実行 entry point

```bash
pixi run python -m src.etl.lineage.collect
```

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/lineage/__init__.py` | 空 |
| `src/etl/lineage/collect.py` | `collect(conn, bronze_root) -> int` (returns rows written) + main entry |
| `tests/test_etl/test_lineage_collect.py` | smoke + 各 SILVER 表マッピング検証 |

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | (必要なら) `meta_lineage` 列追加 (`silver_table` / `bronze_source` / `bronze_table` / `partition_date` / `row_count`) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/silver_loaders/*` | 並列衝突回避、本カードは post-hoc |
| `src/etl/integrate_duckdb.py` | 別カード対象 |

---

## Steps

### Step 1: 既存 meta_lineage スキーマ確認 + 必要なら ALTER TABLE

```bash
grep -A 20 "meta_lineage" src/db/schema.py
```

不足列があれば schema.py 末尾の lineage extension セクションに追加。

### Step 2: silver_table → bronze_source マッピング

```python
SILVER_TO_BRONZE = {
    "anime": ["anilist", "ann", "mal", "bgm", "mediaarts"],
    "persons": ["anilist", "ann", "mal", "bgm", "keyframe", "seesaawiki"],
    "credits": ["anilist", "ann", "mal", "bgm", "seesaawiki", "keyframe"],
    "characters": ["anilist", "ann", "mal", "bgm"],
    "character_voice_actors": ["anilist", "ann", "mal", "bgm"],
    "studios": ["anilist", "ann", "mal", "bgm", "mediaarts", "seesaawiki", "keyframe"],
    "anime_studios": ["anilist", "ann", "mal", "mediaarts", "seesaawiki", "keyframe"],
    # 単一 source 派生表
    "anime_genres": ["anilist", "mal"],
    "anime_episodes": ["ann"],
    "anime_companies": ["ann"],
    "anime_releases": ["ann"],
    "anime_news": ["ann"],
    "anime_relations": ["anilist", "mal"],
    "anime_recommendations": ["mal"],
    "anime_broadcasters": ["mediaarts"],
    "anime_broadcast_schedule": ["mediaarts"],
    "anime_video_releases": ["mediaarts"],
    "anime_production_companies": ["mediaarts"],
    "anime_production_committee": ["mediaarts", "seesaawiki"],
    "anime_original_work_links": ["mediaarts"],
    "anime_theme_songs": ["seesaawiki"],
    "anime_episode_titles": ["seesaawiki"],
    "anime_gross_studios": ["seesaawiki"],
    "anime_original_work_info": ["seesaawiki"],
    "person_jobs": ["keyframe"],
    "person_studio_affiliations": ["keyframe"],
    "anime_settings_categories": ["keyframe"],
    "sakuga_work_title_resolution": ["sakuga_atwiki"],
}
```

### Step 3: collect() 実装

各 (silver_table, bronze_source) ペアで:
- BRONZE 該当 partition の row count
- SILVER 表の該当 source 行 count
- max date / min date
- meta_lineage に INSERT or UPSERT

### Step 4: テスト

合成 SILVER + BRONZE で集計検証。

### Step 5: 実行 + 結果保存

```bash
pixi run python -m src.etl.lineage.collect
```

`meta_lineage` 表に 28 表 × 平均 3 source = ~85 行追加。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_lineage_collect.py
pixi run python -c "
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute('SELECT silver_table, COUNT(*) FROM meta_lineage GROUP BY 1 ORDER BY 1').fetchdf())
"
```

---

## Stop-if conditions

- [ ] `meta_lineage` 表が DDL 不在 (silver schema 不整合) → schema.py 確認 + 報告で停止
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm -rf src/etl/lineage/
rm -f tests/test_etl/test_lineage_collect.py
git checkout src/db/schema.py
# meta_lineage 行削除 (本カード追加分のみ)
duckdb result/silver.duckdb -c "DELETE FROM meta_lineage WHERE created_at >= '2026-05-02'"
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] meta_lineage row count > 50
- [ ] DONE: `18_data_integrity/03_meta_lineage_extend`

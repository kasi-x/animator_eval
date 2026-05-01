# Task: anime_relations / anime_recommendations cross-source 拡充

**ID**: `19_silver_postprocess/03_relations_recommendations_extend`
**Priority**: 🟡
**Estimated changes**: 約 +250 / -30 lines, 3-4 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`anime_relations` と `anime_recommendations` の SILVER 行を AniList のみ / MAL のみだった現状から、bangumi / ann (relations only) もマージして cross-source 統合する。

---

## Hard constraints

- **H1**: recommendations は MAL の score-based 順位を SILVER scoring に**入れない**。display 順序情報のみ
- **H3**: 同一 anime_id 確定済の relations 統合のみ、entity_resolution ロジック不変
- **H4**: 各行に `source` 列 (`'anilist'` / `'mal'` / `'bangumi'` / `'ann'`) を保持
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 現状確認:
```bash
duckdb result/silver.duckdb -c "
SELECT relation_type, COUNT(*) FROM anime_relations GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
SELECT COUNT(*), COUNT(DISTINCT anime_id) FROM anime_recommendations
"
```
- [ ] `pixi run test` baseline pass

---

## 設計

### anime_relations 拡充

現状: AniList の `relations_json` パース、MAL の `anime_relations` 表

追加 source:
- **bangumi**: `subjects` の relations フィールド (REST API でも取得済) → BRONZE `bangumi/relations` (要確認)
- **ann**: `related` 表 (Card 14/03 で SILVER 統合済の `anime_relations` の ann 部分。確認要)

### anime_recommendations 拡充

現状: MAL のみ (`anime_recommendations` 表、Card 14/08 で実装)

追加 source:
- **AniList**: `Recommendation` GraphQL field (BRONZE 側に存在するか確認要)
- **bangumi**: `subjects` の relations 中に "サマー連動" 等の関連作品あり (recommendation とは異なるが、共起関係として活用可)

### スキーマ統合

`anime_relations`:
```
PRIMARY KEY (source_anime_id, target_anime_id, relation_type, source)
```

`anime_recommendations`:
```
PRIMARY KEY (anime_id, recommended_id, source)
```

`source` 列がない場合は ALTER TABLE で追加。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/silver_loaders/bangumi.py` | relations 追加 INSERT |
| `src/etl/silver_loaders/anilist.py` | recommendations 追加 INSERT (BRONZE に該当列ある場合) |
| `src/etl/silver_loaders/ann.py` | (要確認) ann_related → anime_relations 統合 |
| `src/db/schema.py` | `anime_relations` / `anime_recommendations` に `source` 列追加 + PK 修正 (末尾追記) |
| `tests/test_etl/test_silver_<source>.py` | 各 loader の関連テスト追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/integrate_duckdb.py` | dispatcher 既統合 |
| `src/analysis/entity_resolution.py` | H3 |

---

## Steps

### Step 1: BRONZE 側のデータ確認

```bash
duckdb result/silver.duckdb -c "
SELECT 'bangumi/relations' AS src, COUNT(*) FROM read_parquet('result/bronze/source=bangumi/table=*/date=*/*.parquet', union_by_name=true) WHERE 'related_subjects' IS NOT NULL
"
```

各 source で relations / recommendations 列が存在するか確認。なければ scrape 拡張要 (本カード Stop)。

### Step 2: schema 拡張

`source` 列追加 (なければ)、PK 修正。

### Step 3: 各 loader に INSERT 追加

bangumi / anilist / ann それぞれの loader に relations / recommendations の INSERT 文追加。

### Step 4: テスト

各 loader の合成 fixture テスト追加。

### Step 5: 実 SILVER で検証

```bash
duckdb result/silver.duckdb -c "
SELECT source, COUNT(*) FROM anime_relations GROUP BY 1 ORDER BY 2 DESC;
SELECT source, COUNT(*) FROM anime_recommendations GROUP BY 1 ORDER BY 2 DESC
"
```

各 source 別 row が増えたこと確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_bangumi.py tests/test_etl/test_silver_anilist.py tests/test_etl/test_silver_ann.py
```

---

## Stop-if conditions

- [ ] BRONZE に relations / recommendations 列が存在しない (parser 未対応) → scrape 系タスク化、本カード Stop
- [ ] PK 修正が既存 row と衝突
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout src/etl/silver_loaders/ src/db/schema.py tests/test_etl/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] anime_relations / anime_recommendations の row 増加確認 (source 別)
- [ ] DONE: `19_silver_postprocess/03_relations_recommendations_extend`

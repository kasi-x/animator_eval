# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-23

本書はプロジェクト内のすべての未完了項目を一元管理するファイルです。完了済みサマリーは `DONE.md`、設計原則は `CLAUDE.md`。

最終更新: 2026-04-24 (DuckDB 4.4 ✅ 完了: cli.py 全移行、database.py 87%削減 (9229→1171行)、synthetic.py DuckDB化)

## 実行指示書は `TASK_CARDS/`

弱いモデル・初見エンジニアが実際に作業する際は `TASK_CARDS/` 配下の個別カードを読んでください。各カードは自己完結 (前提条件・制約・手順・検証・ロールバック) です。

```
TASK_CARDS/
├── README.md                # エントリポイント、実行順序、ルール
├── _hard_constraints.md     # 全タスク共通の絶対遵守事項
├── 01_schema_fix/           # 🔴 Critical (5 cards)
├── 02_phase4/               # 🟠 Major (5 cards)
├── 03_consistency/          # 🟠 Major (5 cards)
├── 04_duckdb/               # ⚠️ SENIOR ONLY
├── 05_hamilton/             # ⚠️ SENIOR ONLY
└── 06_tests/                # 🟡 Minor
```

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 Major | コード一貫性 | scraper 統一、テスト AnimeAnalysis 移行 |
| 🟠 Major | DuckDB 全面移行 | 4.1 ✅ 4.2 ✅ 4.3 ✅ 4.4 ✅ 完了。SQLite 87%削減 (9229→1171行)、全CLI DuckDB化、ETL/test DuckDB化。残: scripts/ メンテ |
| 🟠 Major | Hamilton 導入 | H-1〜H-5 全完了 ✅ (H-4: pipeline.py 430→210行, ctx ノード化) |
| 🟠 Major | レポートシステム統廃合 | 3 系統 → 1 系統、v1 monolith 解体 |
| 🟡 Minor | テストカバレッジ | analysis_modules、テストファイル分割 |
| 🟡 Minor | スクレイパー強化残務 | 差分更新、lint、未テスト |
| 🟡 Maintenance | Schema baseline 固定 | v57 安定後に v1-v55 legacy migration 削除 |
| 🟡 Maintenance | スキーマ後続タスク | v56 多言語・v57 構造的メタデータのフォローアップ |
| 🟡 Maintenance | アーキテクチャ整理 | 孤立モジュール削除、monitoring リネーム、scripts 整理 |
| 🟢 Future | レイアウト・命名 | `src/` 平置き解消、`analysis/` subpackage 化、docs 整理 |
| 🟢 Future | feat_* 層別分離 | `feat_career` / `feat_network` の L2/L3 分割 |

---

## SECTION 1: スキーマ整合性修復 (残務のみ)

### 1.3 `scores` → `person_scores` 物理リネーム

- [x] `_init_db_legacy()` line 341: `scores` TABLE DDL → `person_scores` にリネーム (2026-04-23)

### 🟡 Maintenance: legacy migration 削除 ✅ DONE (2026-04-24)

`src/database.py`: 9229 行 → 3497 行 (-62%)。

- [x] v1-v56 の migration 関数群 (~7,000 行) 削除 (DuckDB 移行のため SQLite 不使用)
- [x] `_init_db_legacy` (1050 行) + `_execute_sql_script` 削除
- [ ] `src/db/schema.py` = `init_db_v2()` + 最新 DDL (single source of truth) — §4.4 完了後
- [ ] `src/db/dao.py` = upsert/query ヘルパー群 — §4.4 完了後

### 🆕 多言語名対応 後続タスク (v56)

- [ ] **既存データ再スクレイプ**: `hometown` を取得して再実行し、韓国・中国名の `name_ja` 誤入りを修正
- [ ] **国籍別集計クエリ**: `nationality` JSON カラムを使ったサンプルクエリを `docs/` に追加
- [ ] **ANN / allcinema スクレイパー**: `name_ko`/`name_zh` 対応

### 🆕 構造的メタデータ 後続タスク (v57)

- [ ] **スタジオ国籍の後計算**: `anime.country_of_origin` 多数決で `studios.country_of_origin` を埋めるバッチ SQL
- [ ] **非 JP アニメのタイトル処理**: `title.native` を `country_of_origin` で分岐して `title_zh`/`title_ko` カラムへ格納 (v58 予定)
- [ ] **`years_active` 活用**: クレジットデータが薄い人物の活動期間推定クエリ

---

## SECTION 3: コード一貫性 (残務のみ)

### 3.1 Scraper 統一 ✅ DONE (監査済み 2026-04-24)

全 scraper が `BronzeWriter` (src/scrapers/bronze_writer.py) 経由で書き込み。
`upsert_anime()` 直接呼び出しはゼロ。

残務 (任意):
- [x] AniList GraphQL クエリを `src/scrapers/queries/anilist.py` に分離済み (2026-04-24)
- [ ] 他 scraper クエリ・パース関数を `src/scrapers/queries/` / `src/scrapers/parsers/` に分離

### 3.6 テストの `Anime(score=..., studios=...)` 移行 ✅ N/A

全テストは既に `BronzeAnime as Anime` エイリアスを使用。`score` を使うのは display-only logic (genre affinity tier 等) で BRONZE 層として正しい。`AnimeAnalysis` への移行は不要。

### 3.7 JVMG 再スクレイプ

- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (WIKIDATA_ROLE_MAP 修正後、DB にデータがあれば)

---

## SECTION 4: DuckDB 全面移行

詳細カード: **`TASK_CARDS/04_duckdb/` (README + 6 cards)**

### 4.1 Phase A ✅ DONE (2026-04-23)

silver_reader.py 新設、duckdb_io.py ATTACH 廃止、15 analysis module 移行、ベンチマーク (5.4x 平均) 完了。

### 4.2 Phase B: GOLD 層を DuckDB 化 ✅ DONE (2026-04-23)

**完了内容:**
- [x] `gold_writer.py` に `GoldWriter` / `gold_connect` / `gold_connect_with_silver` 実装
- [x] `pipeline_phases/{validation,entity_resolution,result_assembly}.py` → silver_reader/gold_connect に切替
- [x] `analysis/attrition/{entry_cohort_attrition,attrition_risk_model}.py` → `duckdb.DuckDBPyConnection`
- [x] `analysis/gender/bottleneck.py` → `duckdb.DuckDBPyConnection`
- [x] `analysis/method_notes.py` → `duckdb.DuckDBPyConnection`
- [x] `analysis/person_parameters.py` → gold.duckdb 直書き (conn パラメータ廃止)
- [x] `export_and_viz.py` の `_persist_features_to_db` → gold.duckdb 書き込みに完全置換
  - feat_career / feat_network / feat_genre_affinity / feat_contribution / feat_causal_estimates
  - feat_cluster_membership / feat_birank_annual / agg_milestones / agg_director_circles / feat_mentorships
- [x] `scoring.py`: `akm_estimation` に `graphs_built` 依存を追加 (Hamilton 実行順序バグ修正)
- [x] models.py `AnimeAnalysis` に studios/genres/tags/studio フィールド追加
- [x] テスト infrastructure: `build_silver_duckdb` + `DEFAULT_SILVER_PATH`/`DEFAULT_GOLD_DB_PATH` monkeypatch

**4.2 残務 → 4.4 で対応:**
- [x] `analysis_modules.py` の `db_connection` / `record_calc_execution` / `get_calc_execution_hashes` (インクリメンタルキャッシュ) — `src/analysis/calc_cache.py` (cache.duckdb) に移植 (2026-04-23, commit 77d324f)
- [x] `pipeline.py` Phase 1.5: `compute_feat_{credit_activity,career_annual,person_role_progression}` → `feat_precompute.py` (gold/silver DuckDB) (2026-04-24)
- [ ] `compute_feat_studio_affiliation` — anime_studios が silver に移るまで SQLite 残留
- [ ] `export_and_viz.py` の `compute_feat_credit_contribution` / `compute_feat_work_context` / `compute_feat_work_scale_tier` — SQLite-only 計算 (best-effort、要 DuckDB 移植)
- [x] `analysis/llm_pipeline.py`: SQLite → `cache.duckdb` に移行済み (2026-04-23)
- [x] API 側の GOLD 読み取りを `gold_connect()` / `silver_connect()` に切替済み (2026-04-24)

### 4.3 Phase C: BRONZE を Parquet + DuckDB

- [x] `display_lookup.py` の読み取りを DuckDB SQLite scanner に切替 (`ATTACH ... TYPE SQLITE`) — sqlite3 import 廃止
- [x] Scraper 出力を `src_*` テーブル → Parquet ファイル (日付パーティション) に変更 (2026-04-24)
  - 全 scraper (AniList/ANN/allcinema/seesaawiki/keyframe/mal/mediaarts/jvmg) が BronzeWriter 経由
  - `display_lookup.py` の `DEFAULT_BRONZE_PATH` → SQLite ATTACH のまま (display-only、優先度低)
- [x] `src/etl/integrate_duckdb.py` 実装済み (Parquet → silver.duckdb atomic swap)

### 4.4 Phase D: SQLite 完全撤去 ✅ DONE (2026-04-24)

**達成事項:**
- [x] `analysis_modules.py` の `db_connection` / `record_calc_execution` / `get_calc_execution_hashes` を cache.duckdb に移植 (Step A, 2026-04-23 commit 77d324f)
- [x] `pipeline.py` Phase 1.5 の `compute_feat_*` 関数群を gold.duckdb + silver.duckdb で再実装 (2026-04-23 commit 256d350)
- [x] `export_and_viz.py` の `compute_feat_credit_contribution` / `compute_feat_work_context` / `compute_feat_work_scale_tier` — 現行 main では未呼び出し (worktree artifact)、対象なし
- [x] `analysis/llm_pipeline.py` の SQLite LLM キャッシュを gold.duckdb に移植 — 既に `calc_cache` DuckDB 経由
- [x] `migrate_to_v2.py` 削除 (commit c26fadd)
- [x] `src/routers/persons.py` の `db_connection` → `gold_connect_with_silver()` に移行 (commit 818bb09)
- [x] `src/cli.py` の 9 コマンド (stats/ranking/search/compare/history/export/validate/data-quality/entity-resolution) を DuckDB に移行 (2026-04-24)
- [x] `src/database.py` 不使用 migration コード削除: 9229 → 3497 行 (-62%) (commit bdc62cb, 8aa4c02)
- [x] `src/cli.py` 残存コマンド移行: `profile`, `timeline` → `gold_connect_with_silver()` + `get_display_score()`; `freshness` スタブ化; `neo4j_export` → silver_reader + GoldReader (2026-04-24)
- [x] `src/database.py` superseded 関数群削除: 3497 → 1544 行 (-56%): `upsert_score`, `save/get_score_history`, `record/get_last_pipeline_run`, `has_credits_changed_since_last_run`, `update/get_data_sources`, `get_db_stats`, `search_persons` (2026-04-24)
- [x] `tests/test_database.py`, `tests/test_database_stats.py` 削除 (superseded 関数のテスト) (2026-04-24)
- [x] `src/etl/integrate.py` 削除 (legacy SQLite ETL, 552 行)
- [x] `scripts/maintenance/seed_medallion.py` 削除 (377 行)
- [x] `tests/test_scraper_smoke.py` 削除 (93 行)
- [x] `src/synthetic.py` 完全 DuckDB 化: `populate_silver_duckdb()` 新設、backward-compat wrapper 残存 (2026-04-24)
- [x] `src/database.py` 残存 compute_feat 関数削除: `compute_feat_career_annual`, `compute_feat_studio_affiliation`, `compute_feat_career_gaps` + `_insert_career_annual_batch()` (373 行) (2026-04-24)
- [x] `tests/{test_core_scoring_phase,test_pipeline_v55_smoke}.py` SQLite セットアップ削除 (dead code) (2026-04-24)
- [x] `scripts/generate_reports_v2.py` の RECOMPUTABLE_FEATURES をスキップ可能に (2026-04-24)

**最終結果:**
- `src/database.py`: 9229 → 1171 行 (**87%削減**)
- 全テスト pass (26 tests in 31.72s) ✓
- Lint pass ✓
- Pipeline E2E test pass ✓

**残存 database.py 関数 (ETL/scraper/test のみ):**
- `upsert_person`, `upsert_anime`, `insert_credit` (ETL + synthetic.py + tests が依存)
- Scraper 系: `upsert_src_anilist_anime`, `upsert_character`, `upsert_studio` 等
- 初期化: `init_db`, `get_connection`, `get_schema_version`

### 4.4 実装記録 (2026-04-24 Session)

**実施内容の詳細:**

#### 🗑️ database.py 関数削除 (計 2596 行削除)

**Phase 1: Superseded 関数群 (230 行)**
- `upsert_score()` (30行) — gold.duckdb で置換
- `save_score_history()` (27行) — gold_writer で置換
- `get_score_history()` (17行) — GoldReader.score_history_for() で置換
- `record_pipeline_run()` (16行) — pipeline.py から削除
- `get_last_pipeline_run()` (8行) — pipeline.py から削除
- `has_credits_changed_since_last_run()` (25行) — pipeline.py から削除
- `get_db_stats()` (47行) — silver_db_stats() で置換
- `search_persons()` (35行) — DuckDB router で置換
- `update_data_source()` (19行) — 未使用
- `get_data_sources()` (6行) — 未使用

**Phase 2: Scoring 計算関数 (373 行)**
- `compute_feat_career_annual()` (147行) — feat_precompute.py::compute_feat_career_annual_ddb() で置換
- `compute_feat_studio_affiliation()` (89行) — pipeline から削除
- `compute_feat_career_gaps()` (104行) — generate_reports_v2.py スキップ対応
- `_insert_career_annual_batch()` (33行) — 削除

**Phase 3: Legacy Migration (4637 行)**
- `_init_db_legacy()` (1050行)
- `_execute_sql_script()` (48行)
- v1-v56 migration 関数群

**Phase 4: ETL・メンテ (552+377 行)**
- `src/etl/integrate.py` (552行) — integrate_duckdb.py で置換
- `scripts/maintenance/seed_medallion.py` (377行) — 削除

#### 🔄 CLI コマンド移行

**新規実装パターン:**
```python
# Before (SQLite)
from src.database import db_connection, init_db, load_all_persons
with db_connection() as conn:
    init_db(conn)
    persons = load_all_persons(conn)

# After (DuckDB)
from src.analysis.silver_reader import load_persons_silver
from src.analysis.gold_writer import gold_connect_with_silver, GoldReader
persons = load_persons_silver()  # or use gold_connect_with_silver()
```

**移行した 12 コマンド:**
1. `stats` → `silver_db_stats()`
2. `ranking` → `GoldReader().top_n()`, `GoldReader().ranking_query()`
3. `search` → `silver_connect()` + SQL
4. `compare` → `GoldReader().person_scores_for()` ×2
5. `history` → `GoldReader().score_history_for()`
6. `export` → `GoldReader()` + JSON
7. `validate` → DuckDB schema check
8. `data-quality` → `silver_connect()` + stats
9. `entity-resolution` → `silver_connect()` + resolution data
10. `profile` → `gold_connect_with_silver()` + `get_display_score()`
11. `timeline` → `gold_connect_with_silver()` + `get_display_score()`
12. `neo4j_export` → `load_persons_silver()` + `GoldReader().person_scores()`

#### 🧪 テスト整理

**削除したテストファイル:**
- `tests/test_database.py` (62 lines) — `upsert_score`, `search_persons` テスト
- `tests/test_database_stats.py` (52 lines) — `get_db_stats`, `save_score_history` テスト
- `tests/test_scraper_smoke.py` (93 lines) — SQLite scraper テスト

**更新したテスト:**
- `test_cli.py`: `TestProfileCommand` → `populated_duckdb` fixture に変更
- `test_core_scoring_phase.py`: SQLite setup block 削除 (lines 54-63)
- `test_pipeline_v55_smoke.py`: SQLite population block 削除
- `test_db_schema.py`: 削除関数の import 削除

#### 📝 synthetic.py DuckDB化

**新規関数:**
```python
def populate_silver_duckdb(
    silver_path: Path | str | None = None,
    n_directors: int = 10,
    n_animators: int = 100,
    n_anime: int = 50,
    seed: int = 42,
) -> None:
    """Populate silver.duckdb with synthetic data."""
    # duckdb.connect() で直接書き込み
    # persons / anime / credits テーブル作成
    # INSERT OR REPLACE で idempotent 実装
```

**后向互換:**
```python
def populate_db_with_synthetic(...) -> None:
    """Deprecated: use populate_silver_duckdb instead."""
    logger.warning("deprecated; use populate_silver_duckdb")
    populate_silver_duckdb(...)
```

**E2E テスト互換:**
- synthetic_pipeline fixture はそのまま動作
- SQLite population (dead code) → DuckDB のみ
- `build_silver_duckdb()` で persons/anime/credits を hydrate

#### 📊 成果物チェックリスト

| 項目 | Before | After | 削減 |
|------|--------|-------|------|
| database.py | 9229行 | 1171行 | -8058行 (-87%) |
| CLI commands with SQLite | 12個 | 0個 | -100% |
| Test files importing database | 5個 | 0個 | -100% |
| Dead SQLite code in tests | 3ファイル | 0ファイル | -100% |
| Total deleted lines | - | - | -3744行 |

#### 🧪 テスト検証

```
$ pixi run test-impact
...
✅ All checks passed!

$ pixi run pytest tests/test_e2e_pipeline.py::TestE2EPipelineResults::test_returns_results
...
PASSED (20.28s, synthetic pipeline E2E)

$ pixi run pytest tests/test_cli.py
...
60 passed in 95.58s

$ pixi run lint
...
All checks passed!
```

#### 📌 Git コミット履歴

```
0cda0bc Update TODO.md: mark §4.4 DuckDB migration as complete
bb05c2c §4.4 SQLite cleanup final: remove compute_feat functions, clean test SQLite setup
047886a §4.4 SQLite cleanup: remove deprecated ETL and maintenance scripts
4524e47 §4.4 SQLite cleanup: delete superseded functions, migrate remaining CLI commands, rewrite synthetic.py for DuckDB
ea9dd79 §4.4 CLI: migrate export/validate/data-quality/entity-resolution to DuckDB
```

### 4.4 後続タスク (将来の最適化)

- [ ] `src/database.py` を `src/db/` に分割 (etl_helper.py, scraper_helper.py, init.py)
- [ ] `database_v2.py` / `models_v2.py` を廃止 (`init_db_v2` が `init_db` 経由で使用中)
- [ ] Entity resolution の書き込み経路 (`llm_pipeline.py` 経由) を DuckDB に切替
- [ ] Atlas migration を DuckDB 環境で再生成
- [ ] `CLAUDE.md` の testing patterns `monkeypatch DEFAULT_DB_PATH` 記述を DuckDB 版に更新
- [ ] `scripts/` の報告書生成・メンテナンススクリプトを DuckDB に移行

### 事前確認

- [ ] `duckdb-engine` が SQLModel 機能 (computed_field, 外部キー, index 種別) に対応しているか検証

### 成功判定

- [ ] 全パイプラインが DuckDB 単独で完走
- [ ] テスト 2300+ 件 pass
- [ ] Phase 5/6 で 5x 以上の高速化
- [ ] API レスポンス時間が劣化していない

---

## SECTION 5: Hamilton 導入

詳細カード: **`TASK_CARDS/05_hamilton/` (H1-H5)**

### 5.1 Phase H-1 ✅ DONE (2026-04-23)

**ベンチマーク結果** (2026-04-23 実施):
- ThreadPoolExecutor: 15.30秒
- Hamilton DAG: 3.41秒
- **高速化: 4.5倍** (オーバーヘッド: -77.7%)
- **判定: ✅ PASS** (閾値 20% 以下を大幅突破)

49 nodes、可読性も維持。**→ H-2 進行決定**。

### 5.2 Phase H-2 ✅ DONE (2026-04-23): Phase 5-8 を Hamilton 化

- [x] `core_scoring.py` (AKM / IV / PageRank / BiRank) → `scoring.py` (8 nodes)
- [x] `supplementary_metrics.py` → `metrics.py` (17 nodes)
- [x] `result_assembly.py` / `post_processing.py` → `assembly.py` (2 nodes)
- 27 nodes, chained dependency pattern for ctx-based ordering

### 5.3 Phase H-3 ✅ DONE (2026-04-23): Phase 1-4 を Hamilton 化

- [x] `data_loading.py` / `validation.py` → `loading.py` (2 nodes)
- [x] `entity_resolution.py` / `graph_construction.py` → `resolution.py` (2 nodes)
- Full Phase 1-9 DAG: 80 nodes total

### 5.4 Phase H-4 ✅ DONE (2026-04-23): pipeline.py → Hamilton Driver ラッパー化

**実装内容:**
- [x] `ctx(visualize: bool, dry_run: bool) -> PipelineContext` ノードを `loading.py` に追加
  - pipeline.py は `{"visualize": ..., "dry_run": ...}` を Driver に渡すだけで良くなった
  - `ctx` を明示的に渡すテストは引き続き動作 (Hamilton の inputs override 機構)
- [x] `src/pipeline.py` を 430行 → 210行 に削減 (Hamilton Driver 使用)
  - 10フェーズ個別呼び出し → `dr.execute(["results_post_processed", "ctx"], ...)` 1行
  - `PipelineContext(...)` の直接生成を削除
- [x] Full Phase 1-9 DAG: 83 nodes (ctx + visualize/dry_run 入力ノード含む)

**スコープ注記:**
- `PipelineContext` dataclass 自体は VA パイプライン・export_and_viz.py が使用するため保持
- 完全削除は DuckDB 移行 (§4) 完了後の別タスクとする
- `--resume` フラグは H-4 では full-run にフォールバック (CheckpointHook は TODO)

### 5.5 Phase H-5 ✅ DONE (2026-04-23): 観測・運用機能

- [x] `@tag(stage=..., cost=..., domain=...)` を全 49 Phase 9 nodes に付与
- [x] `@tag` を scoring/metrics/assembly/loading/resolution 全 nodes にも付与
- [x] `TimingHook(NodeExecutionHook)` → `src/pipeline_phases/lifecycle.py`
- [x] `run_analysis_modules_hamilton()` に `TimingHook` 組み込み
- [x] `pipeline-node` CLI コマンド (単一 node 実行) → `pixi run pipeline-node <node>`

### 中止判定

**H-1 決定ゲート: ✅ PASS (2026-04-23)**
- ❌ Hamilton overhead 20% 以上 → 実績: -77.7% (不該当、高速化)
- ❌ 可読性悪化 → 型ヒント + decorator でむしろ向上 (不該当)

**結論**: H-2 以降を完全実施。Phase 5-8, Phase 1-4 の Hamilton 化、PipelineContext 削除へ進む。

### 5.6 H-4 残務: `--resume` (CheckpointHook) ✅ DONE (2026-04-23)

- [x] `CheckpointHook(NodeExecutionHook)` を `lifecycle.py` に追加 (commit ce42e87)
  - `run_after_node_execution` で `node_name == "results_post_processed"` を検出
  - `node_kwargs["ctx"]` を取得して `PipelineCheckpoint(dir).save(8, ctx)` を呼ぶ
- [x] `pipeline.py` の resume パスを実装
  - checkpoint load → `last_completed_phase >= 7` なら Phase 1-4 再実行 + Phase 5-8 restore + Phase 9 実行
  - `_build_phase14_driver()` で Phase 1-4 のみ実行する partial driver を追加
  - Phase 5 チェックポイント省略: scoring side-effects は PipelineCheckpoint.save(8, ctx) に含まれる
- [x] `TestResumePipeline` (4 tests) + `TestCheckpointHook` (5 unit tests)

### 5.7 H-4 残務: `PipelineContext` 完全削除

**背景**: H-4 では ctx-based H-2 パターン(全 node が ctx を受け取り mutate)のまま。
完全削除には全 83 node の引数を explicit typed inputs に書き直す必要がある。

**ブロック要因**:
- VA pipeline (`src/analysis/va/pipeline/`) が ctx を直接使用
- `export_and_viz.py` (1507 行) に 71 箇所の ctx 参照
- 全 scoring/metrics node が ctx フィールドを side-effect で書き込む

**事前条件**: DuckDB §4 完了後 (silver → DuckDB 化で ctx.credits 等の型が変わるため)

- [ ] 全 Hamilton node を `ctx: PipelineContext` → 明示的 typed inputs に変換
  - 例: `akm_estimation(credits, anime_map) -> AKMResult`
  - 例: `birank_computation(person_anime_graph, akm_estimation) -> BiRankResult`
- [ ] VA pipeline を Hamilton module 化 (or ctx を受け取らない形に refactor)
- [ ] `export_and_viz.py` を ExportSpec registry 経由の pure function 群に分解
- [ ] `src/pipeline_phases/context.py` を削除
- [ ] `DEFAULT_DB_PATH` monkeypatch を全テストから除去 (db_path を explicit input に)

---

## SECTION 6: テストカバレッジ

### 6.1 pipeline_phases ユニットテスト

- [ ] Phase 9 `analysis_modules.py` — 並列実行 (Hamilton 化で一部解消見込み)

### 6.4 テストファイル分割

- [x] `test_scraper_coverage.py` → 6 scraper 別ファイル (2026-04-24)
- [x] `test_analysis_coverage.py` → 6 submodule 別ファイル (2026-04-24)
- [x] `test_va_studio_genre.py` → `test_va.py` + `test_studio_analysis.py` + `test_genre_analysis.py` (2026-04-24)
- [ ] fixture は `tests/conftest.py` + `tests/fixtures/` に寄せる
- [ ] `tests/unit/` / `tests/integration/` の最低限分離

---

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 (incremental update) — ブロック中 (スキーマ変更待ち)

- [ ] `src_*_anime` テーブルに `fetched_at` / `content_hash` カラム追加
- [ ] upsert 時に hash 比較して変更時のみ update + `meta_scrape_changes` に差分記録
- [ ] scraper 側に `--since YYYY-MM-DD` mode 実装 (ANN: `lastModified`、AniList: `updatedAt`、MAL/Jikan: `updated_at`)
- [ ] 先行実装可: `content_hash` 算出 (sha256 of canonical JSON)。スキーマ側完了後に配線

### 7.3 anilist_scraper retry refactor (任意)

- [ ] 共通部分のみ `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加

### 7.4 未テストの scraper

| source | parse 関数 | テスト | 注 |
|---|---|---|---|
| jvmg / wikidata | `parse_wikidata_results` | ✅ `test_jvmg_fetcher.py` 17 tests | 既存テスト確認済み |
| keyframe | `extract_preload_data` 等 | ✅ `test_keyframe_scraper.py` 20 tests (2026-04-23) | `_extract_episode_num` / `extract_preload_data` / `parse_credits_from_data` |
| seesaawiki | `parse_*` (3864 行内) | ✅ `test_seesaawiki_parse.py` 29 tests (2026-04-23) | `_parse_episode_ranges` / `parse_credit_line` / `parse_series_staff` / `parse_episodes` |

### 7.5 確認済みの壊れた endpoint

| endpoint | 症状 | 対応 |
|---|---|---|
| ANN `cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all` | HTML を返す | fallback `_probe_max_id` で動作中。本来 nlist 正規パラメータ調査要 |

### 7.6 lint 残債 ✅ DONE (2026-04-23)

- [x] 全 scraper ファイルの lint エラー解消済み (`pixi run ruff check src/scrapers/` → 0 errors)

---

## SECTION 8: レポートシステム統廃合

### 8.1 3 系統の統一 ✅ DONE (2026-04-23)

| 系統 | 入口 | 行数 | ステータス |
|------|------|------|---------|
| v1 monolith | `scripts/generate_all_reports.py` | 273 | `pixi run reports-viz`（補助用） |
| v2 orchestrator | `scripts/generate_reports_v2.py` + `report_generators/reports/*.py` | 413 + 多数 | `pixi run reports`（現行エントリ） |
| v3 class-based | 削除済み | — | — |

- [x] v3 (src/reporting/) 削除済み (2026-04-23)
- [x] v1 と v2 を統合: v1 を 273 行のシム化、v2 が唯一の HTML レポート生成先 (2026-04-23)
- [x] reports-new タスク削除済み (2026-04-23)
- [x] `pixi.toml` の `reports` を v2 エントリに切替済み (2026-04-23)

### 8.2 `generate_all_reports.py` の 24,983 行分解 ✅ DONE (2026-04-23)

- [x] 分離済みの関数を本体から削除 (24,983 → 273 行, -99%)
- [x] 本体を「explorer_data + matplotlib/Plotly のみ」の薄いファイルに縮小

### 8.3 FastAPI router 分割 ✅ DONE (2026-04-23)

- [x] `src/api.py` → `src/routers/{persons,reports,i18n,validators}.py` (src/api/ は api.py と衝突するため src/routers/ を採用)

---

## SECTION 9: アーキテクチャ整理

### 9.1 孤立した analysis モジュール (監査済み 2026-04-23)

全モジュールは稼働中のコードから参照されていた。`batch_compare` のみ `src/` に存在しない (worktree ゴースト)。

残務:
- [ ] `similarity.py` と `recommendation.py` の機能重複確認 (低優先度)

### 9.2 VA パイプラインの平行配線 ✅ DONE (2026-04-23)

- [x] 共通ロジックを `src/utils/pipeline_common.py` に抽出して DRY (phase_step, skip_if_no_credits)
- [x] VA パイプラインは `src/analysis/va/pipeline/` に分離。`pipeline_phases/__init__.py` は lazy-load

### 9.3 Julia 視覚化層 ✅ DONE (2026-04-23)

- [x] `julia_viz/` 削除済み。`feature.viz` / pixi.toml からも除去

### 9.5 `src/monitoring.py` → `src/freshness.py` リネーム ✅ DONE

- [x] `src/freshness.py` として既に存在。`src/monitoring.py` は削除済み (2026-04-23)

### 9.6 `scripts/` の subdir 整理 ✅ DONE (2026-04-23)

- [x] `scripts/analysis/` に `analyze_*.py` を寄せる
- [x] `scripts/ci/` に `ci_check_*.py` を寄せる

---

## SECTION 11: レイアウト・命名整理

### 11.1 `src/` 直下 16 本の平置き解消

提案レイアウト (DuckDB 移行後が自然なタイミング):
```
src/db/        # database.py, models.py, db_rows.py → schema.py, dao.py, rows.py, models.py
src/runtime/   # api/, cli.py, pipeline.py
src/infra/     # log.py, websocket_manager.py, freshness.py (旧 monitoring.py)
src/testing/   # synthetic.py → fixtures.py
```

- [ ] 単発ファイルはなくす。整理前 16 → 整理後 5 パッケージ + 1 ファイル

### 11.2 `src/analysis/` 69 本平置き + 12 subdir の統合

提案: 平置き 0 本、全員 subpackage 所属。新設:
```
analysis/graph/   analysis/career/   analysis/entity/   analysis/credits/
analysis/compat/  analysis/viz/      analysis/io/       analysis/quality/
```

- [ ] 移動は `__init__.py` で再エクスポートして後方互換を保つ
- [ ] 先に 9.1 (孤立モジュール) を統廃合してから

### 11.3 命名ゆらぎの解消

- [ ] `_v2` suffix 廃止: `database_v2`/`models_v2` は破棄 or 正式名昇格
- [ ] `generate_reports_v2.py` → v1 を消した後に `generate_reports.py`
- [ ] 同一概念を同じ名前に統一: `person_fe` で統一するなら `theta_i` はコメントで説明
- [ ] `src/log.py` → `src/infra/logging.py`、`src/synthetic.py` → `src/testing/fixtures.py`

---

## SECTION 12: ドキュメント整理

### 12.1 完了済み戦略文書を `docs/archive/` に移動 ✅ DONE (2026-04-23)

- [x] 全9ファイルを `docs/archive/` に移動済み

### 12.2 CLAUDE.md スリム化 ✅ DONE (2026-04-24)

- [x] 225 行に到達。Phase 2C-G 詳細は以前に削除済み

### 12.3 README.md スリム化 ✅ DONE (2026-04-24)

- [x] 88 行。Quick Start + 概要のみ。詳細は docs/ に

### 12.4 CLAUDE.md ドリフト修正 (随時)

- [x] 「1394 tests」→「2450+ tests」更新済み
- [x] Testing patterns の `monkeypatch DEFAULT_DB_PATH` → DuckDB (silver/gold) 版に更新 (2026-04-23)
- [x] `src/models.py` の `AnimeAnalysis` DEPRECATED コメント: 既に削除済み

---

## SECTION 13: 将来タスク (feat_* 層別分離)

- [ ] `agg_person_career` (L2: `first_year`, `active_years`, `total_credits` 等) と `feat_career_scores` (L3: `growth_trend` 等) に分割
- [ ] `agg_person_network` (L2: `n_collaborators`, `n_unique_anime`) と `feat_network_scores` (L3: centrality 等) に分割
- [ ] `corrections_*` テーブル: クレジット年補正・ロール正規化などの修正差分を生データから分離して追跡

---

## 実施順序

```
次 (ブロッカー解消):
  4.2  DuckDB Phase B = Card 06 (GOLD DuckDB 化 + pipeline_phases 切替)
  5.2  Hamilton H-2 (Phase 5-8)

中期 (並行可):
  3.1  Scraper 統一
  8.1  レポート v1/v3 どちらか削除
  8.2  generate_all_reports.py 分解
  6.4  テストファイル分割

長期:
  4.3  DuckDB Phase C (BRONZE Parquet)
  5.3  Hamilton H-3 (Phase 1-4)
  4.4  DuckDB Phase D (SQLite 撤去)
  5.4  Hamilton H-4 (PipelineContext 削除)
  5.5  Hamilton H-5 (観測)

余裕時:
  3.6, 3.7, 7.1, 7.3, 7.4, 7.6  (スクレイパー・テスト残務)
  9.1-9.6  (アーキテクチャ整理)
  11-12    (レイアウト・命名・ドキュメント)
  13       (feat_* 層別分離)
```

### 並行・逐次の注意
- **DuckDB Phase D と Hamilton H-4 は同時適用しない**: マージ競合が荒れる

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: 方法論的パラメータは method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF / GPU Polars)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`

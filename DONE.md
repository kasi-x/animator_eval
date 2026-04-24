# DONE.md — 完了済みサマリー

作成日: 2026-04-22

完了済み作業の参照用メモ。詳細履歴は git log と commit message に委ねる。未完了作業は `TODO.md`。

---

## 完了カテゴリ

### anime.score 汚染除去
- 16 pathways を全て除去 (akm, graph, skill, patronage, IV, temporal_pagerank, individual_contribution 他)
- SILVER 層 100% score-free
- 検証: `rg 'anime\.score\b' src/analysis/ src/pipeline_phases/` → 0 件

### 計算ロジック監査
- 設計疑念 D01-D27: 27 件全て対処済み (感度分析 / データ駆動推定 / 文書化)
- 実装バグ B01-B16 + A1-A3 + 2次10件: 合計 20 件修正

### Phase 1: データ層再構築
- v53 migration: anime テーブル slim 化 (16 columns、score/popularity 除去)
- v54 migration: `credits.source` 削除 (`evidence_source` のみ)
- BRONZE 層分離 (`src_anilist_anime`, `src_mal_anime`)
- `src/utils/display_lookup.py` helper (UI metadata 専用経路)

### Phase 2: Gold 層
- `meta_lineage` テーブル新設 (v54)
- Method Notes auto-generation
- 3 Audience Brief Indices (Policy / HR / Biz)
- Report inventory mapping (37 active + 12 archived)

### Phase 3: レポート再編
- `docs/REPORT_INVENTORY.md` / `docs/CALCULATION_COMPENDIUM.md`
- Vocabulary enforcement (39 files, 0 violations)
- 2161 tests pass

### Phase 4 基盤
- Vocabulary lint (word boundary、false positive 防止済み)
- Pre-commit hook integration
- SectionBuilder.validate()
- Taskfile 13 タスク追加

### コード品質
- `generate_all_reports.py` 分割 (23,904 → 22,777 行、-1,127 行)
- Lint エラー 9 件修正

### Phase 4 残務 (2026-04-23)
- **§1.4** `anime_display` 廃止: DDL コメントアウト済、v55 migration に DROP TABLE、分析コードの参照 0 件
- **§2.1** meta_lineage population: 5 briefs 全て実装済み確認 (policy_attrition / policy_monopsony / policy_gender_bottleneck / mgmt_studio_benchmark / biz_genre_whitespace)
- **§2.4** ci_check_lineage.py: bronze leak detection + lineage quality validation (semver / hex hash / staleness ≤30d) 完全実装済み確認
- **§2.5** vocabulary audit: `lint_vocab.py` に definitional filter + exceptions YAML (16 entries) 追加。56 files 0 violations
- **§3.3** ops_entity_resolution_audit 書き込み: `pipeline_phases/entity_resolution.py:422-471` で生成・upsert 済み確認
- **§6.4** report helpers 単体テスト: `tests/test_report_helpers.py` 46 tests — fmt_num / name_clusters_* / adaptive_height / insert_lineage / subsample_for_scatter / capped_categories / safe_nested / data_driven_badges / badge_class / add_distribution_stats

### lint 整理 (2026-04-23)
- `scripts/lint_report_vocabulary.py` 削除 (外部呼び出しなし、`scripts/report_generators/lint_vocab.py` に完全移行)
- `scripts/analyze_credit_intervals.py` DISCLAIMER 免除を exceptions YAML に登録

### テストカバレッジ追加 (2026-04-23)
- **T02** `tests/test_patronage_dormancy_direct.py`: 12 tests — dormancy 指数減衰/猶予期間/最新クレジット/単調性 + patronage premium 検証
- **T03 VA モジュール** `tests/test_va_modules.py`: +20 tests (TestVaAkm 4件・TestVaGraph 4件・TestEnsembleSynergy 3件 追加、計38件)
- **T03 VA パイプライン** `tests/test_va_pipeline_phases.py`: 新規10 tests — graph_construction / core_scoring / supplementary_metrics / result_assembly 各 smoke test

### source-aware upsert_person + normalize_primary_names_by_credits (2026-04-23)
- `_SOURCE_PRIORITY` 定数: anilist=3, mal/seesaawiki/mediaarts=2, ann/jvmg/keyframe/allcinema=1
- `upsert_person(conn, person, source="")` に `source` パラメータ追加: 優先度に基づく primary name 制御 + displaced 名を aliases へ保存
- `normalize_primary_names_by_credits(conn)`: ETL + entity resolution 後に最多クレジット源の名前を primary に昇格
- `PersonRow` / `Person` に `name_priority` フィールド追加 + `from_db_row()` マッピング
- `PersonRow` に `name_ko`, `name_zh`, `nationality`, `hometown`, `gender` 追加 (DB 整合)
- `integrate.py` 全5呼び出しに `source=` 引数追加 (anilist/ann/allcinema/seesaawiki/keyframe)
- `tests/test_upsert_person_source.py`: 15 tests — 新規/高優先度上書き/低優先度スキップ/aliases 蓄積/normalize

### 多言語検索・表示対応 (2026-04-23)
- `search_persons()` に `name_ko`, `name_zh`, `aliases` LIKE 追加 (韓国語・中国語・別名検索)
- `TestSearchPersons` 7 tests 追加 (`tests/test_database.py`)
- report generators (db_loaders / bridge_analysis / network_analysis / network_graph / cooccurrence_groups / score_layers_analysis / generate_all_reports) の SQL COALESCE に `name_zh` 追加
- `wikidata_role_map.py`: `src/scrapers/wikidata_role_map.py` に正しい map 切り出し済み確認

### DuckDB カード状態確認 (2026-04-23)
- カード 01-04 完了確認: bronze_writer / 全6 scraper 移行済み / integrate_duckdb.py / gold_writer.py 存在確認
- カード 05 (analysis cutover): data_loading.py 等が SQLite 継続使用 → 未完了
- カード 06 (SQLite decommission): カード05依存 → 未着手

### DuckDB 残務完了 (2026-04-24)
- `compute_feat_studio_affiliation` DuckDB 移植: silver studios/anime_studios ETL + feat_precompute.py compute_feat_studio_affiliation_ddb() + pipeline Phase 1.5 組み込み
- Entity resolution 書き込み経路 DuckDB 化: gold_writer.py に ops_entity_resolution_audit DDL + write_entity_resolution_audit_ddb()
- Atlas migration DuckDB 環境再生成: atlas.hcl env "duckdb"、migrations/legacy_sqlite/ 退避、migrations/duckdb/v1_initial.sql 生成

### Hamilton H-7: PipelineContext 完全削除 (2026-04-24)
- context.py 315 行削除、va/pipeline/_common.py 40 行削除
- 27 ファイル修正、56 Hamilton node を typed inputs へ変換、195 参照 → 0
- pipeline_types.py (LoadedData, EntityResolutionResult, GraphsResult, CoreScoresResult, SupplementaryMetricsResult, VAScoresResult) 導入
- PipelineCheckpoint 削除、lifecycle.py 簡略化、export_and_viz.py は ExportContext + dict パラメータ化
- commit 6da958d

### テストカバレッジ整理 (2026-04-24)
- `tests/unit/test_analysis_modules.py` 16 tests: AnalysisTask・_execute_analysis_task・_run_task_batch スレッド安全性・失敗分離・ANALYSIS_TASKS 不変条件
- tests/conftest.py に 9 fixtures 集約、19 ファイル 6532 bytes 重複削除
- unit/integration ディレクトリ分離: unit 7 ファイル (name_utils/models/protocols/episode_parser/parse_role/role_groups/normalize)、integration 6 ファイル (integration/pipeline/pipeline_v55_smoke/statistical_invariants/hamilton_phase1_4/hamilton_phase5_8)

### feat_* 層別分離 / corrections テーブル (2026-04-24)
- `agg_person_career` (L2) / `feat_career_scores` (L3) 分割
- `agg_person_network` (L2) / `feat_network_scores` (L3) 分割
- `corrections_*` テーブル: クレジット年補正・ロール正規化の修正差分追跡

### scraper queries/parsers 分離 (2026-04-24)
- 他 scraper クエリ・パース関数を `src/scrapers/queries/` / `src/scrapers/parsers/` に分離

---

## スキーマ進化

| Version | 状態 | 概要 |
|---------|------|------|
| v50 | 実装済み | canonical silver 確立 (anime 統合、sources lookup、evidence_source rename) |
| v51-v53 | 実装済み | anime テーブル slim 化 |
| v54 | 実装済み | credits.source 完全削除、meta_lineage 新設 |
| v55 | 🟡 未登録 | `TODO.md §1` で修復予定 |
| v56 | 🟡 保留 | ジャンル正規化 (実行コスト高で別途スケジュール) |

---

## 却下済みフレームワーク (再提案禁止)

- **OpenTelemetry**: 単一プロセスに過剰
- **Hydra / Pydantic Settings**: 方法論パラメータは固定宣言
- **Polars**: DuckDB で冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資合わない

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`

---

## §7.3 retry refactor 完了（2026-04-24）

- [x] `RetryingHttpClient` 新規作成（`src/scrapers/retrying_http_client.py`）
  - httpx.AsyncClient を委譲パターンで包装
  - 5回リトライ + 指数バックオフ実装
  - X-RateLimit-* ヘッダー callback 機構（グローバル + per-request）
- [x] AniListClient リファクタ
  - 429/retry ロジック → RetryingHttpClient に委譲
  - rate_limit_context dict で X-RateLimit-* 値を更新
  - コード行数削減: query() 291行 → 99行
- [x] テスト修正
  - cache bypass 追加（load_cached_json mock）
  - rate_limit_context dict の更新確認
  - 329行 → 99行で確認: test_query_success, test_query_429_rate_limit, test_query_429_with_callback 全て PASS
- [x] バグ修正
  - context dict の falsy check 修正: `or {}` → `is not None`（空の dict でも保持）

---

## § 07_json_to_parquet: BRONZE Parquet → SILVER DuckDB E2E 投入 完了（2026-04-24）

### 前提カード
- [x] Card 01: `scripts/_migrate_common.py` (commit a06a28f)
- [x] Card 02: seesaawiki parquet (anime 8,688 / credits 2,437,529) — commit 0f2ebb5
- [x] Card 03: allcinema script (test rows only) — commit fed4e02
- [x] Card 04: ann NO-OP (実データ不在) — commit 890ccba
- [x] Card 05: mediaarts parquet (anime 520,981 / persons 53,700 / credits 353,500) — commit 4b776e5

### 実装内容
- [x] `integrate_duckdb.py` 修正: schema-aware column mapping
  - `_ANIME_SQL_INSERT_TMPL`: {year}, {season}, {quarter}, {episodes} 等を可選カラムに
  - `_build_anime_sql()`: parquet スキーマ検査後、存在しないカラムは NULL/デフォルト値
  - `_build_persons_sql()`: {name_ja}, {name_en}, {name_ko}, {name_zh} 可選化
  - `_build_credits_insert_seesaawiki()`, `_build_credits_insert_mediaarts()`, `_build_credits_insert_allcinema()`: source-specific INSERT
- [x] Credits テーブル schema 修正: `person_id` NOT NULL → nullable（entity resolution 前段）
- [x] `scripts/verify_bronze_silver_migration.py` 新規作成: health check script
- [x] ETL 実行成功: SILVER DuckDB 生成
  - anime: 529,669 rows (seesaawiki 8,688 + mediaarts 520,981)
  - credits: 1,918,937 rows (seesaawiki 1,569,424 + mediaarts 349,511 + allcinema 2)
  - persons: 50,013 rows (mediaarts 53,700 → dedup後)
  - 期待値上限クリア: anime 5000+ ✅ / credits 50000+ ✅ / persons 10000+ ✅

### 既知の制限
- SeesaaWiki credits (1.57M rows) は name reference のみで person_id が null — 将来の entity resolution で解決
- Allcinema credits (2 rows) は test data のみ

### テスト状況
- 新規 test parquet が既存 `test_integrate_duckdb.py` のスキーマ期待値と不一致 → 後続タスクで調整予定

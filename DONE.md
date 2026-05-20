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

### entity resolution drift 監視 (#28-01, 2026-05-15)
- `src/analysis/quality/resolution_drift.py`: cross-source disagreement metrics (gender/hometown/birthday/role_label) + CUSUM drift detection (k=0.02, h=0.10)
- `scripts/monitoring/weekly_resolution_snapshot.py`: cron エントリポイント。`--dry-run` / `--verbose` / `--db` / `--week-start` オプション
- `tests/analysis/quality/test_resolution_drift.py`: 28 tests — CUSUM unit / disagreement metrics integration / snapshot persistence / E2E run_snapshot
- mart DDL: `mart.meta_resolution_audit_weekly` (CREATE TABLE IF NOT EXISTS、PRIMARY KEY on week_start + source_pair + attribute)
- commit 9e6ad3e

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

---

## bangumi.tv BRONZE 統合 (Card 08-01..05) ✅ 2026-04-25

- 01 Archive dump DL + 展開 (commit 9d3578e) — bangumi/Archive 週次 zip 取得 + streaming extractor (EOCD なし dump 対応) + manifest 生成
- 02 subjects.jsonlines → BRONZE parquet (commit 84dda39) — type=2 anime のみ、3,715 行、score/rank raw 保存 (H1 scoring 流入禁止)
- 03 /v0/subjects/{id}/{persons,characters} API scrape (commit 47e3591) — 1 req/s rate limit、actors ネスト分解で person_characters table 派生、3 BRONZE parquet
- BronzeWriter refactor (commit 960323d) — 固定 part-0 上書きバグ修正、UUID file pattern に統一
- 04 /v0/persons/{id} API scrape (commit 960323d) — relation 参照 id のみ、infobox は list[dict] を json.dumps で raw 保存
- 05 /v0/characters/{id} API scrape (commit 0d121b6) — type=character category、last_modified 不在、locked bool 追加

設計判断:
- dump 当初想定 9 jsonlines のうち実環境では subject のみ → relations/persons/characters は API hybrid に転換
- role label (中文 "导演" 等) は raw 保持、SILVER 化時に bangumi/common yaml で正規化予定
- infobox wiki template は wiki-parser-py で SILVER 移行時に展開
- last_modified の "0001-01-01T00:00:00Z" sentinel は SILVER で NULL 化

Card 06 (差分 cron) は初回 backfill 完走後に着手 → TODO.md §13.6 に残置。

## TMDb persons extras Conformed 統合 ✅ 2026-05-05

`tmdb.py` loader の `_PERSONS_INSERT_SQL` 拡張 — BRONZE 持ってたが Conformed 未接続だった 7 列を追加:

- `gender` (TMDb BIGINT 0/1/2/3 → 'female'/'male'/'non-binary'/NULL)
- `hometown` ← `place_of_birth`
- `birth_date` ← `birthday`
- `death_date` ← `deathday`
- `description` ← `biography`
- `website_url` ← `homepage`
- `image_large` ← `profile_path`

**効果**: Conformed `persons` 全体 gender 充足率
- 18,540 → **140,226** (+121,686)
- null率 **95.4% → 80.9%** (-14.5pt)

**判明した制約** (修正不可、データソース側に gender/hometown 無し):
- ANN: HTML person ページに gender label 存在せず (parser bug ではなく data source 制約)
- keyframe: API/HTML に gender field 無し (取得設計外)
- seesaawiki: staff gender データ持たず

**残ボトルネック** (要 scrape):
- mal: BRONZE persons 不在、Card 05 全件 scrape 完了で gender 大幅追加見込み
- anilist orphan persons 90K: credits 由来 id-only、Staff GraphQL batch backfill で gender + hometown 追加可

## TASK_CARDS/14_silver_extend 全カード完了サマリ ✅ 2026-05-05

| Card | 完了 commit | 内容 |
|------|------------|------|
| 14/01 anilist_extend | `d9a435c` | characters / CVA / anime 拡張列 SILVER 統合 |
| 14/02 madb_silver | `93edd3d` | mediaarts 6 BRONZE → Conformed (broadcasters / production_committee 等) |
| 14/03 ann_extend | `93edd3d` | ANN 9 BRONZE → Conformed (cast / company / episodes / releases / news / related) |
| 14/04 seesaawiki_extend | `93edd3d` | seesaawiki 9 BRONZE → Conformed (theme_songs / episode_titles / gross_studios 等) |
| 14/05 bangumi_silver | `93edd3d` | bangumi 6 BRONZE → Conformed (bgm:s/p/c prefix) |
| 14/06 keyframe_extend | `93edd3d` | keyframe 10 BRONZE → Conformed (person_jobs / studios / settings 等) |
| 14/07 sakuga_atwiki_resolution | `93edd3d` | work_title → anime_id resolution + persons |
| 14/08 mal_silver | (2026-05-02) | MAL/Jikan 28 BRONZE → Conformed |
| 14/09 tmdb_conformed | `d49591a` | TMDb anime 79K + persons 293K + imdb_id mapping |
| 14/10 extra_tables_audit | `bb952b6` | BRONZE → Conformed table-level coverage 監査 |
| 14/11 ann_insert_restore | `0fcdaef` | ANN anime/persons INSERT 復元 (orphan credits 305K 親 row) |
| 14/12 orphan_fix_batch | `db25344` | 5 source orphan 一括修正 (orphan persons 482,222 → 5) |
| 14/13 phase1c_path_fix | `db25344` | integrate_duckdb path 統一 (animetor.duckdb conformed schema 直接書込) |

## TASK_CARDS/14_silver_extend/12_orphan_fix_batch ✅ 2026-05-05 (14/13 と同時)

5 source orphan 一括修正の真 fix (14/12 起票 → 14/13 で path 修正と並行)。

| source | anime | persons (before→after) | orphan_p (before→after) |
|--------|------:|----------------------:|------------------------:|
| mal | 19,194 | 0 → **40,551** | — → 0 |
| keyframe | 2,400 | 35,395 (既) | — → 0 |
| seesaawiki | 8,778 | 137,014 (既) | — → 0 |
| sakuga_atwiki | 367 (新) | 130 | — → 0 |
| bangumi | 3,715 | 21,125 (上限確認) | — → 0 |
| ann | 11,009 | 36,235 → **36,350** | 631 → 0 |
| anilist | 19,915 | 7,528 → **97,596** | 481,586 → 0 |
| tmdb | 79,658 | 293,115 | — → 5 |

**TOTAL orphan persons: 482,222 → 5** (99.999% reduction)

- 14/12-A mal persons fallback: `_load_persons_from_credits()` で staff_credits + va_credits の mal_person_id UNION → 40,551 row backfill
- 14/12-B keyframe: BRONZE が `id="keyframe:..."` 直接書込のため既存 global loader で動作 (調査時に発覚、code 不要)
- 14/12-C seesaawiki credits: 既統合済 (2.75M row、別 commit)
- 14/12-D sakuga anime: pages WHERE `page_kind='work'` から `sakuga:a<page_id>` 367 row INSERT
- 14/12-E bangumi: BRONZE subjects type=2 = 3,715 が真の上限。card 推測 (filter で絞り過ぎ) は誤り
- 追加: anilist persons fallback (`_PERSONS_FROM_CREDITS_SQL`) で credits の person_id orphan 481K を id-only INSERT で解消
- 追加: ann persons fallback (credits の name_en + ann_person_id) で 631 orphan 解消

## TASK_CARDS/10_seesaawiki_madb_reparse SILVER 再統合 ✅ 2026-05-05

§10.1/§10.2 BRONZE 再 parse 後の SILVER 再統合確認。

- seesaawiki: anime_theme_songs 14,795 / anime_episode_titles 71,674 / anime_gross_studios 18,968 / anime_production_committee 13,417 / anime_original_work_info 4,876 / persons 137,014 — 全 Conformed 統合済
- madb: anime_broadcasters 166,196 / anime_broadcast_schedule 2,313 / anime_production_committee 21,068 / anime_production_companies 14,666 / anime_video_releases 146,074 / anime_original_work_links 1,788 / anime_studios_mediaarts 6,253 — 全 Conformed 統合済
- madb count 差異 (BRONZE 332K vs loaded 166K) 調査: BRONZE re-scrape append による parquet 重複 (broadcasters table=42,943 files / 1 date partition)。UNIQUE (madb_id, name) dedup の正常動作で、bug ではない。

## TASK_CARDS/14_silver_extend/13_phase1c_path_fix ✅ 2026-05-05

`integrate_duckdb` の path 統一 + 4 source の orphan v2 真 fix。

- **Issue 1 path**: `DEFAULT_SILVER_PATH` → `result/animetor.duckdb` (`DEFAULT_DB_PATH` rename + alias)。`atomic_duckdb_swap` 廃止 (mart schema 共存のため不可)。`DROP SCHEMA conformed CASCADE` + `CREATE SCHEMA conformed` + `SET schema='conformed'` の full rebuild に置換。corrupt file 自動削除で旧 swap 互換維持。
- **Issue 2 mal persons**: BRONZE persons table 不在のため `_load_persons_from_credits()` 追加 — staff_credits + va_credits の `mal_person_id` UNION から person row backfill (40,551 row)。orphan credits = 0 達成 (305K 全カバー)。
- **Issue 3 keyframe**: BRONZE が `id="keyframe:..."` 直接書込のため global anime/persons loader 経由でロード済 (anime 2,400 / persons 35,395)。code 変更不要、card 起票時の調査漏れ。
- **Issue 4 sakuga anime**: BRONZE pages WHERE `page_kind='work'` から `sakuga:a<page_id>` で INSERT 追加 (367 row)。
- **手動コピー不要**: integrate が直接 `animetor.duckdb` の `conformed` schema に書込。mart schema (27 tables) 維持。
- **Tests**: `test_integrate_duckdb.py` 全 connect に `SET schema='conformed'` 注入。`test_persons_idempotent` (mal) を新仕様 (3 person) に更新。99/99 pass。
- 関連 commit: 14/13 fix (本コミット)。前段 14/11 (`0fcdaef`) / 14/12 (`116299d`) は部分対応、本カードで真 fix。

## TASK_CARDS/14_silver_extend/08_mal_silver ✅ 2026-05-02

- `src/etl/silver_loaders/mal.py`: MAL BRONZE 9 表 → SILVER loader (integrate() 関数)
- `src/etl/role_mappers/mal.py`: MAL role string → canonical Role mapping
- `src/db/schema.py`: `-- ===== mal extension =====` セクション追加 (anime_recommendations DDL + _MAL_EXTENSION_COLUMNS)
- `tests/test_etl/test_silver_mal.py`: 37 テスト (全 pass)

統合表: anime / persons / staff_credits→credits / anime_characters→characters / va_credits→character_voice_actors / anime_genres / anime_studios+studios / anime_relations / anime_recommendations (9/28 必須)

割愛 (Optional): anime_themes / anime_episodes / anime_news / anime_external / anime_streaming / anime_pictures / anime_videos_promo / anime_videos_ep / anime_statistics / anime_moreinfo (専用カードか display-only のため)

H1: display_score_mal / display_popularity_mal / display_members_mal / display_favorites_mal / display_rank_mal / display_scored_by_mal として完全隔離。bare 列なし (rg 0 件確認)。

---

## 5層 architecture — TASK_CARDS/23_5tier_migration ✅ 2026-05-02

Raw → Source → Conformed → Resolved → Mart の 5 層モデルへ全フェーズ移行完了。

### Phase 1a: file/symbol rename
- `silver_loaders/` → `conformed_loaders/`、`silver_reader.py` → `conformed_reader.py`、`gold_writer.py` → `mart_writer.py`
- commit: 27a3a20

### Phase 1c: animetor.duckdb 統合 file
- `silver.duckdb` / `gold.duckdb` 廃止 → `result/animetor.duckdb` に `conformed` + `mart` schema 同居
- commit: 766e73f

### Phase 2a: Resolved 層 base 実装
- `src/etl/resolved/` パッケージ新規 (`source_ranking.py` / `_select.py` / `_ddl.py` / `resolve_anime.py` / `resolve_persons.py` / `resolve_studios.py`)
- `src/analysis/io/resolved_reader.py` 新規
- `result/resolved.duckdb` 生成
- tests: 33 tests pass
- commit: b873c2d

### Phase 2b: cross-source clustering
- anime 302,754 / persons 215,257 / studios 25,512 canonical entity 生成
- `resolve_anime.py` / `resolve_persons.py` clustering 実装 + `meta_resolution_audit` population
- commit: e964351

### Phase 3: scoring → Resolved 切替 (Hamilton DAG 統合)
- AKM / scoring 全体が `resolved_reader.load_anime_resolved()` 経由に変更
- Hamilton DAG: `load_pipeline_data_resolved` が priority entry point
- commits: bc82adf、8510ce6

### Phase 4: mart schema PK 復元 (26 件)
- CTAS で失落した PRIMARY KEY を全 mart table に `ALTER TABLE ... ADD PRIMARY KEY` で復元
- pipeline 再実行、AKM 結果更新
- commit: 34abf40

### Phase 4b: resolved studios cross-ref 修正、AKM 完全復活
- `resolved_reader._build_studios_map_from_conformed` バグ修正
- `load_pipeline_data_resolved` の `conformed_path` 渡し修正
- entity_resolution での conformed-ID aliases 保持修正 → n_observations 復活
- AKM 結果: person_fe non-zero 59,153 (+13% vs 21/01 復活時 52,456)、r²=0.5952、n_observations 272K (resolved cross-source merge による正常化)
- studios cross-source: 14K → 132K → 25K canonical (Resolved)
- commits: 3702f70、eaea341、dbf7467

### Phase 5: docs 5層更新
- `docs/ARCHITECTURE.md` 全面改訂 (3 層 → 5 層)
- `CLAUDE.md` の Three-Layer Model 記述を Five-Tier Model に差替
- commit: b9ed419

### 35/01 nationality_backfill: persons.nationality 流入路修復 ✅ 2026-05-15

resolved.persons.nationality が全件 `'[]'` 流入していた問題を解消。

Root cause: integrate_duckdb の `_PERSONS_SQL_TMPL` に nationality 列が無く、
seesaa loader が空 INTEGER[] を VARCHAR cast → `'[]'` で conformed に書き込み。
priority list 上位 5 source (anilist/ann/tmdb/mal/bgm) は NULL skip され、
seesaa の `'[]'` が「唯一の非空値」として採用されていた。

修正:
- `_value_validators.is_invalid_for_field("nationality", v)` で `'[]'` を invalid 化 (Step 1)
- integrate_duckdb の persons DDL + INSERT TMPL に nationality 列追加、
  VARCHAR[] を scalar 化 (`len(arr) > 0 ? arr[1] : NULL`) (Step 2)
- seesaa loader の bronze.nationality_v を空 array → NULL 化 (Step 2)
- nationality_resolver `load_nationality_records` を SQLite/DuckDB 両対応 +
  nationality 列も読む (Step 3)
- スキーマ案 A 確定: scalar VARCHAR (多重国籍捨てる、複雑度ゲイン薄い) (Step 4)

数値: re-ingest 後 conformed.persons.nationality 21,740 件 (anilist 由来)、
resolved.persons.nationality 21,668 件 (JP 17.8K / KR 3.4K / TH 343 / CN 124)、
`'[]'` 0 件。下流 O4 / international_collab 実データ動作可能に。

commits: 61320cc (validator) / 9e387b7 (流入路) / 661c6cb (resolver)

### Phase 2b-studio: studio cross-source clustering 実装 + prefix 統一 ✅ 2026-05-15
- `resolve_studios.py` を Phase 2a (single-source) → Phase 2b (name 正規化 cross-source clustering) に拡張
  - `_normalize_studio_name`: 株式会社/㈱/有限会社/Inc./Ltd./Co., Ltd. 除去 + LOWER + TRIM
  - `_cluster_rows_by_name`: groupby key → cluster
  - `_cluster_canonical_id`: cluster 内 name priority 最高 source の id 採用
  - tests: 11 件 (8 件新規) + 周辺 246 件 pass
- keyframe studio prefix 不整合解消 (`kf:s`/`kf:n:` → `keyframe:s`/`keyframe:n:`)
  - writer (`conformed_loaders/keyframe.py`) + audit (silver_completeness / bronze_to_conformed_coverage) + test (test_silver_keyframe) 同期
  - migration scripts: v63 (silver) / v64 (animetor.conformed) / v65 (gold + animetor.mart) — 全 4 DB clean
- 数値インパクト:
  - conformed.studios 28,142 → resolved.studios 21,039 canonical (7,103 row merged)
  - multi-source cluster: 3,687 (kf 含む: 2,119)
  - feat_studio_affiliation 188,536 行を `kf:` → `keyframe:` rename
- 関連 task card: `TASK_CARDS/19_resolved_cluster_fix/05_keyframe_id_dedup.md`
- 残課題: `<src>:n:<name>` 救済 ID singleton 5,654 件 / anime SHA-hash 1,567 件 は scraper 上流対応推奨
- commit: d8bd282 (loader prefix unify) + b8578d6 (wip snapshot に Phase 2b 実装含む)

---

## 22系 silver coverage 改善 — TASK_CARDS/22_silver_coverage ✅ 2026-05-02

### 22/01: anime_studios coverage 2.5%→7.7%
- Conformed 層の anime_studios JOIN バグ修正 (21/01 silent fail 対策)
- anime_studios linked: 14K → 43K
- commit: 8207bbe

### 22/02: anime_studios 残取込 7.7%→8.6%
- bangumi infobox からのスタジオ抽出追加
- madb `is_main=False` 制作協力スタジオ取込
- commit: 02ce1c6 (bangumi.py 復元統合)

### 22/03: silver 全列 coverage audit ツール
- `src/etl/audit/silver_column_coverage.py` 新規作成
- commit: 52d73d8

### 22/04: persons/characters missing 列
- `persons.gender` null 率: 100% → 88.5% (AniList / bangumi からの補填)
- `persons.description` null 率: 100% → 90.6%
- `characters.gender` 取込
- commit: 01532a4 (loader 復元)

### 22/05: anime 拡張列 cross-source copy
- `anime.external_links_json` / `anime.trailer_url` 等を Conformed 層で cross-source 補填
- Resolved 層機能の前倒し実装
- commit: 343cc72

### 22/06: 22/04 test 復元
- 22/04 で失われたテストを 22/02/05 と統合
- commit: eb63a61

---

## v3 Reports & Visualization (2026-05-06)

commits: 6b2fa6e → 404f161 → b1e9faa → 65a9303 → bf91532 → 0ad2fb2 → dcb3cc2 → dd712c2 → 45a6435 (9 commits)

### 11 chart primitive 実装 (P1-P11)

- `src/viz/primitives/ci_scatter.py` (P1): 点推定 + 誤差バー + null reference line
- `src/viz/primitives/km_curve.py` (P2): 生存曲線 + Greenwood confidence band
- `src/viz/primitives/event_study.py` (P3): 介入前後 dynamic effect + bootstrap band
- `src/viz/primitives/small_multiples.py` (P4): facet grid + per-facet CI + multi-page PDF
- `src/viz/primitives/ridge.py` (P5): 分布重ね + KDE quantile band
- `src/viz/primitives/box_strip_ci.py` (P6): box + raw strip + 95% CI mark
- `src/viz/primitives/sankey.py` (P7): キャリア段階遷移 flow
- `src/viz/primitives/radial_network.py` (P8): ego-network 局所図
- `src/viz/primitives/heatmap.py` (P9): 相関 / 共起行列
- `src/viz/primitives/parallel_coords.py` (P10): 多軸 parallel coordinates
- `src/viz/primitives/choropleth_jp.py` (P11): 都道府県 choropleth stub (GeoJSON pending)

### 可視化基盤

- `src/viz/theme.py` / `palettes.py` (Okabe-Ito 8色、460-hex table) / `typography.py`: 全レポート共通統一
- `src/viz/interactivity.py`: linked brushing (brief 内 primitive 横断)
- `src/viz/export.py`: HTML / SVG / PDF 並走 export (kaleido)

### SPEC / BriefArc / Glossary v3

- `ReportSpec` データクラス (7 フィールド) 全 45 reports に curated SPEC 追加
- `BriefArc` データクラス: 4 段 narrative arc (現象提示→null対比→解釈限界→代替視点)
- Phase 5 strict mode 有効化: `ci_check_report_spec.py` で SPEC 未宣言レポートをブロック
- `make_default_spec` ヘルパー追加
- `docs/GLOSSARY_v3.md`: 用語 canonical 定義 + `forbidden_vocab` 19 例外 (rationale 付き)
- `docs/REPORT_DESIGN_v3.md` / `docs/VIZ_SYSTEM_v3.md`: 設計仕様書新規作成

### 名称変更 / 修正

- `biz_undervalued_talent` → `biz_exposure_gap` (報告語の評価的 framing 除去)
- `mgmt_director_mentor` title 変更: 「育成力ランキング」→「監督下デビュー人数と5年後可視性プロファイル」
- KPI strip + chart caption 標準化: 全 brief section で自動 KPI バッジ
- `BizUndervaluedTalentReport` BC alias 追加 → `45a6435` で削除完了

### TODO §16 残務全消化 (commit 45a6435)

- §16.1 BC alias 削除: `BizUndervaluedTalentReport` 完全削除、外部参照ゼロ
- §16.2 ChoroplethJP 真 render: dataofjapan/land MIT GeoJSON 47 features 取得 + go.Choropleth 実装
- §16.3 DB migration v63: `mart.meta_report_spec` DDL + SHA-256 spec_hash idempotent upsert + pipeline 統合
- §16.4 viz tests 拡張: graceful fallback 21 + spec gate 18 + choropleth 6 = 45 新 tests

### 数値達成

- 45 / 45 reports curated SPEC, strict mode 0 violations
- 11 chart primitive (P1-P11) 全実装、ChoroplethJP は真 GeoJSON render
- 3 brief 4 段 narrative arc curated
- 460 hex Okabe-Ito 統一
- 211 v3 関連 tests pass (regression なし)
- ReportSpec 永続化 (DB v63 mart.meta_report_spec, idempotent upsert)
- export SVG / PNG / multi-page PDF (kaleido + pypdf)
- interactivity (linked brushing + cross-filter)
- Glossary v3 27 用語 + forbidden_vocab 19 documented exceptions

---

## 並列検証・整合化 (2026-05-15 second wave)

既実装カードの検証 + docs sync + lint 残務消化 (sonnet × 4 + haiku × 2 並列)。

### 26/02 international_collab (実装は事前完了、検証 2026-05-15)

- `src/analysis/network/international_collab.py` 893 行 + `nationality_resolver.py` 346 行 + `structure_international.py` 853 行
- 41 tests pass、lint clean、forbidden vocab 0、anime.score 漏洩 0
- Method 充足: country tag / 役職別海外比率 / JP-CN/KR/SE_ASIA edges per anime / role progression / Louvain + null model permutation
- レポート登録 + brief 組込済
- 制約: nationality 入力 0% (`'[]'` 流入バグ) で実データ動作には `35/01` 完了必要

### 15/04 O4 foreign_talent (実装事前完了、lint 残務 + 検証 2026-05-15)

- `scripts/report_generators/reports/o4_foreign_talent.py` 994 行 + `nationality_resolver.py` 共有
- 27 tests pass、lint 残務 (F401×3 + F841 + F541×4) を ruff auto-fix + 手動 1 件で解消、再 lint 0 件
- Method 充足: FE 分布 (violin + Mann-Whitney U) / 役職進行 (KM + log-rank) / studio FE 散布図 / limited mobility bias (Andrews 2008) 注記
- レポート登録 + brief 組込済 (REPORT_INVENTORY L143/287/301/321/346-347)
- 制約: 26/02 同様 nationality 0% で `35/01` 後に実動作

### 15/x cross_cutting (commit `27a1cad`, 2026-05-13)

- `docs/report_cross_cutting.md` 278 行 (§1 brief mapping / §2 新 audience 12ヶ月 deferral / §3 method gate / §4 11×8 metric matrix / §5 lint vocab / §6 method note template / §7 roadmap)
- `scripts/lint_report_vocabulary.py` `CONTEXTUAL_BIGRAMS` L97-142 で 4 文脈 2-gram パターン実装 (失われた人材 / 不在能力 / 埋もれた才能 / 眠っている実力)
- `scripts/report_generators/section_builder.py` `METHOD_NOTE_TEMPLATES` 8 手法 (cox / mwu / km / counterfactual / louvain / propensity / did / weighted_pagerank)、`_base.py` で自動呼出
- `forbidden_vocab_exceptions.yaml` 180 行 / 23 例外、lint pass (51 files scanned, 3 ranking_framing exception 内)

### 19_resolved_cluster_fix docs sync (2026-05-15)

- 02_persons_tmdb_homonym (commit `f0d4547`) / 03_audit_post_fix (`f0d4547`) / 05_keyframe_id_dedup (Phase 2b `d8bd282`+`09a13df`) を完了状態に揃え
- README.md sub-cards 表 + 各 sub-card 冒頭 Status banner + TODO.md priority 表を実状態に同期

### 新規発見: resolved.nationality `'[]'` 流入バグ → `35/01` カード起票

- 調査結果: conformed.persons.nationality は seesaa 148K 行のみ `'[]'` (空 JSON array)、他 source 全 NULL
- `_select.py:_is_empty()` が `'[]'` を非空扱いで通過、seesaa 値が「唯一の非空値」として採用 → resolved.persons.nationality 全件 `'[]'`
- 対応カード: `TASK_CARDS/35_data_quality_backfill/01_nationality_backfill.md` (validator 強化 + anilist loader UPDATE パス追加 + nationality_resolver DuckDB 化 + scalar 化)
- O4 / international_collab はこの修復後に実データで意味のある分布を出す

---

## 並列カード消化 (2026-05-15)

4 カード並列実装 (sonnet × 3 + opus × 1、worktree isolation)。

### 27/01 missingness disclosure (commit `44dac5d`)

- `src/analysis/quality/coverage_matrix.py` — coverage[source, role_group, year] 行列
- `scripts/report_generators/_coverage_block.py` — HTML caveat block
- `scripts/report_generators/reports/_base.py` — base hook 配線 (auto-inject、DB 不在 graceful degrade)
- 46 tests pass / hits 8 件 / lint_vocab 0

### 15/01 O3 ip_dependency (commit `0b5b8e8`)

- `O3IpDependencyReport` (既存) のみ修正: unused import / dead var / interpretation 配線漏れ / docstring "ability" → "performance framing"
- 38 tests pass / anime.score 0 / forbidden vocab 0

### 15/03 O2 mid_management (commit `b433ca2` merge of `78f6a32`)

- `O2MidManagementReport` (既存) docstring "ability" → "evaluative framing"
- KM curve / studio blockage / cohort 昇進ファネル / HR+Policy brief 組込済
- 15 tests pass / lint_vocab 0

### 25/02 opportunity_residual_null (commit `d03d57c`)

- `src/analysis/scoring/opportunity.py` cross-sectional → **true (person, year) panel**
- studio FE (modal) + year FE、Shannon entropy 役職多様性
- analytical CI (within-person σ/√n, t_{n-1, 0.975}) + permutation null (1000 perms, 0.55s)
- `compute_opportunity_residual_from_credits()` / `_role_diversity_entropy()` / `_build_panel_from_credits()` / `residual_qq_deviation()`
- 46 tests pass (was 30) / 下流 161+35 pass / calibration 0.9500 (target 0.950, tol ±0.020)
- Q-Q deviation 0.039 (Stop-if 0.5 下) / permutation 0.55s (Stop-if 24h 下)
- `docs/method_notes/opportunity_residual.md` 仮定 + 代替 spec 文書化

### Session 2026-05-20: レポート高度化セッション

10 commit、200+ tests pass、レポート 38 → 40、analysis modules 8 新規 + 1 既存補強。

#### 35/01 nationality_backfill 完了 (commit `df2debb`)
- `infer_country_from_hometown()` 新規 (英文都市/国名 token + word-boundary)
- `integrate_duckdb.py` の persons.hometown→nationality backfill step (DuckDB UDF)
- `nationality_resolver` の resolved/conformed schema 自動解決
- resolved.persons 非空率 **3.48% → 12.26%** (76,279 件)、'[]' ゼロ達成
- name_utils テスト +15

#### Mart schema 透過化 (commit `3894bc7`)
- `gold_connect` で resolved.duckdb 自動 ATTACH + TEMP VIEW
- analysis 層が `FROM credits` 等 bare 名で書ける状態を回復
- international_collab / nationality_resolver / o8_soft_power の schema fallback 整理
- 136 tests pass

#### 25/04 Oaxaca-Blinder 分解実装 (commit `ce7d0a6`)
- `src/analysis/equity/oaxaca_decomp.py` 新規 (decompose / bootstrap CI 1000)
- `scripts/report_generators/reports/equity_oaxaca.py` v2 report
- 14 tests pass、§15 gender 70% 充足後に本格動作
- gender 不足下では skeleton + 警告 mode

#### 8 新規分析 modules (commit `7fb53d4`)
- `network/resilience.py` (32 tests): hub/bridge 除去 simulation、fragility_ratio、critical persons
- `equity/cohort_inequality.py` (23 tests): Gini/Theil-T/Atkinson 時系列
- `career/cox_visibility.py` (13 tests): Cox PH + Schoenfeld test + temporal holdout
- `causal/heterogeneous_effects.py` (15 tests): DiD → CATE 分解 + T-learner
- `quality/credit_anomaly.py` (14 tests): Poisson outlier + KL + source disagreement
- `career/mentor_effect.py` (11 tests): event-study + matched DiD
- `quality/power_analysis.py` (21 tests): t-test/regression/correlation power + MDE
- `briefs/executive_summary.py` (12 tests): KeyFinding template

#### 2 新規 v2 reports (commit `8b75fe0`)
- `NetworkResilienceReport` (policy)
- `CohortInequalityReport` (hr)

#### レポート品質強化 (本セッション後半)
- DiD robustness: `did_robustness.py` (placebo / E-value / joint leads、14 tests)
- visibility_warning に Cox section 並設可能化 (`_build_cox_section()`)
- forbidden_vocab に `subjective_evaluation` category 追加 (潜在力 / 成長余地 等)
- `lint_findings_separation.py` 新規: AST 解析で Findings / Interpretation 分離 audit
- `cross_reference.py` 新規 + 既存 reports の link graph 構築 (8 tests)
- `tests/integration/test_new_modules_integration.py` 10 integration tests

#### 文書整備
- `docs/method_notes/` に 7 新規 note (resilience / cohort / cox / HTE / mentor / anomaly / power)
- policy brief: structural_fragility + opportunity_decomposition 2 section 追加 (5→9)
- hr brief: cohort_structural_inequality 1 section 追加 (6→7)
- `REPORT_INVENTORY.md` 同期 (policy 7 / hr 9)

#### 36 AniList orphan backfill カード起票
- `TASK_CARDS/36_anilist_orphan_backfill/01_orphan_backfill.md` (実装着手は scrape 律速で保留可)

#### labor-first audit 0 violations 維持 (54 files)

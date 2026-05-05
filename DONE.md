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

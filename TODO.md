# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-23

本書はプロジェクト内のすべての未完了項目を一元管理するファイルです。完了済みサマリーは `DONE.md`、設計原則は `CLAUDE.md`。

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
| 🟠 Major | DuckDB 全面移行 | Phase A ✅。次: Card 06 GOLD DuckDB 化 (4.2) |
| 🟠 Major | Hamilton 導入 | H-1 ✅。次: H-2 Phase 5-8 Hamilton 化 |
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

### 🟡 Maintenance: legacy migration 削除

`src/database.py` が 9,000 行超に膨れている主因は v1-v55 の migration 関数群 (~7,000 行)。production DB が v57 で安定したら:

- [ ] `src/db/schema.py` = `init_db_v2()` + 最新 DDL (single source of truth)
- [ ] `src/db/dao.py` = upsert/query ヘルパー群
- [ ] v1-v55 の migration 関数をまとめて削除 (git history で参照可)

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

### 3.1 Scraper 統一

**現状**: 6 scraper が `upsert_anime()` を直接呼び、`integrate.py` の dual-write パターンを bypass。

- [ ] 各 scraper はモデルを組み立て、`integrate_*()` 関数 (or 新ラッパー `upsert_canonical_anime()`) 経由に統一
- [ ] `upsert_anime()` を bronze upsert に改名 or 内部で dual-write
- [ ] GraphQL クエリ文字列を `src/scrapers/queries/` に分離 (`PERSON_DETAILS_QUERY` 等)
- [ ] パース関数を `src/scrapers/parsers/` に分離 (`_parse_anime_staff` 等)
- [ ] scraper 本体は「fetch → parse → write」の orchestration だけに

### 3.6 テストの `Anime(score=..., studios=...)` 移行

- [ ] 短期: `BronzeAnime` シム維持 (破壊しない)
- [ ] 中長期: `AnimeAnalysis(...)` に段階移行 (`rg 'Anime\(.*score=' tests/ --count` でスコープ確認)

### 3.7 JVMG 再スクレイプ

- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (WIKIDATA_ROLE_MAP 修正後、DB にデータがあれば)

---

## SECTION 4: DuckDB 全面移行

詳細カード: **`TASK_CARDS/04_duckdb/` (README + 6 cards)**

### 4.1 Phase A ✅ DONE (2026-04-23)

silver_reader.py 新設、duckdb_io.py ATTACH 廃止、15 analysis module 移行、ベンチマーク (5.4x 平均) 完了。

**Card 05 で積み越し → Card 06**:
- `pipeline_phases/{data_loading,validation,entity_resolution,result_assembly}.py` — GOLD 書き込みと conn 共用
- `analysis/attrition/*.py`, `analysis/gender/bottleneck.py` — `feat_career`/`feat_career_gaps` GOLD テーブルを読む
- `analysis/{method_notes,person_parameters,llm_pipeline}.py` — GOLD 書き込み or LLM キャッシュ

### 4.2 Phase B: GOLD 層を DuckDB 化 (= Card 06)

- [ ] GOLD テーブル (person_scores, score_history, meta_*, agg_*, feat_*) を gold.duckdb へ
- [ ] パイプライン最終 Phase (`export_and_viz.py`) が gold.duckdb に書く
- [ ] API 側の GOLD 読み取りを DuckDB に切替
- [ ] attrition/gender/method_notes/person_parameters の conn を gold_connect() に切替
- [ ] `pipeline_phases/` の読み取り経路を silver_reader に、書き込み経路を gold_connect() に切替

### 4.3 Phase C: BRONZE を Parquet + DuckDB

- [ ] Scraper 出力を `src_*` テーブル → Parquet ファイル (日付パーティション) に
- [ ] `display_lookup.py` の読み取り先を Parquet に切替

### 4.4 Phase D: SQLite 完全撤去

- [ ] Entity resolution の書き込み経路を DuckDB に切替
- [ ] `src/database.py` を廃止 (DAO 群を `src/db/` に移管)
- [ ] `database_v2.py` / `models_v2.py` — DuckDB 移行で活かす計画がないなら削除
- [ ] `migrate_to_v2.py` は使い捨て script。`01_schema_fix/01_one_shot_copy.md` 実行後に削除
- [ ] Atlas migration を DuckDB 環境で再生成

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

### 5.2 Phase H-2: Phase 5-8 を Hamilton 化

- [ ] `core_scoring.py` (AKM / IV / PageRank / BiRank)
- [ ] `supplementary_metrics.py` / `result_assembly.py` / `post_processing.py`

### 5.3 Phase H-3: Phase 1-4 を Hamilton 化

- [ ] `data_loading.py` / `validation.py` / `entity_resolution.py` / `graph_construction.py`

### 5.4 Phase H-4: `PipelineContext` 完全削除

- [ ] `PipelineContext` dataclass を削除
- [ ] `src/pipeline.py` を Hamilton `Driver` の薄いラッパーに

### 5.5 Phase H-5: 観測・運用機能

- [ ] `@tag(stage="phase5", cost="expensive")` を各 node に付与
- [ ] 実行時間計測 adapter (Hamilton lifecycle hook)

### 中止判定

**H-1 決定ゲート: ✅ PASS (2026-04-23)**
- ❌ Hamilton overhead 20% 以上 → 実績: -77.7% (不該当、高速化)
- ❌ 可読性悪化 → 型ヒント + decorator でむしろ向上 (不該当)

**結論**: H-2 以降を完全実施。Phase 5-8, Phase 1-4 の Hamilton 化、PipelineContext 削除へ進む。

---

## SECTION 6: テストカバレッジ

### 6.1 pipeline_phases ユニットテスト

- [ ] Phase 9 `analysis_modules.py` — 並列実行 (Hamilton 化で一部解消見込み)

### 6.4 テストファイル分割

- [ ] `test_scraper_coverage.py` を scraper 別に分割 (`test_anilist_scraper.py`, `test_mal_scraper.py` 等)
- [ ] `test_analysis_coverage.py` は submodule 単位に分割
- [ ] `test_va_studio_genre.py` → `test_va.py` + `test_studio.py` + `test_genre.py`
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

### 8.1 3 系統の統一

| 系統 | 入口 | 行数 | ステータス |
|------|------|------|---------|
| v1 monolith | `scripts/generate_all_reports.py` | 24,983 | `pixi run reports` の現行エントリ |
| v2 orchestrator | `scripts/generate_reports_v2.py` + `report_generators/reports/*.py` | 413 + 多数 | 現行並行 |
| v3 class-based | `src/reporting/` | 2,725 | 使われていない (`pixi run reports-new`) |

- [ ] v1 か v3 のどちらかを即座に消す (v2 は移行過渡期の束なので最終形に寄せて閉じる)
- [ ] `pixi.toml` の `reports` / `reports-new` を 1 つにまとめる

### 8.2 `generate_all_reports.py` の 24,983 行分解

- [ ] 分離済みの関数を本体から削除 (v1/v2 でダブっている分を潰す)
- [ ] 本体を「v2/v3 にない暫定的な関数のみ」の薄いファイルに縮める (目標 < 2,000 行)

### 8.3 FastAPI 2 系統の統合

- [ ] `src/api.py` 自体を `src/api/{persons,reports,i18n}.py` の router 単位に分割 (1,322 行の分解)

---

## SECTION 9: アーキテクチャ整理

### 9.1 孤立した analysis モジュール (監査済み 2026-04-23)

全モジュールは稼働中のコードから参照されていた。`batch_compare` のみ `src/` に存在しない (worktree ゴースト)。

残務:
- [ ] `similarity.py` と `recommendation.py` の機能重複確認 (低優先度)

### 9.2 VA パイプラインの平行配線

`src/pipeline_phases/` に VA 専用 4 ファイル (340 行、本家と構造同一):

- [ ] 共通ロジックを `src/pipeline_phases/common/` に抽出して DRY
- [ ] または VA パイプラインを `src/analysis/va/` の module 集合として扱い `pipeline_phases/` から分離

### 9.3 Julia 視覚化層の要否確認

- [ ] `juliacall` は `scripts/setup_julia_env.py` のみ。Python 分析コードからは一切呼ばれていない (grep 確認済み 2026-04-23)。`julia_viz/` に Julia モジュール (JuliaViz.jl, chart_types.jl, renderers/) あり
- [ ] 削除判定: `julia_viz/` と `feature.viz` (pixi.toml) の削除を検討 — matplotlib/Plotly と役割重複

### 9.5 `src/monitoring.py` → `src/freshness.py` リネーム ✅ DONE

- [x] `src/freshness.py` として既に存在。`src/monitoring.py` は削除済み (2026-04-23)

### 9.6 `scripts/` の subdir 整理

- [ ] `scripts/analysis/` に `analyze_*.py` を寄せる
- [ ] `scripts/ci/` に `ci_check_*.py` を寄せる
- [ ] Top-level は entry-point のみ

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

### 12.1 完了済み戦略文書を `docs/archive/` に移動

```
docs/REFACTORING_SUMMARY.md
docs/PHASE1_DATA_LAYER_REFACTOR.md
docs/BUG_FIXES_PHASE_B.md
docs/REPORT_STRATEGY.md
docs/TEST_OPTIMIZATION_STRATEGY.md
docs/COMMUNITY_DETECTION_ENHANCEMENTS.md
docs/COVERAGE_REPORT.md
docs/SCHEMA_VERSIONING_STATUS.md
docs/TEST_AND_AUDIT.md
```

- [ ] 上記を `docs/archive/` に移動
- [ ] `docs/` 直下に残すのは: ARCHITECTURE, CALCULATION_COMPENDIUM, REPORT_PHILOSOPHY, DATA_DICTIONARY, REPORT_INVENTORY, schema.dbml のみ

### 12.2 CLAUDE.md スリム化 (827 行 → ~300 行目標)

- [ ] Phase 2C/2D/2E/2F/2G の詳細 (~400 行) を `docs/` サブファイルへ退避
- [ ] CLAUDE.md はプロジェクト原則・禁止事項・ディレクトリ案内に絞る

### 12.3 README.md スリム化 (512 行 → ~150 行目標)

- [ ] README.md = 「何をやっていてどう始めるか」(外部向け) に限定
- [ ] API/CLI/Architecture の詳細は `docs/` へ

### 12.4 CLAUDE.md ドリフト修正 (随時)

- [ ] 「1394 tests」→「2300+ tests」
- [ ] Testing patterns の `monkeypatch DEFAULT_DB_PATH` → DuckDB 切替後に更新 (Section 4 Phase D 完了時)
- [ ] `src/models.py` の `AnimeAnalysis` に付いた DEPRECATED コメントを削除

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

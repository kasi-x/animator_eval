# TODO — Animetor Eval

## プロジェクトの目的

**個人の貢献を可視化・定量化** → スタジオが適正な報酬を支払う根拠を提供 → アニメ業界の健全化

スコアは「能力」ではなく「ネットワーク上の位置と密度」を表す。
個人にフォーカスが当たり、正当な評価と報酬につながることが最終ゴール。

---

## P0-EVAL — 最重要（個人評価の信頼性）

### P0-EVAL-1: anime_value.py プレースホルダー解消

`anime_value.py` の5次元のうち4つが `0.5` 固定のプレースホルダー。
potential_value の根幹であり、報酬根拠として使えない状態。

- **現状のプレースホルダー**:
  - `commercial_success` → 0.5 固定
  - `cultural_impact` → 0.5 固定
  - `critical_reception` → 0.5 固定
  - `industry_influence` → 0.5 固定
  - `technical_quality` のみ実装済み
- **Files**: `src/analysis/anime_value.py`, `src/analysis/potential_value.py`
- **Acceptance**:
  - 各次元が実データに基づく値を返す（AniList score, MAL score, 受賞歴, 続編数 etc.）
  - 実データが取得できない場合のフォールバック戦略が明示されている
  - 既存テスト + 新規テスト通過
- **Complexity**: High（データソースの調査・統合が必要）

### P0-EVAL-2: 全スコアへの信頼区間付与

点推定だけでは報酬交渉に使えない。各スコアに信頼区間を付ける。

- **対象スコア**: authority, trust, skill, potential_value, composite
- **手法候補**:
  - Bootstrap resampling（クレジットデータをリサンプリング）
  - Bayesian credible interval（事前分布 + 観測データ）
  - Cross-validation variance（既存の `crossval.py` を拡張）
- **Files**: `src/analysis/confidence.py`, `src/pipeline_phases/core_scoring.py`,
  `src/models.py` (ScoreSet に interval フィールド追加)
- **Acceptance**:
  - 各スコアに `(lower, upper, confidence_level)` が付与される
  - クレジット数が少ない人ほど区間が広くなる（正しい不確実性表現）
  - JSON出力に interval が含まれる
- **Complexity**: High

### P0-EVAL-3: structural_estimation 実データ検証

構造推定（DID, 操作変数法, Event Study）が実データで妥当な結果を出すか検証。
報酬根拠として使うなら、因果推定の信頼性が命。

- **Files**: `src/analysis/structural_estimation.py`, `src/analysis/causal_studio_identification.py`
- **Acceptance**:
  - 実データ（production DB）での推定結果が経済学的に妥当
  - 推定のロバストネスチェック（異なる仕様で結果が安定）
  - プレースホルダー的な推定（サンプルサイズ不足等）に警告表示
  - 推定不可能なケースでエラーではなく明示的な "insufficient data" を返す
- **Complexity**: High

---

## P0 — Critical (Robustness & Security)

### P0-1: Scraper Custom Exception Hierarchy + Shared Retry

**Status**: ✅ Done

Custom exception classes replacing generic `RuntimeError` across all scrapers.
Shared async retry utility with exponential backoff and `Retry-After` support.

- **Files**: `src/scrapers/exceptions.py` (new), `src/scrapers/retry.py` (new),
  `src/scrapers/anilist_scraper.py`, `src/scrapers/mal_scraper.py`,
  `src/scrapers/mediaarts_scraper.py`, `src/scrapers/jvmg_fetcher.py`
- **Tests**: `tests/test_scraper_exceptions.py`

### P0-2: Image Downloader Retry + Content Validation

**Status**: ✅ Done

### P0-3: Database Context Manager

**Status**: ✅ Done

### P0-4: API Security (CORS, Rate Limiting, API Key)

**Status**: ✅ Done

### P0-5: Secrets Management

**Status**: ✅ Done

---

## P1-EVAL — 個人評価の強化

### P1-EVAL-1: explain.py 強化（個人向けレポート）

「あなたの評価はこうで、根拠はこれ」を個人が理解できる形で見せる。
現在の explain.py はスコア分解のみ。キャリアストーリーと根拠を加える。

- **Files**: `src/analysis/explain.py`, `src/report.py`
- **Acceptance**:
  - 個人プロファイルに「スコアの根拠」セクション追加
  - 上位貢献作品、主要コラボレーター、キャリア転機を含む
  - 日本語・英語の自然言語テキスト生成
  - 信頼区間（P0-EVAL-2）の平易な説明
- **Complexity**: Medium-High

### P1-EVAL-2: studio_bias の拡充（待遇差分析）

スタジオ間の待遇差を統計的に示す。業界健全化の直接ツール。

- **Files**: `src/analysis/studio_bias_correction.py`, `src/analysis/bias_detector.py`
- **Acceptance**:
  - 同等スコアの人材がスタジオ間でどう評価されるかの比較
  - バイアス検出結果の統計的有意性検定
  - レポート出力（匿名化オプション付き）
- **Complexity**: Medium

### P1-EVAL-3: 重複モジュール統合

- `growth.py` vs `growth_acceleration.py` → 統合
- `circles.py` vs `community_detection.py` → circles を community_detection に吸収
- **Files**: `src/analysis/growth.py`, `src/analysis/growth_acceleration.py`,
  `src/analysis/circles.py`, `src/analysis/community_detection.py`
- **Acceptance**:
  - 重複機能が1箇所に統合
  - 既存のJSON出力フォーマットは維持（後方互換）
  - パイプライン呼び出し元を更新
- **Complexity**: Medium

### P1-EVAL-4: 作品軸モジュールの個人軸への再構成

`anime_stats`, `seasonal`, `decades` 等の作品軸統計を個人視点に変換。
例: 「この人はどの季節に最も活発か」「この人のキャリアは年代ごとにどう変化したか」

- **Files**: `src/analysis/anime_stats.py`, `src/analysis/seasonal.py`
- **Acceptance**:
  - 個人IDを軸とした集計オプション追加
  - 既存の作品軸出力は維持
- **Complexity**: Medium

---

## P1 — Important (Infrastructure)

### P1-1: MediaArts SSL Verification Fix

The MediaArts SPARQL client uses `verify=False` as a workaround for expired
SSL certificates. This should be replaced with certificate pinning or a proper
CA bundle.

- **Files**: `src/scrapers/mediaarts_scraper.py`
- **Acceptance**:
  - Try system CA first, fall back to `verify=False` with structured warning
  - Or bundle the specific CA cert for `mediaarts-db.artmuseums.go.jp`
- **Complexity**: Low

### P1-2: MAL / MediaArts / JVMG Checkpoint & Resume

AniList already has checkpoint/resume support. Other scrapers lack it, meaning
a crash at 80% progress loses all data.

- **Files**: `src/scrapers/mal_scraper.py`, `src/scrapers/mediaarts_scraper.py`,
  `src/scrapers/jvmg_fetcher.py`
- **Acceptance**:
  - Each scraper saves checkpoint JSON periodically (every N pages)
  - `--resume` flag loads checkpoint and continues from last offset
  - Checkpoint includes: offset, timestamp, partial results count
- **Complexity**: Medium (3 files, follow AniList pattern)

### P1-3: AniList Scraper Batch Function Bug

The `batch_fetch_staff_credits()` function (around line ~1147) references
undefined helper functions. This code path may be unreachable but should be
fixed or removed.

- **Files**: `src/scrapers/anilist_scraper.py`
- **Acceptance**: All code paths in the module are functional or removed
- **Complexity**: Low

### P1-4: Scraper Test Coverage (Network / Retry Behavior)

Current scraper tests are minimal. Need mocked network tests for retry logic,
rate limit handling, and error recovery.

- **Files**: `tests/test_scrapers/` (new directory)
- **Acceptance**:
  - Mocked httpx responses for each scraper
  - Test: retry on 5xx, backoff on 429, parse errors
  - Test: checkpoint save/load (P1-2 prerequisite)
  - Coverage target: 70%+ on scraper modules
- **Complexity**: Medium-High

### P1-5: Incremental Pipeline Mode

Currently the pipeline always recomputes everything from scratch. For large
datasets (20K+ anime), this takes several minutes. An incremental mode that
only reprocesses changed/new data would greatly improve iteration speed.

- **Files**: `src/pipeline.py`, `src/pipeline_phases/`
- **Acceptance**:
  - `--incremental` flag: only reprocess persons with new credits since last run
  - Uses `get_persons_with_new_credits()` (already exists in `database.py`)
  - Graph reconstruction limited to affected neighborhoods
  - Score deltas logged for audit trail
- **Complexity**: High (touches all pipeline phases)

### P1-6: Deployment Configuration (Docker)

**Status**: ✅ Done

### P1-7: API Input Validation Hardening

Some endpoints accept raw string inputs that could be tightened with Pydantic
validation or regex constraints.

- **Files**: `src/api.py`
- **Acceptance**:
  - Person IDs validated against `^[a-z]+:p?\d+$` pattern
  - Anime IDs validated against `^[a-z]+:\d+$` pattern
  - Query strings sanitized (length limits, no SQL-injection-like patterns)
  - All Query parameters have explicit types and bounds
- **Complexity**: Low-Medium

### P1-8: Structured Scraper Logging Consistency

Logging across scrapers is inconsistent — some use event names, others use
messages. Standardize on structured event logging with consistent field names.

- **Files**: `src/scrapers/*.py`
- **Acceptance**:
  - All log events use `snake_case` event names
  - Standard fields: `source`, `item_count`, `elapsed_ms`, `attempt`
  - Rate limit events include `retry_after_seconds`
  - Error events include `error_type` and `error_message`
- **Complexity**: Low

### P1-9: Split Pixi Environments (Scraping vs Analysis)

Scraping (`httpx`, `beautifulsoup4`, `lxml`) and analysis (`networkx`, `openskill`,
`matplotlib`) have minimal overlap. Splitting into separate pixi features/environments
reduces install size, avoids dependency conflicts, and allows faster CI for targeted jobs.

- **Files**: `pixi.toml`
- **Acceptance**:
  - `pixi run -e scrape <command>` uses scraping dependencies only
  - `pixi run -e analysis <command>` uses analysis dependencies only
  - `pixi run -e dev <command>` includes everything (default for development)
  - CI runs scraping and analysis tests in parallel with separate envs
- **Complexity**: Medium (pixi feature/environment configuration, CI matrix)

### P1-10: Lint Cleanup + Python 3.12 Deprecation Fix

**Status**: ✅ Done

### P1-11: AniList Scraper main() 分割 (514行 → 5関数)

`anilist_scraper.py` の `main()` が514行の巨大関数。以下に分割:

1. `fetch_top_anime()` — GraphQL anime一覧取得
2. `fetch_staff_credits_batch()` — staff credits 取得 + checkpoint
3. `save_batch_to_database()` — batch DB保存（**P1-3 の F821 未定義関数を実装**）
4. `download_images_batch()` — 画像非同期DL
5. `main()` — オーケストレーター（~100行）

- **Files**: `src/scrapers/anilist_scraper.py`
- **Acceptance**:
  - 各関数 ≤100 行
  - F821 (undefined name) lint エラー解消
  - 既存テスト通過
- **Complexity**: Medium-High（巨大リファクタ、テスト必要）
- **Note**: P1-3 (batch function bug) を包含

### P1-12: Analysis Module テストカバレッジ

20の分析モジュール（計~4000行）にテストがない。

**Tier 1 (Critical — 報酬根拠に直結)**:
- `structural_estimation.py` (~700行) — 因果推定（DID, Event Study）
- `causal_studio_identification.py` (1017行) — スタジオ因果効果
- `potential_value.py` (~180行) — ポテンシャル推定
- `contribution_attribution.py` (~250行) — Shapley値

**Tier 2 (Important — 個人評価の補助)**:
- `community_detection.py` (695行) — コミュニティ検出 + 師弟関係
- `bias_detector.py` (~150行) — バイアス検出

**Tier 3 (低優先)**:
- `anime_value.py`, `compensation_analyzer.py`, `core_periphery.py`,
  `genre_specialization.py`, `growth_acceleration.py`, `insights_report.py`,
  `neo4j_direct.py`, `path_finding.py`, `structural_holes.py`,
  `studio_bias_correction.py`, `temporal_influence.py`,
  `structural_estimation_html.py`, `event_study_viz.py`

- **Files**: `tests/` (new test files)
- **Acceptance**: Tier 1 modules at 70%+ coverage
- **Complexity**: High（Tier 1 だけで ~2150行のテスト対象）

---

## P1-PERF — Pipeline Performance Optimization (残ボトルネック)

**Status**: ✅ **All PERF optimizations complete!**

元のパイプライン実行時間: **182.6s** (12,164 persons, 2.68M edges)
最適化後の予測: **40-60s** (3-4.5x speedup)

<details>
<summary>完了済み PERF 詳細（クリックで展開）</summary>

### PERF-1: GraphML Export 高速化 (78.8s → 1-3s) ✅

Commit: 3753f17 — `nx.write_graphml_lxml()` + `prettyprint=False` + `round(2)`

### PERF-2: Influence Tree 高速化 (60s → 1-2s) ✅

Commit: 3753f17 — person_highest_stage dict 事前構築で O(1) ルックアップ

### PERF-3: Structural Estimation 高速化 (59.7s → 3-6s) ✅

Commit: fdc24c9 — Counter 事前集計 + defaultdict person-year index

### PERF-4: Contribution Attribution 高速化 (39.8s → 3-5s) ✅

Commit: ba26c69 — marginal_cache + anime_credits_index

### PERF-5: Bridge Detection 高速化 (16.5s → 3-6s) ✅

Commit: ba26c69 — itertools.combinations (C実装) に置換

| # | ボトルネック | 現在 | 目標 | 効果 |
|---|---|---|---|---|
| PERF-1 | GraphML Export | 78.8s | 1-3s | ★★★ |
| PERF-2 | Influence Tree | 60.0s | 1-2s | ★★★ |
| PERF-3 | Structural Estimation | 59.7s | 3-6s | ★★☆ |
| PERF-4 | Contribution Attribution | 39.8s | 3-5s | ★★☆ |
| PERF-5 | Bridge Detection | 16.5s | 3-6s | ★☆☆ |
| | **合計** | **254.8s** | **11-22s** | |

</details>

---

## P2 — Nice to Have (Future)

### P2-1: API Response Caching (Redis / In-Memory)

Heavy endpoints (ranking, search) re-read JSON files on every request. An
in-memory or Redis cache with TTL would reduce latency.

- **Files**: `src/api.py`, `src/utils/json_io.py`
- **Complexity**: Medium

### P2-2: Frontend SPA（個人ポートフォリオ）

個人が自分のスコア・キャリア・根拠を閲覧できるWebアプリ。
スタジオ向けダッシュボードではなく、**個人向けポートフォリオ**がメイン。

- **Files**: `frontend/` (new directory)
- **機能**:
  - 個人プロファイルページ（スコア + 信頼区間 + 根拠）
  - キャリアタイムライン（インタラクティブ）
  - 同等ポジションの人材との比較（匿名化）
  - 多言語対応（既存i18nシステム活用）
- **Complexity**: High

### P2-3: Performance Benchmarks (CI)

Automated performance benchmarks in CI to detect regressions in pipeline speed.

- **Files**: `benchmarks/` (new directory), `.github/workflows/ci.yml`
- **Complexity**: Medium

### P2-4: Neo4j Direct Query Mode

Currently Neo4j is export-only (CSV). Direct graph queries via `neo4j` driver
would enable richer analysis.

- **Files**: `src/analysis/neo4j_direct.py` (new)
- **Complexity**: Medium

### P2-5: Data Freshness Monitoring

Alerting when data sources haven't been scraped within expected intervals.

- **Files**: `src/monitoring.py` (new), `src/api.py`
- **Complexity**: Low-Medium

### P2-6: Pipeline Crash Resume

If the pipeline crashes mid-execution, resume from the last completed phase
instead of starting over.

- **Files**: `src/pipeline.py`, `src/pipeline_phases/context.py`
- **Complexity**: Medium

---

## Completed (Reference)

<details>
<summary>完了済みタスク一覧（クリックで展開）</summary>

### P0-1 ~ P0-5: Robustness & Security ✅

- Scraper exceptions + retry, image downloader, db_connection, API security, secrets management

### Rust Extension (Phase 10) ✅

Pipeline acceleration from ~9min to ~1-2min via PyO3/maturin Rust extension.
Brandes' betweenness (rayon parallel), collaboration edge aggregation,
betweenness cache deduplication.

### Pipeline Bottleneck Optimization ✅

- `collaboration_strength.py`: 101.8s → 29.9s (3.4x)
- `graphml_export.py`: 112.7s → 78.8s (1.4x)

### Docker Deployment ✅

Multi-stage Dockerfile (Rust build + Python app), docker-compose with app + Neo4j.

### P1-10: Lint Cleanup ✅

All 69 lint errors resolved. `pixi run lint` clean.

### PERF-1 ~ PERF-5: Performance Optimization ✅

Total: 254.8s → 11-22s across 5 bottlenecks.

</details>

---

## Legal Compliance (Ongoing)

これらはバックログではなく、常に遵守すべきハード制約:

- [ ] スコアを「能力」と表現しない — 「ネットワーク上の位置と密度」のみ
- [ ] エンティティ解決の偽陽性 = 潜在的名誉毀損（信用毀損）
- [ ] 公開されたクレジットデータのみ使用
- [ ] 全レポートに JA + EN の免責事項を含む
- [ ] AniList rate limit: 90 req/min — exponential backoff 実装済み
- [ ] MAL (Jikan) rate limit: 3 req/s, 60 req/min — 実装済み
- [ ] **報酬根拠として提示する場合、信頼区間の表示を必須とする**（P0-EVAL-2）

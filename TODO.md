# TODO — Animetor Eval

## プロジェクトの目的

**個人の貢献を可視化・定量化** → スタジオが適正な報酬を支払う根拠を提供 → アニメ業界の健全化

個人にフォーカスが当たり、正当な評価と報酬につながることが最終ゴール。

### 設計思想: 二層モデル

既存の3軸（Authority / Trust / Skill）は **ネットワーク上の位置** を測る指標。
PageRankに補正を重ねても測定対象は変わらないため、**別の測定器**として
個人貢献指標（Individual Contribution Profile）を追加する。

```
Layer 1: Network Profile（参考情報 — 既存の3軸）
  Authority / Trust / Skill / Composite
  → 「この人はネットワーク上でどういう位置にいるか」

Layer 2: Individual Contribution Profile（報酬根拠 — 新規）
  peer_percentile / opportunity_residual / consistency / independent_value
  → 「機会を統制した上で、この人の独自の貢献はどれだけか」
```

---

## P0-EVAL — 最重要（個人貢献指標の構築）

### P0-EVAL-1: ピア比較パーセンタイル

**Status**: ✅ Done

同じ役職 × 同じキャリア年数のコホート内で順位を算出。

- **Files**: `src/analysis/individual_contribution.py`
- **Tests**: `tests/test_individual_contribution.py` (25 tests)

### P0-EVAL-2: 機会統制残差（Opportunity-Adjusted Residual）

**Status**: ✅ Done

OLS回帰で機会要因を統制し、残差を個人の独自貢献として抽出。

- **Files**: `src/analysis/individual_contribution.py`

### P0-EVAL-3: 一貫性スコア（Consistency Score）

**Status**: ✅ Done

作品間のスコア変動係数から一貫性を測定。

- **Files**: `src/analysis/individual_contribution.py`

### P0-EVAL-4: 独立貢献度（Independent Value）

**Status**: ✅ Done

コラボレーターへの波及効果から個人の独立した貢献を推定。

- **Files**: `src/analysis/individual_contribution.py`

### P0-EVAL-5: Individual Contribution Profile の統合と出力

**Status**: ✅ Done

4指標をパイプライン Phase 9 に統合、JSON出力、API、explain統合。

- **Files**: `src/pipeline_phases/analysis_modules.py`, `src/pipeline_phases/export_and_viz.py`,
  `src/utils/json_io.py`, `src/api.py`, `src/analysis/explain.py`
- **出力**: `result/json/individual_profiles.json`
- **API**: `GET /api/persons/{id}/profile` (二層モデル: network_profile + individual_profile)
- **Tests**: `tests/test_individual_contribution.py` (25 tests)

---

## P1-EVAL-0 — ネットワーク指標の改善（補助）

既存の3軸+補正系はネットワーク参考情報として維持。
ただし以下の問題は修正が必要:

### P1-EVAL-0a: anime_value.py プレースホルダー解消

**Status**: ✅ Done

Replaced 3 hardcoded `0.5` placeholders with actual data: `anime.score` for
external/critical value, tag variety for novelty.

- **Files**: `src/analysis/anime_value.py`

### P1-EVAL-0b: structural_estimation の位置づけ明確化

**Status**: ✅ Done

構造推定は「参考研究」として維持。報酬根拠の主指標にはしない。

- **Files**: `src/analysis/structural_estimation.py`

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

**Status**: ✅ Done

Added `explain_individual_profile()` function interpreting peer_percentile,
opportunity_residual, consistency, independent_value with Japanese descriptions.
Integrated into `/api/persons/{id}/profile` endpoint.

- **Files**: `src/analysis/explain.py`, `src/api.py`

### P1-EVAL-2: studio_bias の拡充（待遇差分析）

**Status**: ✅ Done

Added `compute_studio_disparity()` for cross-studio treatment analysis,
`StudioDisparityResult` dataclass, `GET /api/studio-disparity` endpoint.
Fixed `extract_studio_from_anime()` to use `anime.studios` list.

- **Files**: `src/analysis/studio_bias_correction.py`, `src/api.py`,
  `src/pipeline_phases/supplementary_metrics.py`, `src/utils/json_io.py`

### P1-EVAL-3: 重複モジュール統合

**Status**: ✅ Done

Renamed `GrowthMetrics` → `AccelerationMetrics` in `growth_acceleration.py`
to disambiguate from `protocols.py`. Modules serve different purposes and
remain separate.

- **Files**: `src/analysis/growth_acceleration.py`

### P1-EVAL-4: 作品軸モジュールの個人軸への再構成

**Status**: ✅ Done

Added `compute_person_anime_stats()` and `compute_person_seasonal_activity()`
for person-axis aggregation.

- **Files**: `src/analysis/anime_stats.py`, `src/analysis/seasonal.py`

---

## P1 — Important (Infrastructure)

### P1-1: MediaArts SSL Verification Fix

**Status**: ✅ Done

Try system CA first, fall back to `verify=False` with structured warning.

- **Files**: `src/scrapers/mediaarts_scraper.py`

### P1-2: MAL / MediaArts / JVMG Checkpoint & Resume

**Status**: ✅ Done

Added checkpoint/resume to MAL scraper and JVMG fetcher following AniList pattern.

- **Files**: `src/scrapers/mal_scraper.py`, `src/scrapers/jvmg_fetcher.py`

### P1-3: AniList Scraper Batch Function Bug

**Status**: ✅ Done (included in P1-11 decomposition)

### P1-4: Scraper Test Coverage (Network / Retry Behavior)

**Status**: ✅ Done

120 mocked tests covering retry logic, rate limiting, error handling,
checkpoint operations, image download, and edge cases across all 5 scrapers.

- **Files**: `tests/test_scraper_coverage.py`

### P1-5: Incremental Pipeline Mode

**Status**: ✅ Done

`--incremental` flag skips pipeline when no credit changes since last run,
returning cached results from JSON. Full recompute on any data change.
`pixi run pipeline-inc` task added.

- **Files**: `src/pipeline.py`, `src/database.py`, `src/api.py`, `pixi.toml`
- **Tests**: 7 new tests in `tests/test_pipeline.py`

### P1-6: Deployment Configuration (Docker)

**Status**: ✅ Done

### P1-7: API Input Validation Hardening

**Status**: ✅ Done

Added Pydantic validation for person/anime IDs and query parameter bounds.

- **Files**: `src/api.py`

### P1-8: Structured Scraper Logging Consistency

**Status**: ✅ Done

Standardized all scrapers on `snake_case` event names with consistent fields
(`source`, `item_count`, `elapsed_ms`, `attempt`, `retry_after_seconds`).

- **Files**: `src/scrapers/*.py`

### P1-9: Split Pixi Environments (Scraping vs Analysis)

**Status**: ✅ Done

Split `pixi.toml` into composable features (scrape, analysis, api, dev, rust, neo4j)
and environments (default, scrape, analysis, serve).

- **Files**: `pixi.toml`

### P1-10: Lint Cleanup + Python 3.12 Deprecation Fix

**Status**: ✅ Done

### P1-11: AniList Scraper main() 分割 (514行 → 5関数)

**Status**: ✅ Done

Decomposed `main()` into `_load_anime_ids()`, `_fetch_staff_phase()`,
`_fetch_person_details_phase()`. F821 lint errors resolved.

- **Files**: `src/scrapers/anilist_scraper.py`

### P1-12: Analysis Module テストカバレッジ

**Status**: ✅ Done

115 mocked tests covering potential_value, contribution_attribution,
studio_bias_correction, growth_acceleration, anime_value, and
individual_contribution edge cases. Fixed ZeroDivisionError in
studio_bias_correction.py.

- **Files**: `tests/test_analysis_coverage.py`, `src/analysis/studio_bias_correction.py`

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

## P2 — Nice to Have

### P2-1: API Response Caching with TTL

**Status**: ✅ Done

Replaced LRU cache with TTL-based cache (default 300s) in json_io.py.
Added Cache-Control headers to GET API responses via middleware.

- **Files**: `src/utils/json_io.py`, `src/api.py`

### P2-2: Frontend SPA（個人ポートフォリオ）

**Status**: ✅ Done

Vanilla HTML/CSS/JS SPA at `/static/portfolio.html` with search, profile
(two-layer model), ranking, i18n (EN/JA), disclaimer. Safe DOM manipulation
(no innerHTML with untrusted data).

- **Files**: `static/portfolio.html`, `static/portfolio.js`

### P2-3: Performance Benchmarks (CI)

**Status**: ✅ Done

`pixi run bench` runs pipeline with synthetic data, outputs phase-level
timings as JSON. `--compare` flag detects >20% regressions. CI integration.

- **Files**: `benchmarks/bench_pipeline.py`, `.github/workflows/ci.yml`, `pixi.toml`

### P2-4: Neo4j Direct Query Mode

**Status**: ✅ Done

Added `Neo4jReader` class with shortest path, common collaborators,
neighborhood, influential paths, community subgraph, and stats queries.
API endpoints: `/api/neo4j/{path,common,neighborhood,stats}`.

- **Files**: `src/analysis/neo4j_direct.py`, `src/api.py`

### P2-5: Data Freshness Monitoring

**Status**: ✅ Done

`src/monitoring.py` checks `data_sources` table against per-source thresholds
(7d for AniList/MAL, 30d for MediaArts/Wikidata). API endpoint, CLI command.

- **Files**: `src/monitoring.py`, `src/api.py`, `src/cli.py`, `tests/test_monitoring.py`

### P2-6: Pipeline Crash Resume

**Status**: ✅ Done

`PipelineCheckpoint` saves intermediate results after phases 5, 7, 9.
`--resume` flag re-runs phases 1-4 (data from DB) then restores checkpointed
scores/results. Checkpoint deleted on success.

- **Files**: `src/pipeline.py`, `src/pipeline_phases/context.py`, `pixi.toml`

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

### P1-7 ~ P1-12: Infrastructure Improvements ✅

- P1-7: API input validation hardening
- P1-8: Structured scraper logging consistency
- P1-9: Pixi environment splitting (features/environments)
- P1-10: Lint cleanup (69 errors → 0)
- P1-11: AniList scraper decomposition (main() → 3 functions)
- P1-12: Analysis module test coverage (115 tests)

### PERF-1 ~ PERF-5: Performance Optimization ✅

Total: 254.8s → 11-22s across 5 bottlenecks.

### P2-1 ~ P2-6: Nice to Have ✅

- P2-1: TTL-based API response caching (300s default)
- P2-2: Frontend SPA (search, profile, ranking, i18n)
- P2-3: Performance benchmarks with CI integration
- P2-4: Neo4j direct query mode (6 query types + 4 API endpoints)
- P2-5: Data freshness monitoring (API + CLI)
- P2-6: Pipeline crash resume (checkpoint after phases 5, 7, 9)

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

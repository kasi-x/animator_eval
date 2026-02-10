# TODO — Animetor Eval

Priority-based roadmap organized by urgency. Each item includes affected files,
acceptance criteria, and complexity estimate.

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
- **Acceptance**:
  - `ScraperError` base with `source`, `url`, `metadata` attributes
  - `RateLimitError` with `retry_after`, `AuthenticationError`, `DataParseError`,
    `EndpointUnreachableError`, `ContentValidationError`
  - `retry_async()` respects `Retry-After` header, exponential backoff
  - All scrapers raise typed exceptions instead of `RuntimeError`
- **Complexity**: Medium (4-5 files, straightforward)

### P0-2: Image Downloader Retry + Content Validation

**Status**: ✅ Done

Add retry with exponential backoff and content validation to image downloads.

- **Files**: `src/scrapers/image_downloader.py`
- **Acceptance**:
  - 3 retries with exponential backoff on failure
  - `Retry-After` header respected on 429
  - `Content-Type` must start with `image/`
  - Files <1KB rejected as corrupt
- **Complexity**: Low

### P0-3: Database Context Manager

**Status**: ✅ Done

`db_connection()` context manager to replace manual `conn.close()` patterns.
Auto-commit on success, rollback on exception, always close.

- **Files**: `src/database.py`, `src/api.py`, `src/cli.py`, `src/pipeline.py`,
  `src/scrapers/mediaarts_scraper.py`, `src/scrapers/jvmg_fetcher.py`
- **Tests**: `tests/test_database.py`
- **Acceptance**:
  - `with db_connection() as conn:` works for all callers
  - Auto-commit on clean exit, rollback on exception
  - All `try/finally: conn.close()` patterns migrated
  - All bare `conn.close()` patterns migrated
  - No leaked connections on exception paths
- **Complexity**: Medium-High (many call sites across 6+ files)

### P0-4: API Security (CORS, Rate Limiting, API Key)

**Status**: ✅ Done

Production-ready API security: CORS middleware, rate limiting, API key auth
for write endpoints.

- **Files**: `src/api.py`, `pixi.toml`
- **Tests**: `tests/test_api.py`
- **Dependencies**: `slowapi>=0.1.9`
- **Acceptance**:
  - CORS: configurable `CORS_ORIGINS` from env, defaults to `["http://localhost:*"]`
  - Rate limiting: 60/min GET, 2/min `/api/v1/pipeline/run`
  - API key: `verify_api_key()` dependency on `/api/v1/pipeline/run`
  - Dev mode: no API key configured → allow with log warning
  - Production: `API_SECRET_KEY` env var required for write endpoints
- **Complexity**: Medium (1 main file + dependency)

### P0-5: Secrets Management

**Status**: ✅ Done

Centralized .env loading, startup validation, and example file.

- **Files**: `.env.example` (new), `src/utils/config.py`,
  `src/scrapers/anilist_scraper.py`
- **Acceptance**:
  - `.env.example` with all env vars documented (placeholder values)
  - `load_dotenv_if_exists()` — uses `python-dotenv`, no manual parsing
  - `validate_environment()` — warns about missing optional vars
  - AniList scraper's manual .env loading replaced with centralized utility
- **Dependencies**: `python-dotenv>=1.0`
- **Complexity**: Low-Medium (3 files)

---

## P1 — Important (For Other Agents)

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

Multi-stage Docker build with Rust extension compilation and Python app.

- **Files**: `Dockerfile`, `docker-compose.yml`, `.dockerignore`
- **Acceptance**:
  - Multi-stage build (Rust extension + Python app)
  - `docker compose up` starts API + optional Neo4j
  - Environment variables for all configuration
  - Health check endpoint used for container health
- **Complexity**: Medium

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

69 lint errors (56 auto-fixable) + Python 3.12 deprecation warnings。
一括で `ruff check --fix` で大半解消。残りは手動。

**Subtasks**:

1. **P1-10a**: `ruff check --fix src/ tests/` — 56 unused imports (F401) + empty f-strings (F541)
2. **P1-10b**: Bare `except:` → 具体的な例外型に修正
   - `src/analysis/causal_studio_identification.py:282` → `except (ValueError, ZeroDivisionError):`
3. **P1-10c**: Dead code (F841 unused variables) 削除
   - `src/analysis/community_detection.py:192, 502, 662`
4. **P1-10d**: Python 3.12 deprecation `asyncio.get_event_loop()` → `asyncio.to_thread()`
   - `src/api.py:786` — pipeline executor
   - `src/websocket_manager.py:87` — broadcast_sync

- **Acceptance**: `pixi run lint` がエラー0
- **Complexity**: Low（ほぼ自動修正、手動修正4箇所）

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

**Tier 1 (Critical — 複雑アルゴリズム)**:
- `causal_studio_identification.py` (1017行) — 因果推論
- `structural_estimation.py` (~700行) — Event Study
- `community_detection.py` (695行) — コミュニティ検出 + 師弟関係

**Tier 2 (Important — JSON export対象)**:
- `contribution_attribution.py` (~250行) — Shapley値
- `potential_value.py` (~180行) — ポテンシャル推定
- `bias_detector.py` (~150行) — バイアス検出

**Tier 3 (低優先)**:
- `anime_value.py`, `compensation_analyzer.py`, `core_periphery.py`,
  `genre_specialization.py`, `growth_acceleration.py`, `insights_report.py`,
  `neo4j_direct.py`, `path_finding.py`, `structural_holes.py`,
  `studio_bias_correction.py`, `temporal_influence.py`,
  `structural_estimation_html.py`, `event_study_viz.py`

- **Files**: `tests/` (new test files)
- **Acceptance**: Tier 1 modules at 70%+ coverage
- **Complexity**: High（Tier 1 だけで ~2400行のテスト対象）

---

## P1-PERF — Pipeline Performance Optimization (残ボトルネック)

現在のパイプライン実行時間: **182.6s** (12,164 persons, 2.68M edges)
以下の5ボトルネックを解消すれば **60-80s** まで短縮可能。

**注意**: 長期放送作品（ポケモン等）や劇場アニメはスタッフが多くて当然。
スタッフ数によるフィルタリングは行わないこと。

### PERF-1: GraphML Export 高速化 (78.8s → 1-3s)

**効果: ★★★ (25-50x)** | **複雑度: Low** | **実装時間: 30分**

**問題箇所**: `src/analysis/graphml_export.py`

`nx.write_graphml()` (line 88) が 12,164ノード + 2.68Mエッジを
prettyprint付きXMLにシリアライズしている。float属性がフル精度で出力
されるため、生成ファイルが約246MBになり、I/Oバウンドで遅い。

**原因コード**:

```python
# line 57: float全精度で出力
attrs[key] = float(ps[key])  # e.g. 75.12345678901234

# line 88: prettyprint=Trueがデフォルト
nx.write_graphml(g, str(output_path))
```

**修正**:

1. **line 17** — 関数シグネチャに `prettyprint` と `round_decimals` パラメータ追加
2. **line 57** — float属性を `round(float(ps[key]), round_decimals)` に変更
3. **line 88** — `nx.write_graphml_lxml(g, str(output_path), prettyprint=False)` に変更

**呼び出し側**: `src/pipeline_phases/analysis_modules.py` line 126-128

```python
graphml_file = export_graphml(
    context.persons, context.credits, person_scores=scores_for_graphml,
    collaboration_graph=context.collaboration_graph,
    prettyprint=False, round_decimals=2,  # ← 追加
)
```

**受入条件**:
- 出力GraphMLが `nx.read_graphml()` で読み込み可能
- Gephiインポート可能（prettyprint=Falseでもフォーマットは同じ）
- スコアの精度低下は0.01以内（実用上影響なし）
- `pixi run test` 全通過
- 78.8s → 1-3s

---

### PERF-2: Influence Tree 高速化 (60s → 1-2s)

**効果: ★★★ (40-60x)** | **複雑度: Low** | **実装時間: 30分**

**問題箇所**: `src/analysis/influence.py`

`_get_highest_stage()` (lines 81-88) が全クレジットリストを線形スキャン
して1人分のキャリアステージを取得。これがmenteeループ内 (line 144) で
毎回呼ばれるため、O(mentee数 × 全クレジット数) = O(60K × 34K) の計算量。

**原因コード**:

```python
# lines 81-88: 全クレジットを毎回スキャンする関数
def _get_highest_stage(person_id: str, credits: list[Credit]) -> int:
    stages = [
        CAREER_STAGE.get(c.role, 0)
        for c in credits              # ← 34,637件を毎回全走査
        if c.person_id == person_id
    ]
    return max(stages) if stages else 0

# line 144: mentee毎に呼び出し (数万回)
highest = _get_highest_stage(mentee_id, credits)
```

**修正**:

1. `compute_influence_tree()` の冒頭（line 91付近）で person_id → 最高ステージ
   の辞書を1パスで構築する

```python
person_highest_stage: dict[str, int] = {}
for c in credits:
    stage = CAREER_STAGE.get(c.role, 0)
    if stage > person_highest_stage.get(c.person_id, 0):
        person_highest_stage[c.person_id] = stage
```

2. line 144 の呼び出しを `person_highest_stage.get(mentee_id, 0)` に置換
3. `_get_highest_stage()` 関数は削除可能（他で使われていなければ）

**追加修正（任意）**: `_find_mentor_mentee_pairs()` 内の年情報取得
(lines 64-73) でも `anime_map.get(aid)` を毎回呼んでいる。
`anime_years = {aid: a.year for aid, a in anime_map.items() if a.year}` を
事前構築すると追加で2-3x高速化。

**受入条件**:
- O(1) ルックアップで全クレジットの線形スキャンが消滅
- 出力（influence tree JSON）が修正前と同一
- `pixi run test` 全通過
- 60s → 1-2s

---

### PERF-3: Structural Estimation 高速化 (59.7s → 3-6s)

**効果: ★★☆ (10-20x)** | **複雑度: Medium** | **実装時間: 1-2時間**

**問題箇所**: `src/analysis/structural_estimation.py`

Event Study 推定関数 `estimate_event_study()` 内の demeaning ループ
(lines 682-704) で、各観測値に対して `mean_time_k` を計算するために
全観測リストを2回スキャンしている。これが 7期間 × 全観測 × 全観測 で
**O(k × n²)** になっている。

**原因コード**:

```python
# lines 695-698: 三重ネストの内側（各観測で全リスト2回走査）
mean_time_k = sum(
    1 for o, r in observations_with_reltime      # ← 全リスト走査1回目
    if o.person_id == obs.person_id and r == k
) / len([o for o, _ in observations_with_reltime  # ← 全リスト走査2回目
        if o.person_id == obs.person_id])
```

この `mean_time_k` は (person_id, k) のペアで一意に決まる値。
同じ person_id と k の組み合わせに対して何千回も再計算している。

**修正**:

1. ループ前に `Counter` で事前集計

```python
from collections import Counter

person_obs_count = Counter(
    obs.person_id for obs, _ in observations_with_reltime
)
person_k_count = Counter(
    (obs.person_id, rel_t) for obs, rel_t in observations_with_reltime
)
```

2. lines 695-698 を O(1) ルックアップに置換

```python
mean_time_k = person_k_count.get((obs.person_id, k), 0) / \
              max(person_obs_count.get(obs.person_id, 1), 1)
```

**追加問題箇所**: `estimate_fixed_effects()` 内 (lines 223-227付近)

```python
# person_year_data の全キーをスキャンして1人分を取得
all_person_years = [
    yr for (pid, yr), _ in person_year_data.items()
    if pid == person_id  # ← 全エントリスキャン
]
```

**修正**: ループ前に `defaultdict(list)` で person_id → years を構築し、
`person_years_index.get(person_id, [])` に置換。

**受入条件**:
- Event Study の demeaning が O(n) に削減
- Fixed Effects の person-year ルックアップが O(1)
- `pixi run test` 全通過、推定結果が修正前と同一
- 59.7s → 3-6s

---

### PERF-4: Contribution Attribution 高速化 (39.8s → 3-5s)

**効果: ★★☆ (8-15x)** | **複雑度: Medium** | **実装時間: 1-2時間**

**問題箇所1**: `src/analysis/contribution_attribution.py`

`compute_shapley_value_approximate()` (lines 150-179) で、各サンプリング
反復ごとに coalition 内の全スタッフの `estimate_marginal_contribution()` を
再計算している。同じ (person_id, role) に対する限界貢献値は定数なのに
毎回呼んでいる。

**原因コード**:

```python
# lines 150-179: sample_size (50-1000) 回のループ
for _ in range(min(sample_size, 1000)):
    staff_copy = list(all_staff)          # line 152: O(staff) リストコピー
    random.shuffle(staff_copy)            # line 153: O(staff) シャッフル
    position = next(i for i, (pid, _)     # line 157: O(staff) 線形探索
                    in enumerate(staff_copy) if pid == person_id)
    coalition = staff_copy[:position]     # line 162
    value_with_coalition = sum(
        estimate_marginal_contribution(   # lines 165-169: 毎回再計算
            pid, r, anime_value, person_scores, staff_quality_avg
        )
        for pid, r in coalition
    )
```

**修正**:

ループ前に限界貢献を全スタッフ分キャッシュ:

```python
marginal_cache = {
    pid: estimate_marginal_contribution(pid, r, anime_value, person_scores, avg)
    for pid, r in all_staff
}

for _ in range(sample_size):
    # ... shuffle, position ...
    value_with_coalition = sum(marginal_cache[pid] for pid, _ in coalition)
    value_with_person = value_with_coalition + marginal_cache[person_id]
```

**問題箇所2**: `src/pipeline_phases/supplementary_metrics.py` line 181

100アニメ分のループで毎回全クレジットを線形フィルタリング:

```python
# line 181: 全クレジットを100回スキャン
anime_credits = [c for c in context.credits if c.anime_id == anime_id]
```

**修正**: ループ前に `anime_credits_index: dict[str, list[Credit]]` を構築

```python
anime_credits_index = defaultdict(list)
for c in context.credits:
    anime_credits_index[c.anime_id].append(c)

# ループ内: O(1)
anime_credits = anime_credits_index.get(anime_id, [])
```

**追加修正（任意）**: 100アニメの処理は互いに独立なので
`ThreadPoolExecutor(max_workers=8)` で並列化可能。

**受入条件**:
- 限界貢献のキャッシュで冗長計算を排除
- クレジットの事前インデックスで O(1) ルックアップ
- `pixi run test` 全通過
- 39.8s → 3-5s

---

### PERF-5: Bridge Detection 高速化 (16.5s → 3-6s)

**効果: ★☆☆ (3-5x)** | **複雑度: Low** | **実装時間: 30分**

**問題箇所**: `src/analysis/bridges.py` lines 40-45

Python の二重ネストループでペアを生成。内側のスライス `plist[i + 1:]` が
毎回新しいリストを作成する。

**原因コード**:

```python
# lines 40-45
for anime_id, persons in anime_persons.items():
    plist = sorted(persons)
    for i, p1 in enumerate(plist):
        all_persons.add(p1)
        for p2 in plist[i + 1 :]:     # ← スライスで毎回リスト生成
            edges[(p1, p2)].append(anime_id)
```

**修正**:

`itertools.combinations` (C実装) に置換:

```python
import itertools

for anime_id, persons in anime_persons.items():
    plist = sorted(persons)
    all_persons.update(plist)
    for p1, p2 in itertools.combinations(plist, 2):
        edges[(p1, p2)].append(anime_id)
```

`plist` は既にソート済みなので `p1 < p2` が保証される（canonical order不要）。

**受入条件**:
- `itertools.combinations` 使用
- `pixi run test` 全通過、出力同一
- 16.5s → 3-6s

---

### PERF 合計インパクト見積

| # | ボトルネック | 現在 | 目標 | 効果 | 実装時間 |
|---|---|---|---|---|---|
| PERF-1 | GraphML Export | 78.8s | 1-3s | ★★★ | 30分 |
| PERF-2 | Influence Tree | 60.0s | 1-2s | ★★★ | 30分 |
| PERF-3 | Structural Estimation | 59.7s | 3-6s | ★★☆ | 1-2時間 |
| PERF-4 | Contribution Attribution | 39.8s | 3-5s | ★★☆ | 1-2時間 |
| PERF-5 | Bridge Detection | 16.5s | 3-6s | ★☆☆ | 30分 |
| | **合計** | **254.8s** | **11-22s** | | **3-6時間** |

**パイプライン全体**: 182.6s → **60-80s** (他の処理を含む見積)

**実装順序**: PERF-1 → PERF-2 (各30分で計140s削減) → PERF-3 → PERF-4 → PERF-5

**共通の受入条件**:
- `pixi run test` が全通過（現在1030 passed, 5 pre-existing failures in retry tests）
- `pixi run lint` がクリーン
- パイプライン出力JSONが修正前と実質同一（float丸め以外）

---

## P2 — Nice to Have (Future)

### P2-1: API Response Caching (Redis / In-Memory)

Heavy endpoints (ranking, search) re-read JSON files on every request. An
in-memory or Redis cache with TTL would reduce latency.

- **Files**: `src/api.py`, `src/utils/json_io.py`
- **Complexity**: Medium

### P2-2: Frontend SPA

Replace static HTML files with a proper single-page application (React/Vue/Svelte)
for score browsing, search, and visualization.

- **Files**: `frontend/` (new directory)
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

### Rust Extension (Phase 10) ✅

Pipeline acceleration from ~9min to ~1-2min via PyO3/maturin Rust extension.
Brandes' betweenness (rayon parallel), collaboration edge aggregation,
betweenness cache deduplication.

- Measured: betweenness 547x speedup, eigenvector 4.9x
- Files: `rust_ext/`, `src/analysis/graph_rust.py`, `tests/test_graph_rust.py`

### Pipeline Bottleneck Optimization ✅

Optimized two major bottlenecks discovered after Rust acceleration:

- `collaboration_strength.py`: Two-pass optimization (count first, detail later) — **101.8s → 29.9s (3.4x)**
- `graphml_export.py`: Accept pre-built collaboration_graph to avoid O(n²) recomputation — **112.7s → 78.8s (1.4x)**
- Total pipeline: **225s → 182.6s** (12,164 persons, 2.68M edges)

### Docker Deployment ✅

Multi-stage Dockerfile (Rust build + Python app), docker-compose with app + Neo4j.

- Files: `Dockerfile`, `docker-compose.yml`, `.dockerignore`

---

## Legal Compliance (Ongoing)

These are hard constraints, not backlog items:

- [ ] Never frame scores as "ability" — only "network position and density"
- [ ] Entity resolution false positives = potential defamation (信用毀損)
- [ ] Only use publicly available credit data from released works
- [ ] All reports include JA + EN disclaimers
- [ ] AniList rate limit: 90 req/min — exponential backoff implemented
- [ ] MAL (Jikan) rate limit: 3 req/s, 60 req/min — implemented

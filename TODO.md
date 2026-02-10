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

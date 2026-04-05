# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Animetor Eval** is a service that evaluates anime industry professionals (animators, directors, etc.) by analyzing credit data from publicly released works. It models the industry as a trust network and produces quantitative scores based on collaboration patterns, not subjective opinion.

The goal: make individual contributions visible so that studios pay fair compensation, contributing to a healthier anime industry. Scores reflect network position and collaboration density, never subjective "ability" judgments.

## Critical Design Principle: No Viewer Ratings

**anime.score (AniList/MAL viewer ratings) must NEVER be used in any scoring formula.**

Viewer ratings are driven by factors independent of staff contribution (source material popularity, marketing, streaming platform, broadcast timing). Using them contradicts the project's core principle.

See `todo.md` for the full audit of 16 contamination pathways being removed, and `docs/CALCULATION_COMPENDIUM.md` for the correct formulas.

### Permitted Data Sources

| Data | Nature | Example |
|------|--------|---------|
| **Credit records** | Objective fact | Person A credited as key_animator on Work X |
| **Roles** | Objective fact | 24 role types |
| **Work metadata** | Objective fact | Episode count, broadcast duration, format (TV/Movie/OVA) |
| **Studio info** | Objective fact | Production studio, co-production relationships |
| **Production scale** | Derived from credits | Staff count (computable from credit records) |
| **Timeline** | Objective fact | Active years, career duration, gaps |
| **Co-credit relationships** | Structural data | Who was credited on the same work |
| **Network position** | Derived data | Centrality, bridges, community structure |

### Prohibited Data Sources

| Data | Reason |
|------|--------|
| anime.score (viewer ratings) | Subjective, unrelated to production contribution |
| anime.popularity | Same — audience metrics, not staff quality |
| External reviews/critic scores | Subjective judgment |

**Exception:** anime.score may be stored in the database and displayed as informational metadata in reports, but must never enter any scoring formula, edge weight, or optimization target.

## Architecture

### Two-Layer Evaluation Model

**Layer 1: Network Profile** (reference — 3 axes)

| Axis | Algorithm | What It Measures |
|------|-----------|-----------------|
| **Authority** | Weighted PageRank | Proximity to high-profile directors/works in the collaboration graph (structural weights only: `role_weight x episode_coverage x duration_mult`) |
| **Trust** | Cumulative edge weight | Repeat engagements — being called back by the same supervisors |
| **Credit Density** | Credit frequency x role progression | Production involvement trajectory (replaces OpenSkill which was anime.score-dependent) |

**Layer 2: Individual Contribution Profile** (compensation basis)

| Metric | Method | What It Measures |
|--------|--------|-----------------|
| **peer_percentile** | Cohort ranking of person_fe | Position within same role x career year cohort |
| **opportunity_residual** | OLS regression | Individual contribution after controlling for opportunity factors (avg_staff_count, avg_studio_fe) |
| **consistency** | Normalized CV | Score stability across works |
| **independent_value** | Spillover analysis | Contribution independent of collaborator effects (using credit density, not anime.score) |

### AKM (Person Fixed Effect) — Core Component

The AKM decomposition estimates individual contribution net of studio effects:

```
log(production_scale_ij) = theta_i + psi_j + epsilon_ij
```

- `production_scale = staff_count x episodes x duration_mult` — measures "being called to large-scale productions"
- `theta_i` = person fixed effect (individual contribution)
- `psi_j` = studio fixed effect (studio baseline)
- **NOT** anime.score — the outcome variable is purely structural

### Integrated Value (IV)

```
IV_i = (lambda_1 x theta_i + lambda_2 x birank_i + lambda_3 x studio_exp_i + lambda_4 x awcc_i + lambda_5 x patronage_i) x D_i
```

- Lambda weights: fixed prior weights (CV optimization against anime.score is removed)
- Dormancy D applied multiplicatively after the weighted sum
- All 5 components must be free of anime.score contamination

### 10-Phase Pipeline

```
src/pipeline_phases/
|- data_loading.py          # Phase 1: Load from SQLite
|- validation.py            # Phase 2: Data quality checks
|- entity_resolution.py     # Phase 3: Name deduplication (5-step)
|- graph_construction.py    # Phase 4: NetworkX graphs
|- core_scoring.py          # Phase 5: Authority/Trust/Credit Density + AKM + IV
|- supplementary_metrics.py # Phase 6: Centrality, decay, career stage
|- result_assembly.py       # Phase 7: Build result dicts
|- post_processing.py       # Phase 8: Percentiles, confidence
|- analysis_modules.py      # Phase 9: 20+ modules (parallel)
|- export_and_viz.py        # Phase 10: JSON export + visualization
```

Each phase is independently testable via shared `PipelineContext` dataclass.

### Graph Model

- **Nodes**: Animators, directors, works (anime titles)
- **Edges**: Participation in a work, role held (24 role types), co-credit relationships
- **Edge weights**: `role_weight x episode_coverage x duration_mult` (structural only)
  - director prominence bonus, repeat collaboration bonus, role-based weighting
  - **No anime.score multiplier** — `_work_importance()` uses duration/format only

### Rust Extension

`rust_ext/` contains a PyO3/maturin-based `animetor_eval_core` module for graph algorithm acceleration:
- Brandes' betweenness centrality (rayon parallel) — 50-100x speedup
- Collaboration edge aggregation — 10-30x speedup
- Degree and eigenvector centrality
- Graceful fallback to Python/NetworkX via `src/analysis/graph_rust.py`

Build: `pixi run build-rust`

## Build & Run Commands

```bash
pixi install              # Install dependencies
pixi run test             # pytest tests/ -v
pixi run lint             # ruff check src/ tests/
pixi run format           # ruff format src/ tests/
pixi run pipeline         # Full pipeline
pixi run pipeline-viz     # Pipeline + visualization
pixi run pipeline-inc     # Incremental (skip if no data changes)
pixi run pipeline-resume  # Resume from crash checkpoint
pixi run bench            # Run performance benchmarks
pixi run build-rust       # Build Rust extension
pixi run serve            # Start API server (localhost:8000)
pixi run lab              # JupyterLab
```

## Directory Structure

```
animetor_eval/
|- src/
|   |- pipeline_phases/     # 10-phase pipeline (data_loading → export)
|   |- analysis/            # Analysis modules (grouped by domain)
|   |   |- scoring/         #   Core algorithms: AKM, BiRank, IV, PageRank
|   |   |- network/         #   Graph analysis: bridges, communities, trust
|   |   |- genre/           #   Genre affinity, ecosystem, specialization
|   |   |- studio/          #   Studio profiling, clustering, timeseries
|   |   |- va/              #   Voice actor: AKM, graph, trust, diversity
|   |   |- causal/          #   Causal inference: DML, structural estimation
|   |   |- graph.py         #   Core NetworkX graph builder (shared)
|   |   |- visualize.py     #   Static charts (matplotlib)
|   |   `- *.py             #   Career, cohort, compatibility, etc.
|   |- scrapers/            # Data collection (AniList, SeesaaWiki)
|   |- utils/               # Config, JSON I/O, role constants
|   |- viz/                 # v2 report architecture (chart_spec, renderers)
|   |- i18n/                # Internationalization (EN/JA)
|   |- models.py            # Pydantic v2 data models
|   |- database.py          # SQLite DAO (schema v26)
|   |- pipeline.py          # Pipeline orchestrator
|   |- api.py               # FastAPI server (42+ endpoints + WebSocket)
|   `- cli.py               # CLI (22+ commands, typer + Rich)
|- scripts/
|   |- generate_all_reports.py  # Main report generator
|   |- generate_reports_v2.py   # v2 architecture entry point
|   |- report_generators/       # Shared templates & helpers
|   `- maintenance/             # One-off scripts (scraping fixes, backfill)
|- tests/                   # 1947 tests (pytest)
|- docs/                    # All documentation
|   |- ARCHITECTURE.md      #   System design and data flow
|   |- CALCULATION_COMPENDIUM.md  # Formula reference
|   `- *.md                 #   Neo4j, LLM, event study guides
|- static/                  # Frontend (portfolio SPA, pipeline monitor)
|- rust_ext/                # PyO3/maturin Rust extension
|- result/json/             # 26 JSON pipeline outputs
|- CLAUDE.md                # Claude Code instructions
|- todo.md                  # Audit progress tracker
`- pixi.toml                # Dependencies
```

## Key Patterns

### Testing

- **Monkeypatch `DEFAULT_DB_PATH`** (not `get_connection`) — pipeline imports at module load
- Also patch `src.pipeline.JSON_DIR`, `src.analysis.visualize.JSON_DIR`, `src.utils.config.JSON_DIR`
- **structlog + pytest**: `cache_logger_on_first_use=False` in conftest.py
- **Dataclass return types**: Analysis functions return dataclass instances (attribute access, not dict)
- Integration tests use synthetic data (5 directors, 30 animators, 15 anime)

### Code Conventions

- **structlog** (never stdlib logging)
- **Pydantic v2** for data models
- **httpx async** for all HTTP
- **Role constants**: `src/utils/role_groups.py` is single source of truth
- **JSON I/O**: `src/utils/json_io.py` with 22+ named loaders and TTL caching

## Tech Stack

- Python 3.12, pixi (conda-forge + pypi)
- NetworkX (graph), Pydantic v2 (models)
- httpx (async HTTP), structlog (logging), typer + Rich (CLI)
- FastAPI + uvicorn + WebSocket (API)
- matplotlib + Plotly (visualization)
- Rust/PyO3/maturin (graph algorithm acceleration)
- SQLite WAL mode (storage)
- ruff (lint/format), pytest (tests)

## Legal Constraints

These are hard requirements, not suggestions:

- **Never frame low scores as "lack of ability"** — scores represent network density and position only
- **Public benefit framing**: The service aims to make individual contributions visible, supporting fair compensation and industry health (public interest purpose)
- **Data source restriction**: Only publicly available credit data from released works
- **No viewer ratings in scoring**: anime.score must never be used as an input, outcome variable, optimization target, or edge weight in any scoring formula
- **Entity resolution accuracy**: Name matching errors can constitute defamation under Japanese law — treat this as a blocking quality gate
- **Disclaimers**: All reports include JA + EN disclaimers
- **Compensation basis**: When presenting as compensation evidence, confidence intervals are required — these must be analytically derived (SE = sigma/sqrt(n)), not heuristic

## Known Issues (see todo.md)

The project is undergoing a major correctness audit. Key categories:

- **anime.score contamination** (16 pathways): Being removed from all scoring formulas
- **Implementation bugs** (B01-B16): Including studio_exposure inconsistency, IV renormalization missing, BiRank update order, closeness weight inversion, confidence interval scale mismatch
- **Design concerns** (D01-D27): Including unjustified magic numbers, missing significance tests, circular dependencies

See `todo.md` for the complete audit with code references, impact analysis, and fix roadmap.

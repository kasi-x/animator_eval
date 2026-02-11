# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Animetor Eval** is a service that evaluates anime industry professionals (animators, directors, etc.) by analyzing credit data from publicly released works. It models the industry as a trust network and produces quantitative scores based on collaboration patterns, not subjective opinion.

The goal: make individual contributions visible so that studios pay fair compensation, contributing to a healthier anime industry. Scores reflect network position and collaboration density, never subjective "ability" judgments.

## Architecture

### Two-Layer Evaluation Model

**Layer 1: Network Profile** (reference — existing 3 axes)

| Axis | Algorithm | What It Measures |
|------|-----------|-----------------|
| **Authority** | Weighted PageRank | Proximity to high-profile directors/works in the collaboration graph |
| **Trust** | Cumulative edge weight | Repeat engagements — being called back by the same supervisors |
| **Skill** | OpenSkill (PlackettLuce) | Recent project contributions and growth trajectory |

**Layer 2: Individual Contribution Profile** (compensation basis — new)

| Metric | Method | What It Measures |
|--------|--------|-----------------|
| **peer_percentile** | Cohort ranking | Position within same role x career year cohort |
| **opportunity_residual** | OLS regression | Individual contribution after controlling for opportunity factors |
| **consistency** | 1 - CV | Score stability across works |
| **independent_value** | Spillover analysis | Contribution independent of collaborator effects |

### 10-Phase Pipeline

```
src/pipeline_phases/
├── data_loading.py          # Phase 1: Load from SQLite
├── validation.py            # Phase 2: Data quality checks
├── entity_resolution.py     # Phase 3: Name deduplication (5-step)
├── graph_construction.py    # Phase 4: NetworkX graphs
├── core_scoring.py          # Phase 5: Authority/Trust/Skill
├── supplementary_metrics.py # Phase 6: Centrality, decay, career stage
├── result_assembly.py       # Phase 7: Build result dicts
├── post_processing.py       # Phase 8: Percentiles, confidence
├── analysis_modules.py      # Phase 9: 20+ modules (parallel)
└── export_and_viz.py        # Phase 10: JSON export + visualization
```

Each phase is independently testable via shared `PipelineContext` dataclass.

### Graph Model

- **Nodes**: Animators, directors, works (anime titles)
- **Edges**: Participation in a work, role held (24 role types), co-credit relationships
- Edge weights: director prominence bonus, repeat collaboration bonus, role-based weighting

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
pixi run test             # pytest tests/ -v (1319 tests)
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
├── src/
│   ├── pipeline_phases/     # 10-phase pipeline modules
│   ├── analysis/            # 41+ analysis modules
│   ├── scrapers/            # Data collection (AniList, MAL, MediaArts, JVMG)
│   ├── utils/               # Config, JSON I/O, role groups, performance
│   ├── i18n/                # Internationalization (EN/JA)
│   ├── pipeline.py          # Pipeline orchestrator
│   ├── models.py            # Pydantic v2 data models
│   ├── database.py          # SQLite DAO (schema v7)
│   ├── api.py               # FastAPI server (42+ endpoints + WebSocket)
│   ├── cli.py               # CLI (22 commands, typer + Rich)
│   ├── monitoring.py        # Data freshness monitoring
│   └── websocket_manager.py # WebSocket progress broadcasting
├── rust_ext/                # PyO3/maturin Rust extension
├── static/                  # Frontend (portfolio SPA, pipeline monitor)
├── benchmarks/              # Performance benchmarks
├── tests/                   # 1319 tests
├── result/json/             # 26 JSON pipeline outputs
└── pixi.toml                # Dependencies (composable features)
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
- NetworkX (graph), OpenSkill (Skill scoring), Pydantic v2 (models)
- httpx (async HTTP), structlog (logging), typer + Rich (CLI)
- FastAPI + uvicorn + WebSocket (API)
- matplotlib + Plotly (visualization)
- Rust/PyO3/maturin (graph algorithm acceleration)
- SQLite WAL mode (storage)
- ruff (lint/format), pytest (1319 tests)

## Legal Constraints

These are hard requirements, not suggestions:

- **Never frame low scores as "lack of ability"** — scores represent network density and position only
- **Public benefit framing**: The service aims to make individual contributions visible, supporting fair compensation and industry health (公益目的)
- **Data source restriction**: Only publicly available credit data from released works
- **Entity resolution accuracy**: Name matching errors can constitute defamation (信用毀損) under Japanese law — treat this as a blocking quality gate
- **Disclaimers**: All reports include JA + EN disclaimers
- **Compensation basis**: When presenting as compensation evidence, confidence intervals are required

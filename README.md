# Animetor Eval

> Anime industry professional evaluation system — Making individual contributions visible to support fair compensation and a healthier industry

[![Tests](https://img.shields.io/badge/tests-1319%20passing-success)](https://github.com/kasi-x/animator_eval)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Overview

**Animetor Eval** is a system that visualizes and quantifies the contributions of anime industry professionals (animators, directors, etc.) based on **credit data**. It focuses on individuals, providing studios with evidence for fair compensation and contributing to a healthier industry. The industry is modeled as a trust network and scored across three axes:

| Axis | Algorithm | What It Measures |
|---|---|---|
| **Authority** | Weighted PageRank | Proximity to prominent directors and works in the collaboration graph |
| **Trust** | Repeat Engagement | Number of repeat engagements by the same directors |
| **Skill** | OpenSkill (PlackettLuce) | Recent project contributions and growth trajectory |

### Core Principles

- **Objectivity**: Uses only publicly available credit data
- **Network metrics**: Scores represent network position and density, not "ability"
- **Purpose**: Visualize individual contributions to support fair compensation and industry health (public benefit)
- **Legal considerations**: Entity resolution accuracy is implemented conservatively due to defamation risk

## Key Features

- **Two-layer evaluation model**: Layer 1 (Authority x Trust x Skill) + Layer 2 (Individual contribution metrics)
- **Individual contribution metrics**: Peer comparison percentile, opportunity-controlled residual, consistency, independent contribution
- **Entity resolution**: 5-step name matching (exact match -> cross-source -> romaji -> similarity -> AI-assisted)
- **Career analysis**: Role transitions, growth trends, milestone detection
- **Network analysis**: Collaboration intensity, director circles, bridge detection
- **Visualization**: 23 matplotlib static charts + 6 Plotly interactive visualizations
- **Parallel execution**: 20 modules running concurrently via ThreadPoolExecutor (4-6x speedup)
- **Rust extension**: Graph algorithm acceleration via PyO3/maturin (50-100x speedup)
- **WebSocket monitoring**: Real-time pipeline progress broadcasting (10-phase tracking)
- **Internationalization (i18n)**: Full English/Japanese support (CLI, API, frontend)
- **Performance monitoring**: Detailed metrics (percentiles, memory delta, cache statistics)
- **REST API**: 42+ endpoints (FastAPI + WebSocket)
- **CLI**: 22 commands (typer + Rich)
- **Frontend**: Portfolio SPA (search, profile, ranking)

## Quick Start

### Requirements

- Python 3.12+
- [pixi](https://pixi.sh/) package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/kasi-x/animator_eval.git
cd animator_eval

# Install dependencies
pixi install

# Run tests (1319 tests, ~270 seconds)
pixi run test
```

### Running the Pipeline

```bash
# Run full pipeline (score calculation + JSON output)
pixi run pipeline

# Run with visualizations
pixi run pipeline-viz

# Incremental mode (skip if no data changes)
pixi run pipeline-inc

# Resume from crash checkpoint
pixi run pipeline-resume

# Data validation only (--dry-run)
pixi run validate
```

### CLI Usage Examples

```bash
# Show ranking (top 20)
pixi run ranking

# Person profile
pixi run profile "Hayao Miyazaki"

# Search for a person
pixi run search "Tanaka"

# Find similar persons
pixi run similar "person_id_123"

# Compare scores (2 persons)
pixi run compare "person_1" "person_2"

# Career timeline
pixi run timeline "person_id_123"

# Database statistics
pixi run stats
```

### Starting the API Server

```bash
pixi run api

# Open http://localhost:8000/docs in your browser
# OpenAPI (Swagger) documentation will be displayed
```

## Highlights

### WebSocket Real-Time Monitoring

View pipeline progress in real time during execution:

```bash
# After starting the API server
# Visit http://localhost:8000/static/pipeline_monitor_i18n.html
```

**Features**:
- 10-phase progress tracking (data loading -> export/visualization)
- Execution time per phase (milliseconds)
- Real-time log display
- Gradient UI
- Language switcher (EN/JA)

**WebSocket endpoint**: `ws://localhost:8000/ws/pipeline`

### Internationalization (i18n)

Full multilingual support (English and Japanese):

**CLI**:
```bash
# Display in English
animetor-eval stats --lang en

# Display in Japanese
animetor-eval stats --lang ja

# Auto-detect from environment variable
export ANIMETOR_LANG=ja
animetor-eval stats
```

**API**:
```bash
# Get translation dictionary
curl http://localhost:8000/api/i18n/en
curl http://localhost:8000/api/i18n/ja
```

**Frontend**: Real-time language switching via toggle button

### Performance Monitoring

Detailed performance metrics:

```bash
# Show latest performance report
animetor-eval performance

# List all reports
animetor-eval performance --all

# Show specific report
animetor-eval performance --file performance_20260210_123456.json
```

**Tracked metrics**:
- **Timing**: Median, P95, P99, standard deviation
- **Memory**: RSS, VMS, usage, delta
- **Cache**: Hit rate, hits/misses
- **Counters**: Custom metrics

**Auto-export**: Saved to `result/json/performance_TIMESTAMP.json` after pipeline execution

## Architecture

### Pipeline (10 Phases)

```
src/pipeline_phases/
├── data_loading.py          # Phase 1: Load data from DB
├── validation.py            # Phase 2: Data quality checks
├── entity_resolution.py     # Phase 3: Name deduplication (5-step)
├── graph_construction.py    # Phase 4: NetworkX graph construction
├── core_scoring.py          # Phase 5: Authority/Trust/Skill calculation
├── supplementary_metrics.py # Phase 6: 8 supplementary metrics
├── result_assembly.py       # Phase 7: Result data assembly
├── post_processing.py       # Phase 8: Percentiles, confidence intervals
├── analysis_modules.py      # Phase 9: 20 analysis modules (parallel)
└── export_and_viz.py        # Phase 10: JSON export + visualization
```

### Graph Model

- **Nodes**: Animators, directors, works (anime titles)
- **Edges**: Work participation, roles (24 types), collaboration relationships
- **Edge weights**: Director prominence bonus, repeat collaboration bonus, role-based weighting

### Data Sources

- [AniList](https://anilist.co/) GraphQL API
- [Jikan](https://jikan.moe/) (unofficial MAL REST API)
- [Media Arts Database](https://mediaarts-db.bunka.go.jp/) JSON-LD dump
- [Wikidata](https://www.wikidata.org/) SPARQL (JVMG)

## Performance Optimizations

The project has undergone comprehensive refactoring (Phases 1-4 + parallelization) achieving the following optimizations:

| Optimization | Method | Impact |
|---|---|---|
| **Graph construction** | Pre-aggregate edges | 3-5x speedup |
| **Entity resolution** | First-character blocking + LRU cache | 10-100x speedup |
| **Trust calculation** | Constant hoisting + precomputation | 40-50% speedup |
| **Analysis phase** | ThreadPoolExecutor (20 parallel) | 4-6x speedup |
| **API responses** | TTL cache (300s) | 30-50% speedup |
| **Rust extension** | PyO3/maturin + rayon parallel | 50-100x speedup |

## Output Files

### JSON (26 files)

```
result/json/
├── scores.json              # All person scores (sorted by composite)
├── circles.json             # Director circles
├── anime_stats.json         # Anime quality statistics
├── summary.json             # Pipeline execution summary
├── transitions.json         # Role transition analysis
├── influence.json           # Influence tree
├── crossval.json            # Cross-validation results
├── studios.json             # Studio analysis
├── seasonal.json            # Seasonal trends
├── collaborations.json      # Collaboration pairs (top 500)
├── outliers.json            # Statistical outliers
├── teams.json               # Team composition patterns
├── growth.json              # Growth trends
├── time_series.json         # Time series analysis
├── decades.json             # Decade-by-decade analysis
├── tags.json                # Person tags (auto-labeling)
├── role_flow.json           # Role flow analysis
├── bridges.json             # Bridge node detection
├── mentorships.json         # Mentor relationship inference
├── milestones.json          # Career milestones
├── network_evolution.json   # Network evolution
├── genre_affinity.json      # Genre affinity
├── productivity.json        # Productivity metrics
├── performance.json         # Performance monitoring
├── graphml/                 # GraphML export (Neo4j compatible)
└── ...
```

### Other Outputs

- **CSV**: `scores.csv` (UTF-8 BOM, with percentiles)
- **SQLite**: `result/db/animetor_eval.db` (score history, execution history)
- **Visualization**: `result/visualizations/*.png` (23 static + 6 interactive HTML)

## Development

### Running Tests

```bash
pixi run test              # All tests (1319)
pixi run lint              # ruff lint
pixi run format            # ruff format
```

### Generating Synthetic Data

```bash
# Generate synthetic data for testing/demo
pixi run python -c "
from src.synthetic import generate_synthetic_data
persons, anime, credits = generate_synthetic_data(
    n_directors=5,
    n_animators=30,
    n_anime=15
)
print(f'Generated {len(persons)} persons, {len(anime)} anime, {len(credits)} credits')
"
```

### Jupyter Lab

```bash
pixi run lab
# Analysis notebooks are saved in result/notebooks/
```

## API Endpoints

42+ endpoints + WebSocket (see http://localhost:8000/docs for details):

### New
- `GET /api/i18n/{language}` - Get translation dictionary (en/ja)
- `POST /api/pipeline/run` - Async pipeline execution
- `WS /ws/pipeline` - Real-time progress broadcasting
- `GET /static/pipeline_monitor_i18n.html` - Monitoring UI
- `GET /static/portfolio.html` - Portfolio SPA

### Persons
- `GET /api/persons` - All person scores (paginated)
- `GET /api/persons/search` - Person search
- `GET /api/persons/{id}` - Profile details
- `GET /api/persons/{id}/profile` - Individual contribution profile (two-layer model)
- `GET /api/persons/{id}/similar` - Similar persons
- `GET /api/persons/{id}/history` - Score history
- `GET /api/persons/{id}/network` - Network analysis
- `GET /api/persons/{id}/milestones` - Career milestones

### Ranking & Statistics
- `GET /api/ranking` - Ranking (with filters)
- `GET /api/stats` - DB statistics
- `GET /api/summary` - Pipeline summary
- `GET /api/data-quality` - Data quality report

### Anime
- `GET /api/anime` - Anime statistics list
- `GET /api/anime/{id}` - Anime details

### Analysis
- `GET /api/transitions` - Role transitions
- `GET /api/crossval` - Cross-validation
- `GET /api/influence` - Influence tree
- `GET /api/studios` - Studio analysis
- `GET /api/seasonal` - Seasonal trends
- `GET /api/collaborations` - Collaboration pairs
- `GET /api/outliers` - Outlier detection
- `GET /api/teams` - Team analysis
- `GET /api/growth` - Growth trends
- `GET /api/time-series` - Time series
- `GET /api/decades` - Decade-by-decade
- `GET /api/tags` - Person tags
- `GET /api/role-flow` - Role flow
- `GET /api/bridges` - Bridge detection
- `GET /api/mentorships` - Mentor relationships
- `GET /api/network-evolution` - Network evolution
- `GET /api/genre-affinity` - Genre affinity
- `GET /api/productivity` - Productivity

### Data Quality & Monitoring
- `GET /api/freshness` - Data source freshness
- `GET /api/studio-disparity` - Inter-studio compensation disparity analysis

### Neo4j
- `GET /api/neo4j/path` - Shortest path search
- `GET /api/neo4j/common` - Common collaborators
- `GET /api/neo4j/neighborhood` - Neighborhood exploration
- `GET /api/neo4j/stats` - Graph statistics

### Utilities
- `GET /api/compare` - Compare scores of 2 persons
- `GET /api/recommend` - Recommendations
- `GET /api/predict` - Predictions
- `GET /api/health` - Health check

## CLI Commands

22 commands available (all support `--lang en/ja` option):

### Basic
- `stats` - Database statistics (i18n supported)
- `ranking` - Score ranking
- `profile` - Person profile
- `search` - Person search
- `compare` - Score comparison
- `similar` - Similar person search

### Career Analysis
- `timeline` - Career timeline
- `history` - Score history
- `milestones` - Milestones
- `productivity` - Productivity analysis

### Network Analysis
- `bridges` - Bridge detection
- `mentorships` - Mentor relationships
- `net-evolution` - Network evolution
- `genre-affinity` - Genre affinity

### Validation & Analysis
- `crossval` - Cross-validation
- `influence` - Influence tree
- `validate` - Data validation

### Utilities
- `export` - Export
- `performance` - Performance report
- `freshness` - Data source freshness check
- `neo4j-export` - Neo4j export
- `neo4j-query` - Neo4j query execution
- `neo4j-stats` - Neo4j statistics

## Tech Stack

- **Language**: Python 3.12
- **Package management**: pixi (conda-forge + pypi)
- **Graph**: NetworkX
- **Scoring**: OpenSkill (PlackettLuce)
- **Data models**: Pydantic v2
- **HTTP**: httpx (async)
- **Logging**: structlog
- **CLI**: typer + Rich (i18n supported)
- **API**: FastAPI + uvicorn + WebSocket
- **Visualization**: matplotlib + Plotly
- **Internationalization**: JSON-based i18n (EN/JA)
- **Real-time communication**: WebSocket (progress broadcasting)
- **Performance**: Detailed metrics tracking (percentile, memory delta)
- **DB**: SQLite (WAL mode)
- **Acceleration**: Rust extension (PyO3/maturin, rayon parallel)
- **Testing**: pytest (1319 tests)
- **Lint/Format**: ruff

## Directory Structure

```
animetor_eval/
├── src/
│   ├── pipeline_phases/        # 10-phase pipeline (data_loading → export)
│   ├── analysis/               # Analysis modules (grouped by domain)
│   │   ├── scoring/            #   Core algorithms: AKM, BiRank, IV, PageRank
│   │   ├── network/            #   Graph analysis: bridges, communities, trust
│   │   ├── genre/              #   Genre affinity, ecosystem, specialization
│   │   ├── studio/             #   Studio profiling, clustering, timeseries
│   │   ├── va/                 #   Voice actor: AKM, graph, trust, diversity
│   │   ├── causal/             #   Causal inference: DML, structural estimation
│   │   ├── graph.py            #   Core NetworkX graph builder (shared)
│   │   ├── visualize.py        #   Static charts (matplotlib)
│   │   ├── visualize_interactive.py  # Interactive charts (Plotly)
│   │   └── *.py                #   Career, cohort, compatibility, etc.
│   ├── scrapers/               # Data collection (AniList, SeesaaWiki)
│   ├── utils/                  # Config, JSON I/O, role constants
│   ├── viz/                    # v2 report architecture (chart_spec, renderers)
│   ├── i18n/                   # EN/JA translations
│   ├── models.py               # Pydantic v2 data models
│   ├── database.py             # SQLite DAO (schema v26)
│   ├── pipeline.py             # Pipeline orchestrator
│   ├── api.py                  # FastAPI server (42+ endpoints, WebSocket)
│   └── cli.py                  # CLI (22+ commands, typer + Rich)
├── scripts/
│   ├── generate_all_reports.py # Main report generator (HTML + charts)
│   ├── generate_reports_v2.py  # v2 architecture entry point
│   ├── report_generators/      # Shared templates & helpers for reports
│   └── maintenance/            # One-off scripts (scraping fixes, backfill)
├── tests/                      # 1947 tests (pytest)
├── docs/                       # All documentation
│   ├── ARCHITECTURE.md         #   System design and data flow
│   ├── CALCULATION_COMPENDIUM.md # Formula reference for all metrics
│   ├── STRUCTURAL_ESTIMATION.md
│   └── *.md                    #   Neo4j, LLM, event study guides, etc.
├── static/                     # Frontend (portfolio SPA, pipeline monitor)
├── rust_ext/                   # PyO3/maturin Rust extension (graph speedup)
├── result/
│   ├── json/                   # 26 pipeline JSON outputs
│   └── reports/                # Generated HTML reports
├── CLAUDE.md                   # Claude Code instructions
├── todo.md                     # Audit progress tracker
└── pixi.toml                   # Dependencies
```

## Legal Considerations

### Data Sources
- Uses only publicly available credit data
- Scraping strictly adheres to rate limits (AniList: 90 req/min, Jikan: 3 req/s)

### Score Interpretation
- Scores are **network position and density metrics**, not measurements of "ability" or "talent"
- All outputs include the following disclaimer:

> **Disclaimer**: These scores are network density and position metrics based on credit data. They do not evaluate individual ability or talent. The purpose is to visualize individual contributions to support fair compensation and a healthier industry.

### Entity Resolution Accuracy
- False positive matches directly create defamation risk
- 5-step conservative name matching process implemented
- AI assistance applied only with minimum confidence 0.8, within same source

## License

MIT License

## Contributing

Issues and Pull Requests are welcome.

## References

- Page, L., Brin, S., et al. (1999). "The PageRank Citation Ranking"
- Weng, R.C., Lin, C.J. (2011). "A Bayesian Approximation Method for Online Ranking"
- Newman, M.E.J. (2010). "Networks: An Introduction"

## Links

- [Documentation](docs/)
- [API Specification](http://localhost:8000/docs) (after starting the server)
- [Issues](https://github.com/kasi-x/animator_eval/issues)

---

**Animetor Eval** - Objective evaluation system for the anime industry through network analysis

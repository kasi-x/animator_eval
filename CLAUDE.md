# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Animetor Eval** is a service that evaluates anime industry professionals (animators, directors, etc.) by analyzing credit data from publicly released works. It models the industry as a trust network and produces quantitative scores based on collaboration patterns, not subjective opinion.

The goal: make individual contributions visible so that studios pay fair compensation, contributing to a healthier anime industry. Scores reflect network position and collaboration density, never subjective "ability" judgments.

## Report Writing Philosophy

See `docs/REPORT_PHILOSOPHY.md` for the full philosophy (v2). Key principles:

- **Transparent perspectivism**: "Objective disclosure" is impossible — every metric embeds choices (which indicator, which time window, which threshold). The obligation is to make those choices visible and verifiable, not to hide them behind a neutral tone.
- **Findings / Interpretation structural separation**: Every report section has a Findings layer (no evaluative adjectives, no causal verbs, no normative language) and an optional Interpretation layer (explicitly labelled, first-person subject, at least one alternative interpretation stated).
- **No "implied advocacy"**: Do not arrange facts to lead the reader to a predetermined conclusion without stating it. If a conclusion follows from the data, state it explicitly in the Interpretation section. The v1 "Implied Business Value" approach is prohibited.
- **Document separation by audience**: Executive brief (2–4 pages, findings + charts only) / Main report / Technical appendix / Data statement — not three layers crammed into one document.
- **Mandatory method gates**: Confidence intervals required for all individual-level estimates. Null-model comparison required for all group-level claims. Holdout validation required for any predictive claim. Publication blocked until these are satisfied.
- **No adjectives that interpret**: Use the narrowest accurate label. "翌年クレジット可視性喪失率" not "離職率".

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

### Three-Layer Database Model (v53-v54)

**BRONZE layer** (source data, unmodified): `src_anilist_anime`, `src_mal_anime`, `src_jvmg_*`
- Raw external data including anime.score
- Never used for scoring — retained for audit and historical comparison only
- Updates from scrapers; immutable

**SILVER layer** (canonical data, score-free): `anime`, `anime_external_ids`, `anime_display`, `credits`, `persons`, `roles`
- Normalized structural data for all computations
- `anime` table contains NO score / popularity / description / genres / studios columns (v53 migration)
- `credits.source` column dropped (v54 migration); only `evidence_source` remains
- All scoring algorithms read exclusively from silver layer
- **Display lookup helper**: `src/utils/display_lookup.py` provides the ONLY access path from silver to bronze (used only for UI/report display, never for computation)

**GOLD layer** (analysis output, audience-specific): `scores`, `score_history`, `meta_*` tables
- Computed results and metadata lineage (`meta_lineage` table)
- One row per person-audience combination
- `meta_lineage` records formula version, CI method, null model, holdout validation, inputs hash

**Key constraint**: All analysis code (`src/analysis/`, `src/pipeline_phases/`) uses SILVER layer only. Display lookup is imported only by:
- Report generators (for UI display metadata)
- API layer (for endpoint responses)
- Never by analysis or scoring modules

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

## Schema Management (SQLModel + Atlas)

The project uses **SQLModel + Atlas** for schema-as-code and automated migrations:

### Single Source of Truth

- **`src/models_v2.py`**: 37 SQLModel table definitions (SILVER, BRONZE, GOLD layers)
  - All constraints, foreign keys, indexes defined in Python
  - Never scatter DDL — always update SQLModel first
  - Version controlled and peer reviewable

### 3-Layer Architecture (Detailed)

#### BRONZE Layer (Source Data)
Raw external data from scrapers, **includes anime.score**:
- `src_anilist_anime`, `src_anilist_credits`, `src_anilist_persons` — AniList GraphQL
- `src_ann_*` — Anime News Network
- `src_allcinema_*` — AllCinema
- `src_seesaawiki_*` — SeesaaWiki
- `src_mal_anime`, `src_mal_characters` — MyAnimeList
- `src_madb_anime` — MADB (Japanese production database)

**Data policy**: Immutable snapshots of external sources. Retained for audit trail and historical comparison. Never used for scoring.

#### SILVER Layer (Canonical Data)
Normalized, integrated, **score-free** data for all computations:
- **Core**: `anime`, `persons`, `credits`, `studios`, `anime_studios`
- **Lookup tables**: `roles` (24 standard roles), `sources` (5 data sources)
- **Relationships**: `anime_genres`, `anime_tags`, `anime_relations`, `characters`, `character_voice_actors`
- **Normalization**: `anime_external_ids`, `person_external_ids`, `person_aliases`
- **Metadata**: `analysis`, `display_lookup`, `ext_ids`

**Key constraint**: NO anime.score / popularity / favourites / description in SILVER. These live only in BRONZE.

#### GOLD Layer (Analysis Output)
Precomputed metrics for reporting (one row per person-audience combination):
- **Aggregation**: `agg_genre_affinity`, `agg_role_ecosystem`, `agg_studio_team_composition`
- **Features**: `feat_career_dynamics`, `feat_network_centrality` (+ 15+ more from existing pipeline)
- **Metadata**: `meta_policy_score`, `meta_hr_observation` (+ 10+ audience-specific tables)
- **Audit**: `meta_lineage` (formula version, CI method, null model, holdout validation, inputs hash)

**Data policy**: Computed from SILVER layer only. One write per pipeline run. Read by report generators and API.

### Display Lookup (Bronze ↔ Report Access)

Reports need anime.score, description, images (from BRONZE) for display. Use helper:

```python
# reports/person_card.py
from src.utils.display_lookup import get_display_metadata

display = get_display_metadata(anime_id, 'anilist')  # ← Only path from SILVER to BRONZE
return {
    'score': display['score'],  # From BRONZE
    'description': display['description'],  # From BRONZE
    'person_fe': person_scores['theta_i'],  # From GOLD (computed from SILVER)
}
```

**Rule**: Analysis code (`src/analysis/`+) never imports `display_lookup`. Reports only.

### Automated Migrations

- **`atlas.hcl`**: Atlas configuration (dev/prod environments)
- **`migrations/v55_add_gold_layer.sql`**: Generated migration for GOLD tables
- **Future**: `pixi run python -m atlas migrate diff` auto-generates new migrations
- **Idempotency**: All migration statements use `IF NOT EXISTS`

### Generated Documentation

- **`docs/schema.dbml`**: DBDiagram.io-compatible ER diagram (auto-generated via `scripts/generate_dbml.py`)
- **`docs/DATA_DICTIONARY.md`**: Column-level documentation (auto-generated from SQLModel)

## Report Briefs Architecture (Phase 4)

### Overview

Report Briefs is a 3-audience document system replacing the monolithic report architecture. Each brief is independently published but shares infrastructure (method gates, vocabulary enforcement, lineage tracking).

### Briefs

| Brief | Audience | Primary Sections | Use Case |
|-------|----------|------------------|----------|
| **Policy Brief** | Policymakers, labor regulators | Market concentration, gender bottleneck, attrition, policy recommendations | Antitrust review, labor regulation, equity policy |
| **HR Brief** | Studio managers, recruiters | Team chemistry, succession planning, compensation fairness, retention action | Hiring strategy, talent development, retention |
| **Business Brief** | Investors, studio executives | Market whitespace, emerging teams, undervalued staff, investment action | M&A, partnerships, growth opportunities |

### Infrastructure (scripts/report_generators/)

```
report_brief.py              # Base class + MethodGate, LineageMetadata
├── ReportBrief              # Abstract brief with validation, vocabulary enforcement
├── MethodGate               # Transparent methodology declaration
└── LineageMetadata          # Data lineage + confidence intervals

briefs/
├── __init__.py
├── policy_brief.py          # Policy brief implementation (4 sections)
├── hr_brief.py              # HR brief implementation (4 sections)
└── business_brief.py        # Business brief implementation (4 sections)

generate_briefs_v2.py        # Orchestrator: generate → validate → summarize
```

### Key Features

**1. Method Gates (3 per brief)**
Each brief declares its methodology transparently:
- Algorithm: What calculation method?
- CI/Validation: How do we know it works?
- Null model: What would random data produce?
- Confidence intervals: What's the uncertainty band?
- Limitations: What could go wrong?

**2. Vocabulary Enforcement**
Automated detection blocks prohibited terms (ability, skill, talent, competence, capability) using regex with word boundaries. Prevents misinterpretation of scores as subjective "talent" judgments.

**3. Findings/Interpretation Separation**
- **Findings**: Neutral facts, no adjectives, no causal language
- **Interpretation**: First-person ("I observe..."), multiple hypotheses, decision implications

**4. Lineage Tracking**
Every brief records:
- Data cutoff date
- Generation timestamp
- Analyst notes
- Confidence interval source (analytical vs. heuristic)

### Usage

```bash
# Generate all briefs (regenerates JSON files)
task report-briefs

# Validate existing briefs (check gates + vocabulary)
task report-validate

# Python API
from scripts.report_generators.briefs.policy_brief import generate_policy_brief

policy = generate_policy_brief()
policy.validate()  # (is_valid, error_list)
policy.to_dict()   # JSON-serializable dict
```

### Output Format

All briefs export to `result/json/{brief_id}_brief.json` with structure:
```json
{
  "metadata": { "title", "description", "audience", "target_readers" },
  "sections": {
    "section_1": { "findings": "...", "interpretation": "..." },
    ...
  },
  "method_gates": [
    { "name", "algorithm", "ci_method", "validation_method", "null_model", "limitations" },
    ...
  ],
  "lineage": { "data_cutoff", "analyst_notes", "confidence_interval_source" },
  "generated_at": "2026-04-22T12:58:00Z"
}
```

### Vocabulary Rules

**Prohibited (all exact word matches):**
- ability, skill, talent, competence, capability

**Permitted (preferred terms):**
- network position, centrality, opportunity
- collaboration density, co-credit frequency
- production scale, studio exposure
- career trajectory, role progression

**Rationale:** These prohibited terms imply subjective judgment. Scores measure structural network position, not individual quality.

## Technical Appendix Architecture (Phase 2C)

### Overview

The Technical Appendix consolidates 15+ research reports into a unified document with cross-references to the 3 main briefs (Policy, HR, Business). Each technical report includes methodology, validation, and links to specific brief sections it supports.

### Core Components

**TechnicalReport** (`technical_appendix.py`)
- Metadata: id, title, category, description, file_path
- Methodology: algorithm, data_source, time_window
- Quality gates: has_confidence_intervals, has_null_model, has_validation
- Cross-references: briefs_referenced, sections_referenced (e.g., {"policy": ["market_concentration"]})
- Deprecation tracking: deprecated flag + reason

**TechnicalAppendix**
- Aggregates multiple TechnicalReport instances
- Validates: file accessibility, brief references, core reports have gates
- Exports: unified JSON with category index and cross-reference matrix

**Catalog** (`technical_appendix_catalog.json`)
- Declarative list of 15 reports
- Metadata for discovery and validation

### Reports (15 total)

| Category | Count | Examples |
|----------|-------|----------|
| **Core Scoring** | 2 | AKM Decomposition, IV Weights |
| **Network Analysis** | 3 | Centrality, Bridges, Knowledge Spanners |
| **Bias Detection** | 1 | Demographic & Role Disparities |
| **Causal Inference** | 2 | DML Estimates, Identification Strategy |
| **Career Dynamics** | 2 | Attrition, Generational Cohorts |
| **Genre Analysis** | 1 | Specialists & Crossovers |
| **Studio Profiling** | 1 | Collaboration Network |
| **Confidence Methods** | 1 | CI Methodology & Validation |
| **Data Quality** | 1 | Dataset Statistics |
| **Archival** | 1 | Performance Benchmarks (deprecated) |

### Cross-References (Matrix)

Each brief links to specific reports:

- **Policy Brief** (6 reports): Bias, Causal ID, Career, Network, Studio
- **HR Brief** (5 reports): AKM, Bias, Career, DML, IV
- **Business Brief** (6 reports): AKM, DML, Genre, IV, Knowledge Spanners, Studio

Example: Policy's **market_concentration** section links to:
- Network/Bridges (structural holes)
- Studio/Network (co-production clusters)
- Causal/ID (identification assumptions)

### Usage

```bash
# Generate technical appendix (all reports + cross-references)
task appendix-generate

# Validate appendix structure + cross-references
task appendix-validate

# Python API
from scripts.report_generators.technical_appendix import create_default_appendix

appendix = create_default_appendix()
is_valid, errors = appendix.validate()
appendix_dict = appendix.to_dict()

# Get all reports supporting HR brief
hr_reports = appendix.get_by_brief("hr")  # dict[category -> list[TechnicalReport]]
```

### Output Format

Exports to `result/json/technical_appendix.json`:
```json
{
  "metadata": {
    "generated_at": "2026-04-22T13:01:50Z",
    "total_reports": 15,
    "active_reports": 14
  },
  "reports_by_category": {
    "core_scoring": [...],
    "network": [...],
    ...
  },
  "cross_references": {
    "policy": { "total": 6, "by_category": {...} },
    "hr": { "total": 5, "by_category": {...} },
    "business": { "total": 6, "by_category": {...} }
  }
}
```

### Quality Gates for Core Reports

All core_scoring reports require at least one of:
- `has_confidence_intervals: true` — Analytical SEs or bootstrap
- `has_null_model: true` — What would random data produce?

This ensures readers can assess uncertainty + significance.

### Deprecation Strategy

Old/redundant reports (e.g., 140+ performance benchmark JSONs) are:
1. Marked `deprecated: true` in catalog
2. Excluded from active counts + brief cross-references
3. Retained for historical audit trail
4. Included in archival category for documentation

Example:
```json
{
  "id": "performance_archive",
  "deprecated": true,
  "deprecation_reason": "Pipeline performance normalized; Rust acceleration removed need for continuous benchmarking."
}
```

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

## CI/CD Integration (Phase 2D)

### GitHub Actions Workflows

**1. Report Validation** (`.github/workflows/report-validation.yml`)
- Triggers: On PR to `scripts/report_generators/` or report scripts
- Checks:
  - ✅ Brief validation (4 sections, 3 method gates each)
  - ✅ Technical appendix validation (15 reports, cross-references)
  - ✅ Vocabulary lint (no ability, skill, talent, competence, capability)
  - ✅ Regeneration check (no unexpected diffs)
- Blocks merge if any check fails
- Posts comment with status (success/failure)

**2. Nightly Regeneration** (`.github/workflows/nightly-reports.yml`)
- Triggers: Daily at 02:00 UTC (10:00 JST)
- Tasks:
  - ✅ Regenerate all 3 briefs
  - ✅ Regenerate technical appendix
  - ✅ Validate both + show report diff
  - ✅ Create PR if changes detected
- Optional: Archive old performance benchmarks (>30 days)

### Pre-commit Hooks

**Vocabulary Linter** (`scripts/report_generators/lint_vocab.py`)
- Checks for prohibited terms in report generator source files
- Patterns: `\bability\b`, `\bskill\b`, `\btalent\b`, etc. (word boundaries)
- Runs automatically before commit on report generator changes

**Structure Linter** (`scripts/report_generators/lint_structure.py`)
- Validates required sections in v2 reports
- Enforces: 概要, Findings, Interpretation, Data Statement, Disclaimers

### Report Diff Tool

**`scripts/report_diff.py`**
- Compares generated reports with git HEAD
- Shows: added/removed/changed fields + metadata
- Usage:
  ```bash
  task report-diff                # Show all diffs
  task report-diff --brief policy # Show policy brief only
  ```

### Taskfile Commands

```bash
task report-briefs       # Generate all 3 briefs
task report-validate     # Validate with gates + vocabulary
task appendix-generate   # Generate technical appendix
task appendix-validate   # Validate appendix structure
task report-diff         # Show before/after changes
task nightly             # Full nightly: generate, validate, diff
```

### Validation Gates (Blocking)

| Gate | Check | Behavior |
|------|-------|----------|
| **Brief Structure** | 4 sections, 3 method gates | Block merge |
| **Appendix Structure** | 15 reports, cross-refs valid | Block merge |
| **Vocabulary** | No prohibited terms | Block merge |
| **File Accessibility** | All referenced files exist | Block merge |
| **Regeneration** | Unexpected diffs | Warning (info) |

### Deployment Strategy

1. **On Pull Request**: Run validation.yml
   - Catch issues early before merge
   - Auto-comment with pass/fail status

2. **On Schedule (Nightly)**: Run nightly-reports.yml
   - Regenerate to detect data drift
   - Create PR for review

3. **On Merge to Main**: CI continues
   - Reports auto-published to `result/json/`
   - Artifacts available for web serving

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [v3.0] - 2026-05-06

### Added

- **11 chart primitives (P1-P11)** in `src/viz/primitives/`:
  - P1 `CIScatter` — point estimates with error bars and null reference line
  - P2 `KMCurve` — Kaplan-Meier survival curves with Greenwood confidence band
  - P3 `EventStudyPanel` — pre/post dynamic effects with bootstrap band and placebo lines
  - P4 `SmallMultiples` — facet grid (cohort × role) with per-facet CI and shared null reference
  - P5 `RidgePlot` — distribution overlays with KDE quantile band and null KDE shading
  - P6 `BoxStripCI` — box + raw strip + 95% CI mark with null median line
  - P7 `SankeyFlow` — career stage transition with edge width shrinkage
  - P8 `RadialNetwork` — ego-network local view with edge weight CI and null density shading
  - P9 `HeatMap` — correlation / co-occurrence matrix
  - P10 `ParallelCoords` — multi-axis parallel coordinates
  - P11 `ChoroplethJP` — prefecture-level choropleth (GeoJSON pending)
- **Interactivity layer** (`src/viz/interactivity.py`): linked brushing across primitives in the same brief section
- **Export layer** (`src/viz/export.py`): parallel generation of interactive HTML, static SVG, and print PDF from a single `Spec` via kaleido
- **Glossary v3** (`docs/GLOSSARY_v3.md`): canonical term definitions for all 45 reports; 19 `forbidden_vocab` exceptions registered with rationale
- **`ReportSpec` dataclass**: 7-field method declaration (`claim`, `identifying_assumption`, `null_model`, `method_gate`, `sensitivity_grid`, `interpretation_guard`, `data_lineage`). All 45 reports now carry a curated SPEC.
- **`BriefArc` dataclass**: 4-stage narrative arc (phenomenon → null contrast → interpretive limit → alternative view) enforced for Policy / HR / Business briefs
- **`ci_check_report_spec.py`**: strict-mode CI gate — reports without a valid `ReportSpec` are blocked at Phase 5
- **`make_default_spec` helper**: scaffolding shortcut for new reports
- **KPI strip + chart caption standard**: auto-extracted KPI badges on all brief sections
- **`src/viz/palettes.py`**: Okabe-Ito 8-color palette unification; 460-hex table for accessibility

### Changed

- `biz_undervalued_talent` report renamed to `biz_exposure_gap` (theta_i 高 / 露出機会低ペア候補リスト) — forbidden evaluation framing removed from report name
- `mgmt_director_mentor` title narrowed to "監督下デビュー人数と5年後可視性プロファイル" — "育成力" framing removed
- `src/viz/theme.py`, `palettes.py`, `typography.py`: unified across all 45 reports; per-report `fig.update_layout` calls removed
- Phase 5 strict mode enabled: `ReportSpec` absence blocks pipeline execution

### Deprecated

- `BizUndervaluedTalentReport` class alias — BC compatibility shim active for one release, scheduled for removal in v3.1

### Fixed

- CI band rendering omission: primitives now enforce `auto_ci=True` and `auto_null=True` by default, closing the `REPORT_PHILOSOPHY.md §3.1` viz gap
- Palette colorblind incompatibility: Okabe-Ito replaces ad-hoc per-report color lists

---

## [Unreleased]

### Planned

- Remove `BizUndervaluedTalentReport` BC alias (v3.1)
- P11 `ChoroplethJP` GeoJSON data integration
- DB migration for `ReportSpec` persistence in `mart.meta_report_spec`

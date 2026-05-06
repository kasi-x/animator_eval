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

- ~~`BizUndervaluedTalentReport` class alias — BC compatibility shim~~ → 削除完了 (commit 45a6435)

### Fixed

- CI band rendering omission: primitives now enforce `auto_ci=True` and `auto_null=True` by default, closing the `REPORT_PHILOSOPHY.md §3.1` viz gap
- Palette colorblind incompatibility: Okabe-Ito replaces ad-hoc per-report color lists

---

## [v3.0.1] - 2026-05-06

### Added (TODO §16 残務全消化, commit 45a6435)

- **§16.2 ChoroplethJP 真 render**: `data/geo/japan_prefectures.geojson` (dataofjapan/land MIT, 47 features) を `scripts/maintenance/fetch_jp_geojson.py` で取得。`src/viz/primitives/choropleth_jp.py` を `go.Choropleth` (geojson + featureidkey="properties.nam_ja") に実装。GeoJSON 不在 / unknown 名 / 強制 fallback で bar fallback 維持。
- **§16.3 DB migration v63**: `mart.meta_report_spec` テーブル DDL (`src/analysis/io/mart_writer.py`) と `write_report_specs()` 関数 (SHA-256 spec_hash, idempotent upsert) を追加。`src/pipeline_phases/post_processing.py` に upsert step (`46 SPEC`, non-fatal) を統合。
- **§16.4 viz tests 拡張**: `tests/viz/test_primitives_graceful_fallback.py` (P1-P11 各 graceful fallback 21 tests) + `tests/reports/test_spec_gate.py` (strict mode toggle / `make_default_spec` / `BriefArc.to_html` 18 tests) + `tests/unit/test_viz_choropleth_jp.py` (6 tests)
- **5 reports curated KPI/caption** (Agent H): `policy_gender_bottleneck` / `policy_generational_health` / `compensation_fairness` / `mgmt_attrition_risk` / `mgmt_succession`

### Removed

- **§16.1**: `BizUndervaluedTalentReport` BC alias を削除 (`biz_exposure_gap.py` + `__init__.py`)、外部参照ゼロ確認、`V2_REPORT_CLASSES` 件数 46 維持

### Tests

- 211 / 211 v3 関連 tests pass (regression なし)
- `ci_check_report_spec-strict`: 45 / 45 modules pass

---

## [Unreleased]

### Planned

- 各 report に actual `link_brushing` / `cross_filter` 統合 (現状 hr_brief_index に scaffold のみ)
- `temporal_foresight` 予測精度 holdout 検証 (現状「foresight」名称は概念的のみ)
- インライン手動 KPI/caption の更なる拡張 (~30 reports auto-extract のまま)
- Atlas migration apply (DB v63 物理 schema 反映)
- CI workflow に GeoJSON fetch step 追加 (`data/` git ignored の補完)

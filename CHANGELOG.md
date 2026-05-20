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

## [v3.1] - 2026-05-20

レポート高度化 4 ラウンド: 35/01 nationality_backfill 完了 → 9 新規 analysis module
→ レポート品質 scorecard (mean 79.5) → 完備性 audit 全パス。

### Added

- **10 新規 analysis module** (260+ tests):
  - `src/analysis/network/resilience.py`: hub/bridge 除去 simulation、fragility_ratio
  - `src/analysis/equity/oaxaca_decomp.py`: Oaxaca-Blinder + bootstrap CI
  - `src/analysis/equity/cohort_inequality.py`: Gini/Theil/Atkinson 時系列
  - `src/analysis/career/cox_visibility.py`: Cox PH + Schoenfeld + temporal holdout
  - `src/analysis/career/mentor_effect.py`: event-study + matched DiD
  - `src/analysis/causal/heterogeneous_effects.py`: subgroup CATE + T-learner
  - `src/analysis/causal/did_robustness.py`: placebo + E-value + joint leads
  - `src/analysis/quality/credit_anomaly.py`: Poisson / KL / source-disagreement
  - `src/analysis/quality/power_analysis.py`: t/regression/correlation power + MDE
  - `src/analysis/quality/multiple_testing.py`: Bonferroni / Holm / BH

- **4 新規 v2 reports** (38 → 42):
  EquityOaxaca / NetworkResilience / CohortInequality / MentorEffect / CreditAnomalyAudit

- **品質ライブラリ + 監査スクリプト**:
  `viz_quality.py` (WCAG AA + Okabe-Ito CVD-safe palette + forest/violin/heatmap)、
  `reproducibility_footer.py` (git_sha + spec_hash + timestamp footer auto-inject)、
  `cross_reference.py` (56/56 reports linked)、
  `briefs/_keyfindings_loader.py`、
  `quality_scorecard.py` (mean 79.5/100)、
  `ci_check_spec_coverage.py` (55/55 SPEC)、
  `ci_check_method_gate.py` (55/55 method gate pass)、
  `lint_findings_separation.py`、`lineage_register.py`

- **9 新規 Mart テーブル DDL** (`_DDL` + `_MART_PK_MAP`):
  feat_did_hte / feat_mentor_* (3) / feat_credit_anomaly_flags / feat_did_robustness /
  feat_network_resilience / feat_cohort_inequality / feat_oaxaca_decomposition

- **brief 強化**:
  policy 5 → 10 / hr 6 → 8 / business 6 → 7 sections。全 brief 冒頭に
  executive_summary auto-inject。

- **7 method_notes**:
  network_resilience / cohort_inequality / cox_visibility / heterogeneous_effects /
  mentor_effect / credit_anomaly / power_analysis

### Changed

- `gold_connect()`: resolved.duckdb 自動 ATTACH + TEMP VIEW で `FROM credits` 等
  bare-name SQL を透過化
- `_base.write_report()`: cross-reference + reproducibility footer auto-inject
- `forbidden_vocab.yaml`: `subjective_evaluation` category 追加 (11 語)
- `ReportSpec`: `alternative_interpretations` field 追加、
  identifying_assumption ≥ 30 char rule

### Fixed

- 35/01 nationality 流入路: resolved.persons 非空率 **3.48% → 12.26%** (76K 件)
- 14 Interpretation 一人称マーカー欠落 → 0
- credit_anomaly_audit クエリ 106s 完走可能化
- 4 vocab violations → 0

### Verified

- **263 新規 tests pass**
- quality scorecard mean **79.5 / 100**
- labor-first vocab: **0 violations / 56 files**
- Findings/Interpretation lint: **0 warnings**
- SPEC coverage: **55/55**
- Method gate audit: **55/55 pass**
- Cross-reference: **56/56 reports linked**

### Commits (Session 2)

`df2debb` → `3894bc7` → `ce7d0a6` → `04f4964` → `7fb53d4` → `8b75fe0` →
`04cb2a4` → `1f031c6` → `a412662` → `6af6c4b` → `e437eda`

---

## [Unreleased]

### Planned

- 各 report に actual `link_brushing` / `cross_filter` 統合 (現状 hr_brief_index に scaffold のみ)
- `temporal_foresight` 予測精度 holdout 検証 (現状「foresight」名称は概念的のみ)
- インライン手動 KPI/caption の更なる拡張 (~30 reports auto-extract のまま)
- Atlas migration apply (DB v63 物理 schema 反映)
- CI workflow に GeoJSON fetch step 追加 (`data/` git ignored の補完)
- 9 feat テーブルへの post_processing driver 実装 (現状 DDL のみ、計算経路未実装)
- AniList orphan backfill 実走 (#36 カード)、§15 gender enrichment 70% 達成

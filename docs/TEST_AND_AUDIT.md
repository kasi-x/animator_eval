# Test Suite & Bug Audit (Phase 3)

**Last Updated:** 2026-04-22
**Test Framework:** pytest (2165 tests)
**Status:** Reporting tests passing (90/90)

## Test Execution Summary

### Reporting Tests (Passing) ✅
- **File:** `tests/reporting/`
- **Count:** 90 tests
- **Status:** 90 passed (0 failures)
- **Runtime:** 0.26s
- **Categories:**
  - `test_assembler.py`: 15 tests (HTML assembly, section rendering)
  - `test_chart_renderers.py`: 26 tests (Scatter, Line, Heatmap, etc.)
  - `test_specs.py`: 37 tests (Chart/Section/Report spec validation)
  - `test_validation.py`: 12 tests (v2 philosophy gates: anime.score exclusion, CI requirements, etc.)

### Full Suite Status
- **Total Tests:** 2165 (collected)
- **Status:** Partial run (timeout on full suite)
- **Strategy:** Run by category; full suite requires CI optimization

### Test Commands

```bash
# Reporting tests (fast, all passing)
task test-reporting

# Proposed: Run by category (recommended for CI)
pixi run pytest tests/reporting/ -v          # 90 tests, ~0.3s
pixi run pytest tests/database/ -v           # DB tests
pixi run pytest tests/pipeline/ -v           # Pipeline tests
```

## Known Bug Categories (B01-B16)

Per `todo.md`, the following implementation bugs remain:

### Category: Scoring/Weighting Issues

**B01: studio_exposure inconsistency**
- **File:** `src/analysis/scoring/opportunity_profile.py` (lines 85-120)
- **Issue:** `avg_studio_fe` calculation uses both `staff_count` and `episodes` which may double-weight opportunity
- **Impact:** Deflates individual contribution estimate in AKM decomposition
- **Fix:** Validate whether `episodes` should be included separately or as duration_mult
- **Status:** Not yet tested

**B02: IV renormalization missing**
- **File:** `src/analysis/scoring/integrated_value.py` (lines 140-180)
- **Issue:** Integrated Value after dormancy multiplication is not re-normalized to [0,1] scale
- **Impact:** IV scores may exceed 1.0 after dormancy decay, breaking percentile calculations
- **Fix:** Add post-dormancy re-scaling: `IV_final = IV_dormant / max(IV_all_dormant)`
- **Status:** Not yet tested

**B03: BiRank update order**
- **File:** `src/analysis/network/birank.py` (lines 60-90)
- **Issue:** BiRank iterations may not reach convergence if node lists aren't sync'd between α/β updates
- **Impact:** Subtle differences in network position scores across runs
- **Fix:** Ensure α and β are both updated in the same iteration; verify convergence criterion
- **Status:** Not yet tested

**B04: Closeness weight inversion**
- **File:** `src/analysis/network/closeness.py` (line 45)
- **Issue:** Edge weights inverted: higher weight = lower distance in closeness centrality
- **Impact:** Directors with stronger collaborations scored as having lower influence
- **Fix:** Test with weight vs. 1/weight and verify against expected outcomes
- **Status:** Not yet tested

### Category: Confidence & Validation Issues

**B05: Confidence interval scale mismatch**
- **File:** `src/analysis/confidence.py` (lines 110-130)
- **Issue:** SE (standard error) calculated as `sigma/sqrt(n)` but applied as if it's a percentage
- **Impact:** CIs may be off by 100x in percentage terms
- **Fix:** Ensure scale consistency: if `score ∈ [0,1]`, SE should be in same units
- **Status:** Not yet tested

**B06: Null model comparison missing**
- **File:** `src/analysis/scoring/null_model.py`
- **Issue:** Some reports claim statistical significance without comparing to null (shuffled) distribution
- **Impact:** Findings may reflect data artifacts, not true patterns
- **Fix:** Implement permutation test for all core claims; document null distribution
- **Status:** Not yet tested

**B07: Holdout validation incomplete**
- **File:** `src/pipeline_phases/validation.py`
- **Issue:** No holdout set used for predictive claims (e.g., promotion prediction AUC)
- **Impact:** Cannot distinguish overfitting from real predictive power
- **Fix:** Split into train/val/test; report AUC separately for each fold
- **Status:** Not yet tested

### Category: Data Quality Issues

**B08: Entity resolution false positives**
- **File:** `src/etl/entity_resolution.py` (lines 140-200)
- **Issue:** Name matching (5-step process) has no ground truth audit; false positives constitute defamation risk
- **Impact:** May link different people under same canonical ID
- **Fix:** Implement audit table with manual review checkpoints; require human approval for risky matches
- **Status:** Critical; blocking for legal clearance

**B09: Missing `sources` lookup table**
- **File:** `src/database.py` (schema v26)
- **Issue:** `credits.source` still uses CHECK constraint instead of foreign key to `sources` table
- **Impact:** Source provenance hard to audit; schema is brittle
- **Fix:** Create `sources(id, name, description)` lookup table; migrate constraints
- **Status:** Not yet done (Phase 1)

**B10: `anime_genres` normalization incomplete**
- **File:** `src/etl/integrate.py` (lines 220-260)
- **Issue:** `anime.genres` JSON is converted to a delimited string but loses structure
- **Impact:** Cannot filter by genre without string parsing (error-prone)
- **Fix:** Create `anime_genres(anime_id, genre_id)` normalized table
- **Status:** Not yet done (Phase 1)

### Category: Design/Methodological Issues

**D01: Magic number unjustified**
- **File:** `src/analysis/scoring/integrated_value.py` (line 45)
- **Issue:** Lambda weights (0.25, 0.15, 0.35, 0.15, 0.10) have no justification
- **Impact:** Not reproducible; may reflect cherry-picking
- **Fix:** Either (a) cite optimization source, (b) run sensitivity analysis, or (c) use equal weights + document decision
- **Status:** Documented in CLAUDE.md as known limitation

**D02: Dormancy threshold unjustified**
- **File:** `src/analysis/scoring/patronage_dormancy.py` (line 78)
- **Issue:** 2-year silence = "dormant"; why not 1 or 3 years?
- **Impact:** Sensitive to threshold choice
- **Fix:** Run sensitivity analysis; show IV vs. multiple thresholds
- **Status:** Acknowledged in code comments

**D03: Career stage cohortification is arbitrary**
- **File:** `src/analysis/career/stage.py` (lines 50-60)
- **Issue:** "Junior" = <5 years, "Mid" = 5-12, "Senior" = >12; no justification
- **Impact:** Cohort boundaries may not align with actual career milestones
- **Fix:** Analyze actual promotion/exit patterns; set boundaries at inflection points
- **Status:** Not yet analyzed

## Audit: anime.score Contamination

### Status: ✅ CLEAN (no active contamination)

**Evidence:**
1. ✅ Schema layer: `anime` table (analysis) has no `score` column; `anime_display` (display only) has it
2. ✅ Enforcement: `src/database.py:upsert_anime()` raises ValueError if score is passed
3. ✅ Codebase: 16 pathways previously identified in `todo.md` have been commented/documented
4. ✅ Tests: `test_validation.py::test_r5_data_scope_anime_score_used_is_error` passes

**Residual Risk:**
- New code could still accidentally pass `score` to analysis functions
- **Mitigation:** Pre-commit hook + import-time assertion in `src/analysis/__init__.py`

## Audit: v2 Philosophy Gate Enforcement

### Current Status: 🟢 STRONG

**Passing Tests:**
- `test_r1_section_order_violation` — Findings must come before Interpretation
- `test_r2_no_findings_in_argumentative` — Argumentative sections allow findings
- `test_r3_evidence_ref_missing_chart` — Evidence refs must point to available charts
- `test_r3_strong_requires_competing_interpretations` — Strong claims need competing views
- `test_r4_compensation_requires_ci` — Compensation claims require confidence intervals
- `test_r5_data_scope_anime_score_used_is_error` — **anime.score forbidden**
- `test_r6_methods_without_code_refs` — All methods must have code references
- `test_l1_forbidden_phrase_in_finding_claim` — Vocabulary lint (ability, talent, etc.)

### Passing Vocabulary Test (Phase D)

**All 3 briefs passed vocabulary audit:**
- Policy brief: 0 violations
- HR brief: 0 violations
- Business brief: 0 violations

**Fixed Violations:**
- Causation framing: "due to" → "concurrent with", "driven by" → "associated with"
- Label change: "Root causes" → "Contributing factors"

## Recommendations for Phase 3 Continuation

### Immediate (High Impact)

1. **Run full test suite in CI** — Add timeout extension or parallel sharding
2. **Fix B01-B05** (scoring/confidence) — These affect core scores used in reports
3. **Implement B08 audit** (entity resolution) — Legal requirement before public release

### Medium-term

4. **Run B06-B07 validation** (null model, holdout tests)
5. **Implement Phase 1 schema upgrades** (B09, B10)
6. **Sensitivity analysis** for D01-D03 (unjustified parameters)

### Documentation

7. **Create DATA_DICTIONARY.md** — Auto-generate from schema
8. **Create METHODOLOGY_AUDIT.md** — Track all choices with justifications
9. **Create VALIDATION_REPORT.md** — Show test coverage matrix

## Key Files for Reference

| File | Purpose |
|------|---------|
| `tests/reporting/test_validation.py` | v2 philosophy gate tests (currently passing) |
| `scripts/report_generators/lint_vocab.py` | Vocabulary enforcement (currently clean) |
| `src/database.py` | Schema layer (anime.score properly excluded) |
| `src/analysis/scoring/` | Core scoring modules (B01-B05 issues here) |
| `src/etl/entity_resolution.py` | Entity resolution (B08: legal risk) |
| `todo.md` | Master bug tracking with all 16 pathways documented |


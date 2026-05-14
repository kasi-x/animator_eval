# Method Note: Opportunity Residual

**Status**: implemented (2026-05-13), upgraded to true panel (2026-05-15)
**Module**: `src/analysis/scoring/opportunity.py`
**Hard constraints**: H1 (no anime.score), H4 (analytical CI required)

---

## Purpose

`opportunity_residual[i]` quantifies whether person i receives structurally
more or fewer credits than their network position (AKM theta_i), career
tenure, and role diversity predict.

Positive residual: structural surplus — more credited work than comparable
peers at the same structural position.
Negative residual: structural deficit — fewer credits than the structural
controls predict (「機会過少」).

This metric is labor-first: it detects opportunity gaps that are structural,
not individual-capability-based.

---

## Specification

### Regression model (canonical: per-(person, year) panel)

```
log(credit_count[i, year]) = β0 + β1·theta_i + β2·tenure_i
                           + β3·role_diversity_i
                           + α_studio[i] + γ_year + ε[i, year]
```

| Term | Description | H1 compliance |
|------|-------------|---------------|
| `log(credit_count[i, year])` | log(1 + credits for person i in year y) | structural count |
| `theta_i` | AKM person fixed effect (production scale) | H1-safe: no anime.score |
| `tenure_i` | year − first_credit_year (per row) | structural |
| `role_diversity_i` | Shannon entropy of person's role distribution, normalised to [0,1] | structural |
| `α_studio[i]` | modal-studio fixed effect (person's most-frequent studio across career) | structural |
| `γ_year` | year fixed effect (year dummies, reference = most-frequent year) | structural |

`anime.score` is **never** used as outcome, predictor, or control.

The canonical entry point is
`compute_opportunity_residual_from_credits(credits, anime_map, theta_map, ...)`.
`compute_individual_profiles(...)` uses the panel path automatically when
credits + anime_map carry year information; otherwise it falls back to the
cross-sectional features-only model below.

### Cross-sectional fallback

When only the pre-built features dict is available (no panel year info), the
module fits:

```
log(credit_count[i]) = β0 + β1·theta_i + β2·tenure_i
                     + β3·unique_studios_i + role_dummies + ε[i]
```

This is a degraded mode — n_years=1, no CI possible. Provided for backward
compatibility with call sites that pre-aggregate before calling.

### Residual and point estimate

```
opportunity_residual[i] = mean over years ( ε[i, year] )
```

For the canonical panel path multiple ε[i, year] are averaged per person;
for the cross-sectional fallback this collapses to the single residual ε[i].

### Analytical CI (H4)

```
SE[i]     = std(ε[i, 1..n]) / √n_years[i]     ddof=1
CI95[i]   = mean ± t_{n-1, 0.975} × SE[i]
```

Uses **t-distribution** (not z=1.96) for correct finite-sample coverage.
The t-quantile converges to 1.96 as n → ∞.

Requires n_years ≥ 2 for CI.  Single-observation persons receive
`se=None, ci_lower=None, ci_upper=None`.

CI coverage calibration: empirical 95% CI captures the true mean in
approximately 95% of replications under simulation (±3pp tolerance,
verified in `tests/analysis/scoring/test_opportunity.py::TestAnalyticalCI::
test_ci_coverage_calibration`).

### Permutation null model

H0: opportunity is independent of person identity.

```python
for _ in range(1000):
    y_perm = permute(y)          # shuffle outcome labels
    ε_null = OLS_residuals(y_perm, X)
    record |ε_null[i]|

p_value[i] = #{|ε_null[i]| >= |ε_obs[i]|} / 1000    # two-sided
```

Calling `compute_individual_profiles(..., opportunity_n_permutations=1000)`
activates the permutation null.  Default is 0 (skip) for performance in
tests and incremental runs.

---

## Output fields

All fields are stored in `IndividualProfile` and propagated into
`profiles[pid]` dict:

| Field | Type | Description |
|-------|------|-------------|
| `opportunity_residual` | `float \| None` | mean OLS residual |
| `opportunity_residual_se` | `float \| None` | analytical SE = σ/√n |
| `opportunity_residual_ci_lower` | `float \| None` | CI95 lower bound |
| `opportunity_residual_ci_upper` | `float \| None` | CI95 upper bound |
| `opportunity_residual_p_value` | `float \| None` | permutation empirical p (two-sided) |

---

## Assumptions and limitations

1. **Studio FE = modal studio**: The studio fixed effect uses each person's
   most-frequent studio across their entire career, not their per-year studio.
   Rationale: per-year-studio would explode the design matrix and overfit thin
   years (most persons have ≤ 2 credits per year). The modal studio captures
   the dominant labor-market location for the person. Persons who genuinely
   move between studios receive a single-studio FE — interpret with care.

2. **Role diversity = normalised Shannon entropy**: Computed on the role
   distribution of the person's entire career, not per-year. Bounded to [0, 1]
   by dividing by log(K) where K = number of role categories observed.
   0 = single-role specialist; 1 = uniform across observed roles. Per-year
   diversity was considered but the year-to-year role distribution is too
   sparse for most animators.

3. **OLS linearity on log-counts**: The log-credit-count ~ controls model
   assumes linear effects of theta, tenure, diversity. If systematic
   non-linearity appears (Q-Q deviation > 0.5, see `residual_qq_deviation`),
   consider a Poisson/Negative-Binomial GLM. Stop-if condition documented
   below.

4. **Permutation null is person-identity test**: The permutation shuffles
   the outcome vector y across all panel rows, then refits OLS and recomputes
   per-person mean residuals. This tests whether person identity carries
   information about the residual beyond what controls predict; it does
   not test the joint significance of all predictors. Two-sided empirical
   p-value (`p_value_permutation`).

5. **Analytical SE within-person**: SE[i] = σ(ε[i, :]) / √n_years[i] uses
   the within-person sample standard deviation (ddof=1). This is *not* the
   classical OLS SE on β — it is the SE of the per-person residual mean
   treated as a sample average. The two coincide asymptotically when person
   effects are i.i.d.; CI coverage calibration (95% ±2pp) is verified by
   `--calibration-check` and the test
   `test_ci_coverage_calibration`.

---

## Alternative specifications considered

| Alternative | Decision | Rationale |
|-------------|----------|-----------|
| Per-year studio FE | rejected | Explodes dimensionality, overfits thin years |
| Person FE (TWFE within-transform) | rejected | Would absorb theta_i, defeats the purpose |
| Cluster-robust SE (sandwich) at β level | not adopted as canonical | Our quantity of interest is the per-person residual mean; classical within-person SE is more directly interpretable. Cluster-robust SE on β can be added separately if β estimates become the report quantity. |
| `iv_score` as outcome (legacy heuristic) | rejected | iv_score is a composite of network metrics; residual would be within-composite, not structural opportunity |
| `anime.score` as control | forbidden | H1 violation |
| Bayesian hierarchical CI | rejected | Analytical t-CI is sufficient for compensation-basis claims |
| 10 000 permutations | rejected | 1 000 already gives p-resolution 0.001; ×10 cost not justified |

---

## Stop-if conditions

- Residuals systematically skewed: `residual_qq_deviation` > 0.5 on real
  data → switch to Poisson / Negative-Binomial GLM on credit counts.
- Permutation null 1 000 draws takes > 60 s on the full dataset → reduce to
  100 with variance reduction (antithetic sampling) or subsample to 1 000
  persons.
- CI coverage calibration deviates from 0.95 by more than ±0.02 in the
  `--calibration-check` CLI → review SE formula; may indicate within-person
  serial correlation that violates the i.i.d. residual assumption.

---

## Verification CLI

```bash
pixi run python -m src.analysis.scoring.opportunity --calibration-check
# [PASS] CI coverage calibration: empirical=0.9500 target=0.950 tolerance=±0.020
```

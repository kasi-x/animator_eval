# Method Note: Opportunity Residual

**Status**: implemented (2026-05-13)
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

### Regression model

```
log(credit_count[i]) = β0 + β1·theta_i + β2·tenure_i
                     + β3·role_diversity_i + role_dummies + ε[i]
```

| Term | Description | H1 compliance |
|------|-------------|---------------|
| `log(credit_count[i])` | log(1 + total credits) for person i | structural count |
| `theta_i` | AKM person fixed effect (production scale) | H1-safe: no anime.score |
| `tenure_i` | career years active | structural |
| `role_diversity_i` | unique studios count (structural breadth proxy) | structural |
| role dummies | reference-category-dropped one-hot encoding | structural |

`anime.score` is **never** used as outcome, predictor, or control.

### Residual and point estimate

```
opportunity_residual[i] = mean over rows ( ε[i] )
```

In the current cross-sectional fallback (one row per person), this equals
the single residual ε[i].  When per-year panel data are available upstream,
multiple ε[i, year] are averaged.

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

1. **Cross-sectional fallback**: The current features dict provides one
   aggregate row per person (not per year).  The n_years reported is
   therefore 1 for all persons, so CI is None.  A future upgrade should
   inject per-year (person, year) rows when available from the Resolved
   layer.

2. **Controls are proxies**: `unique_studios` is used as a role-diversity
   proxy.  It captures structural breadth (more studios = more diverse
   collaboration experience) but does not directly measure role type
   diversity across works.

3. **OLS linearity**: The log-credit-count ~ controls relationship is
   assumed linear.  If systematic non-linearity is observed (large
   Q-Q deviation), consider a log-link GLM or spline expansion.

4. **Permutation null is person-identity test**: The permutation shuffles
   outcome labels, testing whether person identity predicts the residual.
   It does not test the joint significance of all predictors.

---

## Alternative specifications considered

| Alternative | Reason not adopted |
|-------------|-------------------|
| `iv_score` as outcome (previous heuristic) | iv_score is a composite of the same network metrics; residual would be within-composite variation, not structural opportunity |
| anime.score as control | H1 violation |
| Bayesian hierarchical CI | Overkill; analytical t-CI sufficient for compensation claim basis |
| 10,000 permutations | Computational cost; 1000 achieves p-value resolution of 0.001 |

---

## Stop-if conditions

- Residuals systematically skewed (Q-Q plot, Shapiro-Wilk p < 0.001) on
  real data → consider log-link GLM for count outcome
- Permutation null 1000 draws takes > 60s on full dataset → reduce to 100
  with variance-reduction (antithetic sampling) or subsample to 1000 persons

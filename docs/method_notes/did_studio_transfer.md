# Method Note: Studio Transfer DiD Analysis

**Status**: implemented (2026-05-13)
**Module**: `src/analysis/causal/did_studio_transfer.py`
**Report**: `scripts/report_generators/reports/causal_studio_transfer.py`
**Hard constraints**: H1 (no anime.score), H2 (no ability framing), H4 (analytical CI)

---

## Purpose

Estimates the causal effect of an inter-studio move (studio transfer treatment)
on three structural position outcomes:

| Outcome | Description |
|---------|-------------|
| `theta_i` | AKM person fixed effect (production scale structural signal) |
| `opportunity_residual` | OLS residual from credit-count model (structural surplus/deficit) |
| `log_credit_count` | log(1 + annual credited works) |

All outcomes are H1-safe: `anime.score` is never used as outcome, predictor,
or control at any stage.

Results are framed as "structural position changes" per H2 — never as
"growth", "improvement", or ability-related language.

---

## Treatment Definition

```
transfer[i, t] = 1 if:
    primary_studio[i, t-1] != primary_studio[i, t]
    AND credits_at_new_studio[i, t] >= 3   (exclude brief visits)
    AND credits_at_old_studio[i, t-1] >= 3
```

**primary_studio** = studio with the most credits in a rolling 3-year window
ending at year t. Tie-break: most recent year with maximum credits.

Only the **first qualifying transfer** per person is used as the event.

---

## Specification

### Two-way FE DiD

```
y[i, t] = alpha_i + gamma_t + beta * post[i, t] * treated[i]
        + delta_1 * tenure[i, t] + delta_2 * role_diversity[i, t]
        + epsilon[i, t]
```

| Term | Description |
|------|-------------|
| `alpha_i` | person fixed effect (absorbed via within-transformation) |
| `gamma_t` | year fixed effect (absorbed via within-transformation) |
| `beta` | ATT: average treatment effect on the treated |
| `post[i, t]` | 1 for t >= event_year[i], else 0 |
| `treated[i]` | 1 if person i makes a qualifying transfer |
| `tenure[i, t]` | years since first credited work |
| `role_diversity[i, t]` | number of distinct role groups in year t |

### Event-study

```
y[i, t] = alpha_i + gamma_t
        + Σ_{k ≠ -1, k ∈ [-5, +5]} beta_k * 1[t - event_year[i] = k]
        + epsilon[i, t]
```

- `k = -1` is the omitted baseline period (immediately before transfer).
- For control persons, all event-study indicators equal 0.
- Pre-period leads: k ∈ {-5, -4, -3, -2} test parallel trends.

### Estimation

1. **Within-transformation**: iterative alternating projection (Gaure 2013)
   removes person and year fixed effects without forming large dummy matrices.
   Convergence tolerance: 1e-8, max iterations: 100.

2. **OLS on demeaned data**: `np.linalg.lstsq` on the within-demeaned y and X.

3. **Cluster-robust SE**: person-level sandwich estimator (HC1 finite-sample
   correction: g/(g-1) × (n-1)/(n-p), where g = number of persons).

4. **CI**: `beta ± t_{n_persons - 1, 0.975} × SE` (t-distribution, conservative df).

---

## Parallel Trends Test

Joint Wald F-test on pre-period leads k ∈ {-3, -2}:

```
H0: beta_{-3} = beta_{-2} = 0
F  = (R beta)' (R V R')^{-1} (R beta) / q,   q = 2
```

where `R` is a 2×p selection matrix, `V` is the cluster-robust covariance matrix.

- **Fail to reject H0** (p ≥ 0.05): parallel trends assumption supported.
- **Reject H0** (p < 0.05): potential pre-trend violation. DiD estimates
  require additional caution; consider synthetic control or staggered DiD.

The test uses only k ∈ {-3, -2} (not k = -4, -5) to focus on the most
informative pre-period and avoid power dilution from early leads that may
have few observations.

---

## Control Group Selection

Cohort × role-group exact matching:
- **Cohort bin**: (cohort_year // 5) × 5 (5-year cohort windows)
- **Role group**: primary role group at event year

Control persons must share at least one (cohort_bin, role_group) combination
with any treated person, and must never make a qualifying transfer in the
observation window.

---

## Sample Requirements

| Parameter | Value |
|-----------|-------|
| Minimum qualifying transfers (treated persons) | ≥ 1 (pipeline reports < 500 as power warning) |
| Minimum panel observations for estimation | 20 |
| Event-study window | ±5 years |

---

## Output Tables (Resolved/Mart layer)

| Table | Contents |
|-------|----------|
| `feat_did_studio_transfer` | DiD ATT estimates (beta, SE, CI, p-value) per outcome |
| `feat_did_event_study` | Event-study coefficients (beta_k, SE, CI, p) per outcome × k |
| `feat_did_parallel_trends` | Parallel trends test results (F, p, trends_parallel) per outcome |

---

## Assumptions and Limitations

1. **Parallel trends**: The identifying assumption is that treated and control persons
   would have followed parallel trends in the absence of the transfer. The joint
   F-test on pre-period leads provides a partial check but does not identify
   violations due to time-varying confounders after k = -3.

2. **SUTVA**: No interference between treated persons (treatment of one person
   does not affect outcomes of others). This may be violated if transfers cause
   network reorganization that also affects colleagues.

3. **Self-selection into transfer**: Persons who transfer may differ from those
   who do not in unobservable ways (motivation, external offers). The DiD
   estimator controls for time-invariant differences via person FE but not
   for time-varying selection.

4. **Primary studio definition**: The rolling 3-year window captures sustained
   association. A person credited across multiple studios in one year may have
   an ambiguous primary studio.

5. **Staggered treatment timing**: Different persons transfer in different years.
   The standard two-way FE DiD may not recover the ATT under heterogeneous
   treatment effects across cohorts (Callaway-Sant'Anna 2021 concern). A
   staggered-adoption robust estimator is a recommended extension.

---

## Alternative Specifications Considered

| Alternative | Reason not adopted |
|-------------|-------------------|
| Propensity score matching | Requires parametric propensity model; cohort exact-match preferred for transparency |
| Synthetic control | Appropriate if parallel trends violated; flagged as fallback in stop-if condition |
| Staggered adoption (Callaway-Sant'Anna) | Recommended extension (separate card) |
| anime.score as outcome | H1 violation — prohibited unconditionally |
| "Improvement" framing for positive beta | H2 violation — results are "structural position changes" |

---

## Stop-if Conditions

| Condition | Action |
|-----------|--------|
| treated < 200 | Report power warning; consider alternative treatment (COVID shock, etc.) |
| parallel trends F-test p < 0.05 | Flag violation; DiD estimates presented with strong caveat |
| No qualifying transfers in data | Return None; pipeline reports data gap |

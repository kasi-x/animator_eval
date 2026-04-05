# Animetor Eval — Calculation Compendium

> **Purpose:** Reference document for every scoring formula, classification rule, and statistical method used in the pipeline and report generation. Covers both the core evaluation pipeline (`src/`) and the report generator (`scripts/generate_all_reports.py`).
>
> **Design principle:** All formulas use only structural/objective data (credit records, roles, production metadata, co-credit relationships). Viewer ratings (anime.score) are never used in any scoring formula. See the "Prohibited Inputs" column in each section.

---

## Table of Contents

1. [Data Quality Constants](#1-data-quality-constants)
2. [Core Scores (Pipeline)](#2-core-scores-pipeline)
   - 2.1 Person Fixed Effect (theta) — AKM
   - 2.2 BiRank — Bipartite PageRank
   - 2.3 Patronage Premium (Pi)
   - 2.4 Dormancy Penalty (D)
   - 2.5 Studio Exposure
   - 2.6 AWCC
   - 2.7 Integrated Value (IV / iv_score)
3. [Graph Edge Weights](#3-graph-edge-weights)
4. [Layer 2: Individual Contribution Profile](#4-layer-2-individual-contribution-profile)
   - 4.1 Peer Percentile
   - 4.2 Opportunity Residual
   - 4.3 Consistency
   - 4.4 Independent Value
5. [Confidence Intervals](#5-confidence-intervals)
6. [Career Classification](#6-career-classification)
   - 6.1 Career Stage Groups
   - 6.2 Role Type Groups
   - 6.3 Loss Type Classification
7. [Personnel Flow Metrics](#7-personnel-flow-metrics)
   - 7.1 Stock
   - 7.2 Entry / Exit Counting
   - 7.3 Career Stage Transitions
8. [Expected Ability Score](#8-expected-ability-score)
9. [Blank Returnee Detection](#9-blank-returnee-detection)
10. [Seasonal Debut Counting](#10-seasonal-debut-counting)
11. [Value Flow Analysis](#11-value-flow-analysis)
12. [Cluster Analyses](#12-cluster-analyses)
13. [Decade Comparison](#13-decade-comparison)
14. [SHAP Feature Importance](#14-shap-feature-importance)

---

## 1. Data Quality Constants

| Constant | Value | Meaning |
|---|---|---|
| `RELIABLE_MAX_YEAR` | 2025 | Last year with stable data (through Fall 2025 cour) |
| `STAT_MAX_YEAR` | 2024 | Upper bound for statistical/trend analysis |
| `EXIT_CUTOFF_YEAR` | 2022 | Last year for exit/attrition counting |
| `FLOW_START_YEAR` | 1990 | First year for flow charts |
| `HIGH_IV_THRESHOLD` | 30.0 | Legacy iv_score threshold for "high value" tier |

**Rationale for EXIT_CUTOFF_YEAR:**
For a person whose latest credit year is 2023-2025, data collection may still be ongoing. Using `latest_year` as "exit year" would incorrectly classify active persons as having left the industry. The cutoff excludes these false positives.

---

## 2. Core Scores (Pipeline)

### 2.1 Person Fixed Effect (theta) — AKM

Source: `src/pipeline_phases/core_scoring.py`, `src/analysis/akm.py`

**Model:**
```
log(production_scale_ij) = theta_i + psi_j + X_ij beta + epsilon_ij
```

**Outcome variable (production_scale):**
```
production_scale_ij = staff_count_j x episodes_j x duration_mult_j
```

- `staff_count_j` = number of unique credited staff on anime j (from credit records)
- `episodes_j` = number of episodes
- `duration_mult_j` = format-based multiplier (Movie: 1.5, TV: 1.0, OVA: 0.8, etc.)
- **Interpretation:** "Being called to large-scale productions" is a structural indicator of industry valuation

**Parameters:**
- `theta_i` = person fixed effect — individual contribution to production scale, net of studio
- `psi_j` = studio fixed effect — baseline production scale of studio j
- `X_ij` = control variables: experience (career years), role_weight, genre FE, format FE, year FE

**Estimation:** Iterative alternating least squares (Mundlak within-estimator)

**Post-estimation steps (in order):**
1. **Shrinkage** (James-Stein): `theta_shrunk = (1 - kappa/n_obs) x theta_raw`
   - kappa = `clip(sigma2_resid / sigma2_signal, 2.0, 50.0)` — bounds need justification (see todo.md D05)
2. **Debias by observation count**: Corrects for correlation between theta and n_obs
   - Should only trigger when slope is statistically significant (see todo.md D09)
3. **Studio FE redistribution**: Transfers a fraction alpha of studio FE to movers
   - `alpha = max(0.0, slope_mover - slope_stayer)` — lacks formal identification (see todo.md D06)

**Known issues:**
- Processing order (shrinkage -> debias -> redistribution) is non-commutative; redistribution -> shrinkage -> debias would be more theoretically consistent (todo.md D08)
- `_build_panel()` uses `anime.studios[0]` but `studio_assignments` uses weighted voting — studio label mismatch (todo.md B03)
- Low mover fraction (<10%) silently zeros out studio_fe (todo.md D15)

**Prohibited inputs:** anime.score must NOT be used as the outcome variable y. The previous implementation used `log(anime_score_ij)` which measured "contribution to viewer ratings" — contradicting the project's design principles.

Result stored as `person_fe` in `scores.json`.

### 2.2 BiRank — Bipartite PageRank

Source: `src/analysis/birank.py`

Bipartite graph G = (Persons U Anime, Credits). Edge weight W[i,j] = credit weight (role importance x episode coverage x duration_mult).

**Transition matrices:**
```
S = D_p^{-1} . W       (person -> anime, row-normalized)
T = W . D_a^{-1}       (anime -> person, col-normalized)
```

**Iterative update (Gauss-Jacobi, simultaneous):**
```
p_new = alpha . T . u + (1-alpha) . p_0
u_new = beta  . S^T . p + (1-beta) . u_0
```

- alpha = 0.85 (person damping), beta = 0.85 (anime damping)
- p_0, u_0 = uniform initial vectors
- Convergence: L1 norm change < 1e-6, max 100 iterations
- **IMPORTANT:** u_new must use the OLD p (not p_new) — Gauss-Jacobi requires simultaneous update

**Known issue:** Current implementation uses p_new in u_new calculation (Gauss-Seidel), converging to a different fixed point than the theoretical BiRank (He et al. 2017). See todo.md B04.

**Prohibited inputs:** Edge weights must not include anime.score as a multiplier.

Result stored as `birank` in `scores.json`.

### 2.3 Patronage Premium (Pi)

Source: `src/analysis/patronage_dormancy.py`

Measures how much a person benefited from repeat collaboration with high-prestige directors.

**Formula:**
```
Pi_i = sum_d (PR_d x log(1 + N_id))
```

- `PR_d` = person_fe of director d (or BiRank — but must avoid circular dependency, see todo.md D18)
- `N_id` = number of co-credit collaborations between person i and director d
- `log(1 + N)` = diminishing returns on repeat collaboration

**Prohibited inputs:** The Quality term (`Quality_id = average anime score of shared works`) has been removed. Frequency and director prestige alone are sufficient — the "quality" of a collaboration should not be measured by viewer ratings.

**Known issue:** Directors have Patronage = 0 by construction (their credits are skipped). This is a structural asymmetry (todo.md D17).

Result stored as `patronage` in `scores.json`.

### 2.4 Dormancy Penalty (D)

Source: `src/analysis/patronage_dormancy.py`

Penalizes persons with long inactivity gaps relative to current year.

```
D(i, t) = exp(-delta x max(0, gap_i - tau_grace))
```

- `gap_i` = years since last credited work
- `tau_grace` = 2 years (no penalty within grace period)
- `delta` = 0.5 (half-life of ~1.4 years past grace period)
- D = 1.0 if gap <= tau_grace; D -> 0 as gap -> infinity

**Rationale for delta=0.5:** Anime production is seasonal with typical 1-2 year gaps between projects. A half-life of ~1.4 years (ln(2)/0.5) past the 2-year grace period means a 5-year total gap (3 years past grace) gives D≈0.22, reflecting that such extended absence strongly signals career exit or reduced engagement.

Result stored as `dormancy` in `scores.json`.

### 2.5 Studio Exposure

Source: `src/analysis/integrated_value.py`

Weighted sum of studio fixed effects for all studios a person has worked at.

```
studio_exposure_i = sum_j (psi_j x years_ij / total_years_i)
```

- `psi_j` = studio fixed effect from AKM
- `years_ij` = years person i worked at studio j
- Time-weighted: longer tenures at high-FE studios score higher

**Known issue:** studio_exposure is computed 3 times in the pipeline with inconsistent formulas. Phase 6 uses a simplified version (unweighted unique-studio average) that differs from the canonical function. The final IV score uses the Phase 6 version. See todo.md B01.

**Must use:** `compute_studio_exposure()` from `integrated_value.py` everywhere. Phase 6 must be fixed to call the canonical function.

### 2.6 AWCC (Anime-Weighted Collaboration Centrality)

Source: `src/pipeline_phases/supplementary_metrics.py`

Measures how centrally positioned a person is in production networks, weighted by production scale.

**Prohibited inputs:** Must NOT use anime.score as the weighting factor. Weight by production scale (staff_count x episodes) instead.

### 2.7 Integrated Value (IV / iv_score)

Source: `src/analysis/integrated_value.py`

**Formula:**
```
IV_i = (lambda_1 x z(theta_i) + lambda_2 x z(birank_i) + lambda_3 x z(studio_exp_i) + lambda_4 x z(awcc_i) + lambda_5 x z(patronage_i)) x D_i
```

Where `z(x) = (x - mean) / std` is z-score normalization per component.

**Lambda weights:**

| Option | Method | Status |
|--------|--------|--------|
| **(a) Prior fixed** | Equal weights (0.2 each) or domain-informed priors | Recommended — simplest, no optimization target needed |
| (b) person_fe prediction | Cross-validated prediction of held-out person_fe | Alternative — avoids anime.score but introduces circularity |
| (c) Next-year production scale | Predict next year's production scale | Alternative — temporal separation ensures no leakage |

**Prohibited:** Lambda optimization against anime.score (previous implementation minimized MSE predicting average anime.score per person).

**Known issues:**
- Phase 6 recomputes IV without passing `component_std`/`component_mean`, producing unnormalized scores (todo.md B02)
- L2 regularization `alpha=0.5` constrains lambdas to +-10-15% of prior, effectively making CV meaningless (todo.md D07)

**Normalization:**
- Each component z-score normalized before weighting
- Final scores normalized to [0, 100] range via min-max scaling
- Dormancy D applied multiplicatively after the weighted sum

Result stored as `iv_score` in `scores.json`.

---

## 3. Graph Edge Weights

Source: `src/analysis/graph.py`

### Work Importance (Structural Only)

```
importance = duration_mult
```

- `duration_mult` = format-based multiplier (Movie: 1.5, TV: 1.0, etc.)

**Prohibited:** `score_mult = anime.score / 10.0` must be removed from `_work_importance()`. Edge weights must not encode viewer ratings.

### Person-Person Edge Weight

```
edge_weight += commit_a x commit_b x episode_overlap x importance
```

- `commit_a`, `commit_b` = commitment multipliers from role categories
- `episode_overlap` = Jaccard overlap of episode assignments (or 1.0 if unknown)
- **Note:** The multiplicative form `commit_a x commit_b` creates quadratic scaling (see todo.md D03)

### Closeness Centrality

**Known bug:** `nx.closeness_centrality(subg, distance="weight")` treats weight as distance (larger = farther), but this graph's weights are similarity (larger = closer). Results are inverted for all persons. See todo.md B05.

**Fix:** Either remove `distance="weight"` or add `distance = 1/weight` attribute.

---

## 4. Layer 2: Individual Contribution Profile

Source: `src/analysis/individual_contribution.py`

Layer 2 metrics are designed as the **compensation basis** — they must be defensible when used as evidence for fair pay.

### 4.1 Peer Percentile

Cohort-based ranking within same role x career year group.

```
peer_percentile_i = rank(person_fe_i within cohort) / cohort_size x 100
```

**Note:** Currently uses iv_score for ranking, but iv_score is a composite of person_fe and other components. Using person_fe directly avoids circular dependency (todo.md D14).

### 4.2 Opportunity Residual

OLS regression controlling for opportunity factors, residual = individual contribution.

```
iv_score_i = beta_0 + beta_1 x career_years_i + beta_2 x avg_staff_count_i + beta_3 x unique_studios_i + role_dummies + epsilon_i
```

- `opportunity_residual_i = epsilon_i` (studentized residual)

**Prohibited controls:** `avg_anime_score` must NOT be included as a control variable. Use `avg_staff_count` (production scale proxy) and `avg_studio_fe` (studio quality proxy) instead.

**Known issues:**
- Single-role case creates a zero dummy column (todo.md B06)
- Ridge regularization `1e-8 x I` masks the rank deficiency but distorts hat matrix

### 4.3 Consistency

Score stability across works.

```
consistency_i = max(0.0, 1.0 - CV_i)
```

Where `CV_i = std(scores) / |mean(scores)|` is the coefficient of variation.

**Known issue:** AKM residual path uses `exp(-std)` while fallback path uses `1 - CV` — different scales create discontinuity when data availability changes (todo.md B10). Must be unified to normalized CV.

### 4.4 Independent Value

Measures whether a person's presence improves collaborators' outcomes.

```
independent_value_i = mean(with_i_residuals) - mean(without_i_residuals)
```

Where residuals are computed relative to project quality (average collaborator score).

**Prohibited:** `work_score = anime.score` must be replaced. Use credit density or BiRank-based project quality.

**Known issue:** pid's own IV is not excluded from `proj_quality`, biasing results downward (todo.md B07).

---

## 5. Confidence Intervals

Source: `src/analysis/confidence.py`

**Legal requirement:** When presenting scores as compensation evidence, confidence intervals are required.

### Analytical SE (Recommended)

```
CI_i = theta_i +- t_{alpha/2} x sigma_resid / sqrt(n_obs_i)
```

- `sigma_resid` = residual standard error from AKM regression
- `n_obs_i` = number of credit observations for person i
- `t_{alpha/2}` = critical value (1.96 for 95% CI)

### Known Issues

**B08 — Scale mismatch:** `compute_score_range()` assumes `scale=100.0` but actual scores are in vastly different ranges (iv_score: -1 to +2, birank: 0 to 0.01). The fixed `max_margin = scale x 0.5` produces meaningless confidence intervals. Must either operate on percentile-transformed scores or accept per-axis scale parameters.

**B09 — Bootstrap wrong target:** `compute_bootstrap_confidence()` bootstraps the mean of AKM residuals (which is approximately 0 by OLS construction), not person_fe uncertainty. The resulting CI width is near-zero and uninformative. Must either use analytical SE (recommended) or cluster-bootstrap resampling that re-estimates the full AKM.

---

## 6. Career Classification

### 6.1 Career Stage Groups

Source: `generate_industry_overview()` in `scripts/generate_all_reports.py`

Rule-based mapping from `highest_stage` (pipeline output, 0-6) to three groups:

| Group | Stage Range | Typical Roles |
|---|---|---|
| Newcomer | stage 0-2 | In-between, key animator, 2nd key animator |
| Mid-level | stage 3-4 | Animation director assistant, animation director, episode director |
| Veteran | stage 5-6 | Chief animation director, series director, director |

`highest_stage` is the career peak stage at analysis time (not stage at any given year).

**Note on career advancement vs dropout for Newcomers:**
- **Dropout**: `latest_year == yr AND highest_stage <= 2` — never advanced
- **Career advancement**: Promotion event in `milestones.json` with `type == "promotion"` and `from_stage <= 2, to_stage >= 3` — mutually exclusive with dropout by construction

### 6.2 Role Type Groups

Source: `ROLE_TYPE_DEF` in `generate_industry_overview()`

| Key | Label | Primary roles |
|---|---|---|
| `animator` | Animation | Animation drawing roles |
| `director` | Direction | Episode director, series director |
| `designer` | Design | Character designer, art director |
| `production` | Production | Production staff |
| `writing` | Script | Script, series composition |
| `technical` | Technical/CG | Technical, 3DCG |
| `other` | Other | Everything else |

Assigned from `primary_role` field in `scores.json`. Falls back to `"other"`.

**Known issue:** CHIEF_ANIMATION_DIRECTOR is in DIRECTOR_ROLES but classified as `animation_supervision` in ROLE_CATEGORY — dual classification (todo.md D12).

### 6.3 Loss Type Classification

Source: `LOSS_TYPES` in `generate_industry_overview()`

Applied to persons with `latest_year <= EXIT_CUTOFF_YEAR` (confirmed exits):

| Type | Condition (priority order) |
|---|---|
| Ace departure | `iv_score > HIGH_IV_THRESHOLD (30.0)` |
| Veteran retirement | `highest_stage >= 5` |
| Mid-level departure | `highest_stage >= 3` |
| Newcomer early dropout | `highest_stage <= 2` (default) |

Mutually exclusive by priority: iv_score check takes precedence over stage.

---

## 7. Personnel Flow Metrics

### 7.1 Stock

Number of persons "active" in a given year Y:

```
stock(Y) = |{ i : first_year_i <= Y <= latest_year_i }|
```

Computed for `Y in [FLOW_START_YEAR, EXIT_CUTOFF_YEAR]`.
Stratified by career stage group to produce `stock_by_sg[stage_group][year]`.

**Limitation:** `latest_year` = year of last known credit. The EXIT_CUTOFF_YEAR cap mitigates false "exits."

### 7.2 Entry / Exit Counting

```
entry(Y) = |{ i : first_year_i == Y }|
exit(Y)  = |{ i : latest_year_i == Y }|,  Y <= EXIT_CUTOFF_YEAR
```

Stratified by: career stage group, talent tier, role type.

### 7.3 Career Stage Transitions

Source: `milestones.json`, event `type == "promotion"`

```
transition(from_group -> to_group, Y) = |{
    i : promotion event at year Y,
        stage_group(from_stage) != stage_group(to_stage)
}|
```

Tracked transitions: Newcomer -> Mid-level, Mid-level -> Veteran, Newcomer -> Veteran

---

## 8. Expected Ability Score

Source: `generate_industry_overview()`, `src/analysis/expected_ability.py`

**Goal:** Estimate a person's "contextual potential" from who they worked with and where, independent of their own accumulated track record.

### Collaborator Quality per Anime

```
avg_collab_iv[a] = mean({ iv_score_i : i in cast(a) })
```

### Per-Person Aggregation

For person i, across all animes A_i they participated in:

```
person_collab_iv[i] = mean({ avg_collab_iv[a] : a in A_i })
```

**Prohibited:** Weighting by anime.score must be removed. Use equal weighting or weight by production scale (staff_count x episodes).

### Studio Prestige

```
studio_prestige[i] = max({ studio_avg_iv[j] : j in studios(i) })
```

### Composite Expected Ability

```
expected_raw[i] = 0.50 x norm(person_collab_iv[i])
               + 0.30 x norm(avg_production_scale[i])
               + 0.20 x norm(studio_prestige[i])
```

**Weights rationale:**
- 0.50 — collaborator quality is the strongest signal; peers set the standard
- 0.30 — production scale filters out small/niche productions (structural proxy for project importance)
- 0.20 — studio prestige provides an institutional baseline

**Prohibited:** The second component must NOT use `person_work_score` (mean anime.score). Use average production scale instead.

### Four-Tier Classification

Threshold: `_EXP_HIGH_PCTILE = 70.0` (top 30%)

| Tier | Condition | Interpretation |
|---|---|---|
| Proven excellence | expected >= 70 AND actual >= 70 | High bar environment + proven track record |
| Rising star | expected >= 70 AND actual < 70 | High-quality environment; potential not yet reflected |
| Hidden talent | expected < 70 AND actual >= 70 | Proved themselves despite modest environment |
| Standard | expected < 70 AND actual < 70 | Baseline |

---

## 9. Blank Returnee Detection

Source: Chart I in `generate_industry_overview()`

**Data source:** Direct DB query covering all persons with credit history (not just top-200 from growth.json).

For each person i, maximum consecutive gap between active years. Requires actual return (gap in the middle of career, not at the end).

| Category | Gap Range |
|---|---|
| Short blank | 3-4 years |
| Medium blank | 5-9 years |
| Long blank | 10+ years |

---

## 10. Seasonal Debut Counting

**True newcomer definition:**
```
is_newcomer(i, year) = (first_year[i] == year)
```

Each (person, decade, season, cour_type) counted at most once via deduplication set.

---

## 11. Value Flow Analysis

```
lost_value[Y]   = sum{ iv_score_i : latest_year_i == Y, Y <= EXIT_CUTOFF_YEAR }
growth_value[Y] = sum{ iv_score_i : latest_year_i == Y, trend_i == "rising" }
entry_value[Y]  = sum{ iv_score_i : first_year_i == Y }
```

All three use iv_score as the value proxy. Charts show totals (absolute industry impact).

---

## 12. Cluster Analyses

All use scikit-learn `KMeans` with `StandardScaler` normalization. `random_state=42` throughout.

### 12.1 Stage Boundary Validation (K=3)

**Features (4):** `highest_stage`, `active_years`, `total_credits`, `iv_score`

```
agreement_rate = |{ i : stage_group_rule(i) == stage_group_kmeans(i) }| / N
```

### 12.2 Score x Career Cluster (K=5)

**Features (7):** `iv_score`, `birank`, `patronage`, `person_fe`, `highest_stage`, `active_years`, `total_credits`

Cluster names assigned dynamically by iv_score centroid rank.

### 12.3 Person Ranking Cluster (K=dynamic)

**K selection:** `K = min(8, max(3, n_persons // 5))`

**Features (8):** `birank`, `patronage`, `person_fe`, `iv_score`, `total_credits`, `degree_centrality`, `betweenness_centrality`, `eigenvector_centrality`

---

## 13. Decade Comparison

Direct DB query: `year x format -> (anime_count, person_count, credit_count)`

Two-panel display:
- **Demand** = anime_count by format
- **Supply** = person_count by format

---

## 14. SHAP Feature Importance

**Model:** `GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)`
**Target:** `iv_score`

**Features (13):** birank, patronage, person_fe, awcc, ndi, dormancy, career_friction, peer_boost, total_credits, active_years, highest_stage, degree_centrality, betweenness_centrality

**Run requirement:** `PYTHONNOUSERSITE=1` to prevent incompatible numba.

**Charts:** Mean |SHAP| bar, Beeswarm, Dependence plots (top 4), Top vs Bottom comparison.

---

## Appendix A: Known Implementation Bugs

See `todo.md` Part 2 for full details. Summary of highest-impact bugs:

| ID | Summary | Impact |
|---|---|---|
| B01 | studio_exposure computed 3x with different formulas | Final IV uses wrong version |
| B02 | Phase 6 IV recompute missing z-score normalization | 2.4x score deviation |
| B03 | AKM panel uses studios[0] vs weighted studio_assignments | Studio label mismatch |
| B04 | BiRank Gauss-Seidel instead of Gauss-Jacobi | Different convergence point |
| B05 | closeness_centrality weight = similarity, treated as distance | All values inverted |
| B08 | Confidence interval scale assumes 100.0 for all axes | CI meaningless |
| B09 | Bootstrap CI targets residual mean (approx 0), not person_fe | CI uninformative |

## Appendix B: Key Variable Names Reference

| Variable | Type | Description |
|---|---|---|
| `pid_first_year` | `dict[str, int]` | person_id -> first credit year |
| `pid_latest_year` | `dict[str, int]` | person_id -> latest credit year |
| `pid_stage` | `dict[str, int]` | person_id -> highest_stage (0-6) |
| `pid_iv` | `dict[str, float]` | person_id -> iv_score |
| `pid_trend` | `dict[str, str]` | person_id -> trend ("rising"/"stable"/...) |
| `pid_role_type` | `dict[str, str]` | person_id -> role type key |
| `stock_by_sg` | `dict[str, dict[int, int]]` | stage_group -> year -> stock count |
| `entry_by_sg` | `dict[str, dict[int, int]]` | stage_group -> year -> entry count |
| `exit_by_sg` | `dict[str, dict[int, int]]` | stage_group -> year -> exit count |

---

*Last updated: 2026-03-02*
*Reflects the corrected design as defined in todo.md. See old_CALCULATION_COMPENDIUM.md for the previous version.*

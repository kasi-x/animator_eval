# Animetor Eval — Calculation Compendium

> **Purpose:** Reference document for every scoring formula, classification rule, and statistical method used in the pipeline and report generation. Covers both the core evaluation pipeline (`src/`) and the report generator (`scripts/generate_all_reports.py`).

---

## Table of Contents

1. [Data Quality Constants](#1-data-quality-constants)
2. [Core Scores (Pipeline)](#2-core-scores-pipeline)
   - 2.1 Person Fixed Effect (θ) — AKM
   - 2.2 BiRank — Bipartite PageRank
   - 2.3 Patronage Premium (Π)
   - 2.4 Dormancy Penalty (D)
   - 2.5 Studio Exposure
   - 2.6 AWCC
   - 2.7 Integrated Value (IV / iv_score)
3. [Career Classification](#3-career-classification)
   - 3.1 Career Stage Groups
   - 3.2 Role Type Groups
   - 3.3 Loss Type Classification
4. [Personnel Flow Metrics](#4-personnel-flow-metrics)
   - 4.1 Stock
   - 4.2 Entry / Exit Counting
   - 4.3 Career Stage Transitions
5. [Expected Ability Score](#5-expected-ability-score)
   - 5.1 Collaborator Quality per Anime
   - 5.2 Per-Person Aggregation
   - 5.3 Studio Prestige
   - 5.4 Composite Expected Ability
   - 5.5 Percentile Ranking
   - 5.6 Four-Tier Classification
6. [Blank Returnee Detection](#6-blank-returnee-detection)
7. [Seasonal Debut Counting](#7-seasonal-debut-counting)
8. [Value Flow Analysis](#8-value-flow-analysis)
9. [Cluster Analyses](#9-cluster-analyses)
   - 9.1 Stage Boundary Validation (K=3)
   - 9.2 Score × Career Cluster (K=5)
   - 9.3 Person Ranking Cluster (K=dynamic)
10. [Decade Comparison](#10-decade-comparison)
11. [SHAP Feature Importance](#11-shap-feature-importance)

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
For a person whose latest credit year is 2023–2025, data collection may still be ongoing. Using `latest_year` as "exit year" would incorrectly classify active persons as having left the industry. The cutoff excludes these false positives.

---

## 2. Core Scores (Pipeline)

### 2.1 Person Fixed Effect (θ) — AKM

Source: `src/pipeline_phases/core_scoring.py`, AKM (Abowd–Kramarz–Margolis) wage decomposition.

```
log(anime_score_ij) = θ_i + ψ_j + ε_ij
```

- `θ_i` = person fixed effect — individual contribution to work quality net of studio
- `ψ_j` = studio fixed effect — baseline quality level of studio j
- Estimated via iterative alternating least squares (Mundlak within-estimator)
- Result stored as `person_fe` in `scores.json`

### 2.2 BiRank — Bipartite PageRank

Source: `src/analysis/birank.py`

Bipartite graph G = (Persons ∪ Anime, Credits). Edge weight W[i,j] = credit weight (role importance × repeat bonus × director bonus).

**Transition matrices:**
```
S = D_p^{-1} · W       (person → anime, row-normalized)
T = W · D_a^{-1}       (anime → person, col-normalized)
```

**Iterative update (until convergence):**
```
p_new = α · T · u + (1-α) · p_0
u_new = β · S^T · p   + (1-β) · u_0
```

- α = 0.85 (person damping), β = 0.85 (anime damping)
- p_0, u_0 = uniform initial vectors (or query-biased)
- Convergence: L1 norm change < 1e-6, max 100 iterations
- Result stored as `birank` in `scores.json`

### 2.3 Patronage Premium (Π)

Source: `src/analysis/patronage_dormancy.py`

Measures how much a person benefited from repeat collaboration with high-prestige directors.

```
Π_i = Σ_d  (PR_d × log(1 + N_id) × Quality_id)
```

- `PR_d` = BiRank score of director d
- `N_id` = number of co-credit collaborations between person i and director d
- `Quality_id` = average anime score of works they shared
- `log(1 + N)` = diminishing returns on repeat collaboration
- Result stored as `patronage` in `scores.json`

### 2.4 Dormancy Penalty (D)

Source: `src/analysis/patronage_dormancy.py`

Penalizes persons with long inactivity gaps relative to current year.

```
D(i, t) = exp(-δ × max(0, gap_i - τ_grace))
```

- `gap_i` = years since last credited work
- `τ_grace` = 2 years (no penalty within grace period)
- `δ` = 0.3 (decay rate; configurable)
- D = 1.0 if gap ≤ τ_grace; D → 0 as gap → ∞
- Result stored as `dormancy` in `scores.json`

### 2.5 Studio Exposure

Source: `src/analysis/integrated_value.py`

Weighted sum of studio fixed effects for all studios a person has worked at.

```
studio_exposure_i = Σ_j  (I{i∈j} · ψ_j · years_ij / total_years_i)
```

- `ψ_j` = studio fixed effect from AKM
- `years_ij` = years person i worked at studio j
- Time-weighted: longer tenures at high-quality studios score higher

### 2.6 AWCC (Anime-Weighted Collaboration Centrality)

Source: `src/pipeline_phases/supplementary_metrics.py`

Measures how centrally positioned a person is in high-quality works, weighted by those works' reception scores.

### 2.7 Integrated Value (IV / iv_score)

Source: `src/analysis/integrated_value.py`

**Formula:**
```
IV_i = (λ₁·θ_i + λ₂·birank_i + λ₃·studio_exp_i + λ₄·awcc_i + λ₅·patronage_i) × D_i
```

**Weight optimization:**
λ = (λ₁, λ₂, λ₃, λ₄, λ₅) are optimized via leave-one-out cross-validation on anime scores:

```
minimize  Σ_folds MSE( Σ_k λ_k · X_k[test], anime_score[test] )
subject to  Σ_k λ_k = 1,  λ_k ≥ 0
```

- Optimizer: `scipy.optimize.minimize` (SLSQP)
- Feature matrix X: each column is one component, row-normalized per person
- Falls back to equal weights (1/5 each) if < 20 persons in training set
- Dormancy D is applied multiplicatively *after* the weighted sum (not optimized)
- Final scores normalized to [0, 100] range via min-max scaling
- Result stored as `iv_score` in `scores.json`

---

## 3. Career Classification

### 3.1 Career Stage Groups

Source: `generate_industry_overview()` in `scripts/generate_all_reports.py`

Rule-based mapping from `highest_stage` (pipeline output, 0–6) to three groups:

| Group | Stage Range | Typical Roles |
|---|---|---|
| 新人 (Newcomer) | stage 0–2 | 動画・原画・第二原画 |
| 中堅 (Mid-level) | stage 3–4 | 作画監督補佐・作画監督・演出 |
| ベテラン (Veteran) | stage 5–6 | 総作画監督・シリーズ監督・監督 |

`highest_stage` is the career peak stage at analysis time (not stage at any given year).

**Note on "キャリアアップ" vs "退職" for Newcomers:**
- **退職/ドロップアウト**: `latest_year == yr AND highest_stage ≤ 2` — never advanced; classified into 新人 exit
- **キャリアアップ**: Promotion event in `milestones.json` with `type == "promotion"` and `from_stage ≤ 2, to_stage ≥ 3` — these two categories are mutually exclusive by construction

### 3.2 Role Type Groups

Source: `ROLE_TYPE_DEF` in `generate_industry_overview()`

| Key | Label | Primary roles |
|---|---|---|
| `animator` | 動画/原画 | Animation drawing roles |
| `director` | 演出/監督 | Episode director, series director |
| `designer` | デザイナー | Character designer, art director |
| `production` | 制作 | Production staff |
| `writing` | 脚本/構成 | Script, series composition |
| `technical` | 技術/CG | Technical, 3DCG |
| `other` | その他 | Everything else |

Assigned from `primary_role` field in `scores.json`. Falls back to `"other"`.

### 3.3 Loss Type Classification

Source: `LOSS_TYPES` in `generate_industry_overview()`

Applied to persons with `latest_year ≤ EXIT_CUTOFF_YEAR` (confirmed exits):

| Type | Condition (priority order) |
|---|---|
| エース離脱 | `iv_score > HIGH_IV_THRESHOLD (30.0)` |
| ベテラン引退 | `highest_stage ≥ 5` |
| 中堅離脱 | `highest_stage ≥ 3` |
| 新人早期離脱 | `highest_stage ≤ 2` (default) |

Mutually exclusive by priority: iv_score check takes precedence over stage.

---

## 4. Personnel Flow Metrics

### 4.1 Stock

Number of persons "active" in a given year Y, defined as:

```
stock(Y) = |{ i : first_year_i ≤ Y ≤ latest_year_i }|
```

Computed for `Y ∈ [FLOW_START_YEAR, EXIT_CUTOFF_YEAR]`.
Stratified by career stage group to produce `stock_by_sg[stage_group][year]`.

**Limitation:** `latest_year` = year of last known credit. Persons who are active but haven't had credits recorded recently will appear as "exited." The EXIT_CUTOFF_YEAR cap mitigates this.

### 4.2 Entry / Exit Counting

```
entry(Y) = |{ i : first_year_i == Y }|
exit(Y)  = |{ i : latest_year_i == Y }|,  Y ≤ EXIT_CUTOFF_YEAR
```

Stratified by: career stage group, talent tier, role type.

Years included for entry: `FLOW_START_YEAR ≤ Y ≤ RELIABLE_MAX_YEAR`
Years included for exit:  `FLOW_START_YEAR ≤ Y ≤ EXIT_CUTOFF_YEAR`

### 4.3 Career Stage Transitions

Source: `milestones.json`, event `type == "promotion"`

```
transition(from_group → to_group, Y) = |{
    i : promotion event at year Y,
        _stage_group(from_stage) != _stage_group(to_stage)
}|
```

Tracked transitions: 新人→中堅, 中堅→ベテラン, 新人→ベテラン
Year filter: `FLOW_START_YEAR ≤ Y ≤ RELIABLE_MAX_YEAR`

---

## 5. Expected Ability Score

Source: `generate_industry_overview()`, Chart D computation block (`_exp_` prefixed variables)

**Goal:** Estimate a person's "contextual potential" from who they worked with and where, independent of their own accumulated track record. Useful for evaluating newcomers before they have an established iv_score.

### 5.1 Collaborator Quality per Anime

For each anime a:

```
avg_collab_iv[a] = mean({ iv_score_i : i ∈ cast(a) })
```

All persons on the same anime, including the target person themselves. The mean iv_score of co-workers signals the quality bar of the production.

### 5.2 Per-Person Aggregation

For person i, across all animes A_i they participated in, weighted by anime score (higher-rated works carry more signal):

```
person_collab_iv[i] = Σ_{a ∈ A_i} (avg_collab_iv[a] × score_a)
                      ────────────────────────────────────────
                             Σ_{a ∈ A_i} score_a
```

If `score_a` is missing (unscored work), weight defaults to 1.0.

```
person_work_score[i] = mean({ score_a : a ∈ A_i, score_a > 0 })
```

### 5.3 Studio Prestige

For studio j, computed from all persons who ever worked there:

```
studio_avg_iv[j] = mean({ iv_score_i : i ∈ workforce(j) })
```

For person i, prestige = the maximum studio average among all studios they worked at:

```
studio_prestige[i] = max({ studio_avg_iv[j] : j ∈ studios(i) })
```

Reuses `studio_person_years` (already loaded for Chart G) — no additional DB query.

### 5.4 Composite Expected Ability

Normalize each component to [0, 1], then combine:

```
expected_raw[i] = 0.50 × (person_collab_iv[i] / max_collab_iv)
                + 0.30 × (person_work_score[i] / max_work_score)
                + 0.20 × (studio_prestige[i]   / max_studio_iv)
```

**Weights rationale:**
- 0.50 — collaborator quality is the strongest signal; peers set the standard
- 0.30 — work reception score filters out poor-quality productions
- 0.20 — studio prestige provides an institutional baseline

### 5.5 Percentile Ranking

Convert raw scores to percentile rank (0–100) via bisect on sorted values:

```
expected_pctile[i] = bisect_left(sorted(expected_raw.values()), expected_raw[i])
                     ──────────────────────────────────────────────────────────── × 100
                                    len(expected_raw) - 1
```

Same formula applied to `iv_score` values → `actual_pctile[i]`.

### 5.6 Four-Tier Classification

Threshold: `_EXP_HIGH_PCTILE = 70.0` (top 30%)

| Tier | Condition | Interpretation |
|---|---|---|
| **優秀確定** | expected ≥ 70 AND actual ≥ 70 | High bar environment + proven track record |
| **期待の星** | expected ≥ 70 AND actual < 70 | High-quality environment; potential not yet reflected in score |
| **隠れた実力** | expected < 70 AND actual ≥ 70 | Proved themselves despite modest environment |
| **標準** | expected < 70 AND actual < 70 | Baseline — neither high-signal environment nor high score |

**Empirical distribution (2026-02-28 snapshot, 58,406 persons):**

| Tier | Count | Fraction |
|---|---|---|
| 優秀確定 | 7,753 | 13.3% |
| 期待の星 | 9,750 | 16.7% |
| 隠れた実力 | 3,334 | 5.7% |
| 標準 | 37,569 | 64.3% |

**"期待の星" Unfulfilled Rate:**
Among 期待の星 persons with career span ≥ 5 years (sufficient time to accumulate actual performance), the fraction still below the actual ability threshold:

```
unfulfilled_rate = |{ i ∈ 期待の星 : latest_year_i - first_year_i ≥ 5 }|
                   ──────────────────────────────────────────────────────
                              |{ i ∈ 期待の星 }|
```

---

## 6. Blank Returnee Detection

Source: Chart I in `generate_industry_overview()`

**Data source:** Direct DB query — `credits JOIN anime` for all persons from 1980 to `EXIT_CUTOFF_YEAR`. This replaces the `growth.json` approach (which covers only the top 200 persons by score).

**Algorithm:**

For each person i, collect their set of distinct active years:

```
active_years[i] = { a.year : (i, a) ∈ credits, a.year ≤ EXIT_CUTOFF_YEAR }
```

Maximum consecutive gap:

```
max_gap[i] = max({ years[k+1] - years[k] - 1 : k ∈ 0..n-2 })
             where years = sorted(active_years[i])
```

**Requires actual return**: Person must have credits *after* the detected gap (gap must occur in the middle of the career, not at the end).

**Three blank categories:**

| Category | Gap Range | Notes |
|---|---|---|
| 短期ブランク | 3–4 years | May be false positive for long-production involvement |
| 中期ブランク | 5–9 years | Reliable signal |
| 長期ブランク | 10+ years | Highly reliable; rare |

**Caution on short blanks:** Major productions (theatrical films, multi-year series) can take 2–4 years from start to credit. A 3–4 year gap may reflect production schedule, not genuine inactivity.

**Empirical counts (EXIT_CUTOFF_YEAR=2022, all 99,114 persons with credit history):**

| Category | Count |
|---|---|
| 短期ブランク (3–4yr) | 9,783 |
| 中期ブランク (5–9yr) | 6,686 |
| 長期ブランク (10yr+) | 3,457 |

---

## 7. Seasonal Debut Counting

Source: Chart C in `generate_industry_overview()` Seasonal Patterns section

**True newcomer definition:**

```
is_newcomer(i, year) = (first_year[i] == year)
```

`first_year` is taken from `pid_first_year` dict (built from `scores.json`). A person is counted as a newcomer exactly once per year: the year their first credit appears in the database.

**Deduplication:** Each (person, decade, season, cour_type) combination is counted at most once via a `debut_seen` set to prevent inflation from multiple credits within the same cour.

**Decade-based grouping:**

```
decade(year) = (year // 10) * 10
```

Groups: 1990, 2000, 2010, 2020.

**Cour type mapping:**

| Cour Type | Episode Count |
|---|---|
| movie_or_special | ≤ 1 |
| single_cour | 2–14 |
| multi_cour | 15–28 |
| long_cour | ≥ 29 |
| unknown | NULL or 0 |

---

## 8. Value Flow Analysis

Source: Charts F1–F3 in `generate_industry_overview()`

**Lost value** (talent that left the industry):

```
lost_value[Y] = Σ{ iv_score_i : latest_year_i == Y, Y ≤ EXIT_CUTOFF_YEAR }
```

**Growth value** (active persons on an upward trajectory):

```
growth_value[Y] = Σ{ iv_score_i : latest_year_i == Y, trend_i == "rising" }
```

`trend_i` is the growth trajectory label from `growth.json`.

**Entry value** (new entrants to the industry):

```
entry_value[Y] = Σ{ iv_score_i : first_year_i == Y }
```

All three use the raw `iv_score` as the value proxy. The charts show totals, not per-person averages, to reflect absolute industry impact.

---

## 9. Cluster Analyses

All use scikit-learn `KMeans` with `StandardScaler` normalization. `random_state=42` throughout.

### 9.1 Stage Boundary Validation (K=3)

Source: K-Means validation block in `generate_industry_overview()`, Chart A section

**Features (4):** `highest_stage`, `active_years`, `total_credits`, `iv_score`

**Purpose:** Verify that the rule-based stage groups (新人/中堅/ベテラン) correspond to natural clusters in the data.

```
agreement_rate = |{ i : stage_group_rule(i) == stage_group_kmeans(i) }| / N
```

Cluster labels assigned by sorting cluster centroids by `highest_stage` dimension.

**Hyperparameters:** K=3, `n_init=20`

### 9.2 Score × Career Cluster (K=5) — Chart J

Source: Chart J in `generate_industry_overview()`

**Features (7):** `iv_score`, `birank`, `patronage`, `person_fe`, `highest_stage`, `active_years`, `total_credits`

**Cluster names** (assigned by iv_score centroid rank, ascending):

| Rank | Name |
|---|---|
| 0 (lowest iv) | 低スコア層（新人中心） |
| 1 | 中低スコア・活動中 |
| 2 | 中スコア・中堅層 |
| 3 | 高スコア・ベテラン |
| 4 (highest iv) | トップ層（エース） |

**Hyperparameters:** K=5, `n_init=20`

**Output:** Entry/exit/stock time series per cluster (same method as §4.1–4.2).

### 9.3 Person Ranking Cluster (K=dynamic)

Source: `generate_person_ranking()` in `scripts/generate_all_reports.py`

**Features (8):** `birank`, `patronage`, `person_fe`, `iv_score`, `total_credits`, `degree_centrality`, `betweenness_centrality`, `eigenvector_centrality`

**K selection:**
```
K = min(8, max(3, n_persons // 5))
```

Cluster names generated dynamically by `_name_clusters_by_rank(centers, feat_specs)` helper — labels derived from relative centroid rankings per feature, avoiding hardcoded thresholds.

---

## 10. Decade Comparison

Source: `generate_industry_overview()` Decade Comparison section

**DB query:**

```sql
SELECT a.year, a.format,
    COUNT(DISTINCT a.id)        AS anime_count,   -- demand
    COUNT(DISTINCT c.person_id) AS person_count,  -- supply
    COUNT(c.id)                 AS credit_count
FROM anime a
LEFT JOIN credits c ON c.anime_id = a.id
WHERE a.year BETWEEN 1980 AND {RELIABLE_MAX_YEAR}
  AND a.format IN ('TV','MOVIE','OVA','ONA','TV_SHORT')
GROUP BY a.year, a.format
```

Two-panel display:
- **Demand** = `anime_count` by format (number of productions created)
- **Supply** = `person_count` by format (number of unique persons employed)

The demand/supply ratio by format reveals which production types are credit-intensive.

---

## 11. SHAP Feature Importance

Source: `generate_shap_report()` in `scripts/generate_all_reports.py`

**Model:** `GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)`
**Target:** `iv_score`

**Features (13):**

| # | Feature | Source |
|---|---|---|
| 1 | `birank` | BiRank score |
| 2 | `patronage` | Patronage premium |
| 3 | `person_fe` | AKM person fixed effect |
| 4 | `awcc` | Anime-weighted collaboration centrality |
| 5 | `ndi` | Network diversity index |
| 6 | `dormancy` | Dormancy penalty multiplier |
| 7 | `career_friction` | Derived career difficulty metric |
| 8 | `peer_boost` | Peer network boost |
| 9 | `total_credits` | Total number of credits |
| 10 | `active_years` | Years with at least one credit |
| 11 | `highest_stage` | Peak career stage (0–6) |
| 12 | `degree_centrality` | Graph degree centrality |
| 13 | `betweenness_centrality` | Betweenness centrality |

**SHAP computation:**

```python
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_sample)   # X_sample: up to 3,000 rows
```

`expected_value` handling: `float(ev.item() if hasattr(ev, "item") else ev)` — guards against 0-d numpy array from older SHAP versions.

**Run requirement:** `PYTHONNOUSERSITE=1` — prevents user-site numba (incompatible NumPy version) from shadowing the pixi environment's NumPy.

**Charts produced:**
1. Mean |SHAP| bar chart — global feature importance
2. Beeswarm plot — per-sample SHAP value distribution
3. Dependence plots — top 4 features: SHAP value vs feature value
4. Top-10 vs Bottom-10 mean SHAP comparison

---

## Appendix: Key Variable Names Reference

| Variable | Type | Description |
|---|---|---|
| `pid_first_year` | `dict[str, int]` | person_id → first credit year |
| `pid_latest_year` | `dict[str, int]` | person_id → latest credit year |
| `pid_stage` | `dict[str, int]` | person_id → highest_stage (0–6) |
| `pid_iv` | `dict[str, float]` | person_id → iv_score |
| `pid_trend` | `dict[str, str]` | person_id → trend ("rising"/"stable"/…) |
| `pid_role_type` | `dict[str, str]` | person_id → role type key |
| `pid_first_year` | `dict[str, int]` | Built from `scores.json` LIST (not dict) |
| `stock_by_sg` | `dict[str, dict[int, int]]` | stage_group → year → stock count |
| `entry_by_sg` | `dict[str, dict[int, int]]` | stage_group → year → entry count |
| `exit_by_sg` | `dict[str, dict[int, int]]` | stage_group → year → exit count |
| `transitions` | `dict[str, dict[int, int]]` | "新人→中堅" → year → count |
| `_exp_pid_expected` | `dict[str, float]` | person_id → expected ability pctile |
| `_exp_pid_actual` | `dict[str, float]` | person_id → actual ability pctile |
| `_exp_pid_tier` | `dict[str, str]` | person_id → tier label |
| `studio_person_years` | `dict[str, dict[int, set]]` | studio → year → {person_ids} |

---

*Last updated: 2026-02-28*
*Covers pipeline version as of commit d626a6e and report generator as of this session.*

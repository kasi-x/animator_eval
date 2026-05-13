# Method Note Template — Standardized Structure (v2)

**Purpose**: Standardized skeleton for method notes across all analysis modules.  
**Introduced**: 2026-05-13 (TASK_CARDS/15_extension_reports/x_cross_cutting, §4)  
**Reference**: docs/report_cross_cutting.md §6, docs/REPORT_PHILOSOPHY.md §3

---

## Structure Overview

Each method note shall follow this structure. Sections marked **(required)** must be included; sections marked **(optional)** may be omitted if not applicable.

```
1. Header (Title, Status, Module, Constraints)
2. Purpose (1–2 sentences, labor-first framing)
3. Specification
   3.1 Model / Definition (formula, pseudocode, or algorithm description)
   3.2 Estimation / Computation (steps, parameters, seeding)
   3.3 Confidence Interval (if applicable)
   3.4 Null Model / Baseline (if applicable)
4. Output Fields (table of results)
5. Known Limitations (assumption violations, caveats)
6. Interpretation Guide (what high / low values mean; explicit prohibitions)
7. References (citations, code pointers)
```

---

## Template (Copy & Customize)

```markdown
# Method Note: [Method Name] ([Report Family] / [O-ID if applicable])

**Status**: implemented | planned  
**Module**: `src/analysis/[category]/[module].py`  
**Hard constraints**: H1 (anime.score excluded), H4 (CI required for individual-level), ...

---

## Purpose

[1–2 sentences describing what this method measures or estimates.]

[One sentence on labor-first framing: e.g., "This metric indicates structural position, not individual ability."]

---

## Specification

### Model / Definition

[Formula, pseudocode, or English description of the algorithm.]

**Example** (if appropriate):

```
θ_i = person FE in log(production_scale) = β0 + Σ θ_i·I(person=i) + ...
```

| Term | Description | H1 Compliance | Notes |
|------|---|---|---|
| `outcome` | what is being measured | (yes/no: is anime.score involved?) | unit, source |
| `covariate` | control variable | | |
| ... | | | |

### Estimation / Computation

[Step-by-step algorithm or function calls.]

**Parameters** (with defaults):
- `seed`: 42 (or mechanism for ensuring reproducibility)
- `n_iterations`: [if applicable]
- `convergence_tolerance`: [if applicable]

**Pseudocode**:
```python
for each individual i:
    compute X_i
    regress Y ~ X_i + controls
    extract coefficient / residual
```

**Implementation**: `src/analysis/.../function_name()`

### Confidence Interval **(required for individual-level claims)**

[Which CI method is used? (analytical SE, bootstrap, Bayesian, ...)]

**Formula** (if analytical):
```
SE[i] = σ[i] / √n[i]        (ddof=1)
CI95[i] = mean ± t_{n-1, 0.975} × SE[i]
```

**Bootstrap** (if applicable):
- Resampling scheme: [with-replacement / block / stratified?]
- Number of iterations: B = 1000 (default)
- Seed: 42
- Percentile method: [e.g., 2.5th and 97.5th for 95% CI]

**Coverage calibration**: [Has this CI been validated? e.g., "Empirical coverage is 94.8% ± 2pp in simulation."]

**Edge cases**:
- [e.g., "n < 2 observations: CI = None"]
- [e.g., "ties > 50%: Continuity correction applied"]

### Null Model / Baseline **(required for population-level claims)**

[What is the null hypothesis H0?]

**H0**: [statement]

**Test statistic**: [e.g., "log-rank statistic", "permutation p-value", ...]

**Null distribution**:
```python
for _ in range(n_permutations):
    permuted_y = shuffle(y)
    statistic_null.append( compute_stat(permuted_y, X) )

p_value = #{|statistic_null| >= |statistic_obs|} / n_permutations
```

**Seed**: 42  
**Permutations**: 1000 (default)

**Meaning of p-value**: [e.g., "Probability that observed difference would occur under random reshuffling of group labels."]

**Baseline comparator** (if applicable): [e.g., "industry median blockage score", "degree-preserving rewire graph"]

---

## Output Fields

| Field Name | Type | Description | Unit | Example |
|---|---|---|---|---|
| `[name]` | float / int / str | [what is stored] | [unit] | [sample value] |
| `[name]_se` | float | Standard error | [same as main field] | 0.042 |
| `[name]_ci_lower` | float | CI lower bound (95%) | [same] | 0.156 |
| `[name]_ci_upper` | float | CI upper bound (95%) | [same] | 0.284 |
| `[name]_p_value` | float | Permutation null p-value | dimensionless, [0,1] | 0.037 |

**Storage**:
- Individual-level results: `individual_profiles[person_id]` (Pydantic dataclass)
- Population-level results: `meta_*` table in Mart schema (`src/db/schema.py`)

---

## Known Limitations

### Assumption Violations

- **Assumption**: [e.g., "Proportional hazards assumption (Cox)"]
  - **Violation**: [what can go wrong]
  - **Check**: [how to diagnose; e.g., "Schoenfeld residual test"]
  - **Consequence**: [effect on CI / bias]
  - **Mitigation**: [if any]

### Sample Size & Coverage

- Minimum n: [e.g., "n ≥ 2 observations per person for CI"]
- Known biases when n < [threshold]: [description]

### Excluded Cases

- [e.g., "Persons with < 1 year tenure excluded"]
- [e.g., "Anime with < 2 staff excluded"]

### H1 Compliance (anime.score exclusion)

**Explicitly NOT used**:
- [Which anime.score fields are excluded?]
- [Why?]

**Checked by**: `pixi run python scripts/lint_vocab.py [module_file]` (no violations allowed)

### Measurement Specificity (narrow labeling)

[What is actually measured? What is NOT measured?]

**Example**:
- **Measured**: "Co-credit density among team members in production years N, M"
- **NOT measured**: "Team chemistry", "collaboration quality", "ability to work together"

---

## Interpretation Guide

### What a High Value Means

[Observational language only. Avoid causal or evaluative framing.]

**Example**:
- ✅ "High birank score co-occurs with centrality in the co-credit network and high out-degree to upstream roles."
- ❌ "High birank indicates strong ability to lead teams." (ability framing prohibited)

### What a Low Value Means

[Same non-causal, non-evaluative language.]

### Do NOT Interpret As

- [Explicit list of prohibited interpretations, often from forbidden_vocab.yaml]
- ❌ "Ability" / "talent" / "skill level"
- ❌ "Quality of work" / "professional merit"
- ❌ "Causation" (unless identification strategy is named)
- ❌ "Ranking" of individuals (use percentile + CI instead)

### When to Question This Metric

[Edge cases where this metric may mislead:]
- [e.g., "If anime.score appears artificially high for recent works, birank may conflate temporal trend with structural position."]
- [e.g., "If overseas subcontractors are systematically uncredited, co-credit metrics underestimate their network position."]

---

## References

### Academic / Methodological

- [Author Year] — Citation for the underlying method (e.g., "Cox, D. R. (1972). Regression models and life tables.")
- [Author Year] — Causal inference background (if applicable)

### Code & Tests

- **Implementation**: `src/analysis/[category]/[module].py::[function_name]`
- **Tests**: `tests/analysis/[category]/test_[module].py::[test_name]`
- **Integration**: `src/pipeline_phases/[phase_number]_[name].py` (which phase calls this method)

### Related Method Notes

- [Link to related methods; e.g., "See Method Note: AKM fixed effects (foundation for this metric)"]

---

## Example Instantiation

**File**: `docs/method_notes/cox_regression_o1.md`

```markdown
# Method Note: Cox Proportional Hazards Regression (O1 Gender Ceiling)

**Status**: implemented  
**Module**: `src/analysis/causal/gender_progression.py`  
**Hard constraints**: H1 (no anime.score), H4 (CI required)

---

## Purpose

Estimates the association between gender and time-to-credit-visibility-loss while 
controlling for career stage. Hazard ratios indicate relative rate of visibility 
loss between gender groups, holding other covariates constant. This metric describes 
network position, not individual ability.

---

## Specification

### Model

```
h(t | Z) = h0(t) × exp(β1·gender_f + β2·cohort_5y + β3·role_rank)
```

| Term | Description | H1 Compliance |
|---|---|---|
| `h0(t)` | baseline hazard (unspecified) | yes |
| `gender_f` | binary (F=1, M=0, NULL=NA) | yes (structural grouping) |
| `cohort_5y` | 5-year debut cohort | yes |
| `role_rank` | median role position (ordinal 0–4) | yes |

### Estimation

Using `lifelines.KaplanMeierFitter` + `CoxPHFitter`:

```python
from lifelines import CoxPHFitter
kmf = CoxPHFitter()
kmf.fit(T=time_to_loss, E=event, X=covariates, show_progress=False)
hr = np.exp(kmf.params_)
ci_lower, ci_upper = kmf.confidence_interval_
```

Seed: None (Cox MLE is deterministic).

### Confidence Interval

Analytical CI via observed information matrix (default in lifelines).
95% CI: `exp(log_hr ± z_0.975 × se_coef)` where z_0.975 ≈ 1.96.

Coverage: Asymptotic (normal approximation to log(HR)).

### Null Model

H0: gender and hazard are independent, conditional on other covariates.

**Test**: log-rank on partial residuals (Schoenfeld test for PH assumption).

p-value interpretation: Probability of observing |HR - 1| as extreme or more extreme under H0.

---

## Output Fields

| Field | Type | Example |
|---|---|---|
| `gender_f_hr` | float | 1.34 |
| `gender_f_se` | float | 0.087 |
| `gender_f_ci_lower` | float | 1.16 |
| `gender_f_ci_upper` | float | 1.55 |
| `gender_f_p_value` | float | 0.0042 |

---

## Known Limitations

### Assumption Violations

- **PH assumption**: Hazard ratio must be constant over time.
  - **Check**: Schoenfeld residual plot (see `kmf.plot_partial_hazard()`).
  - **Risk**: If violated, HR is a time-averaged quantity (may be misleading).

### Coverage & Bias

- **Gender coverage**: ~11.5% of dataset (gender data missing for 88.5%).
  - Effective sample: 10,619 women + 20,560 men = 31,179 / 273,000 total.
  - Risk: Demographic skew if gender-missing persons systematically differ.

- **Observed confounding**: Role trajectory, studio tenure, overseas subcontracting not fully controlled.

### H1 Compliance

anime.score is NOT used in outcome, covariates, or stratification.

### Measurement

**Measured**: Co-occurrence of gender (at debut) and credit visibility loss timing.  
**NOT measured**: Cause of visibility loss, career satisfaction, ability to secure work.

---

## Interpretation Guide

### High HR (gender_f HR = 1.34)

Female-debuted persons co-occur with 34% higher rate of credit-visibility loss 
compared to male-debuted, conditional on cohort, role, and other covariates. 

This describes network structure (opportunity differential), not ability or 
choice differences.

### Do NOT interpret as

- ❌ "Women are 34% less capable" (ability framing)
- ❌ "Women leave the industry 34% more" (causation without mechanism)
- ❌ "Studios discriminate 34% more against women" (causation + intent)

**Alternative interpretation**: Visibility loss may reflect differential access to 
key roles, project continuity, or uncredited work; differences are structural, not 
individual.

---

## References

- Cox, D. R. (1972). Regression models and life-tables. *J. R. Stat. Soc. B*, 34, 187–220.
- Implementation: `src/analysis/causal/gender_progression.py::cox_proportional_hazards()`
- Tests: `tests/analysis/causal/test_gender_progression.py::TestCoxRegression`
```

---

## Appendix: When to Use Each Method Note

| Method | When to use | Typical Report(s) |
|--------|---|---|
| **Cox PH** | Time-to-event with covariates | O1 (gender × visibility loss timing) |
| **Mann-Whitney U** | Compare medians, two groups | O1, O4, O8 (distribution differences) |
| **Kaplan-Meier** | Survival curves, stratified | O1, O2 (career continuity by cohort) |
| **Counterfactual + Bootstrap** | "What if person X absent?" | O3 (IP dependency) |
| **Louvain** | Community detection | O6 (international collab clusters) |
| **Propensity Matching / IPW** | Observational causal adjustment | O5 (education outcome) |
| **DID** | Natural experiment | O7 (policy/event impact on credits) |
| **Weighted PageRank** | Network centrality | O6 (global positioning) |

---

## Version Control

- **v1.0** (2026-05-13): Initial template, 8 method stubs in section_builder.py
- **Future**: Add more method templates as O5/O6 cards implement new techniques

---

**Last updated**: 2026-05-13  
**Maintained by**: Animetor Eval Documentation  
**Related files**: docs/report_cross_cutting.md §6, CLAUDE.md §7

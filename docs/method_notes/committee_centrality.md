# Method Note: 制作委員会 Bipartite Influence Centrality (26-01)

**Status**: implemented
**Module**: `src/analysis/network/committee_centrality.py`
**Report**: `scripts/report_generators/reports/structure_committee.py`
**Hard constraints**: H1 (viewer ratings excluded), H2 (no ability / dominance framing)

---

## Purpose

Production committee membership records form a bipartite graph linking each
anime to its listed investor companies. The projection of this graph onto a
company-only graph captures the structural pattern of co-investment, which
this method summarizes as **eigenvector centrality** (per company) and
**Herfindahl-Hirschman Index** (per period).

The metric measures **co-investment topology**, not individual firms'
"dominance" or "market power". A pre/post 2017 split is a *descriptive*
contrast against the conventional "delivery-platform expansion" reference;
causal claims about platform entry live in card 25-01.

---

## Specification

### Model / Definition

Bipartite graph `G_BP = (V_A ∪ V_C, E)`:

- `V_A` = Resolved-layer anime (canonical_id)
- `V_C` = company name strings from `anime_production_committee`
- `E ⊆ V_A × V_C` with edge weight `w(a, c) = 1 + log1p(episodes(a))`

Company–company projection `G_P`:

- nodes = `V_C` with at least `_MIN_ANIME_PER_COMPANY = 2` anime
- for each anime `a` and each pair `(c_i, c_j)` of companies linked to `a`,
  add `min(w(a, c_i), w(a, c_j))` to the projection edge `(c_i, c_j)`

Eigenvector centrality on `G_P`:

```
A x = λ x      (A = weighted adjacency of G_P)
```

solved via `networkx.eigenvector_centrality_numpy`. Fallbacks (in order):

1. `eigenvector_centrality` (power iteration, `max_iter=1000`, `tol=1e-08`)
2. weighted degree centrality (always succeeds)

HHI:

```
share_i = membership_count(c_i) / Σ membership_count(c_j)
HHI     = Σ share_i² × 10000
```

reported on the industry-standard 0–10000 scale.

| Term | Description | H1 compliance |
|------|-------------|---------------|
| `episodes(a)` | structural anime metadata | yes (no viewer rating) |
| `share_i`     | membership-count fraction | yes |
| `centrality_i`| eigenvector / fallback    | yes |

### Estimation / Computation

Implementation pseudocode:

```python
memberships = load_committee_memberships(conn)
g_bp        = build_bipartite_graph(memberships, min_anime_per_company=2)
g_proj      = project_to_company_graph(g_bp)
centrality, method_note = _eigenvector_or_fallback(g_proj)
hhi_rows    = compute_period_hhi(memberships, boundary_year=2017)
```

**Parameters** (with defaults):

- `boundary_year = 2017` — pre/post split for "delivery-platform expansion"
  descriptive contrast.
- `min_anime_per_company = 2` — drop one-off investors from the projection.
- `_MIN_COMPANIES_FOR_HHI = 10` — minimum company count for HHI emission.
- `_MIN_GROUP_N = 5` — minimum memberships per period.
- `_EIG_MAX_ITER = 1000`, `_EIG_TOLERANCE = 1e-08`.

### Confidence Interval

Not applicable for the population-level structural metrics in this card.
Both centrality and HHI are reported as point estimates with explicit
sample-size context (`n_anime`, `n_companies`, `n_memberships`).

Future work: degree-preserving bipartite null model (configuration model or
Chung-Lu rewiring) for a non-parametric significance band on centrality.

### Null Model / Baseline

Not implemented in card 26-01 (deferred to follow-up). The pre/post HHI
contrast is descriptive only — *no causal-cutoff inference is asserted*.

---

## Output Fields

`CommitteeCentralityResult` (dataclass):

| Field | Type | Description |
|---|---|---|
| `memberships` | `list[CommitteeMembership]` | (anime, company, year, episodes) tuples loaded from DB |
| `centralities` | `list[CentralityRow]` | per-period eigenvector centrality |
| `period_hhi` | `list[PeriodHHI]` | per-period HHI, top-10 share, market sizes |
| `boundary_year` | `int` | period split year (default 2017) |
| `n_unique_companies` | `int` | total distinct company strings |
| `n_unique_anime` | `int` | total distinct resolved anime |
| `coverage_note` | `str` | human-readable coverage summary |
| `low_coverage_warning` | `bool` | True if sample size is below thresholds |
| `centrality_note` | `str` | which centrality method actually succeeded |

---

## Known Limitations

### Coverage

- Committee credit coverage maps roughly 5.7k anime out of 333k resolved
  anime (~1.7%). Recent TV series are over-represented; films and older
  works are under-represented.
- `anime_production_committee` is sourced from seesaawiki and madb only;
  AniList / ANN do not encode investor-side information.

### Entity Resolution

- Investor company strings are **not** entity-resolved in this scope.
  Variations such as "株式会社X" vs "X 株式会社" vs "X" count as distinct
  nodes, deflating individual companies' centrality and HHI share.
- This is a known follow-up (see TASK_CARDS/26_industry_structure/01).

### Period Boundary

- 2017 is a *conventional* reference for the delivery-platform expansion;
  it is *not* a causal cutoff. Reading the pre/post HHI delta as a Netflix
  *effect* is invalid — the event-study design lives in card 25-01.

### H1 Compliance (viewer-rating exclusion)

- The viewer-rating column is **not** read in the analysis module.
  Edge weighting uses `episodes` only.
- Verified by `pixi run python scripts/report_generators/lint_vocab.py`
  and a unit test (`test_no_anime_score_in_analysis`).

### Measurement Specificity

- **Measured**: Co-investment topology of committee-listed companies on
  anime where such records are available.
- **NOT measured**: Decision-making influence, IP control, negotiation
  power, market dominance.

---

## Interpretation Guide

### What a High Centrality Means

The company co-invests with many other high-centrality companies, i.e. it
occupies a position with dense second-order connectivity in the observed
co-investment network.

✅ "Company X is a structural hub in the observed co-investment graph."
❌ "Company X dominates / controls the market." (forbidden framing)

### What a High HHI Means

A small number of companies account for a large share of total committee
memberships *in the recorded sample*. This is a structural statement
about the credit record, not about market power.

✅ "Committee membership shares are more concentrated in 2018-2022."
❌ "Big firms have captured the industry." (causal + evaluative)

### Do NOT Interpret As

- ❌ "Market dominance" / "monopoly"
- ❌ "Decision-making power"
- ❌ "Bargaining capability"
- ❌ Causal effect of Netflix's market entry (use card 25-01 instead)

### When to Question This Metric

- Pre-2010 anime have especially sparse committee records → centrality and
  HHI in the pre period reflect partial sampling.
- A single "parent ↔ subsidiary" entity resolution would compress several
  nodes into one and could materially change the ranking.

---

## References

### Code & Tests

- **Implementation**: `src/analysis/network/committee_centrality.py`
- **Tests**: `tests/analysis/network/test_committee_centrality.py`
- **Report**: `scripts/report_generators/reports/structure_committee.py`

### Related Method Notes

- DiD studio transfer (card 25-01) — causal complement to the descriptive
  pre/post HHI contrast.
- International collaboration edge structure (card 26-02) — sibling
  bipartite/Louvain analysis applied to person × studio cross-border ties.

---

## Version Control

- **v1.0** (2026-05-15): Initial implementation (TASK_CARDS/26-01).

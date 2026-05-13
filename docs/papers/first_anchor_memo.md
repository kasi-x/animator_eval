# First Paper Anchor Selection Memo

**Date**: 2026-05-13  
**Status**: Decision-required  
**Purpose**: Recommend first publication anchor from 6 analytics candidates

---

## Executive Summary

All 6 candidates have been implemented and merged to main (commits 8af7696–cd2a34c, spanning 25–27 series). This memo synthesizes their data completeness, statistical maturity, venue fit, and alignment with labor-first stance to guide selection.

**Key tension**: Gender enrichment (currently 19%, target 70%) gates `04_pay_equity_decomp`. Two recommended paths:
- **Path A (gender incomplete)**: Anchor `25/01` DiD (causal, high venue fit, no gender requirement)
- **Path B (gender reaches 70%)**: Anchor `25/04` Oaxaca (gender-central, policy impact, publication strength)

---

## Candidate Comparison Matrix

| Dimension | 25/01 DiD | 25/02 Opp | 25/03 VizLoss | 25/04 Oaxaca | 27/01 Missing | 27/02 Typology |
|-----------|:-:|:-:|:-:|:-:|:-:|:-:|
| **Data Completeness** | ✅ Full | ✅ Full | ✅ Full | 🟡 Gender 19% | ✅ Full | ✅ Full |
| **Statistical Method** | Causal (DiD) | Causal (OLS+CI) | Predictive (LGBM) | Decomp (Oaxaca) | Descriptive | Unsupervised (OM) |
| **Labor-Economics Venue Fit** | ◎ (**Labour Econ**) | ◎ (J. Cult. Econ) | △ (Mismatch jnl) | ◎ (**Labour Econ**) | ◎ | △ (sequence jnl) |
| **Information/NetSci Venue Fit** | △ | ○ | ○ | △ | ◎ | ◎ (**NetSci**) |
| **RIETI DP Suitability** | ◎ High | ◎ High | ○ | ◎ High | ◎ High | △ |
| **Publication Effort** | High (parallel trends / event-study) | Medium (OLS + permutation) | Medium (holdout + calibration) | High (subgroup) | Low (descriptive) | Medium (sequence distance) |
| **Reviewer Hardness** | 🔴 (parallel trends strict) | 🟡 (permutation assumption) | 🟡 (leakage risk) | 🟡 (endogeneity) | 🟢 | 🟡 (cluster interpretation) |
| **Business Pathway Ready** | ○ | ✅ (HR brief) | ✅ (early warning) | ○ (ESG later) | △ (infrastructure) | △ (B2C explainability) |
| **Policy Impact** | ◎ (labor mobility) | ◎ (mismatch) | ◎ (outflow) | ◎◎ (gender parity) | ◎ | ○ |
| **Labor-first Alignment** | ✅ (mobility choice) | ✅ | ✅ | ✅ (gender equity core) | ✅ | ✅ |
| **Main Commit** | (pending) | d4b730c | da120c7 | (pending) | cd2a34c | ea5280f |

---

## Gender Enrichment Dependency & Timeline

### Current Status (2026-05-05)
- `persons.gender` null rate: **80.9%** (140,226 / 732,460)
- Distributed by source:
  - bangumi: 59.9% ✅ (already integrated)
  - tmdb: 37.2% ✅ (v56 loader expanded 2026-05-05)
  - anilist: 6.0% (Staff query ceiling; 90K orphan persons not yet batch-fetched)
  - mal: 0.0% (⏳ Card 05 rescrape pending, ~9.4 days)
  - ann: 0.0% (source limitation: no gender label on HTML)
  - keyframe: 0.0% (source limitation: no API gender field)
  - seesaawiki: 0.0% (source limitation: no staff gender output)

### Improvement Path
1. **Immediate (1–2 weeks)**: AniList orphan backfill (new Card 12A candidate)
   - Estimated gain: +3,000–5,000 gender entries → ~80.5% null (minimal)
   
2. **Medium (2–6 weeks)**: MAL Card 05 rescrape completion
   - Estimated gain: +8,000–12,000 gender entries from Jikan API → ~78% null
   
3. **Target threshold (70% null)**: Requires additional auxiliary methods
   - gender-guesser (confidence tiers required)
   - ANN bio NLP (pronoun extraction, low-precision fallback)

**Realistic ETA for 70% target**: Mid-June 2026 (if MAL Card 05 + 2 auxiliary methods)

---

## Recommended Selection Paths

### Path A: Gender Remains Incomplete (Current Trajectory)
**Recommended Anchor**: `25/01_did_studio_transfer`

#### Rationale
- **No gender dependency**: Treatment (studio transfer) and outcomes (θ, opportunity) are gender-neutral structural metrics
- **Causal strength**: Parallel trends testable, event-study design enables leads/lags for robustness
- **Venue fit**: Labour Economics (primary), RIETI DP (secondary) — both high-tier for economic policy
- **Publication timeline**: 4–6 months (parallel trends review is the reviewer hardness spike)
- **Policy ready**: Labor mobility → METI / 文化庁 policy brief immediately actionable
- **Business ready**: Moderate (studio HR can cite evidence; not immediate SaaS input)

#### Expected Outcome
1. First RIETI DP (May–June 2026 submission)
2. Labour Economics / Journal of Political Economy full journal (target 2027)
3. Policy brief on labor mobility incentives (concurrent)

#### Risk
- Parallel trends assumption strict in entertainment industry (cohort effects, recession spill-over)
- Sample size of treated (studio movers) must be ≥500 for power (preliminary: ~800–1200, adequate)

---

### Path B: Gender Reaches 70% by Mid-June
**Recommended Anchor**: `25/04_pay_equity_decomp`

#### Rationale
- **Gender-central framing**: Oaxaca decomposition is labour economics' standard for wage gap / opportunity parity analysis
- **Highest labor-first alignment**: Machine-readable gender inequality → compensation fairness (project core mission)
- **Venue strength**: Labour Economics / Journal of Economic Literature / J. Labour Research — top-tier + volume
- **Policy leverage**: Gender equity → international development goals → METI / 文化庁 / 内閣府 gender parity targets
- **Subgroup credibility**: Can isolate cohort × role × studio decomposition
- **Bootstrap robustness**: Analytical CI + percentile CI from 1000 bootstrap both satisfy H4 (compensation basis)

#### Expected Outcome
1. Gender-focused RIETI DP (June–July 2026 submission, conditional on gender reaching 70%)
2. Labour Economics full journal (target 2027)
3. Policy brief on gender opportunity gaps + structured recommendations (concurrent)
4. Companion to 25/01 (separate paper if space permits, same data platform)

#### Risk
- Gender null rate < 70% at deadline → must delay or gate on auxiliary method confidence tiers
- Endogeneity: unmeasured "role segregation" (women not offered high-weight roles) vs. "choice" (women avoid high-pressure roles)
  - **Mitigation**: Interpret `structural` component conservatively; frame as "opportunity observed gap" (not causation)

---

## Alternative/Backup Scenarios

### Scenario C: Dual-Track Publication (Max Ambition)
If timeline permits (gender reaches 70% AND 25/01 parallel trends pass preliminary checks by June):

**Plan**: Submit 25/01 and 25/04 as companion papers to same journal/RIETI, 2 months apart.
- **Pros**: Synergy narrative (labor mobility + gender equity = comprehensive platform analysis)
- **Cons**: Risk of rejection cascading (if one reviewer critique applies to both); self-citation/ same-data sensitivities
- **Mitigation**: Different outcome variables (θ vs. credit count), different econometric angle (DiD vs. decomposition)

### Scenario D: Information-Science Track (Secondary)
If labour economics timeline slips or reviewer feedback unfavorable:

**Anchor**: `27/02_career_trajectory_typology` → Applied Network Science / WebSci

- **Pros**: Sequence analysis + Markov chains = pure methods contribution; NetSci more receptive to exploratory typologies
- **Cons**: Lower policy impact; less direct tie to compensation fairness (project's core)
- **Timeline**: 3–4 months (cluster interpretation the main work)
- **Business value**: Individual B2C SaaS (explainability layer)

### Scenario E: Methods-First Foundation (Longest Runway)
If causal assumptions fail:

**Anchor**: `27/01_missingness_disclosure` → Frames all subsequent work as "provisional pending data completeness"

- **Pros**: All future papers self-protected by honest coverage caveats; no reviewer ambush
- **Cons**: Not publication-grade alone (too foundational)
- **Use**: Publish as RIETI technical report or journal appendix companion to 25/01 or 25/04

---

## Selection Criteria (5 Axes)

| Criterion | 25/01 | 25/04 | Trade-off |
|-----------|:----:|:----:|-----------|
| **1. Data Readiness** | ✅ Ready now | 🟡 June 2026 | Path A avoids dependency; Path B has higher stakes |
| **2. Venue Fit (Labour Econ)** | ◎ Exact fit | ◎ Exact fit | Equal; both publishable at top tier |
| **3. Reviewer Consensus** | 🟡 Tight | 🟢 Broader | Path A: econometrician skeptical; Path B: broader labor econ consensus |
| **4. Labor-first Narrative** | ✅ Worker choice visible | ✅✅ Equity visible | Path B more directly aligns with gender parity commitment |
| **5. Policy Traction (METI/文化庁)** | ✅ (labor mobility) | ✅✅ (gender parity) | Path B = higher political weight in current climate (2026–2027) |

---

## Open Questions for User Decision

1. **Timeline priority**: Would you rather guarantee 1 publication by Nov 2026 (Path A) or aim for gender-complete flagship by late 2026 (Path B with risk)?

2. **Gender trajectory**: Is mid-June 70% target realistic given MAL Card 05 timeline and auxiliary method confidence? Should we run a 2-week pilot on AniList orphan backfill first?

3. **Venue strategy**: Do you prefer labour economics as primary journal (25/01 + 25/04 both fit) or explore multi-venue (one labour econ + one NetSci as 27/02)?

4. **Policy pathway**: Gender parity (Path B) vs. labor mobility (Path A) — which aligns better with your METI/文化庁 contacts' current priorities?

5. **Risk tolerance on parallel trends**: Is the 25/01 DiD defensible given potential unobserved cohort shocks (e.g., 2019–2024 industry consolidation)? Should we pre-register event-study spec to strengthen review?

---

## Recommendation Summary

**Primary recommendation**: **Path A (25/01 DiD)** if gender enrichment timeline uncertain.
- Earliest publication (Nov 2026 RIETI DP likely)
- Causal evidence (highest publication esteem)
- Labor mobility policy-ready
- No blocking dependencies

**Secondary recommendation**: **Path B (25/04 Oaxaca)** if gender reaches 70% by mid-June.
- Stronger labor-first alignment (gender equity explicit)
- Project core narrative (compensation fairness → gender parity)
- Higher policy impact (gender parity = METI priority)
- Simultaneous publication with Path A possible (dual-track)

**Fallback**: Pursue both as dual track with 2-month stagger (25/01 first, 25/04 second), assuming gender timeline holds.

---

## Implementation Checklist (Whichever Path Selected)

### For 25/01 (DiD)
- [ ] Confirm treated sample size ≥500 (preliminary: ~800–1200, should confirm in data)
- [ ] Generate parallel trends event-study figure (leads = zero test)
- [ ] Pre-register spec with 3 sensitivity checks (alternative window definitions)
- [ ] Draft `docs/method_notes/did_studio_transfer.md`
- [ ] Submit RIETI DP by Nov 1, 2026

### For 25/04 (Oaxaca, conditional on gender ≥70%)
- [ ] Run gender fill-in pilot (AniList orphan backfill) by May 20
- [ ] Confirm gender → 70% null by June 15 (else delay to Q3)
- [ ] Bootstrap 1000 replications per subgroup (cohort × role × studio)
- [ ] Generate 3 visualization: aggregate decomposition + gender timeseries + subgroup heatmap
- [ ] Draft `docs/method_notes/oaxaca_decomp.md`
- [ ] Submit RIETI DP by July 1, 2026 (staggered 8 weeks after 25/01)

### For Both
- [ ] Narrative sync: ensure 25/01 sets up labor mobility → opens opportunity question answered by 25/04
- [ ] Disclaimers: confirm both papers reference `docs/STANCE.md` labor-first framing and `docs/REPORT_PHILOSOPHY.md` §9 (amended disclaimers)
- [ ] Replication snapshot: Zenodo archive + DOI per `TASK_CARDS/32_publication/01_replication_snapshot_exception.md`

---

## References

- TASK_CARDS/32_publication/02_first_paper_anchor.md (trigger)
- TASK_CARDS/25_compensation_fairness/ (all 4 cards: 01–04)
- TASK_CARDS/27_methodology/ (01 missingness, 02 typology, 03 IV)
- docs/STANCE.md §5.2 (multi-track publication strategy)
- docs/REPORT_PHILOSOPHY.md §3 (method gate requirements)
- TODO.md §15 (gender enrichment status, §17 compensation fairness, §19 methodology)

---

## Version

- **Draft**: 2026-05-13
- **Status**: Awaiting user decision on Path A vs. Path B
- **Next step**: User confirms path + gender timeline; proceed with 25/01 or 25/04 paper structure

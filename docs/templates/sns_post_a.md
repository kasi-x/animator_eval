# Template: Tier A (Weekly Snippet — X Native)

**Purpose**: Quick, data-forward post (280 char max) with 1 figure. Drive weekly visibility and traffic to full analysis.

**Time to produce**: 15–20 minutes  
**Platform**: X (formerly Twitter)  
**Frequency**: 1–2 per week  

---

## Structure

```
[Finding in narrowest language] | [Figure ID]
[Cohort + time window + caveat]
[Source/link label]

[Hashtags, ~2–3]
```

## Character Budget (280 max)

- **Line 1** (finding + figure ref): 120 chars
- **Line 2** (cohort/time/caveat): 80 chars
- **Line 3** (source/link): 30 chars
- **Hashtags**: 50 chars
- **Total**: ~280 chars

---

## Template (Blank Fill-in)

```
[YEAR range] [COHORT/GROUP], [METRIC] shifted [DIRECTION] [PERCENT]% 
(N=[sample], 95% CI: [bounds]). [Structural observation, not evaluation].

Data: [source abbreviation] | Method: [link]

#アニメ制作 #業界データ #[optional_topical_tag]
```

---

## Filled-In Examples

### Example 1: Network Density

```
Studio co-credit partners: 2018 avg 5.2 → 2025 avg 3.1 partners/year 
(TV anime, N=450, CI: 2.8–3.4). Production budget scaling, not selection.

Data: Q2 2025 analysis | Method: link-to-methodology

#アニメ制作 #業界データ #労働環境
```

**Breakdown**:
- Finding: "partners ... 5.2 → 3.1"
- Caveat: "Production budget scaling, not selection" (avoids ability framing)
- Cohort: "TV anime" + "N=450"
- Confidence interval: "[2.8–3.4]"
- No evaluative adjectives

---

### Example 2: Gender Cohort

```
Early-career women animators (debut 2015–2020): avg 3 co-studios/year. 
Men same cohort: 4.1 studios (95% CI: 3.7–4.5). Structural segregation 
by role (In-between concentration). Not ability.

Data: Cohort analysis Q2 | Method: [link]

#アニメ制作 #ジェンダー #構造観察
```

**Breakdown**:
- Metric comparison: "3 vs 4.1 studios"
- Caveat: "Structural segregation by role. Not ability." (explicit framing)
- Cohort: "Early-career 2015–2020"
- CI: "[3.7–4.5]"

---

### Example 3: Policy Angle

```
Studio consolidation: in 2023, 67% of TV anime produced by top-5 studios 
(vs 52% in 2015). Production concentration gini: 0.56→0.72. 
Potential labor bargaining implications.

Data: Studio concentration Q1 | Method: [link]

#アニメ制作 #政策 #業界構造
```

**Breakdown**:
- Finding: "67% → top-5 studios"
- Time window: "2023 vs 2015"
- Metric: "gini 0.56 → 0.72"
- No causation claimed ("potential... implications" is hedged)

---

### Example 4: Methodological Note

```
Merged 3 new data sources (AniList entity resolution, cross-validation). 
Person-level precision ↑ to 94% (prev 87%). Updated all historical scores.

Data: April release | Method: [link]

#アニメ制作 #データ透明性 #アップデート
```

**Breakdown**:
- Transparency: "Merged 3 new sources"
- Quality metric: "Precision ↑ to 94%"
- Clear: This is a method upgrade, not a finding

---

## Anti-Patterns: What NOT to Do

### ✗ BAD Example 1: Evaluative

```
Studio A is AMAZING at developing talent — 5.2 co-studios/year avg!

Data: [link]

#才能 #スター選手
```

**Problems**:
- "AMAZING" = evaluative adjective (forbidden)
- "developing talent" = ability framing
- "#才能" = forbidden vocab
- No caveat, cohort, CI, or time window

---

### ✗ BAD Example 2: Causal Claim Without Method

```
Budget cuts are destroying animator collaboration. 
That's why younger animators can't build networks.

Data: [link]

#アニメ業界 #失業
```

**Problems**:
- Causal claim ("destroying") without named ID strategy
- Ability framing ("can't build networks")
- No cohort, CI, time window, or N
- Implies evaluation ("younger animators" suffer, unstated)

---

### ✗ BAD Example 3: Ranking

```
Top 10 best-connected animators (2025):
1. [Name] — 8.7 studio partners
2. [Name] — 8.3 partners
...

Best networks win!
```

**Problems**:
- Explicit ranking framing (forbidden)
- Implies individual "worth" by rank
- No consent for named individuals
- Evaluative ("Best networks win")
- No caveat about what network position means

---

## Checklist Before Publishing

```
□ Finding in narrowest language? (not evaluative)
□ Cohort specified? (e.g., "TV anime," "early-career women")
□ Time window clear? (e.g., "2015–2025")
□ N (sample size) included?
□ 95% CI shown (if individual/studio named)?
□ No forbidden vocab? (lint-check: pixi run lint-vocab < post.txt)
□ No causal claim without ID strategy named?
□ No ranking language ("top," "best," "worst")?
□ No ability framing ("talented," "能力," "skilled")?
□ Figure has caption + axes labeled?
□ Link to methodology? (not just "link to data")
□ Hashtags chosen? (2–3 relevant, no forbidden)
□ Under 280 chars (including figure reference)?
```

---

## How to Adapt This Template

1. **Pick your finding**: From completed analysis module, report section, or policy event.
2. **Name cohort + time window**: e.g., "Studio A, TV anime, 2018–2025"
3. **State metric narrowly**: "co-studio partners," not "collaboration" or "network strength"
4. **Add caveat**: "Production budget scaling" or "Role-based segregation" (never "ability")
5. **Insert CI**: "[X–Y]" for 95% confidence interval
6. **Link to full analysis**: Make it easy to verify
7. **Lint vocabulary**: `pixi run lint-vocab < post.txt`
8. **Review tone**: Would a union organizer feel this respects their members? Would a policy staffer find this credible?
9. **Publish & track**: Note which posts drive replies, clicks, or retweets

---

## Image Guidelines for Tier A

- **Size**: 1,200 × 600 px (landscape)
- **Format**: PNG or JPG, <500 KB
- **Annotation**: Title, axes, units, time window, N
- **Color**: Use accessible palette (ColorBrewer, high contrast)
- **Alt-text** (required for accessibility): "Bar chart showing studio co-credit partners declining from 5.2 (2018) to 3.1 (2025) for 450 TV anime titles."

**Example annotation**:
```
Studio Co-Credit Partners (TV anime)
2018–2025 | N=450 | Avg count

[Y-axis label: Partners/year]
[X-axis label: Year]
[Data line declining left-to-right]

95% CI shown as band around mean.
```

---

## Cadence Reminder

- **Post day**: Wednesday 7 am JST (morning visibility, before journalist reads)
- **Engagement window**: 24–72 hours (replies, retweets)
- **Link to full report**: In follow-up reply or in source link
- **Archive**: Copy post + figure into monthly note document (backup for X ephemerality)

---

**Template version**: 1.0 (2026-05-13)  
**Related**: `docs/sns_operations.md` §2.1, §3.2 (tone rules), `forbidden_vocab.yaml`

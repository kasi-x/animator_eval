# SNS Operations — Animetor Eval Stakeholder Outreach

**Version**: 1.0  
**Last updated**: 2026-05-13  
**Audience**: Core project operator, partner communicators  
**Related docs**: `STANCE.md` (§8), `REPORT_PHILOSOPHY.md`, `forbidden_vocab.yaml`

---

## 0. Context

Animetor Eval publishes via academic conferences and peer-reviewed journals (publication 59% weight per `STANCE.md`). However, publication cycles are slow (12–24 months from submission to acceptance). SNS channels supplement this gap to maintain visibility with stakeholders:

- **Individual animators** (potential B2C users)
- **Industry journalists / bloggers** (amplification, citation)
- **Policy stakeholders** (METI, Cultural Affairs, labor unions)
- **Academic researchers** (cross-disciplinary collaboration)

This document specifies platform selection, content tier design, labor-first tone rules, posting schedule, and crisis communication.

---

## 1. Platform Selection & Strategy

### 1.1 Primary: X (formerly Twitter)

**Rationale**: Industry journalists, policy staffers, and individual animators use X as the primary venue for industry news and professional networking. Largest reach for labor-first framing.

**Cadence**: Weekly primary posting + daily retweets / replies  
**Character limit**: 280 characters + 1 image per post (thread for deeper dives)  
**Content type**: Weekly data snippet (Tier A) + industry trend threads (Tier B)  

**Tactical**:
- Follow JAniCA, major labor-focused accounts, economics journalists, policy think tanks
- Reply to industry coverage with data-backed perspective (without self-promotion)
- Link to detailed analysis (note articles, full reports)
- Use relevant hashtags: #アニメ業界 #アニメーター #労働環境 #アニメ制作 #業界データ

### 1.2 Secondary: note (ノート)

**Rationale**: Reach readers seeking longer-form analysis; archive for persistent visibility (X posts disappear into feed); establish thought leadership with industry.

**Cadence**: Monthly 1,500–3,000 character articles (Tier C) + quarterly 5,000–8,000 character essays (Tier D)  
**Content type**: Industry observation, data interpretation, policy argument  

**Tactical**:
- Cross-link from X (X post → note link)
- Use note's "recommended" section to reach broader audience
- Enable comments for feedback from animators, journalists
- Archive all SNS content here as backup

### 1.3 Tertiary: LinkedIn

**Rationale**: Reach international researchers, multinational studios with Japan operations, policy consultants.

**Cadence**: Monthly repost of note articles + quarterly English-language essays  
**Content type**: Structured findings, methodology, English-language publication announcements  

**Tactical**:
- Target academics, policy researchers, management consultants in animation / media
- Use LinkedIn's "article" feature for long-form
- Link to peer-reviewed publications and preprints
- Maintain professional tone (avoid labor activism framing in English context)

### 1.4 Experimental: Bluesky

**Rationale**: Emerging platform with growing academic / tech audience; potential future replacement for X if platform risk materializes.

**Cadence**: Repost X content once daily (low maintenance)  
**Content type**: Copy of X posts + reference to detailed analysis  

**Tactical**:
- Set up account early to claim handle
- Low-maintenance reposting; do not develop unique Bluesky strategy yet
- Monitor engagement; upgrade strategy if community grows

---

## 2. Content Tier Architecture (A–D)

### 2.1 Tier A: Weekly Snippet (X primary)

**Frequency**: 1–2 per week  
**Length**: 150–280 characters (X native)  
**Format**: 1 data point + 1 figure + source attribution  
**Time to produce**: 15–20 minutes  

**Purpose**: Maintain weekly visibility, drive traffic to reports, generate discussion.

**Structure**:
```
[Data claim in narrowest language] | [figure/chart]
[Year range / cohort / method caveat]
Source: [brief link]
```

**Example**:
```
In Studio A (2018–2025), median credit visibility dropped 27% YoY for TV anime → 35% for movies. Network density effect, not selection. Structural shift visualized in [link] #アニメ制作
```

**Content sources**:
- Latest industry report findings
- Quarterly pipeline results
- Policy-relevant observations (gender gap, studio concentration, etc.)
- Methodology notes (e.g., "New data source integrated, retroactive recomputation")

**Tone**:
- Neutral, fact-forward (no evaluative adjectives)
- Narrow label: "credit visibility loss" not "unemployment"
- Always include CI or cohort position if a person/studio is named
- Avoid "best," "worst," "brilliant," "struggling"

---

### 2.2 Tier B: Bi-weekly Deep-Dive (X threads)

**Frequency**: 2–3 per month  
**Format**: X thread (4–8 posts) or single long-form post with breaks  
**Length**: ~1,500 characters across thread  
**Time to produce**: 30–45 minutes  

**Purpose**: Build narrative around findings; reach readers who follow threads for substance.

**Structure**:
```
[1] Hook: narrow framing of phenomenon
[2–4] Data evidence (1 figure per post, with annotation)
[5–6] Interpretation/implications (labeled explicitly as opinion)
[7] Link to full analysis / methodology
[8] Invite discussion / feedback
```

**Example thread**:
```
[1] "3 years of studio co-credit data reveal: who works w/ whom is becoming MORE concentrated, not less. Why?"

[2] "[CHART] Network density among top 30 studios: gini coeff 0.63 → 0.71 (2020–2023). Edges increasingly clustered."

[3] "This is NOT director preference. Same directors work w/ different studios in different years. What changed?"

[4] "[CHART] Production budgets show: projects w/ >10 studios historically averaged 5. Now? avg 2.8. Consolidation."

[5] "Interpretation: tighter budgets + streaming schedules force smaller crew rosters. Labor consequence: fewer 'free' collaborators."

[6] "[CHART] For animators w/ 3+ studio relationships: collab partners↓44%. Those w/ 1 studio: +12%."

[7] "Full analysis: [link]. Methodology: [link]."

[8] "Feedback welcome — am I reading this right? Which studios or roles see different patterns?"
```

**Content sources**:
- Data from completed analysis modules (collaboration, network, studio concentration)
- Structured response to industry event (policy change, strike, funding announcement)
- Comparison studies (gender, cohort, regional)

**Tone**:
- Lead with data, not assumption
- Always name the identification strategy when making causal claims ("This is NOT X because we see Y in Z context")
- Label "Interpretation" section explicitly
- Invite disagreement / alternative readings

---

### 2.3 Tier C: Monthly Long-Form Article (note primary)

**Frequency**: 1 per month  
**Length**: 1,500–3,000 characters  
**Format**: note markdown (headings, inline formatting, 1–2 embedded figures)  
**Time to produce**: 60–90 minutes  

**Purpose**: Establish thought leadership; create persistent reference for journalists, policymakers; build SEO for "アニメ業界" + related keywords.

**Structure** (template in `docs/templates/sns_post_c.md`):
```
# Title: [Specific phenomenon + Data-backed claim]

## Problem (50–100 words)
[Phenomenon + scope + why it matters to animators/studios]

## Evidence (300–500 words + 1–2 figures)
[Finding 1 + Figure 1 with annotation]
[Finding 2 + data citation]
[Finding 3 + context]

## Interpretation: Labor Consequences (200–300 words)
[Explicitly labeled first-person interpretation]
[What this means for different cohorts: early-career, mid-career, management]
[Explicit caveat: "This is structural observation, not ability-based ranking"]

## What Next? (100–150 words)
[3–4 possible responses: by studios, by labor unions, by policy]
[No recommendation, just framing]

## Sources & Notes
[Full citations, methodology links, data transparency]
```

**Example article**:

**Title**: "アニメ制作の『共クレジット関係』はなぜ縮小しているか — 4年の業界ネットワーク解析"

- Problem: Historically, animators worked with 5–8 different studios per year. That number dropped to 2–3. Why?
- Evidence: [Graph 1] Raw collab edges. [Graph 2] Network density gini. [Graph 3] Budget trajectory.
- Interpretation: Budget pressure + streaming schedules. First-person: "I interpret this as..." Not "animators are failing to network."
- What Next: Studios could; unions could; policy could.

**Content sources**:
- Monthly report findings that deserve narrative context
- Response to industry event (funding crises, strikes, policy proposals)
- Long-term trend analysis (decade+ perspective)
- Methodology explainer (entity resolution, what we measure / don't)

**Tone**:
- Narrative + data hybrid
- Use first-person explicitly in Interpretation section
- Heavy on context: Why this metric matters + for whom
- Avoid ability framing; frame as structural (budget, network topology, temporal)

---

### 2.4 Tier D: Quarterly Essay (note primary + LinkedIn secondary)

**Frequency**: 1 per quarter (3–4 per year)  
**Length**: 5,000–8,000 characters  
**Format**: note long-form article (polish, cite, embed figures)  
**Time to produce**: 2–3 hours  

**Purpose**: Establish synthesis voice; connect multiple findings to larger argument; reach policy / academic audiences.

**Structure** (template outline):
```
# Title: [Systemic Argument] — [Data-Backed Evidence]

## 1. Introduction: The Question (300–500 words)
[What structural change is happening?]
[Why does it matter?]
[Who is affected?]

## 2. Evidence: Three Angles (1,000–1,500 words + 3 figures)
[Finding A: Network perspective]
[Finding B: Temporal perspective]
[Finding C: Cohort/role perspective]
[All with explicit caveats]

## 3. Interpretation & Implications (800–1,200 words)
[Multiple plausible explanations with labels]
[Who benefits? Who bears cost?]
[Institutional & market mechanisms]
[Explicitly: "This is my reading. Alternative readings include X, Y."]

## 4. What's at Stake? (300–400 words)
[Implications for labor market, bargaining power, career stability]
[Implications for creative output]
[Policy lever points]

## 5. Closing: Next Steps (200–300 words)
[Questions for further research]
[What would change our reading?]
[Calls to action (without directives): "Studios could...", "Unions might...", "Policymakers could...]

## Sources, Methods, Caveats (500+ words)
[Full methodology section]
[Data sources, limitations, entity resolution transparency]
[How findings differ if we change assumptions X, Y, Z]
```

**Example essay**:

**Title**: "アニメ業界の『協業ネットワーク』はなぜ崩壊しているのか — 構造的要因と労働市場への波及"

- Introduction: Describe the collapse (edge count, density, cohort isolation).
- Evidence: Network metrics + temporal cohort analysis + role-specific patterns.
- Interpretation: Budget hypotheses, scheduling pressure, studio consolidation. Explicit: "These are correlational readings; I do not claim causation."
- At Stake: Career mobility, early-career mentorship loss, studio monopsony power, creative risk-taking.
- Next Steps: What would prove/disprove each hypothesis?

**Content sources**:
- Accumulated findings from 3–6 months of analysis work
- Response to major industry event (acquisition, strike, policy proposal)
- Synthesis of multiple reports + industry context
- Publication pre-announcement (build readership before formal publication)

**Tone**:
- Intellectual, unafraid to stake a position
- Lead with "I interpret..." not "The data shows..."
- Extensive caveat section (longer than typical article format)
- Scholarly tone without jargon; accessible to policymakers + educated public

---

## 3. Labor-First Tone Rules

All content (Tiers A–D) must comply with the following:

### 3.1 Forbidden Vocabulary

**HARD BLOCKS** (no exceptions):

| Category | Forbidden Examples | Replacement |
|----------|-------------------|-------------|
| **Ability framing** | 能力 / talent / skill level / 優秀 / 劣る | network position / credit density / co-worker diversity |
| **Causal claims (no method)** | 引き起こす / cause / lead to / のせいで | correlates with / occurs alongside / co-occurs with |
| **Evaluative adjectives** | 素晴らしい / excellent / terrible / 見事な | percentile / quartile / structured count |
| **Ranking framing** | ランキング / top 10 / 1 位 / A-list | cohort position / percentile band / distribution |
| **Hiring framing** | 採用すべき / should be hired / 推薦できる | network diversity / production scale / structural position |

**Detection**: Use `pixi run lint-vocab` to check posts before publishing. See `scripts/report_generators/lint_vocab.py`.

### 3.2 Who You Name, How You Name Them

**Rule 1: Never rank individuals or studios.**
- FORBIDDEN: "Top 5 animators by centrality"
- ALLOWED: "Animators in the 90th percentile of network centrality (95% CI [88–92%]) tend to..."

**Rule 2: If naming a specific person, you must have consent or they must be historical.**
- FORBIDDEN: "[Living animator name] shows low collaboration diversity this year — needs to network more."
- ALLOWED (with consent): "[Animator name] was interviewed about their collaboration strategy; they emphasized..."
- ALLOWED (historical): "[Deceased director name]'s studio operated with an unusual co-credit pattern..."

**Rule 3: If naming a studio, contextualize as structural choice, not performance.**
- FORBIDDEN: "Studio A is better at developing talent than Studio B."
- ALLOWED: "Studio A has higher co-credit diversity (avg 7.2 partners/year) vs. Studio B (avg 4.1). This could reflect [3 possible mechanisms]."

**Rule 4: Structural claims require transparent method.**
- FORBIDDEN: "The 2023 drop in animator mobility means the industry is consolidating."
- ALLOWED: "In our network analysis (entity resolution: [method], time window: [2020–2023]), we observe a 34% drop in inter-studio collab edges. This is consistent with (a) consolidation, (b) budget pressure, (c) streaming format constraints. We cannot distinguish without [additional data/method]."

### 3.3 Caveat Insertion Rules

**For individual/studio names**: Always add confidence interval or cohort context.

```
[Name] is in the [Xth percentile] for [metric] 
among [cohort] (95% CI: [bounds]), 
[source/year range].
```

**For causal claims**: Always name the identification strategy or downgrade to correlation.

```
We observe [X] and [Y] co-occur [in context Z]. 
This is consistent with causal mechanisms [A], [B], [C], 
but we do not claim causation without [method].
```

**For policy recommendations**: Always present alternatives.

```
One response would be [policy A]. 
Alternatively, [policy B]. 
We take no position, but note [distributional consequence].
```

### 3.4 Tone Markers (Words That Help)

**"I interpret..."** (explicitly signal opinion)  
**"We observe..."** (stick to evidence when not interpreting)  
**"This is consistent with..."** (avoid false certainty on mechanism)  
**"The data do not settle..."** (mark open questions)  
**"Alternative reading..."** (show you've considered other views)  

---

## 4. Posting Schedule & Workflow

### 4.1 Weekly Cycle (X-focused)

| Day | Task | Time | Platform |
|-----|------|------|----------|
| **Mon** | Identify 1 Tier A snippet from last week's work or reports | 15 min | Plan in notes |
| **Wed** | Publish Tier A (morning Japan time, ~7 am JST for visibility) | 10 min | X |
| **Fri** | Engage: reply to comments, retweet relevant industry posts | 15 min | X |
| **Sun** | Plan next week's Tier A & look ahead for Tier B timing | 10 min | Notes |

### 4.2 Bi-Weekly Cycle (Tier B — threads)

| Cadence | Task | Trigger |
|---------|------|---------|
| **Every 2 weeks** | Publish 1 Tier B thread (weeks 2, 4 of month) | Completed analysis module or industry event |
| **Before publishing** | Draft thread in note doc (make it permanent record) | Same day or +1 day |
| **After engagement** | Compile top replies; cross-link in follow-up note | +3 days |

### 4.3 Monthly Cycle (note secondary + LinkedIn)

| Day of Month | Task | Time | Platform |
|--------------|------|------|----------|
| **1–10** | Identify Tier C topic from completed reports or policy event | 20 min | Notes |
| **11–20** | Write Tier C article (~1,500–3,000 char) | 60–90 min | note draft |
| **21–25** | Polish, vocabulary lint, fact-check | 20 min | note final |
| **26–30** | Publish; link from X; cross-post to LinkedIn | 15 min | note + LinkedIn |

### 4.4 Quarterly Cycle (Tier D essay)

| Time | Task | Effort |
|------|------|--------|
| **Month 1–2** | Accumulate findings; scope argument | 30 min (lightweight tracking) |
| **Month 2** | Outline Tier D essay; draft methodology section | 45 min |
| **Month 3 (weeks 1–2)** | Write & polish full essay | 2–3 hours |
| **Month 3 (weeks 3–4)** | Lint, fact-check, embed figures, cite sources | 45 min |
| **Month 3 (end)** | Publish on note; cross-post English version to LinkedIn | 20 min |

### 4.5 Effort Budget

**Target**: 30 minutes / day for SNS operations (strict upper bound).

- **X daily**: 15 min (post + replies + reads)
- **Other platforms**: 10 min (weekly aggregation)
- **Tier B/C/D drafting**: 5 min daily average (amortized across production window)
- **Buffer for crisis communication**: 10 min available

**If exceeding 30 min/day**: Downgrade to Tier A only; suspend Tier B/C until publication cycle is faster.

---

## 5. Crisis Communication

### 5.1 Triggers for Response

| Situation | Threshold | Response |
|-----------|-----------|----------|
| **Misinformation about our data** | Any claim citing Animetor work incorrectly | Clarify within 24 hours (X thread) + backup post on note |
| **Industry event** (strike, acquisition, policy) | Major event affecting animators | Brief within 48 hours (Tier B thread + note post) |
| **Personal attack or defamation** | Accusation that we've harmed someone | Respond with data + caveat section, avoid conflict escalation |
| **Data error discovered** | We catch error in our own analysis | Immediate retraction (if published); full correction note |

### 5.2 Response Template (Misinformation)

```
[Quote the incorrect claim]

Our data: [correct statement + source]

Caveat: [Why the incorrect reading is plausible / what would prove it right]

Sources: [link to methodology, CI, limitations]
```

### 5.3 Response Template (Data Error)

```
CORRECTION: [Original claim]

Correct claim: [Revised finding + CI]

Root cause: [What went wrong]

Impact: [Which reports/posts affected]

Timeline: [When did we discover, when did we fix, when did we publish?]

We apologize for the error and have re-computed [X]. 
Archived version: [link to old post with "RETRACTED" header].
```

### 5.4 Escalation: Do Not Engage

**Do not respond** if:
- Accusation is clearly motivated by factional interest (e.g., rival researcher, studio defending reputation)
- Response would require revealing confidential data
- Claim is so garbled that a response looks defensive rather than clarifying

**Instead**: Ignore for 1 week, then decide whether silence is adequate.

---

## 6. Content Calendar Template

**Use this template to plan** (in a pinned note or shared doc):

```
# SNS Content Calendar — [Month Year]

## Week 1
- **Mon 5/13**: Tier A topic = "Studio concentration analysis completed"
- **Wed 5/15**: Post Tier A (X) — studio co-credit consolidation gini
- **Fri 5/17**: Engage on X (15 min)

## Week 2
- **Mon 5/20**: Tier B scope = "Gender cohort analysis findings"
- **Wed 5/22**: Publish Tier B thread (8 posts) — early-career women's collab diversity

## Week 3
- **Mon 5/27**: Tier A topic = "Q2 policy brief snapshot"
- **Wed 5/29**: Post Tier A (X)

## Week 4
- **Mon 6/3**: Tier C scope = "Consolidation implications for labor market"
- **Thu 6/6**: Publish Tier C article on note (1,800 char)
- **Fri 6/7**: Cross-link to LinkedIn

## Quarterly (June end)
- **Tier D**: "Network Collapse & Negotiating Power" essay (5,000 char)
```

---

## 7. Multi-Platform Workflow

### 7.1 X → note → LinkedIn flow

1. **Tier A (X-first)**: Post to X. If resonates (10+ retweets), expand into short note summary.
2. **Tier B (X-thread)**: Post thread on X. Save thread as note article (add intro + caveats).
3. **Tier C (note-first)**: Draft on note; link from X; cross-post English to LinkedIn.
4. **Tier D (long-term)**: Publish on note; translate/adapt for LinkedIn; announce on X.

### 7.2 Backup & Archive

- **X is ephemeral**: Store all X posts in a pinned note document (monthly archive).
- **note is permanent**: All tier C & D articles live here. Link from X/LinkedIn.
- **LinkedIn is professional**: English-language essays + publication announcements.
- **Bluesky**: Daily copy of X content; treat as mirror, not original channel.

### 7.3 Image/Figure Requirements

- **Tier A**: 1 figure (1,200 × 600 px, PNG/JPG, <500 KB). Annotated with metric name + year range.
- **Tier B**: 1 figure per post (2–3 figures per thread).
- **Tier C/D**: Embed 2–4 figures inline (note markdown supports image upload).
- **Accessibility**: All figures include alt-text describing data + axes.

---

## 8. Feedback & Iteration

### 8.1 What to Track

- **Engagement**: Likes, retweets, replies (note which posts drive replies)
- **Traffic**: Clicks to full reports / analyses (use utm parameters in links)
- **Feedback**: Comments from animators, journalists, researchers; categorize by request type
- **Misinformation**: How often our data is misquoted, and by whom

### 8.2 Quarterly Review

- **Publication**: Did SNS posts lead to interviews, policy inquiries, or pilot inquiries?
- **Quality**: Did posts meet labor-first tone rules? Any vocab violations?
- **Efficiency**: Did we stay under 30 min/day budget? If not, why?
- **Reach**: Are we connecting with intended audiences (animators, journalists, policy)?

### 8.3 Downgrade Trigger

If SNS consumes >45 min/day for >2 consecutive weeks:
- Suspend Tier B & C
- Publish Tier A only (low maintenance)
- Resume higher tiers after publication cycle accelerates

---

## 9. Related Policies & Enforcement

### 9.1 Vocabulary Lint

Before publishing any tier, run:

```bash
pixi run lint-vocab < post.txt
```

(Exact command: see `scripts/report_generators/lint_vocab.py`)

### 9.2 Fact-Check Checklist

```
□ Data claims: Do we have a source? Is it current analysis or published report?
□ Caveats: Have we named method, time window, entity resolution strategy?
□ Names: If naming person/studio, do we have consent or historical context?
□ Attribution: Have we linked to full methodology + data statement?
□ Tone: Does this avoid ability framing + ranking + hiring language?
```

### 9.3 Publication Coordination

- If simultaneously publishing academic paper + SNS content: post SNS after embargo lifts (not before)
- If citing unpublished findings: label as "pre-print" or "internal analysis" (not "published")
- If citing RIETI DP or preprint: link to exact version (ArXiv ID / SSRN / Zenodo DOI)

---

## 10. Examples of Good & Bad Posts

### Example 1: Tier A ✓ GOOD

```
Studio co-credit partners dropped 38% among 2023 TV anime 
vs 2018 baseline (N=450 titles). Network density gini: 0.56→0.72. 
Budget pressure / streaming format trade-off?

Data: [link] | Method: [link]

#アニメ制作 #業界データ
```

**Why**: Narrow label ("partners dropped" not "fragmented"). Includes cohort (TV anime), time window (2018–2023), N. Links to data. No ability framing.

### Example 2: Tier A ✗ BAD

```
Anime studios are consolidating and killing collaboration. 
Top studios hoard talent while smaller studios struggle. 
This is why young animators can't find mentorship.

Read more: [link]
```

**Why**: Causal claim (consolidation "killing" collab) without named method. Evaluative language ("hoard," "struggle"). Misframes structural metric as failure.

### Example 3: Tier B ✓ GOOD

```
[1] New finding: women animators have 23% lower co-credit 
diversity than men (95% CI: 15%–31%), controlling for cohort. 
Why?

[2] [CHART] Role distribution reveals: women are concentrated 
in In-between (65%) vs. Key frame (35%). Men: 40% / 50%.

[3] Does this role split cause lower diversity? 
Or do lower-diversity networks sort women into In-between? 
Hard to say without experiment.

[4] We interpret as: structural segregation by role. 
Not "women animators underperform at networking."
Interpretation requires policy context (hiring, mentorship, pay).

[5] @JAniCA @AnimatorGuildJapan — does this match your membership data? 
Would love to compare.
```

**Why**: Labels finding as "we find," not causal. Explicitly marks interpretation. Invites alternative readings. No ability framing.

### Example 4: Tier C ✓ GOOD

**Title**: アニメ制作における『女性アニメーター』と『役職分離』 — 4年の構造観察

- Opens with data: 23% co-credit diversity gap.
- Shows role distribution: In-between concentration.
- Interprets (labeled): "I see this as structural segregation, not ability-based sorting."
- Caveats: "These are observational findings; causation requires experiment or natural experiment."
- Policy: "Studios could [A], unions might [B], policy could [C]. No recommendation from me."

**Why**: Separates evidence from interpretation. No gender-ability framing. Transparent about limitations.

### Example 5: Tier D ✓ GOOD

**Title**: アニメ業界の『役職分離』はなぜ性別に相関するのか — 制度設計 vs. 選別

- Structure: Evidence of correlation, multiple mechanisms, labor market implications.
- Interpretation (explicit): "One reading: studios prefer In-between work as junior/casual roles, which happen to be feminized. Another: women select In-between as lower-stress option. Hard to disentangle."
- At Stake: Wage gap (In-between pays less). Career mobility (In-between ≠ Key frame pipeline). Bargaining power (in-between workers organize separately).
- Caveats: "If we changed assumption [X], finding shifts to [Y]. Here's why [Y] is less likely but not impossible."

**Why**: Intellectual honesty. Sits with ambiguity. Acknowledges distributional stakes without prescribing.

---

## 11. Off-Limits Topics

**Do NOT post about**:

1. **Individual animator personal details** (family, health, location) — Even if public, it's not structural data.
2. **Comparison of individual animators by "ability"** — Even if framed as network position, avoid naming if it feels like ranking.
3. **Prediction of who will be "next to leave industry"** — This veers into hiring/firing framing.
4. **Studio competitive advantage** — Avoid positioning Animetor as "studio evaluation tool." Structural observation only.
5. **Viewer/fan opinion** — anime.score, MAL ratings, etc. are display metadata, not analysis.

---

## Appendix: Vocab Lint Commands

```bash
# Check a single post (CLI)
echo "text to check" | pixi run lint-vocab

# Check a file
pixi run lint-vocab < post.md

# Check and show suggestions
pixi run lint-vocab --suggestions < post.md
```

---

## Appendix: Tone Self-Audit

Before publishing, ask:

- [ ] Did I name a structural mechanism? (Or did I imply individual failure?)
- [ ] If I said "top X" or "best," did I replace with percentile + CI?
- [ ] If I named a person/studio, did I have consent or historical context?
- [ ] Did I mark Interpretation section? (Or did I sneak opinion into Findings?)
- [ ] Could someone use this to rank or hire? If yes, reframe.
- [ ] Did I explain why the metric matters? (For whom? In what context?)
- [ ] Are my figures annotated with CI, time window, N?

If all checks pass: publish.

---

**Version history**:
- **2026-05-13**: v1.0 initial draft. 4-tier structure (A–D), labor-first tone rules, platform strategy, posting schedule, crisis communication, vocab enforcement, examples.

# Animetor Eval: Frequently Asked Questions

---

## Q1: Is this a personal capability or competence evaluation?

**A:** No. Animetor Eval measures **structural position and collaboration density** in a network of public credits, not personal capability, skill, artistic vision, or creative merit.

The distinction is critical:
- **Personal capability assessment** would claim: "Animator A is more skilled than Animator B"
- **Structural position** observes: "In the 2020–2025 period, Animator A received X% more diverse project invitations than the median peer at their career stage"

We measure **opportunity and network access**, not innate capability. Someone with lower visibility scores may be:
- Working without credit (assistant roles, uncredited pre-production)
- Choosing selective projects
- Dealing with visa or health constraints
- Experiencing structural discrimination
- Simply newer to the industry

All of these are structural facts, not ability facts.

**Reference**: `docs/REPORT_PHILOSOPHY.md` §1 (Perspectivism & Framing)

---

## Q2: Does Animetor Eval score individuals? Can I get competitive rank?

**A:** We calculate individual-level scores (network centrality, collaboration frequency, career progression), but **not a global competitive rank**.

Instead:
- **Cohort percentiles**: Where you stand relative to peers at your career stage (debut year), role, and primary studio
- **Confidence intervals**: All person-level scores include uncertainty bounds (95% CI or bootstrap range)
- **No leaderboard**: No "Top 100 Animators" or competitive ordering — meaningless across different roles and career arcs
- **Temporal view**: Scores are tagged to specific periods (e.g., "2020–2025 collaboration frequency percentile: 72nd, 95% CI [65, 79]")

Individual scores are viewable only in context: *"Your collaboration frequency places you above X% of animators who debuted in 2015–2018, based on 2020–2025 public credits."* This is a *position*, not an ordinal *rank*.

**Reference**: `CLAUDE.md` §Integrated Value (IV), `docs/CALCULATION_COMPENDIUM.md`

---

## Q3: Can studios use Animetor Eval for hiring?

**A:** Our project **design explicitly forbids it**, and we recommend against it.

Why:
- These are **network metrics, not capability metrics**. Using them to hire would misframe structural advantage as individual merit.
- **Goodhart's Law**: If studios optimize hiring around our scores, the scores stop measuring what we intended (network position) and start measuring gaming of our algorithm.
- **Labor protection**: Using structural indicators as hiring filters can entrench existing inequalities (e.g., high scores clustering around already-privileged demographics).

**Our policy:**
- We do not market to studios as an HR tool
- We do not provide APIs that enable automated filtering based on our scores
- We actively discourage employment-based use in all reports and documentation

**If you are a studio**: You are welcome to view summary industry reports and understand the structure. We do not support automated hiring pipelines.

**Reference**: `docs/STANCE.md` §4.2 (Individual Studios), `docs/REPORT_PHILOSOPHY.md` §9 (Disclaimers) & §8 (Goodhart Risk)

---

## Q4: Do individuals have to opt-in? Can I be removed?

**A:** No opt-in required (public credit data), but **opt-out is available**.

**Why no opt-in:**
- All data comes from already-public credits (end-of-episode scrolls, official staff lists, public databases like AniList, MyAnimeList, ANN)
- We do not claim to have "collected" this data; we have **aggregated and analyzed** it
- Mandatory opt-in would prevent structural observation of entire industries

**Opt-out policy:**
- **On-request removal**: Any person can request deletion of their personal page and exclusion from all future analyses
- **Response time**: 7 calendar days from request to completion
- **Scope**: Removes your individual record and derived scores; does not retroactively alter historical aggregate statistics where anonymization has already occurred
- **How to request**: Email akizora.biz@gmail.com with subject "Animetor Eval data removal request"

**Data handling:**
- Individual data is never sold or shared with third parties
- Display layer (public profiles) requires explicit activation; unless opted in, individual data is accessible only to the person themselves
- Internal analysis uses anonymized aggregates wherever possible

**Reference**: `docs/STANCE.md` §3 (Individual Information), `docs/press_kit/contact.md` (Opt-out SLA)

---

## Q5: Are studios or committees involved? Is this impartial?

**A:** We are **neutral on studio involvement** but **labor-first in design philosophy**.

**Neutrality on partnerships:**
- We do not take funding from studios, committees, or streaming platforms
- We do not co-develop metrics with any industry actor
- We accept pilot partnerships with studios that understand our labor-first framing and do not influence our analysis
- We publish regardless of studio feedback

**Labor-first design** (not neutral):
- We intentionally design infrastructure for workers to understand their own positions
- We intentionally reveal structural barriers (gender, cohort, studio size effects) that harm workers
- We do not hide unflattering findings to appease studios or industry groups
- We see transparent observation as supporting labor rights

**What we are not:**
- **Anti-studio**: Studios are complex institutions; we analyze structures, not judge organizations
- **Activist researchers**: We maintain analytical rigor and do not campaign for specific policies
- **Neutral observers**: We chose labor-first framing explicitly (see `docs/STANCE.md`)

**Reference**: `docs/STANCE.md` §1 (Labor-first framing), §4 (Stakeholder Relations)

---

## Q6: Is Animetor Eval using anime.score (viewer ratings)?

**A:** Absolutely not. **anime.score is never used in any scoring calculation.**

Why this matters:
- anime.score (popularity, viewer aggregates) can reflect luck, marketing, timing, or genre fashion — not individual contribution
- We measure **structure** (who works with whom), not **output quality**
- Using anime.score would violate our core design rule (Hard Rule #1 in `CLAUDE.md`)

**What we do with anime metadata:**
- **Allowed**: Studio, episode count, duration, format (TV/OVA/film), release date — factual production parameters
- **Display only**: We hold anime.score in a source data layer for reference, but it never flows into scoring models

**Validation:**
- `DONE.md` §anime.score removal: "16 pathways removed, SILVER 100% score-free"
- Verification: `rg 'anime\.score\b' src/analysis/ src/pipeline_phases/` → 0 matches

**Reference**: `CLAUDE.md` §Hard Rules (H1), `docs/REPORT_PHILOSOPHY.md` §7 (Prohibited Practices)

---

## Q7: Is Animetor Eval criticizing the anime industry?

**A:** We are **observing structure, not criticizing actors**.

We do observe:
- Concentration in production (a few "hub" individuals coordinate most work)
- Career visibility gaps (credit disappearance year-over-year)
- Unequal opportunity distribution (gender, cohort, studio size)

We do **not**:
- Claim studios are "bad" or individuals are "failing"
- Recommend that any person leave the industry or change jobs
- Judge artistic or business decisions
- Propose specific policy without qualification and evidence

**The difference:**
- **Critique**: "Studios should not concentrate production so heavily" (normative)
- **Observation**: "In our dataset, XX% of credited work comes from YY% of persons, compared to ZZ% in a random model" (descriptive)

Critique may follow from observation, but we keep them separate.

**Reference**: `docs/REPORT_PHILOSOPHY.md` §2 (Findings vs. Interpretation), `docs/STANCE.md` §0 (Purpose)

---

## Q8: What is the goal of this project?

**A:** To provide **visible, auditable infrastructure for labor rights in anime**.

Concretely:
1. **Self-advocacy**: Workers can answer "Where do I fit?" with quantitative evidence
2. **Wage negotiation**: "My peer group's median project count is X; I'm at 75th percentile; here's my asking range"
3. **Policy grounding**: Governments and unions can point to data when discussing credit publicity and worker protections
4. **Structural diagnosis**: "Is attrition high because workers leave, or because studios don't credit work?" Our data can help answer that

**Not the goal:**
- Employer profiling or selection
- Predicting "star potential"
- Automating any decisions
- Replacing human judgment

**Reference**: `docs/STANCE.md` §1-2 (Labor-first framing, Purpose pathways)

---

## Q9: What happens if I find an error in my data?

**A:** We have a correction and appeals process.

**If you find a mistake:**
1. Email akizora.biz@gmail.com with:
   - Your name and role(s) in anime
   - The error (wrong credit, wrong role, wrong anime, etc.)
   - Evidence (link to official credit, Wikipedia, etc.)
2. We review and respond within 7 business days
3. If confirmed, we update the database and publish a correction notice
4. Historical reports are **not retroactively changed** (to maintain reproducibility), but future reports reflect the correction

**What we will correct:**
- Wrong person (homonym mistaken for you)
- Wrong role or anime
- Missing credit
- Invalid parse (duplicate, malformed)

**What we cannot change:**
- Subjective disagreement ("I think my role should be listed as director, not animation director")
- Requests to remove accurate, verifiable public credits
- Disputes about sorting or positioning methodology (these follow from our methodology, not data errors)

**Reference**: `docs/REPORT_PHILOSOPHY.md` §5 (Reproducibility)

---

## Q10: Will Animetor Eval publish academic papers?

**A:** Yes. Publication strategy is under way.

**Current status:**
- Target venues: Labour Economics, Journal of Cultural Economics, Applied Network Science
- Expected timeline: First papers in 2026–2027
- Data availability: On-request basis for researchers (limited open-source in later phases)
- Replication: Peer reviewers and requesters can access anonymized data snapshots

**What this means for media:**
- After peer-reviewed publication, we expect industry and policy attention
- Press materials based on published findings are more credible than pre-publication working papers
- We will issue media kits around each major paper release

**Reference**: `docs/STANCE.md` §5 (Academic Stance)

---

## Q11: Is Animetor Eval a commercial business?

**A:** Animetor Eval is operated as a **startup with labor-first governance**, not primarily as a commercial venture.

**Funding model:**
- Currently self-funded (user's salary + modest savings)
- Exploring academic grants (学振, SciREX) and mission-aligned investors
- Not seeking VC investors expecting rapid scale or profit-driven research changes

**B2B vs. B2C:**
- **B2B (studios, committees)**: Not our primary path; would risk labor-first principles
- **B2C (individual animators)**: Future SaaS allowing workers to view their own profiles
- **Public good**: Industry reports published freely

**Sustainability:**
- Policy briefings as potential pathway to government funding (METI, 文化庁)
- Individual subscriptions (labor union members, studios that align with our values)
- Research contracts (universities, think tanks)

**Non-goal:**
- Becoming a recruitment platform
- Selling worker data to employers
- Optimizing for studio HR convenience over worker welfare

**Reference**: `docs/STANCE.md` §7 (Funding / Legal Form)

---

## Q12: How do you handle sensitive personal data (DOB, nationality)?

**A:** We **keep minimal personal data and do not display it publicly**.

**What we keep internally:**
- Date of birth (for cohort analysis: debut year, career arc comparisons)
- Nationality (for international collaboration analysis)
- Aliases / pen names (for entity resolution)

**What we do NOT display:**
- No individual date-of-birth on any public page or API
- No nationality inferences in public profiles
- No identifying personal information beyond name + credited roles

**Why we keep it:**
- Age cohort analysis requires birth-year inference (we use debut year instead where possible)
- International collaboration requires work country, not personal nationality
- Aliases ensure we match the same person across name variations

**Safeguards:**
- Internal data access only; no third-party sharing
- Regular audits for accidental leakage
- Separate storage from public display layer

**Reference**: `docs/STANCE.md` §3.2 (Age/DOB handling)

---

**For additional questions**, contact: akizora.biz@gmail.com

See `docs/press_kit/contact.md` for media response policy and timeframes.

# Animetor Eval: One-Pager

## Project Overview

**Animetor Eval** is a research-first analysis platform that quantifies structural positions and collaboration networks in the Japanese anime industry using public credit data. Our mission: **make visible the work of individual animators, directors, and production staff to support fair labor practices and career transparency.**

We do not evaluate personal capability. We measure network position, collaboration density, and career progression using data-driven methods with explicit confidence bounds.

---

## Mission Statement

Create **labor-first infrastructure** for anime workers to:
1. See where their contributions fit in the industry structure
2. Support wage negotiation with quantitative, auditable evidence
3. Reveal structural barriers to opportunity (gender, career stage, studio size)
4. Ground industry policy conversations in measurable observation

---

## Data Sources

- **AniList, MyAnimeList, ANN, Seesaa Wiki, allcinema, TMDB, JVMG, Keyframe**: Credit records spanning 1963–2026
- **Coverage**: ~35,000 anime works, ~78,000 persons, ~540,000 credit records (as of Q1 2026)
- **Scope**: Public credit data only (end-of-episode credits, official staff lists, open databases)

---

## Key Findings [Placeholder — Updated on publication]

### (Example Structure)

**Finding 1: Network Concentration**
Among 10,000+ credited collaborators in 2020–2025, core production teams cluster around 200–300 "hub" individuals (directors, key animators, producers). The top 5% of persons by collaboration frequency account for XX% of all credited participations. *Method: Weighted PageRank on co-credit graph; 95% CI: [XX, XX]; n=X,XXX persons.*

**Finding 2: Career Visibility Gaps**
Persons with 3+ consecutive years of credited work show a XX% rate of credit disappearance in the following year (95% CI: [XX, XX]). This gap varies significantly by role (animation staff: XX%; directors: XX%) and studio affiliation. *Null model comparison: random model predicts XX%.*

**Finding 3: Gender Opportunity Structure**
Female animators and directors are underrepresented in high-connectivity roles (bridge builders, frequent collaborators) at a rate not explained by participation frequency. *Holdout validation on 2017–2019 cohort predicts 2020–2025 role distribution with AUC = XX.*

---

## Method Gate

All findings meet strict methodological standards:

- **Individual estimates** (person-level scores): Always reported with confidence intervals or bootstrap bands
- **Group comparisons**: Always compared against null models (random rewiring, degree-preserving baselines, demographic matched pairs)
- **Predictive claims**: Always validated on held-out time periods (train: ≤T, test: >T)
- **Sensitivity analysis**: Key results reported across alternative time windows, thresholds, and aggregation units

---

## What This Is NOT

- **Personal capability assessment**: These metrics do not measure technical skill, artistic vision, or creative vision
- **Competitive ordering**: No global leaderboard; only cohort percentiles within defined peer groups
- **Hiring guidance**: Designed for worker transparency, not employer selection
- **Popularity measure**: Does not use viewer ratings, anime scores, or external reviews
- **Industry criticism**: Structural observation, not judgment of specific actors or studios

---

## Disclaimer

**注意事項** | **Notice**

---

本レポートに含まれる数値は、公開クレジットデータに基づくネットワーク構造および協業密度の記述的指標である。これらは個人の技量、芸術性、または職業的価値の評価ではなく、そのような評価として解釈されるべきではない。

本指標は測定者が選択した定義・集計単位・時代窓に依存しており、別の選択からは別の数値が得られる。本レポートは「客観的真実の開示」ではなく「明示された選択に基づく記述」である。

本指標を採用・報酬・契約・人事評価の単一または主要な根拠として使用することを運営者は推奨せず、そのような使用の結果について責任を負わない。

---

All figures in this report are descriptive metrics of network structure and collaboration density, derived from publicly available credit data. They do not constitute and should not be interpreted as assessments of individual ability, skill, artistry, or professional worth.

These metrics depend on definitional, aggregational, and temporal choices made by the analyst; alternative choices would yield different figures. This report is not an "objective disclosure of truth" but a "description under stated choices."

The operators do not endorse the use of these metrics as the sole or primary basis for hiring, compensation, contract, or personnel decisions, and disclaim responsibility for outcomes of such use.

---

## Contact

**取材・お問い合わせ** | **Media Inquiries**

Email: akizora.biz@gmail.com

**Response commitment**: 1 business week for confirmed inquiry; fact-checking available before publication.

See `docs/press_kit/contact.md` for full media relations policy.

---

**Last updated**: May 2026 | Project version: v3.0.1

# Ethics Statement Template — Animetor Eval Publication

**Status**: Draft (pending legal review by `TASK_CARDS/29_legal/01_data_protection_review`)

**Audience**: High-impact journals (Labour Economics, Journal of Cultural Economics, Applied Network Science)

**Purpose**: This template provides a complete ethics disclosure for publication submission. Project stakeholders should review and adapt for each venue's specific requirements.

---

## 1. Data Sources and Legal Basis

### 1.1 Source Description

This research analyzes the anime industry's collaboration networks based on **publicly disclosed credit data** from the following sources:

| Source | Coverage | Scope | Access Method | License |
|--------|----------|-------|----------------|---------|
| AniList | Anime metadata, credits (1970–present, ~17k anime) | Person names, roles, episode coverage | Public GraphQL API | CC0 (metadata); TOS compliance |
| MyAnimeList (MAL) | Anime metadata, character roles (1997–present, ~19k anime) | Person names, character credits, ratings | Public API / web scrape | TOS compliance required |
| AniDB (ANN) | Anime encyclopedia, staff credits (1970–present, ~30k anime) | Person names, roles, voice actors, studios | Public web scrape | robots.txt compliant |
| themoviedb (TMDb) | Anime, film, TV person data (1990–present) | Person names, cross-ID references (TMDB ID, IMDB ID) | Public API | CC0 / TOS compliance |
| allcinema | Japanese film / animation database (1970–present, ~10k anime) | Japanese character names, staff credits, studio info | Public web scrape | robots.txt compliant |

**Legal basis**: All data was collected from sources explicitly published as part of public credit information (end credits, staff databases, publicly available APIs). No authentication bypass, rate-limit evasion, or data-scraping ToS violations were committed. `robots.txt` and API terms were observed throughout collection.

### 1.2 Data Handling Compliance

- **GDPR scope**: Research is conducted by an individual (not an EU-based organization). No special data processing permissions apply. Reliance on "publicly available" / "secondary data" exceptions is subject to venue interpretation.
- **Japan Personal Information Protection Act (PIPA)**: Data subjects are identified individuals ("persons holding credits"). The research does not require pre-publication consent under PIPA §17 (research exemption) because:
  1. Credit information is already publicly disclosed.
  2. Aggregation and structural analysis do not constitute "new disclosure" but rather structured republication of existing public facts.
  3. On-request deletion mechanisms (opt-out) are provided.
- **US Fair Use**: Research on publicly available secondary data is broadly protected under fair use doctrine, though this analysis does not substitute fair use for disclosure.

---

## 2. Personal Data Scope and Identifiability

### 2.1 Data Elements

The research dataset includes:

| Category | Elements | Identifiability | Handling |
|----------|----------|-----------------|----------|
| **Identity** | Full name (JA + EN), romanized aliases, pen names, IMDB/TMDB/AniList IDs | Directly identifiable | Public output (portfolios, network graphs) |
| **Work history** | Anime titles, roles, episodes, years active, studios | Directly identifiable | Public output |
| **Temporal data** | Debut year, career span, gaps in credit visibility | Quasi-identifiable (combined with role patterns) | Public output |
| **Network position** | Co-credit relationships, betweenness centrality, PageRank scores | Derived; identifies via association | Public output |
| **Structural indicators** | AKM fixed effects, diversity indices, authority scores | De-identified by construction (person FE only) | Public output |
| **Internal only** | Date of birth, nationality, gender (inferred), address (if scraped) | Sensitive | Not in public API; internal use only (cohort inference) |

### 2.2 De-identification Scope

**The research intentionally does NOT de-identify individuals.** This is a deliberate design choice:

1. **Purpose alignment**: The project aims to help workers retrieve and verify their own credit contributions. De-identification defeats this goal.
2. **Public data**: Credit information is already published under people's real names. Anonymization would artificially obscure facts already in the public domain.
3. **Harm mitigation via transparency**: Rather than anonymity, the project uses:
   - Named methodology (not black-box scoring)
   - Explicit confidence intervals around person-level estimates
   - Clear documentation of data sources and calculation logic
   - On-request deletion mechanism (see §4)

### 2.3 Sensitive Attributes

The following attributes are collected but **never included in public reports or APIs**:

- Date of birth (used internally for cohort estimation only)
- Inferred nationality, ethnicity, gender (used for bias detection and diversity analysis; disclosed only in aggregate)
- Email addresses, social media handles (collected if public, used for opt-out contact only)

---

## 3. Research Aims and Use Restrictions

### 3.1 Intended Use

This research is conducted under the **labor-first normative stance** (documented in `/docs/STANCE.md`). The aims are:

1. **Worker self-verification**: Enable individuals to verify their own credit history and structural position within the industry.
2. **Collective transparency**: Provide evidence for labor organizing, wage negotiation, and policy advocacy by and for workers.
3. **Structural observation**: Document network patterns, opportunity gaps (gender, cohort), and labor mobility for academic inquiry.

### 3.2 Prohibited Use

The research authors **explicitly forbid** use of these scores for:

- **Hiring or firing decisions**: Scores reflect network position, not personal capability. Using them as performance metrics violates the project's legal and ethical principles.
- **Ordinal sequencing of individuals**: Any listing of workers in ordinal position by score (1st, 2nd, etc.) implies serial ordering of human worth and is prohibited under the labor-first stance.
- **Algorithmic decision-making**: Automated selection, screening, or ordinal sorting by score must not use these metrics without explicit human review and modification.
- **Defamatory characterization**: Scores must never be framed as measuring individual "potential," "proficiency," "aptness," or "capacity." Legal risk of 信用毀損 (defamation under Japanese law).

---

## 4. Risk Mitigation: Entity Resolution and Data Quality

### 4.1 Homonymy and Misidentification Risk

The largest source of harm in this research is **incorrectly attributing credits to the wrong person**. A single homonym error—e.g., crediting one animator with work performed by a person of the same name—can falsely inflate that person's score and damage reputation.

**5-stage resolution process** (documented in `src/analysis/entity_resolution.py` and `src/etl/resolved/`):

| Stage | Method | Acceptance Threshold |
|-------|--------|----------------------|
| **Exact match** | Source ID (AniList ID, TMDB ID, AniDB ID) already tied to name in ≥2 sources | 100% confidence |
| **Cross-source ID linking** | Official cross-references (e.g., MAL import of AniList) | 99.5% confidence |
| **Romaji normalization** | Hepburn/Nihon-shiki standardization + fuzzy name matching (Jaro-Winkler ≥0.95) | 95% confidence |
| **Similarity-based clustering** | ML cosine similarity (0.90–0.94 range) on co-credit patterns + name strings | 85% confidence (flagged for review) |
| **AI-assisted resolution** | Manual review + Claude API semantic matching for remaining ambiguous cases | 80% confidence (analyst confirmation required) |

Persons matched only at stage 4 or 5 are **flagged in the output** with a `resolution_confidence` field; consumers are advised to avoid using low-confidence matches for high-stakes decisions.

### 4.2 Data Quality and Missingness

- **Coverage bias**: Data is limited to works with public credits (excludes ghost work, uncompensated, or anonymized contributions). Reports disclose this limitation.
- **Temporal gaps**: Some historical credits (pre-1995) are sparse or missing. Comparisons across decades include uncertainty intervals.
- **Name variation**: Pseudonyms, stage names, and non-Latin scripts introduce error. The `entity_resolution_eval.py` module measures false positive / negative rates.

---

## 5. Consent Basis and Opt-Out Mechanism

### 5.1 Consent Model

This research **does not seek pre-publication consent** from data subjects. Instead, it relies on the following:

1. **Public availability doctrine**: Data subjects voluntarily published their credits in public databases or end credits. They consented to publication when they accepted the job or posted their name publicly.
2. **Observed behavior**: Many animators already maintain public portfolios on AniList, TMDB, and personal websites, indicating comfort with credit visibility.
3. **Secondary data exemption**: The research aggregates and analyzes existing public data rather than collecting new data from subjects.

However, **individual opt-out is always honored** (see §5.2).

### 5.2 On-Request Deletion (Opt-Out)

Data subjects may request removal at any time. The procedure is:

| Step | Timeline | Action |
|------|----------|--------|
| **1. Request submission** | User initiates | Contact deletion request via portfolio page, email (delete@example.com), or deletion form |
| **2. Identity verification** | Day 1–2 | User confirms identity (co-credit history, SNS account, email verification) |
| **3. Approval** | Day 3–5 | Analyst reviews request (simple approval; disputes escalate) |
| **4. Deletion execution** | Day 5–7 | Removal from Resolved layer; next pipeline run excludes person from Mart scores |
| **5. Audit log** | Ongoing | Record in `mart.meta_optout_audit` table (person_id, removed_at, sla_met) |

**SLA**: 7 days from verified request to full removal. If deletion cannot be completed within 7 days, the person's page is marked "removed on request" and scores are suppressed immediately.

**Scope of deletion**:
- Removed from: Portfolio API, explorer UI, person scores table (Mart layer)
- Retained for audit: Resolved layer (marked as deleted) and Source / Conformed layers (as historical record)
- Consequence: Re-running the pipeline will regenerate Mart tables without the deleted person

---

## 6. Conflict of Interest

### 6.1 Author Position and Affiliation

**Project governance**: Animetor Eval is a **personal / startup project** operated by an individual PhD student. It is **not affiliated with the author's academic institution** and does not involve the author's academic advisors.

- Author is a doctoral candidate at [Institution Name] (degree independent of this research)
- The research is conducted as a separate personal / startup initiative
- No institutional funding, supervision, or liability protection applies

### 6.2 Commercial Interests

**Startup status**: The project is operated as a private startup with the following disclosed interests:

| Interest | Stake | Mitigation |
|----------|-------|-----------|
| **B2C SaaS revenue** | Planned but not yet operational. Revenue model: individual animators pay subscription for portfolio analytics. | Scores are not optimized for SaaS monetization (e.g., no "premium tiers" that bias toward higher scores). Methodology is frozen and published; future revenue does not affect past analyses. |
| **Data licensing** | Research data may be licensed to third parties (studios, policy organizations, research labs) under CC-BY-SA. | Licensing agreements explicitly forbid use as hiring/firing signals or studio ranking. See /docs/STANCE.md §4. |
| **Policy influence** | The author advocates for labor-protective policy (credit disclosure requirements, wage standards) that would benefit workers and potentially increase industry-wide data quality. | This is intentional and disclosed (labor-first stance in /docs/STANCE.md §1). The advocacy does not bias the research methodology but does bias the project's purpose. |

### 6.3 Competing Interests

- **No competing financial interest**: Author has no financial stake in individual animators' salaries, studio profitability, or streaming platform revenues.
- **Reputational interest**: Author's reputation depends partly on score accuracy and on the project's ethical standing. This creates incentive to be transparent about limitations.

---

## 7. Replication and Data Availability

### 7.1 Standard Policy (On-Request)

By default, replication datasets are **not frozen** and are available on-request to:

- Peer reviewers during review process
- Authors responding to published criticism
- Researchers seeking to replicate or extend the work

This allows the project to incorporate new data and fix bugs without maintaining multiple historical snapshots.

### 7.2 High-Impact Venue Exception

For submissions to **high-impact journals** (Labour Economics, Journal of Cultural Economics, Applied Network Science), the following are frozen and archived:

- **Resolved-layer database** (`result/resolved.duckdb` snapshot)
- **Mart-layer database** (`result/gold.duckdb` snapshot)
- **Code hash** (git commit) of analysis scripts used in the paper
- **Environment lock file** (`pixi.lock`) to ensure reproducibility of dependencies
- **Method documentation** (`docs/method_notes/<paper>.md`)
- **Score frozen version** (`mart.meta_score_frozen` table) with λ weights and all person-level estimates

These are archived to **Zenodo** with DOI citation in the paper. See `TASK_CARDS/32_publication/01_replication_snapshot_exception.md` for detailed procedure.

### 7.3 Data Sensitivity and Embargoes

- **Public release date**: Research datasets are available immediately upon publication (no embargo period).
- **Pre-publication reviewers**: Peer reviewers receive the full snapshot under confidentiality agreement.
- **Code availability**: Analysis code is available on GitHub (`https://github.com/[repo]`) under [LICENSE] starting from publication date (can be earlier if authors agree).

---

## 8. Governance and Accountability

### 8.1 Self-Review Checklist

This research underwent the following **self-review** steps (in lieu of formal institutional IRB, as the institution does not claim governance over independent student research):

- [ ] **Data source compliance**: All sources accessed via public APIs or robots.txt-compliant scraping; no authentication bypass
- [ ] **Entity resolution**: Misidentification risk mitigated via 5-stage process; low-confidence matches flagged
- [ ] **Harm analysis**: Person-level disclosure identified as primary risk; opt-out mechanism provided
- [ ] **Methodology transparency**: Findings/Interpretation sections separated; no causal claims without identification strategy
- [ ] **Vocabulary compliance**: Report checked against forbidden terms (avoid capacity framing, defamatory language, ordinal person-level sorting); see `scripts/report_generators/lint_vocab.py`
- [ ] **Stakeholder engagement**: Labor unions (JAniCA) and individual animators consulted during design (ongoing; see `/docs/STANCE.md §4`)
- [ ] **Conflict disclosure**: Commercial interests, affiliation, and advocacy positions documented above

### 8.2 Oversight and Appeals

For questions or complaints about this research's ethics:

1. **Data subjects**: Contact `delete@example.com` for opt-out or complaint
2. **Reviewers / institutions**: Contact project GitHub or [author email]
3. **Public accountability**: All complaints and responses are logged in `mart.meta_complaints_audit` (de-identified)

---

## 9. Disclaimers (Required per Project Policy)

### 9.1 Limitation of Findings

- Scores represent **network position and co-credit density**, not individual ability, talent, or competence.
- Confidence intervals quantify sampling variation; they do not account for unmeasured confounding or data source bias.
- Missing data (unmapped pseudonyms, ghost credits, data source gaps) is substantial; comparisons across demographics carry uncertainty.

### 9.2 Prohibited Uses

These scores **must not be used** for:
- Hiring, firing, or performance evaluation decisions
- Algorithmic ordinal sorting or automated personnel screening
- Defamatory or reputation-damaging assertions about individuals
- Circumvention of labor protections or wage-setting processes

---

## 10. References

- `/docs/STANCE.md` — Project labor-first normative stance
- `/docs/REPORT_PHILOSOPHY.md` — Report writing philosophy (perspectivism, Findings/Interpretation separation)
- `/CLAUDE.md` — Design principles (Hard Rules H1–H4)
- `TASK_CARDS/29_legal/01_data_protection_review.md` — Legal review findings (to be completed)
- `TASK_CARDS/29_legal/03_optout_mechanism.md` — Opt-out implementation
- `TASK_CARDS/32_publication/01_replication_snapshot_exception.md` — Replication snapshot procedure
- `src/analysis/entity_resolution.py` — Entity resolution algorithm
- `scripts/report_generators/lint_vocab.py` — Forbidden vocabulary enforcement

---

## Change Log

- **2026-05-13**: Initial draft. Pending legal review completion.

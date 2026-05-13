# B2C SaaS Design: Individual Animator Portfolio & Insights Platform

**Version**: 1.0  
**Date**: 2026-05-13  
**Status**: Design (pre-implementation)  
**Scope**: MVP architecture, feature spec, pricing, authentication strategy, data exposure scope, roadmap  
**Related**: `TASK_CARDS/31_business/02_b2c_design.md`, `docs/STANCE.md §1-4`, `docs/REPORT_PHILOSOPHY.md`

---

## 1. Executive Summary

Animetor Eval's B2C SaaS enables individual animators and production staff to **understand their own structural position** within the anime industry's collaboration network. The product complements the public policy and publication tracks by providing a labor-first tool directly to workers.

**Core Value Propositions**:
- **Tier 0** (Free, public): Discover your work history and industry percentile
- **Tier 1** (Free, authenticated): Transparent IV decomposition + PDF negotiation brief
- **Tier 2** (Subscription, ¥500–1500/mo): Advanced analytics, counterfactual modeling, industry transparency

**Labor-first Stance Integration**: All tiers designed to *support worker autonomy*, not screen workers for employers. No hiring/evaluation framing (see `STANCE.md §1.2`). Data exposure strictly bounded by labor rights, not commercial utility to studios.

---

## 2. Product Tiers: Feature Specification

### Tier 0: Public Portfolio Page (Free, no authentication)

**URL**: `/portfolio/{person_canonical_id}`  
**Access**: Public (discoverable, indexed, linkable)

**Features**:

| Feature | Spec |
|---------|------|
| **Work History** | All credited works (anime title, year, role(s), episode count, format) with source attribution |
| **Structural Position** | Cohort percentile (vs. peers in same role + debut decade) for: `theta_i` (person FE) / `birank` (network authority) / `awcc` (collaboration density) |
| **Cohort Comparison** | Side-by-side stats vs. role + debut cohort (n, mean, median, 25th/75th percentile) with sample size and CI disclosure |
| **Career Timeline** | Interactive chart: works/year, avg role seniority, studio diversity (Herfindahl index) |
| **Public Disclaimers** | JA + EN: "Structural position indicator, not ability assessment. See methodology." + link to `docs/REPORT_PHILOSOPHY.md` |
| **Opt-out Button** | Links to email opt-out form (29/03 mechanism) |

**Boundary Conditions**:
- No individual score displayed as absolute ordinal (e.g., no "#47 animator")
- No cross-person comparison table or leaderboard on public tier
- No salary estimation
- No hiring/recruitment language in copy

**Design Notes**:
- Portfolio aggregates all sources (AniList, MAL, ANN, Seesaa, etc.) with source icons for transparency
- Percentile displayed as range (`Top 25-35%` with CI) not point estimate
- Historical data access: only user's own data, not retroactive normalization

---

### Tier 1: Authenticated Insights (Free, SNS-verified)

**Precondition**: SNS account verification (X / pixiv / GitHub) or email + portfolio self-attestation  
**Access**: User's own data only  
**New Features**:

| Feature | Spec |
|---------|------|
| **IV Decomposition** | 5 components with transparent weights and CI: (λ₁·theta + λ₂·birank + λ₃·studio_exp + λ₄·awcc + λ₅·patronage) × dormancy_mult |
| **Component Interpretation** | For each component: name, formula (in plain language), role in IV, person-specific value, cohort median, percentile |
| **Cohort Trajectory** | Anonymized comparison group (5-10 similar persons, same role + ±2 years debut, similar studio history, names redacted) with paths over 3-5 years |
| **Negotiation PDF** | 1-2 page report: public work history + IV summary + "how this might matter for compensation talk" (with labor rights disclaimer) |
| **Career Resilience** | Dormancy multiplier explained: "Years since last credited work" risk indicator + what it means for visibility |

**Boundary Conditions**:
- Cohort trajectory shown as anonymized silhouettes (no names, colored only)
- PDF labeled "Structural Position Reference" (not "salary justification")
- No counterfactual predictions in Tier 1 (reserved for Tier 2)
- No cross-studio pay comparisons (source risk)

**Authentication Methods** (at least one required):

| Method | Flow | Verification |
|--------|------|---------------|
| **X (Twitter) OAuth** | Login via X API, verify account age ≥6mo, no obvious bot flags | Person canonical_id looked up via X handle in AniList / MAL |
| **Pixiv OAuth** | Login via Pixiv API, verify account age ≥1 year | Portfolio (fanart / character designs) optional; accept any verified account |
| **GitHub OAuth** | Login via GitHub, require account age ≥6mo (lower bar for dev-adjacent staff: programmers, technical coordinators) | Optional: GitHub history hints matching (commit author name) |
| **Email + Portfolio Attestation** | Self-hosted flow: provide email + link to portfolio (personal website / SNS post crediting work) | Manual review (batched weekly, <48 hr SLA during pilot) |

**Rationale**: SNS verification accepts that false attestation risk exists (e.g., impersonation). Mitigated by:
- CV fraud detection heuristics: if claimed name has no credits in 5 years, flag for manual review
- Transparent logging: show which verification method was used
- Revoking access if fraud detected (via abuse report)

---

### Tier 2: Advanced Analytics & Transparency (Subscription, ¥500/1000/1500/mo)

**Precondition**: Tier 1 authenticated + active subscription  
**Access**: User's own data + anonymized industry benchmarks  
**New Features**:

| Feature | Spec | Price Tier |
|---------|------|-----------|
| **Counterfactual Modeling** | "What if I moved to Studio X?" / "What if I took an episode director role?" — AKM-based prediction of scale impact (±σ CI) | ¥1500 |
| **Role-Pair Credit Transparency** | "How many director-level persons publish credits for role X at studios A, B, C?" — data to support "please publish my credits" requests | ¥1000 |
| **Career Alerts** | Notification when: (a) new credits appear for you (data lag detection), (b) industry trends in your role shift >±σ | ¥1000 |
| **Custom Cohort Search** | Filter similar persons by: role, debut year range, studio list, credit frequency — download anonymized cohort (5-50 persons, names redacted) | ¥1000 |
| **Compensation Peer Groups** | Aggregated stats (median studio tenure, credit frequency, role progression speed) for your role + studio pair (3-5 person minimum for privacy) | ¥1500 |

**Pricing Rationale** (see §3 for full comparison):

| Tier | Price | Target Duration |
|------|-------|-----------------|
| **Tier 2 Basic** | ¥500/mo | Month-to-month (career check-ins) |
| **Tier 2 Standard** | ¥1000/mo | Quarterly (ongoing negotiation support) |
| **Tier 2 Premium** | ¥1500/mo | Annual subscription (planning tool) |

**Boundary Conditions**:
- Counterfactuals generated only for user's own profile, not for browsing others
- Studio-level compensation data shown only if ≥5 persons in cohort (privacy floor)
- No ordinal sorting of studios (omit comparative best/worst language)
- No recommendation engine ("you should apply to X")

---

## 3. Pricing Strategy

### Rationale

**Target Market**: ~2,000 active animators (AniList top-credited persons + JAniCA membership overlap, estimated)

**Economic Constraints**:
- Animator median income: ¥2.5–3.2 million/year (~¥200k/mo after tax)
- SaaS subscription budget: 0.5–1.5% of income typical (¥1k–4.8k/mo)
- Competitive: Notion Personal (¥1100/mo), Linear (¥960/mo starter)
- Labor-adjacent appeal: budgets may come from personal dev / career planning allocations

**Animetor Proposal**:

| Tier | Monthly | Annual | Value Prop | Break-even Users @ 60% margin |
|------|---------|--------|-----------|------------------------------|
| **Tier 0** | Free | — | Portfolio + percentile | N/A |
| **Tier 1** | Free (auth) | — | IV decomposition + PDF | N/A |
| **Tier 2 Base** | ¥500 | ¥5,500 (8% discount) | Career check-ins | ~50 @ 30% COGS |
| **Tier 2 Std** | ¥1,000 | ¥11,000 (8% discount) | Quarterly planning | ~25 @ 30% COGS |
| **Tier 2 Premium** | ¥1,500 | ¥16,500 (8% discount) | Annual negotiation support | ~20 @ 30% COGS |

**Acquisition Channels**:
- JAniCA partnership: bulk discount (¥400/mo for members, 20% discount)
- SNS cross-posting (X, Pixiv artist networks, Bluesky anime communities)
- Anime convention booths (year 1)
- Journal + publication launch (publicity)

### Pricing Comparison Table

| Product | Price | Target | Notes |
|---------|-------|--------|-------|
| **Notion Personal** | ¥1,100/mo | Teams / writers | Storage + integrations |
| **Linear** | ¥960/mo | Dev teams | Project management |
| **Figma** | ¥1,650/mo | Designers | Collaboration tool |
| **Animetor Tier 2 Base** | ¥500/mo | Individual animators | Structural position only |
| **Animetor Tier 2 Std** | ¥1,000/mo | Career planners | + industry transparency |
| **Animetor Tier 2 Premium** | ¥1,500/mo | Negotiation prep | + counterfactuals |

**Justification**: Tier 2 Base undercuts Notion (¥500 vs ¥1,100) because Animetor is *narrower* (anime industry only) but *deeper* (proprietary structural data). Premium tier (¥1,500) aligns with Figma for power users planning major career moves.

---

## 4. Authentication Strategy

### Goals

1. Verify that a person can administer their own profile (prevent impersonation)
2. Link SNS identity to canonical person_id without requiring manual review for most users
3. Minimize friction (OAuth preferred, email fallback for non-social animators)
4. Detect & block fraud (name mismatch, orphaned accounts)

### Methods & Flows

#### 4.1 SNS OAuth (Primary)

**Supported**: X (Twitter), Pixiv, GitHub  
**Flow**:

```
1. User clicks "Sign in with [Platform]"
2. OAuth → login & permission grant
3. Get OAuth identity (user_id, handle, email, date_joined)
4. Look up handle in AniList / MAL / canonical_persons table
5. If match found: create session, link to person_id
6. If no match: offer manual entry (name + work example) or skip Tier 1
```

**Lookup Strategy**:
- First: exact match on external_id (anilist_user_id / mal_user_id)
- Second: fuzzy match on display name (Levenshtein ≤2 edits)
- Third: manual entry fallback ("I am X, here is my portfolio link")

**Checks**:
- Fraud detection: if claimed name_ja / name_en has no credits in last 5 years and wasn't historically active → *warn* (offer manual review)
- Bot flags: if X account age <6 months OR Pixiv account age <12 months → reject (can retry later)

#### 4.2 Email + Portfolio Attestation (Fallback)

**When**: SNS not available (e.g., older animators without social presence, privacy-concerned users)

**Flow**:

```
1. User enters email + name (ja + en) + portfolio link
2. System sends verification email (OTP)
3. User confirms OTP
4. If canonical match exists: auto-link
5. If no match: queue for manual review (batched, <48h SLA during pilot)
6. Manual: operator checks portfolio URL for credit mentions + role consistency
```

**Portfolio Examples Accepted**:
- Personal website / artist portfolio
- SNS post crediting work ("I was key animator on X season Y")
- LinkedIn / GitHub with role in bio
- Public document (CV, portfolio PDF)

**Decision Tree**:

```
Credit appears in database + name match → auto-approve
Credit appears + name fuzzy match → approve + flag for audit
No credit in 5 years but historical → approve + warn user
No credit ever found → reject, suggest resubmit
```

---

## 5. Data Exposure Scope & Guardrails

### 5.1 What Users Can See

**Their own**: All data in mart schema (scores, components, history, network position)  
**Cohort (anonymized)**: 5–10 similar persons, names redacted, colored trajectories only  
**Industry aggregate**: Role + studio + year statistics (≥5-person cells only)  
**Public persons**: Work history + percentile only (no components, no salary proxies)

### 5.2 What's Explicitly Hidden

| Data | Reason |
|------|--------|
| **DOB / age** | STANCE §3.2: public display prohibited (internal-only for cohort definition) |
| **Salary estimates** | No compensation inference (too noisy, hiring-adjacent risk) |
| **Predicted turnover risk** | Dormancy used only as component in IV (not as "churn risk score") |
| **Hiring suitability** | STANCE §1.2: explicitly banned |
| **Studio comparisons (ranked)** | STANCE §4.2: no studio leaderboards |
| **Persons under age 18** | If found in data: no public portfolio page (internal flag: `persons.is_minor`) |
| **Studio pay grades** | Even if publicly available: not aggregated by Animetor (union data governance) |

### 5.3 Opt-out Mechanism (29/03 Dependency)

Implemented via `src/db/schema.py` → `persons.opted_out` boolean field.

**UI Flow**:
- Button on every Tier 0 portfolio page: "Request removal from Animetor"
- Links to form: name confirmation + email + optional reason
- SLA: deletion from public pages within 7 days
- Backend: sets `opted_out = true`, revokes all API access, removes from JSON caches
- Retention: anonymized row kept in DB (for "we honored X requests" counting) but unreachable by API

**Reversal**: One-time reactivation via email link (for data subject who changed mind)

---

## 6. API Endpoints (Tier 0 & 1 Public)

### 6.1 Public Portfolio (Tier 0)

```
GET /api/persons/{canonical_id}/portfolio
  → {
      person_id, name_ja, name_en, image_url,
      work_history: [{ anime_id, title_ja, title_en, year, role, episodes, format }],
      structural_position: {
        theta_i: { value, percentile, ci_lower, ci_upper, cohort_size },
        birank: { value, percentile, ci_lower, ci_upper },
        awcc: { value, percentile, ci_lower, ci_upper },
        cohort: { role, debut_decade, n }
      },
      career_stats: { first_year, latest_year, total_works, studios_worked_with }
      disclaimer: "...",
      opted_out: false
    }
```

### 6.2 Authenticated IV Decomposition (Tier 1+)

```
GET /api/persons/me/iv (requires session)
  → {
      components: [
        { name: "Person FE (theta_i)", formula: "log(scale_ij) fixed effect", weight: 0.25, value: 1.34, ci: [1.12, 1.58], percentile: 62 },
        { name: "BiRank", formula: "weighted authority in collab network", weight: 0.20, value: 0.89, ci: [0.76, 1.02], percentile: 58 },
        { name: "Studio Experience", formula: "FE of studios worked at", weight: 0.15, value: 1.21, ci: [0.95, 1.47], percentile: 70 },
        { name: "AWCC", formula: "avg weighted clustering coeff", weight: 0.20, value: 0.34, ci: [0.28, 0.41], percentile: 45 },
        { name: "Patronage", formula: "shared credits with high-theta persons", weight: 0.20, value: 0.92, ci: [0.72, 1.12], percentile: 51 }
      ],
      dormancy_multiplier: { value: 0.95, ci: [0.88, 1.02], years_since_credit: 1 },
      iv_score: 1.07,
      cohort_percentile: 58,
      similar_persons_count: 8,  // anonymized cohort size
      methodology_url: "https://.../iv_decomposition_method_note.pdf"
    }
```

### 6.3 Negotiation PDF Export (Tier 1+)

```
GET /api/persons/me/negotiation-brief (requires session)
  → {
      pdf_url: "https://cdn.animetor.local/briefs/{uuid}.pdf",
      expires_in_hours: 24,
      content_summary: "Work history + IV summary + 'how to use this'"
    }
```

---

## 7. Frontend Architecture

### 7.1 Tier 0: Public Portfolio Page

**Path**: `/portfolio/{canonical_id}`  
**Tech**: Static HTML (Jinja2 templating) + D3.js timeline + Plotly percentile viz  
**Load**: Pre-rendered via `scripts/render_portfolios.py` (cron nightly)  
**CDN**: CloudFlare / Vercel Edge (cacheable 24h)

**Sections**:
1. Hero (name, role, years active, image)
2. Work History (table, filterable by year/role/studio)
3. Structural Position (3 cards: theta, birank, awcc with CI viz)
4. Career Timeline (interactive: credits/year, avg seniority, studio diversity)
5. Disclaimers + Opt-out

### 7.2 Tier 1/2: Authenticated Dashboard

**Path**: `/app/person` (after login)  
**Tech**: SPA (React 18 + TypeScript, Vite build)  
**State**: Zustand (simple, no Redux overhead)  
**API**: `/api/persons/me/*` endpoints

**Sections**:
- Overview: IV score + 5 components (cards with popovers)
- Trajectory: Cohort comparison chart (anonymized shadows)
- PDF Export: Download button
- (Tier 2) Alerts: Recent credits + role changes
- (Tier 2) Custom Cohort: Search + filter interface

**Mobile**: Responsive (Tailwind CSS), works on iPhone/Android

---

## 8. Roadmap (MVP → Growth)

### Phase 1: MVP (Months 1–3 post-design)

**Tier 0 + Tier 1 launch**:
- [ ] Implement Tier 0 public portfolio page (port from existing routers)
- [ ] SNS OAuth (X, Pixiv)
- [ ] Email fallback flow + manual review queue
- [ ] IV decomposition endpoint (use existing `src/analysis/scoring/iv_decomposition.py`)
- [ ] Negotiation PDF renderer (Jinja2 + reportlab)
- [ ] Opt-out form + deletion flow
- [ ] Pilot launch (50–100 beta users via JAniCA outreach)

**Blockers**:
- `27_methodology/03_iv_xai` must be implemented (IV component weights finalized)
- `29_legal/01_data_protection_review` completed (GDPR / APPI compliance)
- `29_legal/03_optout_mechanism` specified (SLA, reversal policy)

**Success Metrics**:
- ≥50 Tier 1 authentications in first month
- Zero fraud detections in manual review queue
- <2% opt-out rate (sign of user confidence)

### Phase 2: Tier 2 Launch (Months 4–6)

- [ ] Counterfactual model (AKM predictions for role/studio moves)
- [ ] Role-pair credit transparency (aggregate % publishing by role)
- [ ] Career alerts (new credits, role trends)
- [ ] Custom cohort search + download
- [ ] Stripe subscription setup (Japan-ready payment processor)
- [ ] Email notification system (async task queue: Celery + Redis)

**Success Metrics**:
- ≥10 Tier 2 subscriptions by end of month 6
- MRR ≥¥10k (¥500 × 20 users)
- Average subscription length >3 months

### Phase 3: B2B Variants (Months 7–12)

- [ ] **Studio HR read-only view**: Anonymized cohort analytics for own studio (opt-in pilot)
- [ ] **JAniCA data export**: Quarterly labor market brief (union use)
- [ ] **Policy brief data package**: Structured input for government / ministry reports

---

## 9. Risk Assessment & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|-----------|
| **Impersonation** | Medium | High (labor credibility) | Multi-method auth (not SNS alone) + fraud detection heuristics |
| **Data residuals** (employer inference) | Medium | High (labor trust) | Strict CI bounds on individual scores, no ordinal display |
| **Rejected users** (can't authenticate) | High | Medium (churn) | Email fallback + generous manual review SLA (48h) |
| **Low conversion** (Tier 2) | High | Low (MVP success) | Start with free Tier 1 + JAniCA partnership for bulk marketing |
| **Goodhart risk** | Medium | Medium (long-term) | Footnote in methodology: "scores reflect network, not ability"; monitor media coverage |
| **Opt-out flood** | Low | Low | Transparent deletion SLA + prominent disclaimer builds trust |

---

## 10. Dependency Checklist

**Pre-launch Must-Haves**:

- [ ] `27_methodology/03_iv_xai`: IV component weights finalized, CI formula specified
- [ ] `29_legal/01_data_protection_review`: GDPR / APPI / labor law review complete
- [ ] `29_legal/03_optout_mechanism`: On-request deletion SLA & reversal policy documented
- [ ] `31_business/01_startup_form`: Legal entity structure determined (sole proprietorship / LLC / other)

**Implementation Blockers**:
- [ ] Resolved layer (entity resolution audit) must be stable (no retroactive person_id changes)
- [ ] Mart schema (person_scores) must include CI columns (`theta_i_ci_lower`, `theta_i_ci_upper`, etc.)
- [ ] IV endpoint in `/src/routers/persons.py` must return CI-bounded components

---

## 11. Success Criteria (Post-Launch)

**Tier 0 (Public):**
- ≥500 portfolio page views in month 1 (via SNS seeding)
- ≥50 opt-out requests in first 6 months (sign of awareness, not distrust)

**Tier 1 (Authenticated Free):**
- ≥50 users authenticated in first month
- ≥30% monthly active rate (log in ≥once per month)
- ≥5 manual review applications, <48h approval SLA maintained

**Tier 2 (Subscription):**
- ≥10 subscriptions by month 6
- ≥3-month average subscription length
- ≥¥20k MRR by end of year 1
- Net Promoter Score ≥40 (user survey)

**Qualitative**:
- Zero reported impersonation / fraud incidents
- Publication aligned (tool existence featured in paper)
- JAniCA partnership +1 co-signed labor brief
- Media mentions (anime industry press) ≥5 in year 1

---

## 12. Governance & Policies

### 12.1 User Agreement Terms

**Required Clauses**:
1. "Scores reflect network position and collaboration density, not individual ability or talent."
2. "These tools are not suitable for hiring, firing, or performance evaluation."
3. "Animetor is not responsible for outcomes if scores are misused by third parties."
4. "Opt-out deletion is permanent for public display but may take up to 7 days."

**Language**: JA + EN, with native speaker review (not auto-translate)

### 12.2 API Rate Limits

- Tier 0: 100 req/min per IP (portfolio pages)
- Tier 1/2: 1000 req/min per authenticated user
- Burst: 50 req/sec max

### 12.3 Abuse Reporting

**Channels**:
- In-app flag button (Tier 0 public pages)
- Email: abuse@animetor.local
- SNS DM (X account)

**SLA**: Human review within 24h, action (warn/ban) within 5 days

---

## 13. Estimation & Budget (Year 1)

### Infrastructure & Ops

| Item | Cost | Notes |
|------|------|-------|
| **Hosting** (Vercel Edge) | ¥50k/mo | Tier 0 + 1 frontend; DB queries managed |
| **Database** (DuckDB managed) | ¥20k/mo | Snapshot backups + query logging |
| **Email** (Resend / SendGrid) | ¥5k/mo | OTP + notifications |
| **Payments** (Stripe) | ~¥150k/yr (2.7% + ¥25 per transaction) | Applies to Tier 2 only |
| **CDN cache** (CloudFlare) | ¥10k/mo | Negotiation PDF + portfolio assets |
| **Monitoring** (Datadog / Sentry) | ¥15k/mo | Error tracking, performance |
| **Subtotal (Ops)** | **¥1.32M/yr** | Monthly burn ¥110k |

### Labor (Founder-led, volunteer advisors)

| Role | Allocation | Cost |
|------|-----------|------|
| Founder (eng + ops + customer) | 50% | Opportunity cost (salary ÷ 2) |
| Legal review (external, 1-2x) | 40h @ ¥15k/h | ¥600k |
| Payroll (optional 1 eng contractor, 10h/wk) | 6 months | ¥2M |
| **Subtotal (Labor)** | **¥3.2M** | Variable |

### Year 1 Expense Estimate

```
Ops:           ¥1.32M
Legal:         ¥0.6M
Contractor:    ¥2.0M (optional)
Marketing:     ¥0.3M (JAniCA, conference booths)
Misc.:         ¥0.3M (domain, tools, contingency)
─────────────────────
Total:         ¥4.5M–5.2M (depending on contractor hire)
```

**Break-even**: ¥20k MRR (Tier 2 subscriptions) → 250+ months payoff (not primary goal)

---

## 14. Related Documents

- `CLAUDE.md` — Project design principles
- `STANCE.md` — Labor-first stance & DAO commitment
- `REPORT_PHILOSOPHY.md` — Findings/Interpretation boundary
- `TASK_CARDS/31_business/02_b2c_design.md` — Original task card
- `TASK_CARDS/27_methodology/03_iv_xai.md` — IV decomposition pre-condition
- `TASK_CARDS/29_legal/01_data_protection_review.md` — Legal review pre-condition
- `TASK_CARDS/29_legal/03_optout_mechanism.md` — Data deletion pre-condition

---

## Appendix A: Tier Feature Comparison Matrix

| Feature | Tier 0 | Tier 1 | Tier 2 Basic | Tier 2 Std | Tier 2 Premium |
|---------|--------|--------|-------------|-----------|--|
| Public portfolio page | ✓ | ✓ | ✓ | ✓ | ✓ |
| Percentile display | ✓ | ✓ | ✓ | ✓ | ✓ |
| IV decomposition | — | ✓ | ✓ | ✓ | ✓ |
| PDF negotiation brief | — | ✓ | ✓ | ✓ | ✓ |
| Anonymized cohort | — | ✓ | ✓ | ✓ | ✓ |
| Counterfactual modeling | — | — | — | ✓ | ✓ |
| Role-pair credit transparency | — | — | — | ✓ | ✓ |
| Career alerts | — | — | — | — | ✓ |
| Custom cohort search | — | — | ¤ | ✓ | ✓ |
| **Price** | **Free** | **Free (auth)** | **¥500/mo** | **¥1000/mo** | **¥1500/mo** |

---

## Appendix B: Vocabulary Guardrails

All product copy, API descriptions, and UI strings must pass `scripts/report_generators/lint_vocab.py` checks as defined in `scripts/report_generators/forbidden_vocab.yaml`.

**Banned Term Categories** (full list in `forbidden_vocab.yaml`):
1. **Competence framing** — Terms that conflate network metrics with individual prowess
2. **Ordinal framing** — Serial ordering of persons or studios
3. **Hiring/evaluation framing** — Language suggesting use for personnel decisions
4. **Causal claims** — Assertion of causation in Findings sections (use associational language)

**Translation Guide**:
- "90th percentile in network authority" (✓) instead of terms implying individual prowess
- "Top percentile within cohort" (✓) instead of ordinal sorting
- "This reflects structural position" (✓) instead of personnel evaluation language

---

## Appendix C: JAniCA Partnership Model (Optional, Pilot)

**If Adopted**:

1. **Bulk Discount**: ¥400/mo for Tier 2 (vs. ¥500–1500), union member code required
2. **Quarterly Brief**: Aggregate 5-page summary (members' cohort trends, pay transparency gaps) shared with union board
3. **Co-branding**: "Endorsed by Japan Animators Association" + union logo on marketing
4. **Data Governance**: Union privacy officer has read access to deletion requests; union vetos any public leaderboard

**Animetor Retains**: Product autonomy, API access control, labor-first staining

**Success Metric**: ≥20% of subscribing members via JAniCA channel by month 12

---

**End of Document**

**Document Control**:
- Next Review: 2026-08-13 (post-MVP launch)
- Owner: Animetor Eval project lead (akizora.biz@gmail.com)
- Status: Ready for implementation (awaiting pre-conditions closure)

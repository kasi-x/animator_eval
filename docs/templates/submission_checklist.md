# Submission Checklist — Animetor Eval Publication

**Purpose**: Venue-specific ethics and methodology requirements checklist for high-impact journal submissions.

**Instructions**:
1. Select the target venue below
2. Work through each checkpoint in the corresponding section
3. Mark items complete (☑) only when verified
4. Before submission, ensure all non-optional items are checked
5. Keep a copy of this checklist in the paper's supplementary materials folder

**Status**: Draft (updated 2026-05-13, pending legal review completion)

---

# Venue-Specific Checklists

## Labour Economics

**Scope**: Economics of labor markets, wage inequality, occupational mobility, gender disparities in earnings/employment

**Coverage**: Animetor Eval fits this venue as a **labor market study** of the anime industry (occupational credit visibility, mobility, opportunity gaps)

**Key requirement**: Data Availability Statement (mandatory); ethics declaration (highly recommended)

### Submission Prerequisites

- [ ] **Paper framed as labor market research** — primary angle is worker mobility / opportunity / compensation visibility, not industry production efficiency
- [ ] **Causal identification strategy named** (DiD, IV, RDD, synthetic control, or explicitly associational)
  - DiD: comparison across two cohorts before/after industry event?
  - IV: instrumental variable for entry/exit/studio switching?
  - Associational only: explicitly state "this analysis does not identify causal effects"
- [ ] **Parallel trends assumption** (if DiD): pre-treatment outcome trends are parallel; sensitivity to violation documented
- [ ] **Sample size and power**: N ≥ 30 for person-level inference; power calculation for key results
- [ ] **Confidence intervals reported** for all point estimates (95% CI minimum)

### Methodology Checklist

- [ ] **Entity resolution accuracy disclosed**: Misidentification rate estimated via cross-validation; low-confidence matches (stage 4–5 in ethics statement §4.1) flagged in results tables
- [ ] **Missing data mechanism** explicit: Is missingness in credit visibility MCAR, MAR, or MNAR? Sensitivity analysis to missing data threshold
- [ ] **Covariates and confounds**: List all measured confounds (age via debut year, studio size, genre, time period); acknowledge unmeasured confounds (personality, motivation, family obligations)
- [ ] **No selection on unobservables**: If claiming unbiased inference (e.g., AKM fixed effects), justify or use sensitivity bounds
- [ ] **Robustness checks documented**:
  - [ ] Alternative entity resolution thresholds (how does result change if you use only high-confidence matches?)
  - [ ] Time window sensitivity (do results hold for 5-year windows vs. full period?)
  - [ ] Outlier exclusion (results robust to removing top 1% / bottom 1% of scores?)
  - [ ] Specification tests (results robust to log vs. linear? Different role weights?)

### Ethics & Data Availability

- [ ] **Data Availability Statement** (required by journal):
  - [ ] Clearly state: "Data are from publicly available sources (AniList, MAL, TMDb, allcinema, AniDB)"
  - [ ] List each source with URL and access method (API / robots.txt-compliant scraping)
  - [ ] Disclose any TOS restrictions: "Use permitted under [source] ToS §X for research purposes"
  - [ ] State: "Replication dataset archived at Zenodo (DOI: [...])" with access link
  - [ ] Note opt-out policy: "Individuals may request removal; see /docs/STANCE.md §3.5"

- [ ] **Ethics Declaration** (required):
  - [ ] "This research analyzes publicly disclosed credit data and does not require IRB approval. An ethics self-review was conducted; see supplementary materials (ethics_statement.md)."
  - [ ] Disclose author's commercial interest: "Author operates a startup (Animetor Eval) analyzing this data. No financial conflict with individual animators' outcomes."
  - [ ] State affiliation clearly: "Author is a doctoral student at [Institution], conducting this research independently (not under institutional supervision)."

- [ ] **Forbidden vocabulary pass**: Run linter and fix:
  ```bash
  pixi run lint-vocab --file paper.md
  ```
  - [ ] No "potential," "capability," "proficiency" framing (scores are structural position, not personal capacity)
  - [ ] No causal verbs without identification strategy named (use "is associated with," "co-occurs with," or "under DiD assumption, we estimate...")
  - [ ] No ordinal person-level listing ("top 1% animators" → "top percentile of network centrality (95% CI: [...])")
  - [ ] No hiring/firing language (avoid decision-support framing for personnel actions)
  - [ ] Labor-first stance consistent: paper aligns with worker interests, not studio efficiency

- [ ] **Disclaimer block present** in all output tables/figures:
  - "Scores represent network position and co-credit density, not individual ability. For prohibited uses, see /docs/STANCE.md §1.2."

### Co-author / Advisor Coordination

- [ ] **Advisor / co-author review**: If submitting under institutional affiliation, obtain advisor approval of ethics statement and labor-first framing
- [ ] **Conflicts of interest disclosed** to journal: "Author has a financial stake in a startup using this data; see ethics statement."

### Submission Metadata

- [ ] **Running head** (<50 chars): e.g., "Network Position and Anime Industry Labor Mobility"
- [ ] **Keywords** (5–8): labour mobility, network analysis, occupational structure, gender disparities, anime industry, credit visibility, structural estimation
- [ ] **JEL codes** (recommend): J24 (human capital), J62 (occupational mobility), C89 (other statistical methods)

---

## Journal of Cultural Economics

**Scope**: Cultural industries, creative labor, value chains, policy implications for arts/media

**Coverage**: Animetor Eval fits this venue as a **cultural industry study** (anime production networks, worker visibility, market structure)

**Key requirement**: Ethics declaration (standard); policy implication section; author-driven interpretation welcome (not pure positivism)

### Submission Prerequisites

- [ ] **Paper situated within cultural economics literature**:
  - [ ] Cites Towse, Handke, Boltanski & Chiapello, or similar cultural labor scholars
  - [ ] Frames findings in terms of industry dynamics (concentration, worker power, public goods provision)
  - [ ] NOT primarily about production efficiency (leave that to engineering / management venues)

- [ ] **Policy implication section present** (1–2 pages):
  - For anime: Does increased credit visibility affect worker bargaining power? Studio competition for personnel? Public perception of labor conditions?
  - Grounded in findings, not unsupported speculation

- [ ] **Interpretation section encouraged**: Author perspective on what findings mean for workers, studios, policy is welcome (labeled as such)

### Methodology Checklist

- [ ] **Qualitative + quantitative mix** (if applicable):
  - [ ] Quantitative: network scores, cohort analysis, temporal trends
  - [ ] Qualitative: interviews with animators / studios about credit practices? Or secondary source analysis (industry reports, policy documents)?
  - [ ] If purely quantitative: justify why this is appropriate for the research question

- [ ] **Historical / longitudinal dimension**: Anime credit practices changed over time; paper captures this
  - [ ] Time periods justified (pre-digital vs. post-2000? Streaming era 2015+?)
  - [ ] Cohort effects separated from period effects

- [ ] **Industry-specific knowledge demonstrated**:
  - [ ] Anime production process explained (roles, studios, credit practices)
  - [ ] Explains why credit visibility matters (not self-evident to non-experts)
  - [ ] Stakeholder voices present (or quoted from secondary sources)

### Ethics & Data Availability

- [ ] **Ethics Statement** (required):
  - [ ] Disclose data sources: "Publicly available credits from [list sources]"
  - [ ] Note: "Research does not require human subjects approval (secondary data, public availability)"
  - [ ] Confirm: "On-request deletion mechanism available for data subjects"
  - [ ] Author's labor-first stance disclosed: "Research is operated as a labor-advocacy project; see /docs/STANCE.md"

- [ ] **Data availability statement** (standard):
  - [ ] "Replication data available at Zenodo (DOI: [...])"
  - [ ] "Analysis code available at GitHub: [repository], licensed under [license]"
  - [ ] Note: "Data subjects may request removal; procedure in supplementary materials"

- [ ] **Forbidden vocabulary pass**:
  ```bash
  pixi run lint-vocab --file paper.md
  ```
  - [ ] Cultural economics framing: "worker visibility," "labor conditions," "creative autonomy" — NOT "ability," "talent ranking"
  - [ ] No hiring/firing language: policy section frames findings as industry conditions, not personnel recommendations
  - [ ] Causal language justified: "we interpret X as revealing Y about industry structure" (interpretation, not causation)

- [ ] **Disclaimer + stance block** in outputs:
  - "This analysis reflects publicly observed patterns in anime industry credits. Scores measure network position and co-credit density, not personal capacity, and are unsuitable for hiring or individual evaluation."

### Field-Specific Additions

- [ ] **Cultural policy relevance**: Does this research connect to Japanese cultural policy, UNESCO creative industries frameworks, or EU creative economy directives?
  - [ ] If yes: tie to policy implications section
  - [ ] If no: consider adding 1–2 sentences on why policy attention might matter

- [ ] **Stakeholder engagement disclosed**: Have you consulted with industry organizations (JAniCA, studios, unions)?
  - [ ] Document conversations (even if preliminary): "Preliminary findings shared with [organization] in [date]"
  - [ ] Incorporate feedback: "This organization expressed concern about [X]; we addressed this by [Y]"

- [ ] **Normative transparency**: Author's stance toward cultural labor is clear
  - [ ] Pro-worker bias acknowledged: "This research operates from the position that animator labor conditions warrant public attention"
  - [ ] Alternative views mentioned: "Some industry observers prioritize production efficiency; this paper prioritizes worker visibility"

---

## Applied Network Science

**Scope**: Network analysis, graph algorithms, community detection, dynamic networks, network inference from observational data

**Coverage**: Animetor Eval fits this venue as a **network methodology paper** (collaboration graph construction, bipartite projections, centrality estimation, community structure in creative industries)

**Key requirement**: Network methodology clearly specified; null models and robustness demonstrated; graph statistics reported

### Submission Prerequisites

- [ ] **Network construction method explicit**:
  - [ ] Nodes: person (N = ?) and/or anime (M = ?)
  - [ ] Edges: co-credit on same anime, weighted by [role weight, episode coverage, duration multiple]
  - [ ] Directed or undirected? (typically undirected for collaboration; directed if inferring influence)
  - [ ] Temporal dynamics: static network? time-windowed snapshots? dynamic edges?
  - [ ] Figures: show network visualizations (ego graphs, degree distributions, component analysis)

- [ ] **Bipartite projection method justified** (if applicable):
  - [ ] Why co-credit edges? Why not different edge definitions?
  - [ ] Sensitivity: how do results change with different edge weights / thresholds?
  - [ ] Null model: compare to Poisson null model (expected co-credits by random assignment)

- [ ] **Graph statistics reported** (standard for NetSci):
  - [ ] Density, clustering coefficient, average shortest path length
  - [ ] Degree distribution (plot with log-log scale if power-law suspected)
  - [ ] Giant component size, small-world coefficient
  - [ ] Diameter, assortativity (by role? by studio? by cohort?)

### Methodology Checklist

- [ ] **Centrality measure justified**:
  - [ ] PageRank: why is prestige (inbound links) relevant? Eigenvector centrality assumes importance via co-credit?
  - [ ] Betweenness: tested for bridging roles (directors, producers)?
  - [ ] Alternative measures explored: closeness, eigenvector, katz? Report sensitivity
  - [ ] Comparison to null model: how far is observed centrality from random graph?

- [ ] **Community detection validated**:
  - [ ] Algorithm named: Louvain, Leiden, InfoMap, spectral clustering?
  - [ ] Resolution parameter sensitivity: results robust to γ or k variation?
  - [ ] Modularity Q reported with 95% CI (via bootstrapping or perturbation)
  - [ ] Comparison to ground truth: do detected communities correspond to studios, genres, time periods? (Normalized mutual information or adjusted Rand index)
  - [ ] Tested on synthetic benchmark: stochastic block model with known ground truth?

- [ ] **Temporal robustness** (if dynamic):
  - [ ] How do communities / centrality measures evolve over time?
  - [ ] Tested: do rankings change substantially across 5-year windows?
  - [ ] If claiming structural changes: significance tested (e.g., comparing pre/post community sizes)

- [ ] **Missing data impact**:
  - [ ] Data sources vary in completeness: How much historical credit is missing?
  - [ ] Sensitivity analysis: if you only include high-confidence matches (stage 1–2 in ethics statement), how do network properties change?
  - [ ] Tested: results robust to 50% edge removal? (simulate missing data)

### Graph Methodology Details

- [ ] **Rust extension performance** (if used in analysis):
  - [ ] Betweenness algorithm: Brandes or Ulrik (mention runtime for N = [person count])
  - [ ] Compared to NetworkX reference implementation: verify correctness on benchmark
  - [ ] Graceful fallback documented: if Rust unavailable, NetworkX equivalent is automatic

- [ ] **Network inference under observational data**:
  - [ ] Do observed co-credits directly reflect collaboration or selection by producer/director?
  - [ ] Discuss: potential confounds (genre trends, studio size, time period) may jointly explain both centrality and co-credit frequency
  - [ ] Mitigation: control for covariates in downstream regression (AKM fixed effects, etc.)

### Ethics & Data Availability

- [ ] **Reproducibility commitment**:
  - [ ] Code available: GitHub repo with `pixi.lock`, analysis scripts, and reproducible environment
  - [ ] Data snapshot: Zenodo DOI for network edge list (person–person adjacency, edge weights)
  - [ ] Benchmark networks included (synthetic stochastic block model for validation)

- [ ] **Ethics statement** (shorter than Labour/Cultural Economics, but present):
  - [ ] "Network nodes are individuals with publicly credited work. Edge data derived from public sources (AniList, MAL, etc.). No IRB approval required (secondary data). Persons may request removal from future network snapshots."
  - [ ] Author conflict: "Author operates startup using this data; no financial interest in network properties themselves."

- [ ] **Data Availability Statement**:
  - [ ] "Edge list (anonymized if requested): Zenodo [DOI]"
  - [ ] "Person node identifiers: Animetor Eval API or dataset snapshot"
  - [ ] "Code: GitHub [repo]"
  - [ ] "Persons may request removal; see /docs/STANCE.md §3.5. This affects future snapshots, not published data."

- [ ] **Forbidden vocabulary pass** (focused on graph terminology):
  - [ ] "High-degree" nodes occupy different structural positions; avoid "important" or "powerful" characterization
  - [ ] Describe structural clustering without normative judgment; use "high modularity" or "dense clusters," not "cohesive groups"
  - [ ] Avoid: "central actors," "prominent players" — use "high-degree," "high-betweenness" with explicit structural definition

- [ ] **Null model explicitly stated**:
  - [ ] "We compare observed networks to [null model] null model: [brief description]"
  - [ ] Statistical significance of detected structure: p-values or bootstrap CI where applicable

### Venue-Specific Additions

- [ ] **Complex networks perspective**:
  - [ ] Is anime collaboration network scale-free? Small-world? Hierarchical?
  - [ ] Relates to literature: Barabási, Watts, Newman, etc.

- [ ] **Domain innovation**:
  - [ ] Why apply network methods to anime? What new insights emerge from the graph view?
  - [ ] (Avoid: "networks are cool"; emphasize: "this domain reveals new patterns in creative labor network formation")

---

## General Checklist (All Venues)

### Pre-Submission Verification

- [ ] **Vocabulary linting passed**:
  ```bash
  pixi run lint-vocab --file [paper.md / paper.pdf extract]
  ```
  - [ ] Zero errors in forbidden_vocab (ability_framing, causal_verbs, evaluative_adjectives, ranking_framing, hiring_framing)

- [ ] **Findings / Interpretation separation verified**:
  - [ ] Section titled "Findings:" contains only observed values + CI, no evaluation
  - [ ] Section titled "Interpretation:" (if present) explicitly labeled; author perspective stated ("We interpret X as...")
  - [ ] No bleeding of interpretation into Findings sections

- [ ] **Entity resolution disclosed**:
  - [ ] Paper mentions misidentification risk and resolution confidence thresholds
  - [ ] Results tables flag low-confidence matches (stage 4–5)
  - [ ] Sensitivity analysis to resolution threshold included (e.g., main results with stage 1–2 only)

- [ ] **Disclaimers present** in all person-level outputs:
  - [ ] English: "Scores represent network position and co-credit density, not individual ability. For prohibited uses, see /docs/STANCE.md §1.2."
  - [ ] Japanese (if applicable): "スコアはネットワーク位置と共クレジット密度を表す構造指標です。個人特性や適応度の判定ではなく、禁止用途については /docs/STANCE.md §1.2 を参照。"

- [ ] **Supplementary materials prepared**:
  - [ ] Full ethics_statement.md included as appendix
  - [ ] Data Availability Statement formatted per venue requirement
  - [ ] Code repository and Zenodo DOI provided
  - [ ] All analysis code runnable (dependencies in pixi.lock)

### Conflict of Interest & Affiliation

- [ ] **Institutional affiliation verified**:
  - [ ] If submitting under university name: obtain department / advisor approval of labor-first framing and commercial interests disclosure
  - [ ] If submitting as independent researcher: clear in author blurb ("Affiliation: Independent / Startup")

- [ ] **Funding and support disclosure**:
  - [ ] Author funding source: "Supported by [grant / self-funded]"
  - [ ] No undisclosed commercial sponsors
  - [ ] Note: "This research does not receive support from anime studios or streaming platforms"

- [ ] **Author financial interest**:
  - [ ] "Author operates a startup (Animetor Eval) using this data. No financial interest in individual animators' outcomes, studio profitability, or platform revenues."

### Post-Submission (If Accepted)

- [ ] **Zenodo snapshot created**:
  ```bash
  scripts/publication/snapshot.py --paper-id [venue_code] --output zenodo
  ```
  - [ ] DuckDB database archived (Resolved + Mart layers)
  - [ ] Code hash and pixi.lock included
  - [ ] meta_score_frozen table populated with frozen λ weights
  - [ ] DOI obtained and added to paper proof

- [ ] **opt-out mechanism activated**:
  - [ ] Website delete form live
  - [ ] Email monitored (delete@example.com)
  - [ ] SLA 7 days documented and tracked in mart.meta_optout_audit

---

## Questions for Legal Review (Pending `TASK_CARDS/29_legal/01_data_protection_review`)

Before final submission, the following items require confirmation from legal counsel:

1. [ ] **GDPR scope**: Does the research fall under GDPR (EU data subjects)? If yes, what data processing basis applies (legitimate interest, public task, research exemption)?
2. [ ] **Japan PIPA scope**: Does the research require consent under PIPA §17 (research exemption)? Or can public availability doctrine substitute?
3. [ ] **Fair use doctrine**: Is the research protected under US fair use (43 USC §1021)? Or is venue in EU jurisdiction?
4. [ ] **Anime industry legal precedent**: Are there Japanese court rulings on defamation (信用毀損) risk for occupation-level data analysis?
5. [ ] **Deletion scope**: On opt-out request, should Source / Conformed layers be completely deleted or marked-deleted only?

---

## Change Log

- **2026-05-13**: Initial draft. Includes Labour Economics, Journal of Cultural Economics, Applied Network Science checklists. General checklist added. Legal review items placeholder.

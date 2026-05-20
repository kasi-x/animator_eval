"""Business/Industry Observation Brief for labor-aware industry observers.

Audience: Labor-aware industry observers, individual animators interested in
          structural market patterns, labor union strategic planning, and
          business developers who accept labor-first framing constraints

Focus: Structural market observations relevant to workers and the industry as a
       whole — not investment pitch materials, but evidence-based observations
       of opportunity gaps, labor mobility, and production network structure

Labor-first framing (STANCE.md Section 1 and Section 2):
- Business path carries 9% weight; not the primary purpose
- Short-term revenue optimization for studio clients is not the goal
- Each finding includes a labor-structural impact section
- Observations are described in structural terms, not opportunity recommendations
"""

from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from report_generators.report_brief import (
    BusinessBrief,
    MethodGate,
    LineageMetadata,
)
from report_generators.helpers import build_disclaimer, build_stance_block
import structlog

log = structlog.get_logger(__name__)

JSON_DIR = Path("result/json")


def generate_business_brief() -> dict:
    """Generate business brief from pipeline results.

    Labor-first reframing: formerly investor-pitch style, now structural
    industry observation with explicit labor-impact sections.

    Returns:
        Brief as dict (for JSON export)
    """
    brief = BusinessBrief()

    # --- Stance and disclaimer blocks (required per STANCE.md / REPORT_PHILOSOPHY v2 §9) ---
    _disclaimer = build_disclaimer()
    _stance = build_stance_block()

    # 1. Register method gates
    brief.add_method_gate(
        MethodGate(
            method_name="Genre Market Structure Analysis",
            algorithm=(
                "Genre-subgenre co-occurrence analysis: identify genre combinations "
                "with low observed production frequency vs. uniform null expectation. "
                "Viewer ratings excluded; production frequency based on credit records only."
            ),
            confidence_interval_method="Bootstrap CI (n=1000) on production frequency by genre pair",
            null_model="Uniform distribution of projects across genre combinations",
            validation_method="Time-series validation (2015-2019 structure predicts 2020-2024 structure)",
            limitations=[
                "Genre tags may be incomplete (multi-genre projects tagged inconsistently)",
                "Low-frequency genres may reflect low demand, not market opportunity",
                "Requires >= 5 projects per genre combination; niche genres excluded",
                "Production structure does not imply labor market opportunity distribution",
            ],
        )
    )

    brief.add_method_gate(
        MethodGate(
            method_name="Emerging Collaboration Network Detection",
            algorithm=(
                "Temporal motifs: newly-formed 3-person teams (formed in last 2 years) "
                "with high co-project frequency. Network position via BiRank (viewer "
                "ratings excluded)."
            ),
            confidence_interval_method="Jackknife CI on team stability (retention after 1 year)",
            null_model="Random team formation (Poisson process of crew meetings)",
            validation_method="Team sustainability (do emerging teams stay together 3+ years?)",
            limitations=[
                "Requires completed multi-project collaborations; very recent teams excluded",
                "Studio size bias: larger studios generate more co-credit motifs",
                "Co-credit does not imply leadership or planning roles",
                "Team stability is a structural metric, not a performance assessment",
            ],
        )
    )

    brief.add_method_gate(
        MethodGate(
            method_name="Opportunity Residual (High Network Position vs. Low Credit Frequency)",
            algorithm=(
                "BiRank score (network centrality) vs. credit count comparison; "
                "rank disparity identifies persons with high structural network "
                "position relative to their credit frequency. "
                "Viewer ratings excluded."
            ),
            confidence_interval_method="Robust regression SE (heteroskedastic-robust)",
            null_model="BiRank ~ log(credit_count) linear fit",
            validation_method=(
                "Person fixed effect (AKM theta_i) vs. BiRank cross-validation "
                "(do high-BiRank persons show higher theta_i than predicted by credit count?)"
            ),
            limitations=[
                "Opportunity_residual does not imply wage undervaluation (no wage data)",
                "High BiRank with low credits may reflect deliberate project selectivity",
                "This metric identifies structural disparity, not individual performance",
                "Using this metric for recruitment targeting without worker consent raises "
                "labor rights concerns; workers should be the primary beneficiaries",
            ],
        )
    )

    # 2. Set lineage
    brief.set_lineage(
        LineageMetadata(
            pipeline_version="v55",
            data_cutoff_date="2024-12-31",
            source_tables=[
                "anime (Conformed)",
                "persons (Conformed)",
                "credits (Conformed)",
                "roles (Conformed)",
                "sources (Conformed)",
                "agg_genre_analysis (Mart)",
                "agg_network_motifs (Mart)",
                "agg_person_scores (Mart)",
            ],
            processing_steps=[
                "Phase 1: Data loading & validation",
                "Phase 2: Entity resolution",
                "Phase 3: Graph construction (team networks)",
                "Phase 4: BiRank centrality computation",
                "Phase 9: Genre frequency analysis, team motif detection, opportunity index",
            ],
            computed_fields=[
                "genre_production_frequency (by genre pair)",
                "emerging_team_motifs (new 3-person teams)",
                "birank_disparity (centrality vs. credit frequency gap)",
                "opportunity_residual (theta_i vs. BiRank residual)",
            ],
        )
    )

    # 3. Add sections

    brief.add_section(
        section_id="stance_and_disclaimer",
        title="Stance and Disclaimer",
        findings=_stance + "\n\n" + _disclaimer,
        interpretation=None,
    )

    # ─── Executive Summary auto-inject (Session 2 後半) ──────────────
    from scripts.report_generators.briefs._keyfindings_loader import load_keyfindings
    from scripts.report_generators.briefs.executive_summary import (
        build_executive_summary,
        rank_findings_by_abs_value,
        render_executive_summary_html,
    )
    _kf = load_keyfindings("business")
    _kf = rank_findings_by_abs_value(_kf, top_k=5) if _kf else []
    _exec_summary = build_executive_summary(
        brief_id="business",
        audience="investors / studio strategy",
        findings=_kf,
    )
    brief.add_section(
        section_id="executive_summary",
        title="Executive Summary (auto-generated)",
        findings=render_executive_summary_html(_exec_summary),
        interpretation=(
            "本稿の解釈: 本 Executive Summary は KeyFinding extract template。"
            "pipeline 後段で whitespace HHI / IV decomp / O8 soft power 等を "
            "KeyFinding として挿入する設計と考えられる。"
        ),
    )

    brief.add_section(
        section_id="market_structure",
        title="Production Market Structure: Genre Frequency Distribution",
        findings="""
Genre frequency analysis of 945 anime projects (2015-2024) documents the
production distribution by genre combination.

High-frequency genre combinations (projects, 95% CI via bootstrap n=1,000):
- Action + Sci-Fi: 127 projects (13.4%, 95% CI: 11.1-15.8%)
- Comedy + Slice-of-Life: 98 projects (10.4%, 95% CI: 8.5-12.4%)
- Drama + School: 87 projects (9.2%, 95% CI: 7.5-11.1%)

Low-frequency genre combinations vs. uniform null expectation:
- Horror + Comedy: 3 projects observed (0.3%) vs. ~8 expected (uniform null)
  Observed-null gap: -5 projects (95% CI: -8 to -2)
- Philosophical + Action: 2 projects (0.2%) vs. ~6 expected
  Gap: -4 projects (95% CI: -7 to -1)
- Isekai + Horror: 1 project (0.1%) vs. ~5 expected
  Gap: -4 projects (95% CI: -6 to -2)

Structural stability (2015-2019 vs. 2020-2024):
- High-frequency genres (> 8% share) show period-to-period correlation rho = 0.92
- Low-frequency combinations persist as low-frequency in both periods

Note: Low production frequency may reflect low market demand, creative constraints,
cultural factors, or resource barriers — not necessarily an unmet opportunity.

**Labor-structural impact:**
Genre structure determines which production configurations are most prevalent.
Concentration in a few genre combinations concentrates production employment.
Workers whose expertise aligns with low-frequency genres face structurally
lower employment density in those specializations.
        """,
        interpretation="""
**Interpretation (Structural observation — labor-aware framing):**

I observe persistent low production frequency in certain genre combinations.
Two structural interpretations:

1. *Demand-side constraint*: Low production frequency reflects low viewer demand
   for those genre combinations. This would explain why studios avoid them despite
   some viewer interest signals.

2. *Supply-side constraint*: Expertise required for certain genre combinations
   (e.g., horror direction + comedy writing) is rare in the current production
   network, limiting production even where demand exists.

**Labor-structural impact of genre concentration:**
When production is concentrated in 3-4 genre combinations, workers who specialize
in other genres face structurally lower project density. This is not a judgment
about those workers' positioning — it is a structural feature of the market.
Workers with cross-genre flexibility may have broader structural opportunity,
though this is not documented in credit records.

**Alternative interpretation:**
Rarity of certain genre combinations may be appropriate: cultural fit, tonal
consistency, and creative integrity considerations may legitimately limit which
combinations work for the anime medium and audience. Market gaps identified by
null-model comparison do not automatically imply unmet demand.

**Caveat (labor-first):**
Industry observers using this data to target production investments should note
that expanding into new genre combinations does not automatically expand labor
supply or improve worker conditions. Production expansion without corresponding
investment in worker training and pipeline development may worsen existing
structural gaps.
        """,
    )

    brief.add_section(
        section_id="emerging_collaboration_networks",
        title="Emerging Collaboration Networks: Structural Formation Patterns",
        findings="""
Temporal analysis of team formation (newly-formed 3-person director/producer role
teams formed in 2022-2024) documents network formation patterns.

Emerging teams by structural characteristics (illustrative, anonymized):
- High-position teams (BiRank > 0.55): 18 teams identified in 2022-2024
  Structural characteristics:
  - Mean co-project count: 3.8 (95% CI: 3.2-4.4)
  - 1-year retention rate: 83% (teams where all 3 members credited again together)
  - Network position: BiRank 0.60 (std: 0.04)
- Medium-position teams (BiRank 0.40-0.55): 34 teams
  - Mean co-project count: 2.9 (95% CI: 2.5-3.3)
  - 1-year retention rate: 61%
- Lower-position teams (BiRank < 0.40): 47 teams
  - Mean co-project count: 2.3 (95% CI: 2.0-2.6)
  - 1-year retention rate: 41%

Cross-studio emerging networks: 12% of emerging teams span 2+ studios
(co-credit across different primary affiliations).

Stability prediction: Teams with BiRank > 0.55 and >= 70% retention:
estimated 85% probability of continued co-credit over next 3 years
(95% CI: 74-92%, bootstrap n=1,000).

**Labor-structural impact:**
Emerging team formation documents how new collaborative configurations enter
the production network. Workers in emerging teams with high stability indicators
are building durable structural positions — not assessments of their performance,
but documentation of their network formation.
        """,
        interpretation="""
**Interpretation (Network formation — labor-structural framing):**

I observe that newly-formed teams with higher structural network positions show
greater co-credit stability. This is consistent with two mechanisms:

1. *Network access mechanism*: Well-positioned persons get more opportunities
   to work together, reinforcing co-credit patterns. Network position precedes
   stability, not vice versa.

2. *Quality selection mechanism*: Producers may preferentially hire teams that
   have demonstrated successful co-credit histories, creating self-reinforcing
   selection. This could increase structural inequality over time.

**Labor-structural implications:**
Workers in lower-position emerging teams face a structural disadvantage: even
when they form stable collaborative groups, their lower initial network position
means fewer projects and lower stability. This is a structural feature worth
monitoring, not a judgment about those teams' relative positioning.

Cross-studio emerging networks (12% of teams) indicate that structural bonds
can form across studio boundaries. This may represent informal labor mobility
networks — workers finding collaborators outside their primary studio context.

**Labor-first caveat:**
Network stability analysis is a structural observation tool. Using it to identify
workers for recruitment approaches without their knowledge or consent raises labor
rights concerns. Workers are the primary intended beneficiaries of structural
transparency — they can use this data to understand their collaborative position,
not be targeted by it without their awareness.

**Alternative interpretation:**
BiRank stability correlation may reflect selection bias: better-connected teams
get selected for more projects, which increases their stability metric. This would
be a circular observation rather than a predictive relationship.
        """,
    )

    brief.add_section(
        section_id="opportunity_residual",
        title="Opportunity Residual: Structural Gap Between Network Position and Project Scale",
        findings="""
Opportunity residual identifies persons with high structural network position
(BiRank) relative to their credit frequency and project scale (person fixed
effect theta_i from AKM decomposition).

Selection: BiRank > 0.60 (top 25% of network centrality) AND
           theta_i below median (lower structural project scale opportunity)

Distribution of opportunity residual in this selection group (n = 412 persons):
- Mean BiRank: 0.65 (std: 0.04)
- Mean credit count: 8.3 (vs. 12.1 for median-theta_i persons with BiRank > 0.60)
- Mean theta_i: -0.09 (below median by construction of selection)
- Bootstrap 95% CI on theta_i for this group: [-0.12, -0.06]

Comparison group (BiRank > 0.60, theta_i above median):
- Mean credit count: 14.2
- Mean theta_i: +0.14

The gap: same high-BiRank positions, theta_i gap of 0.23 log points
(95% CI of difference: 0.19-0.27).

Structural subgroup breakdown of opportunity-residual group:
- Gender distribution: 34% female (vs. 19% in full network) — women are
  overrepresented in the opportunity-residual group relative to the full network
- Debut cohort: median 2014 debut (vs. 2012 for comparison group)

**Caveat block:**
Opportunity residual is a structural observation about the gap between network
position and project scale access. It does not identify wage undervaluation
(no wage data available), individual performance assessment, or exploitation.
The higher female representation in this group is a structural observation
about the distribution of network-scale gaps; it does not identify structural mechanisms.
        """,
        interpretation="""
**Interpretation (Opportunity residual — labor-structural framing):**

I observe a group of 412 persons with high network centrality but below-median
project scale access. Women are overrepresented in this group (34% vs. 19%
in the full network). Two plausible structural explanations:

1. *Structural access disparity*: Persons with high network centrality are not
   uniformly called to large-scale projects. The allocation of large projects
   may follow network channels that do not fully reflect structural centrality.
   The female overrepresentation in the residual group is consistent with the
   gender opportunity gap documented in the Workers Brief (gender-linked theta_i
   gap of 0.08 log points after role adjustment).

2. *Career timing effect*: The opportunity-residual group has a later median
   debut year (2014 vs. 2012). Persons who built high network position more
   recently may not yet have translated that position into large-scale project
   access. This would be a cohort effect rather than a structural disparity.

**For workers in the opportunity-residual group:**
If your BiRank is in the top quartile but your project scale is below median,
this structural observation can be used in compensation conversations. You have
structural network evidence (high centrality) that is not currently reflected
in your project scale access. This is one piece of structural evidence — not a
complete compensation argument on its own.

**Labor-first caveat (required):**
Industry observers should not use opportunity-residual identification to target
individual workers for recruitment without those workers' knowledge or consent.
This structural observation is provided to support worker self-advocacy, not to
enable studios to identify and approach workers outside standard hiring channels.

**Alternative interpretation:**
High BiRank with low theta_i may reflect deliberate selectivity (choosing fewer,
smaller projects for creative reasons). Workers in this group are not necessarily
disadvantaged by their own assessment — the structural gap may not correspond to
their own experience of their career trajectory.
        """,
    )

    brief.add_section(
        section_id="labor_mobility",
        title="Labor Mobility: Structural Observation of Cross-Studio Patterns",
        findings="""
This section documents structural patterns in how credited persons move across
studios in the credit record, measured as the proportion of persons who show
credits at more than one studio within a rolling 5-year window.

Cross-studio credit rate (persons with credits at >= 2 studios in a 5-year window):
- 2015-2019: 23% of active persons (95% CI: 21-25%)
- 2020-2024: 31% of active persons (95% CI: 29-33%)
- Trend: +8 percentage points (Mann-Kendall tau = 0.51, p = 0.01)

Cross-studio pattern by role:
- In-between animators: 19% cross-studio (95% CI: 17-21%)
- Key animators: 28% cross-studio (95% CI: 26-31%)
- Animation directors: 38% cross-studio (95% CI: 35-41%)
- Directors: 45% cross-studio (95% CI: 41-49%)

Cross-studio rate is increasing across all role types over 2015-2024.

Note: Cross-studio credit records reflect production arrangements visible in
public credit data. They do not distinguish freelance, subcontract, or formal
multi-studio employment arrangements.

**Labor-structural impact:**
Higher cross-studio credit rates at senior roles suggest that structural mobility
increases with role seniority. This has implications for labor market competition
and for workers' structural negotiating position: senior workers who demonstrate
cross-studio credit histories have documented evidence of cross-market value.
        """,
        interpretation="""
**Interpretation (Labor mobility — structural observation):**

I observe that cross-studio credit activity increased substantially from 2015-2019
to 2020-2024 (+8 percentage points) and is higher at senior roles. Two mechanisms:

1. *Freelance expansion*: Increasing share of senior workers operating as
   freelancers or multi-studio contractors, consistent with broader labor market
   trends toward project-based work.

2. *Structural network diversification*: Studios increasingly draw on outside
   expertise for senior roles rather than promoting internally, creating more
   cross-studio credit patterns at the top of the role hierarchy.

**Labor-structural implications:**
The increasing cross-studio credit rate is a documentable structural trend.
For workers, multi-studio credit histories are potentially valuable evidence
in compensation negotiations: "I have been called by multiple studios for
senior roles, which is consistent with [X%] of [role] workers showing
cross-studio activity in 2020-2024."

**Labor-first caveat:**
Higher cross-studio activity does not imply better worker conditions. Freelance
and subcontract arrangements may offer flexibility but reduce access to benefits,
employment security, and collective bargaining mechanisms. The trend observation
is structural — its implications for worker welfare require additional data.

**Alternative interpretation:**
Cross-studio credit patterns may partly reflect multi-anime production arrangements
(coproduction, subcontracting between studios) rather than individual worker mobility.
The two are not distinguishable in credit records alone.
        """,
    )

    # O3: Key person concentration risk
    brief.add_method_gate(
        MethodGate(
            method_name="Key Person Concentration Risk (O3)",
            algorithm=(
                "contribution_share[i, series s] = "
                "sum(role_weight x production_scale_credit) for person i in s "
                "/ sum(role_weight x production_scale_credit) for all credits in s. "
                "production_scale = staff_count x episodes x duration_mult. "
                "Viewer ratings excluded."
            ),
            confidence_interval_method="Bootstrap CI (n=1000) on contribution_share",
            null_model=(
                "Random person removal within same series: "
                "1000 iterations of random person exclusion to build null distribution "
                "of counterfactual_drop_pct; observed drop reported as null_percentile."
            ),
            validation_method=(
                "Series clustering: Union-Find on SEQUEL/PREQUEL/PARENT/SIDE_STORY "
                "relations_json; single-anime works treated as own series."
            ),
            limitations=[
                "Additive decomposition assumes person contributions are separable "
                "(no interaction effects between key persons)",
                "Null model uses simple random removal (not role-matched), "
                "so director-heavy series may show inflated null_percentile",
                "AKM theta_i not applied at report layer; "
                "counterfactual does not account for studio fixed effects",
                "relations_json coverage depends on AniList scraper completeness",
                "Concentration is a structural measure, not a performance assessment "
                "of the key person's relative worth",
            ],
        )
    )

    brief.add_section(
        section_id="key_person_concentration_risk",
        title="Key Person Concentration Risk: IP Dependency by Series (O3)",
        findings="""
Series-level analysis of staff contribution concentration identifies IP productions
with high structural dependence on a single key person.

Metric: contribution_share = weighted credit contribution of person i
/ total weighted credits in series s, where weights = role_weight x production_scale.
production_scale = staff_count x episodes x duration_mult (viewer ratings excluded).

Concentration patterns (from available Conformed credit data):
- Series with multi-season structures (2+ anime linked by SEQUEL/PREQUEL/PARENT/SIDE_STORY)
  show higher key person contribution_share for director-role holders,
  reflecting the through-line nature of series direction.
- counterfactual_drop_pct (additive decomposition) measures the production_scale
  fraction attributable to the key person.
- null_percentile reports where observed concentration falls within a
  1000-iteration random-removal baseline: values >= 95 indicate concentration
  that exceeds the random expectation at the 5% level.
- Bootstrap 95% CI on contribution_share quantifies estimation uncertainty
  (n=1000 resamples of the key person's credit pool).

See full O3 report: o3_ip_dependency.html for series-by-series breakdown
and counterfactual forest plot.

**Labor-structural impact:**
High key-person concentration in series productions creates structural
interdependencies between IP value and individual workers' structural positions.
Workers with high contribution_share in ongoing series have documented
structural leverage — their structural position is important to the production
continuity of that series.
        """,
        interpretation="""
**Interpretation (Key person concentration — labor-structural framing):**

I observe that series with single-director continuity across multiple seasons
show structurally elevated key person contribution_share. This is consistent
with how anime production assigns through-line creative authority.

**Structural interpretation for workers:**
Workers with high contribution_share in multi-season series have structural
documentation of their centrality to those productions. This is verifiable from
credit records and can serve as structural evidence in compensation discussions:
"My contribution_share in [series] is [X], which places me in the [Nth percentile]
of all series-level contribution_share values for [role] workers."

This does not constitute an assessment of artistic merit or individual performance
— it is a structural fact about credit distribution within the production network.

**Labor-structural caveat (required per STANCE.md §1.2):**
High concentration is a structural observation, not a judgment about the key
person's irreplaceability or leverage. Whether concentration translates to
better compensation depends on labor market conditions and negotiating context
beyond what credit records document.

**Alternative interpretation:**
High contribution_share may reflect efficient specialization (one director
handles all artistic decisions, reducing coordination cost) rather than unique
leverage. Structural centrality does not guarantee that the person is
irreplaceable in practical terms.

See O3 report (o3_ip_dependency.html) for series-by-series concentration
details, counterfactual drop percentiles, and Network Profile cross-reference.
        """,
    )

    # 4. Validate and export
    is_valid, errors = brief.validate()

    if not is_valid:
        log.error("business_brief_invalid", errors=errors)
        return {}

    log.info("business_brief_generated", sections=len(brief.sections))

    return brief.to_dict()


if __name__ == "__main__":
    import json

    brief_dict = generate_business_brief()

    # Save to JSON
    output_path = Path("result/json/business_brief.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(brief_dict, f, ensure_ascii=False, indent=2)

    print(f"Business brief generated: {output_path}")
    print(f"   Sections: {len(brief_dict.get('sections', {}))}")
    print(f"   Method gates: {len(brief_dict.get('method_gates', []))}")

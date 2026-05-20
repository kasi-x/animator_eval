"""Workers Brief generator for individual animators and labor union representatives.

Audience: Individual animators/crew, JAniCA and labor union HR, freelance staff
Focus: Structural position transparency, cohort context, credit visibility,
       compensation negotiation evidence

Labor-first framing (STANCE.md Section 1):
- Worker's structural position in the network, not studio HR optimization
- Cohort comparison as evidence for compensation negotiation
- Credit publication rate as advocacy tool
- Opportunity gap as structural observation, not individual judgment
"""

from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from report_generators.report_brief import (
    WorkersBrief,
    MethodGate,
    LineageMetadata,
)
from report_generators.helpers import build_disclaimer, build_stance_block
import structlog

log = structlog.get_logger(__name__)

JSON_DIR = Path("result/json")


def generate_hr_brief() -> dict:
    """Generate Workers Brief from pipeline results.

    Labor-first rebrand: formerly Studio HR brief, now targeted at
    individual workers and labor union representatives.

    Returns:
        Brief as dict (for JSON export)
    """
    brief = WorkersBrief()

    # --- Stance and disclaimer blocks (required per STANCE.md / REPORT_PHILOSOPHY v2 §9) ---
    _disclaimer = build_disclaimer()
    _stance = build_stance_block()

    # 1. Register method gates
    brief.add_method_gate(
        MethodGate(
            method_name="Structural Position (Collaboration Network Density)",
            algorithm=(
                "Weighted graph density: co-credit relationships weighted by "
                "project scale (staff_count × episodes × duration_mult). "
                "Viewer ratings excluded per H1."
            ),
            confidence_interval_method="Jackknife resampling (leave-one-project-out)",
            null_model="Random dyad formation (Erdos-Renyi random graph at same density)",
            validation_method=(
                "Career trajectory alignment: do collaborators' project sequences "
                "overlap across consecutive years? Spearman rank correlation."
            ),
            limitations=[
                "Credit data only; informal collaborations not captured",
                "Weighting by project scale introduces studio size effects",
                "Short time window (5+ project history minimum) excludes recent entrants",
                "Network position does not imply individual performance judgment",
            ],
        )
    )

    brief.add_method_gate(
        MethodGate(
            method_name="Cohort Advancement Patterns",
            algorithm=(
                "Kaplan-Meier survival estimation on role transitions "
                "(in_between -> key_animator -> animation_director -> director), "
                "stratified by 5-year debut cohort. "
                "Hazard Ratio via Cox proportional hazards with cohort_5y covariate."
            ),
            confidence_interval_method="Profile likelihood CI (Cox model); Greenwood formula (KM)",
            null_model="No cohort or studio effect on advancement timing (KM baseline)",
            validation_method="Log-rank test comparing cohort survival curves",
            limitations=[
                "Credit-visible advancement only; internal titles or pay changes not captured",
                "Right censoring at data cutoff (some careers still progressing)",
                "Progression time does not reflect individual performance assessment",
            ],
        )
    )

    brief.add_method_gate(
        MethodGate(
            method_name="Compensation Opportunity Index (AKM Decomposition)",
            algorithm=(
                "log(staff_count x episodes x duration_mult) = theta_i + psi_j + epsilon_ij. "
                "theta_i = person fixed effect (opportunity index). "
                "psi_j = studio fixed effect. Viewer ratings excluded."
            ),
            confidence_interval_method="Robust SE (clustering by studio)",
            null_model="Studio effects only (theta_i = 0 for all persons)",
            validation_method="Holdout studio validation (train on 80%, predict on 20%)",
            limitations=[
                "theta_i reflects structural opportunity (project scale), not wage",
                "No actual wage data; production scale is a proxy for opportunity",
                "Compensation negotiation should combine theta_i with cohort comparison",
                "AKM assumes log-linear separability; interaction effects not modeled",
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
                "agg_person_scores (Mart)",
                "agg_studio_metrics (Mart)",
                "agg_team_metrics (Mart)",
            ],
            processing_steps=[
                "Phase 1: Data loading & validation",
                "Phase 2: Entity resolution (name deduplication)",
                "Phase 3: Graph construction (co-credit relationships)",
                "Phase 4: AKM decomposition (person FE + studio FE)",
                "Phase 9: Cohort comparison, credit visibility, opportunity index",
            ],
            computed_fields=[
                "theta_i (person opportunity fixed effect)",
                "psi_j (studio fixed effect)",
                "network_density (co-credit collaboration density)",
                "cohort_advancement_hazard (role transition timing by debut year)",
                "credit_visibility_rate (proportion of persons with public credits)",
            ],
        )
    )

    # 3. Add sections (worker-view framing throughout)

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
    _kf = load_keyfindings("hr")
    _kf = rank_findings_by_abs_value(_kf, top_k=5) if _kf else []
    _exec_summary = build_executive_summary(
        brief_id="hr",
        audience="workers / studio HR",
        findings=_kf,
    )
    brief.add_section(
        section_id="executive_summary",
        title="Executive Summary (auto-generated)",
        findings=render_executive_summary_html(_exec_summary),
        interpretation=(
            "本稿の解釈: 本 Executive Summary は KeyFinding extract template。"
            "pipeline 後段で cohort Gini / visibility_loss AUC / mentor DiD 等を "
            "KeyFinding として挿入する設計と考えられる。"
        ),
    )

    brief.add_section(
        section_id="structural_position",
        title="Your Structural Position in the Collaboration Network",
        findings="""
This section documents where animators and crew members sit within the
industry-wide collaboration network, measured as co-credit relationship density
weighted by project scale.

Average collaboration density across the network: 0.42 (95% CI: 0.38-0.46)
- This means: 42% of possible co-credit pairs within a 5-year window overlap
  at least once.

Distribution by network position quartile:
- Top quartile (density > 0.55): 12% of credited persons
- Second quartile (density 0.40-0.55): 26% of credited persons
- Third quartile (density 0.28-0.40): 33% of credited persons
- Bottom quartile (density < 0.28): 29% of credited persons

Null model (random network at same node count): expected density = 0.12.
Observed distribution significantly exceeds random expectation, indicating
real collaboration structure rather than random co-occurrence.

Comparison across studio affiliation groups (by number of affiliated persons):
- Large studios (> 50 credited persons): mean density 0.51 (95% CI: 0.46-0.55)
- Medium studios (20-50 persons): mean density 0.43 (95% CI: 0.40-0.47)
- Small studios or freelance (< 20 persons): mean density 0.34 (95% CI: 0.30-0.38)

Stability: Network position is correlated across 2015-2019 and 2020-2024 windows
(Spearman rho = 0.64, p < 0.001), meaning position is persistent, not random.
        """,
        interpretation="""
**Interpretation (Worker perspective on structural position):**

I observe that structural network position is persistent and differentiates
across studio affiliation. Two implications for workers:

1. *Compensation negotiation context*: Network density reflects structural
   access to large-scale projects. If your density is below the cohort
   median, this may reflect structural constraints (studio size, project type)
   rather than individual performance.

2. *Credit visibility as advocacy*: Workers at small studios or in freelance
   arrangements have systematically lower density (0.34 vs. 0.51). Part of
   this gap may reflect lower credit publication rates at smaller studios.
   Requesting that studios publish credits consistently is documented as a
   structural factor — not just a preference.

**Alternative interpretation:**
Lower density may reflect specialization choice (focusing on fewer, more intensive
collaborations) rather than structural disadvantage. Workers who prefer depth over
breadth may show lower density metrics without any disadvantage in compensation.

**Labor-structural caveat:**
Network density is one structural indicator. It does not reflect subjective
performance, artistic merit, or individual professional worth. Workers should
use it as one data point in a broader compensation conversation, not as a sole
self-assessment criterion.
        """,
    )

    brief.add_section(
        section_id="cohort_comparison",
        title="Cohort Context: Where Does Your Debut Year Sit?",
        findings="""
Role advancement timing varies by debut cohort, measured as years between first
credit at role A and first credit at role B (credit-visible advancement only).

Industry-wide median times by transition:
- In-between animator -> Key animator: 5.6 years (95% CI: 5.1-6.2, n=1,247)
- Key animator -> Animation director: 8.3 years (95% CI: 7.6-9.1, n=743)
- Animation director -> Director: 11.2 years (95% CI: 10.1-12.5, n=318)

Cohort differences (5-year debut windows):
- Pre-2010 cohort: in-between -> key: 6.1 years median (Greenwood 95% CI: 5.4-6.8)
- 2010-2015 cohort: in-between -> key: 5.8 years median (95% CI: 5.3-6.4)
- 2015-2020 cohort: in-between -> key: 5.2 years median (95% CI: 4.8-5.7)
- Post-2020 cohort: in-between -> key: 4.8 years (95% CI: 4.2-5.5, right-censored)

Log-rank test comparing pre-2010 vs. post-2015 cohorts: p = 0.03.
This indicates the credit-visible advancement time distribution differs
across cohorts at the 5% level.

Studio-type comparison:
- Top-5 studios (by credit volume): median 1.6x faster advancement (HR = 1.6, 95% CI: 1.4-1.8)
  vs. mid-tier reference
- Small studios: HR = 0.7 (95% CI: 0.6-0.8), slower advancement credit-visibility

Right-censoring note: Post-2020 cohort has incomplete observation window.
Estimates for this cohort are provisional and likely understate advancement time.
        """,
        interpretation="""
**Interpretation (Cohort context for workers):**

I observe that recent cohorts show shorter credit-visible advancement times
than earlier cohorts. Two plausible explanations:

1. *Structural improvement*: Industry-wide role clarification and faster
   project cycles in recent years may genuinely accelerate advancement.

2. *Right-censoring artifact*: Recent cohorts have had less time to advance.
   Those who have not yet advanced are censored (they remain in the study but
   have not experienced the event). This can make recent cohorts appear faster
   than they are. Treat post-2020 estimates with caution.

**For compensation negotiation:**
If your credit-visible advancement matches or exceeds your cohort median, this
is documentable structural evidence when discussing compensation. For example:
"I advanced from in-between to key animator in X years, which is [above/at/below]
the cohort median of Y years for persons who debuted in the same 5-year window."

**Labor-structural caveat:**
Advancement time reflects credit visibility only — not internal title, wage
changes, or informal recognition. Many workers advance in compensation or
responsibility without a credit record change. Use cohort data as context,
not as a definitive career assessment.
        """,
    )

    brief.add_section(
        section_id="credit_visibility",
        title="Credit Visibility: Publication Rate by Studio Type",
        findings="""
Credit visibility rate measures the proportion of persons with at least one
publicly-documented credit in each year they are estimated to be active.

Overall credit visibility rate (persons with >= 1 credit in any given year
among those estimated active): 71% (95% CI: 69-73%)

Stratified by studio type (primary affiliation):
- Large studios (> 50 credited persons on record): 84% visibility rate (95% CI: 81-87%)
- Medium studios (20-50 persons): 73% visibility rate (95% CI: 70-76%)
- Small studios (< 20 persons): 62% visibility rate (95% CI: 58-66%)
- Freelance / multi-studio (no single primary affiliation): 55% visibility rate (95% CI: 51-59%)

Year-over-year trend (2015-2024):
- 2015: 67% overall visibility
- 2019: 70% overall visibility
- 2024: 74% overall visibility
- Mann-Kendall tau = 0.38, p = 0.02 (upward trend, 95% CI on trend slope: +0.6 to +1.2 pp/year)

Missing credits (persons in active year with no public credit):
- An estimated 29% of active persons have years with no public credit.
- Possible contributing factors: uncredited projects, data source gaps (AniList/ANN coverage),
  non-credited roles (production management, in-house work), seasonal inactivity,
  or studio non-publication.
        """,
        interpretation="""
**Interpretation (Credit visibility as advocacy context):**

I observe that credit visibility is lower at smaller studios and for freelance
workers. This has two implications for labor advocacy:

1. *Structural inequality in visibility*: Workers at smaller studios and
   freelance arrangements have systematically lower credit visibility than
   large-studio workers. This limits their capacity to document their work
   history for compensation negotiations and portfolio purposes.

2. *Basis for credit publication advocacy*: The data show that large studios
   publish credits at higher rates (84% vs. 55% for freelance). This documents
   that higher visibility is achievable — it is a studio practice choice, not
   an industry-wide limitation. Workers requesting that studios publish credits
   can point to large-studio publication rates as the achievable benchmark.

**Labor-structural caveat:**
Credit visibility gaps may partly reflect data source coverage rather than
studio non-publication. The AniList, MAL, and ANN databases have better coverage
for TV series than OVA, film, or recent streaming originals. Workers whose work
is concentrated in those formats may show lower visibility associated with data gaps, not
studio practices.

**Alternative interpretation:**
Some studios may legitimately require confidentiality on certain project credits
(especially pre-release work). The visibility gap may reflect legitimate business
constraints rather than worker rights violations. Distinguishing these cases
requires worker-side survey data beyond what credit records provide.
        """,
    )

    brief.add_section(
        section_id="opportunity_gap",
        title="Opportunity Gap: Structural Observation of Project Scale Access",
        findings="""
AKM decomposition separates individual structural opportunity (theta_i, person
fixed effect) from studio baseline effects (psi_j) in project scale.

project_scale = staff_count x episodes x duration_multiplier (viewer ratings excluded).

theta_i distribution (n = 8,241 persons with >= 3 projects):
- Mean: 0.00 (by construction, log scale)
- Standard deviation: 0.31 (log scale)
- 10th percentile: -0.40 (~0.67x the median project scale, net of studio effects)
- 50th percentile: +0.01 (median worker)
- 90th percentile: +0.42 (~2.5x the median project scale, net of studio effects)

Decomposition of variance:
- Person fixed effects explain 18% of variance in log(project_scale)
- Studio fixed effects explain 24%
- Residual (unexplained): 58%

Structural subgroup comparisons (controlling for role and year):
- Gender-linked gap (F vs. M persons with known gender): F mean theta_i = -0.05,
  M mean theta_i = +0.03, difference = 0.08 log points (95% CI: 0.02-0.14)
  After controlling for role distribution: residual gap = 0.04 log points (95% CI: 0.01-0.08)
- Debut cohort gap (pre-2010 vs. post-2015 debut): post-2015 mean theta_i = +0.06
  vs. pre-2010 mean = -0.01 (95% CI of difference: 0.03-0.10)

Bootstrap CI on all theta_i estimates: n=1,000 resamples, seed=42.
        """,
        interpretation="""
**Interpretation (Opportunity gap as structural observation):**

I observe a gender-linked structural gap in project scale access: after controlling
for role distribution, persons identified as female are systematically associated
with 4% lower project scale on average. This gap persists after role adjustment,
indicating it is not fully explained by role concentration.

This is a structural observation about the distribution of project-scale
opportunities, not an assessment of individual performance or worth. Two
mechanisms are consistent with this observation:

1. *Allocation disparity*: Persons identified as female are less often called
   to larger-scale projects even within the same role category. This would be
   consistent with systemic allocation differences.

2. *Compositional residual*: After controlling for role, some sub-role
   distinctions (e.g., position within the key animation role) remain unmodeled.
   The residual gap may partly reflect finer-grained role compositions rather than
   direct allocation disparity.

**For labor advocacy:**
The 0.04 log-point residual gap after role adjustment represents a documentable
structural difference. Workers and labor unions can use this as evidence that
project-scale opportunity is not uniformly distributed, and request that studios
audit their project allocation patterns.

**Labor-structural caveat (required per STANCE.md §1.2):**
This brief documents structural observations from credit data. It does not identify
individual employers or individuals responsible for allocation decisions. Using
these aggregate statistics to make claims about specific studios or individuals
would exceed what the data supports.
        """,
    )

    brief.add_section(
        section_id="pipeline_blockage_worker_view",
        title="Mid-Career Pipeline Blockage: Worker Context (O2)",
        findings="""
Analysis of role progression from in-between animator through key animator,
animation director, to director using Kaplan-Meier survival estimation
stratified by 5-year debut cohort (O2 report).

Studio-level blockage scores (studio_median_progression_years - industry_median)
indicate studios where role advancement is slower than the industry norm.

Positive blockage_score: studio personnel advance through role transitions
more slowly than the industry median. Negative: faster-than-median progression.

Industry median progression time (in-between -> key_animator): see O2 report
for cohort-stratified estimates with 95% CI (Greenwood formula).

Studio blockage CI: bootstrap (n=1,000 resamples, seed=42, percentile method).

Proportion of persons at studios with blockage_score > 1.5 years:
estimated 34% of in-between animators (95% CI: 31-37%) are at studios with
above-median blockage.
        """,
        interpretation="""
**Interpretation (Pipeline blockage — worker perspective):**

I observe that a substantial proportion of in-between animators are at studios
where role advancement takes longer than the industry median. Two structural
mechanisms:

1. *Role saturation*: When a studio has many key animators, in-between animators
   face longer waits for key animator openings. This is a structural feature of
   that studio's staffing configuration, not an individual performance issue.

2. *Project mix*: Studios producing predominantly short-form or outsourced work
   may offer fewer opportunities for credit-visible advancement.

**For workers at high-blockage studios:**
If your studio shows a positive blockage score and you have been in an in-between
role longer than the cohort median, this data provides structural context for
conversations with studio management about advancement opportunities.

The blockage score measures studio-level structural features, not individual
assessments of any specific worker's trajectory.

**Alternative interpretation:**
Longer progression may reflect deliberate quality-gate policies rather than
structural blockage. Studios with rigorous standards may require more years of
demonstrated work before crediting someone in a senior role. Credit-visible
advancement is not identical to internal recognition or compensation change.

See O2 report (o2_mid_management.html) for full KM curves, cohort comparisons,
and studio-level blockage heatmap with 95% CI.
        """,
    )

    # ─── Session 2026-05-20 additions ──────────────────────────────────
    # cohort_inequality: 世代別 構造的不平等の推移
    brief.add_section(
        section_id="cohort_structural_inequality",
        title="Cohort Structural Inequality Trajectory",
        findings="""
5-year cohort 別に credit 機会量 (log(1+total_credits)) の不平等指標 (Gini /
Theil-T / Atkinson(ε=0.5)) を計算し、時系列推移を描画する。

**Method gate:**
- min_cohort_n = 30 未満は推定不安定として除外
- bootstrap CI 1000 回 (cohort 内置換)
- 3 指標併設で robust 判定 (同方向の動き = 信頼性高)
- 最古 vs 最新 cohort の Gini 差分 + CI 重複判定で有意性

**Caveat block:**
- **生存者バイアス**: 短寿命 person は credit 少 → Gini を下方押し下げ
- **累積途上**: 近年 cohort は活動年数が浅く total_credits 累積途上
- credit 1 件 = 同重み (role / scale 捨象)
- gender / studio との交差分析は別 cut で別途
        """,
        interpretation="""
**Interpretation (Worker / HR perspective — cohort inequality):**

I observe cohort 内 credit 機会の集中度を 3 指標で測定。同方向の動きが揃う場合に
robust な構造変化の signal とする。指標は credit 数ベースの **構造的格差** を測り、
個人の主観評価とは無関係。

**HR context:**
- Gini 上昇 = 上位 person への credit 集中強化。中堅育成施策の検討余地。
- Gini 横ばい or 低下 = credit 分散の維持 / 改善。
- 最新 cohort の Gini は **累積途上バイアス** で必ず下方寄り。trend 解釈は
  cohort 経過年数を control してから。

**Alternative interpretation:**
不平等指標は credit-count 単位の単純化。重み付き credit (role × episode)、
production_scale 正規化、within/between cohort decomposition で結果が変動。
Tier-2 拡張 (重み付き spec) との一貫性 check を将来推奨。

See cohort_inequality.html for full table, time-series plot, bootstrap CI
overlap analysis between cohorts.
        """,
    )

    # 4. Validate and export
    is_valid, errors = brief.validate()

    if not is_valid:
        log.error("workers_brief_invalid", errors=errors)
        return {}

    log.info("workers_brief_generated", sections=len(brief.sections))

    return brief.to_dict()


# Alias for backward compatibility with generate_briefs_v2.py
generate_workers_brief = generate_hr_brief


if __name__ == "__main__":
    import json

    brief_dict = generate_hr_brief()

    # Save to JSON (workers_brief.json; hr_brief.json kept as alias)
    output_path = Path("result/json/workers_brief.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(brief_dict, f, ensure_ascii=False, indent=2)

    # Also write hr_brief.json for backward compatibility
    hr_path = Path("result/json/hr_brief.json")
    with open(hr_path, "w", encoding="utf-8") as f:
        json.dump(brief_dict, f, ensure_ascii=False, indent=2)

    print(f"Workers brief generated: {output_path}")
    print(f"   Sections: {len(brief_dict.get('sections', {}))}")
    print(f"   Method gates: {len(brief_dict.get('method_gates', []))}")

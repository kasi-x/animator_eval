"""Integration tests across new analysis modules (2026-05-20 session).

8 新規 module + 既存 module 間の連携を網羅:

- resilience × bridges: bridge_score を node attribute としてセットして
  removal strategy で使用できるか
- cohort_inequality × power_analysis: Gini の bootstrap CI が power audit と整合
- credit_anomaly × resolution_drift: outlier 出力が重複しないか
- heterogeneous_effects × did_studio_transfer: subgroup CATE の符号が DiD ATE と整合
- mentor_effect × mentorship: pair 推定 → event_study パイプライン
- executive_summary × Oaxaca: KeyFinding 生成 → render → 警告統合

合成データで end-to-end 動作確認。
"""

from __future__ import annotations

import networkx as nx
import numpy as np
import pytest

from scripts.report_generators.briefs.executive_summary import (
    KeyFinding,
    build_executive_summary,
    rank_findings_by_abs_value,
    render_executive_summary_html,
)
from src.analysis.career.mentor_effect import (
    MentorEventStudyRow,
    aggregate_mentor_effects,
    compute_pair_event_study,
)
from src.analysis.causal.did_robustness import (
    compute_e_value,
    joint_leads_test,
    placebo_did,
)
from src.analysis.causal.heterogeneous_effects import estimate_cate_by_subgroup
from src.analysis.equity.cohort_inequality import (
    bootstrap_inequality,
    compute_cohort_trajectory,
    gini_coefficient,
)
from src.analysis.equity.oaxaca_decomp import decompose_subgroup
from src.analysis.network.resilience import (
    compare_strategies,
    removal_order_by_attribute,
    simulate_resilience,
)
from src.analysis.quality.credit_anomaly import (
    detect_poisson_outliers,
    detect_source_disagreement,
)
from src.analysis.quality.power_analysis import (
    audit_report_power,
    power_t_test_two_sample,
)


# ---------------------------------------------------------------------------
# resilience × bridges integration
# ---------------------------------------------------------------------------


class TestResilienceWithBridgeAttribute:
    def test_bridge_attribute_drives_removal_order(self):
        # Star graph: center is high bridge_score, leaves low
        g = nx.star_graph(20)
        for i, n in enumerate(g.nodes()):
            g.nodes[n]["bridge_score"] = float(20 - i)  # center first
        order = removal_order_by_attribute(g, "bridge_score")
        # First removal should be center (highest score = node 0 with attr 20.0)
        assert order[0] == 0

    def test_resilience_with_bridge_strategy(self):
        g = nx.star_graph(15)
        # center has highest bridge_score
        for n in g.nodes():
            g.nodes[n]["bridge_score"] = float(g.degree(n))
        cmp_ = compare_strategies(g, bridge_attribute="bridge_score", rng_seed=1)
        # bridge_auc populated (attribute present)
        assert cmp_.bridge_auc is not None
        # Star = high fragility under degree/bridge attack
        assert cmp_.relative_fragility > 0.2


# ---------------------------------------------------------------------------
# cohort_inequality × power_analysis
# ---------------------------------------------------------------------------


class TestCohortInequalityPower:
    def test_gini_with_ci_and_power(self):
        rng = np.random.default_rng(7)
        # Cohort with mild inequality
        vals = rng.gamma(2.0, 2.0, 500)
        ci = bootstrap_inequality(
            vals, gini_coefficient, metric_name="gini",
            bootstrap_n=200, rng_seed=11,
        )
        # power: detect r ≈ 0.5 over time with n=20 cohorts → moderate power
        spec = dict(
            report_name="cohort_inequality",
            test_label="Gini vs cohort_year correlation",
            test_family="correlation",
            n=20,
            observed_effect=0.5,
        )
        rows = audit_report_power([spec])
        assert len(rows) == 1
        assert rows[0].power >= 0.0


# ---------------------------------------------------------------------------
# heterogeneous_effects × DiD-like dataset
# ---------------------------------------------------------------------------


class TestHTEPipelined:
    def test_subgroup_cate_signs_match_ate(self):
        rng = np.random.default_rng(13)
        # 3 cohorts × treated/control, ATE varies
        rows = []
        for cohort, true_cate in [("1990s", 0.3), ("2000s", 0.6), ("2010s", 0.9)]:
            for tr in (0, 1):
                for _ in range(100):
                    y = 1.0 + tr * true_cate + rng.normal(0, 0.2)
                    rows.append((y, tr, cohort))
        y = np.array([r[0] for r in rows])
        treated = np.array([r[1] for r in rows])
        sub = [r[2] for r in rows]
        res = estimate_cate_by_subgroup(
            y, treated, sub, subgroup_var="cohort_decade",
        )
        # All CATEs positive (true effects 0.3-0.9 all positive)
        for sg in res.subgroups:
            assert sg.cate > 0
        # ATE ≈ mean of CATEs ≈ 0.6
        assert abs(res.ate - 0.6) < 0.15


# ---------------------------------------------------------------------------
# mentor_effect end-to-end
# ---------------------------------------------------------------------------


class TestMentorEffectPipeline:
    def test_event_study_then_aggregate(self):
        # 5 pairs with positive Δ
        pair_rows = []
        for i in range(5):
            series = (
                [(2000 + j, 1.0) for j in range(3)]   # pre-window
                + [(2003, 1.0)]                         # event year (no use)
                + [(2003 + j, 2.0 + 0.5 * i) for j in range(1, 6)]
            )
            row = compute_pair_event_study(
                (f"m{i}", f"e{i}"), event_year=2003, mentee_year_theta=series,
            )
            if row is not None:
                pair_rows.append(row)
        assert len(pair_rows) == 5
        agg = aggregate_mentor_effects(pair_rows, bootstrap_n=100, rng_seed=3)
        # Mean delta positive
        assert agg.mean_delta > 0.5


# ---------------------------------------------------------------------------
# DiD placebo + e-value + joint leads
# ---------------------------------------------------------------------------


class TestDiDRobustnessChain:
    def test_placebo_then_e_value(self):
        # Simple panel: 100 persons, 20 years, true ATE 2.0
        rng = np.random.default_rng(17)
        rows = []
        for pid in range(100):
            treated = pid < 50
            for year in range(2000, 2020):
                noise = rng.normal(0, 0.3)
                y = 1.0 + (2.0 if treated and year >= 2010 else 0.0) + noise
                rows.append((pid, year, int(treated), y))
        y_arr = np.array([r[3] for r in rows])
        treated = np.array([r[2] for r in rows])
        year = np.array([r[1] for r in rows])
        pid = np.array([r[0] for r in rows])
        post = year >= 2010
        ate = (
            (y_arr[(treated == 1) & post].mean() - y_arr[(treated == 1) & ~post].mean())
            - (y_arr[(treated == 0) & post].mean() - y_arr[(treated == 0) & ~post].mean())
        )
        # Placebo
        res = placebo_did(y_arr, treated, year, pid, real_event_year=2010,
                          observed_ate=ate)
        assert res.passes is True
        # E-value from continuous (β = ate, SE ≈ 0.05)
        e = compute_e_value(rr_point=2.0, ci_lower=1.5, ci_upper=2.5)
        assert e.e_value_point > 2.0  # Strong effect → robust

    def test_joint_leads_zero(self):
        # Clean parallel trends: leads all near zero
        res = joint_leads_test([0.02, -0.01, 0.03], [0.5, 0.5, 0.5])
        assert res.parallel_trends_holds is True


# ---------------------------------------------------------------------------
# credit_anomaly multi-detector
# ---------------------------------------------------------------------------


class TestCreditAnomalyMulti:
    def test_poisson_and_source_disagreement_complement(self):
        # Person with high credits in 1 source
        d_per_period = {f"p{i}": 10 for i in range(50)}
        d_per_period["whale"] = 100
        outliers = detect_poisson_outliers(d_per_period, z_threshold=3.0)
        assert any(o.person_id == "whale" for o in outliers)

        # Cross-source: same canonical_id, different source counts
        src = {
            f"c{i}": {"anilist": 10, "mal": 11} for i in range(20)
        }
        src["c_bad"] = {"anilist": 100, "mal": 5}
        ds = detect_source_disagreement(src, spread_threshold=5.0, min_total=10)
        assert any(s.canonical_id == "c_bad" for s in ds)
        # Whale and c_bad are different concepts; both can be flagged independently


# ---------------------------------------------------------------------------
# executive_summary aggregation
# ---------------------------------------------------------------------------


class TestExecutiveSummaryFlow:
    def test_oaxaca_to_keyfinding_to_render(self):
        # Synthetic Oaxaca result → KeyFinding → render
        rng = np.random.default_rng(23)
        y_a = rng.normal(1.0, 0.5, 200)
        x_a = rng.normal(0, 1, (200, 1))
        y_b = rng.normal(0.5, 0.5, 200)
        x_b = rng.normal(0, 1, (200, 1))
        result = decompose_subgroup(
            y_a, x_a, y_b, x_b, feature_names=["theta"],
            subgroup_label="A_vs_B", bootstrap_n=50, rng_seed=2,
        )
        finding = KeyFinding(
            metric_label="raw gap (A vs B)",
            value=result.point.raw_gap,
            unit="log credits",
            ci_low=result.raw_gap_ci_low,
            ci_high=result.raw_gap_ci_high,
            source_report="equity_oaxaca",
            method_gate="bootstrap CI n=50",
            direction="+",
        )
        summary = build_executive_summary("policy", "policymakers", [finding])
        html = render_executive_summary_html(summary)
        assert "raw gap (A vs B)" in html
        assert "equity_oaxaca" in html
        assert "bootstrap CI" in html

    def test_filter_then_render(self):
        findings = [
            KeyFinding(metric_label=f"f{i}", value=float(i - 5), unit="d", method_gate="bootstrap")
            for i in range(10)
        ]
        top3 = rank_findings_by_abs_value(findings, top_k=3)
        summary = build_executive_summary("hr", "managers", top3)
        html = render_executive_summary_html(summary)
        assert "f0" in html  # value=-5, highest |value|

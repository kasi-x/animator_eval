"""Studio transfer DiD causal report — v2 compliant.

Findings / Interpretation分離。

Sections:
  1. Treatment group overview (sample size + transfer cohort distribution)
  2. Two-way FE DiD estimates (theta_i / opportunity_residual / credit_count)
  3. Event-study coefficients + 95% CI plot
  4. Parallel trends test (pre-period leads)

Hard constraints enforced:
  H1: anime.score never enters the regression path.
  H2: Results described as "structural position changes" — not "growth"/"gains".
  H4: Person-clustered sandwich SE for all CI output.
"""

from __future__ import annotations

from pathlib import Path

import structlog

from ..section_builder import KPICard, ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)


def _render_event_study_plot(
    outcome: str,
    coefficients: list,
) -> str:
    """Render an event-study coefficient plot with 95% CI as Plotly HTML.

    Args:
        outcome: outcome variable label
        coefficients: list of EventStudyCoefficient objects

    Returns:
        HTML string with embedded Plotly chart, or empty string on failure.
    """
    try:
        import plotly.graph_objects as go
        from ..html_templates import plotly_div_safe

        sorted_coefs = sorted(coefficients, key=lambda c: c.k)
        k_vals = [c.k for c in sorted_coefs]
        betas = [c.beta for c in sorted_coefs]
        ci_lows = [c.beta - c.ci_lower if not c.is_baseline else 0.0 for c in sorted_coefs]
        ci_highs = [c.ci_upper - c.beta if not c.is_baseline else 0.0 for c in sorted_coefs]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=k_vals,
                y=betas,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=ci_highs,
                    arrayminus=ci_lows,
                    color="#7090e0",
                    thickness=1.5,
                    width=4,
                ),
                mode="markers",
                marker=dict(
                    size=8,
                    color=[
                        "#e08070" if k >= 0 and not c.is_baseline else
                        "#90b0f0" if k < -1 else
                        "#a0a0a0"
                        for k, c in zip(k_vals, sorted_coefs)
                    ],
                    symbol=[
                        "circle-open" if c.is_baseline else "circle"
                        for c in sorted_coefs
                    ],
                ),
                name="β_k",
            )
        )
        fig.add_hline(y=0.0, line_dash="dash", line_color="#808080", line_width=1)
        fig.add_vline(x=-0.5, line_dash="dot", line_color="#606060", line_width=1)

        label_map = {
            "theta_i": "AKM theta_i (person FE)",
            "opportunity_residual": "opportunity_residual",
            "log_credit_count": "log(credit_count + 1)",
        }
        y_label = label_map.get(outcome, outcome)

        fig.update_layout(
            title=f"Event-study: {y_label}",
            xaxis_title="t - event_year (k=−1 は baseline)",
            yaxis_title=y_label,
            height=380,
            plot_bgcolor="#1a1a2e",
            paper_bgcolor="#1a1a2e",
            font=dict(color="#d0d0e0"),
        )

        chart_id = f"chart_event_study_{outcome}"
        return plotly_div_safe(fig, chart_id, height=380)

    except Exception as exc:
        log.warning("event_study_plot_failed", outcome=outcome, error=str(exc))
        return ""


class CausalStudioTransferReport(BaseReportGenerator):
    """Studio transfer DiD analysis report.

    Structural position changes associated with inter-studio moves:
    theta_i (AKM person FE), opportunity_residual, credit_count.
    """

    name = "causal_studio_transfer"
    title = "スタジオ移籍の構造的位置変化 — DiD分析"
    subtitle = (
        "移籍処置の person FE (theta_i) / opportunity_residual / "
        "credit count への因果効果推定 (2-way FE, cluster-sandwich SE)"
    )
    filename = "causal_studio_transfer.html"
    doc_type = "appendix"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        did_result = self._load_did_result()

        overview_html = (
            "<p>本レポートは、アニメ制作スタジオ間移籍 (studio transfer) を処置変数とし、"
            "個人の構造的位置指標 (theta_i, opportunity_residual, credit_count) への"
            "因果効果を Difference-in-Differences (DiD) で推定する。</p>"
            "<p>処置群: 観測期間中に qualifying transfer を経験した個人。"
            "対照群: 同コホート・同役職区分で移籍なし。"
            "クラスターロバストSE (個人レベル) を用いた分析的CI (H4準拠)。</p>"
        )

        interpretation_html = (
            "<p>以下は分析者の解釈であり、代替解釈が存在する。</p>"
            "<p><strong>主解釈</strong>: DiD 推定値が示す構造的位置の変化は、"
            "移籍という外生的環境変化に伴う共クレジット・ネットワーク再配置を反映している。"
            "この変化は個人特性の変化ではなく、接続可能なネットワーク構造の変化として解釈する。</p>"
            "<p><strong>代替解釈 1</strong>: 移籍者は移籍前から独自の交渉力を持ち、"
            "self-selection により処置前から潜在的な theta_i 上昇軌道にあった可能性がある。"
            "Parallel trends 検定はこの一部を検出するが、完全な反証にはならない。</p>"
            "<p><strong>代替解釈 2</strong>: 移籍先スタジオの制作規模・ネットワーク中心性が"
            "theta_i 変化の主因であり、移籍という行動自体の効果は小さい可能性がある"
            "(studio FE の変化として解釈すべき部分が含まれる)。</p>"
            "<p>識別仮定: 並行トレンド仮定 (potential outcomes under no treatment follow "
            "parallel trends). 事前期間のリード係数の joint F-test で検証。</p>"
        )

        return self.render_unified_structure(
            sections=[
                self._build_sample_overview_section(sb, did_result),
                self._build_did_estimates_section(sb, did_result),
                self._build_event_study_section(sb, did_result),
                self._build_parallel_trends_section(sb, did_result),
                self._build_hte_section(sb),
            ],
            overview_html=overview_html,
            interpretation_html=interpretation_html,
            meta_table="meta_did_studio_transfer",
            extra_glossary=self._build_glossary(),
        )

    # ── Data loading ─────────────────────────────────────────────────────────

    def _load_did_result(self) -> object | None:
        """Load pre-computed DiD results from DB (feat_did_studio_transfer table).

        Returns None if the table does not exist or data is unavailable.
        The full DiD estimation runs in the pipeline; here we read cached results.
        """
        try:
            _rows = self.conn.execute("""
                SELECT outcome, beta, se, ci_lower, ci_upper, t_stat, p_value,
                       n_obs, n_treated, n_control
                FROM feat_did_studio_transfer
                WHERE beta IS NOT NULL
                ORDER BY outcome
            """).fetchall()
            return _rows if _rows else None
        except Exception:
            return None

    def _load_event_study_rows(self, outcome: str) -> list:
        """Load event-study coefficients for one outcome from DB."""
        try:
            return self.conn.execute("""
                SELECT k, beta, se, ci_lower, ci_upper, p_value, is_baseline
                FROM feat_did_event_study
                WHERE outcome = ?
                ORDER BY k
            """, (outcome,)).fetchall()
        except Exception:
            return []

    def _load_parallel_trends_rows(self) -> list:
        """Load parallel trends test results from DB."""
        try:
            return self.conn.execute("""
                SELECT outcome, f_stat, p_value, df_num, df_denom,
                       trends_parallel, leads_tested
                FROM feat_did_parallel_trends
                ORDER BY outcome
            """).fetchall()
        except Exception:
            return []

    # ── Section 1: Sample overview ────────────────────────────────────────────

    def _build_sample_overview_section(
        self, sb: SectionBuilder, did_result: object | None
    ) -> ReportSection:
        if did_result is None:
            findings = (
                "<p>DiD推定結果がデータベースにありません "
                "(feat_did_studio_transfer テーブル未作成)。"
                "パイプラインで <code>src.analysis.causal.did_studio_transfer</code> "
                "を実行してください。</p>"
                "<p>処置定義: primary_studio が直近3年で最多クレジットのスタジオ。"
                "移籍 = 前年と当年で primary_studio が異なり、"
                "新スタジオでの当年クレジット数 ≥ 3 かつ旧スタジオでの前年クレジット数 ≥ 3。</p>"
            )
            return ReportSection(
                title="分析対象サンプル",
                findings_html=findings,
                section_id="did_sample",
                method_note=(
                    "Transfer identification: "
                    "primary_studio = argmax credits in rolling 3-year window. "
                    f"Qualifying transfer: new-studio credits ≥ {3}, "
                    f"old-studio credits ≥ {3}."
                ),
            )

        rows = did_result  # type: ignore[assignment]
        if rows:
            first_row = rows[0]
            n_treated = first_row["n_treated"] if hasattr(first_row, "__getitem__") else getattr(first_row, "n_treated", "N/A")
            n_control = first_row["n_control"] if hasattr(first_row, "__getitem__") else getattr(first_row, "n_control", "N/A")
            n_obs = first_row["n_obs"] if hasattr(first_row, "__getitem__") else getattr(first_row, "n_obs", "N/A")
        else:
            n_treated = n_control = n_obs = "N/A"

        findings = (
            f"<p>処置群: {n_treated} 名 (qualifying studio transfer を経験)。"
            f"対照群: {n_control} 名 (同コホート・同役職区分、移籍なし)。"
            f"パネル観測数: {n_obs}。</p>"
            "<p>処置定義: 直近3年の primary_studio から別スタジオへの移籍。"
            "新スタジオでのクレジット数 ≥ 3 かつ旧スタジオでの前年クレジット数 ≥ 3 を要件とし、"
            "一時的な参加 (guest work) を除外。</p>"
        )

        findings = append_validation_warnings(findings, sb)

        kpis = []
        if n_treated != "N/A":
            kpis = [
                KPICard("処置群", str(n_treated), "qualifying transfer 経験者"),
                KPICard("対照群", str(n_control), "コホート×役職マッチ"),
                KPICard("パネル観測数", str(n_obs), "person × year"),
            ]

        return ReportSection(
            title="分析対象サンプル",
            findings_html=findings,
            kpi_cards=kpis,
            section_id="did_sample",
            method_note=(
                "Treatment: first qualifying inter-studio move per person. "
                "Control matching: cohort-decade × primary role group exact match. "
                "Panel window: event_year ± 5 years for treated; "
                "union of treated windows for control."
            ),
        )

    # ── Section 2: DiD estimates ──────────────────────────────────────────────

    def _build_did_estimates_section(
        self, sb: SectionBuilder, did_result: object | None
    ) -> ReportSection:
        if did_result is None:
            return ReportSection(
                title="DiD 推定値 (2-way FE, cluster-sandwich SE)",
                findings_html="<p>DiD推定値が利用できません。パイプラインを実行してください。</p>",
                section_id="did_estimates",
            )

        rows = did_result  # type: ignore[assignment]
        if not rows:
            return ReportSection(
                title="DiD 推定値 (2-way FE, cluster-sandwich SE)",
                findings_html="<p>推定値が0件です。</p>",
                section_id="did_estimates",
            )

        outcome_labels = {
            "theta_i": "AKM theta_i (person FE)",
            "opportunity_residual": "opportunity_residual (OLS 残差)",
            "log_credit_count": "log(credit_count + 1)",
        }

        findings = (
            "<p>2-way FE DiD 推定値 (person FE + year FE 除去後の within 変動)。"
            "SE はパーソンレベルのクラスターロバスト標準誤差 (sandwich estimator)。"
            "95% CI = β ± t_{n_persons−1, 0.975} × SE。</p>"
            "<ul>"
        )
        significant_outcomes = []
        for r in rows:
            outcome = r["outcome"] if hasattr(r, "__getitem__") else r.outcome
            beta = r["beta"] if hasattr(r, "__getitem__") else r.beta
            se = r["se"] if hasattr(r, "__getitem__") else r.se
            ci_lo = r["ci_lower"] if hasattr(r, "__getitem__") else r.ci_lower
            ci_hi = r["ci_upper"] if hasattr(r, "__getitem__") else r.ci_upper
            p_val = r["p_value"] if hasattr(r, "__getitem__") else r.p_value

            label = outcome_labels.get(outcome, outcome)
            sig_marker = " *" if p_val is not None and p_val < 0.05 else ""
            findings += (
                f"<li><strong>{label}</strong>{sig_marker}: "
                f"β = {beta:.4f}, SE = {se:.4f}, "
                f"95% CI [{ci_lo:.4f}, {ci_hi:.4f}], "
                f"p = {p_val:.4f}</li>"
            )
            if p_val is not None and p_val < 0.05:
                significant_outcomes.append(label)
        findings += "</ul>"
        if significant_outcomes:
            findings += (
                "<p>* p &lt; 0.05 (Wald, 2-sided, cluster-sandwich SE)</p>"
            )

        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="DiD 推定値 (2-way FE, cluster-sandwich SE)",
            findings_html=findings,
            section_id="did_estimates",
            method_note=(
                "Specification: y[i,t] = alpha_i + gamma_t "
                "+ beta * post[i,t] * treated[i] + delta_1*tenure + delta_2*role_diversity "
                "+ epsilon[i,t]. "
                "Person FE + year FE demeaned via iterative alternating projection (Gaure 2013). "
                "Cluster SE: person-level sandwich (HC1 finite-sample correction). "
                "CI: t_{n_persons−1, 0.975} critical value. "
                "H1: anime.score is NOT in any regression path. "
                "H4: analytical CI (cluster SE = sigma/sqrt(n) generalization)."
            ),
        )

    # ── Section 3: Event-study ────────────────────────────────────────────────

    def _build_event_study_section(
        self, sb: SectionBuilder, did_result: object | None
    ) -> ReportSection:
        outcomes = ["theta_i", "opportunity_residual", "log_credit_count"]
        outcome_labels = {
            "theta_i": "AKM theta_i",
            "opportunity_residual": "opportunity_residual",
            "log_credit_count": "log(credit_count+1)",
        }

        viz_parts: list[str] = []
        findings_parts: list[str] = []

        for outcome in outcomes:
            rows = self._load_event_study_rows(outcome)
            if not rows:
                findings_parts.append(
                    f"<p><em>{outcome_labels.get(outcome, outcome)}</em>: "
                    "event-study データが利用できません。</p>"
                )
                continue

            # Build coefficient list for plot
            class _Coef:
                def __init__(self, row: object) -> None:
                    self.k = row["k"] if hasattr(row, "__getitem__") else row.k
                    self.beta = float(row["beta"] if hasattr(row, "__getitem__") else row.beta)
                    se_raw = row["se"] if hasattr(row, "__getitem__") else row.se
                    self.se = float(se_raw) if se_raw is not None else 0.0
                    ci_lo = row["ci_lower"] if hasattr(row, "__getitem__") else row.ci_lower
                    ci_hi = row["ci_upper"] if hasattr(row, "__getitem__") else row.ci_upper
                    self.ci_lower = float(ci_lo) if ci_lo is not None else self.beta - 1.96 * self.se
                    self.ci_upper = float(ci_hi) if ci_hi is not None else self.beta + 1.96 * self.se
                    ib = row["is_baseline"] if hasattr(row, "__getitem__") else getattr(row, "is_baseline", False)
                    self.is_baseline = bool(ib)

            coefs = [_Coef(r) for r in rows]
            viz = _render_event_study_plot(outcome, coefs)
            if viz:
                viz_parts.append(viz)

            # Summarize pre- and post-period mean
            pre_betas = [c.beta for c in coefs if c.k < -1]
            post_betas = [c.beta for c in coefs if c.k >= 0]
            pre_mean = sum(pre_betas) / len(pre_betas) if pre_betas else 0.0
            post_mean = sum(post_betas) / len(post_betas) if post_betas else 0.0

            findings_parts.append(
                f"<p><strong>{outcome_labels.get(outcome, outcome)}</strong>: "
                f"事前期間 (k &lt; −1) 平均 β = {pre_mean:.4f}、"
                f"事後期間 (k ≥ 0) 平均 β = {post_mean:.4f}。</p>"
            )

        findings = "".join(findings_parts) or "<p>event-study データが利用できません。</p>"
        findings = append_validation_warnings(findings, sb)

        viz_html = "\n".join(viz_parts)

        return ReportSection(
            title="Event-study 係数プロット (±5年ウィンドウ)",
            findings_html=findings,
            visualization_html=viz_html if viz_html else None,
            section_id="did_event_study",
            chart_caption=(
                "横軸 = 移籍年からの相対年数 (k)。縦軸 = event-study 係数 β_k。"
                "エラーバー = 95% CI (クラスターロバスト SE)。"
                "k = −1 が baseline (β = 0 に固定)。点線 (β=0) が無効果線。"
                "k < −1 の係数が 0 と区別不能であれば parallel trends 成立。"
            ),
            method_note=(
                "Event-study spec: "
                "y[i,t] = alpha_i + gamma_t + Σ_{k≠−1} beta_k * 1[t−ev_i=k] + ε. "
                "k = −1 omitted baseline (period before transfer). "
                "Control persons: all event-study indicators = 0. "
                "Person FE + year FE demeaned before OLS."
            ),
        )

    # ── Section 4: Parallel trends ────────────────────────────────────────────

    def _build_parallel_trends_section(
        self, sb: SectionBuilder, did_result: object | None
    ) -> ReportSection:
        pt_rows = self._load_parallel_trends_rows()

        if not pt_rows:
            findings = (
                "<p>Parallel trends 検定結果がデータベースにありません。</p>"
                "<p>事前期間リード (k ∈ {−3, −2}) の joint F-test (cluster-sandwich Wald test) で検定。"
                "H0: β_{-3} = β_{-2} = 0 (事前期間に処置群・対照群で trend の差異なし)。"
                "H0 棄却不能 (p ≥ 0.05) であれば parallel trends 仮定の支持証拠となる。</p>"
            )
            return ReportSection(
                title="Parallel Trends 検定 (事前期間リード F-test)",
                findings_html=findings,
                section_id="did_parallel_trends",
            )

        findings = (
            "<p>事前期間リード (k ∈ {−3, −2}) の joint F-test (cluster-sandwich Wald, "
            "H0: β_{−3} = β_{−2} = 0):</p><ul>"
        )
        all_parallel = True
        for r in pt_rows:
            outcome = r["outcome"] if hasattr(r, "__getitem__") else r.outcome
            f_stat = r["f_stat"] if hasattr(r, "__getitem__") else r.f_stat
            p_val = r["p_value"] if hasattr(r, "__getitem__") else r.p_value
            parallel = r["trends_parallel"] if hasattr(r, "__getitem__") else r.trends_parallel
            if not parallel:
                all_parallel = False
            verdict = "成立 (H0 棄却不能)" if parallel else "違反の可能性 (H0 棄却)"
            findings += (
                f"<li><strong>{outcome}</strong>: "
                f"F = {f_stat:.3f}, p = {p_val:.4f} → {verdict}</li>"
            )
        findings += "</ul>"

        if not all_parallel:
            findings += (
                "<p>一部のアウトカムで parallel trends の潜在的違反が検出された。"
                "DiD 推定値の因果解釈には注意が必要。"
                "Synthetic control などの代替推定量の検討を推奨する。</p>"
            )
        else:
            findings += (
                "<p>全アウトカムで parallel trends 仮定が支持された "
                "(事前期間リードが 0 と統計的に区別不能)。</p>"
            )

        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="Parallel Trends 検定 (事前期間リード F-test)",
            findings_html=findings,
            section_id="did_parallel_trends",
            method_note=(
                "Parallel trends test: "
                "joint Wald F-test on pre-period leads k ∈ {-3, -2} = 0. "
                "Person-clustered sandwich covariance matrix. "
                "F = (R beta)' (R V R')^{-1} (R beta) / q, where q=2 restrictions. "
                "H0 rejected at alpha=0.05 → potential parallel trends violation → "
                "DiD estimates require caution (consider synthetic control)."
            ),
        )

    # ── Section 5: HTE (heterogeneous treatment effect by subgroup) ──────────

    def _build_hte_section(self, sb: SectionBuilder) -> ReportSection:
        """Subgroup CATE 表示。pipeline で feat_did_hte が生成されていれば描画、
        無ければ skeleton + 設計開示。"""
        rows = self._load_hte_rows()
        if not rows:
            findings = (
                "<p>HTE section: subgroup CATE / T-learner 結果は本データ範囲では"
                "未生成 (feat_did_hte 未投入)。pipeline post_processing が HTE を"
                "計算するまでは ATE のみ表示する。</p>"
                "<p>本 section は src/analysis/causal/heterogeneous_effects.py の "
                "interaction-term DiD で cohort_decade × gender × studio_tier の "
                "subgroup CATE を可視化する設計枠組み。homogeneity F-test の p-value "
                "で「treatment 効果が subgroup 間で有意に異なるか」を判定する。</p>"
            )
            method_note = (
                "interaction-term spec: y = α + β·treated + Σ_s γ_s·sub_s + "
                "Σ_s δ_s·(treated × sub_s) + ε。HC0 SE で per-subgroup CATE = β + δ_s "
                "を構成。homogeneity F-test (`scipy.stats.f`) は δ_s 全て 0 の H0 を検定する。"
                "T-learner (Künzel 2019) は Random Forest で個体 CATE を non-parametric "
                "推定し、heterogeneity driver feature を variance proxy で抽出する。"
            )
            return ReportSection(
                title="HTE: subgroup CATE 分解 (設計枠組み)",
                findings_html=findings,
                method_note=method_note,
                section_id="hte_subgroup",
            )

        # Render rows
        body_rows = "".join(
            f"<tr><td>{r[0]}</td><td>{r[1]:+.4f}</td>"
            f"<td>[{r[2]:+.4f}, {r[3]:+.4f}]</td><td>{r[4]:,}</td></tr>"
            for r in rows
        )
        findings = (
            "<p>subgroup × outcome の CATE table:</p>"
            "<table><thead><tr><th>subgroup</th><th>CATE</th>"
            "<th>95% CI</th><th>n</th></tr></thead>"
            f"<tbody>{body_rows}</tbody></table>"
        )
        return ReportSection(
            title="HTE: subgroup CATE 分解",
            findings_html=findings,
            method_note=(
                "interaction-term DiD + HC0 SE。homogeneity F-test で subgroup 間有意差検定。"
                "ATE の単純平均と一致しない場合、heterogeneity driver の探索を推奨。"
            ),
            section_id="hte_subgroup",
        )

    def _load_hte_rows(self) -> list:
        """Load HTE subgroup × outcome rows (feat_did_hte)。"""
        try:
            return self.conn.execute("""
                SELECT subgroup_label, cate, ci_lower, ci_upper, n_obs
                FROM feat_did_hte
                WHERE cate IS NOT NULL
                ORDER BY subgroup_label
            """).fetchall()
        except Exception:
            return []

    # ── Glossary ──────────────────────────────────────────────────────────────

    def _build_glossary(self) -> dict[str, str]:
        return {
            "theta_i": (
                "AKM person fixed effect: 制作規模アウトカムの個人間差異を捉える構造的指標。"
                "anime.score を使用しない。"
            ),
            "opportunity_residual": (
                "機会残差: theta_i・在職年数・役職多様性で予測されるクレジット数からの乖離。"
                "正 = 予測を超えるクレジット獲得、負 = 構造的機会不足。"
            ),
            "credit_count": "年間クレジット数 (作品への参加記録数、回帰では log 変換)。",
            "DiD": (
                "Difference-in-Differences: 処置群・対照群の before-after 差分を比較する "
                "準実験的因果推定手法。識別仮定は parallel trends。"
            ),
            "parallel_trends": (
                "並行トレンド仮定: 処置がなかった反事実世界で処置群・対照群が "
                "同じトレンドを辿ること。事前期間リード係数が 0 に近いことで検証。"
            ),
            "cluster SE": (
                "クラスターロバスト標準誤差: 同一個人の観測値間の相関を考慮した "
                "sandwich 推定量。個人レベルでクラスタリング。"
            ),
            "event_year": "スタジオ移籍が発生した年 (t=0)。",
            "primary_studio": "直近3年で最多クレジットのスタジオ (タイブレーク: 最新年)。",
        }


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import SensitivityAxis, make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="causal_studio_transfer",
    audience="technical_appendix",
    sensitivity_grid=[
        SensitivityAxis(name="placebo_year_offsets", values=[-5, -4, -3, 3, 4, 5]),
        SensitivityAxis(name="e_value_sensitivity", values=["VanderWeele 2017"]),
        SensitivityAxis(name="joint_leads_alpha", values=[0.05]),
    ],
    claim=(
        "person × year panel における studio transfer を処置とする DiD で、"
        "theta_i / opportunity_residual / credit_count への ATE が 95% CI で "
        "0 を跨がないこと、かつ parallel trends 仮定 (leads joint F-test p > 0.05) "
        "を満たす場合に因果効果の証拠として採用する。"
    ),
    identifying_assumption=(
        "parallel trends: potential outcome under no treatment が treated / control 群"
        "で平行に推移。selection on observables (cohort × role) で観測可能 confounder を吸収。"
        "unobserved confounder の sensitivity は E-value (did_robustness.py) で別途評価。"
    ),
    null_model=[
        "leads (-3, -2, -1) joint Wald F-test for parallel trends",
        "placebo: fake_event_year ± offset での DiD",
        "E-value (VanderWeele & Ding 2017)",
    ],
    sources=["credits", "persons", "anime_studios", "feat_did_studio_transfer"],
    meta_table="meta_did_studio_transfer",
    estimator="2-way FE DiD + cluster-robust SE (person)",
    ci_estimator="analytical", n_resamples=0,
    extra_limitations=[
        "Self-selection: 移籍者の事前 trajectory が control と乖離する可能性",
        "Studio FE 変化と移籍効果の分離困難",
        "Limited mobility bias (Andrews 2008): 単スタジオ person は推定除外",
        "treatment timing 異質性 (rolling event) は static DiD では bias 可能性",
    ],
    alternative_interpretations=(
        "正の ATE は移籍効果ではなく self-selection (移籍前から theta 上昇軌道) を反映。parallel trends test + placebo + E-value で頑健性確認要。",
        "studio FE 変化 (移籍先 studio の構造的特性) が theta 変化の主因で、移籍行動自体の効果は小さい可能性。Andrews-2way decomposition 推奨。",
        "treatment timing 異質性 (rolling event) で static DiD は negative weight 含み biased。Callaway-Sant'Anna (2021) で再推定すれば結果が変わる可能性。",
    ),
)

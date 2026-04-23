"""Score Layers Analysis report — v2 compliant.

Full 3-layer score decomposition porting all 15 charts from the original
generate_score_layers_report() in generate_all_reports.py:

- Section 1: Summary stats + 3-layer violin (Chart 1)
- Section 2: Component distributions (Chart 2, histogram grid)
- Section 3: Correlation matrix heatmap (Chart 3)
- Section 4: IV weight composition (Chart 4)
- Section 5: Layer density scatter (Charts 5a, 5b)
- Section 6: Layer gap distribution (Chart 6)
- Section 7: Top-50 parallel coordinates (Chart 7)
- Section 8: Person FE vs BiRank + Patronage density (Charts 8, 9)
- Section 9: Role-based 3-layer radar (Chart 10)
- Section 10: Career stage box plot (Chart 11)
- Section 11: Person FE confidence intervals (Chart 12)
- Section 12: Gini coefficients (Chart 14)
- Section 13: Partial R-squared (Chart 15)
- Section 14: Dormancy impact (Chart 16)
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats as sp_stats
from scipy.stats import rankdata

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import JSON_DIR, adaptive_height, density_scatter_2d
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_LAYER_COLORS = {
    "causal": "#f093fb",
    "structural": "#06D6A0",
    "collab": "#667eea",
}

_COMP_COLORS = {
    "person_fe": "#f093fb",
    "birank": "#06D6A0",
    "patronage": "#667eea",
    "awcc": "#FFD166",
    "ndi": "#a0d2db",
    "studio_fe_exposure": "#f5576c",
    "iv_score": "#fda085",
    "dormancy": "#c0c0d0",
}

_RADAR_ROLES = [
    ("animator", "#f093fb"),
    ("director", "#06D6A0"),
    ("designer", "#667eea"),
    ("production", "#FFD166"),
    ("writing", "#f5576c"),
    ("technical", "#a0d2db"),
]


def _pct_rank(arr: np.ndarray) -> np.ndarray:
    """Compute percentile rank (0-100)."""
    return rankdata(arr, method="average") / len(arr) * 100


def _gini(arr: np.ndarray) -> float:
    """Gini coefficient of an array (values shifted to non-negative)."""
    arr = np.sort(np.abs(arr))
    n = len(arr)
    if n == 0 or arr.sum() == 0:
        return 0.0
    index = np.arange(1, n + 1)
    return float((2 * np.sum(index * arr) / (n * np.sum(arr))) - (n + 1) / n)


class ScoreLayersAnalysisReport(BaseReportGenerator):
    name = "score_layers_analysis"
    title = "スコア層別分解分析"
    subtitle = (
        "因果 / 構造 / 協業 — 3層分解の特性・相関・分布・不平等度を15チャートで分析"
    )
    filename = "score_layers_analysis.html"

    glossary_terms = {
        "因果層 (Causal Layer)": (
            "AKM固定効果推定によるperson_fe。"
            "スタジオ効果を除去した個人の因果的貢献。"
        ),
        "構造層 (Structural Layer)": (
            "BiRank、AWCC、NDIの3指標。"
            "ネットワーク上の位置を記述する統計量（因果推論なし）。"
        ),
        "協業層 (Collaboration Layer)": (
            "Patronage Premium（監督起用プレミアム）と"
            "Studio FE Exposure（スタジオ環境の質）。"
        ),
        "PC1 分散説明率": (
            "PCA第1主成分が全コンポーネントの分散をどれだけ説明するか。"
            "高いほど1次元にまとめやすい。"
        ),
        "パーセンタイル": "全人物中の相対的順位。50=中央値。90=上位10%。",
        "Gini係数": "不平等度指標。0=完全平等、1=完全不平等。",
        "部分R²": (
            "特定の変数を除去したときのR²低下量。"
            "大きいほどその変数の説明力が高い。"
        ),
    }

    # ── Data loading ─────────────────────────────────────────────

    def _load_score_data(self) -> list[dict] | None:
        """Load all score components from feat_person_scores + feat_career."""
        try:
            rows = self.conn.execute("""
                SELECT
                    fps.person_id,
                    p.name_ja, p.name_zh, p.name_en,
                    fps.person_fe, fps.person_fe_se, fps.person_fe_n_obs,
                    fps.birank, fps.patronage, fps.awcc,
                    fps.ndi, fps.studio_fe_exposure, fps.iv_score,
                    fps.dormancy, fps.career_friction,
                    fps.iv_score_pct, fps.person_fe_pct,
                    fps.birank_pct, fps.patronage_pct, fps.awcc_pct,
                    fc.primary_role, fc.highest_stage, fc.first_year
                FROM feat_person_scores fps
                JOIN persons p ON fps.person_id = p.id
                LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
                WHERE fps.person_fe IS NOT NULL
                  AND fps.birank IS NOT NULL
                  AND fps.iv_score IS NOT NULL
            """).fetchall()
        except Exception:
            return None
        if len(rows) < 50:
            return None
        return [dict(r) for r in rows]

    def _load_iv_weights(self) -> dict:
        """Load IV weight config from JSON (not in DB)."""
        path = JSON_DIR / "iv_weights.json"
        if path.exists():
            with open(path) as f:
                return json.load(f)
        return {}

    # ── Array building ───────────────────────────────────────────

    def _build_arrays(
        self, data: list[dict]
    ) -> dict[str, np.ndarray]:
        """Build numpy arrays from score data rows."""
        n = len(data)
        pfe = np.array([d["person_fe"] or 0.0 for d in data])
        br = np.array([d["birank"] or 0.0 for d in data])
        pat = np.array([d["patronage"] or 0.0 for d in data])
        awcc = np.array([d["awcc"] or 0.0 for d in data])
        ndi = np.array([d["ndi"] or 0.0 for d in data])
        st_exp = np.array([d["studio_fe_exposure"] or 0.0 for d in data])
        iv = np.array([d["iv_score"] or 0.0 for d in data])
        dorm = np.array([d["dormancy"] or 0.0 for d in data])

        # Percentile ranks
        pfe_pct = _pct_rank(pfe)
        br_pct = _pct_rank(br)
        pat_pct = _pct_rank(pat)
        awcc_pct = _pct_rank(awcc)
        ndi_pct = _pct_rank(ndi)
        st_pct = _pct_rank(st_exp)

        # 3-layer aggregates (percentile space)
        causal_agg = pfe_pct
        structural_agg = (br_pct + awcc_pct + ndi_pct) / 3
        collab_agg = (pat_pct + st_pct) / 2

        names = [
            d.get("name_ja") or d.get("name_zh") or d.get("name_en") or d["person_id"]
            for d in data
        ]

        return {
            "n": n,
            "pfe": pfe, "br": br, "pat": pat, "awcc": awcc,
            "ndi": ndi, "st_exp": st_exp, "iv": iv, "dorm": dorm,
            "pfe_pct": pfe_pct, "br_pct": br_pct, "pat_pct": pat_pct,
            "awcc_pct": awcc_pct, "ndi_pct": ndi_pct, "st_pct": st_pct,
            "causal_agg": causal_agg,
            "structural_agg": structural_agg,
            "collab_agg": collab_agg,
            "names": names,
        }

    # ── Main generate ────────────────────────────────────────────

    def generate(self) -> Path | None:
        data = self._load_score_data()
        if not data:
            return None

        iv_data = self._load_iv_weights()
        arrays = self._build_arrays(data)
        sb = SectionBuilder()

        section_builders = [
            self._build_summary_violin_section,
            self._build_component_histograms_section,
            self._build_correlation_section,
            self._build_iv_weights_section,
            self._build_density_scatter_section,
            self._build_gap_distribution_section,
            self._build_parallel_coords_section,
            self._build_pfe_birank_patronage_section,
            self._build_role_radar_section,
            self._build_career_stage_section,
            self._build_pfe_ci_section,
            self._build_gini_section,
            self._build_partial_r2_section,
            self._build_dormancy_section,
        ]

        sections: list[str] = []
        for builder in section_builders:
            sections.append(
                sb.build_section(builder(sb, data, arrays, iv_data))
            )

        return self.write_report("\n".join(sections))

    # ── Section 1: Summary + 3-layer violin ──────────────────────

    def _build_summary_violin_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        n = a["n"]
        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]

        # Inter-layer correlations
        r_cs, _ = sp_stats.pearsonr(causal_agg, structural_agg)
        r_cc, _ = sp_stats.pearsonr(causal_agg, collab_agg)
        r_sc, _ = sp_stats.pearsonr(structural_agg, collab_agg)

        # Top-10% overlap
        top10_n = max(n // 10, 1)
        top10_causal = set(np.argsort(causal_agg)[-top10_n:])
        top10_struct = set(np.argsort(structural_agg)[-top10_n:])
        top10_collab = set(np.argsort(collab_agg)[-top10_n:])
        overlap_cs = len(top10_causal & top10_struct) / top10_n * 100
        overlap_cc = len(top10_causal & top10_collab) / top10_n * 100
        overlap_sc = len(top10_struct & top10_collab) / top10_n * 100

        lw = iv_data.get("lambda_weights", {})
        weight_method = iv_data.get("weight_method", "fixed_prior")
        var_expl = iv_data.get("pca_variance_explained", 0)

        # Distribution summaries for each layer
        c_summ = distribution_summary(causal_agg.tolist(), label="causal_pct")
        s_summ = distribution_summary(
            structural_agg.tolist(), label="structural_pct"
        )
        co_summ = distribution_summary(collab_agg.tolist(), label="collab_pct")

        findings = (
            f"<p>3層スコア分解（n={n:,}人、"
            f"ウェイト手法: {weight_method}"
            f"{f', PC1分散説明率: {var_expl:.1%}' if var_expl else ''}"
            f"）:</p><ul>"
            f"<li>因果層（Person FEパーセンタイル）: "
            f"{format_distribution_inline(c_summ)}, "
            f"{format_ci((c_summ['ci_lower'], c_summ['ci_upper']))}.</li>"
            f"<li>構造層（BiRank+AWCC+NDI平均パーセンタイル）: "
            f"{format_distribution_inline(s_summ)}, "
            f"{format_ci((s_summ['ci_lower'], s_summ['ci_upper']))}.</li>"
            f"<li>協業層（Patronage+Studio Exp平均パーセンタイル）: "
            f"{format_distribution_inline(co_summ)}, "
            f"{format_ci((co_summ['ci_lower'], co_summ['ci_upper']))}.</li>"
            f"</ul>"
            f"<p>層間Pearson相関係数: "
            f"因果-構造 r={r_cs:.3f}, "
            f"因果-協業 r={r_cc:.3f}, "
            f"構造-協業 r={r_sc:.3f}。 "
            f"上位10%の重複率: 因果-構造 {overlap_cs:.1f}%, "
            f"因果-協業 {overlap_cc:.1f}%, "
            f"構造-協業 {overlap_sc:.1f}%。</p>"
        )
        if lw:
            weight_str = " / ".join(
                f"{k}={v:.1%}"
                for k, v in sorted(lw.items(), key=lambda x: -x[1])
            )
            findings += f"<p>IVウェイト: {weight_str}。</p>"

        # Violin plot
        rng = np.random.default_rng(42)
        sample_idx = rng.choice(n, min(5000, n), replace=False)
        fig = go.Figure()
        for name, arr, color in [
            ("因果層 (Person FE)", causal_agg[sample_idx], _LAYER_COLORS["causal"]),
            ("構造層 (BR+AWCC+NDI)", structural_agg[sample_idx], _LAYER_COLORS["structural"]),
            ("協業層 (Pat+Studio)", collab_agg[sample_idx], _LAYER_COLORS["collab"]),
        ]:
            fig.add_trace(go.Violin(
                y=arr, name=name, box_visible=True, meanline_visible=True,
                fillcolor=color, opacity=0.6, line_color=color,
                points="outliers",
            ))
        fig.update_layout(
            yaxis_title="パーセンタイル",
            showlegend=False,
            violinmode="group",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="3層スコア分布比較（パーセンタイル空間）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "layer_violin", height=450),
            method_note=(
                "因果層 = Person FE パーセンタイル順位。"
                "構造層 = BiRank, AWCC, NDI パーセンタイル順位の平均。"
                "協業層 = Patronage, Studio FE Exposure パーセンタイル順位の平均。"
                "Pearson 相関は全体集団で計算。"
                "バイオリンは描画のため最大5,000名にサブサンプリング。"
            ),
            section_id="summary_violin",
        )

    # ── Section 2: Component histogram grid ──────────────────────

    def _build_component_histograms_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        n = a["n"]
        comp_names = [
            "Person FE", "BiRank", "Patronage", "AWCC",
            "NDI", "Studio Exp", "IV Score", "Dormancy",
        ]
        comp_arrays = [
            a["pfe"], a["br"], a["pat"], a["awcc"],
            a["ndi"], a["st_exp"], a["iv"], a["dorm"],
        ]
        comp_colors = [
            "#f093fb", "#06D6A0", "#667eea", "#FFD166",
            "#a0d2db", "#f5576c", "#fda085", "#c0c0d0",
        ]

        # Build summary stats for findings
        summaries = []
        for cname, arr in zip(comp_names, comp_arrays):
            s = distribution_summary(arr.tolist(), label=cname)
            summaries.append(s)

        findings = (
            f"<p>8コンポーネントの生値分布（n={n:,}）:</p><ul>"
        )
        for s in summaries:
            findings += (
                f"<li><strong>{s['label']}</strong>: "
                f"{format_distribution_inline(s)}。</li>"
            )
        findings += "</ul>"

        fig = make_subplots(
            rows=2, cols=4,
            subplot_titles=comp_names,
            horizontal_spacing=0.06, vertical_spacing=0.12,
        )
        for idx, (arr, color) in enumerate(zip(comp_arrays, comp_colors)):
            r, c = divmod(idx, 4)
            q01, q99 = np.percentile(arr, [1, 99])
            clipped = arr[(arr >= q01) & (arr <= q99)]
            fig.add_trace(
                go.Histogram(
                    x=clipped, nbinsx=60, marker_color=color,
                    opacity=0.75, showlegend=False,
                ),
                row=r + 1, col=c + 1,
            )
            med = float(np.median(arr))
            fig.add_vline(
                x=med, line_dash="solid", line_color="#ff4444",
                line_width=1.5, row=r + 1, col=c + 1,
            )
            mean_val = float(np.mean(arr))
            fig.add_vline(
                x=mean_val, line_dash="dot", line_color="#4488ff",
                line_width=1, row=r + 1, col=c + 1,
            )
        # Log x-axis for power-law distributed components
        fig.update_xaxes(type="log", row=1, col=2)  # BiRank
        fig.update_xaxes(type="log", row=1, col=3)  # Patronage
        fig.update_xaxes(type="log", row=1, col=4)  # AWCC
        fig.update_layout(height=550, margin=dict(t=60, b=40))

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="全コンポーネント分布（生値）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "component_histograms", height=550,
            ),
            method_note=(
                "赤の実線 = 中央値、青の点線 = 平均。"
                "1–99パーセンタイル範囲外の外れ値は可視化のためクリップ。"
                "BiRank, Patronage, AWCC は冪乗分布の形状により対数x軸を使用。"
            ),
            section_id="component_histograms",
        )

    # ── Section 3: Correlation heatmap ───────────────────────────

    def _build_correlation_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        comp_names = [
            "person_fe", "birank", "patronage",
            "awcc", "ndi", "studio_exp", "iv_score",
        ]
        comp_arrays = [
            a["pfe"], a["br"], a["pat"],
            a["awcc"], a["ndi"], a["st_exp"], a["iv"],
        ]

        k = len(comp_names)
        corr_matrix = np.zeros((k, k))
        for i in range(k):
            for j in range(k):
                corr_matrix[i, j] = np.corrcoef(
                    comp_arrays[i], comp_arrays[j]
                )[0, 1]

        # Find max off-diagonal correlation
        off_diag = corr_matrix.copy()
        np.fill_diagonal(off_diag, 0)
        max_corr_idx = np.unravel_index(np.argmax(np.abs(off_diag)), off_diag.shape)
        max_pair = (comp_names[max_corr_idx[0]], comp_names[max_corr_idx[1]])
        max_val = off_diag[max_corr_idx]
        min_corr_idx = np.unravel_index(np.argmin(off_diag), off_diag.shape)
        min_pair = (comp_names[min_corr_idx[0]], comp_names[min_corr_idx[1]])
        min_val = off_diag[min_corr_idx]

        findings = (
            f"<p>7つのスコアコンポーネント間のPearson相関行列"
            f"（n={a['n']:,}）。</p>"
            f"<p>対角線外の最大絶対相関: "
            f"r({max_pair[0]}, {max_pair[1]})={max_val:.3f}。 "
            f"対角線外の最小相関: "
            f"r({min_pair[0]}, {min_pair[1]})={min_val:.3f}。</p>"
        )

        fig = go.Figure(go.Heatmap(
            z=corr_matrix,
            x=comp_names, y=comp_names,
            colorscale="RdBu_r", zmid=0, zmin=-1, zmax=1,
            text=np.round(corr_matrix, 3),
            texttemplate="%{text:.3f}",
            textfont=dict(size=11),
            hovertemplate="r(%{x}, %{y}) = %{z:.4f}<extra></extra>",
        ))
        fig.update_layout(height=500, xaxis_tickangle=30)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="コンポーネント間相関行列",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "corr_heatmap", height=500),
            method_note=(
                "生値（パーセンタイル変換前）によるPearson相関。"
                "1.0に近い値は情報の冗長性を示す。0に近い値は独立した視点を示す。"
                "負の値はトレードオフ関係を示す。"
            ),
            section_id="correlation_matrix",
        )

    # ── Section 4: IV weight composition ─────────────────────────

    def _build_iv_weights_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        lw = iv_data.get("lambda_weights", {})
        weight_method = iv_data.get("weight_method", "fixed_prior")
        var_expl = iv_data.get("pca_variance_explained", 0)

        if not lw:
            return ReportSection(
                title="IV Score 構成ウェイト",
                findings_html=(
                    "<p>IVウェイトデータ（iv_weights.json）が利用できません。</p>"
                ),
                section_id="iv_weights",
            )

        sorted_lw = sorted(lw.items(), key=lambda x: x[1])
        max_comp = sorted_lw[-1]
        min_comp = sorted_lw[0]

        method_label = (
            f"PCA PC1 (variance explained: {var_expl:.1%})"
            if weight_method == "PCA_PC1"
            else "fixed prior"
        )

        findings = (
            f"<p>IV Scoreウェイト導出手法: {method_label}。 "
            f"主要コンポーネント数: {len(lw)}。</p>"
            f"<p>最大ウェイト: {max_comp[0]}={max_comp[1]:.3f}。 "
            f"最小ウェイト: {min_comp[0]}={min_comp[1]:.3f}。 "
            f"ウェイト合計: {sum(lw.values()):.3f}。</p>"
        )

        weight_colors = [
            "#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#f5576c",
        ]
        fig = go.Figure(go.Bar(
            y=[k for k, _ in sorted_lw],
            x=[v for _, v in sorted_lw],
            orientation="h",
            marker_color=weight_colors[:len(sorted_lw)],
            text=[f"{v:.3f}" for _, v in sorted_lw],
            textposition="outside",
        ))
        fig.update_layout(xaxis_title="ウェイト (λ)", height=350)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="IV Score 構成ウェイト",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "iv_weights_bar", height=350,
            ),
            method_note=(
                f"ウェイトは {method_label} により導出。"
                "棒グラフは統合IV Scoreへの各コンポーネントの寄与を示す。"
                "ウェイトは合計1.0に正規化。"
                "Dormancy乗数は加重合計後に適用。"
            ),
            section_id="iv_weights",
        )

    # ── Section 5: Density scatter (causal vs structural/collab) ─

    def _build_density_scatter_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]
        names = a["names"]
        n = a["n"]

        # Correlation values
        r_cs = float(np.corrcoef(causal_agg, structural_agg)[0, 1])
        r_cc = float(np.corrcoef(causal_agg, collab_agg)[0, 1])

        findings = (
            f"<p>層パーセンタイルの密度等高線プロット（n={n:,}）:</p><ul>"
            f"<li>因果層 vs 構造層（r={r_cs:.3f}）: "
            f"対角線からの乖離は個人貢献（AKM）とネットワーク位置の"
            f"不一致を示す。</li>"
            f"<li>因果層 vs 協業層（r={r_cc:.3f}）: "
            f"対角線からの乖離は個人貢献と協業環境の"
            f"不一致を示す。</li>"
            f"</ul>"
        )

        # 5a: causal vs structural
        fig5a = density_scatter_2d(
            causal_agg.tolist(), structural_agg.tolist(),
            xlabel="因果層パーセンタイル",
            ylabel="構造層パーセンタイル",
            title="因果層 vs 構造層",
            label_names=names, label_top=10, height=500,
        )
        fig5a.add_shape(
            type="line", x0=0, y0=0, x1=100, y1=100,
            line=dict(color="rgba(255,255,255,0.3)", dash="dash"),
        )

        # 5b: causal vs collab
        fig5b = density_scatter_2d(
            causal_agg.tolist(), collab_agg.tolist(),
            xlabel="因果層パーセンタイル",
            ylabel="協業層パーセンタイル",
            title="因果層 vs 協業層",
            label_names=names, label_top=10, height=500,
        )
        fig5b.add_shape(
            type="line", x0=0, y0=0, x1=100, y1=100,
            line=dict(color="rgba(255,255,255,0.3)", dash="dash"),
        )

        viz_html = (
            plotly_div_safe(fig5a, "layer_density_cs", height=500)
            + plotly_div_safe(fig5b, "layer_density_cc", height=500)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="層間密度散布図",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "2D密度等高線（Histogram2dContour）。y値による上位N人物をラベル表示。"
                "対角線 = 層間完全一致。"
                "対角線の上側: 構造/協業層が因果層より高い。"
                "対角線の下側: 因果層が構造/協業層より高い。"
            ),
            section_id="density_scatter",
        )

    # ── Section 6: Gap distribution ──────────────────────────────

    def _build_gap_distribution_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]
        n = a["n"]

        gap_cs = causal_agg - structural_agg
        gap_cc = causal_agg - collab_agg

        cs_summ = distribution_summary(gap_cs.tolist(), label="causal-structural")
        cc_summ = distribution_summary(gap_cc.tolist(), label="causal-collab")

        findings = (
            f"<p>ギャップ分布（因果層パーセンタイルから他層パーセンタイルを減算、"
            f"n={n:,}）:</p><ul>"
            f"<li>因果層 - 構造層: "
            f"{format_distribution_inline(cs_summ)}, "
            f"{format_ci((cs_summ['ci_lower'], cs_summ['ci_upper']))}。 "
            f"std={cs_summ['std']:.1f}。</li>"
            f"<li>因果層 - 協業層: "
            f"{format_distribution_inline(cc_summ)}, "
            f"{format_ci((cc_summ['ci_lower'], cc_summ['ci_upper']))}。 "
            f"std={cc_summ['std']:.1f}。</li>"
            f"</ul>"
        )

        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=gap_cs, nbinsx=80, name="因果-構造",
            marker_color=_LAYER_COLORS["causal"], opacity=0.6,
        ))
        fig.add_trace(go.Histogram(
            x=gap_cc, nbinsx=80, name="因果-協業",
            marker_color=_LAYER_COLORS["collab"], opacity=0.6,
        ))
        fig.update_layout(
            barmode="overlay",
            xaxis_title="パーセンタイルギャップ",
            yaxis_title="人数",
            height=400,
        )
        fig.add_annotation(
            x=0.95, y=0.95, xref="paper", yref="paper",
            text=(
                f"因果-構造 σ={np.std(gap_cs):.1f}<br>"
                f"因果-協業 σ={np.std(gap_cc):.1f}"
            ),
            showarrow=False, font=dict(size=12, color="#e0e0f0"),
            bgcolor="rgba(0,0,0,0.5)", bordercolor="#666",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="層間ギャップスコア分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "gap_histogram", height=400,
            ),
            method_note=(
                "ギャップ = 因果層パーセンタイル − 他層パーセンタイル。"
                "正の値: 当該人物は構造/協業層より因果層（AKM）で高順位。"
                "負の値: その逆。"
                "0中心の分布は全体的な整合を示す。"
            ),
            section_id="gap_distribution",
        )

    # ── Section 7: Parallel coordinates (top 50) ─────────────────

    def _build_parallel_coords_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        iv = a["iv"]
        n = a["n"]
        top50_idx = np.argsort(iv)[-50:]

        # Build dimension arrays
        dims = [
            dict(label="Person FE", values=a["pfe_pct"][top50_idx], range=[0, 100]),
            dict(label="BiRank", values=a["br_pct"][top50_idx], range=[0, 100]),
            dict(label="Patronage", values=a["pat_pct"][top50_idx], range=[0, 100]),
            dict(label="AWCC", values=a["awcc_pct"][top50_idx], range=[0, 100]),
            dict(label="NDI", values=a["ndi_pct"][top50_idx], range=[0, 100]),
            dict(label="Studio Exp", values=a["st_pct"][top50_idx], range=[0, 100]),
        ]

        top50_iv_min = float(iv[top50_idx].min())
        top50_iv_max = float(iv[top50_idx].max())

        findings = (
            f"<p>IV Score上位50人のパラレル座標プロット"
            f"（全{n:,}人中50人）。各軸は6コンポーネントのパーセンタイル順位（0-100）を示す。"
            f"線の色はIV Score"
            f"（範囲: {top50_iv_min:.3f}〜{top50_iv_max:.3f}）を表す。</p>"
        )

        fig = go.Figure(go.Parcoords(
            line=dict(
                color=iv[top50_idx],
                colorscale="Plasma",
                showscale=True,
                colorbar=dict(title="IV Score"),
            ),
            dimensions=dims,
        ))
        fig.update_layout(height=500)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="上位50人のパラレル座標プロット",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "parallel_coords", height=500,
            ),
            method_note=(
                "IV Scoreにより上位50人を選抜。各縦軸はパーセンタイル順位（0-100）。"
                "全軸で高位置を通る線は、一様に高順位の人物を示す。"
                "特定軸での大きな落ち込みは、コンポーネントレベルの弱点を示す。"
            ),
            section_id="parallel_coords",
        )

    # ── Section 8: Person FE vs BiRank + Patronage density ───────

    def _build_pfe_birank_patronage_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        pfe = a["pfe"]
        br = a["br"]
        pat = a["pat"]
        names = a["names"]
        n = a["n"]

        r_pb = float(np.corrcoef(pfe, br)[0, 1])
        r_pp = float(np.corrcoef(pfe, pat)[0, 1])

        findings = (
            f"<p>生値による密度等高線プロット（n={n:,}）:</p>"
            f"<ul>"
            f"<li>Person FE vs BiRank: r={r_pb:.3f}。 "
            f"両指標は同一のクレジットデータから導出されており、"
            f"相関は構造的に内在している。</li>"
            f"<li>Person FE vs Patronage: r={r_pp:.3f}。 "
            f"Patronageは監督起用プレミアム、"
            f"Person FEはスタジオ効果を除去した個人貢献を測定。</li>"
            f"</ul>"
        )

        # Chart 8: PFE vs BiRank
        fig8 = density_scatter_2d(
            pfe.tolist(), br.tolist(),
            xlabel="Person FE (θ)",
            ylabel="BiRank",
            title="Person FE vs BiRank",
            label_names=names, label_top=12, height=500,
        )
        fig8.add_annotation(
            x=0.02, y=0.98, xref="paper", yref="paper",
            text=f"r={r_pb:.3f}, n={n:,}",
            showarrow=False, font=dict(size=11, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
            bordercolor="#FFD166", borderwidth=1, borderpad=4,
        )

        # Chart 9: PFE vs Patronage
        fig9 = density_scatter_2d(
            pfe.tolist(), pat.tolist(),
            xlabel="Person FE (θ)",
            ylabel="Patronageプレミアム",
            title="Person FE vs Patronage",
            label_names=names, label_top=10, height=500,
        )
        fig9.add_annotation(
            x=0.02, y=0.98, xref="paper", yref="paper",
            text=f"r={r_pp:.3f}, n={n:,}",
            showarrow=False, font=dict(size=11, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
            bordercolor="#FFD166", borderwidth=1, borderpad=4,
        )

        viz_html = (
            plotly_div_safe(fig8, "pfe_vs_birank", height=500)
            + plotly_div_safe(fig9, "pfe_vs_patronage", height=500)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="Person FE vs BiRank / Patronage（密度等高線）",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "生値（パーセンタイル変換前）に対する密度等高線。"
                "Person FE (AKM θ_i) はスタジオ効果を除去した値。"
                "BiRank は人物-アニメ二部グラフに基づくネットワーク位置指標。"
                "Patronage は監督起用プレミアム。"
                "BiRank と Patronage はいずれも Person FE と同一のクレジットデータから導出されるため、"
                "正の相関は独立ソースからの収束的妥当性検証ではなく、部分的に構造的に生じる。"
            ),
            interpretation_html=(
                '<p style="color:#b0b8c8;font-size:0.9rem;">'
                "<strong>執筆者:</strong> Animetor Eval 分析システム</p>"
                '<div class="competing-interp">'
                '<div class="ci-claim">解釈: Person FE と BiRank の間の正の相関は、'
                "両指標が共通の潜在因子を捉えていることを反映している可能性がある。</div>"
                '<div class="ci-alts">代替解釈:<ol>'
                "<li>両者は同一のクレジットデータから導出される。"
                "正の相関は構造的に保証されており、独立ソースからの収束的妥当性検証ではない。</li>"
                "<li>AKM θ_i はスタジオ効果を除去し、BiRank はネットワーク位置を反映する。"
                "両者の差がスタジオ環境に依存しない個人貢献である。</li>"
                "<li>総クレジット数（活動量）が両指標を同時に駆動する第三変数である可能性がある。</li>"
                "</ol></div></div>"
            ),
            section_id="pfe_birank_patronage",
        )

    # ── Section 9: Role radar ────────────────────────────────────

    def _build_role_radar_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        # Build role-level aggregates
        role_map: dict[str, dict[str, list[float]]] = {}
        for i, d in enumerate(data):
            role = d.get("primary_role") or "?"
            if role == "?":
                continue
            if role not in role_map:
                role_map[role] = {
                    "pfe": [], "br": [], "pat": [], "awcc": [], "st": [],
                }
            role_map[role]["pfe"].append(float(a["pfe_pct"][i]))
            role_map[role]["br"].append(float(a["br_pct"][i]))
            role_map[role]["pat"].append(float(a["pat_pct"][i]))
            role_map[role]["awcc"].append(float(a["awcc_pct"][i]))
            role_map[role]["st"].append(float(a["st_pct"][i]))

        categories = ["Person FE", "BiRank", "Patronage", "AWCC", "Studio Exp"]

        # Build findings from roles with enough data
        active_roles = []
        for role, color in _RADAR_ROLES:
            if role in role_map and len(role_map[role]["pfe"]) >= 10:
                active_roles.append(role)

        findings_parts = [
            f"<p>5コンポーネントにおける役職別中央値パーセンタイル"
            f"（n >= 10の役職のみ）:</p><ul>"
        ]
        for role in active_roles:
            rm = role_map[role]
            n_role = len(rm["pfe"])
            meds = [
                float(np.median(rm["pfe"])),
                float(np.median(rm["br"])),
                float(np.median(rm["pat"])),
                float(np.median(rm["awcc"])),
                float(np.median(rm["st"])),
            ]
            findings_parts.append(
                f"<li><strong>{role}</strong> (n={n_role}): "
                + ", ".join(
                    f"{c}={m:.0f}" for c, m in zip(categories, meds)
                )
                + ".</li>"
            )
        findings_parts.append("</ul>")
        findings = "".join(findings_parts)

        fig = go.Figure()
        for role, color in _RADAR_ROLES:
            if role not in role_map or len(role_map[role]["pfe"]) < 10:
                continue
            rm = role_map[role]
            vals = [
                float(np.median(rm["pfe"])),
                float(np.median(rm["br"])),
                float(np.median(rm["pat"])),
                float(np.median(rm["awcc"])),
                float(np.median(rm["st"])),
            ]
            fig.add_trace(go.Scatterpolar(
                r=vals + [vals[0]],
                theta=categories + [categories[0]],
                name=role, line_color=color,
                fill="toself", opacity=0.3,
            ))
        fig.update_layout(
            polar=dict(radialaxis=dict(range=[0, 100], showticklabels=True)),
            height=500,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="役職別の3層プロファイル比較",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "role_radar", height=500),
            method_note=(
                "レーダーチャートは5コンポーネントにわたる役職別中央値パーセンタイルを示す。"
                "10人未満の役職は除外。primary_role は feat_career より。"
                "役職間の形状差は、各役職がネットワーク位置と個人貢献をどのように蓄積するかの"
                "構造的差異を反映している。"
            ),
            section_id="role_radar",
        )

    # ── Section 10: Career stage box plot ────────────────────────

    def _build_career_stage_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]

        stage_data: dict[int, dict[str, list[float]]] = {
            st: {"causal": [], "struct": [], "collab": []}
            for st in range(7)
        }
        for i, d in enumerate(data):
            st = d.get("highest_stage")
            if st is None:
                continue
            st = int(st)
            if 0 <= st <= 6:
                stage_data[st]["causal"].append(float(causal_agg[i]))
                stage_data[st]["struct"].append(float(structural_agg[i]))
                stage_data[st]["collab"].append(float(collab_agg[i]))

        stage_labels = [
            "0:新人", "1:ジュニア", "2:中堅", "3:熟練",
            "4:ベテラン", "5:マスター", "6:レジェンド",
        ]

        # Build findings
        findings_parts = [
            "<p>キャリアステージ別の3層パーセンタイル分布"
            "（highest_stage 0-6）:</p><ul>"
        ]
        for st in range(7):
            sd = stage_data[st]
            n_st = len(sd["causal"])
            if n_st == 0:
                continue
            findings_parts.append(
                f"<li><strong>{stage_labels[st]}</strong> (n={n_st}): "
                f"causal median={np.median(sd['causal']):.0f}, "
                f"structural median={np.median(sd['struct']):.0f}, "
                f"collab median={np.median(sd['collab']):.0f}.</li>"
            )
        findings_parts.append("</ul>")
        findings = "".join(findings_parts)

        fig = go.Figure()
        for layer_name, key, color in [
            ("因果層", "causal", _LAYER_COLORS["causal"]),
            ("構造層", "struct", _LAYER_COLORS["structural"]),
            ("協業層", "collab", _LAYER_COLORS["collab"]),
        ]:
            x_vals: list[str] = []
            y_vals: list[float] = []
            for st in range(7):
                arr = stage_data[st][key]
                if arr:
                    x_vals.extend([stage_labels[st]] * len(arr))
                    y_vals.extend(arr)
            fig.add_trace(go.Box(
                x=x_vals, y=y_vals, name=layer_name,
                marker_color=color, opacity=0.7,
                boxmean=True,
            ))
        fig.update_layout(
            boxmode="group",
            yaxis_title="パーセンタイル",
            height=500,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="キャリアステージ別の3層スコア推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "stage_boxplot", height=500,
            ),
            method_note=(
                "キャリアステージ（highest_stage）は feat_career より: "
                "0=新人, 1=ジュニア, 2=中堅, 3=熟練, "
                "4=ベテラン, 5=マスター, 6=レジェンド。"
                "箱ひげ図は各ステージにおける層パーセンタイルの分布を示す。"
                "ダイヤモンドマーカー = 平均。"
            ),
            section_id="career_stage",
        )

    # ── Section 11: Person FE CI forest plot ─────────────────────

    def _build_pfe_ci_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        # Build CI data from DB
        ci_persons = []
        try:
            rows = self.conn.execute("""
                SELECT
                    p.name_ja, p.name_zh, p.name_en, fps.person_id,
                    fps.person_fe, fps.person_fe_se, fps.person_fe_n_obs
                FROM feat_person_scores fps
                JOIN persons p ON fps.person_id = p.id
                WHERE fps.person_fe IS NOT NULL
                  AND fps.person_fe_se IS NOT NULL
                  AND fps.person_fe_se > 0
                ORDER BY fps.person_fe DESC
                LIMIT 30
            """).fetchall()
        except Exception:
            rows = []

        for r in rows:
            pfe_val = r["person_fe"]
            se = r["person_fe_se"]
            name = r["name_ja"] or r["name_zh"] or r["name_en"] or r["person_id"]
            ci_persons.append({
                "name": name,
                "pfe": pfe_val,
                "se": se,
                "n_obs": r["person_fe_n_obs"] or 0,
                "lower": pfe_val - 1.96 * se,
                "upper": pfe_val + 1.96 * se,
            })

        if not ci_persons:
            return ReportSection(
                title="Person FE 信頼区間（上位30人）",
                findings_html=(
                    "<p>feat_person_scoresにPerson FE SEデータがありません。"
                    "person_fe_seを計算するにはパイプラインを再実行してください。</p>"
                ),
                section_id="pfe_ci",
            )

        # Findings
        se_vals = [c["se"] for c in ci_persons]
        se_summ = distribution_summary(se_vals, label="person_fe_se_top30")
        avg_width = np.mean([c["upper"] - c["lower"] for c in ci_persons])

        findings = (
            f"<p>Person FE（theta_i）上位{len(ci_persons)}人の"
            f"95%信頼区間。"
            f"SE = sigma_resid / sqrt(n_obs)。</p>"
            f"<p>SE分布（上位30人）: "
            f"{format_distribution_inline(se_summ)}。 "
            f"平均CI幅: {avg_width:.3f}。</p>"
        )

        n_items = len(ci_persons)
        h = adaptive_height(n_items)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[c["pfe"] for c in ci_persons],
            y=[c["name"] for c in ci_persons],
            mode="markers",
            marker=dict(size=8, color="#f093fb"),
            error_x=dict(
                type="data",
                array=[c["upper"] - c["pfe"] for c in ci_persons],
                arrayminus=[c["pfe"] - c["lower"] for c in ci_persons],
                color="#f093fb", thickness=1.5,
            ),
            hovertemplate=(
                "%{y}<br>theta=%{x:.3f} +/- %{error_x.array:.3f}"
                "<extra></extra>"
            ),
        ))
        fig.update_layout(
            xaxis_title="Person FE (θ) ± 95% CI",
            height=h,
            yaxis=dict(autorange="reversed"),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="Person FE 信頼区間（上位30人）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "pfe_ci_forest", height=h),
            method_note=(
                "person_fe_se は feat_person_scores より: SE = σ_resid / sqrt(n_obs)、"
                "n_obs は AKM 推定に使用されたクレジット作品数。"
                "95% CI = θ ± 1.96 × SE（正規近似）。"
                "狭いCI = 推定が安定。広いCI = 観測が少ないか残差分散が大きい。"
            ),
            section_id="pfe_ci",
        )

    # ── Section 12: Gini coefficients ────────────────────────────

    def _build_gini_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        gini_values = {
            "Person FE": _gini(a["pfe"] - a["pfe"].min()),
            "BiRank": _gini(a["br"]),
            "Patronage": _gini(a["pat"]),
            "AWCC": _gini(a["awcc"]),
            "NDI": _gini(a["ndi"]),
            "Studio Exp": _gini(a["st_exp"] - a["st_exp"].min()),
            "IV Score": _gini(a["iv"] - a["iv"].min()),
        }
        sorted_gini = sorted(gini_values.items(), key=lambda x: x[1])

        findings = (
            f"<p>7つのスコアコンポーネントのGini係数（n={a['n']:,}）。 "
            f"1.0に近いほど集中度が高い"
            f"（少数の人物が大きなシェアを占める）:</p><ul>"
        )
        for name, g in sorted_gini:
            findings += f"<li><strong>{name}</strong>: Gini={g:.3f}。</li>"
        findings += "</ul>"

        fig = go.Figure(go.Bar(
            y=[k for k, _ in sorted_gini],
            x=[v for _, v in sorted_gini],
            orientation="h",
            marker_color=[
                "#06D6A0" if v < 0.4
                else "#FFD166" if v < 0.6
                else "#f5576c"
                for _, v in sorted_gini
            ],
            text=[f"{v:.3f}" for _, v in sorted_gini],
            textposition="outside",
        ))
        fig.update_layout(
            xaxis_title="Gini係数",
            xaxis_range=[0, 1],
            height=350,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="コンポーネント別不平等度（Gini係数）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "gini_bar", height=350),
            method_note=(
                "Gini係数は絶対値に対して計算、負値を取り得るコンポーネント（Person FE、"
                "Studio Exp, IV Score）は最小値を0にシフトして算出。"
                "緑: Gini < 0.4（平等度中程度）、黄: 0.4-0.6、赤: > 0.6（高集中）。"
                "冪乗分布のコンポーネント（BiRank, Patronage）は Gini が高くなると想定される。"
            ),
            section_id="gini",
        )

    # ── Section 13: Partial R-squared ────────────────────────────

    def _build_partial_r2_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        from sklearn.linear_model import LinearRegression

        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]
        iv = a["iv"]

        x_layers = np.column_stack([causal_agg, structural_agg, collab_agg])
        lr_full = LinearRegression().fit(x_layers, iv)
        r2_full = lr_full.score(x_layers, iv)

        layer_names = ["因果層", "構造層", "協業層"]
        partial_r2: dict[str, float] = {}
        for drop_idx, name in enumerate(layer_names):
            x_reduced = np.delete(x_layers, drop_idx, axis=1)
            r2_reduced = LinearRegression().fit(x_reduced, iv).score(
                x_reduced, iv
            )
            partial_r2[name] = r2_full - r2_reduced

        findings = (
            f"<p>3層パーセンタイルによるIV Scoreの線形回帰"
            f"（n={a['n']:,}）: フルモデルR²={r2_full:.4f}。</p>"
            f"<p>部分R²（drop-one法）:</p><ul>"
        )
        for name in layer_names:
            findings += (
                f"<li><strong>{name}</strong>: "
                f"部分R²={partial_r2[name]:.4f}。</li>"
            )
        r2_sum = sum(partial_r2.values())
        findings += (
            f"</ul><p>部分R²合計: {r2_sum:.4f}。 "
            f"フルR²（{r2_full:.4f}）との差: "
            f"{r2_full - r2_sum:.4f}（共有分散）。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=list(partial_r2.keys()),
            y=list(partial_r2.values()),
            marker_color=[
                _LAYER_COLORS["causal"],
                _LAYER_COLORS["structural"],
                _LAYER_COLORS["collab"],
            ],
            text=[f"{v:.3f}" for v in partial_r2.values()],
            textposition="outside",
        ))
        fig.add_annotation(
            x=0.95, y=0.95, xref="paper", yref="paper",
            text=f"フルR²={r2_full:.4f}",
            showarrow=False,
            font=dict(size=13, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
        )
        fig.update_layout(yaxis_title="Partial R²", height=400)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="3層がIV Scoreを説明する割合",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "partial_r2", height=400,
            ),
            method_note=(
                "IV Score を3層パーセンタイル集約値で OLS 回帰。"
                "部分R² = R²_フル - R²_層除外（drop-one法）。"
                "部分R²の合計がフルR²に近い場合、各層は独立に寄与している。"
                "合計 < フルR² の場合、共有分散が存在する。"
                "これは記述的分解であり、因果的主張ではない。"
            ),
            section_id="partial_r2",
        )

    # ── Section 14: Dormancy impact ──────────────────────────────

    def _build_dormancy_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        iv = a["iv"]
        dorm = a["dorm"]
        n = a["n"]

        dorm_impact = []
        for i in range(n):
            d = float(dorm[i])
            iv_val = float(iv[i])
            if d > 0 and d < 1.0 and iv_val != 0:
                pre_iv = iv_val / d
                impact = iv_val - pre_iv
                name = a["names"][i]
                dorm_impact.append({
                    "name": name,
                    "dormancy": d,
                    "pre_iv": pre_iv,
                    "post_iv": iv_val,
                    "impact": impact,
                })
        dorm_impact.sort(key=lambda x: x["impact"])

        if len(dorm_impact) < 10:
            return ReportSection(
                title="Dormancy の影響",
                findings_html=(
                    f"<p>Dormancy影響データ不足: "
                    f"D &lt; 1.0の人物が{len(dorm_impact)}人"
                    f"（最低10人必要）。</p>"
                ),
                section_id="dormancy_impact",
            )

        top_penalty = dorm_impact[:5]
        low_penalty = dorm_impact[-5:]
        waterfall_persons = top_penalty + low_penalty

        # Dormancy distribution for findings
        all_dorm = [di["dormancy"] for di in dorm_impact]
        dorm_summ = distribution_summary(all_dorm, label="dormancy_affected")
        abs_impacts = [abs(di["impact"]) for di in dorm_impact]
        impact_summ = distribution_summary(abs_impacts, label="abs_impact")

        findings = (
            f"<p>Dormancy乗数（D_i）分析。 "
            f"D &lt; 1.0の人物: 全{n:,}人中{len(dorm_impact):,}人。</p>"
            f"<p>影響を受けた人物のDormancy乗数: "
            f"{format_distribution_inline(dorm_summ)}。</p>"
            f"<p>IV Scoreへの絶対影響量: "
            f"{format_distribution_inline(impact_summ)}, "
            f"{format_ci((impact_summ['ci_lower'], impact_summ['ci_upper']))}。"
            f"</p>"
        )

        wf_names = [p["name"][:15] for p in waterfall_persons]
        wf_impact = [p["impact"] for p in waterfall_persons]
        wf_dormancy = [p["dormancy"] for p in waterfall_persons]

        bar_colors = [
            "#f5576c" if imp < 0 else "#06D6A0" for imp in wf_impact
        ]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=wf_names, y=wf_impact, name="Dormancyの影響",
            marker_color=bar_colors,
            text=[
                f"D={d:.2f}<br>Δ={imp:+.3f}"
                for d, imp in zip(wf_dormancy, wf_impact)
            ],
            textposition="outside", textfont=dict(size=9),
            hovertemplate=(
                "<b>%{x}</b><br>Δ IV: %{y:+.3f}<extra></extra>"
            ),
        ))
        fig.add_hline(
            y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)",
        )
        fig.update_layout(
            yaxis_title="Δ IV Score（Dormancy適用前後）",
            xaxis_tickangle=-30, height=500,
            showlegend=False,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="Dormancy の影響",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "dormancy_waterfall", height=500,
            ),
            method_note=(
                "Dormancy 乗数 D_i は5つのIVコンポーネントの加重合計に乗算的に適用される。"
                "D=1.0 はペナルティなし、D<1.0 は活動ギャップペナルティを示す。"
                "Dormancy適用前IVは iv_score / D として推定。影響量 = iv_score - 適用前IV。"
                "最もペナルティの大きい5名と最も小さい5名を表示。"
            ),
            section_id="dormancy_impact",
        )

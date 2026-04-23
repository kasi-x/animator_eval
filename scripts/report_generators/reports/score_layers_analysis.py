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
from ._base import BaseReportGenerator, append_validation_warnings

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

    # ── Section 1 helpers ─────────────────────────────────────────

    def _compute_layer_correlations(
        self,
        causal_agg: np.ndarray,
        structural_agg: np.ndarray,
        collab_agg: np.ndarray,
    ) -> tuple[float, float, float]:
        r_cs, _ = sp_stats.pearsonr(causal_agg, structural_agg)
        r_cc, _ = sp_stats.pearsonr(causal_agg, collab_agg)
        r_sc, _ = sp_stats.pearsonr(structural_agg, collab_agg)
        return float(r_cs), float(r_cc), float(r_sc)

    def _compute_top10_overlaps(
        self,
        causal_agg: np.ndarray,
        structural_agg: np.ndarray,
        collab_agg: np.ndarray,
        n: int,
    ) -> tuple[float, float, float]:
        top10_n = max(n // 10, 1)
        top10_causal = set(np.argsort(causal_agg)[-top10_n:])
        top10_struct = set(np.argsort(structural_agg)[-top10_n:])
        top10_collab = set(np.argsort(collab_agg)[-top10_n:])
        overlap_cs = len(top10_causal & top10_struct) / top10_n * 100
        overlap_cc = len(top10_causal & top10_collab) / top10_n * 100
        overlap_sc = len(top10_struct & top10_collab) / top10_n * 100
        return overlap_cs, overlap_cc, overlap_sc

    def _findings_summary_distributions(
        self,
        n: int,
        weight_method: str,
        var_expl: float,
        c_summ: dict,
        s_summ: dict,
        co_summ: dict,
        r_cs: float,
        r_cc: float,
        r_sc: float,
        overlap_cs: float,
        overlap_cc: float,
        overlap_sc: float,
    ) -> str:
        return (
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

    def _findings_summary_iv_weights(self, lw: dict) -> str:
        if not lw:
            return ""
        weight_str = " / ".join(
            f"{k}={v:.1%}"
            for k, v in sorted(lw.items(), key=lambda x: -x[1])
        )
        return f"<p>IVウェイト: {weight_str}。</p>"

    def _make_layer_violin_figure(
        self,
        causal_agg: np.ndarray,
        structural_agg: np.ndarray,
        collab_agg: np.ndarray,
        n: int,
    ) -> go.Figure:
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
        return fig

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

        r_cs, r_cc, r_sc = self._compute_layer_correlations(
            causal_agg, structural_agg, collab_agg
        )
        overlap_cs, overlap_cc, overlap_sc = self._compute_top10_overlaps(
            causal_agg, structural_agg, collab_agg, n
        )

        lw = iv_data.get("lambda_weights", {})
        weight_method = iv_data.get("weight_method", "fixed_prior")
        var_expl = iv_data.get("pca_variance_explained", 0)

        c_summ = distribution_summary(causal_agg.tolist(), label="causal_pct")
        s_summ = distribution_summary(structural_agg.tolist(), label="structural_pct")
        co_summ = distribution_summary(collab_agg.tolist(), label="collab_pct")

        findings = self._findings_summary_distributions(
            n, weight_method, var_expl,
            c_summ, s_summ, co_summ,
            r_cs, r_cc, r_sc,
            overlap_cs, overlap_cc, overlap_sc,
        )
        findings += self._findings_summary_iv_weights(lw)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="3層スコア分布比較（パーセンタイル空間）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_layer_violin_figure(causal_agg, structural_agg, collab_agg, n),
                "layer_violin", height=450,
            ),
            method_note=(
                "因果層 = Person FE パーセンタイル順位。"
                "構造層 = BiRank, AWCC, NDI パーセンタイル順位の平均。"
                "協業層 = Patronage, Studio FE Exposure パーセンタイル順位の平均。"
                "Pearson 相関は全体集団で計算。"
                "バイオリンは描画のため最大5,000名にサブサンプリング。"
            ),
            section_id="summary_violin",
        )

    # ── Section 2 helpers ─────────────────────────────────────────

    _COMP_NAMES = [
        "Person FE", "BiRank", "Patronage", "AWCC",
        "NDI", "Studio Exp", "IV Score", "Dormancy",
    ]
    _COMP_ARRAY_KEYS = ["pfe", "br", "pat", "awcc", "ndi", "st_exp", "iv", "dorm"]
    _COMP_HIST_COLORS = [
        "#f093fb", "#06D6A0", "#667eea", "#FFD166",
        "#a0d2db", "#f5576c", "#fda085", "#c0c0d0",
    ]

    def _findings_component_distributions(
        self, n: int, summaries: list[dict]
    ) -> str:
        out = f"<p>8コンポーネントの生値分布（n={n:,}）:</p><ul>"
        for s in summaries:
            out += (
                f"<li><strong>{s['label']}</strong>: "
                f"{format_distribution_inline(s)}。</li>"
            )
        out += "</ul>"
        return out

    def _make_component_histograms_figure(
        self,
        comp_arrays: list[np.ndarray],
    ) -> go.Figure:
        fig = make_subplots(
            rows=2, cols=4,
            subplot_titles=self._COMP_NAMES,
            horizontal_spacing=0.06, vertical_spacing=0.12,
        )
        for idx, (arr, color) in enumerate(zip(comp_arrays, self._COMP_HIST_COLORS)):
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
            fig.add_vline(
                x=float(np.median(arr)), line_dash="solid", line_color="#ff4444",
                line_width=1.5, row=r + 1, col=c + 1,
            )
            fig.add_vline(
                x=float(np.mean(arr)), line_dash="dot", line_color="#4488ff",
                line_width=1, row=r + 1, col=c + 1,
            )
        # Log x-axis for power-law distributed components
        fig.update_xaxes(type="log", row=1, col=2)  # BiRank
        fig.update_xaxes(type="log", row=1, col=3)  # Patronage
        fig.update_xaxes(type="log", row=1, col=4)  # AWCC
        fig.update_layout(height=550, margin=dict(t=60, b=40))
        return fig

    # ── Section 2: Component histogram grid ──────────────────────

    def _build_component_histograms_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        n = a["n"]
        comp_arrays = [a[k] for k in self._COMP_ARRAY_KEYS]

        summaries = [
            distribution_summary(arr.tolist(), label=cname)
            for cname, arr in zip(self._COMP_NAMES, comp_arrays)
        ]

        findings = self._findings_component_distributions(n, summaries)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="全コンポーネント分布（生値）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_component_histograms_figure(comp_arrays),
                "component_histograms", height=550,
            ),
            method_note=(
                "赤の実線 = 中央値、青の点線 = 平均。"
                "1–99パーセンタイル範囲外の外れ値は可視化のためクリップ。"
                "BiRank, Patronage, AWCC は冪乗分布の形状により対数x軸を使用。"
            ),
            section_id="component_histograms",
        )

    # ── Section 3 helpers ─────────────────────────────────────────

    _CORR_COMP_NAMES = [
        "person_fe", "birank", "patronage",
        "awcc", "ndi", "studio_exp", "iv_score",
    ]
    _CORR_ARRAY_KEYS = ["pfe", "br", "pat", "awcc", "ndi", "st_exp", "iv"]

    def _compute_correlation_matrix(
        self, comp_arrays: list[np.ndarray]
    ) -> np.ndarray:
        k = len(comp_arrays)
        corr_matrix = np.zeros((k, k))
        for i in range(k):
            for j in range(k):
                corr_matrix[i, j] = np.corrcoef(comp_arrays[i], comp_arrays[j])[0, 1]
        return corr_matrix

    def _find_off_diagonal_extremes(
        self, corr_matrix: np.ndarray, comp_names: list[str]
    ) -> tuple[tuple[str, str], float, tuple[str, str], float]:
        """Return (max_pair, max_val, min_pair, min_val) for off-diagonal entries."""
        off_diag = corr_matrix.copy()
        np.fill_diagonal(off_diag, 0)
        max_idx = np.unravel_index(np.argmax(np.abs(off_diag)), off_diag.shape)
        min_idx = np.unravel_index(np.argmin(off_diag), off_diag.shape)
        return (
            (comp_names[max_idx[0]], comp_names[max_idx[1]]),
            float(off_diag[max_idx]),
            (comp_names[min_idx[0]], comp_names[min_idx[1]]),
            float(off_diag[min_idx]),
        )

    def _findings_correlation_overview(
        self,
        n: int,
        max_pair: tuple[str, str],
        max_val: float,
        min_pair: tuple[str, str],
        min_val: float,
    ) -> str:
        return (
            f"<p>7つのスコアコンポーネント間のPearson相関行列"
            f"（n={n:,}）。</p>"
            f"<p>対角線外の最大絶対相関: "
            f"r({max_pair[0]}, {max_pair[1]})={max_val:.3f}。 "
            f"対角線外の最小相関: "
            f"r({min_pair[0]}, {min_pair[1]})={min_val:.3f}。</p>"
        )

    def _make_correlation_heatmap_figure(
        self, corr_matrix: np.ndarray, comp_names: list[str]
    ) -> go.Figure:
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
        return fig

    # ── Section 3: Correlation heatmap ───────────────────────────

    def _build_correlation_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        comp_arrays = [a[k] for k in self._CORR_ARRAY_KEYS]
        corr_matrix = self._compute_correlation_matrix(comp_arrays)
        max_pair, max_val, min_pair, min_val = self._find_off_diagonal_extremes(
            corr_matrix, self._CORR_COMP_NAMES
        )

        findings = self._findings_correlation_overview(
            a["n"], max_pair, max_val, min_pair, min_val
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="コンポーネント間相関行列",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_correlation_heatmap_figure(corr_matrix, self._CORR_COMP_NAMES),
                "corr_heatmap", height=500,
            ),
            method_note=(
                "生値（パーセンタイル変換前）によるPearson相関。"
                "1.0に近い値は情報の冗長性を示す。0に近い値は独立した視点を示す。"
                "負の値はトレードオフ関係を示す。"
            ),
            section_id="correlation_matrix",
        )

    # ── Section 4 helpers ─────────────────────────────────────────

    _IV_WEIGHT_COLORS = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#f5576c"]

    def _compute_iv_weight_method_label(
        self, weight_method: str, var_expl: float
    ) -> str:
        if weight_method == "PCA_PC1":
            return f"PCA PC1 (variance explained: {var_expl:.1%})"
        return "fixed prior"

    def _findings_iv_weights(
        self,
        lw: dict,
        sorted_lw: list[tuple[str, float]],
        method_label: str,
    ) -> str:
        max_comp = sorted_lw[-1]
        min_comp = sorted_lw[0]
        return (
            f"<p>IV Scoreウェイト導出手法: {method_label}。 "
            f"主要コンポーネント数: {len(lw)}。</p>"
            f"<p>最大ウェイト: {max_comp[0]}={max_comp[1]:.3f}。 "
            f"最小ウェイト: {min_comp[0]}={min_comp[1]:.3f}。 "
            f"ウェイト合計: {sum(lw.values()):.3f}。</p>"
        )

    def _make_iv_weights_figure(
        self, sorted_lw: list[tuple[str, float]]
    ) -> go.Figure:
        fig = go.Figure(go.Bar(
            y=[k for k, _ in sorted_lw],
            x=[v for _, v in sorted_lw],
            orientation="h",
            marker_color=self._IV_WEIGHT_COLORS[:len(sorted_lw)],
            text=[f"{v:.3f}" for _, v in sorted_lw],
            textposition="outside",
        ))
        fig.update_layout(xaxis_title="ウェイト (λ)", height=350)
        return fig

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
        method_label = self._compute_iv_weight_method_label(weight_method, var_expl)
        findings = self._findings_iv_weights(lw, sorted_lw, method_label)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="IV Score 構成ウェイト",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_iv_weights_figure(sorted_lw), "iv_weights_bar", height=350,
            ),
            method_note=(
                f"ウェイトは {method_label} により導出。"
                "棒グラフは統合IV Scoreへの各コンポーネントの寄与を示す。"
                "ウェイトは合計1.0に正規化。"
                "Dormancy乗数は加重合計後に適用。"
            ),
            section_id="iv_weights",
        )

    # ── Section 5 helpers ─────────────────────────────────────────

    def _findings_density_scatter(
        self, n: int, r_cs: float, r_cc: float
    ) -> str:
        return (
            f"<p>層パーセンタイルの密度等高線プロット（n={n:,}）:</p><ul>"
            f"<li>因果層 vs 構造層（r={r_cs:.3f}）: "
            f"対角線からの乖離は個人貢献（AKM）とネットワーク位置の"
            f"不一致を示す。</li>"
            f"<li>因果層 vs 協業層（r={r_cc:.3f}）: "
            f"対角線からの乖離は個人貢献と協業環境の"
            f"不一致を示す。</li>"
            f"</ul>"
        )

    def _make_density_scatter_figure(
        self,
        x_vals: list[float],
        y_vals: list[float],
        xlabel: str,
        ylabel: str,
        title: str,
        names: list[str],
    ) -> go.Figure:
        fig = density_scatter_2d(
            x_vals, y_vals,
            xlabel=xlabel, ylabel=ylabel, title=title,
            label_names=names, label_top=10, height=500,
        )
        fig.add_shape(
            type="line", x0=0, y0=0, x1=100, y1=100,
            line=dict(color="rgba(255,255,255,0.3)", dash="dash"),
        )
        return fig

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

        r_cs = float(np.corrcoef(causal_agg, structural_agg)[0, 1])
        r_cc = float(np.corrcoef(causal_agg, collab_agg)[0, 1])

        findings = self._findings_density_scatter(n, r_cs, r_cc)
        findings = append_validation_warnings(findings, sb)

        fig5a = self._make_density_scatter_figure(
            causal_agg.tolist(), structural_agg.tolist(),
            "因果層パーセンタイル", "構造層パーセンタイル", "因果層 vs 構造層", names,
        )
        fig5b = self._make_density_scatter_figure(
            causal_agg.tolist(), collab_agg.tolist(),
            "因果層パーセンタイル", "協業層パーセンタイル", "因果層 vs 協業層", names,
        )
        viz_html = (
            plotly_div_safe(fig5a, "layer_density_cs", height=500)
            + plotly_div_safe(fig5b, "layer_density_cc", height=500)
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

    # ── Section 6 helpers ─────────────────────────────────────────

    def _findings_gap_distribution(
        self,
        n: int,
        cs_summ: dict,
        cc_summ: dict,
    ) -> str:
        return (
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

    def _make_gap_histogram_figure(
        self, gap_cs: np.ndarray, gap_cc: np.ndarray
    ) -> go.Figure:
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
        return fig

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

        findings = self._findings_gap_distribution(n, cs_summ, cc_summ)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="層間ギャップスコア分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_gap_histogram_figure(gap_cs, gap_cc), "gap_histogram", height=400,
            ),
            method_note=(
                "ギャップ = 因果層パーセンタイル − 他層パーセンタイル。"
                "正の値: 当該人物は構造/協業層より因果層（AKM）で高順位。"
                "負の値: その逆。"
                "0中心の分布は全体的な整合を示す。"
            ),
            section_id="gap_distribution",
        )

    # ── Section 7 helpers ─────────────────────────────────────────

    _PARCOORDS_DIM_KEYS = [
        ("Person FE", "pfe_pct"),
        ("BiRank", "br_pct"),
        ("Patronage", "pat_pct"),
        ("AWCC", "awcc_pct"),
        ("NDI", "ndi_pct"),
        ("Studio Exp", "st_pct"),
    ]

    def _findings_parallel_coords(
        self, n: int, top50_iv_min: float, top50_iv_max: float
    ) -> str:
        return (
            f"<p>IV Score上位50人のパラレル座標プロット"
            f"（全{n:,}人中50人）。各軸は6コンポーネントのパーセンタイル順位（0-100）を示す。"
            f"線の色はIV Score"
            f"（範囲: {top50_iv_min:.3f}〜{top50_iv_max:.3f}）を表す。</p>"
        )

    def _make_parallel_coords_figure(
        self, a: dict[str, np.ndarray], top50_idx: np.ndarray
    ) -> go.Figure:
        dims = [
            dict(label=label, values=a[key][top50_idx], range=[0, 100])
            for label, key in self._PARCOORDS_DIM_KEYS
        ]
        fig = go.Figure(go.Parcoords(
            line=dict(
                color=a["iv"][top50_idx],
                colorscale="Plasma",
                showscale=True,
                colorbar=dict(title="IV Score"),
            ),
            dimensions=dims,
        ))
        fig.update_layout(height=500)
        return fig

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

        top50_iv_min = float(iv[top50_idx].min())
        top50_iv_max = float(iv[top50_idx].max())

        findings = self._findings_parallel_coords(n, top50_iv_min, top50_iv_max)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="上位50人のパラレル座標プロット",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_parallel_coords_figure(a, top50_idx), "parallel_coords", height=500,
            ),
            method_note=(
                "IV Scoreにより上位50人を選抜。各縦軸はパーセンタイル順位（0-100）。"
                "全軸で高位置を通る線は、一様に高順位の人物を示す。"
                "特定軸での大きな落ち込みは、コンポーネントレベルの弱点を示す。"
            ),
            section_id="parallel_coords",
        )

    # ── Section 8 helpers ─────────────────────────────────────────

    def _findings_pfe_birank_patronage(
        self, n: int, r_pb: float, r_pp: float
    ) -> str:
        return (
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

    def _make_pfe_birank_figure(
        self,
        pfe: list[float],
        br: list[float],
        names: list[str],
        r_pb: float,
        n: int,
    ) -> go.Figure:
        fig = density_scatter_2d(
            pfe, br,
            xlabel="Person FE (θ)", ylabel="BiRank",
            title="Person FE vs BiRank",
            label_names=names, label_top=12, height=500,
        )
        fig.add_annotation(
            x=0.02, y=0.98, xref="paper", yref="paper",
            text=f"r={r_pb:.3f}, n={n:,}",
            showarrow=False, font=dict(size=11, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
            bordercolor="#FFD166", borderwidth=1, borderpad=4,
        )
        return fig

    def _make_pfe_patronage_figure(
        self,
        pfe: list[float],
        pat: list[float],
        names: list[str],
        r_pp: float,
        n: int,
    ) -> go.Figure:
        fig = density_scatter_2d(
            pfe, pat,
            xlabel="Person FE (θ)", ylabel="Patronageプレミアム",
            title="Person FE vs Patronage",
            label_names=names, label_top=10, height=500,
        )
        fig.add_annotation(
            x=0.02, y=0.98, xref="paper", yref="paper",
            text=f"r={r_pp:.3f}, n={n:,}",
            showarrow=False, font=dict(size=11, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
            bordercolor="#FFD166", borderwidth=1, borderpad=4,
        )
        return fig

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

        findings = self._findings_pfe_birank_patronage(n, r_pb, r_pp)
        findings = append_validation_warnings(findings, sb)

        viz_html = (
            plotly_div_safe(
                self._make_pfe_birank_figure(pfe.tolist(), br.tolist(), names, r_pb, n),
                "pfe_vs_birank", height=500,
            )
            + plotly_div_safe(
                self._make_pfe_patronage_figure(pfe.tolist(), pat.tolist(), names, r_pp, n),
                "pfe_vs_patronage", height=500,
            )
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

    # ── Section 9 helpers ─────────────────────────────────────────

    _RADAR_CATEGORIES = ["Person FE", "BiRank", "Patronage", "AWCC", "Studio Exp"]
    _RADAR_ROLE_KEYS = ["pfe", "br", "pat", "awcc", "st"]

    def _compute_role_map(
        self, data: list[dict], a: dict[str, np.ndarray]
    ) -> dict[str, dict[str, list[float]]]:
        role_map: dict[str, dict[str, list[float]]] = {}
        for i, d in enumerate(data):
            role = d.get("primary_role") or "?"
            if role == "?":
                continue
            if role not in role_map:
                role_map[role] = {k: [] for k in self._RADAR_ROLE_KEYS}
            role_map[role]["pfe"].append(float(a["pfe_pct"][i]))
            role_map[role]["br"].append(float(a["br_pct"][i]))
            role_map[role]["pat"].append(float(a["pat_pct"][i]))
            role_map[role]["awcc"].append(float(a["awcc_pct"][i]))
            role_map[role]["st"].append(float(a["st_pct"][i]))
        return role_map

    def _role_median_vals(self, rm: dict[str, list[float]]) -> list[float]:
        return [float(np.median(rm[k])) for k in self._RADAR_ROLE_KEYS]

    def _findings_role_radar(
        self,
        role_map: dict[str, dict[str, list[float]]],
        active_roles: list[str],
    ) -> str:
        parts = [
            "<p>5コンポーネントにおける役職別中央値パーセンタイル"
            "（n >= 10の役職のみ）:</p><ul>"
        ]
        for role in active_roles:
            rm = role_map[role]
            n_role = len(rm["pfe"])
            meds = self._role_median_vals(rm)
            parts.append(
                f"<li><strong>{role}</strong> (n={n_role}): "
                + ", ".join(
                    f"{c}={m:.0f}" for c, m in zip(self._RADAR_CATEGORIES, meds)
                )
                + ".</li>"
            )
        parts.append("</ul>")
        return "".join(parts)

    def _make_role_radar_figure(
        self, role_map: dict[str, dict[str, list[float]]]
    ) -> go.Figure:
        fig = go.Figure()
        cats = self._RADAR_CATEGORIES
        for role, color in _RADAR_ROLES:
            if role not in role_map or len(role_map[role]["pfe"]) < 10:
                continue
            vals = self._role_median_vals(role_map[role])
            fig.add_trace(go.Scatterpolar(
                r=vals + [vals[0]],
                theta=cats + [cats[0]],
                name=role, line_color=color,
                fill="toself", opacity=0.3,
            ))
        fig.update_layout(
            polar=dict(radialaxis=dict(range=[0, 100], showticklabels=True)),
            height=500,
        )
        return fig

    # ── Section 9: Role radar ────────────────────────────────────

    def _build_role_radar_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        role_map = self._compute_role_map(data, a)
        active_roles = [
            role for role, _ in _RADAR_ROLES
            if role in role_map and len(role_map[role]["pfe"]) >= 10
        ]

        findings = self._findings_role_radar(role_map, active_roles)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="役職別の3層プロファイル比較",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_role_radar_figure(role_map), "role_radar", height=500,
            ),
            method_note=(
                "レーダーチャートは5コンポーネントにわたる役職別中央値パーセンタイルを示す。"
                "10人未満の役職は除外。primary_role は feat_career より。"
                "役職間の形状差は、各役職がネットワーク位置と個人貢献をどのように蓄積するかの"
                "構造的差異を反映している。"
            ),
            section_id="role_radar",
        )

    # ── Section 10 helpers ────────────────────────────────────────

    _STAGE_LABELS = [
        "0:新人", "1:ジュニア", "2:中堅", "3:熟練",
        "4:ベテラン", "5:マスター", "6:レジェンド",
    ]

    def _compute_stage_data(
        self,
        data: list[dict],
        causal_agg: np.ndarray,
        structural_agg: np.ndarray,
        collab_agg: np.ndarray,
    ) -> dict[int, dict[str, list[float]]]:
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
        return stage_data

    def _findings_career_stage(
        self, stage_data: dict[int, dict[str, list[float]]]
    ) -> str:
        parts = [
            "<p>キャリアステージ別の3層パーセンタイル分布"
            "（highest_stage 0-6）:</p><ul>"
        ]
        for st in range(7):
            sd = stage_data[st]
            n_st = len(sd["causal"])
            if n_st == 0:
                continue
            parts.append(
                f"<li><strong>{self._STAGE_LABELS[st]}</strong> (n={n_st}): "
                f"causal median={np.median(sd['causal']):.0f}, "
                f"structural median={np.median(sd['struct']):.0f}, "
                f"collab median={np.median(sd['collab']):.0f}.</li>"
            )
        parts.append("</ul>")
        return "".join(parts)

    def _make_stage_boxplot_figure(
        self, stage_data: dict[int, dict[str, list[float]]]
    ) -> go.Figure:
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
                    x_vals.extend([self._STAGE_LABELS[st]] * len(arr))
                    y_vals.extend(arr)
            fig.add_trace(go.Box(
                x=x_vals, y=y_vals, name=layer_name,
                marker_color=color, opacity=0.7,
                boxmean=True,
            ))
        fig.update_layout(boxmode="group", yaxis_title="パーセンタイル", height=500)
        return fig

    # ── Section 10: Career stage box plot ────────────────────────

    def _build_career_stage_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        stage_data = self._compute_stage_data(
            data, a["causal_agg"], a["structural_agg"], a["collab_agg"]
        )

        findings = self._findings_career_stage(stage_data)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="キャリアステージ別の3層スコア推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_stage_boxplot_figure(stage_data), "stage_boxplot", height=500,
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

    # ── Section 11 helpers ────────────────────────────────────────

    def _fetch_pfe_ci_rows(self) -> list[dict]:
        """Fetch top-30 persons by person_fe with SE > 0 from feat_person_scores."""
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
        return [
            {
                "name": r["name_ja"] or r["name_zh"] or r["name_en"] or r["person_id"],
                "pfe": r["person_fe"],
                "se": r["person_fe_se"],
                "n_obs": r["person_fe_n_obs"] or 0,
                "lower": r["person_fe"] - 1.96 * r["person_fe_se"],
                "upper": r["person_fe"] + 1.96 * r["person_fe_se"],
            }
            for r in rows
        ]

    def _findings_pfe_ci(self, ci_persons: list[dict]) -> str:
        se_vals = [c["se"] for c in ci_persons]
        se_summ = distribution_summary(se_vals, label="person_fe_se_top30")
        avg_width = float(np.mean([c["upper"] - c["lower"] for c in ci_persons]))
        return (
            f"<p>Person FE（theta_i）上位{len(ci_persons)}人の"
            f"95%信頼区間。"
            f"SE = sigma_resid / sqrt(n_obs)。</p>"
            f"<p>SE分布（上位30人）: "
            f"{format_distribution_inline(se_summ)}。 "
            f"平均CI幅: {avg_width:.3f}。</p>"
        )

    def _make_pfe_ci_forest_figure(
        self, ci_persons: list[dict], h: int
    ) -> go.Figure:
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
        return fig

    # ── Section 11: Person FE CI forest plot ─────────────────────

    def _build_pfe_ci_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        ci_persons = self._fetch_pfe_ci_rows()

        if not ci_persons:
            return ReportSection(
                title="Person FE 信頼区間（上位30人）",
                findings_html=(
                    "<p>feat_person_scoresにPerson FE SEデータがありません。"
                    "person_fe_seを計算するにはパイプラインを再実行してください。</p>"
                ),
                section_id="pfe_ci",
            )

        findings = self._findings_pfe_ci(ci_persons)
        findings = append_validation_warnings(findings, sb)

        h = adaptive_height(len(ci_persons))
        return ReportSection(
            title="Person FE 信頼区間（上位30人）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_pfe_ci_forest_figure(ci_persons, h), "pfe_ci_forest", height=h,
            ),
            method_note=(
                "person_fe_se は feat_person_scores より: SE = σ_resid / sqrt(n_obs)、"
                "n_obs は AKM 推定に使用されたクレジット作品数。"
                "95% CI = θ ± 1.96 × SE（正規近似）。"
                "狭いCI = 推定が安定。広いCI = 観測が少ないか残差分散が大きい。"
            ),
            section_id="pfe_ci",
        )

    # ── Section 12 helpers ────────────────────────────────────────

    def _compute_gini_values(
        self, a: dict[str, np.ndarray]
    ) -> list[tuple[str, float]]:
        raw = {
            "Person FE": _gini(a["pfe"] - a["pfe"].min()),
            "BiRank": _gini(a["br"]),
            "Patronage": _gini(a["pat"]),
            "AWCC": _gini(a["awcc"]),
            "NDI": _gini(a["ndi"]),
            "Studio Exp": _gini(a["st_exp"] - a["st_exp"].min()),
            "IV Score": _gini(a["iv"] - a["iv"].min()),
        }
        return sorted(raw.items(), key=lambda x: x[1])

    def _findings_gini(
        self, n: int, sorted_gini: list[tuple[str, float]]
    ) -> str:
        out = (
            f"<p>7つのスコアコンポーネントのGini係数（n={n:,}）。 "
            f"1.0に近いほど集中度が高い"
            f"（少数の人物が大きなシェアを占める）:</p><ul>"
        )
        for name, g in sorted_gini:
            out += f"<li><strong>{name}</strong>: Gini={g:.3f}。</li>"
        out += "</ul>"
        return out

    def _make_gini_figure(
        self, sorted_gini: list[tuple[str, float]]
    ) -> go.Figure:
        fig = go.Figure(go.Bar(
            y=[k for k, _ in sorted_gini],
            x=[v for _, v in sorted_gini],
            orientation="h",
            marker_color=[
                "#06D6A0" if v < 0.4 else "#FFD166" if v < 0.6 else "#f5576c"
                for _, v in sorted_gini
            ],
            text=[f"{v:.3f}" for _, v in sorted_gini],
            textposition="outside",
        ))
        fig.update_layout(xaxis_title="Gini係数", xaxis_range=[0, 1], height=350)
        return fig

    # ── Section 12: Gini coefficients ────────────────────────────

    def _build_gini_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        sorted_gini = self._compute_gini_values(a)
        findings = self._findings_gini(a["n"], sorted_gini)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="コンポーネント別不平等度（Gini係数）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_gini_figure(sorted_gini), "gini_bar", height=350,
            ),
            method_note=(
                "Gini係数は絶対値に対して計算、負値を取り得るコンポーネント（Person FE、"
                "Studio Exp, IV Score）は最小値を0にシフトして算出。"
                "緑: Gini < 0.4（平等度中程度）、黄: 0.4-0.6、赤: > 0.6（高集中）。"
                "冪乗分布のコンポーネント（BiRank, Patronage）は Gini が高くなると想定される。"
            ),
            section_id="gini",
        )

    # ── Section 13 helpers ────────────────────────────────────────

    _PARTIAL_R2_LAYER_NAMES = ["因果層", "構造層", "協業層"]
    _PARTIAL_R2_LAYER_COLOR_KEYS = ["causal", "structural", "collab"]

    def _compute_partial_r2(
        self, a: dict[str, np.ndarray]
    ) -> tuple[float, dict[str, float]]:
        from sklearn.linear_model import LinearRegression

        causal_agg = a["causal_agg"]
        structural_agg = a["structural_agg"]
        collab_agg = a["collab_agg"]
        iv = a["iv"]

        x_layers = np.column_stack([causal_agg, structural_agg, collab_agg])
        r2_full = LinearRegression().fit(x_layers, iv).score(x_layers, iv)
        partial_r2: dict[str, float] = {}
        for drop_idx, name in enumerate(self._PARTIAL_R2_LAYER_NAMES):
            x_reduced = np.delete(x_layers, drop_idx, axis=1)
            r2_reduced = LinearRegression().fit(x_reduced, iv).score(x_reduced, iv)
            partial_r2[name] = r2_full - r2_reduced
        return r2_full, partial_r2

    def _findings_partial_r2(
        self, n: int, r2_full: float, partial_r2: dict[str, float]
    ) -> str:
        out = (
            f"<p>3層パーセンタイルによるIV Scoreの線形回帰"
            f"（n={n:,}）: フルモデルR²={r2_full:.4f}。</p>"
            f"<p>部分R²（drop-one法）:</p><ul>"
        )
        for name in self._PARTIAL_R2_LAYER_NAMES:
            out += f"<li><strong>{name}</strong>: 部分R²={partial_r2[name]:.4f}。</li>"
        r2_sum = sum(partial_r2.values())
        out += (
            f"</ul><p>部分R²合計: {r2_sum:.4f}。 "
            f"フルR²（{r2_full:.4f}）との差: "
            f"{r2_full - r2_sum:.4f}（共有分散）。</p>"
        )
        return out

    def _make_partial_r2_figure(
        self, partial_r2: dict[str, float], r2_full: float
    ) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=list(partial_r2.keys()),
            y=list(partial_r2.values()),
            marker_color=[_LAYER_COLORS[k] for k in self._PARTIAL_R2_LAYER_COLOR_KEYS],
            text=[f"{v:.3f}" for v in partial_r2.values()],
            textposition="outside",
        ))
        fig.add_annotation(
            x=0.95, y=0.95, xref="paper", yref="paper",
            text=f"フルR²={r2_full:.4f}",
            showarrow=False, font=dict(size=13, color="#FFD166"),
            bgcolor="rgba(0,0,0,0.5)",
        )
        fig.update_layout(yaxis_title="Partial R²", height=400)
        return fig

    # ── Section 13: Partial R-squared ────────────────────────────

    def _build_partial_r2_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        r2_full, partial_r2 = self._compute_partial_r2(a)
        findings = self._findings_partial_r2(a["n"], r2_full, partial_r2)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="3層がIV Scoreを説明する割合",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_partial_r2_figure(partial_r2, r2_full), "partial_r2", height=400,
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

    # ── Section 14 helpers ────────────────────────────────────────

    def _compute_dormancy_impact(
        self, a: dict[str, np.ndarray]
    ) -> list[dict]:
        iv = a["iv"]
        dorm = a["dorm"]
        n = a["n"]
        impact_rows = []
        for i in range(n):
            d = float(dorm[i])
            iv_val = float(iv[i])
            if d > 0 and d < 1.0 and iv_val != 0:
                pre_iv = iv_val / d
                impact_rows.append({
                    "name": a["names"][i],
                    "dormancy": d,
                    "pre_iv": pre_iv,
                    "post_iv": iv_val,
                    "impact": iv_val - pre_iv,
                })
        impact_rows.sort(key=lambda x: x["impact"])
        return impact_rows

    def _findings_dormancy(
        self, n: int, dorm_impact: list[dict]
    ) -> str:
        all_dorm = [di["dormancy"] for di in dorm_impact]
        dorm_summ = distribution_summary(all_dorm, label="dormancy_affected")
        abs_impacts = [abs(di["impact"]) for di in dorm_impact]
        impact_summ = distribution_summary(abs_impacts, label="abs_impact")
        return (
            f"<p>Dormancy乗数（D_i）分析。 "
            f"D &lt; 1.0の人物: 全{n:,}人中{len(dorm_impact):,}人。</p>"
            f"<p>影響を受けた人物のDormancy乗数: "
            f"{format_distribution_inline(dorm_summ)}。</p>"
            f"<p>IV Scoreへの絶対影響量: "
            f"{format_distribution_inline(impact_summ)}, "
            f"{format_ci((impact_summ['ci_lower'], impact_summ['ci_upper']))}。"
            f"</p>"
        )

    def _make_dormancy_waterfall_figure(
        self, waterfall_persons: list[dict]
    ) -> go.Figure:
        wf_names = [p["name"][:15] for p in waterfall_persons]
        wf_impact = [p["impact"] for p in waterfall_persons]
        wf_dormancy = [p["dormancy"] for p in waterfall_persons]
        bar_colors = ["#f5576c" if imp < 0 else "#06D6A0" for imp in wf_impact]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=wf_names, y=wf_impact, name="Dormancyの影響",
            marker_color=bar_colors,
            text=[
                f"D={d:.2f}<br>Δ={imp:+.3f}"
                for d, imp in zip(wf_dormancy, wf_impact)
            ],
            textposition="outside", textfont=dict(size=9),
            hovertemplate="<b>%{x}</b><br>Δ IV: %{y:+.3f}<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
        fig.update_layout(
            yaxis_title="Δ IV Score（Dormancy適用前後）",
            xaxis_tickangle=-30, height=500,
            showlegend=False,
        )
        return fig

    # ── Section 14: Dormancy impact ──────────────────────────────

    def _build_dormancy_section(
        self,
        sb: SectionBuilder,
        data: list[dict],
        a: dict[str, np.ndarray],
        iv_data: dict,
    ) -> ReportSection:
        dorm_impact = self._compute_dormancy_impact(a)

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

        waterfall_persons = dorm_impact[:5] + dorm_impact[-5:]
        findings = self._findings_dormancy(a["n"], dorm_impact)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="Dormancy の影響",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_dormancy_waterfall_figure(waterfall_persons),
                "dormancy_waterfall", height=500,
            ),
            method_note=(
                "Dormancy 乗数 D_i は5つのIVコンポーネントの加重合計に乗算的に適用される。"
                "D=1.0 はペナルティなし、D<1.0 は活動ギャップペナルティを示す。"
                "Dormancy適用前IVは iv_score / D として推定。影響量 = iv_score - 適用前IV。"
                "最もペナルティの大きい5名と最も小さい5名を表示。"
            ),
            section_id="dormancy_impact",
        )

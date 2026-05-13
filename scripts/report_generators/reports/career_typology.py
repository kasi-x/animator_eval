"""Career Trajectory Typology report — v2 compliant.

Optimal Matching + Ward hierarchical clustering on annual role sequences
produces 3-7 canonical career trajectory types.  Markov transition matrices
compare within-cluster role flow patterns.

Stop-if gate: silhouette < 0.2 across all k values → typology absent report.

Methods:
  - Sequence: annual primary-role (highest CAREER_STAGE per year)
  - Distance: Optimal Matching, substitution cost = |stage_a − stage_b|
  - Clustering: Ward hierarchical (scipy), k = 3-7
  - Selection: highest mean silhouette on precomputed OM distance matrix
  - Markov: year-to-year transition probabilities per cluster

Only structural data (credit records, role, year).  Viewer ratings are not used.
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STAGE_LABELS: dict[int, str] = {
    0: "non-production",
    1: "動画 / In-Between",
    2: "第二原画 / 2nd Key",
    3: "原画 / Key Animator",
    4: "キャラデザ / Char. Designer",
    5: "作監・監督補 / Anim. Director",
    6: "監督 / Director",
}

_CLUSTER_COLORS: list[str] = [
    "#3593D2", "#E07532", "#3BC494",
    "#E09BC2", "#F8EC6A", "#A97CC2", "#78C2E0",
]


class CareerTypologyReport(BaseReportGenerator):
    """Career trajectory typology: OM + Ward clustering of role sequences."""

    name = "career_typology"
    title = "キャリア軌跡 Typology — Optimal Matching + Ward Clustering"
    subtitle = (
        "年次役職 sequence の Optimal Matching 距離 + Ward 法で抽出した"
        "canonical キャリア軌跡類型 (3-7 types)"
    )
    filename = "career_typology.html"
    doc_type = "main"

    def generate(self) -> Path | None:
        from src.analysis.career.trajectory_typology import (
            compute_trajectory_typology,
        )

        sb = SectionBuilder()
        result = compute_trajectory_typology(self.conn)

        insert_lineage(
            self.conn,
            table_name="meta_career_typology",
            audience="technical_appendix",
            source_silver_tables=["credits"],
            formula_version="v1.0",
            ci_method=(
                "Mean silhouette coefficient on precomputed OM distance matrix "
                "(sklearn.metrics.silhouette_score, metric='precomputed'). "
                "No CI on individual cluster membership — descriptive only."
            ),
            null_model=(
                "Silhouette threshold: score < 0.2 across k=3-7 declares "
                "typology structure absent (stop-if gate). "
                "No within-cluster permutation baseline computed."
            ),
            holdout_method=(
                "Not applicable — descriptive clustering, no predictive claim."
            ),
            description=(
                "Optimal Matching + Ward hierarchical clustering of annual "
                "primary-role sequences. Substitution cost = |stage_a - stage_b|. "
                "Indel cost = 1.0. Best k selected by highest silhouette (k=3-7). "
                "Cluster labels are structural descriptors of sequence shape, "
                "not evaluative assessments."
            ),
            rng_seed=42,
        )

        sections: list[str] = []

        # Overview
        overview_html = self._build_overview(result)

        if result.stop_if_triggered:
            sections.append(sb.build_section(self._build_stop_if_section(sb, result)))
        else:
            sections.append(
                sb.build_section(self._build_silhouette_section(sb, result))
            )
            sections.append(
                sb.build_section(self._build_cluster_overview_section(sb, result))
            )
            for cluster in result.clusters:
                sections.append(
                    sb.build_section(
                        self._build_cluster_detail_section(sb, cluster, result)
                    )
                )
            sections.append(
                sb.build_section(self._build_markov_section(sb, result))
            )

        interpretation_html = self._build_interpretation(result)

        # Wrap interpretation in labeled block per REPORT_PHILOSOPHY v2 §2.2
        interp_block = ""
        if interpretation_html:
            interp_block = (
                '<div class="card interpretation" id="interpretation"'
                ' style="border-left:3px solid #c0a0d0;">'
                '<h2>Interpretation / 解釈</h2>'
                '<p style="font-size:0.8rem;color:#9090b0;">'
                "以下は分析者の解釈であり、代替解釈が存在する。 / "
                "The following reflects the analyst's interpretation; "
                "alternative interpretations exist.</p>"
                f"{interpretation_html}</div>"
            )

        body = overview_html + "\n" + "\n".join(sections) + "\n" + interp_block
        return self.write_report(body)

    # ── Overview ──────────────────────────────────────────────────────────────

    def _build_overview(self, result: object) -> str:
        from src.analysis.career.trajectory_typology import TypologyResult

        r: TypologyResult = result  # type: ignore[assignment]
        if r.stop_if_triggered:
            status = (
                "<strong>Stop-if triggered</strong>: "
                "全 k 値でシルエット係数が閾値 0.2 を下回り、"
                "cluster 構造が検出されませんでした。typology 抽出を見送ります。"
            )
        else:
            status = (
                f"<strong>k={r.best_k}</strong> clusters selected, "
                f"silhouette={r.best_silhouette:.4f}. "
                f"Sequences analysed: {r.n_sequences:,}."
            )

        return (
            '<div class="card" id="overview">'
            "<h2>概要 / Overview</h2>"
            f"<p>{status}</p>"
            "<p>本レポートは公開クレジットデータ上の年次役職遷移系列を "
            "Optimal Matching 距離 + Ward 階層クラスタリングで類型化する。"
            "各類型 (cluster) のラベルは系列形状の構造的記述であり、"
            "個人の評価やランクではない。</p>"
            "</div>"
        )

    # ── Stop-if section ───────────────────────────────────────────────────────

    def _build_stop_if_section(
        self, sb: SectionBuilder, result: object
    ) -> ReportSection:
        from src.analysis.career.trajectory_typology import (
            SILHOUETTE_THRESHOLD,
            TypologyResult,
        )

        r: TypologyResult = result  # type: ignore[assignment]
        scores_html = ""
        if r.silhouette_scores:
            rows = "".join(
                f"<tr><td>k={k}</td><td>{s:.4f}</td></tr>"
                for k, s in sorted(r.silhouette_scores.items())
            )
            scores_html = (
                f"<table><thead><tr><th>k</th>"
                f"<th>silhouette</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )

        findings_html = (
            f"<p>シルエット係数が全 k (k={min(r.silhouette_scores, default=0)}"
            f"..{max(r.silhouette_scores, default=0)}) で閾値 "
            f"{SILHOUETTE_THRESHOLD} を下回りました。"
            f"このデータセットでは cluster 構造が希薄であり、"
            f"typology 類型化を実施しません。</p>"
            f"<p>Stop-if reason: {r.stop_if_reason}</p>"
            f"{scores_html}"
        )

        return ReportSection(
            title="Stop-if: Cluster 構造希薄 — Typology 見送り",
            findings_html=findings_html,
            method_note=(
                f"Silhouette threshold = {SILHOUETTE_THRESHOLD}. "
                "All evaluated k values produced silhouette below threshold. "
                "This is reported as a null finding per REPORT_PHILOSOPHY §3.5."
            ),
            section_id="stop_if",
        )

    # ── Silhouette selection section ──────────────────────────────────────────

    def _build_silhouette_section(
        self, sb: SectionBuilder, result: object
    ) -> ReportSection:
        from src.analysis.career.trajectory_typology import (
            SILHOUETTE_THRESHOLD,
            TypologyResult,
        )

        r: TypologyResult = result  # type: ignore[assignment]
        ks = sorted(r.silhouette_scores.keys())
        sils = [r.silhouette_scores[k] for k in ks]

        fig = go.Figure()
        colors_bar = [
            "#3593D2" if k == r.best_k else "#606070" for k in ks
        ]
        fig.add_trace(
            go.Bar(
                x=[f"k={k}" for k in ks],
                y=sils,
                marker_color=colors_bar,
                hovertemplate="k=%{x}<br>silhouette=%{y:.4f}<extra></extra>",
            )
        )
        fig.add_hline(
            y=SILHOUETTE_THRESHOLD,
            line_dash="dash",
            line_color="#E07532",
            annotation_text=f"threshold={SILHOUETTE_THRESHOLD}",
        )
        fig.update_layout(
            title="Silhouette 係数 by k (Ward clustering, OM distance)",
            xaxis_title="Number of clusters k",
            yaxis_title="Mean silhouette coefficient",
            yaxis=dict(range=[-0.1, 1.0]),
        )

        best_sil = r.silhouette_scores.get(r.best_k, 0.0)
        findings_html = (
            f"<p>Ward 法 + OM 距離行列に対して k={min(ks)}..{max(ks)} の"
            f"silhouette 係数を算出した。"
            f"最大 silhouette は <strong>k={r.best_k}</strong> で "
            f"{best_sil:.4f} (閾値={SILHOUETTE_THRESHOLD})。</p>"
            f"<p>以降の分析は k={r.best_k} を採用する。</p>"
        )

        return ReportSection(
            title="Silhouette 係数による最適 k 選択",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_silhouette", height=380),
            method_note=(
                "Silhouette = mean silhouette coefficient "
                "(sklearn.metrics.silhouette_score, metric='precomputed'). "
                f"Evaluated k = {min(ks)}..{max(ks)}. "
                "Best k = highest silhouette above threshold. "
                "Threshold = 0.2 (stop-if below this for all k)."
            ),
            section_id="silhouette_selection",
        )

    # ── Cluster overview section ───────────────────────────────────────────────

    def _build_cluster_overview_section(
        self, sb: SectionBuilder, result: object
    ) -> ReportSection:
        from src.analysis.career.trajectory_typology import TypologyResult

        r: TypologyResult = result  # type: ignore[assignment]
        clusters = r.clusters

        rows = []
        for cl in clusters:
            medoid_seq_str = " → ".join(
                _STAGE_LABELS.get(s, str(s)) for s in cl.typical_stages[:6]
            )
            if len(cl.typical_stages) > 6:
                medoid_seq_str += " ..."
            rows.append(
                f"<tr>"
                f"<td><strong>Cluster {cl.cluster_id}</strong></td>"
                f"<td>{cl.label}</td>"
                f"<td>{cl.n:,}</td>"
                f"<td style='font-size:0.8rem;'>{medoid_seq_str}</td>"
                f"</tr>"
            )

        table_html = (
            "<table><thead><tr>"
            "<th>ID</th><th>構造ラベル</th><th>人数</th><th>代表系列 (最大6年)</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

        total_n = sum(cl.n for cl in clusters)
        findings_html = (
            f"<p>k={r.best_k} のクラスタリング結果 (n={total_n:,} sequences):</p>"
            f"{table_html}"
            f"<p>構造ラベルは系列の形状 (昇順・安定・遅延昇進等) の記述的名称であり、"
            "個人の評価を含まない。</p>"
        )

        return ReportSection(
            title="Cluster 構成一覧",
            findings_html=findings_html,
            method_note=(
                f"Ward 法 (scipy.cluster.hierarchy.linkage method='ward'), k={r.best_k}. "
                "OM 距離行列 (substitution cost = |stage_a - stage_b|, indel=1.0). "
                "代表系列 = 各クラスター内の medoid (クラスター内OM距離和が最小の個人)。"
                "構造ラベルは medoid の年次 stage 遷移パターンから機械的に導出。"
            ),
            section_id="cluster_overview",
        )

    # ── Per-cluster detail section ─────────────────────────────────────────────

    def _build_cluster_detail_section(
        self,
        sb: SectionBuilder,
        cluster: object,
        result: object,
    ) -> ReportSection:
        from src.analysis.career.trajectory_typology import TrajectoryCluster

        cl: TrajectoryCluster = cluster  # type: ignore[assignment]
        color = _CLUSTER_COLORS[cl.cluster_id % len(_CLUSTER_COLORS)]

        # Stage sequence bar chart for medoid
        stage_nums = cl.typical_stages[:20]
        stage_lbls = [_STAGE_LABELS.get(s, str(s)) for s in stage_nums]
        year_axis = list(range(1, len(stage_nums) + 1))

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=year_axis,
                y=stage_nums,
                marker_color=color,
                text=stage_lbls,
                textposition="outside",
                hovertemplate=(
                    "Year %{x}<br>Stage %{y}<br>%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title=f"Cluster {cl.cluster_id} — 代表系列 (medoid, 最大20年)",
            xaxis_title="相対年 (1=デビュー年)",
            yaxis_title="Career Stage",
            yaxis=dict(range=[0, 7]),
        )

        findings_html = (
            f"<p><strong>Cluster {cl.cluster_id}</strong>: "
            f"構造ラベル「{cl.label}」, n={cl.n:,} 名。</p>"
            f"<p>代表系列 (medoid={cl.medoid_person_id}): "
            f"長さ {len(cl.typical_stages)} 年, "
            f"初期 stage={cl.typical_stages[0] if cl.typical_stages else '-'}, "
            f"最終 stage={cl.typical_stages[-1] if cl.typical_stages else '-'}.</p>"
        )

        return ReportSection(
            title=f"Cluster {cl.cluster_id}: {cl.label}",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(
                fig, f"chart_cluster_{cl.cluster_id}", height=320
            ),
            method_note=(
                "Medoid = クラスター内の OM 距離和最小個人。"
                "横軸は person のデビュー年を 1 とした相対年。"
                "Stage は CAREER_STAGE (role_groups.py) の数値対応。"
            ),
            section_id=f"cluster_{cl.cluster_id}",
        )

    # ── Markov section ─────────────────────────────────────────────────────────

    def _build_markov_section(
        self, sb: SectionBuilder, result: object
    ) -> ReportSection:
        from src.analysis.career.trajectory_typology import TypologyResult

        r: TypologyResult = result  # type: ignore[assignment]
        clusters = r.clusters

        figs = []
        for cl in clusters:
            if not cl.transition_matrix or not cl.stage_labels:
                continue

            mat = cl.transition_matrix
            # Only show stages 1-6 (production roles)
            stage_indices = [i for i, lbl in enumerate(cl.stage_labels) if i >= 1]
            if not stage_indices:
                continue

            mat_sub = [
                [mat[i][j] for j in stage_indices]
                for i in stage_indices
            ]
            axis_labels = [
                _STAGE_LABELS.get(int(cl.stage_labels[i].replace("stage_", "")), cl.stage_labels[i])
                for i in stage_indices
            ]

            fig = go.Figure(
                data=go.Heatmap(
                    z=mat_sub,
                    x=axis_labels,
                    y=axis_labels,
                    colorscale="Blues",
                    zmin=0.0,
                    zmax=1.0,
                    hovertemplate=(
                        "from: %{y}<br>to: %{x}<br>prob=%{z:.3f}<extra></extra>"
                    ),
                    text=[
                        [f"{v:.2f}" for v in row]
                        for row in mat_sub
                    ],
                    texttemplate="%{text}",
                )
            )
            fig.update_layout(
                title=(
                    f"Cluster {cl.cluster_id} ({cl.label}) — "
                    "Markov 遷移確率 (stage t → t+1)"
                ),
                xaxis_title="遷移先 stage",
                yaxis_title="遷移元 stage",
                height=400,
            )
            figs.append((cl.cluster_id, cl.label, fig))

        viz_parts = []
        for cid, lbl, fig in figs:
            viz_parts.append(
                plotly_div_safe(fig, f"chart_markov_{cid}", height=400)
            )
        viz_html = "\n".join(viz_parts) if viz_parts else ""

        findings_html = (
            "<p>各 cluster の年次 stage 遷移確率行列 (Markov chain 近似):</p>"
            "<ul>"
            "<li>行 = 遷移元 stage, 列 = 遷移先 stage</li>"
            "<li>値 = 当該 cluster 内での当年→翌年遷移確率</li>"
            "<li>対角要素が高い cluster は同一 stage への留まり傾向が強い</li>"
            "<li>上三角が高い cluster は stage 上昇傾向が強い</li>"
            "</ul>"
            "<p>遷移確率は cluster 内の全連続年ペアから集計した。"
            "gap 年 (観測なし) は遷移集計から除外している。</p>"
        )

        return ReportSection(
            title="Markov 遷移確率行列 (cluster 別)",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "遷移 (i, j) = cluster 内で year t に stage i、year t+1 に stage j"
                "を保有した年ペア数の合計。行ごとに正規化し確率化。"
                "Non-production (stage 0) は除外。"
                "連続年ペアのみ集計 (gap 年は補完しない)。"
            ),
            section_id="markov_transitions",
        )

    # ── Interpretation (labeled, with alternatives) ───────────────────────────

    def _build_interpretation(self, result: object) -> str:
        from src.analysis.career.trajectory_typology import TypologyResult

        r: TypologyResult = result  # type: ignore[assignment]
        if r.stop_if_triggered:
            return (
                "<p>本レポートの著者は、シルエット係数が閾値を下回った事実は "
                "「キャリア軌跡に類型的構造が存在しない」証拠ではなく、"
                "「本データセット上の系列では類型が分離しにくい」ことを示すと解釈する。</p>"
                "<p><strong>代替解釈</strong>: "
                "データカバレッジの偏り (TV アニメ中心) や系列長の不均一性が "
                "silhouette を低下させている可能性があり、"
                "サブセット (アニメーター専業者のみ等) では類型が検出される可能性がある。</p>"
            )

        n_cl = r.best_k or 0
        return (
            f"<p>本レポートの著者は、{n_cl} 類型への分離が "
            "クレジットデータ上で観察可能な軌跡の構造差異を反映していると解釈する。"
            "ただし、類型は役職遷移の形状に基づく記述的区分であり、"
            "個人の将来経路の予測や評価ではない。</p>"
            "<p><strong>代替解釈</strong>: "
            "Ward クラスタリングは距離行列の凸部分集合を優先するため、"
            "連続スペクトル状の分布が存在する場合でも k 個に分割される。"
            "Gap 統計等の代替指標では異なる k が選択される可能性がある。</p>"
            "<p><strong>前提の開示</strong>: "
            "本解釈はクレジット記録が実際の役職担当に対応するという前提に依拠する。"
            "多担当・無名義・協力スタジオへの出向は捕捉外であることに注意。</p>"
        )


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="career_typology",
    audience="technical_appendix",
    claim=(
        "年次 primary-role sequence の Optimal Matching 距離 + Ward 法により "
        "k=3-7 の canonical 軌跡類型が抽出可能であり、最適 k の mean silhouette ≥ 0.2"
    ),
    identifying_assumption=(
        "クレジット記録の役職タイトル変化が実際のキャリア段階変化に対応する。"
        "Gap 年 (credits なし) は系列から除外し補完しない。"
        "Markov 仮定: 翌年の stage は当年の stage のみに依存 (メモリなし)。"
    ),
    null_model=["N4"],
    sources=["credits"],
    meta_table="meta_career_typology",
    estimator="Optimal Matching distance + Ward hierarchical clustering",
    ci_estimator="analytical_se",
    extra_limitations=[
        "OM 距離は sequence 長に依存するため、短系列の多い個人が不利な可能性",
        "Gap 年補完なしのため、非連続的クレジット者の系列は分断されうる",
        "CAREER_STAGE の数値間隔が等距離と仮定されているが実際は不均等",
        "TV アニメ中心のカバレッジにより劇場・OVA 専業者は系列が短くなりやすい",
    ],
)

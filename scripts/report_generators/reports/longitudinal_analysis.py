"""Longitudinal Analysis report -- v2 compliant.

Full port of the 33-chart monolith with gender/tier stratification.

Sections 1-5 (visually rich, fully ported with gender/tier enrichment):
  1. Temporal Alignment: Spaghetti plots (4 cohort panels, gender color dim) + Lexis heatmap
  2. Sequence Analysis: Sequence index plot (OMA heatmap) + OMA cluster trajectories (5 panels)
  3. Transition Flow: Alluvial/Sankey (5/10/15/20yr checkpoints) + CFD (gender split)
  4. Multidimensional Mapping: MDS similarity + Bipartite person x role
  5. Stock & Flow: Streamgraph (gender overlay) + Horizon chart + Stock/Flow dual panel

Sections 6-11 (key charts included, simplified):
  6. Demand gap, productivity, workforce dynamics
  7. Format/genre staff density
  8. Startup cost OLS, studio size, survival bias
  9. Causal inference (event study, FE regression, PSM)
 10. WPS (weighted productivity index)
"""

from __future__ import annotations

import math
import random
import statistics as _stats
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..color_utils import hex_to_rgba as _hex_to_rgba
from ..helpers import (
    get_agg_milestones,
    get_feat_career,
    get_feat_person_scores,
    load_json,
)
from ..html_templates import plotly_div_safe, strat_panel, stratification_tabs
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

# ---------------------------------------------------------------------------
# Constants (replicated from monolith for self-containedness)
# ---------------------------------------------------------------------------

STAGE_LABELS: dict[int, str] = {
    1: "動画",
    2: "第二原画",
    3: "原画",
    4: "キャラデ",
    5: "作監・演出",
    6: "監督",
}

STAGE_COLORS_HEX: dict[int, str] = {
    0: "#404050",
    1: "#a0a0c0",
    2: "#7EB8D4",
    3: "#4CC9F0",
    4: "#FFD166",
    5: "#FF6B35",
    6: "#F72585",
}

COHORT_LABELS: dict[int, str] = {
    1960: "1960年代",
    1970: "1970年代",
    1980: "1980年代",
    1990: "1990年代",
    2000: "2000年代",
    2010: "2010年代",
    2020: "2020年代",
}

COHORT_COLORS: dict[int, str] = {
    1960: "#4CC9F0",
    1970: "#7209B7",
    1980: "#F72585",
    1990: "#FF6B35",
    2000: "#06D6A0",
    2010: "#FFD166",
    2020: "#aaaaaa",
}

_ROLE_JA: dict[str, str] = {
    "in_between": "動画",
    "key_animator": "原画",
    "animation_director": "作監",
    "director": "監督",
    "episode_director": "演出",
    "special": "その他",
    "layout": "レイアウト",
    "photography_director": "撮影監督",
    "cgi_director": "CGI監督",
}

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}

_GENDER_COLORS = {"Male": "#4CC9F0", "Female": "#F72585", None: "#a0a0c0"}
_GENDER_LABELS = {"Male": "男性", "Female": "女性", None: "不明"}

# Industry milestone years for vline annotations
_MILESTONES = [(2000, "ネット普及"), (2015, "配信台頭"), (2020, "COVID")]

_STARTUP_ROLES: frozenset[str] = frozenset({
    "director", "screenplay", "character_designer", "background_art",
    "music", "original_creator", "producer", "sound_director",
})
_VARIABLE_ROLES: frozenset[str] = frozenset({
    "key_animator", "in_between", "animation_director",
    "episode_director", "photography_director", "background_art",
})
_ROLE_TO_STAGE: dict[str, int] = {
    "in_between": 1, "layout": 2,
    "key_animator": 3, "photography_director": 4,
    "animation_director": 4, "character_designer": 4,
    "episode_director": 5, "director": 6,
    "screenplay": 5, "background_art": 2,
    "finishing": 4, "music": 4, "producer": 5, "sound_director": 4,
}


# ---------------------------------------------------------------------------
# Helper functions (ported from monolith)
# ---------------------------------------------------------------------------


def _get_cohort_decade(first_year: int | None) -> int:
    if not first_year:
        return 2000
    decade = (first_year // 10) * 10
    if decade <= 1960:
        return 1960
    if decade >= 2020:
        return 2020
    return decade


def _hex_alpha(hex_color: str, alpha_hex: str) -> str:
    """Convert '#RRGGBB' + 2-char alpha hex to 'rgba(r,g,b,a)' for Plotly."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = h[0] * 2 + h[1] * 2 + h[2] * 2
    if len(h) != 6:
        return hex_color
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    a = round(int(alpha_hex, 16) / 255, 2)
    return f"rgba({r},{g},{b},{a})"


def _build_stage_sequence(
    person_ids: set[str],
    milestones_data: dict,
    scores_by_pid: dict,
    max_age: int = 25,
) -> tuple[dict[str, list[int]], dict[str, bool]]:
    """Build career-age indexed stage arrays."""
    stage_seqs: dict[str, list[int]] = {}
    is_missing: dict[str, bool] = {}

    for pid in person_ids:
        score_entry = scores_by_pid.get(pid, {})
        first_year = score_entry.get("career", {}).get("first_year")
        if not first_year:
            is_missing[pid] = True
            stage_seqs[pid] = [1] * (max_age + 1)
            continue

        events = milestones_data.get(pid, [])
        promotions: list[tuple[int, int]] = []
        for e in events:
            if e.get("type") == "promotion" and "to_stage" in e and "year" in e:
                promotions.append((int(e["year"]), int(e["to_stage"])))

        if not promotions:
            is_missing[pid] = True
            stage_seqs[pid] = [1] * (max_age + 1)
            continue

        sorted_promos = sorted(promotions, key=lambda x: x[0])
        seq: list[int] = []
        current_stage = 1
        promo_idx = 0
        for age in range(max_age + 1):
            yr = first_year + age
            while promo_idx < len(sorted_promos) and sorted_promos[promo_idx][0] <= yr:
                current_stage = sorted_promos[promo_idx][1]
                promo_idx += 1
            seq.append(max(1, min(6, current_stage)))

        stage_seqs[pid] = seq
        is_missing[pid] = False

    return stage_seqs, is_missing


def _compute_oma_clusters(
    stage_seqs_dict: dict[str, list[int]],
    n_clusters: int = 5,
) -> dict[str, int]:
    """OMA approximate clustering (Ward + Hamming distance)."""
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import pdist

    pids = list(stage_seqs_dict.keys())
    if len(pids) < n_clusters + 1:
        return {pid: 0 for pid in pids}

    x_mat = np.array([stage_seqs_dict[pid] for pid in pids], dtype=np.float32)
    d_condensed = pdist(x_mat, metric="hamming")
    z_link = linkage(d_condensed, method="ward")
    k = min(n_clusters, len(pids))
    labels = fcluster(z_link, k, criterion="maxclust")
    return {pid: int(labels[i]) - 1 for i, pid in enumerate(pids)}


def _build_lexis_surface(
    scores_data: list[dict],
    milestones_data: dict,  # noqa: ARG001
) -> tuple[list[int], list[int], list[list[float | None]]]:
    """Build calendar year x career_age Lexis surface."""
    years = list(range(1970, 2025))
    career_ages = list(range(0, 41))

    by_first_year: dict[int, list[dict]] = {}
    for p in scores_data:
        fy = p.get("career", {}).get("first_year")
        if fy and isinstance(fy, int):
            by_first_year.setdefault(fy, []).append(p)

    cell_scores: dict[tuple[int, int], list[float]] = {}
    for yr in years:
        for age in career_ages:
            fy = yr - age
            persons = by_first_year.get(fy, [])
            for p in persons:
                comp = p.get("iv_score")
                if comp is not None and comp > 0:
                    cell_scores.setdefault((yr, age), []).append(float(comp))

    z_matrix: list[list[float | None]] = []
    for age in career_ages:
        row: list[float | None] = []
        for yr in years:
            vals = cell_scores.get((yr, age))
            if vals:
                row.append(round(sum(vals) / len(vals), 2))
            else:
                row.append(None)
        z_matrix.append(row)

    return years, career_ages, z_matrix


def _add_milestone_vlines(
    fig: go.Figure,
    *,
    row: int | None = None,
    col: int | None = None,
) -> None:
    """Add industry milestone vertical lines to a figure."""
    kwargs: dict[str, Any] = {}
    if row is not None:
        kwargs["row"] = row
    if col is not None:
        kwargs["col"] = col
    for marker_yr, label in _MILESTONES:
        fig.add_vline(
            x=marker_yr,
            line_dash="dash",
            line_color="rgba(255,255,255,0.5)",
            annotation_text=label,
            annotation_position="top",
            annotation_font=dict(color="rgba(255,255,255,0.7)", size=11),
            **kwargs,
        )


def _gender_lookup(conn: Any) -> dict[str, str | None]:
    """Build {person_id: gender} from DB."""
    try:
        rows = conn.execute(
            "SELECT id, gender FROM persons WHERE gender IS NOT NULL"
        ).fetchall()
        return {str(r["id"]): r["gender"] for r in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class LongitudinalAnalysisReport(BaseReportGenerator):
    name = "longitudinal_analysis"
    title = "縦断分析"
    subtitle = (
        "キャリア軌跡・コホート比較・役職推移・離脱パターン "
        "— 全33チャート + 性別/Tier層別分析"
    )
    filename = "longitudinal_analysis.html"

    def generate(self) -> Path | None:  # noqa: C901
        sb = SectionBuilder()

        # ── shared data ───────────────────────────────────────────
        scores_data = get_feat_person_scores()
        if not scores_data:
            return None

        milestones_data = get_agg_milestones() or {}
        time_series_data = load_json("time_series.json") or {}
        growth_data = get_feat_career() or {}

        scores_by_pid: dict[str, dict] = {
            p["person_id"]: p for p in scores_data
        }
        gender_map = _gender_lookup(self.conn)

        # Top persons for detailed analysis
        top500 = sorted(
            [p for p in scores_data if p.get("career", {}).get("first_year")],
            key=lambda p: -(p.get("iv_score") or 0),
        )[:500]
        top500_pids = {p["person_id"] for p in top500}
        top300 = top500[:300]
        top300_pids = {p["person_id"] for p in top300}

        stage_seqs, is_missing = _build_stage_sequence(
            top500_pids, milestones_data, scores_by_pid, max_age=25,
        )
        stage_seqs_300 = {
            pid: seq for pid, seq in stage_seqs.items() if pid in top300_pids
        }
        oma_clusters = _compute_oma_clusters(stage_seqs_300, n_clusters=5)

        # ── build sections ────────────────────────────────────────
        sections: list[str] = []

        # Sections 1-5 (fully ported)
        sections.append(sb.build_section(
            self._build_spaghetti_section(
                sb, top500, scores_by_pid, stage_seqs, is_missing, gender_map,
            )
        ))
        sections.append(sb.build_section(
            self._build_lexis_section(sb, scores_data, milestones_data, time_series_data)
        ))
        sections.append(sb.build_section(
            self._build_sequence_index_section(
                sb, top300_pids, stage_seqs, is_missing, oma_clusters, scores_by_pid,
            )
        ))
        sections.append(sb.build_section(
            self._build_oma_cluster_section(
                sb, stage_seqs, is_missing, oma_clusters, scores_by_pid,
            )
        ))
        sections.append(sb.build_section(
            self._build_sankey_section(
                sb, top500_pids, stage_seqs, is_missing,
            )
        ))
        sections.append(sb.build_section(
            self._build_cfd_section(
                sb, top500_pids, stage_seqs, gender_map, scores_by_pid,
            )
        ))
        sections.append(sb.build_section(
            self._build_mds_section(
                sb, stage_seqs_300, is_missing, oma_clusters, scores_by_pid,
            )
        ))
        sections.append(sb.build_section(
            self._build_bipartite_section(
                sb, top500[:100], milestones_data, scores_by_pid,
            )
        ))
        sections.append(sb.build_section(
            self._build_streamgraph_section(sb, milestones_data, gender_map)
        ))
        sections.append(sb.build_section(
            self._build_horizon_section(sb, growth_data, scores_by_pid)
        ))
        sections.append(sb.build_section(
            self._build_stock_flow_section(
                sb, scores_data, milestones_data, scores_by_pid,
                time_series_data, gender_map,
            )
        ))

        # Sections 6-10 (key charts)
        sections.append(sb.build_section(
            self._build_demand_gap_section(sb, time_series_data)
        ))
        sections.append(sb.build_section(
            self._build_productivity_section(sb, time_series_data)
        ))
        sections.append(sb.build_section(
            self._build_turnover_section(sb, scores_data, time_series_data)
        ))
        sections.append(sb.build_section(
            self._build_format_profile_section(sb)
        ))
        sections.append(sb.build_section(
            self._build_startup_cost_section(sb)
        ))
        sections.append(sb.build_section(
            self._build_causal_section(sb, scores_by_pid)
        ))

        return self.write_report("\n".join(sections))

    # ═══════════════════════════════════════════════════════════════
    # Section 1: Spaghetti plots (Chart 1)
    # ═══════════════════════════════════════════════════════════════

    def _build_spaghetti_section(
        self,
        sb: SectionBuilder,
        top500: list[dict],
        scores_by_pid: dict[str, dict],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        try:
            return self._spaghetti_impl(
                sb, top500, scores_by_pid, stage_seqs, is_missing, gender_map,
            )
        except Exception as e:
            return ReportSection(
                title="スパゲッティプロット -- コホート別キャリアステージ軌跡",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="spaghetti",
            )

    def _spaghetti_impl(
        self,
        sb: SectionBuilder,
        top500: list[dict],
        scores_by_pid: dict[str, dict],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        max_age_plot = 25
        career_ages_axis = list(range(max_age_plot + 1))

        cohort_groups: dict[int, list[dict]] = {1980: [], 1990: [], 2000: [], 2010: []}
        for p in top500:
            fy = p.get("career", {}).get("first_year")
            if not fy:
                continue
            decade = _get_cohort_decade(fy)
            if decade in cohort_groups:
                cohort_groups[decade].append(p)

        subplot_titles = [f"{dec}年代デビュー" for dec in sorted(cohort_groups)]
        fig = make_subplots(
            rows=2, cols=2, subplot_titles=subplot_titles,
            vertical_spacing=0.12, horizontal_spacing=0.08,
        )

        n_by_cohort: dict[int, int] = {}
        for idx, (dec, persons) in enumerate(sorted(cohort_groups.items())):
            row_idx = idx // 2 + 1
            col_idx = idx % 2 + 1
            color = COHORT_COLORS.get(dec, "#888888")
            n_by_cohort[dec] = len(persons)

            if not persons:
                continue

            rng = random.Random(dec)
            sample_persons = rng.sample(persons, min(80, len(persons)))

            for p in sample_persons:
                pid = p["person_id"]
                seq = stage_seqs.get(pid)
                if not seq:
                    continue
                missing = is_missing.get(pid, True)
                gender = gender_map.get(pid)
                # Gender as color dimension
                if gender == "Female":
                    line_color = "rgba(247,37,133,0.18)"
                elif gender == "Male":
                    line_color = (
                        f"rgba({int(color[1:3], 16)},{int(color[3:5], 16)},"
                        f"{int(color[5:7], 16)},0.12)"
                    )
                else:
                    line_color = "rgba(100,100,100,0.07)"

                jitter = [
                    (stage + rng.uniform(-0.1, 0.1))
                    for stage in seq[: max_age_plot + 1]
                ]
                fig.add_trace(
                    go.Scatter(
                        x=career_ages_axis[: len(jitter)],
                        y=jitter,
                        mode="lines",
                        line=dict(
                            color="rgba(100,100,100,0.07)" if missing else line_color,
                            dash="dot" if missing else "solid",
                            width=1,
                        ),
                        showlegend=False,
                        hoverinfo="skip",
                    ),
                    row=row_idx,
                    col=col_idx,
                )

            # Median line
            median_stages: list[float | None] = []
            for age in career_ages_axis:
                vals_at_age = [
                    stage_seqs[p["person_id"]][age]
                    for p in persons
                    if p["person_id"] in stage_seqs
                    and age < len(stage_seqs[p["person_id"]])
                    and not is_missing.get(p["person_id"], True)
                ]
                median_stages.append(_stats.median(vals_at_age) if vals_at_age else None)

            fig.add_trace(
                go.Scatter(
                    x=career_ages_axis,
                    y=median_stages,
                    mode="lines",
                    name=f"{COHORT_LABELS.get(dec, str(dec))} 中央値",
                    line=dict(color=color, width=3),
                    connectgaps=True,
                    hovertemplate=(
                        "career_age: %{x}yr<br>median stage: %{y:.2f}<extra></extra>"
                    ),
                ),
                row=row_idx,
                col=col_idx,
            )

        fig.update_layout(
            title_text="スパゲッティプロット -- コホート別キャリアステージ軌跡",
            showlegend=True,
            legend=dict(orientation="h", y=-0.05),
        )
        for i in range(1, 5):
            r, c = (i - 1) // 2 + 1, (i - 1) % 2 + 1
            fig.update_yaxes(
                tickvals=list(range(1, 7)),
                ticktext=[STAGE_LABELS[s] for s in range(1, 7)],
                range=[0.5, 6.5],
                row=r,
                col=c,
            )

        cohort_desc = "; ".join(
            f"{dec}s: n={n_by_cohort.get(dec, 0)}"
            for dec in sorted(cohort_groups)
        )
        findings = (
            f"<p>IV score上位500人のキャリアステージ軌跡をデビュー年代コホート別に表示"
            f"（{cohort_desc}）。 "
            "ピンク線は女性と識別された人物、青/色付き線は"
            "男性と識別された人物、灰色点線はmilestones.jsonに"
            "昇進記録がない人物（ステージ1で固定）を示す。 "
            "太い実線はコホートの年功別中央値ステージを示す。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="スパゲッティプロット -- コホート別キャリアステージ軌跡",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "spaghetti-chart", height=700),
            method_note=(
                "ステージ系列は milestones.json の昇進イベントから構築。"
                "最大キャリア年齢 = 25 年。IV スコア上位 500 人から"
                "コホートごとに 80 人をサンプリング（SVG サイズ抑制のため）。"
                "性別は persons.gender から取得（NULL は灰色にマッピング）。"
                "ステージ値には ±0.1 のジッタを加え重なりを軽減。"
            ),
            interpretation_html=(
                "<p>スパゲッティプロットは軌跡形状の異質性を示している。"
                "早期コホート（1980年代）ではステージ5-6に到達する人物が多いが、"
                "これは観測期間が長いことと整合的である。"
                "別解釈として、後期コホートでは業界の構造変化（チームの大型化、"
                "専門化の進展）により、ステージ5-6到達率が実質的に低下している"
                "可能性もある。</p>"
            ),
            section_id="spaghetti",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 1b: Lexis heatmap (Chart 2)
    # ═══════════════════════════════════════════════════════════════

    def _build_lexis_section(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        milestones_data: dict,
        time_series_data: dict,
    ) -> ReportSection:
        try:
            return self._lexis_impl(sb, scores_data, milestones_data, time_series_data)
        except Exception as e:
            return ReportSection(
                title="拡張レキシス表面",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="lexis",
            )

    def _lexis_impl(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        milestones_data: dict,
        time_series_data: dict,
    ) -> ReportSection:
        lex_years, lex_ages, lex_z = _build_lexis_surface(scores_data, milestones_data)

        fig = go.Figure(
            go.Heatmap(
                x=lex_years,
                y=lex_ages,
                z=lex_z,
                colorscale="Plasma",
                colorbar=dict(title="平均 IV Score"),
                hoverongaps=False,
                hovertemplate=(
                    "年: %{x}<br>経験年数: %{y}<br>"
                    "平均 IV: %{z:.2f}<extra></extra>"
                ),
            )
        )

        _add_milestone_vlines(fig)

        fig.update_layout(
            title_text="拡張レキシス表面 -- カレンダー年 x 経験年数",
            xaxis_title="暦年",
            yaxis_title="経験年数（デビューからの年数）",
        )

        # Demand context
        ts_series = time_series_data.get("series", {})
        unique_anime_d = ts_series.get("unique_anime", {})
        demand_note = ""
        if unique_anime_d:
            avg_2000_14 = sum(
                unique_anime_d.get(str(y), 0) for y in range(2000, 2015)
            )
            avg_2015_24 = sum(
                unique_anime_d.get(str(y), 0) for y in range(2015, 2025)
            )
            demand_note = (
                f"<p>2000〜2014年の累計アニメ数: {avg_2000_14:,} "
                f"（{avg_2000_14 // 15:,}/年）、"
                f"2015〜2024年: {avg_2015_24:,}（{avg_2015_24 // 10:,}/年）。</p>"
            )

        findings = (
            "<p>レキシス表面: 暦年（X）× 年功（Y）を、"
            "debut_year = year - career_ageとなる人物の平均IV scoreで色分け。 "
            "対角線方向の同色はコホートを追跡し、垂直方向の色変化は"
            "時代効果を反映する。 "
            f"カバー範囲: {min(lex_years)}-{max(lex_years)}、年功0-40。</p>"
            f"{demand_note}"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="拡張レキシス表面",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "lexis-chart", height=520),
            method_note=(
                "セル値 = first_year = 暦年 - 経験年数 を満たす全人物の IV score 平均。"
                "全人物で年次 IV score が入手できないため、総合 IV score を代理指標として使用する。"
                "該当人物のいないセルは空白とする。"
            ),
            section_id="lexis",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 2: Sequence Index Plot (Chart 3)
    # ═══════════════════════════════════════════════════════════════

    def _build_sequence_index_section(
        self,
        sb: SectionBuilder,
        top300_pids: set[str],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._seq_index_impl(
                sb, top300_pids, stage_seqs, is_missing, oma_clusters, scores_by_pid,
            )
        except Exception as e:
            return ReportSection(
                title="シーケンスインデックスプロット",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="seq_index",
            )

    def _seq_index_impl(
        self,
        sb: SectionBuilder,
        top300_pids: set[str],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        sorted_pids = sorted(
            top300_pids & set(oma_clusters.keys()),
            key=lambda pid: (
                oma_clusters.get(pid, 0),
                -(scores_by_pid.get(pid, {}).get("iv_score") or 0),
            ),
        )

        max_age = 25
        z_data: list[list[int]] = []
        hover_data: list[list[str]] = []
        y_labels: list[str] = []
        for pid in sorted_pids:
            seq = stage_seqs.get(pid, [1] * (max_age + 1))
            z_data.append([s if s > 0 else 0 for s in seq[: max_age + 1]])
            name_str = scores_by_pid.get(pid, {}).get("name") or pid[:20]
            cl = oma_clusters.get(pid, 0) + 1
            y_labels.append(f"C{cl} {name_str[:12]}")
            hover_data.append([
                f"{name_str}<br>age={a}yr<br>"
                f"{STAGE_LABELS.get(seq[a] if a < len(seq) else 1, '?')}"
                for a in range(max_age + 1)
            ])

        colorscale = [
            [0.0, STAGE_COLORS_HEX[0]],
            [0.15, STAGE_COLORS_HEX[1]],
            [0.30, STAGE_COLORS_HEX[2]],
            [0.45, STAGE_COLORS_HEX[3]],
            [0.60, STAGE_COLORS_HEX[4]],
            [0.80, STAGE_COLORS_HEX[5]],
            [1.0, STAGE_COLORS_HEX[6]],
        ]

        fig = go.Figure(
            go.Heatmap(
                z=z_data,
                x=list(range(max_age + 1)),
                y=y_labels,
                colorscale=colorscale,
                zmin=0,
                zmax=6,
                colorbar=dict(
                    title="段階",
                    tickvals=[0, 1, 2, 3, 4, 5, 6],
                    ticktext=["不明", "動画", "第二原画", "原画", "キャラデ", "作監", "監督"],
                ),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_data,
            )
        )
        fig.update_layout(
            title_text="シーケンスインデックスプロット (OMAクラスタ順)",
            xaxis_title="経験年数（年）",
            yaxis_title="人物（クラスタ順）",
            yaxis=dict(showticklabels=len(sorted_pids) <= 80),
        )

        findings = (
            f"<p>シーケンスインデックスプロット: n={len(sorted_pids)}人（IV上位300人、"
            "OMAクラスタ順、クラスタ内はIV順でソート）。各行 = 1人、"
            "各列 = 年功0-25。色はキャリアステージを表す。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="シーケンスインデックスプロット",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "seq-index-chart", height=600),
            method_note=(
                "OMA クラスタリング: 段階シーケンス（長さ26）上のハミング距離、"
                "Ward 連結法、5クラスタ。昇進記録のない人物は段階1に固定される"
                "（均一な行として表示される）。"
            ),
            section_id="seq_index",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 2b: OMA Cluster Trajectories (Chart 4)
    # ═══════════════════════════════════════════════════════════════

    def _build_oma_cluster_section(
        self,
        sb: SectionBuilder,
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._oma_impl(sb, stage_seqs, is_missing, oma_clusters, scores_by_pid)
        except Exception as e:
            return ReportSection(
                title="OMAクラスタ代表軌跡",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="oma_clusters",
            )

    def _oma_impl(
        self,
        sb: SectionBuilder,
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        n_clusters = 5
        max_age = 25
        career_ages = list(range(max_age + 1))
        cluster_pids: dict[int, list[str]] = {}
        for pid, cl in oma_clusters.items():
            cluster_pids.setdefault(cl, []).append(pid)

        cluster_names: dict[int, str] = {}
        cluster_stats: dict[int, dict[str, Any]] = {}
        for cl in range(n_clusters):
            pids_cl = cluster_pids.get(cl, [])
            if not pids_cl:
                cluster_names[cl] = f"C{cl + 1}: データ不足"
                cluster_stats[cl] = {}
                continue

            med_seq: list[float] = []
            for age in career_ages:
                vals = [
                    stage_seqs[pid][age]
                    for pid in pids_cl
                    if pid in stage_seqs
                    and age < len(stage_seqs[pid])
                    and not is_missing.get(pid, True)
                ]
                med_seq.append(_stats.median(vals) if vals else 1.0)

            dir_count = sum(
                1 for pid in pids_cl if any(s >= 6 for s in stage_seqs.get(pid, []))
            )
            dir_rate = dir_count / len(pids_cl) if pids_cl else 0
            span_vals: list[int] = []
            for pid in pids_cl:
                p = scores_by_pid.get(pid, {})
                fy = p.get("career", {}).get("first_year")
                ly = p.get("career", {}).get("latest_year")
                if fy and ly:
                    span_vals.append(ly - fy)
            avg_span = _stats.mean(span_vals) if span_vals else 0.0

            max_stage = max(med_seq)
            peak_age = career_ages[med_seq.index(max_stage)]
            if max_stage >= 5.5:
                name = "監督到達型"
            elif max_stage >= 4.5:
                name = "作監・演出到達型"
            elif max_stage >= 3.8:
                name = "作監キャリア型"
            elif peak_age <= 8:
                name = "早期昇進型"
            else:
                name = "長期原画型"
            cluster_names[cl] = f"C{cl + 1}: {name}"
            cluster_stats[cl] = {
                "n": len(pids_cl),
                "dir_rate": dir_rate,
                "avg_span": avg_span,
                "med_seq": med_seq,
            }

        fig = make_subplots(
            rows=2,
            cols=3,
            subplot_titles=[
                cluster_names.get(cl, f"C{cl + 1}") for cl in range(n_clusters)
            ]
            + [""],
            vertical_spacing=0.15,
            horizontal_spacing=0.08,
        )
        palette = ["#4CC9F0", "#F72585", "#06D6A0", "#FFD166", "#FF6B35"]

        for cl in range(n_clusters):
            r = cl // 3 + 1
            c = cl % 3 + 1
            pids_cl = cluster_pids.get(cl, [])
            color = palette[cl]

            for pid in pids_cl[:30]:
                seq = stage_seqs.get(pid, [])
                if not seq or is_missing.get(pid, True):
                    continue
                fig.add_trace(
                    go.Scatter(
                        x=career_ages[: len(seq)],
                        y=seq[: max_age + 1],
                        mode="lines",
                        line=dict(color=_hex_to_rgba(color, 0.18), width=1),
                        showlegend=False,
                        hoverinfo="skip",
                    ),
                    row=r,
                    col=c,
                )

            stats_cl = cluster_stats.get(cl, {})
            med_seq = stats_cl.get("med_seq", [])
            if med_seq:
                fig.add_trace(
                    go.Scatter(
                        x=career_ages,
                        y=med_seq,
                        mode="lines",
                        name=cluster_names.get(cl, f"C{cl + 1}"),
                        line=dict(color=color, width=3),
                        hovertemplate=(
                            f"{cluster_names.get(cl, '')}<br>"
                            f"N={stats_cl.get('n', 0)} "
                            f"dir_rate={stats_cl.get('dir_rate', 0):.1%}<br>"
                            "age=%{x}yr median=%{y:.2f}<extra></extra>"
                        ),
                    ),
                    row=r,
                    col=c,
                )

        fig.update_layout(title_text="OMAクラスタ代表軌跡", showlegend=False)
        for i in range(1, 6):
            r, c = (i - 1) // 3 + 1, (i - 1) % 3 + 1
            fig.update_yaxes(
                tickvals=list(range(1, 7)),
                ticktext=[STAGE_LABELS[s] for s in range(1, 7)],
                range=[0.5, 6.5],
                row=r,
                col=c,
            )

        # Stats table
        table_html = (
            "<table><thead><tr><th>クラスタ</th><th>n</th>"
            "<th>監督到達率</th><th>平均キャリア年数</th></tr></thead><tbody>"
        )
        for cl in range(n_clusters):
            st = cluster_stats.get(cl, {})
            table_html += (
                f"<tr><td>{cluster_names.get(cl, f'C{cl + 1}')}</td>"
                f"<td>{st.get('n', 0)}</td>"
                f"<td>{st.get('dir_rate', 0):.1%}</td>"
                f"<td>{st.get('avg_span', 0):.1f}年</td></tr>"
            )
        table_html += "</tbody></table>"

        findings = (
            "<p>OMAクラスタ代表軌跡（5パネル）。 "
            "細線 = クラスタ内の個人軌跡、"
            "太線 = 中央値。 "
            f"クラスタリング対象人数合計: {sum(len(v) for v in cluster_pids.values())}人。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="OMAクラスタ代表軌跡",
            findings_html=findings,
            visualization_html=(
                plotly_div_safe(fig, "oma-clusters-chart", height=620) + table_html
            ),
            method_note=(
                "クラスタ命名: 中央値軌跡のピーク段階とピーク経験年数に基づく。"
                "監督到達率 = 段階6以上に到達した人物の割合。"
                "クラスタごとに最大30本の個別軌跡を表示する。"
            ),
            section_id="oma_clusters",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 3: Alluvial/Sankey (Chart 5)
    # ═══════════════════════════════════════════════════════════════

    def _build_sankey_section(
        self,
        sb: SectionBuilder,
        top500_pids: set[str],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
    ) -> ReportSection:
        try:
            return self._sankey_impl(sb, top500_pids, stage_seqs, is_missing)
        except Exception as e:
            return ReportSection(
                title="アリュビアル図 -- career_age 5/10/15/20yr遷移",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="sankey",
            )

    def _sankey_impl(
        self,
        sb: SectionBuilder,
        top500_pids: set[str],
        stage_seqs: dict[str, list[int]],
        is_missing: dict[str, bool],
    ) -> ReportSection:
        checkpoints = [5, 10, 15, 20]
        checkpoint_dist: dict[int, dict[int, list[str]]] = {
            cp: {s: [] for s in range(1, 7)} for cp in checkpoints
        }
        for pid in top500_pids:
            seq = stage_seqs.get(pid)
            if not seq or is_missing.get(pid, True):
                continue
            for cp in checkpoints:
                if cp < len(seq):
                    stg = max(1, min(6, seq[cp]))
                    checkpoint_dist[cp][stg].append(pid)

        sankey_nodes: list[str] = []
        sankey_colors: list[str] = []
        node_idx: dict[tuple[int, int], int] = {}
        for cp in checkpoints:
            for stg in range(1, 7):
                cnt = len(checkpoint_dist[cp][stg])
                if cnt > 0:
                    idx = len(sankey_nodes)
                    node_idx[(cp, stg)] = idx
                    sankey_nodes.append(
                        f"{cp}年: {STAGE_LABELS.get(stg, str(stg))} ({cnt})"
                    )
                    sankey_colors.append(STAGE_COLORS_HEX.get(stg, "#888"))

        link_src: list[int] = []
        link_tgt: list[int] = []
        link_val: list[int] = []
        link_lbl: list[str] = []
        for i, cp_from in enumerate(checkpoints[:-1]):
            cp_to = checkpoints[i + 1]
            transition_counts: dict[tuple[int, int], int] = {}
            for pid in top500_pids:
                seq = stage_seqs.get(pid)
                if not seq or is_missing.get(pid, True):
                    continue
                if cp_from >= len(seq) or cp_to >= len(seq):
                    continue
                s_from = max(1, min(6, seq[cp_from]))
                s_to = max(1, min(6, seq[cp_to]))
                transition_counts[(s_from, s_to)] = (
                    transition_counts.get((s_from, s_to), 0) + 1
                )
            for (s_from, s_to), cnt in transition_counts.items():
                if cnt < 2:
                    continue
                src_node = node_idx.get((cp_from, s_from))
                tgt_node = node_idx.get((cp_to, s_to))
                if src_node is not None and tgt_node is not None:
                    link_src.append(src_node)
                    link_tgt.append(tgt_node)
                    link_val.append(cnt)
                    link_lbl.append(
                        f"{cp_from}→{cp_to}年: "
                        f"{STAGE_LABELS.get(s_from, '?')}→"
                        f"{STAGE_LABELS.get(s_to, '?')} ({cnt})"
                    )

        viz_html = ""
        if link_src:
            fig = go.Figure(
                go.Sankey(
                    node=dict(
                        label=sankey_nodes,
                        color=sankey_colors,
                        pad=15,
                        thickness=20,
                    ),
                    link=dict(
                        source=link_src,
                        target=link_tgt,
                        value=link_val,
                        label=link_lbl,
                        color="rgba(160,160,200,0.25)",
                    ),
                )
            )
            fig.update_layout(
                title_text="アリュビアル図 -- キャリアパイプラインのチェックポイント",
                font=dict(size=11),
            )
            viz_html = plotly_div_safe(fig, "alluvial-chart", height=550)
        else:
            viz_html = "<p>Sankey図に必要な昇進記録が不足しています。</p>"

        findings = (
            "<p>アリュビアル/Sankey図: キャリア5, 10, 15, 20年時点のステージを表示。 "
            "帯の幅 = ステージ間の遷移人数。 "
            "昇進記録のある人物のみを含む。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="アリュビアル図 -- career_age 5/10/15/20yr遷移",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "件数 2 未満の遷移はフィルタで除外する。"
                "昇進データのない人物は除外する。"
            ),
            section_id="sankey",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 3b: CFD with gender split (Chart 6)
    # ═══════════════════════════════════════════════════════════════

    def _build_cfd_section(
        self,
        sb: SectionBuilder,
        top500_pids: set[str],
        stage_seqs: dict[str, list[int]],
        gender_map: dict[str, str | None],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._cfd_impl(sb, top500_pids, stage_seqs, gender_map, scores_by_pid)
        except Exception as e:
            return ReportSection(
                title="累積フロー図 (CFD)",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="cfd",
            )

    def _cfd_impl(
        self,
        sb: SectionBuilder,
        top500_pids: set[str],
        stage_seqs: dict[str, list[int]],
        gender_map: dict[str, str | None],
        scores_by_pid: dict[str, dict],  # noqa: ARG002
    ) -> ReportSection:
        max_age = 30
        age_axis = list(range(max_age + 1))

        # Overall CFD
        stage_counts: dict[int, dict[int, int]] = {
            age: {s: 0 for s in range(1, 7)} for age in age_axis
        }
        for pid in top500_pids:
            seq = stage_seqs.get(pid)
            if not seq:
                continue
            for age in age_axis:
                if age < len(seq):
                    stg = max(1, min(6, seq[age]))
                    stage_counts[age][stg] += 1

        fig_all = go.Figure()
        for stg in range(1, 7):
            y_vals = [stage_counts[age][stg] for age in age_axis]
            fig_all.add_trace(
                go.Scatter(
                    x=age_axis,
                    y=y_vals,
                    name=STAGE_LABELS.get(stg, f"S{stg}"),
                    mode="lines",
                    stackgroup="one",
                    line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=0.5),
                    fillcolor=_hex_alpha(STAGE_COLORS_HEX.get(stg, "#888"), "88"),
                )
            )
        fig_all.update_layout(
            title_text="CFD -- キャリアパイプライン（全体）",
            xaxis_title="経験年数",
            yaxis_title="人数（累積）",
            legend=dict(orientation="h", y=-0.12),
        )

        # Gender-split CFD
        gender_stage_counts: dict[str, dict[int, dict[int, int]]] = {}
        for gender_key in ("Male", "Female"):
            gender_stage_counts[gender_key] = {
                age: {s: 0 for s in range(1, 7)} for age in age_axis
            }
        for pid in top500_pids:
            seq = stage_seqs.get(pid)
            if not seq:
                continue
            g = gender_map.get(pid)
            if g not in ("Male", "Female"):
                continue
            for age in age_axis:
                if age < len(seq):
                    stg = max(1, min(6, seq[age]))
                    gender_stage_counts[g][age][stg] += 1

        fig_gender = make_subplots(
            rows=1, cols=2, subplot_titles=["男性", "女性"],
            horizontal_spacing=0.08,
        )
        for gi, gender_key in enumerate(("Male", "Female")):
            col = gi + 1
            for stg in range(1, 7):
                y_vals = [gender_stage_counts[gender_key][age][stg] for age in age_axis]
                fig_gender.add_trace(
                    go.Scatter(
                        x=age_axis,
                        y=y_vals,
                        name=STAGE_LABELS.get(stg, f"S{stg}"),
                        mode="lines",
                        stackgroup=f"g{gi}",
                        line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=0.5),
                        fillcolor=_hex_alpha(STAGE_COLORS_HEX.get(stg, "#888"), "88"),
                        showlegend=(gi == 0),
                    ),
                    row=1,
                    col=col,
                )
        fig_gender.update_layout(
            title_text="CFD -- 性別別",
            legend=dict(orientation="h", y=-0.12),
        )

        tabs_html = stratification_tabs(
            "cfd_tabs", {"all": "全体", "gender": "性別別"}, active="all",
        )
        panels = strat_panel(
            "cfd_tabs", "all",
            plotly_div_safe(fig_all, "cfd-all", height=480),
            active=True,
        )
        panels += strat_panel(
            "cfd_tabs", "gender",
            plotly_div_safe(fig_gender, "cfd-gender", height=480),
        )

        # Bottleneck detection
        bottleneck_age = bottleneck_stg = None
        max_thickness = 0
        for age in range(1, max_age):
            for stg in range(2, 5):
                thickness = stage_counts[age][stg]
                if thickness > max_thickness:
                    max_thickness = thickness
                    bottleneck_age = age
                    bottleneck_stg = stg

        bn_text = ""
        if bottleneck_age and bottleneck_stg:
            bn_text = (
                f" Largest accumulation: career_age={bottleneck_age}, "
                f"{STAGE_LABELS.get(bottleneck_stg, '?')} "
                f"({max_thickness} persons)."
            )

        findings = (
            "<p>累積フロー図（積み上げ面グラフ）: 年功別の各キャリアステージの"
            "在籍人数を表示。性別分割パネルで男女のパイプライン構造を比較。"
            f"{bn_text}</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="累積フロー図 (CFD)",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "クロスセクショナルビュー: 各キャリア年齢において、"
                "暦年に関わらず当該ステージの人物を集計。"
                "性別は persons.gender から取得。NULL の人物は性別別パネルから除外。"
            ),
            section_id="cfd",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 4: MDS (Chart 7)
    # ═══════════════════════════════════════════════════════════════

    def _build_mds_section(
        self,
        sb: SectionBuilder,
        stage_seqs_300: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._mds_impl(
                sb, stage_seqs_300, is_missing, oma_clusters, scores_by_pid,
            )
        except Exception as e:
            return ReportSection(
                title="MDS空間マッピング",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="mds",
            )

    def _mds_impl(
        self,
        sb: SectionBuilder,
        stage_seqs_300: dict[str, list[int]],
        is_missing: dict[str, bool],
        oma_clusters: dict[str, int],
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        from scipy.spatial.distance import pdist, squareform
        from sklearn.manifold import MDS

        mds_pids = [
            pid for pid in stage_seqs_300 if not is_missing.get(pid, True)
        ]
        if len(mds_pids) < 10:
            return ReportSection(
                title="MDS空間マッピング",
                findings_html="<p>昇進データを持つ人物が10人未満です。</p>",
                section_id="mds",
            )

        x_mat = np.array([stage_seqs_300[pid] for pid in mds_pids], dtype=np.float32)
        d_condensed = pdist(x_mat, metric="hamming")
        d_sq = squareform(d_condensed)
        n_mds = len(mds_pids)

        try:
            mds = MDS(
                n_components=2,
                metric_mds=False,
                metric="precomputed",
                random_state=42,
                n_init=4,
                init="random",
                max_iter=200 if n_mds > 100 else 300,
            )
        except TypeError:
            mds = MDS(
                n_components=2,
                metric=False,
                dissimilarity="precomputed",
                random_state=42,
                n_init=4,
                max_iter=200 if n_mds > 100 else 300,
            )
        coords = mds.fit_transform(d_sq)

        colors_mds = [
            COHORT_COLORS.get(
                _get_cohort_decade(
                    scores_by_pid.get(pid, {}).get("career", {}).get("first_year")
                ),
                "#888",
            )
            for pid in mds_pids
        ]
        sizes_mds = [
            max(4, min(20, (scores_by_pid.get(pid, {}).get("iv_score") or 5) * 0.3))
            for pid in mds_pids
        ]
        hover_mds = [
            f"{scores_by_pid.get(pid, {}).get('name', pid[:20])}<br>"
            f"Debut: {scores_by_pid.get(pid, {}).get('career', {}).get('first_year', '?')}<br>"
            f"Cluster: C{oma_clusters.get(pid, 0) + 1}<br>"
            f"IV: {scores_by_pid.get(pid, {}).get('iv_score', 0):.1f}"
            for pid in mds_pids
        ]

        fig = go.Figure(
            go.Scatter(
                x=coords[:, 0],
                y=coords[:, 1],
                mode="markers",
                marker=dict(
                    color=colors_mds,
                    size=sizes_mds,
                    opacity=0.75,
                    line=dict(color="rgba(255,255,255,0.15)", width=0.5),
                ),
                text=hover_mds,
                hovertemplate="%{text}<extra></extra>",
            )
        )
        fig.update_layout(
            title_text="MDS -- キャリアシーケンス類似度マップ",
            xaxis_title="MDS 次元 1",
            yaxis_title="MDS 次元 2",
            showlegend=False,
        )

        findings = (
            f"<p>昇進データを持つ{n_mds}人の非計量MDS 2次元投影。 "
            "近接 = 類似したキャリアステージ系列（ハミング距離）。 "
            "色 = デビュー年代コホート、サイズ = IV score。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="MDS空間マッピング",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "mds-chart", height=520),
            method_note=(
                "ハミング距離行列に対する非計量 MDS（sklearn）。"
                "n_init=4、max_iter=200-300。軸には絶対的な意味はなく、"
                "相対距離のみが解釈可能である。"
            ),
            section_id="mds",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 4b: Bipartite graph (Chart 8)
    # ═══════════════════════════════════════════════════════════════

    def _build_bipartite_section(
        self,
        sb: SectionBuilder,
        top100: list[dict],
        milestones_data: dict,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._bipartite_impl(sb, top100, milestones_data, scores_by_pid)
        except Exception as e:
            return ReportSection(
                title="役職 x 人物 2部グラフ",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="bipartite",
            )

    def _bipartite_impl(
        self,
        sb: SectionBuilder,
        top100: list[dict],
        milestones_data: dict,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        import networkx as nx

        person_roles: dict[str, set[str]] = {}
        for p in top100:
            pid = p["person_id"]
            events = milestones_data.get(pid, [])
            roles_set: set[str] = set()
            for e in events:
                if e.get("type") == "new_role":
                    r = e.get("role", "")
                    if r and r != "special":
                        roles_set.add(r)
            if roles_set:
                person_roles[pid] = roles_set

        if len(person_roles) < 3:
            return ReportSection(
                title="役職 x 人物 2部グラフ",
                findings_html="<p>人物-役職データが不足しています。</p>",
                section_id="bipartite",
            )

        bip = nx.Graph()
        all_roles: set[str] = set()
        for pid, roles_set in person_roles.items():
            bip.add_node(pid, bipartite=0)
            for r in roles_set:
                bip.add_node(r, bipartite=1)
                bip.add_edge(pid, r)
                all_roles.add(r)

        pos = nx.spring_layout(bip, seed=42, k=0.8)

        edge_x: list[float | None] = []
        edge_y: list[float | None] = []
        for u, v in bip.edges():
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=edge_x,
                y=edge_y,
                mode="lines",
                line=dict(width=0.5, color="rgba(160,160,200,0.2)"),
                hoverinfo="none",
                showlegend=False,
            )
        )

        # Person nodes
        px_vals = [pos[pid][0] for pid in person_roles]
        py_vals = [pos[pid][1] for pid in person_roles]
        ps_vals = [
            max(6, min(24, (scores_by_pid.get(pid, {}).get("iv_score") or 5) * 0.35))
            for pid in person_roles
        ]
        ph_vals = [
            f"{scores_by_pid.get(pid, {}).get('name', pid[:15])}<br>"
            f"IV: {scores_by_pid.get(pid, {}).get('iv_score', 0):.3f}"
            for pid in person_roles
        ]
        fig.add_trace(
            go.Scatter(
                x=px_vals,
                y=py_vals,
                mode="markers",
                marker=dict(
                    size=ps_vals,
                    color="#4CC9F0",
                    symbol="circle",
                    line=dict(color="rgba(255,255,255,0.3)", width=0.5),
                ),
                text=ph_vals,
                hovertemplate="%{text}<extra>人物</extra>",
                name="人物",
            )
        )

        # Role nodes
        role_list = sorted(all_roles)
        role_credit_counts: dict[str, int] = {}
        for pid in person_roles:
            events = milestones_data.get(pid, [])
            for e in events:
                r = e.get("role", "")
                if r in all_roles:
                    role_credit_counts[r] = role_credit_counts.get(r, 0) + 1

        rx_vals = [pos[r][0] for r in role_list]
        ry_vals = [pos[r][1] for r in role_list]
        rs_vals = [
            max(10, min(40, math.sqrt(role_credit_counts.get(r, 1)) * 2))
            for r in role_list
        ]
        fig.add_trace(
            go.Scatter(
                x=rx_vals,
                y=ry_vals,
                mode="markers+text",
                marker=dict(
                    size=rs_vals,
                    color="#FF6B35",
                    symbol="square",
                    line=dict(color="rgba(255,255,255,0.3)", width=0.5),
                ),
                text=[_ROLE_JA.get(r, r) for r in role_list],
                textposition="top center",
                textfont=dict(size=9),
                name="役職",
            )
        )
        fig.update_layout(
            title_text="2部グラフ: 人物 × 役職",
            showlegend=True,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        )

        findings = (
            f"<p>2部グラフ: {len(person_roles)}人の人物（円、サイズ=IV）と"
            f"{len(all_roles)}個の役職カテゴリ（四角）。 "
            "エッジは人物が担当した役職への接続を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="役職 x 人物 2部グラフ",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "bipartite-chart", height=580),
            method_note=(
                "Spring layout（NetworkX、seed=42）。"
                "人物サイズ = IV score × 0.35（6-24 にクランプ）。"
                "役職サイズ = sqrt(クレジット数) × 2（10-40 にクランプ）。"
            ),
            section_id="bipartite",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 5: Streamgraph (Chart 9) with gender overlay
    # ═══════════════════════════════════════════════════════════════

    def _build_streamgraph_section(
        self,
        sb: SectionBuilder,
        milestones_data: dict,
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        try:
            return self._streamgraph_impl(sb, milestones_data, gender_map)
        except Exception as e:
            return ReportSection(
                title="ストリームグラフ -- 役職別クレジット年次推移",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="streamgraph",
            )

    def _streamgraph_impl(
        self,
        sb: SectionBuilder,
        milestones_data: dict,
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        main_roles = [
            "in_between", "key_animator", "animation_director",
            "director", "episode_director",
        ]
        role_by_year: dict[str, dict[int, int]] = {}
        role_by_year_gender: dict[str, dict[str, dict[int, int]]] = {}

        for pid, events in milestones_data.items():
            gender = gender_map.get(pid)
            for e in events:
                if e.get("type") in ("career_start", "new_role"):
                    yr = e.get("year")
                    role = e.get("role", "")
                    if yr and role and 1970 <= yr <= 2025:
                        role_by_year.setdefault(role, {}).setdefault(yr, 0)
                        role_by_year[role][yr] += 1
                        if gender in ("Male", "Female"):
                            role_by_year_gender.setdefault(role, {}).setdefault(
                                gender, {}
                            ).setdefault(yr, 0)
                            role_by_year_gender[role][gender][yr] += 1

        stream_years = list(range(1990, 2026))
        stream_data: dict[str, list[int]] = {}
        for role in main_roles:
            vals = [role_by_year.get(role, {}).get(yr, 0) for yr in stream_years]
            if sum(vals) > 50:
                stream_data[role] = vals

        if not stream_data:
            return ReportSection(
                title="ストリームグラフ -- 役職別クレジット年次推移",
                findings_html="<p>ストリームグラフに必要な役職-年データが不足しています。</p>",
                section_id="streamgraph",
            )

        stream_colors = [
            "#a0a0c0", "#7EB8D4", "#4CC9F0", "#FFD166",
            "#FF6B35", "#F72585", "#06D6A0", "#7209B7",
        ]
        role_names = list(stream_data.keys())

        # Overall streamgraph
        fig_all = go.Figure()
        for i, role in enumerate(role_names):
            color = stream_colors[i % len(stream_colors)]
            fig_all.add_trace(
                go.Scatter(
                    x=stream_years,
                    y=stream_data[role],
                    name=_ROLE_JA.get(role, role),
                    mode="lines",
                    stackgroup="stream",
                    line=dict(color=color, width=0.5),
                    fillcolor=_hex_alpha(color, "99"),
                )
            )
        _add_milestone_vlines(fig_all)
        fig_all.update_layout(
            title_text="ストリームグラフ -- 役職需要の経年推移（全体）",
            xaxis_title="年",
            yaxis_title="クレジット数（積み上げ）",
            legend=dict(orientation="h", y=-0.12),
        )

        # Gender-split streamgraph
        fig_gender = make_subplots(
            rows=1, cols=2, subplot_titles=["男性", "女性"],
            horizontal_spacing=0.08,
        )
        for gi, gk in enumerate(("Male", "Female")):
            col = gi + 1
            for i, role in enumerate(role_names):
                gdata = role_by_year_gender.get(role, {}).get(gk, {})
                vals = [gdata.get(yr, 0) for yr in stream_years]
                color = stream_colors[i % len(stream_colors)]
                fig_gender.add_trace(
                    go.Scatter(
                        x=stream_years,
                        y=vals,
                        name=_ROLE_JA.get(role, role),
                        mode="lines",
                        stackgroup=f"gs{gi}",
                        line=dict(color=color, width=0.5),
                        fillcolor=_hex_alpha(color, "99"),
                        showlegend=(gi == 0),
                    ),
                    row=1,
                    col=col,
                )
        fig_gender.update_layout(
            title_text="ストリームグラフ -- 性別別",
            legend=dict(orientation="h", y=-0.12),
        )

        tabs_html = stratification_tabs(
            "stream_tabs", {"all": "全体", "gender": "性別別"}, active="all",
        )
        panels = strat_panel(
            "stream_tabs", "all",
            plotly_div_safe(fig_all, "stream-all", height=500),
            active=True,
        )
        panels += strat_panel(
            "stream_tabs", "gender",
            plotly_div_safe(fig_gender, "stream-gender", height=500),
        )

        findings = (
            "<p>役職カテゴリ別の年間クレジット数ストリームグラフ"
            f"（{', '.join(_ROLE_JA.get(r, r) for r in role_names[:5])}）、"
            f"1990-2025年。性別分割パネルで男女別の役職需要を"
            "個別に表示。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="ストリームグラフ -- 役職別クレジット年次推移",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "クレジット数は milestones.json（career_start + new_role イベント）から取得。"
                "総クレジット数 50 未満の役職は除外。"
                "積み上げ面グラフ; 帯の厚さ = その役職の年次クレジット数。"
            ),
            section_id="streamgraph",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 5b: Horizon chart (Chart 10)
    # ═══════════════════════════════════════════════════════════════

    def _build_horizon_section(
        self,
        sb: SectionBuilder,
        growth_data: dict,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._horizon_impl(sb, growth_data, scores_by_pid)
        except Exception as e:
            return ReportSection(
                title="ホライズンチャート -- 個人クレジット負荷",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="horizon",
            )

    def _horizon_impl(
        self,
        sb: SectionBuilder,
        growth_data: dict,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        growth_persons = growth_data.get("persons", {})
        if not growth_persons:
            return ReportSection(
                title="ホライズンチャート -- 個人クレジット負荷",
                findings_html="<p>growth.jsonの人物データが利用できません。</p>",
                section_id="horizon",
            )

        sorted_pids = sorted(
            growth_persons.keys(),
            key=lambda pid: -(scores_by_pid.get(pid, {}).get("iv_score") or 0),
        )[:200]

        horizon_years = list(range(2000, 2026))
        z_data: list[list[float]] = []
        y_labels: list[str] = []
        for pid in sorted_pids:
            yearly = growth_persons[pid].get("yearly_credits", {})
            row = [int(yearly.get(str(yr), 0)) for yr in horizon_years]
            max_c = max(row) if max(row) > 0 else 1
            z_data.append([c / max_c for c in row])
            name = scores_by_pid.get(pid, {}).get("name") or pid[:15]
            y_labels.append(name[:14])

        fig = go.Figure(
            go.Heatmap(
                z=z_data,
                x=horizon_years,
                y=y_labels,
                colorscale=[
                    [0.0, "rgba(0,0,0,0)"],
                    [0.001, "#0d1b2a"],
                    [0.3, "#1d4e8a"],
                    [0.6, "#2196f3"],
                    [1.0, "#90caf9"],
                ],
                zmin=0,
                zmax=1,
                colorbar=dict(title="負荷"),
                hovertemplate="%{y}<br>%{x}: %{z:.2%}<extra></extra>",
            )
        )
        for marker_yr in [2000, 2015, 2020]:
            if marker_yr in horizon_years:
                fig.add_vline(
                    x=marker_yr, line_dash="dash",
                    line_color="rgba(255,255,255,0.3)",
                )
        fig.update_layout(
            title_text="ホライズンチャート -- 個人クレジット負荷",
            xaxis_title="年",
            yaxis=dict(
                title="人物（IV 降順）",
                showticklabels=len(sorted_pids) <= 60,
            ),
        )

        findings = (
            f"<p>ホライズンチャート: n={len(sorted_pids)}人（IV上位200人）、"
            "2000-2025年。各行はその人物のピーク年で正規化。 "
            "濃いセル = その人物の最大値に対する高活動年。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="ホライズンチャート -- 個人クレジット負荷",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "horizon-chart", height=520),
            method_note=(
                "各行 = 1人、本人の年間最大クレジット数で正規化。"
                "growth.json の yearly_credits フィールドを使用。"
                "人物は IV score 降順でソート。"
            ),
            section_id="horizon",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 5c: Stock & Flow (Chart 11) with gender breakdown
    # ═══════════════════════════════════════════════════════════════

    def _build_stock_flow_section(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        milestones_data: dict,
        scores_by_pid: dict[str, dict],
        time_series_data: dict,
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        try:
            return self._stock_flow_impl(
                sb, scores_data, milestones_data, scores_by_pid,
                time_series_data, gender_map,
            )
        except Exception as e:
            return ReportSection(
                title="ストック&フロー -- ステージ別在籍人数動態",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="stock_flow",
            )

    def _stock_flow_impl(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        milestones_data: dict,
        scores_by_pid: dict[str, dict],
        time_series_data: dict,
        gender_map: dict[str, str | None],
    ) -> ReportSection:
        stock_years = list(range(1980, 2026))
        stage_stock: dict[int, dict[int, int]] = {
            yr: {s: 0 for s in range(1, 7)} for yr in stock_years
        }
        flow_by_year: dict[int, int] = {yr: 0 for yr in stock_years}
        # Gender stock
        gender_stock: dict[str, dict[int, dict[int, int]]] = {
            g: {yr: {s: 0 for s in range(1, 7)} for yr in stock_years}
            for g in ("Male", "Female")
        }

        sample_pids: set[str] = set()
        for p in scores_data:
            fy = p.get("career", {}).get("first_year")
            ly = p.get("career", {}).get("latest_year")
            if fy and ly and 1980 <= fy <= 2025:
                sample_pids.add(p["person_id"])

        for pid in sample_pids:
            p_data = scores_by_pid.get(pid, {})
            fy = p_data.get("career", {}).get("first_year")
            ly = p_data.get("career", {}).get("latest_year") or 2025
            if not fy:
                continue
            gender = gender_map.get(pid)

            events = milestones_data.get(pid, [])
            promotions = sorted(
                [
                    (int(e["year"]), int(e["to_stage"]))
                    for e in events
                    if e.get("type") == "promotion" and "to_stage" in e and "year" in e
                ],
                key=lambda x: x[0],
            )

            cur_stage = 1
            promo_idx = 0
            last_stage = 1
            for yr in stock_years:
                if yr < fy or yr > ly:
                    continue
                while promo_idx < len(promotions) and promotions[promo_idx][0] <= yr:
                    promo_yr, promo_stg = promotions[promo_idx]
                    if promo_stg != last_stage and promo_yr == yr:
                        flow_by_year[yr] += 1
                    cur_stage = promo_stg
                    promo_idx += 1
                cur_stage = max(1, min(6, cur_stage))
                stage_stock[yr][cur_stage] += 1
                if gender in ("Male", "Female"):
                    gender_stock[gender][yr][cur_stage] += 1
                last_stage = cur_stage

        # Overall stock & flow
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=["ストック: ステージ別人数", "フロー: 年間昇進数"],
            vertical_spacing=0.15,
            row_heights=[0.6, 0.4],
        )
        for stg in range(1, 7):
            y_stock = [stage_stock[yr][stg] for yr in stock_years]
            fig.add_trace(
                go.Scatter(
                    x=stock_years,
                    y=y_stock,
                    name=STAGE_LABELS.get(stg, f"S{stg}"),
                    mode="lines",
                    line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=2),
                ),
                row=1,
                col=1,
            )

        total_flow = [flow_by_year[yr] for yr in stock_years]
        fig.add_trace(
            go.Bar(
                x=stock_years,
                y=total_flow,
                name="昇進数",
                marker_color="#06D6A0",
                opacity=0.7,
            ),
            row=2,
            col=1,
        )
        fig.update_layout(
            title_text="ストック&フロー -- ステージ別人材動態",
            legend=dict(orientation="h", y=-0.08),
        )
        fig.update_yaxes(title_text="人数", row=1, col=1)
        fig.update_yaxes(title_text="昇進数", row=2, col=1)

        # Gender stock chart
        fig_gender = make_subplots(
            rows=1, cols=2, subplot_titles=["男性ストック", "女性ストック"],
            horizontal_spacing=0.08,
        )
        for gi, gk in enumerate(("Male", "Female")):
            col = gi + 1
            for stg in range(1, 7):
                y_vals = [gender_stock[gk][yr][stg] for yr in stock_years]
                fig_gender.add_trace(
                    go.Scatter(
                        x=stock_years,
                        y=y_vals,
                        name=STAGE_LABELS.get(stg, f"S{stg}"),
                        mode="lines",
                        line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=1.5),
                        showlegend=(gi == 0),
                    ),
                    row=1,
                    col=col,
                )
        fig_gender.update_layout(
            title_text="性別ストック",
            legend=dict(orientation="h", y=-0.12),
        )

        tabs_html = stratification_tabs(
            "sf_tabs", {"all": "全体", "gender": "性別別"}, active="all",
        )
        panels = strat_panel(
            "sf_tabs", "all",
            plotly_div_safe(fig, "stock-flow-chart", height=680),
            active=True,
        )
        panels += strat_panel(
            "sf_tabs", "gender",
            plotly_div_safe(fig_gender, "stock-flow-gender", height=480),
        )

        peak_flow_yr = stock_years[total_flow.index(max(total_flow))] if total_flow else 0

        findings = (
            "<p>上段パネル: 暦年別の各キャリアステージ在籍人数（ストック）。 "
            "下段パネル: 年間昇進総数（フロー）。 "
            f"昇進ピーク年: {peak_flow_yr}年（{max(total_flow):,}件の昇進）。 "
            "性別分割タブで男女別のストックを個別に表示。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="ストック&フロー -- ステージ別在籍人数動態",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "ステージ追跡は milestones.json の昇進イベントを暦年に適用。"
                "フロー = その年のステージ遷移数。"
                "scores.json に含まれ first_year が 1980〜2025 の全人物を対象。"
            ),
            section_id="stock_flow",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 6: Demand gap (Chart 12)
    # ═══════════════════════════════════════════════════════════════

    def _build_demand_gap_section(
        self, sb: SectionBuilder, time_series_data: dict,
    ) -> ReportSection:
        try:
            return self._demand_gap_impl(sb, time_series_data)
        except Exception as e:
            return ReportSection(
                title="想定需要 vs 実際需要",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="demand_gap",
            )

    def _demand_gap_impl(
        self, sb: SectionBuilder, time_series_data: dict,
    ) -> ReportSection:
        ts_series = time_series_data.get("series", {})
        ts_years_all = time_series_data.get("years", [])
        demand_years = [y for y in ts_years_all if 1990 <= y <= 2025]
        credits_d = ts_series.get("credit_count", {})

        if not demand_years or not credits_d:
            return ReportSection(
                title="想定需要 vs 実際需要",
                findings_html="<p>time_series.jsonのデータが利用できません。</p>",
                section_id="demand_gap",
            )

        def _ts(d: dict, yr: int) -> float:
            return float(d.get(str(yr), 0) or 0)

        base_yrs = [y for y in demand_years if y <= 2010]
        actual_credits = [_ts(credits_d, y) for y in demand_years]

        # OLS on base period
        def _ols(xs: list[int], ys: list[float], x_new: list[int]) -> list[float]:
            n = len(xs)
            if n < 2:
                return [ys[-1]] * len(x_new)
            mx = sum(xs) / n
            my = sum(ys) / n
            denom = sum((x - mx) ** 2 for x in xs)
            if denom == 0:
                return [my] * len(x_new)
            slope = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / denom
            intercept = my - slope * mx
            return [max(0.0, intercept + slope * x) for x in x_new]

        base_credits = [_ts(credits_d, y) for y in base_yrs]
        proj_yrs = [y for y in demand_years if y > 2010]
        expected_proj = _ols(base_yrs, base_credits, proj_yrs)
        expected_all = base_credits + expected_proj

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=demand_years, y=expected_all,
            name="期待値（1990-2010年トレンド）", mode="lines",
            line=dict(color="#FFD166", width=2, dash="dash"),
        ))
        fig.add_trace(go.Scatter(
            x=demand_years, y=actual_credits,
            name="実績クレジット数", mode="lines",
            line=dict(color="#4CC9F0", width=2.5),
        ))
        _add_milestone_vlines(fig)
        fig.update_layout(
            title_text="需要ギャップ: 期待値 vs 実績クレジット数",
            xaxis_title="年",
            yaxis_title="クレジット数",
            legend=dict(orientation="h", y=-0.1),
        )

        findings = (
            f"<p>クレジット需要: {len(demand_years)}年間（1990-2025年）。 "
            "期待値 = 1990-2010年のトレンドからのOLS線形投影。 "
            "実績と期待値のギャップは2010年以降の需要シフトを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="想定需要 vs 実際需要",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "demand-gap-chart", height=480),
            method_note=(
                "time_series.json の 1990〜2010 credit_count に対する OLS 回帰、"
                "2011〜2025 年へ投影。ギャップ = 実績 − 投影値。"
            ),
            section_id="demand_gap",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 6b: Productivity (Chart 13)
    # ═══════════════════════════════════════════════════════════════

    def _build_productivity_section(
        self, sb: SectionBuilder, time_series_data: dict,
    ) -> ReportSection:
        try:
            return self._productivity_impl(sb, time_series_data)
        except Exception as e:
            return ReportSection(
                title="生産性指数の推移",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="productivity",
            )

    def _productivity_impl(
        self, sb: SectionBuilder, time_series_data: dict,
    ) -> ReportSection:
        ts = time_series_data.get("series", {})
        years_all = time_series_data.get("years", [])
        demand_years = [y for y in years_all if 1990 <= y <= 2025]
        credits_d = ts.get("credit_count", {})
        active_d = ts.get("active_persons", {})

        if not demand_years:
            return ReportSection(
                title="生産性指数の推移",
                findings_html="<p>データが利用できません。</p>",
                section_id="productivity",
            )

        def _ts(d: dict, yr: int) -> float:
            return float(d.get(str(yr), 0) or 0)

        prod_per_person: list[float] = []
        for yr in demand_years:
            c_yr = _ts(credits_d, yr)
            a_yr = _ts(active_d, yr)
            prod_per_person.append(c_yr / a_yr if a_yr > 0 else 0)

        # 5-year rolling average
        prod_trend: list[float] = []
        window = 5
        for i in range(len(demand_years)):
            lo = max(0, i - window // 2)
            hi = min(len(demand_years), i + window // 2 + 1)
            prod_trend.append(sum(prod_per_person[lo:hi]) / (hi - lo))

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=demand_years, y=prod_per_person,
            name="1人あたりクレジット数", mode="lines+markers",
            line=dict(color="#4CC9F0", width=2),
            marker=dict(size=4),
        ))
        fig.add_trace(go.Scatter(
            x=demand_years, y=prod_trend,
            name="5年移動平均", mode="lines",
            line=dict(color="#FFD166", width=2.5, dash="dash"),
        ))
        _add_milestone_vlines(fig)
        fig.update_layout(
            title_text="生産性: アクティブ人物1人あたりクレジット数",
            xaxis_title="年",
            yaxis_title="1人あたりクレジット数",
            legend=dict(orientation="h", y=-0.1),
        )

        findings = (
            f"<p>アクティブ人物1人あたりの年間クレジット数、{demand_years[0]}-"
            f"{demand_years[-1]}年。5年間移動平均を破線で表示。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="生産性指数の推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "productivity-chart", height=460),
            method_note=(
                "1人あたりクレジット数 = credit_count / active_persons"
                "（time_series.json より）。移動平均ウィンドウ = 5 年。"
            ),
            section_id="productivity",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 6c: Turnover (Chart 14)
    # ═══════════════════════════════════════════════════════════════

    def _build_turnover_section(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        time_series_data: dict,
    ) -> ReportSection:
        try:
            return self._turnover_impl(sb, scores_data, time_series_data)
        except Exception as e:
            return ReportSection(
                title="現役・引退・新規の時系列",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="turnover",
            )

    def _turnover_impl(
        self,
        sb: SectionBuilder,
        scores_data: list[dict],
        time_series_data: dict,
    ) -> ReportSection:
        ts = time_series_data.get("series", {})
        years_all = time_series_data.get("years", [])
        demand_years = [y for y in years_all if 1990 <= y <= 2025]
        active_d = ts.get("active_persons", {})
        new_ent_d = ts.get("new_entrants", {})

        def _ts(d: dict, yr: int) -> float:
            return float(d.get(str(yr), 0) or 0)

        retired_by_yr: dict[int, int] = {}
        for p in scores_data:
            ly = p.get("career", {}).get("latest_year")
            if ly and 1990 <= ly <= 2020:  # exit = 5yr gap (2025 - 5)
                retired_by_yr[ly] = retired_by_yr.get(ly, 0) + 1

        new_vals = [_ts(new_ent_d, yr) for yr in demand_years]
        active_vals = [_ts(active_d, yr) for yr in demand_years]
        retired_vals = [float(retired_by_yr.get(yr, 0)) for yr in demand_years]
        net_change = [n - r for n, r in zip(new_vals, retired_vals)]

        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=["現役 / 新規 / 引退", "純増減"],
            vertical_spacing=0.12,
            row_heights=[0.6, 0.4],
        )
        fig.add_trace(go.Scatter(
            x=demand_years, y=active_vals, name="現役",
            mode="lines", line=dict(color="#4CC9F0", width=2.5),
            fill="tozeroy", fillcolor="rgba(76,201,240,0.08)",
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=demand_years, y=new_vals, name="新規参入",
            marker_color="#06D6A0", opacity=0.8,
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=demand_years, y=[-v for v in retired_vals], name="引退",
            marker_color="#FF6B35", opacity=0.8,
        ), row=1, col=1)
        net_colors = ["#06D6A0" if v >= 0 else "#FF6B35" for v in net_change]
        fig.add_trace(go.Bar(
            x=demand_years, y=net_change, name="純増減",
            marker_color=net_colors,
        ), row=2, col=1)
        fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)",
                       row=2, col=1)
        fig.update_layout(
            title_text="人材パイプラインダイナミクス",
            barmode="overlay",
            legend=dict(orientation="h", y=-0.06),
        )
        fig.update_yaxes(title_text="人数", row=1, col=1)
        fig.update_yaxes(title_text="純増減", row=2, col=1)

        findings = (
            f"<p>現役・新規参入・引退人数の推移、{demand_years[0]}-"
            f"{demand_years[-1]}年。引退 = latest_yearがその年に該当する人物"
            "（2024年以降は直近打ち切りにより除外）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="現役・引退・新規の時系列",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "turnover-chart", height=600),
            method_note=(
                "新規参入 = time_series.json の new_entrants。"
                "引退 = scores.json career の latest_year が該当年と一致する人物"
                "（2年ラグバッファを想定）。"
                "純増減 = 新規 − 引退。"
            ),
            section_id="turnover",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 7: Format profile (Charts 15-18 condensed)
    # ═══════════════════════════════════════════════════════════════

    def _build_format_profile_section(
        self, sb: SectionBuilder,
    ) -> ReportSection:
        try:
            return self._format_impl(sb)
        except Exception as e:
            return ReportSection(
                title="フォーマット別スタッフ需要プロファイル",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="format_profile",
            )

    def _format_impl(self, sb: SectionBuilder) -> ReportSection:
        rows = self.conn.execute("""
            SELECT a.format, a.duration, a.episodes,
                COUNT(c.id) AS total_credits,
                COUNT(DISTINCT c.person_id) AS unique_persons
            FROM credits c
            JOIN anime a ON c.anime_id = a.id
            WHERE a.format IN ('TV','MOVIE','ONA','OVA','SPECIAL','TV_SHORT')
                AND a.duration > 0 AND a.episodes > 0
                AND a.year BETWEEN 1985 AND 2025
            GROUP BY a.id
        """).fetchall()

        if not rows:
            return ReportSection(
                title="フォーマット別スタッフ需要プロファイル",
                findings_html="<p>フォーマットデータが利用できません。</p>",
                section_id="format_profile",
            )

        fmt_metrics: dict[str, dict[str, list[float]]] = defaultdict(
            lambda: {"pers_per_hr": [], "pers_per_cour": []}
        )
        for r in rows:
            fmt = r["format"]
            dur = r["duration"]
            eps = r["episodes"]
            pers = r["unique_persons"]
            total_hr = (dur * eps) / 60.0
            cour = eps / 12.0
            if total_hr > 0:
                fmt_metrics[fmt]["pers_per_hr"].append(pers / total_hr)
            if cour > 0:
                fmt_metrics[fmt]["pers_per_cour"].append(pers / cour)

        formats = ["MOVIE", "TV", "ONA", "OVA"]
        labels = {"MOVIE": "映画", "TV": "TV", "ONA": "ONA", "OVA": "OVA"}
        colors = {"MOVIE": "#F72585", "TV": "#4CC9F0", "ONA": "#06D6A0", "OVA": "#FFD166"}

        present = [f for f in formats if fmt_metrics[f]["pers_per_hr"]]
        fig = go.Figure()
        for f in present:
            vals = fmt_metrics[f]["pers_per_hr"]
            med = _stats.median(vals) if vals else 0
            fig.add_trace(go.Bar(
                x=[labels.get(f, f)],
                y=[med],
                name=labels.get(f, f),
                marker_color=colors.get(f, "#888"),
                text=[f"{med:.1f}"],
                textposition="outside",
            ))
        fig.update_layout(
            title_text="フォーマット別スタッフ密度（中央値 人/時間）",
            yaxis_title="人/時間（中央値）",
        )

        findings_parts: list[str] = []
        for f in present:
            vals = fmt_metrics[f]["pers_per_hr"]
            med = _stats.median(vals)
            findings_parts.append(f"{labels.get(f, f)}: 中央値 {med:.1f}人/時間（n={len(vals)}）")
        findings = "<p>" + "; ".join(findings_parts) + "。</p>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="フォーマット別スタッフ需要プロファイル",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "format-profile-chart", height=440),
            method_note=(
                "persons_per_hr = unique_persons / (duration × episodes / 60)。"
                "フォーマット別に該当作品全件の中央値を集計。"
                "duration=0 または episodes=0 のフォーマットは除外。"
            ),
            section_id="format_profile",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 8: Startup cost (Charts 19-24 condensed)
    # ═══════════════════════════════════════════════════════════════

    def _build_startup_cost_section(
        self, sb: SectionBuilder,
    ) -> ReportSection:
        try:
            return self._startup_impl(sb)
        except Exception as e:
            return ReportSection(
                title="固定費 vs 変動費構造",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="startup_cost",
            )

    def _startup_impl(self, sb: SectionBuilder) -> ReportSection:
        # Per-anime startup vs variable
        rows = self.conn.execute("""
            SELECT a.id, a.episodes, c.role,
                   COUNT(DISTINCT c.person_id) AS persons
            FROM credits c
            JOIN anime a ON c.anime_id = a.id
            WHERE a.format IN ('TV', 'ONA')
              AND a.episodes >= 4
              AND a.year BETWEEN 1985 AND 2025
              AND c.role IS NOT NULL
            GROUP BY a.id, c.role
        """).fetchall()

        if not rows:
            return ReportSection(
                title="固定費 vs 変動費構造",
                findings_html="<p>固定費データが利用できません。</p>",
                section_id="startup_cost",
            )

        anime_data: dict[str, dict[str, Any]] = {}
        for r in rows:
            aid = str(r["id"])
            if aid not in anime_data:
                cour = max(1, round((r["episodes"] or 12) / 12))
                anime_data[aid] = {"cour": cour, "startup": 0, "variable": 0}
            role = r["role"]
            if role in _STARTUP_ROLES:
                anime_data[aid]["startup"] += r["persons"]
            elif role in _VARIABLE_ROLES:
                anime_data[aid]["variable"] += r["persons"]

        cour_bins: dict[str, list[float]] = {
            "1cour": [], "2cour": [], "3cour+": [],
        }
        for info in anime_data.values():
            s = info["startup"]
            if s <= 0:
                continue
            cc = info["cour"]
            if cc <= 1:
                cour_bins["1cour"].append(s)
            elif cc <= 2:
                cour_bins["2cour"].append(s)
            else:
                cour_bins["3cour+"].append(s)

        fig = go.Figure()
        bin_colors = {"1cour": "#4CC9F0", "2cour": "#FFD166", "3cour+": "#FF6B35"}
        for bn, vals in cour_bins.items():
            if vals:
                fig.add_trace(go.Box(
                    y=vals[:500],
                    name=f"{bn} (n={len(vals)})",
                    marker_color=bin_colors.get(bn, "#888"),
                    boxmean=True,
                ))
        fig.update_layout(
            title_text="クール数別の固定費（固定チーム規模）",
            yaxis_title="ユニーク人数（初期役職）",
        )

        findings_parts = []
        for bn, vals in cour_bins.items():
            if vals:
                med = _stats.median(vals)
                findings_parts.append(f"{bn}: 中央値 {med:.0f}人（n={len(vals)}）")
        findings = (
            "<p>固定チーム（初期役職: 監督、キャラクターデザイナー、音楽など）の"
            "クール数別内訳。" + "; ".join(findings_parts) + "。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="固定費 vs 変動費構造",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "startup-cost-chart", height=480),
            method_note=(
                "初期役職: director、screenplay、character_designer、"
                "background_art、music、original_creator、producer、sound_director。"
                "変動役職: key_animator、in_between、animation_director、"
                "episode_director、photography_director、background_art。"
            ),
            section_id="startup_cost",
        )

    # ═══════════════════════════════════════════════════════════════
    # Section 9: Causal inference (Charts 26-29 condensed)
    # ═══════════════════════════════════════════════════════════════

    def _build_causal_section(
        self,
        sb: SectionBuilder,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        try:
            return self._causal_impl(sb, scores_by_pid)
        except Exception as e:
            return ReportSection(
                title="因果推論 -- スタジオ規模効果",
                findings_html=f"<p>チャート生成エラー: {e}</p>",
                section_id="causal",
            )

    def _causal_impl(
        self,
        sb: SectionBuilder,
        scores_by_pid: dict[str, dict],
    ) -> ReportSection:
        # Build per-person, per-year primary studio tier
        rows = self.conn.execute("""
            SELECT c.person_id, a.year, s.favourites,
                   COUNT(DISTINCT c.anime_id) AS works
            FROM credits c
            JOIN anime a ON c.anime_id = a.id
            JOIN anime_studios ast ON a.id = ast.anime_id AND ast.is_main = 1
            JOIN studios s ON ast.studio_id = s.id
            WHERE a.year BETWEEN 1990 AND 2024 AND s.favourites IS NOT NULL
            GROUP BY c.person_id, a.year, s.id
        """).fetchall()

        if not rows:
            return ReportSection(
                title="因果推論 -- スタジオ規模効果",
                findings_html="<p>スタジオ移籍データが利用できません。</p>",
                section_id="causal",
            )

        def _tier(f: int) -> str:
            if f >= 1000:
                return "大手"
            return "中規模" if f >= 100 else "小規模"

        # Aggregate per (person, year)
        ppy: dict[str, dict[int, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(int))
        )
        for r in rows:
            t = _tier(r["favourites"] or 0)
            ppy[str(r["person_id"])][int(r["year"])][t] += r["works"]

        # Person-year dominant tier
        py_tier: dict[tuple[str, int], str] = {}
        for pid, yr_data in ppy.items():
            for yr, tier_cnt in yr_data.items():
                py_tier[(pid, yr)] = max(tier_cnt, key=tier_cnt.get)

        # Detect transitions
        person_tiers: dict[str, list[tuple[int, str]]] = {}
        for (pid, yr), tier in py_tier.items():
            person_tiers.setdefault(pid, []).append((yr, tier))

        # Event study
        transitions: dict[str, tuple[int, str]] = {}
        for pid, yr_tiers in person_tiers.items():
            if len(yr_tiers) < 4:
                continue
            srt = sorted(yr_tiers)
            for i in range(1, len(srt)):
                if srt[i - 1][1] != "大手" and srt[i][1] == "大手":
                    transitions[pid] = (srt[i][0], "up")
                    break
                if srt[i - 1][1] == "大手" and srt[i][1] != "大手":
                    transitions[pid] = (srt[i][0], "down")
                    break

        # Build event study data
        credits_by_py: dict[tuple[str, int], int] = {}
        for (pid, yr), tier in py_tier.items():
            credits_by_py[(pid, yr)] = credits_by_py.get((pid, yr), 0) + 1

        event_data: dict[str, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))
        for pid, (trans_yr, direction) in transitions.items():
            for t_rel in range(-4, 5):
                cred = credits_by_py.get((pid, trans_yr + t_rel))
                if cred is not None:
                    event_data[direction][t_rel].append(cred)

        t_rels = list(range(-4, 5))
        line_specs = [
            ("up", "小/中規模 → 大手", "#F72585"),
            ("down", "大手 → 小/中規模", "#FF6B35"),
        ]

        fig = go.Figure()
        for key, label, color in line_specs:
            grp = event_data.get(key, {})
            ys: list[float] = []
            yerr: list[float] = []
            valid_t: list[int] = []
            for t in t_rels:
                vals = grp.get(t, [])
                if len(vals) >= 5:
                    ys.append(_stats.median(vals))
                    try:
                        s = _stats.stdev(vals)
                        yerr.append(1.96 * s / (len(vals) ** 0.5))
                    except Exception:
                        yerr.append(0)
                    valid_t.append(t)
            if ys:
                fig.add_trace(go.Scatter(
                    x=valid_t, y=ys, name=label,
                    line=dict(color=color, width=2.5),
                    mode="lines+markers",
                    error_y=dict(
                        type="data", array=yerr, visible=True,
                        color=color, thickness=1.5, width=4,
                    ),
                ))
        fig.add_vline(x=0, line_dash="dash", line_color="gray",
                       annotation_text="遷移年")
        fig.update_layout(
            title_text="イベントスタディ: スタジオ規模遷移前後のクレジット",
            xaxis_title="遷移からの相対年数",
            yaxis_title="年間クレジット中央値（± 95% CI）",
            legend=dict(orientation="h", y=-0.15),
        )

        n_up = sum(1 for _, (_, d) in transitions.items() if d == "up")
        n_down = sum(1 for _, (_, d) in transitions.items() if d == "down")

        findings = (
            f"<p>イベントスタディ: 上方遷移n={n_up}件"
            f"（小/中規模→大規模）、下方遷移n={n_down}件。 "
            "遷移年（t=0）を基準にt=-4からt=+4の"
            "年間クレジット中央値をプロット。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="因果推論 -- スタジオ規模効果",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "event-study-chart", height=500),
            method_note=(
                "スタジオ規模は anime_studios.favourites に基づく: "
                "大手 (≥1000)、中規模 (100–999)、小規模 (<100)。"
                "各暦年における人物ごとの主要規模を算出。"
                "イベントスタディ: 相対時間 t におけるクレジット中央値、"
                "95% CI = 1.96 × stdev / √n。"
                "因果識別は限定的: 遷移への自己選抜バイアスが存在する。"
            ),
            interpretation_html=(
                "<p>上方遷移後にクレジットが減少する場合、大手スタジオでの"
                "長期雇用が可視クレジット数を減らすという説明と整合的である。"
                "別解釈として、上方遷移する人物が他の理由（年齢、専門化）で"
                "偶然にも総アウトプットを減らしている可能性もある。"
                "スタジオ遷移に対する操作変数がないため、因果識別は不完全。</p>"
            ),
            section_id="causal",
        )

"""国際共同制作 edge 構造分析レポート — v2 compliant.

JP ↔ CJK / SE-Asia スタッフの協業 edge 時系列、役職別海外比率、
Louvain community detection + null model permutation を可視化する。

Method overview:
- nationality_resolver: country_of_origin (high) → name_zh/ko 推定 (medium) → unknown
- 役職別海外比率: delegation_roles (動画/仕上げ等) vs creative_lead_roles (原画/作監等)
  の外国籍クレジット比率を年別に集計
- JP-CN / JP-KR / JP-SE_ASIA の協業 edge 密度: 同一作品での共クレジット edge 数 ÷ anime 数
- 動画 → 原画 transition: 海外国籍人物が delegation role から creative lead role へ
  移行した比率 (時系列制約あり: 初回 delegation 年以降のみカウント)
- Louvain community detection (resolution=1.0, seed=42)
- null model permutation test: 非 JP ノードの group ラベルをランダム置換し
  observed_modularity >= null_modularity の割合を p 値とする

Honest gaps (Hard constraint §50):
- credits 漏れバイアス: ANN / AniList は海外スタジオのクレジットを under-report する傾向
- 表記ゆれ: CJK 名表記の揺れによる過小推定リスク (19-02 cluster fix 済だが残存可能性あり)
- null 値 country_of_origin が多い場合、海外比率は下方バイアス

Framing (H2 compliance):
  "海外協業比率" "役職別海外配分" のみ使用。
  雇用喪失フレームおよび取引系フレームは禁止 (カード仕様 §Hard constraints)。
  個人への framing は一切なし。

Audience: policy (primary), biz (secondary)
"""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path

import plotly.graph_objects as go
import structlog

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# Minimum data points for a time-series to be shown (prevents noise plots).
_MIN_YEAR_POINTS = 3

# Country-pair display labels (mirrors international_collab.py constants).
_PAIR_LABELS: dict[str, str] = {
    "JP-CN": "JP × 中国語圏 (CN/TW/HK)",
    "JP-KR": "JP × 韓国 (KR)",
    "JP-SE_ASIA": "JP × 東南アジア",
    "JP-OTHER": "JP × その他海外",
}

# Colors per pair
_PAIR_COLORS: dict[str, str] = {
    "JP-CN": "#E09BC2",
    "JP-KR": "#3BC494",
    "JP-SE_ASIA": "#F8EC6A",
    "JP-OTHER": "#FFB444",
}

# Colors per role group
_ROLE_COLORS: dict[str, str] = {
    "delegation_roles": "#7CC8F2",
    "creative_lead_roles": "#F08060",
    "all": "#A0A0B8",
}

_ROLE_LABELS: dict[str, str] = {
    "delegation_roles": "委託系役職 (動画・仕上・撮影等)",
    "creative_lead_roles": "クリエイティブ主導役職 (原画・作監・監督等)",
    "all": "全役職",
}

# Country group display labels
_GROUP_LABELS: dict[str, str] = {
    "JP": "国内 (JP)",
    "CN": "中国語圏 (CN/TW/HK)",
    "KR": "韓国 (KR)",
    "SE_ASIA": "東南アジア",
    "OTHER": "その他海外",
    "UNKNOWN": "国籍不明",
}

_GROUP_COLORS: dict[str, str] = {
    "JP": "#7CC8F2",
    "CN": "#E09BC2",
    "KR": "#3BC494",
    "SE_ASIA": "#F8EC6A",
    "OTHER": "#FFB444",
    "UNKNOWN": "#808090",
}


class StructureInternationalReport(BaseReportGenerator):
    """国際共同制作 edge 構造分析レポート.

    JP ↔ CJK / SE-Asia 協業構造を時系列・役職別・community 構造で記述する。
    個人の主観的評価は行わない。海外協業の構造的事実を可視化する。

    Policy brief audience (primary).  Business brief (secondary).
    """

    name = "structure_international"
    title = "国際共同制作 edge 構造分析"
    subtitle = (
        "JP ↔ CJK / SE-Asia 協業 edge 時系列 "
        "/ 役職別海外比率 "
        "/ Louvain community detection + null model permutation"
    )
    filename = "structure_international.html"
    doc_type = "main"

    def generate(self) -> Path | None:
        from src.analysis.network.international_collab import (
            InternationalCollabResult,
            analyze_international_collab,
        )

        sb = SectionBuilder()

        result: InternationalCollabResult = analyze_international_collab(
            self.conn,
            perm_rounds=199,  # reduced for report generation speed; use 999 for publication
            louvain_resolution=1.0,
            min_community_size=3,
        )

        coverage_note = self._build_coverage_note(result)

        if result.low_coverage_warning:
            log.warning(
                "structure_international_low_coverage",
                note=result.coverage_note,
            )

        sections = [
            self._build_yearly_ratio_section(sb, result, coverage_note),
            self._build_pair_density_section(sb, result, coverage_note),
            self._build_role_progression_section(sb, result, coverage_note),
            self._build_community_section(sb, result, coverage_note),
        ]

        interpretation_html = self._build_interpretation(result)
        overview_html = self._build_overview(result)

        insert_lineage(
            self.conn,
            table_name="meta_structure_international",
            audience="policy",
            source_silver_tables=["credits", "persons", "anime"],
            formula_version="v1.0",
            ci_method=(
                "外国籍比率: 比率の 95% CI = Wilson score interval (n ≥ 5 セルのみ)。"
                "edge 密度: 点推定のみ (n < 5 anime のセルは非表示)。"
                "role transition rate: 比率のみ、n < 5 は non-applicable。"
            ),
            null_model=(
                "Permutation test: 非 JP ノードの country group ラベルをランダム置換 "
                "(n_rounds=199, seed=42)。"
                "帰無仮説: cross-border group 構造が Louvain modularity に寄与しない。"
                "p = observed_mod >= null_mod の割合 (one-tailed)。"
            ),
            holdout_method="Not applicable (descriptive analysis of observed credit records)",
            description=(
                "International collaboration edge structure analysis: "
                "yearly foreign-participation ratios by role group "
                "(delegation_roles: in_between/finishing/photography etc.; "
                "creative_lead_roles: key_animator/animation_director/director etc.), "
                "JP–foreign co-credit edge density per country pair per year, "
                "delegation-to-creative-lead role transition rates by country group, "
                "Louvain community detection with null-model permutation test. "
                "Country resolution: country_of_origin (high confidence) → "
                "name_zh/name_ko inference (medium confidence) → unknown (low). "
                "Credits under-reporting bias for overseas studios "
                "(ANN/AniList source limitation) documented. "
                "No viewer ratings used. "
                "Framing: 'overseas collaboration ratio', 'role distribution by country group'; "
                "not 'hollowing-out' or 'outsourcing'. "
                "CJK homonym guard: 19-02 cluster fix applied upstream."
            ),
            rng_seed=42,
        )

        return self.render_unified_structure(
            sections=sections,
            overview_html=overview_html,
            interpretation_html=interpretation_html,
            meta_table="meta_structure_international",
            extra_glossary=_GLOSSARY,
        )

    # ------------------------------------------------------------------
    # Coverage note
    # ------------------------------------------------------------------

    def _build_coverage_note(self, result: "InternationalCollabResult") -> str:
        warning = ""
        if result.low_coverage_warning:
            warning = (
                '<span style="color:#e05080;font-weight:bold;">'
                "⚠ 海外人材カバレッジが低い可能性あり。"
                "海外比率は下方バイアスを持つ可能性がある。"
                "</span> "
            )
        return (
            f'<p style="color:#e09050;font-size:0.85rem;">'
            f"[データ品質] {warning}"
            f"{result.coverage_note}"
            f"credits 漏れバイアス: ANN/AniList は海外スタジオのクレジットを"
            f"under-report する傾向があるため、海外比率は過小推定の可能性がある。"
            f"</p>"
        )

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------

    def _build_overview(self, result: "InternationalCollabResult") -> str:
        n_ratio_years = len({r.year for r in result.yearly_ratios})
        n_pairs_with_data = len({d.pair for d in result.pair_densities if d.n_anime > 0})
        n_communities = len(result.communities)
        perm_str = ""
        if result.perm_test:
            perm_str = (
                f"Louvain community の cross-border クラスター信号: "
                f"p = {result.perm_test.p_value:.3f} "
                f"(permutation test, n_rounds={result.perm_test.n_rounds})。"
            )

        return (
            "<p>本レポートは、アニメーション業界クレジットデータから"
            "JP 国内スタッフと中国語圏 (CN/TW/HK) · 韓国 (KR) · 東南アジア "
            "スタッフの協業 edge 構造を時系列で記述する。</p>"
            f"<p>分析対象年: {n_ratio_years} 年分。"
            f"国別ペア: {n_pairs_with_data} ペアにデータあり。"
            f"Louvain community 検出: {n_communities} コミュニティ。"
            f"{perm_str}</p>"
            "<p>全指標は公開クレジットデータに基づく構造的記述であり、"
            "個人・スタジオの主観的評価を意味しない。</p>"
        )

    # ------------------------------------------------------------------
    # Section 1: Yearly foreign-participation ratio
    # ------------------------------------------------------------------

    def _build_yearly_ratio_section(
        self,
        sb: SectionBuilder,
        result: "InternationalCollabResult",
        coverage_note: str,
    ) -> ReportSection:
        from src.analysis.network.international_collab import YearlyForeignRatio

        ratios = result.yearly_ratios
        if not ratios:
            return ReportSection(
                title="役職別 海外参加比率の時系列",
                findings_html=(
                    "<p>役職別海外比率のデータが取得できませんでした。"
                    "credits と persons テーブルのデータ充足が必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "外国籍比率 = n_foreign_credits / (n_total - n_unknown)。"
                    "役職グループ: 委託系 (in_between/仕上/撮影等) / "
                    "クリエイティブ主導 (原画/作監/監督等) / 全役職。"
                    "n < 5 のセルは非表示。視聴者評価不使用。"
                ),
                section_id="yearly_ratio",
            )

        # Build line chart per role_group
        fig = go.Figure()
        groups_shown: dict[str, list[tuple[int, float]]] = {}

        for r in ratios:
            if r.foreign_ratio is None:
                continue
            key = r.role_group
            if key not in groups_shown:
                groups_shown[key] = []
            groups_shown[key].append((r.year, r.foreign_ratio))

        any_data = False
        for rg, pts in sorted(groups_shown.items()):
            if len(pts) < _MIN_YEAR_POINTS:
                continue
            any_data = True
            pts_sorted = sorted(pts, key=lambda x: x[0])
            years = [p[0] for p in pts_sorted]
            vals = [p[1] * 100 for p in pts_sorted]
            color = _ROLE_COLORS.get(rg, "#888888")
            label = _ROLE_LABELS.get(rg, rg)
            fig.add_trace(go.Scatter(
                x=years, y=vals,
                mode="lines+markers",
                name=label,
                line=dict(color=color, width=2),
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{label}</b><br>"
                    "年=%{x}<br>海外比率=%{y:.1f}%<extra></extra>"
                ),
            ))

        if not any_data:
            return ReportSection(
                title="役職別 海外参加比率の時系列",
                findings_html=(
                    f"<p>各役職グループで {_MIN_YEAR_POINTS} 年以上のデータが揃った"
                    "セルがありませんでした。</p>" + coverage_note
                ),
                method_note=(
                    "外国籍比率 = n_foreign_credits / (n_total - n_unknown)。"
                    "最低 n=5 クレジット、最低 3 年分のデータが必要。"
                    "視聴者評価不使用。"
                ),
                section_id="yearly_ratio",
            )

        fig.update_layout(
            title="役職グループ別 海外参加比率 (%) — 年推移",
            xaxis_title="年",
            yaxis_title="海外参加比率 (%)",
            yaxis=dict(range=[0, None]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480,
        )

        # Findings text: summary statistics
        findings_parts: list[str] = []
        for rg, pts in sorted(groups_shown.items()):
            if len(pts) < _MIN_YEAR_POINTS:
                continue
            pts_sorted = sorted(pts, key=lambda x: x[0])
            first_yr, first_val = pts_sorted[0]
            last_yr, last_val = pts_sorted[-1]
            label = _ROLE_LABELS.get(rg, rg)
            direction = "上昇" if last_val > first_val else "低下"
            findings_parts.append(
                f"<li><strong>{label}</strong>: "
                f"{first_yr} 年 {first_val*100:.1f}% → "
                f"{last_yr} 年 {last_val*100:.1f}% ({direction})</li>"
            )

        findings_html = (
            "<p>役職グループ別の海外参加比率の年推移を示す。"
            "委託系役職 (in_between / 仕上 / 撮影等) と"
            "クリエイティブ主導役職 (原画 / 作監 / 監督等) を分けて集計。</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="役職別 海外参加比率の時系列",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_yearly_ratio", height=480),
            method_note=(
                "外国籍比率 = n_foreign_credits / (n_total_credits - n_unknown_credits)。"
                "国籍不明クレジットは分母から除外し量を明示。"
                "役職グループ分類: "
                "委託系 = in_between, in_between_check, photography, finishing, cg, second_key_animator; "
                "クリエイティブ主導 = director, series_director, animation_director, "
                "chief_animation_director, character_design, art_director, key_animator, "
                "storyboard, episode_director。"
                "セル n < 5 は除外。3 年未満の系列は非表示。"
                "credits 漏れバイアス: 海外スタジオは ANN/AniList で under-credited の傾向。"
                "視聴者評価不使用。"
            ),
            section_id="yearly_ratio",
        )

    # ------------------------------------------------------------------
    # Section 2: Collab pair edge density
    # ------------------------------------------------------------------

    def _build_pair_density_section(
        self,
        sb: SectionBuilder,
        result: "InternationalCollabResult",
        coverage_note: str,
    ) -> ReportSection:
        densities = result.pair_densities
        if not densities:
            return ReportSection(
                title="国別ペア 協業 edge 密度の時系列",
                findings_html=(
                    "<p>国別ペア密度データが取得できませんでした。</p>"
                    + coverage_note
                ),
                method_note=(
                    "協業 edge 密度 = JP × 外国籍ペア共クレジット edge 数 / 混合 cast anime 数。"
                    "「混合 cast」= JP 人物と当該国籍グループ人物が 1 人以上いる anime。"
                    "視聴者評価不使用。"
                ),
                section_id="pair_density",
            )

        fig = go.Figure()
        pair_pts: dict[str, list[tuple[int, float]]] = {}

        for d in densities:
            if d.edges_per_anime is None:
                continue
            if d.pair not in pair_pts:
                pair_pts[d.pair] = []
            pair_pts[d.pair].append((d.year, d.edges_per_anime))

        any_data = False
        for pair, pts in sorted(pair_pts.items()):
            if len(pts) < _MIN_YEAR_POINTS:
                continue
            any_data = True
            pts_sorted = sorted(pts, key=lambda x: x[0])
            years = [p[0] for p in pts_sorted]
            epan = [p[1] for p in pts_sorted]
            color = _PAIR_COLORS.get(pair, "#888888")
            label = _PAIR_LABELS.get(pair, pair)
            fig.add_trace(go.Scatter(
                x=years, y=epan,
                mode="lines+markers",
                name=label,
                line=dict(color=color, width=2),
                marker=dict(size=5),
                hovertemplate=(
                    f"<b>{label}</b><br>"
                    "年=%{x}<br>edge/anime=%{y:.2f}<extra></extra>"
                ),
            ))

        if not any_data:
            return ReportSection(
                title="国別ペア 協業 edge 密度の時系列",
                findings_html=(
                    f"<p>{_MIN_YEAR_POINTS} 年以上のデータを持つ"
                    "ペアがありませんでした。</p>" + coverage_note
                ),
                method_note=(
                    "協業 edge 密度 = co-credit edge 数 / mixed-cast anime 数。"
                    "視聴者評価不使用。"
                ),
                section_id="pair_density",
            )

        fig.update_layout(
            title="国別ペア 協業 edge 密度 (edges per anime) — 年推移",
            xaxis_title="年",
            yaxis_title="co-credit edges / anime",
            yaxis=dict(range=[0, None]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480,
        )

        findings_parts: list[str] = []
        for pair, pts in sorted(pair_pts.items()):
            if len(pts) < _MIN_YEAR_POINTS:
                continue
            pts_sorted = sorted(pts, key=lambda x: x[0])
            first_yr, first_epan = pts_sorted[0]
            last_yr, last_epan = pts_sorted[-1]
            label = _PAIR_LABELS.get(pair, pair)
            direction = "増加" if last_epan > first_epan else "減少"
            findings_parts.append(
                f"<li><strong>{label}</strong>: "
                f"{first_yr} 年 {first_epan:.2f} → "
                f"{last_yr} 年 {last_epan:.2f} ({direction})</li>"
            )

        findings_html = (
            "<p>同一作品で JP スタッフと各国籍グループのスタッフが"
            "共クレジットされる頻度 (anime 1 本あたりの edge 数) を国別ペアごとに示す。</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="国別ペア 協業 edge 密度の時系列",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_pair_density", height=480),
            method_note=(
                "協業 edge 密度 = 当該年に JP 人物と外国籍グループ人物が"
                "同一 anime で共クレジットされた edge 数 ÷ "
                "JP × 外国籍グループ混合 cast を持つ anime 数。"
                "edge は無向 (p1, p2) のペアとしてカウント (重複なし)。"
                "「国籍グループ混合 cast」= JP 人物と当該グループ人物が"
                "それぞれ 1 人以上いる anime。"
                "国籍不明人物は JP でも外国籍でもない扱いのため"
                "edge カウントに含まれない。"
                "視聴者評価不使用。"
            ),
            section_id="pair_density",
        )

    # ------------------------------------------------------------------
    # Section 3: Role progression rates
    # ------------------------------------------------------------------

    def _build_role_progression_section(
        self,
        sb: SectionBuilder,
        result: "InternationalCollabResult",
        coverage_note: str,
    ) -> ReportSection:
        progressions = result.role_progressions
        if not progressions:
            return ReportSection(
                title="委託系役職 → クリエイティブ主導役職 移行率",
                findings_html=(
                    "<p>役職移行データが取得できませんでした。</p>"
                    + coverage_note
                ),
                method_note=(
                    "委託系役職での初回クレジット年以降に"
                    "クリエイティブ主導役職でクレジットされた人物の比率。"
                    "視聴者評価不使用。"
                ),
                section_id="role_progression",
            )

        # Bar chart
        groups_with_data = [p for p in progressions if p.transition_rate is not None]
        if not groups_with_data:
            return ReportSection(
                title="委託系役職 → クリエイティブ主導役職 移行率",
                findings_html=(
                    "<p>有効な国籍グループが存在しませんでした "
                    "(n < 5 のグループを除外)。</p>" + coverage_note
                ),
                method_note=(
                    "委託系役職での初回クレジット年以降に"
                    "クリエイティブ主導役職でクレジットされた人物の比率。"
                    "n < 5 のグループは除外。視聴者評価不使用。"
                ),
                section_id="role_progression",
            )

        groups_sorted = sorted(groups_with_data, key=lambda p: p.n_total, reverse=True)
        group_labels = [_GROUP_LABELS.get(p.group, p.group) for p in groups_sorted]
        rates = [(p.transition_rate or 0) * 100 for p in groups_sorted]
        colors = [_GROUP_COLORS.get(p.group, "#888888") for p in groups_sorted]
        hover_texts = [
            f"<b>{_GROUP_LABELS.get(p.group, p.group)}</b><br>"
            f"移行率={p.transition_rate*100:.1f}%<br>"
            f"移行あり={p.n_transitioned:,}, 移行なし={p.n_delegation_only:,}, "
            f"合計={p.n_total:,}"
            for p in groups_sorted
        ]

        fig = go.Figure(go.Bar(
            x=group_labels,
            y=rates,
            marker_color=colors,
            text=[f"{r:.1f}%" for r in rates],
            textposition="outside",
            customdata=hover_texts,
            hovertemplate="%{customdata}<extra></extra>",
        ))
        fig.update_layout(
            title="国籍グループ別 委託系役職 → クリエイティブ主導役職 移行率",
            xaxis_title="国籍グループ",
            yaxis_title="移行率 (%)",
            yaxis=dict(range=[0, max(rates) * 1.2 + 5 if rates else 100]),
            height=420,
        )

        findings_parts = []
        for p in groups_sorted:
            label = _GROUP_LABELS.get(p.group, p.group)
            note_str = f" [{p.note}]" if p.note else ""
            if p.transition_rate is not None:
                findings_parts.append(
                    f"<li><strong>{label}</strong>: "
                    f"移行率 {p.transition_rate*100:.1f}% "
                    f"(移行あり {p.n_transitioned:,} / 委託系スタート {p.n_total:,})"
                    f"{note_str}</li>"
                )

        findings_html = (
            "<p>委託系役職 (in_between / 仕上 / 撮影等) を初回クレジットとして持つ人物のうち、"
            "その後クリエイティブ主導役職 (原画 / 作監 / 監督等) で"
            "クレジットされた比率を国籍グループ別に示す。</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="委託系役職 → クリエイティブ主導役職 移行率",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_role_progression", height=420),
            method_note=(
                "移行率の定義: 委託系役職 (in_between / in_between_check / photography / "
                "finishing / cg / second_key_animator) での初回クレジット年 T_deleg 以降に、"
                "クリエイティブ主導役職 (director / series_director / animation_director / "
                "chief_animation_director / character_design / art_director / key_animator / "
                "storyboard / episode_director) でクレジットされた人物の比率。"
                "T_deleg 以前のクリエイティブ主導クレジットはカウントしない。"
                "国籍解決: country_of_origin (high confidence) "
                "+ name_zh/name_ko 推定 (medium confidence)。"
                "n < 5 のグループは除外。"
                "視聴者評価不使用。"
                "credits 漏れバイアス適用 (海外スタジオは under-credited の傾向)。"
            ),
            section_id="role_progression",
        )

    # ------------------------------------------------------------------
    # Section 4: Community detection
    # ------------------------------------------------------------------

    def _build_community_section(
        self,
        sb: SectionBuilder,
        result: "InternationalCollabResult",
        coverage_note: str,
    ) -> ReportSection:
        communities = result.communities
        perm = result.perm_test

        if not communities:
            return ReportSection(
                title="Louvain community detection — cross-border クラスター",
                findings_html=(
                    "<p>コミュニティ検出データが取得できませんでした。"
                    "グラフ構築に十分なクレジットデータが必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Louvain (networkx.community.louvain_communities, "
                    "resolution=1.0, seed=42)。"
                    "null model: 非 JP ノードの group ラベルを "
                    "n_rounds 回ランダム置換 (seed=42)。"
                    "視聴者評価不使用。"
                ),
                section_id="community_detection",
            )

        # Bar chart: top-15 communities by international fraction
        top_comms = sorted(communities, key=lambda c: c.international_fraction, reverse=True)[:15]

        comm_labels = [f"Comm {c.community_id}" for c in top_comms]
        intl_fracs = [c.international_fraction * 100 for c in top_comms]
        sizes = [c.size for c in top_comms]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=comm_labels,
            y=intl_fracs,
            name="海外メンバー比率 (%)",
            marker_color="#E09BC2",
            customdata=sizes,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "海外メンバー比率=%{y:.1f}%<br>"
                "コミュニティサイズ=%{customdata}<extra></extra>"
            ),
        ))

        fig.update_layout(
            title=f"上位 {len(top_comms)} コミュニティの海外メンバー比率 (Louvain)",
            xaxis_title="コミュニティ",
            yaxis_title="海外メンバー比率 (%)",
            yaxis=dict(range=[0, 100]),
            height=420,
        )

        # Permutation test result text
        perm_html = ""
        if perm:
            sig_str = "p < 0.05 — 偶然水準以上の cross-border クラスター構造を検出" if perm.p_value < 0.05 \
                else "p ≥ 0.05 — 偶然水準内"
            perm_html = (
                f"<p>Null model permutation test (n_rounds={perm.n_rounds}, seed=42): "
                f"observed modularity = {perm.observed_modularity:.4f}, "
                f"p = {perm.p_value:.3f}。{sig_str}。"
                f"非 JP ノード数: {perm.n_international_nodes:,}。</p>"
            )
        else:
            perm_html = "<p>Permutation test は実行されませんでした (データ不足)。</p>"

        # Group composition summary for largest international community
        intl_comms = [c for c in communities if c.international_fraction > 0.1]
        comp_parts: list[str] = []
        for c in sorted(intl_comms, key=lambda x: x.international_fraction, reverse=True)[:3]:
            gcomp = ", ".join(
                f"{_GROUP_LABELS.get(g, g)}: {n}"
                for g, n in sorted(c.group_composition.items(), key=lambda x: x[1], reverse=True)
            )
            comp_parts.append(
                f"<li>Comm {c.community_id} (size={c.size}, "
                f"海外比率={c.international_fraction*100:.1f}%): {gcomp}</li>"
            )

        findings_html = (
            f"<p>Louvain community detection: {len(communities)} コミュニティを検出"
            f"(resolution=1.0, seed=42)。"
            f"うち海外メンバー比率 > 10% のコミュニティ: {len(intl_comms)} 件。</p>"
            + perm_html
            + (
                f"<p>海外比率上位コミュニティの国籍構成:</p>"
                f"<ul>{''.join(comp_parts)}</ul>"
                if comp_parts else ""
            )
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="Louvain community detection — cross-border クラスター",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_community_intl", height=420),
            method_note=(
                "Louvain アルゴリズム: networkx.community.louvain_communities "
                "(resolution=1.0, weight='weight', seed=42)。"
                "node group 属性: country_of_origin (high) または "
                "name_zh/name_ko 推定 (medium)。"
                "国際比率 = 非 JP 高/中信頼度メンバー数 / 既知メンバー数。"
                "Null model: 非 JP ノードの group ラベルを n_rounds 回ランダム置換 (seed=42)。"
                "p 値 = observed_modularity >= null_modularity の割合 (one-tailed)。"
                "コミュニティサイズ < 3 は除外。"
                "視聴者評価不使用。"
                "credits 漏れバイアス: 海外スタジオは under-credited のため "
                "cross-border edge は過小推定の可能性がある。"
            ),
            section_id="community_detection",
        )

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def _build_interpretation(self, result: "InternationalCollabResult") -> str:
        lines: list[str] = []

        # Ratio trend
        delegation_pts = sorted(
            [
                (r.year, r.foreign_ratio)
                for r in result.yearly_ratios
                if r.role_group == "delegation_roles" and r.foreign_ratio is not None
            ],
            key=lambda x: x[0],
        )
        if len(delegation_pts) >= 2:
            first_val = delegation_pts[0][1]
            last_val = delegation_pts[-1][1]
            direction = "増加傾向" if last_val > first_val else "減少傾向"
            lines.append(
                f"委託系役職の海外比率は {direction} にある "
                f"({delegation_pts[0][0]} 年 {first_val*100:.1f}% → "
                f"{delegation_pts[-1][0]} 年 {last_val*100:.1f}%)。"
            )

        # Perm test
        if result.perm_test and result.perm_test.p_value < 0.05:
            lines.append(
                f"Louvain community の cross-border クラスター構造は "
                f"偶然水準 (p={result.perm_test.p_value:.3f}) 以上に有意。"
            )

        if not lines:
            return ""

        return (
            f"<p>本分析の著者は、以下の構造的パターンを観察する: "
            f"{'　'.join(lines)}</p>"
            f"<p>代替解釈: "
            f"(a) 海外比率の変化は credits 漏れバイアスの経時変化を反映している可能性がある "
            f"(ANN/AniList のデータ拡充時期と一致する場合)。"
            f"(b) Louvain community の cross-border クラスターは、"
            f"作品ジャンルや制作規模のサンプルセレクションによる疑似クラスターである可能性がある。"
            f"(c) 役職移行率の差は country_of_origin カバレッジの偏り"
            f"(海外人材の一部が国籍不明として除外) を反映する可能性がある。</p>"
            f"<p>この解釈の前提: 国籍解決の精度 (high/medium confidence) および "
            f"credits データのサンプリング代表性。</p>"
        )


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

_GLOSSARY: dict[str, str] = {
    "委託系役職 (delegation_roles)": (
        "in_between / in_between_check / photography / finishing / cg / "
        "second_key_animator。"
        "主に制作工程の数量的処理を担う役職。"
    ),
    "クリエイティブ主導役職 (creative_lead_roles)": (
        "director / series_director / animation_director / "
        "chief_animation_director / character_design / art_director / "
        "key_animator / storyboard / episode_director。"
        "主に作品の創造的判断を担う役職。"
    ),
    "協業 edge 密度": (
        "同一 anime で JP 人物と外国籍グループ人物が"
        "共クレジットされた edge 数 ÷ 混合 cast anime 数。"
        "「混合 cast」= JP × 当該グループが 1 人以上いる anime。"
    ),
    "Louvain_modularity": (
        "コミュニティ構造の強度 (0–1)。高いほど内部 edge が外部 edge より密。"
        "resolution パラメータで粒度を調整。"
    ),
    "permutation_test_p_value": (
        "non-JP ノードの group ラベルをランダム置換したときの modularity >= "
        "observed modularity の割合。p < 0.05 で偶然水準以上の cross-border 構造。"
    ),
    "credits_under_reporting_bias": (
        "ANN / AniList は海外スタジオのクレジットを under-report する傾向がある。"
        "海外協業比率はこのバイアスにより過小推定の可能性がある。"
    ),
    "CJK_homonym_guard": (
        "19-02 cluster fix (commit f0d4547) 適用済。"
        "CJK 同姓異人問題 (LAN/李豪凌/Haoling 等) に対する"
        "canonical_id 解決が完了している。"
    ),
}


# v3 minimal SPEC
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="structure_international",
    audience="policy",
    claim=(
        "JP ↔ 海外国籍グループの協業 edge 密度が時系列で変化しており、"
        "Louvain community 検出では偶然水準以上の cross-border クラスター構造が"
        "permutation test で確認される"
    ),
    identifying_assumption=(
        "国籍解決: country_of_origin (high) → name_zh-ko (medium) → unknown (low)。"
        "credits データが production year を正確に反映している (漏れバイアスあり)。"
        "CJK 名寄せ: 19-02 cluster fix 適用済。"
    ),
    null_model=["N2", "N3"],
    sources=["credits", "persons", "anime"],
    meta_table="meta_structure_international",
    estimator="Louvain + permutation test (group-label shuffle)",
    ci_estimator="analytical_se",
    n_resamples=None,
    extra_limitations=[
        "credits 漏れバイアス: 海外スタジオは ANN/AniList で under-credited",
        "CJK 名表記ゆれによる過小推定リスク (19-02 cluster fix 済だが残存可能性あり)",
        "null 値 country_of_origin が多い場合、海外比率は下方バイアス",
    ],
)

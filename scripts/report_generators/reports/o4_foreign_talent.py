"""O4 海外人材ポジション分析 — v2 compliant.

国籍別 person FE 分布、国籍 × 役職進行、studio FE 帰属パターンを可視化し、
海外人材の業界内位置を構造的に記述する。Policy / Business brief 向け。

Method overview:
- nationality_resolver: country_of_origin (high) → name_zh/ko 推定 (medium) → unknown
- person FE 分布: 国籍グループ別 violin + Mann-Whitney U (分布差検定)
- 役職進行: O2 の progression_years 関数流用 + 国籍層別 KM curve + log-rank
- studio FE 帰属: 海外人材比率 × studio FE 散布図 (Pearson r, bootstrap CI)
- limited mobility bias: Andrews et al. (2008) — 海外人材はスタジオ間移動が少ない
  → person FE の推定精度が低下する可能性。全プロットにサンプルサイズ表示。

Framing (H2 compliance):
  Results are described as "FE distribution position" and "role-advancement
  timing difference by nationality group".  Viewer ratings excluded.
  All framing uses narrow structural descriptors only.

Audience: policy (primary), biz (secondary)
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..ci_utils import analytical_ci, bootstrap_ci, format_ci
from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# Minimum per group for violin / Mann-Whitney
_MIN_GROUP_N = 5
# Top N studios shown in scatter
_TOP_STUDIOS_N = 20
# Bootstrap samples for studio FE correlation CI
_N_BOOTSTRAP = 1000

# Nationality group display labels (JA)
_GROUP_LABELS: dict[str, str] = {
    "JP": "国内 (JP)",
    "CN": "中国語圏 (CN/TW/HK)",
    "KR": "韓国 (KR)",
    "SE_ASIA": "東南アジア",
    "OTHER": "その他海外",
    "UNKNOWN": "国籍不明",
}

# Color palette per group
_GROUP_COLORS: dict[str, str] = {
    "JP": "#a0d2db",
    "CN": "#f093fb",
    "KR": "#06D6A0",
    "SE_ASIA": "#FFD166",
    "OTHER": "#fda085",
    "UNKNOWN": "#808090",
}

# Pipeline pairs for KM analysis (borrowed from O2)
_PIPELINE_PAIRS: list[tuple[str, str, str]] = [
    ("in_between", "key_animator", "動画→原画"),
    ("key_animator", "animation_director", "原画→作監"),
    ("animation_director", "director", "作監→監督"),
]


# ---------------------------------------------------------------------------
# Inline data structures
# ---------------------------------------------------------------------------


@dataclass
class MannWhitneyNatResult:
    """Mann-Whitney U result for a nationality pair vs JP."""

    group: str
    pair_label: str
    u_statistic: float
    p_value: float
    effect_r: float
    n_group: int
    n_jp: int
    median_group: float | None
    median_jp: float | None


@dataclass
class NationalityKMResult:
    """KM progression curve for a nationality group within a pipeline pair."""

    group: str
    pair_label: str
    timeline: list[float]
    survival: list[float]
    ci_lower: list[float]
    ci_upper: list[float]
    n: int
    n_events: int
    logrank_vs_jp_p: float | None


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------


def _run_mannwhitney(
    fe_jp: list[float],
    fe_group: list[float],
    group_label: str,
) -> MannWhitneyNatResult | None:
    """Mann-Whitney U: group vs JP on person FE distribution."""
    if len(fe_jp) < _MIN_GROUP_N or len(fe_group) < _MIN_GROUP_N:
        return None
    try:
        from scipy.stats import mannwhitneyu
        import numpy as np

        u, p = mannwhitneyu(fe_group, fe_jp, alternative="two-sided")
        n_total = len(fe_group) + len(fe_jp)
        # Effect size r = |Z| / sqrt(n), Z approximated from U
        mean_u = len(fe_group) * len(fe_jp) / 2
        std_u = math.sqrt(len(fe_group) * len(fe_jp) * (n_total + 1) / 12)
        z = (u - mean_u) / std_u if std_u > 0 else 0.0
        effect_r = abs(z) / math.sqrt(n_total) if n_total > 0 else 0.0

        arr_g = [v for v in fe_group if math.isfinite(v)]
        arr_jp = [v for v in fe_jp if math.isfinite(v)]
        med_g = float(np.median(arr_g)) if arr_g else None
        med_jp = float(np.median(arr_jp)) if arr_jp else None

        return MannWhitneyNatResult(
            group=group_label,
            pair_label=f"{_GROUP_LABELS.get(group_label, group_label)} vs JP",
            u_statistic=float(u),
            p_value=float(p),
            effect_r=effect_r,
            n_group=len(fe_group),
            n_jp=len(fe_jp),
            median_group=med_g,
            median_jp=med_jp,
        )
    except Exception as exc:
        log.warning("mannwhitney_nat_failed", group=group_label, error=str(exc))
        return None


def _km_nationality_progression(
    conn: sqlite3.Connection,
    id_to_group: dict[str, str],
    role_from: str,
    role_to: str,
    pair_label: str,
) -> list[NationalityKMResult]:
    """Compute KM survival curves stratified by nationality group."""
    try:
        from src.analysis.career.role_progression import compute_progression_years
        from lifelines import KaplanMeierFitter
        from lifelines.statistics import logrank_test

        records = compute_progression_years(conn, role_from, role_to)
        if not records:
            return []

        # Assign group to each record
        groups_by_person: dict[str, str] = {}
        for rec in records:
            grp = id_to_group.get(rec.person_id, "UNKNOWN")
            groups_by_person[rec.person_id] = grp

        # Build durations/events per group
        from collections import defaultdict
        group_data: dict[str, tuple[list[float], list[bool]]] = defaultdict(lambda: ([], []))

        for rec in records:
            grp = groups_by_person.get(rec.person_id, "UNKNOWN")
            if grp == "UNKNOWN":
                continue
            dur = rec.duration_years if rec.duration_years is not None else 25.0
            event = rec.duration_years is not None
            group_data[grp][0].append(dur)
            group_data[grp][1].append(event)

        jp_durations, jp_events = group_data.get("JP", ([], []))

        results = []
        for group, (durations, events) in group_data.items():
            if len(durations) < _MIN_GROUP_N:
                continue

            kmf = KaplanMeierFitter()
            kmf.fit(durations, event_observed=events, label=group)
            tl = list(kmf.timeline)
            sf = list(kmf.survival_function_[group])
            ci = kmf.confidence_interval_survival_function_
            ci_lo = list(ci.iloc[:, 0])
            ci_hi = list(ci.iloc[:, 1])

            # Log-rank vs JP
            lr_p: float | None = None
            if group != "JP" and len(jp_durations) >= _MIN_GROUP_N:
                try:
                    lr = logrank_test(jp_durations, durations, jp_events, events)
                    lr_p = float(lr.p_value)
                except Exception:
                    pass

            results.append(NationalityKMResult(
                group=group,
                pair_label=pair_label,
                timeline=tl,
                survival=sf,
                ci_lower=ci_lo,
                ci_upper=ci_hi,
                n=len(durations),
                n_events=sum(events),
                logrank_vs_jp_p=lr_p,
            ))

        return results
    except Exception as exc:
        log.warning("km_nationality_failed", pair=pair_label, error=str(exc))
        return []


def _hex_to_rgb(hex_color: str) -> str:
    """Convert #RRGGBB → 'R,G,B' for rgba() use."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"{r},{g},{b}"


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class O4ForeignTalentReport(BaseReportGenerator):
    """O4: 海外人材ポジション分析 — nationality × FE / role-progression / studio.

    Describes the structural position of foreign-national persons in the
    animation industry credit network.  Viewer ratings are excluded.
    All framing uses narrow structural descriptors.

    Policy brief audience (primary).  Business brief (secondary).
    """

    name = "o4_foreign_talent"
    title = "海外人材ポジション分析"
    subtitle = (
        "国籍別 person FE 分布 (Mann-Whitney U) "
        "/ 国籍 × 役職進行 KM curve "
        "/ studio FE 帰属パターン"
    )
    filename = "o4_foreign_talent.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        from src.analysis.network.nationality_resolver import (
            NationalitySummary,
            load_nationality_records,
            person_fe_by_nationality,
            studio_foreign_share,
            GROUP_DOMESTIC,
            GROUP_UNKNOWN,
            CONF_HIGH,
            CONF_MEDIUM,
        )

        sb = SectionBuilder()

        # -- Load nationality records
        nat_records = load_nationality_records(self.conn)
        summary = NationalitySummary.from_records(nat_records)

        coverage_note = self._build_coverage_note(summary)

        # -- Person FE by group
        fe_by_group = person_fe_by_nationality(self.conn, nat_records)

        # -- id → group mapping for KM
        id_to_group: dict[str, str] = {
            r.person_id: r.group
            for r in nat_records
            if r.confidence in (CONF_HIGH, CONF_MEDIUM)
        }

        # -- Studio foreign share
        studio_rows = studio_foreign_share(self.conn, nat_records)

        # -- Build sections
        sections: list[str] = [
            sb.build_section(
                self._build_fe_distribution_section(sb, fe_by_group, summary, coverage_note)
            ),
            sb.build_section(
                self._build_role_progression_section(sb, id_to_group, coverage_note)
            ),
            sb.build_section(
                self._build_studio_fe_section(sb, studio_rows, coverage_note)
            ),
        ]

        interpretation_html = self._build_interpretation(fe_by_group, summary)

        insert_lineage(
            self.conn,
            table_name="meta_o4_foreign_talent",
            audience="policy",
            source_silver_tables=["credits", "persons", "anime"],
            formula_version="v1.0",
            ci_method=(
                "Analytical 95% CI (SE = σ/√n) for group-level FE means; "
                "Bootstrap 95% CI (n=1000, seed=42) for studio foreign-share correlation; "
                "Greenwood formula for KM survival curves"
            ),
            null_model=(
                "Mann-Whitney U two-sided test (no null model required — rank-based); "
                "Log-rank test for KM group comparison; "
                "Limited mobility bias (Andrews et al. 2008): single-studio persons "
                "have reduced person-FE precision — identified by n_studios_per_person=1"
            ),
            holdout_method="Not applicable (descriptive analysis of observed credit records)",
            description=(
                "Foreign national position analysis (O4): "
                "person FE distribution by nationality group (CN/KR/SE-Asia vs JP), "
                "role-advancement KM curves stratified by nationality, "
                "studio-level foreign-national credit share vs studio FE. "
                "Nationality resolved from country_of_origin (high confidence), "
                "name_zh/name_ko (medium confidence), unknown (low confidence). "
                "Viewer ratings not used. "
                "Results describe structural network position differences, "
                "not individual evaluations or subjective assessments. "
                "Limited mobility bias (Andrews et al. 2008) applies: "
                "foreign nationals with fewer studio transitions have lower "
                "person-FE estimation precision."
            ),
            rng_seed=42,
        )

        intro_html = self._build_intro(summary)

        return self.write_report(
            "\n".join(sections),
            intro_html=intro_html,
            extra_glossary=_GLOSSARY,
        )

    # ------------------------------------------------------------------
    # Coverage note
    # ------------------------------------------------------------------

    def _build_coverage_note(self, summary: Any) -> str:
        return (
            f'<p style="color:#e09050;font-size:0.85rem;">'
            f"[データ品質] nationality カバレッジ: "
            f"高確信度 {summary.n_high_confidence:,} 人, "
            f"推定 (medium) {summary.n_medium_confidence:,} 人, "
            f"不明 {summary.n_low_confidence:,} 人 "
            f"(合計 {summary.total_persons:,} 人, 非不明率 {summary.coverage_pct:.1f}%)。"
            f"country_of_origin 欠損人物は name_zh / name_ko からの推定を適用。"
            f"推定は false positive リスクを持つ (CJK 在日人物など)。</p>"
        )

    # ------------------------------------------------------------------
    # Section 1: Person FE distribution by nationality
    # ------------------------------------------------------------------

    def _build_fe_distribution_section(
        self,
        sb: SectionBuilder,
        fe_by_group: dict[str, list[float]],
        summary: Any,
        coverage_note: str,
    ) -> ReportSection:
        import numpy as np

        if not fe_by_group:
            return ReportSection(
                title="国籍別 person FE 分布",
                findings_html=(
                    "<p>feat_person_scores テーブルが利用できないか、"
                    "国籍解決データが不足しています。</p>"
                    + coverage_note
                ),
                method_note=(
                    "person FE = AKM log(production_scale) 分解の個人固定効果 (theta_i)。"
                    "国籍グループ別集計。Mann-Whitney U (two-sided) で JP との分布差を検定。"
                    "Limited mobility bias (Andrews et al. 2008): 単一スタジオ人物の "
                    "FE 推定精度は低い。本節はネットワーク位置の記述であり個人評価ではない。"
                ),
                section_id="fe_distribution",
            )

        # Build violin plot
        fig = go.Figure()
        group_order = ["JP", "CN", "KR", "SE_ASIA", "OTHER"]
        group_order = [g for g in group_order if g in fe_by_group]

        for g in group_order:
            vals = fe_by_group[g]
            color = _GROUP_COLORS.get(g, "#888888")
            label = _GROUP_LABELS.get(g, g)
            fig.add_trace(go.Violin(
                y=vals,
                name=f"{label} (n={len(vals):,})",
                box_visible=True,
                meanline_visible=True,
                line_color=color,
                fillcolor=f"rgba({_hex_to_rgb(color)},0.35)",
                spanmode="soft",
                hovertemplate=(
                    f"<b>{label}</b><br>"
                    "person_fe=%{y:.3f}<extra></extra>"
                ),
            ))

        fig.add_hline(
            y=0.0,
            line_dash="dash",
            line_color="#808090",
            annotation_text="FE=0 (産業平均)",
            annotation_position="right",
        )
        fig.update_layout(
            title="国籍グループ別 person FE 分布 (violin + box)",
            yaxis_title="person FE (log production scale, AKM theta_i)",
            xaxis_title="国籍グループ",
            violingap=0.1,
            violinmode="group",
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        # Mann-Whitney U for each group vs JP
        fe_jp = fe_by_group.get("JP", [])
        mw_parts: list[str] = []
        for g in group_order:
            if g == "JP":
                continue
            result = _run_mannwhitney(fe_jp, fe_by_group[g], g)
            if result is None:
                continue
            direction = (
                "JP より高い"
                if (result.median_group or 0.0) > (result.median_jp or 0.0)
                else "JP より低い"
            )
            mw_parts.append(
                f"<li><strong>{_GROUP_LABELS.get(g, g)}</strong> "
                f"(n={result.n_group:,}): "
                f"中央値 FE = {result.median_group:.3f} ({direction}, JP 中央値 = {result.median_jp:.3f}), "
                f"U={result.u_statistic:.0f}, p={result.p_value:.4f}, "
                f"効果量 r={result.effect_r:.3f}</li>"
            )

        # Group-level CI (analytical)
        ci_parts: list[str] = []
        for g in group_order:
            vals = fe_by_group[g]
            arr = np.array([v for v in vals if math.isfinite(v)])
            if len(arr) < 2:
                continue
            mean_fe = float(np.mean(arr))
            ci_lo, ci_hi = analytical_ci(arr)
            ci_str = format_ci((ci_lo, ci_hi))
            ci_parts.append(
                f"<li>{_GROUP_LABELS.get(g, g)}: 平均 FE = {mean_fe:.3f} "
                f"95% CI {ci_str} (n={len(arr):,})</li>"
            )

        findings_html = (
            f"<p>person FE (AKM theta_i) の国籍グループ別分布を violin で示す。"
            f"全グループで n ≥ {_MIN_GROUP_N} 名が必要。</p>"
            f"<ul>{''.join(ci_parts)}</ul>"
        )
        if mw_parts:
            findings_html += (
                f"<p>各グループと JP の分布差 (Mann-Whitney U, two-sided):</p>"
                f"<ul>{''.join(mw_parts)}</ul>"
            )
        findings_html += coverage_note
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="国籍別 person FE 分布",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_fe_violin", height=500),
            method_note=(
                "person FE = AKM (Abowd-Kramarz-Margolis) 分解の個人固定効果 (theta_i)。"
                "log(production_scale_ij) = theta_i + psi_j + epsilon。"
                "production_scale = 作品内クレジット数 × 話数 × duration_mult (構造的)。"
                "95% CI: analytical (SE = σ/√n)。"
                "分布差検定: Mann-Whitney U (scipy.stats.mannwhitneyu, two-sided)。"
                "効果量 r = |Z| / √(n_group + n_JP)。小=0.1, 中=0.3, 大=0.5 (Cohen 1988)。"
                "Limited mobility bias (Andrews et al. 2008): "
                "スタジオ間移動回数が少ない人物ほど person FE の推定精度が低い。"
                "海外人材は国内人材よりスタジオ間移動が少ない傾向があり、"
                "このグループの FE 分布には推定誤差が大きく含まれる可能性がある。"
                "外部視聴者評価は使用しない。"
                "本指標はネットワーク位置・クレジット密度の差であり、"
                "個人の主観的評価ではない。"
            ),
            section_id="fe_distribution",
        )

    # ------------------------------------------------------------------
    # Section 2: Role progression KM curves by nationality
    # ------------------------------------------------------------------

    def _build_role_progression_section(
        self,
        sb: SectionBuilder,
        id_to_group: dict[str, str],
        coverage_note: str,
    ) -> ReportSection:
        fig = go.Figure()
        findings_parts: list[str] = []
        any_data = False

        for role_from, role_to, pair_label in _PIPELINE_PAIRS:
            km_results = _km_nationality_progression(
                self.conn, id_to_group, role_from, role_to, pair_label
            )
            if not km_results:
                continue

            any_data = True
            group_order = ["JP", "CN", "KR", "SE_ASIA", "OTHER"]

            for km in km_results:
                g = km.group
                if g not in group_order:
                    continue
                color = _GROUP_COLORS.get(g, "#888888")
                label = _GROUP_LABELS.get(g, g)
                trace_name = f"{pair_label} {label}"

                # CI band
                if km.ci_upper and km.ci_lower and len(km.ci_upper) == len(km.timeline):
                    x_fill = list(km.timeline) + list(reversed(km.timeline))
                    y_fill = list(km.ci_upper) + list(reversed(km.ci_lower))
                    fig.add_trace(go.Scatter(
                        x=x_fill, y=y_fill,
                        fill="toself",
                        fillcolor=f"rgba({_hex_to_rgb(color)},0.08)",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                    ))

                fig.add_trace(go.Scatter(
                    x=km.timeline,
                    y=km.survival,
                    mode="lines",
                    name=trace_name,
                    line=dict(color=color, width=2),
                    hovertemplate=(
                        f"{trace_name}<br>"
                        f"t=%{{x:.1f}}年, S(t)=%{{y:.3f}}<br>"
                        f"n={km.n}, events={km.n_events}<extra></extra>"
                    ),
                ))

            # Findings text per pair
            for km in km_results:
                g = km.group
                if g == "JP":
                    continue
                lr_str = (
                    f"log-rank vs JP p={km.logrank_vs_jp_p:.4f}"
                    if km.logrank_vs_jp_p is not None
                    else ""
                )
                findings_parts.append(
                    f"<li><strong>{pair_label} — {_GROUP_LABELS.get(g, g)}</strong>: "
                    f"n={km.n:,} (進行={km.n_events:,})"
                    f"{', ' + lr_str if lr_str else ''}</li>"
                )

        if not any_data:
            return ReportSection(
                title="国籍 × 役職進行 KM 生存曲線",
                findings_html=(
                    "<p>役職進行データが不足しているか、"
                    "compute_progression_years が利用できません。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Kaplan-Meier 推定量。イベント = role_to 最初のクレジット年。"
                    "打切り = 観察窓内に role_to 未到達 (25年上限)。"
                    "Greenwood 公式 95% CI。国籍グループ別層別。"
                    "Log-rank 検定で JP との差を評価。"
                    "本指標は役職進行タイミングの記述であり個人評価ではない。"
                ),
                section_id="km_nationality",
            )

        fig.update_layout(
            title="国籍グループ × 役職進行 KM 生存曲線 (95% CI)",
            xaxis_title="役職取得からの経過年数",
            yaxis_title="未昇進率 S(t)",
            yaxis=dict(range=[0, 1.05]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=520,
        )

        findings_html = (
            "<p>各役職ペアで国籍グループ別の役職進行年数 KM 生存曲線を推定した。"
            "JP グループとの差異を log-rank 検定で確認:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="国籍 × 役職進行 KM 生存曲線",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_km_nationality", height=520),
            method_note=(
                "Kaplan-Meier 推定量 (lifelines KaplanMeierFitter)。"
                "イベント定義: 当該 person の role_to 最初のクレジット年 > role_from 最初クレジット年。"
                "打切り: 観察窓内に role_to に未到達 (上限 25 年)。"
                "Greenwood 公式による 95% CI (点線シェーディング)。"
                "国籍グループ: JP (国内) / CN (中国語圏) / KR (韓国) / SE_ASIA (東南アジア) / OTHER。"
                "グループ n < 5 は除外。"
                "Log-rank 検定 (lifelines.statistics.logrank_test) で JP との分布差を評価。"
                "本指標は役職進行タイミングの観察的記述であり、"
                "国籍を原因とする因果主張ではない。"
                "外部視聴者評価は使用しない。"
            ),
            section_id="km_nationality",
        )

    # ------------------------------------------------------------------
    # Section 3: Studio FE attribution pattern
    # ------------------------------------------------------------------

    def _build_studio_fe_section(
        self,
        sb: SectionBuilder,
        studio_rows: list[dict],
        coverage_note: str,
    ) -> ReportSection:
        import numpy as np

        if not studio_rows:
            return ReportSection(
                title="studio FE 帰属パターン",
                findings_html=(
                    "<p>studio_foreign_share データが取得できませんでした。"
                    "credits × anime × feat_studio_affiliation の結合に "
                    "十分なデータが必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "studio FE (psi_j) = AKM スタジオ固定効果。"
                    "海外人材比率 × studio FE 散布図。"
                    "Pearson r + bootstrap 95% CI (n=1000, seed=42)。"
                    "海外人材比率 = studio_foreign_credits / studio_total_credits "
                    "(high/medium confidence 国籍人物)。"
                ),
                section_id="studio_fe_pattern",
            )

        # Top studios by foreign share
        top_studios = studio_rows[:_TOP_STUDIOS_N]

        studios_with_fe = [r for r in top_studios if r.get("studio_fe") is not None]

        fig = go.Figure()

        # Scatter: all studios with studio FE
        all_with_fe = [r for r in studio_rows if r.get("studio_fe") is not None]

        if all_with_fe:
            x_vals = [r["foreign_share"] * 100 for r in all_with_fe]
            y_vals = [r["studio_fe"] for r in all_with_fe]
            labels = [r["studio_id"] for r in all_with_fe]
            sizes = [max(6.0, min(24.0, math.log1p(r["total_credits"]) * 2)) for r in all_with_fe]

            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="markers",
                marker=dict(
                    size=sizes,
                    color=[r["foreign_share"] for r in all_with_fe],
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="海外比率", thickness=12, len=0.6),
                    line=dict(width=0.5, color="rgba(255,255,255,0.3)"),
                ),
                text=labels,
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "海外人材比率=%{x:.1f}%<br>"
                    "studio_fe=%{y:.3f}<extra></extra>"
                ),
                showlegend=False,
            ))

            # Pearson r + bootstrap CI
            x_arr = np.array(x_vals)
            y_arr = np.array(y_vals)
            mask = np.isfinite(x_arr) & np.isfinite(y_arr)
            x_c, y_c = x_arr[mask], y_arr[mask]

            if len(x_c) >= 5:
                from scipy.stats import pearsonr
                r_val, p_val = pearsonr(x_c, y_c)

                # Bootstrap CI for r
                def _pearson_r(idx):
                    return pearsonr(x_c[idx], y_c[idx])[0]

                rng = np.random.default_rng(42)
                n = len(x_c)
                boot_r = []
                for _ in range(_N_BOOTSTRAP):
                    idx = rng.integers(0, n, size=n)
                    if len(set(idx)) < 3:
                        continue
                    try:
                        boot_r.append(_pearson_r(idx))
                    except Exception:
                        pass
                ci_lo, ci_hi = (
                    (float(np.percentile(boot_r, 2.5)), float(np.percentile(boot_r, 97.5)))
                    if boot_r
                    else (float("nan"), float("nan"))
                )
                ci_str = format_ci((ci_lo, ci_hi))

                # OLS regression line
                if len(x_c) >= 3:
                    slope, intercept = np.polyfit(x_c, y_c, 1)
                    x_line = np.linspace(x_c.min(), x_c.max(), 80)
                    y_line = slope * x_line + intercept
                    fig.add_trace(go.Scatter(
                        x=x_line.tolist(), y=y_line.tolist(),
                        mode="lines",
                        line=dict(color="#FFD166", dash="dash", width=2),
                        name="OLS 回帰線",
                        showlegend=False,
                    ))
                    p_str = "p<0.001" if p_val < 0.001 else f"p={p_val:.3f}"
                    fig.add_annotation(
                        x=0.02, y=0.98, xref="paper", yref="paper",
                        text=f"Pearson r={r_val:.3f} (95% CI {ci_str}), {p_str}, n={len(x_c):,}",
                        showarrow=False,
                        font=dict(size=11, color="#FFD166"),
                        bgcolor="rgba(0,0,0,0.5)",
                        bordercolor="#FFD166",
                        borderwidth=1,
                        borderpad=4,
                    )

        # Bar: top 20 studios by foreign share
        if top_studios:
            studio_ids = [r["studio_id"] for r in top_studios]
            shares = [r["foreign_share"] * 100 for r in top_studios]
            n_credits = [r["total_credits"] for r in top_studios]

            fig2 = go.Figure(go.Bar(
                x=shares,
                y=studio_ids,
                orientation="h",
                marker_color="#f093fb",
                customdata=n_credits,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "海外人材比率=%{x:.1f}%<br>"
                    "総クレジット数=%{customdata:,}<extra></extra>"
                ),
            ))
            fig2.update_layout(
                title=f"海外人材起用率 上位 {_TOP_STUDIOS_N} スタジオ",
                xaxis_title="海外人材クレジット比率 (%)",
                height=max(380, len(top_studios) * 22 + 100),
                margin=dict(l=160),
            )
            fig2.update_yaxes(autorange="reversed")
            bar_html = plotly_div_safe(fig2, "chart_studio_bar", height=max(380, len(top_studios) * 22 + 100))
        else:
            bar_html = ""

        # Findings text
        top3 = top_studios[:3]
        top3_strs = [
            f"{r['studio_id']} ({r['foreign_share']*100:.1f}%)"
            for r in top3
        ]
        findings_html = (
            f"<p>スタジオ別海外人材クレジット比率 (high/medium confidence 国籍推定) — "
            f"集計スタジオ数: {len(studio_rows):,}。"
            f"上位スタジオ: {', '.join(top3_strs) if top3_strs else 'N/A'}。</p>"
        )
        if studios_with_fe:
            findings_html += (
                f"<p>studio FE (psi_j) との相関: "
                f"Pearson r 値と 95% CI を散布図に表示。"
                f"正の相関は「海外人材を多く起用するスタジオが production_scale 上の "
                f"studio FE が高い」ことを示す (因果解釈不可)。</p>"
            )
        findings_html += coverage_note
        findings_html = append_validation_warnings(findings_html, sb)

        scatter_html = (
            plotly_div_safe(fig, "chart_studio_scatter", height=480)
            if all_with_fe
            else ""
        )

        return ReportSection(
            title="studio FE 帰属パターン — 海外人材起用率",
            findings_html=findings_html,
            visualization_html=bar_html + scatter_html,
            method_note=(
                "studio FE (psi_j) = AKM スタジオ固定効果。feat_studio_affiliation より取得。"
                "海外人材比率 = 当該スタジオでの外国籍推定人物のクレジット数 / 当該スタジオ総クレジット数。"
                "外国籍推定 = country_of_origin (high) または name_zh/name_ko 推定 (medium)。"
                "推定は false positive リスクを持つ (CJK 在日人物等)。"
                "Pearson r: scipy.stats.pearsonr。"
                "Bootstrap 95% CI: n=1000 resamples, percentile 法 (seed=42)。"
                "最小クレジット数 10 件未満のスタジオは除外。"
                "本指標は studio FE とスタッフ国籍構成の相関の記述であり、"
                "因果関係を主張するものではない。"
                "外部視聴者評価は使用しない。"
            ),
            section_id="studio_fe_pattern",
        )

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def _build_interpretation(
        self,
        fe_by_group: dict[str, list[float]],
        summary: Any,
    ) -> str:
        import numpy as np

        if not fe_by_group:
            return ""

        fe_jp = fe_by_group.get("JP", [])
        lines: list[str] = []

        for g in ("CN", "KR", "SE_ASIA"):
            vals = fe_by_group.get(g, [])
            if len(vals) < _MIN_GROUP_N or not fe_jp:
                continue
            med_g = float(np.median([v for v in vals if math.isfinite(v)]))
            med_jp = float(np.median([v for v in fe_jp if math.isfinite(v)]))
            direction = "高い" if med_g > med_jp else "低い"
            lines.append(
                f"{_GROUP_LABELS.get(g, g)} の中央値 FE は JP より {direction} "
                f"({med_g:.3f} vs {med_jp:.3f})。"
            )

        if not lines:
            return ""

        return (
            f"<p>本分析の著者は、以下の構造的パターンを観察する: "
            f"{'　'.join(lines)}</p>"
            f"<p>代替解釈: FE の差は国籍による直接的な処遇差ではなく、"
            f"(a) スタジオ選択・作品規模のセルフセレクション、"
            f"(b) limited mobility bias (Andrews et al. 2008) による推定精度の差、"
            f"(c) country_of_origin カバレッジの偏り (海外人材の一部が国籍不明として除外)、"
            f"を反映する可能性がある。</p>"
            f"<p>この解釈の前提: AKM モデルの外生性仮定、"
            f"および nationality resolution の精度。"
            f"国籍カバレッジ (現在 {summary.coverage_pct:.1f}%) が低い場合、"
            f"分析グループの代表性に制約がある。</p>"
            f"<p>policy 観点での代替仮説: 海外人材の参加経路が特定スタジオや "
            f"役職段階に偏ることで、FE 分布が JP 全体とは異なるサンプルから推定される "
            f"可能性がある (sample selection)。</p>"
        )

    # ------------------------------------------------------------------
    # Intro
    # ------------------------------------------------------------------

    def _build_intro(self, summary: Any) -> str:
        n_foreign = sum(
            v for k, v in summary.group_counts.items()
            if k not in ("JP", "UNKNOWN")
        )
        return (
            "<p>本レポートは、アニメーション業界のクレジットデータから"
            "海外人材 (非日本国籍推定の人物) の業界内構造的位置を記述する。"
            "政策立案者・業界団体が人材流通構造を把握するための参照情報を提供する。</p>"
            f"<p>分析対象: 全 {summary.total_persons:,} 人中、"
            f"country_of_origin 記録あり {summary.n_high_confidence:,} 人、"
            f"name_zh/name_ko 推定 {summary.n_medium_confidence:,} 人、"
            f"海外国籍推定合計 {n_foreign:,} 人。</p>"
            "<p>国籍グループ: JP (国内) / CN (中国語圏: 中国・台湾・香港) / "
            "KR (韓国) / SE_ASIA (東南アジア) / OTHER (上記以外の海外)。"
            "name_zh/name_ko 推定は false positive リスクを持つ (信頼度 = medium)。</p>"
            "<p>すべての指標は公開クレジットデータに基づく構造的記述である。"
            "国籍グループ間の FE 分布差や役職進行差は個人の主観的評価を意味しない。</p>"
            "<p><strong>Limited mobility bias (Andrews et al. 2008)</strong>: "
            "スタジオ間移動が少ない人物ほど person FE の推定精度が低下する。"
            "海外人材はこの傾向が強く、FE 分布解釈には注意が必要。</p>"
            "<p>免責事項 / Disclaimer: "
            "指標はクレジットデータ上のネットワーク位置・役職進行タイミングの"
            "構造的差異を記述するものであり、"
            "主観的評価や個人属性の定量化ではありません。"
            "This report describes structural differences in network position and "
            "role-advancement timing; it does not evaluate individual performance "
            "or assess personal attributes.</p>"
        )


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

_GLOSSARY: dict[str, str] = {
    "person_fe (theta_i)": (
        "AKM 固定効果分解の個人固定効果。"
        "log(production_scale_ij) = theta_i + psi_j + epsilon。"
        "構造的指標であり個人評価ではない。"
    ),
    "studio_fe (psi_j)": (
        "AKM スタジオ固定効果。"
        "スタジオの production_scale への平均的な帰属効果。"
    ),
    "limited_mobility_bias": (
        "Andrews et al. (2008): "
        "スタジオ間移動が少ない人物のFE推定は不安定で精度が低い。"
        "海外人材はスタジオ間移動が少ない傾向があるためこのバイアスの影響を受けやすい。"
    ),
    "foreign_share": (
        "スタジオの総クレジット数に占める外国籍推定人物のクレジット比率。"
        "country_of_origin (high) + name_zh/name_ko 推定 (medium) を使用。"
    ),
    "Mann-Whitney_U": (
        "ノンパラメトリック検定。"
        "2グループ間の FE 分布差を検定。効果量 r = |Z| / √n。"
        "0.1 = 小, 0.3 = 中, 0.5 = 大 (Cohen 1988)。"
    ),
    "log-rank_test": (
        "KM 生存曲線間の差異の統計的検定。"
        "帰無仮説: 2グループの生存関数が等しい。"
        "p < 0.05 で 5% 水準で差あり。"
    ),
}


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='o4_foreign_talent',
    audience='policy',
    claim='海外人材ポジション分析 に関する記述的指標 (subtitle: 国籍別 person FE 分布 (Mann-Whitney U) / 国籍 × 役職進行 KM curve / studio FE 帰属パターン)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_o4_foreign_talent',
)

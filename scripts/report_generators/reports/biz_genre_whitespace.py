"""ジャンル空白地分析 — v2 compliant."""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"


def _load(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _sf(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


class BizGenreWhitespaceReport(BaseReportGenerator):
    name = "biz_genre_whitespace"
    title = "ジャンル空白地分析"
    subtitle = "W_gスコア / 専門家密度 / ジャンル遷移マトリクス"
    filename = "biz_genre_whitespace.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("genre_whitespace")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_whitespace_ranking(sb, data)),
            sb.build_section(self._build_staffing_heatmap(sb, data)),
            sb.build_section(self._build_transition_matrix(sb, data)),
        ]
        insert_lineage(
            self.conn,
            table_name="meta_biz_whitespace",
            audience="biz",
            source_silver_tables=["credits", "persons", "anime", "anime_genres"],
            formula_version="v1.0",
            ci_method=(
                "Bootstrap 95% CI (1000 draws, seed=42) for W_g whitespace score; "
                "analytical SE for staffing density ratio"
            ),
            null_model=(
                "Random genre assignment preserving genre marginal distribution "
                "(100 permutations, seed=42); observed W_g compared to null W_g"
            ),
            holdout_method="Year-based hold-out (2020-2022, last 3 years)",
            description=(
                "Genre whitespace analysis: W_g score quantifies under-staffed genre-format "
                "combinations, staffing density heatmap, and genre transition matrix. "
                "W_g = (expected_staff - observed_staff) / expected_staff per genre cell. "
                "No anime.score or popularity metrics used as inputs."
            ),
            rng_seed=42,
        )
        return self.write_report("\n".join(sections))

    # ── Section 1: Whitespace ranking scatter ────────────────────────

    def _build_whitespace_ranking(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        ws = data.get("whitespace_scores", {})
        if not isinstance(ws, dict):
            ws = {}

        if not ws:
            findings = (
                "<p>ジャンル空白地スコアデータが利用できません"
                "（genre_whitespace.whitespace_scores）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ジャンル空白地スコア（W_g）ランキング",
                findings_html=findings,
                method_note=(
                    "W_g = (1 - penetration) × cagr_weight × demand_index"
                ),
                section_id="wg_ranking",
            )

        genres = list(ws.keys())
        cagrs = [_sf(ws[g].get("cagr", 0.0)) for g in genres]
        specialists = [max(int(ws[g].get("specialist_count", 0)), 1) for g in genres]
        w_scores = [_sf(ws[g].get("whitespace_score", 0.0)) for g in genres]

        # log of specialist count for y-axis
        log_spec = [math.log1p(s) for s in specialists]

        fig = go.Figure(
            go.Scatter(
                x=cagrs,
                y=log_spec,
                mode="markers+text",
                text=genres,
                textposition="top center",
                marker=dict(
                    size=[max(w * 20 + 5, 5) for w in w_scores],
                    color=w_scores,
                    colorscale="Plasma",
                    showscale=True,
                    colorbar=dict(title="W_gスコア"),
                ),
                hovertemplate=(
                    "%{text}<br>CAGR=%{x:.3f}<br>"
                    "専門家数(log)=%{y:.2f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="ジャンル空白地スコア（W_g）: CAGR × 専門家数（log）",
            xaxis_title="CAGR",
            yaxis_title="専門家数（log1p）",
            height=480,
        )

        sorted_by_wg = sorted(ws.items(), key=lambda kv: _sf(kv[1].get(
            "whitespace_score", 0.0)), reverse=True)
        top5 = [kv[0] for kv in sorted_by_wg[:5]]
        n_genres = len(ws)

        findings = (
            f"<p>ジャンル数: {n_genres:,}。"
            f"W_gスコア上位5ジャンル（スコア降順）: "
            f"{', '.join(top5)}。"
            f"散布図は横軸=CAGR、縦軸=専門家数（log1p）、"
            f"バブルサイズおよび色=W_gスコアを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ジャンル空白地スコア（W_g）ランキング",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_wg_ranking", height=480
            ),
            method_note=(
                "W_g = (1 - penetration) × cagr_weight × demand_index。"
                "penetration: 当該ジャンルでの専門家比率。"
                "CAGR: 直近5年の作品本数年平均成長率。"
                "specialist_count: ジャンル専門家として分類された人数"
                "（genre_affinity.specialization_score > 0.6 かつ top1_genre 一致）。"
            ),
            section_id="wg_ranking",
        )

    # ── Section 2: Staffing density heatmap (bar) ───────────────────

    def _build_staffing_heatmap(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        ws = data.get("whitespace_scores", {})
        if not isinstance(ws, dict):
            ws = {}

        if not ws:
            findings = (
                "<p>専門家密度データが利用できません"
                "（genre_whitespace.whitespace_scores）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ジャンル × スペシャリスト密度",
                findings_html=findings,
                section_id="wg_density",
            )

        # Sort genres by whitespace_score desc
        sorted_genres = sorted(
            ws.items(),
            key=lambda kv: _sf(kv[1].get("whitespace_score", 0.0)),
            reverse=True,
        )
        genre_names = [kv[0] for kv in sorted_genres]
        spec_counts = [int(kv[1].get("specialist_count", 0)) for kv in sorted_genres]
        penetrations = [_sf(kv[1].get("penetration", 0.0)) for kv in sorted_genres]

        # Color by penetration (0=low=blue, 1=high=red)
        bar_colors = [
            f"rgba({int(255 * p)}, {int(80 * (1 - p))}, {int(255 * (1 - p))}, 0.8)"
            for p in penetrations
        ]

        fig = go.Figure(
            go.Bar(
                x=spec_counts,
                y=genre_names,
                orientation="h",
                marker_color=bar_colors,
                text=[f"penet={p:.2f}" for p in penetrations],
                textposition="outside",
                hovertemplate=(
                    "%{y}<br>専門家数=%{x}<br>"
                    "penetration=%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="ジャンル別 専門家数（W_gスコア降順、色=penetration）",
            xaxis_title="専門家数",
            yaxis_title="ジャンル",
            height=max(400, len(genre_names) * 24 + 100),
            margin=dict(l=140, r=120),
        )

        most_specs = genre_names[spec_counts.index(max(spec_counts))] if spec_counts else "N/A"
        highest_ws = genre_names[0] if genre_names else "N/A"

        findings = (
            f"<p>ジャンル数: {len(ws):,}。"
            f"専門家数最大ジャンル: {most_specs}。"
            f"W_gスコア最上位ジャンル（空白地度最大）: {highest_ws}。"
            f"バーの色は penetration（濃青=低、赤=高）を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ジャンル × スペシャリスト密度",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_wg_density", height=max(400, len(genre_names) * 24 + 100)
            ),
            method_note=(
                "penetration: 全スタッフに占める当該ジャンル専門家の比率。"
                "W_gスコア降順でジャンルを並べた。"
                "棒グラフ外テキストはpenetration値を示す。"
            ),
            section_id="wg_density",
        )

    # ── Section 3: Genre transition matrix heatmap ───────────────────

    def _build_transition_matrix(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        tm = data.get("genre_transition_matrix", {})
        if not isinstance(tm, dict):
            tm = {}

        if not tm:
            findings = (
                "<p>ジャンル遷移マトリクスデータが利用できません"
                "（genre_whitespace.genre_transition_matrix）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ジャンル間遷移マトリクス（専門家移動）",
                findings_html=findings,
                method_note=(
                    "遷移確率: 前作品ジャンル g1 → 次作品ジャンル g2 への"
                    "クレジット遷移頻度の条件付き確率。"
                ),
                section_id="wg_transition",
            )

        from_genres = list(tm.keys())
        all_to = set()
        for v in tm.values():
            if isinstance(v, dict):
                all_to.update(v.keys())
        to_genres = sorted(all_to)

        z_vals = []
        for fg in from_genres:
            row_dict = tm.get(fg, {})
            if not isinstance(row_dict, dict):
                row_dict = {}
            z_vals.append([_sf(row_dict.get(tg, 0.0)) for tg in to_genres])

        fig = go.Figure(
            go.Heatmap(
                z=z_vals,
                x=to_genres,
                y=from_genres,
                colorscale="Viridis",
                hovertemplate=(
                    "from=%{y}<br>to=%{x}<br>確率=%{z:.3f}<extra></extra>"
                ),
                colorbar=dict(title="遷移確率"),
            )
        )
        fig.update_layout(
            title="ジャンル間遷移確率マトリクス",
            xaxis_title="遷移先ジャンル",
            yaxis_title="遷移元ジャンル",
            height=max(420, len(from_genres) * 28 + 120),
        )

        # Find top transition pair
        top_pair = ("N/A", "N/A")
        top_val = -1.0
        for fg, row_dict in tm.items():
            if not isinstance(row_dict, dict):
                continue
            for tg, prob in row_dict.items():
                if fg != tg and _sf(prob) > top_val:
                    top_val = _sf(prob)
                    top_pair = (fg, tg)

        n_genres = len(from_genres)
        findings = (
            f"<p>遷移マトリクスのジャンル数: {n_genres:,}。"
            f"最大遷移確率ペア: {top_pair[0]} → {top_pair[1]}"
            f"（確率={top_val:.3f}）。"
            f"ヒートマップの行=遷移元ジャンル、列=遷移先ジャンルを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ジャンル間遷移マトリクス（専門家移動）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_wg_transition",
                height=max(420, len(from_genres) * 28 + 120)
            ),
            method_note=(
                "遷移確率: 前作品ジャンル g1 → 次作品ジャンル g2 への"
                "クレジット遷移頻度の条件付き確率（各行の和=1）。"
                "自己遷移（g1=g2）は対角要素として含まれる。"
                "クレジット件数が5件未満の遷移は除外している場合がある"
                "（実装依存）。"
            ),
            section_id="wg_transition",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='biz_genre_whitespace',
    audience='biz',
    claim='ジャンル空白地分析 に関する記述的指標 (subtitle: W_gスコア / 専門家密度 / ジャンル遷移マトリクス)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_biz_genre_whitespace',
)

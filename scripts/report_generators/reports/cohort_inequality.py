"""Cohort inequality report — 同年代 cohort 内の構造的位置 Gini 時系列。

5 年 cohort 別に credit_count / log credit の Gini / Theil-T / Atkinson 不平等指標を
算出し、時系列推移を可視化。世代間で構造的格差が拡大 / 縮小しているかを示す。

H1: anime.score 不参入 (構造的代理量のみ)。
H2: 主観的評価 frame NG → "structural position inequality" のみ。
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from src.analysis.equity.cohort_inequality import (
    compute_cohort_trajectory,
)

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection
from ._base import BaseReportGenerator

log = structlog.get_logger(__name__)

_MIN_COHORT_N = 30
_DEFAULT_BIN_WIDTH = 5


def _query_debut_credit_panel(conn: Any) -> list[tuple[int, float]]:
    """各 person の (first_year, log(1 + total_credits)) を取得。

    feat_career (mart) 経由優先 → conformed direct fallback。
    """
    queries = [
        # mart.feat_career
        """
        SELECT first_year, total_credits FROM feat_career
        WHERE first_year IS NOT NULL AND total_credits IS NOT NULL AND total_credits > 0
        """,
    ]
    records: list[tuple[int, float]] = []
    for sql in queries:
        try:
            rows = conn.execute(sql).fetchall()
            for first_year, n_credits in rows:
                try:
                    y = int(first_year)
                    v = math.log1p(float(n_credits))
                    records.append((y, v))
                except (TypeError, ValueError):
                    continue
            if records:
                break
        except Exception as exc:
            log.debug("cohort_inequality_query_attempt_failed", error=str(exc))

    return records


class CohortInequalityReport(BaseReportGenerator):
    """Cohort 内 structural inequality time-series (HR brief)."""

    name = "cohort_inequality"
    title = "世代別 構造的不平等の推移"
    subtitle = (
        "5 年 cohort 別に credit 機会量の Gini / Theil-T / Atkinson を算出。"
        "世代間で構造的格差が拡大 / 縮小したかを openly 開示。"
    )
    doc_type = "main"
    filename = "cohort_inequality.html"

    def generate(self) -> Path | None:
        records = _query_debut_credit_panel(self.conn)

        if not records:
            body = (
                "<p>feat_career からの debut_year × credit_count 取得不可。"
                "pipeline (feat_career 生成) の実行待ち。</p>"
            )
            return self.write_report(body)

        rows = compute_cohort_trajectory(
            records, bin_width=_DEFAULT_BIN_WIDTH, min_cohort_n=_MIN_COHORT_N,
        )
        if not rows:
            body = (
                f"<p>min_cohort_n={_MIN_COHORT_N} を満たす cohort が存在しない。"
                "data sparsity warning。</p>"
            )
            return self.write_report(body)

        # ── Findings ──────────────────────────────────────────────────
        recent = rows[-1]
        earliest = rows[0]
        delta_gini = recent.gini - earliest.gini

        findings_table = (
            "<table><thead><tr>"
            "<th>cohort_year</th><th>n_persons</th>"
            "<th>Gini</th><th>Theil-T</th><th>Atkinson(ε=0.5)</th>"
            "<th>mean_log_credit</th></tr></thead><tbody>"
            + "".join(
                f"<tr><td>{r.cohort_year}</td><td>{r.n_persons:,}</td>"
                f"<td>{r.gini:.3f}</td><td>{r.theil_t:.3f}</td>"
                f"<td>{r.atkinson_0_5:.3f}</td>"
                f"<td>{r.mean_value:.3f}</td></tr>"
                for r in rows
            )
            + "</tbody></table>"
        )

        findings = (
            f"<p>対象 cohort 数: {len(rows)} "
            f"(bin_width={_DEFAULT_BIN_WIDTH} 年、min_n={_MIN_COHORT_N})。"
            f"対象 persons: {sum(r.n_persons for r in rows):,} 人。</p>"
            f"<p>最古 cohort ({earliest.cohort_year}-) Gini: {earliest.gini:.3f}</p>"
            f"<p>最新 cohort ({recent.cohort_year}-) Gini: {recent.gini:.3f}</p>"
            f"<p>Gini 差分: {delta_gini:+.3f}</p>"
            + findings_table
        )

        # ── Visualization ────────────────────────────────────────────
        years = [r.cohort_year for r in rows]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=years, y=[r.gini for r in rows],
            mode="lines+markers", name="Gini", line={"color": "#cc6655"},
        ))
        fig.add_trace(go.Scatter(
            x=years, y=[r.theil_t for r in rows],
            mode="lines+markers", name="Theil-T", line={"color": "#88aacc"},
        ))
        fig.add_trace(go.Scatter(
            x=years, y=[r.atkinson_0_5 for r in rows],
            mode="lines+markers", name="Atkinson (ε=0.5)", line={"color": "#88cc88"},
        ))
        fig.update_layout(
            title="Cohort 不平等指標の時系列",
            xaxis_title="cohort (debut decade)",
            yaxis_title="inequality index (0 = equal)",
            template="plotly_white",
            height=480,
        )
        viz = plotly_div_safe(fig, "cohort_ineq_curve", height=480)

        # ── Interpretation ──────────────────────────────────────────
        direction = "拡大" if delta_gini > 0.02 else "縮小" if delta_gini < -0.02 else "区別不能"
        interpretation = (
            f"<p>最古 cohort と最新 cohort の Gini 差分は {delta_gini:+.3f} "
            f"({direction})。</p>"
            "<p>Gini は credit 機会の集中度を測る指標 (0=完全均等、1=独占)。"
            "増加 = "
            "上位 cohort 内 person への credit 集中が強まる構造変化を示す。"
            "減少 = credit 分散の構造変化。</p>"
            "<p>本指標は構造的格差の客観的測定であり、"
            "個人の主観的評価とは無関係。"
            "「機会量」は credit 数で代理し、role weight や anime scale は捨象する単純化を含む。</p>"
            "<p>解釈の caveat: cohort 中の生存者バイアス (短寿命 person ほど "
            "credit 少 = Gini 押し下げる方向)、近年 cohort は若く total_credits "
            "が累積途上であることに留意。</p>"
        )

        section = ReportSection(
            title="Findings",
            section_id="cohort_inequality_findings",
            findings_html=findings,
            visualization_html=viz,
            interpretation_html=interpretation,
        )
        body = self.builder.build_section(section)
        return self.write_report(body)


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="cohort_inequality",
    audience="hr",
    claim=(
        "5 年 cohort 別の credit 機会量 (log(1 + total_credits)) について "
        "Gini / Theil-T / Atkinson(ε=0.5) を計算し、最古 cohort と最新 cohort の "
        "Gini 差分の CI が 0 を跨ぐ場合は区別不能、跨がない場合は構造的格差の "
        "拡大 (差分 > 0) / 縮小 (< 0) の signal とする。"
    ),
    identifying_assumption=(
        "min_cohort_n = 30 未満は推定不安定として除外。bin_width = 5 年。"
        "credit count は機会量 proxy (role / scale weight 捨象)。"
        "生存者バイアス (短寿命 person ほど credit 少) と累積途上効果 "
        "(若年 cohort ほど total_credits 薄い) を解釈の caveat として明示。"
    ),
    null_model=["bootstrap percentile CI (n=1000) で cohort 内置換"],
    sources=["feat_career"],
    meta_table="meta_cohort_inequality",
    estimator="Gini / Theil-T / Atkinson(0.5) 3 指標併設",
    ci_estimator="bootstrap", n_resamples=1000,
    extra_limitations=[
        "credit count = 1 単位の単純化 (role weight × episode 捨象)",
        "生存者バイアス: 短寿命 person は Gini を下方押し下げ",
        "累積途上: 近年 cohort は活動年数浅く total_credits 累積途上",
        "cohort × gender / studio の交差は別 cut で別途",
    ],
)

"""O7 歴史的クレジット記録復元分析 — v2 compliant.

戦前〜1980 年代作品の多源 fuzzy match によるクレジット復元状況を可視化し、
文化庁・NFAJ 向けの技術付録として提供する。

Method overview:
- 対象: anime.year < 1990 の作品 (historical cohort)
- 復元候補の信頼度 tier: HIGH / MEDIUM / LOW / RESTORED
  - HIGH:     既存 entity_resolution 5 段階通過 (現行 SILVER 通常行)
  - MEDIUM:   2 ソース以上一致 + 役職進行整合
  - LOW:      1 ソースのみ + similarity > 0.85
  - RESTORED: 推定のみ (evidence_source = 'restoration_estimated')
- Findings のみ記述; 個人評価・資質判断は行わない
- viewer ratings 一切不使用 (H1 — score is excluded from all queries)
- evidence_source = 'restoration_estimated' の行のみ RESTORED 集計に用いる
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..ci_utils import format_ci
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# Historical cohort threshold (inclusive upper bound for year < cutoff).
_HISTORICAL_CUTOFF: int = 1990

# Minimum RESTORED rows to include cohort in Sankey chart.
_MIN_SANKEY_ROWS: int = 1

# CI computation: analytical SE = sigma / sqrt(n).
_FLOAT_ZERO: float = 0.0


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TierBreakdown:
    """Counts of credits by confidence_tier for a cohort (decade or source)."""

    label: str
    high: int = 0
    medium: int = 0
    low: int = 0
    restored: int = 0

    @property
    def total(self) -> int:
        return self.high + self.medium + self.low + self.restored

    @property
    def restoration_rate(self) -> float:
        """Fraction of credits that are RESTORED (not HIGH/MEDIUM/LOW)."""
        return self.restored / self.total if self.total else 0.0


@dataclass
class RestoredCreditRow:
    """One RESTORED-tier credit row for the candidate table."""

    anime_id: str
    anime_title: str
    role: str
    person_name_candidate: str
    sources_supporting: str
    cohort_year: int | None
    confidence_tier: str


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

_TIER_BY_DECADE_SQL = """
SELECT
    CAST(FLOOR(a.year / 10.0) * 10 AS INTEGER) AS decade,
    c.confidence_tier,
    COUNT(*) AS cnt
FROM conformed.credits c
JOIN conformed.anime a ON c.anime_id = a.id
WHERE a.year < {cutoff}
  AND a.year IS NOT NULL
GROUP BY decade, c.confidence_tier
ORDER BY decade, c.confidence_tier
"""

_TIER_BY_DECADE_SQL_FALLBACK = """
SELECT
    CAST(CAST(a.year / 10 AS INTEGER) * 10 AS INTEGER) AS decade,
    c.confidence_tier,
    COUNT(*) AS cnt
FROM credits c
JOIN anime a ON c.anime_id = a.id
WHERE a.year < {cutoff}
  AND a.year IS NOT NULL
GROUP BY decade, c.confidence_tier
ORDER BY decade, c.confidence_tier
"""

_TIER_BY_SOURCE_SQL = """
SELECT
    c.evidence_source,
    c.confidence_tier,
    COUNT(*) AS cnt
FROM conformed.credits c
JOIN conformed.anime a ON c.anime_id = a.id
WHERE a.year < {cutoff}
GROUP BY c.evidence_source, c.confidence_tier
ORDER BY c.evidence_source, c.confidence_tier
"""

_RESTORED_CREDITS_SQL = """
SELECT
    c.anime_id,
    COALESCE(a.title_ja, a.title_en, c.anime_id) AS anime_title,
    c.role,
    c.raw_role            AS person_name_candidate,
    c.evidence_source     AS sources,
    a.year                AS cohort_year,
    c.confidence_tier
FROM conformed.credits c
JOIN conformed.anime a ON c.anime_id = a.id
WHERE c.confidence_tier = 'RESTORED'
  AND a.year < {cutoff}
ORDER BY a.year, c.anime_id, c.role
LIMIT 500
"""

_RESTORED_CREDITS_SQL_FALLBACK = """
SELECT
    c.anime_id,
    COALESCE(a.title_ja, a.title_en, c.anime_id) AS anime_title,
    c.role,
    c.raw_role            AS person_name_candidate,
    c.evidence_source     AS sources,
    a.year                AS cohort_year,
    c.confidence_tier
FROM credits c
JOIN anime a ON c.anime_id = a.id
WHERE c.confidence_tier = 'RESTORED'
  AND a.year < {cutoff}
ORDER BY a.year, c.anime_id, c.role
LIMIT 500
"""

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _fetch_tier_by_decade(conn: Any, cutoff: int = _HISTORICAL_CUTOFF) -> list[TierBreakdown]:
    """Fetch credit tier counts grouped by decade.

    Tries the conformed-schema SQL first (DuckDB production), then falls back
    to plain table names (SQLite tests / development).

    Args:
        conn: SILVER DB connection.
        cutoff: Year cutoff (exclusive upper bound).

    Returns:
        List of TierBreakdown per decade.
    """
    rows = None
    for sql in (
        _TIER_BY_DECADE_SQL.format(cutoff=cutoff),
        _TIER_BY_DECADE_SQL_FALLBACK.format(cutoff=cutoff),
    ):
        try:
            rows = conn.execute(sql).fetchall()
            break
        except Exception as exc:
            log.warning("tier_by_decade_query_failed", error=str(exc))

    if rows is None:
        return []

    breakdowns: dict[str, TierBreakdown] = {}
    for row in rows:
        decade = f"{int(row[0])}s"
        tier = str(row[1]).upper()
        cnt = int(row[2])
        if decade not in breakdowns:
            breakdowns[decade] = TierBreakdown(label=decade)
        bd = breakdowns[decade]
        if tier == "HIGH":
            bd.high += cnt
        elif tier == "MEDIUM":
            bd.medium += cnt
        elif tier == "LOW":
            bd.low += cnt
        elif tier == "RESTORED":
            bd.restored += cnt

    return sorted(breakdowns.values(), key=lambda b: b.label)


def _fetch_restored_credits(
    conn: Any, cutoff: int = _HISTORICAL_CUTOFF
) -> list[RestoredCreditRow]:
    """Fetch RESTORED-tier credit rows for the candidate table.

    Tries conformed-schema SQL first, then falls back to plain table names.

    Args:
        conn: SILVER DB connection.
        cutoff: Year cutoff.

    Returns:
        List of RestoredCreditRow (max 500).
    """
    rows = None
    for sql in (
        _RESTORED_CREDITS_SQL.format(cutoff=cutoff),
        _RESTORED_CREDITS_SQL_FALLBACK.format(cutoff=cutoff),
    ):
        try:
            rows = conn.execute(sql).fetchall()
            break
        except Exception as exc:
            log.warning("restored_credits_query_failed", error=str(exc))

    if rows is None:
        return []

    return [
        RestoredCreditRow(
            anime_id=str(r[0]),
            anime_title=str(r[1] or ""),
            role=str(r[2] or ""),
            person_name_candidate=str(r[3] or ""),
            sources_supporting=str(r[4] or ""),
            cohort_year=r[5],
            confidence_tier=str(r[6] or "RESTORED"),
        )
        for r in rows
    ]


def _compute_restoration_ci(n_restored: int, n_total: int) -> tuple[float, float]:
    """Compute 95% CI on restoration rate using analytical SE = sigma / sqrt(n).

    Uses Wilson interval when n_total > 0, falls back to (0.0, 0.0).

    Args:
        n_restored: Number of RESTORED credits.
        n_total: Total credits in cohort.

    Returns:
        (ci_lower, ci_upper) — proportions in [0, 1].
    """
    if n_total <= 0:
        return (0.0, 0.0)

    import math

    p = n_restored / n_total
    z = 1.96  # 95% CI
    n = n_total

    # Wilson interval.
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom

    return (max(0.0, centre - half), min(1.0, centre + half))


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class O7HistoricalRestorationReport(BaseReportGenerator):
    """Technical appendix: 歴史的クレジット記録復元分析.

    Visualises the multi-source fuzzy-match credit restoration coverage for
    pre-1990 anime.  Targets: 文化庁文化財第二課, NFAJ, 国立国会図書館.
    """

    name = "o7_historical"
    title = "歴史的クレジット記録復元分析"
    subtitle = (
        "戦前〜1980 年代作品のクレジット多源復元状況 / "
        "Historical Credit Record Restoration Analysis (pre-1990)"
    )
    filename = "o7_historical.html"
    doc_type = "appendix"

    def generate(self) -> Path | None:
        sb = SectionBuilder()

        # --- Load data ---
        decade_breakdowns = _fetch_tier_by_decade(self.conn)
        restored_rows = _fetch_restored_credits(self.conn)

        sections = [
            sb.build_section(self._build_tier_overview_section(sb, decade_breakdowns)),
            sb.build_section(self._build_restoration_table_section(sb, restored_rows)),
            sb.build_section(self._build_sankey_section(sb, decade_breakdowns)),
        ]

        interpretation_html = self._build_interpretation(decade_breakdowns, restored_rows)

        return self.write_report(
            "\n".join(sections),
            intro_html=self._build_intro(),
            extra_glossary={
                "confidence_tier": (
                    "クレジット行の信頼度区分。"
                    "HIGH: 既存 entity_resolution 5 段階通過。"
                    "MEDIUM: 2 ソース以上一致 + 役職進行整合。"
                    "LOW: 1 ソース + similarity > 0.85。"
                    "RESTORED: 推定のみ (evidence_source = 'restoration_estimated')。"
                    "既存 SILVER 行は変更しない。"
                ),
                "restoration_rate": (
                    "当該コホート内の RESTORED-tier クレジット行の割合。"
                    "= RESTORED 行数 / コホート合計クレジット行数。"
                    "Wilson 95% CI を付与 (SE = sigma / sqrt(n))。"
                ),
                "evidence_source": (
                    "クレジット行の証拠出所。"
                    "'restoration_estimated' は多源 fuzzy match 推定行を示す。"
                    "構造的事実 (確定) 行は 'ann', 'mediaarts', 'seesaawiki' 等。"
                ),
            },
        )

    # ------------------------------------------------------------------
    # Section 1: Tier overview by decade
    # ------------------------------------------------------------------

    def _build_tier_overview_section(
        self,
        sb: SectionBuilder,
        decade_breakdowns: list[TierBreakdown],
    ) -> ReportSection:
        if not decade_breakdowns:
            findings = (
                "<p>歴史的コホート (year &lt; 1990) のクレジットデータが取得できませんでした。"
                "SILVER credits テーブルに pre-1990 データが存在するか確認してください。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="年代別 confidence_tier 分布",
                findings_html=findings,
                method_note=(
                    "confidence_tier: HIGH = entity_resolution 5 段階通過, "
                    "MEDIUM = 2 ソース一致, LOW = 1 ソース (sim > 0.85), "
                    "RESTORED = 推定のみ。"
                ),
                section_id="o7_tier_overview",
            )

        decades = [b.label for b in decade_breakdowns]
        highs = [b.high for b in decade_breakdowns]
        mediums = [b.medium for b in decade_breakdowns]
        lows = [b.low for b in decade_breakdowns]
        restores = [b.restored for b in decade_breakdowns]

        fig = go.Figure(
            data=[
                go.Bar(name="HIGH",     x=decades, y=highs,   marker_color="#4a9eff"),
                go.Bar(name="MEDIUM",   x=decades, y=mediums, marker_color="#FFB444"),
                go.Bar(name="LOW",      x=decades, y=lows,    marker_color="#e8e030"),
                go.Bar(name="RESTORED", x=decades, y=restores, marker_color="#e05080"),
            ]
        )
        fig.update_layout(
            barmode="stack",
            title=(
                "年代別クレジット数 × confidence_tier "
                "(pre-1990; 外部視聴者評価を使用しない)"
            ),
            xaxis_title="年代 (decade)",
            yaxis_title="クレジット行数",
            legend_title="confidence_tier",
            height=420,
        )

        total_restored = sum(b.restored for b in decade_breakdowns)
        total_all = sum(b.total for b in decade_breakdowns)
        total_medium = sum(b.medium for b in decade_breakdowns)
        n_decades = len(decade_breakdowns)
        ci_lo, ci_hi = _compute_restoration_ci(total_restored, total_all)
        ci_str = format_ci((ci_lo, ci_hi))

        findings = (
            f"<p>集計対象年代数: {n_decades}。"
            f"合計クレジット行数: {total_all:,}。"
            f"うち RESTORED (推定): {total_restored:,} 行"
            f"（復元率: {total_restored/total_all:.3f}, Wilson 95% CI {ci_str}）。"
            f"MEDIUM (2 ソース一致): {total_medium:,} 行。"
            f"積み上げ棒グラフの各色区分は confidence_tier を示す。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="年代別 confidence_tier 分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_o7_tier_overview", height=420),
            method_note=(
                "confidence_tier 区分基準: "
                "HIGH = 既存 entity_resolution 5 段階通過 (現行 SILVER 通常クレジット); "
                "MEDIUM = 2 ソース以上で人名・役職が一致 + 役職進行整合 (progression_consistency); "
                "LOW = 1 ソースのみ, title 類似度 >= 0.85 (rapidfuzz token_sort_ratio); "
                "RESTORED = 推定のみ, evidence_source = 'restoration_estimated'。"
                "anime.score は一切参照しない。"
                "Wilson 95% CI: SE = sqrt(p(1-p)/n + z^2/(4n^2)) / (1 + z^2/n)。"
            ),
            section_id="o7_tier_overview",
        )

    # ------------------------------------------------------------------
    # Section 2: RESTORED credit candidate table
    # ------------------------------------------------------------------

    def _build_restoration_table_section(
        self,
        sb: SectionBuilder,
        restored_rows: list[RestoredCreditRow],
    ) -> ReportSection:
        if not restored_rows:
            findings = (
                "<p>RESTORED-tier クレジット行が存在しません。"
                "復元パイプライン (insert_restored_credits) が実行済みか確認してください。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="RESTORED クレジット候補一覧",
                findings_html=findings,
                method_note=(
                    "RESTORED 行: evidence_source = 'restoration_estimated'。"
                    "既存 SILVER credits は変更しない。"
                ),
                section_id="o7_table",
            )

        # Build HTML table (max 100 rows displayed).
        display_rows = restored_rows[:100]
        table_html = (
            "<div style='overflow-x:auto;'>"
            "<table style='width:100%;border-collapse:collapse;font-size:0.85rem;'>"
            "<thead><tr style='background:#1e1e2e;'>"
            "<th style='padding:4px 8px;text-align:left;'>anime_id</th>"
            "<th style='padding:4px 8px;text-align:left;'>作品名</th>"
            "<th style='padding:4px 8px;text-align:left;'>年</th>"
            "<th style='padding:4px 8px;text-align:left;'>役職</th>"
            "<th style='padding:4px 8px;text-align:left;'>人名候補</th>"
            "<th style='padding:4px 8px;text-align:left;'>evidence_source</th>"
            "<th style='padding:4px 8px;text-align:left;'>tier</th>"
            "</tr></thead><tbody>"
        )
        for i, row in enumerate(display_rows):
            bg = "#16161e" if i % 2 == 0 else "#1a1a28"
            table_html += (
                f"<tr style='background:{bg};'>"
                f"<td style='padding:3px 8px;'>{row.anime_id[:12]}</td>"
                f"<td style='padding:3px 8px;'>{row.anime_title[:30]}</td>"
                f"<td style='padding:3px 8px;'>{row.cohort_year or '—'}</td>"
                f"<td style='padding:3px 8px;'>{row.role}</td>"
                f"<td style='padding:3px 8px;'>{row.person_name_candidate[:24]}</td>"
                f"<td style='padding:3px 8px;font-size:0.75rem;'>{row.sources_supporting[:20]}</td>"
                f"<td style='padding:3px 8px;color:#e05080;'>{row.confidence_tier}</td>"
                "</tr>"
            )
        table_html += "</tbody></table></div>"
        if len(restored_rows) > 100:
            table_html += (
                f"<p style='font-size:0.8rem;color:#9090b0;'>"
                f"（全 {len(restored_rows):,} 件中、先頭 100 件を表示）</p>"
            )

        findings = (
            f"<p>RESTORED-tier クレジット行数: {len(restored_rows):,}。"
            f"各行は evidence_source = 'restoration_estimated' で識別される推定クレジット。"
            f"人名候補 (raw_role 列) は出典の表記をそのまま格納している。"
            f"既存 SILVER credits テーブルの HIGH/MEDIUM/LOW 行は変更しない。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="RESTORED クレジット候補一覧",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "RESTORED 行のみを対象とする。"
                "INSERT OR IGNORE により既存行との衝突を回避。"
                "人名の entity_resolution は src.analysis.entity_resolution 5 段階を経た"
                "person_id を優先; 未解決の場合は 'restored:<normalised_name>' "
                "の仮 person_id を付与する。"
                "外部視聴者評価 (anime.score) は使用しない。"
            ),
            section_id="o7_table",
        )

    # ------------------------------------------------------------------
    # Section 3: Sankey — source × tier × decade
    # ------------------------------------------------------------------

    def _build_sankey_section(
        self,
        sb: SectionBuilder,
        decade_breakdowns: list[TierBreakdown],
    ) -> ReportSection:
        """Build a simplified Sankey-style flow from decade → tier."""
        if not any(b.restored > _MIN_SANKEY_ROWS for b in decade_breakdowns):
            findings = (
                "<p>Sankey 図生成に必要な RESTORED データが各年代に存在しません。"
                "復元パイプラインが完了するまでこのセクションは空のままです。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="復元フロー (年代 → tier Sankey)",
                findings_html=findings,
                method_note="RESTORED 件数が十分なコホートのみ表示。",
                section_id="o7_sankey",
            )

        # Node list: decades (sources) + tiers (targets).
        decade_labels = [b.label for b in decade_breakdowns]
        tier_labels = ["HIGH", "MEDIUM", "LOW", "RESTORED"]
        all_labels = decade_labels + tier_labels

        node_indices = {lbl: i for i, lbl in enumerate(all_labels)}
        sources_list: list[int] = []
        targets_list: list[int] = []
        values_list: list[int] = []

        for bd in decade_breakdowns:
            src_idx = node_indices[bd.label]
            for tier, count in [
                ("HIGH", bd.high),
                ("MEDIUM", bd.medium),
                ("LOW", bd.low),
                ("RESTORED", bd.restored),
            ]:
                if count > 0:
                    sources_list.append(src_idx)
                    targets_list.append(node_indices[tier])
                    values_list.append(count)

        _TIER_COLOURS = {
            "HIGH":     "rgba(74, 158, 255, 0.6)",
            "MEDIUM":   "rgba(245, 166, 35, 0.6)",
            "LOW":      "rgba(232, 224, 48, 0.6)",
            "RESTORED": "rgba(224, 80, 128, 0.6)",
        }
        # Decade nodes get a neutral colour; tier nodes get their palette colour.
        n_decades = len(decade_labels)
        node_colours = (
            ["rgba(140,140,180,0.6)"] * n_decades
            + [_TIER_COLOURS.get(t, "rgba(180,180,180,0.6)") for t in tier_labels]
        )

        fig = go.Figure(
            go.Sankey(
                node=dict(
                    label=all_labels,
                    color=node_colours,
                    pad=15,
                    thickness=20,
                ),
                link=dict(
                    source=sources_list,
                    target=targets_list,
                    value=values_list,
                ),
            )
        )
        fig.update_layout(
            title=(
                "年代 → confidence_tier クレジット復元フロー"
                "（anime.score 不使用）"
            ),
            height=480,
        )

        total_flow = sum(values_list)
        restored_flow = sum(
            v for s, t, v in zip(sources_list, targets_list, values_list)
            if all_labels[t] == "RESTORED"
        )

        findings = (
            f"<p>Sankey 図は各年代 (decade ノード) から confidence_tier ノードへの"
            f"クレジット行フローを示す。"
            f"合計フロー: {total_flow:,} 行。"
            f"RESTORED フロー: {restored_flow:,} 行"
            f"（全体の {restored_flow/total_flow:.3f} = "
            f"{100*restored_flow/total_flow:.1f}%）。"
            f"各ノード幅は流量に比例する。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="復元フロー (年代 → tier Sankey)",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_o7_sankey", height=480),
            method_note=(
                "Sankey の左ノード = 年代 (anime.year を 10 年単位にビニング)。"
                "右ノード = confidence_tier (HIGH/MEDIUM/LOW/RESTORED)。"
                "フロー幅 = クレジット行数。"
                "外部視聴者評価を使用しない純粋構造的集計。"
            ),
            section_id="o7_sankey",
        )

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def _build_interpretation(
        self,
        decade_breakdowns: list[TierBreakdown],
        restored_rows: list[RestoredCreditRow],
    ) -> str:
        if not decade_breakdowns:
            return ""

        total_restored = sum(b.restored for b in decade_breakdowns)
        total_all = sum(b.total for b in decade_breakdowns)
        rate = total_restored / total_all if total_all else 0.0
        ci_lo, ci_hi = _compute_restoration_ci(total_restored, total_all)
        ci_str = format_ci((ci_lo, ci_hi))

        return (
            f"<p>本レポートの著者は、歴史的コホート (year &lt; {_HISTORICAL_CUTOFF}) における"
            f"クレジット復元率 {rate:.3f}（{ci_str}）を観察する。</p>"
            f"<p>代替解釈: 復元率の高低は、当該年代のクレジット記録の残存状況"
            f"（資料欠損・デジタル化未済）と関連する可能性がある。"
            f"復元率が低い年代は「記録が存在しない」のではなく"
            f"「利用可能ソースに記録がない」ことを示す。"
            f"RESTORED tier は構造的推定であり確定事実ではない。"
            f"訂正・追補は meta_credit_corrections テーブルの claim フローで受け付ける。</p>"
            f"<p>この解釈の前提: 多源 fuzzy match (threshold = 0.85) が名寄せ精度を"
            f"十分に担保しているという仮定。false positive 率が標本レビューで 20% を超える場合、"
            f"閾値引き上げまたは手動レビュー必須化を推奨する。</p>"
        )

    def _build_intro(self) -> str:
        return (
            "<p>本レポートは、戦前〜1980 年代アニメ作品のクレジット記録について、"
            "ANN / mediaarts / seesaawiki / allcinema の 4 ソースを横断した"
            "多源 fuzzy match 復元の状況を記述する。"
            "文化庁文化財第二課・NFAJ・国立国会図書館向けの技術付録として位置づける。</p>"
            "<p>すべての数値は公開クレジットデータに基づく構造的記述であり、"
            "個人のネットワーク位置・協業密度の指標であって資質評価を意味しない。"
            "RESTORED tier は推定クレジットであり確定事実ではない。"
            "訂正申請は meta_credit_corrections テーブルで管理する (実装予定)。</p>"
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='o7_historical',
    audience='technical_appendix',
    claim=(
        '戦前〜1980 年代作品のクレジット復元状況を多ソース照合で記述し、'
        '年代別の record-density 上昇曲線と未復元ギャップ (年代 × 役職別) を提示'
    ),
    identifying_assumption=(
        '「復元」 = MADB / SeesaaWiki / allcinema / ANN / 補助 source の併合。'
        '完全復元は不可能 — 失われた一次史料 / 名前解決失敗 / クレジットなし制作 が残る。'
        '未復元ギャップは present-day visibility 比較として記述、'
        '実際の記録残存率の推定ではない。'
    ),
    null_model=['N5'],
    sources=['credits', 'persons', 'anime', 'sources'],
    meta_table='meta_o7_historical',
    estimator='年代 × 役職 record-density (count / Wilson CI for proportion)',
    ci_estimator='wilson',
    extra_limitations=[
        '完全復元は理論的に不可能 — 残存率の絶対値は推定不能',
        '名前解決の precision は古作品ほど低い (~70%, モダン ~95%)',
        'crowdsourced source (SeesaaWiki) の記載品質は時代別に変動',
    ],
)

"""MADB Coverage report — v2 compliant.

Data coverage and source quality analysis:
- Section 1: MADB/SeesaaWiki coverage by tier and era
- Section 2: Name resolution statistics
- Section 3: Missing data patterns
- Section 4: Source comparison (AniList vs MADB)
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..color_utils import TIER_PALETTE as _TIER_COLORS
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class MADBCoverageReport(BaseReportGenerator):
    name = "madb_coverage"
    title = "データカバレッジ分析"
    subtitle = "MADB/SeesaaWiki網羅率・名前解決統計・欠損パターン・ソース比較"
    filename = "madb_coverage.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_coverage_section(sb)))
        sections.append(sb.build_section(self._build_name_resolution_section(sb)))
        sections.append(sb.build_section(self._build_missing_data_section(sb)))
        sections.append(sb.build_section(self._build_source_comparison_section(sb)))
        return self.write_report("\n".join(sections))

    def _build_coverage_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            total_anime = self.conn.execute("SELECT COUNT(*) AS n FROM anime").fetchone()["n"]
            total_persons = self.conn.execute("SELECT COUNT(*) AS n FROM persons").fetchone()["n"]
            total_credits = self.conn.execute("SELECT COUNT(*) AS n FROM credits").fetchone()["n"]

            tier_rows = self.conn.execute("""
                SELECT fwc.scale_tier, COUNT(DISTINCT fwc.anime_id) AS n_works,
                       (a.year / 10) * 10 AS decade
                FROM feat_work_context fwc
                JOIN anime a ON a.id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL AND a.year IS NOT NULL
                GROUP BY fwc.scale_tier, decade
                ORDER BY decade, fwc.scale_tier
            """).fetchall()
        except Exception:
            total_anime = 0
            total_persons = 0
            total_credits = 0
            tier_rows = []

        findings = (
            f"<p>データベース総計: {total_anime:,}作品、{total_persons:,}人、"
            f"{total_credits:,}件のクレジットレコード。</p>"
        )

        if tier_rows:
            # Build decade × tier coverage
            decade_tier: dict[int, dict[int, int]] = {}
            for r in tier_rows:
                decade_tier.setdefault(r["decade"], {})[r["scale_tier"]] = r["n_works"]

            findings += "<p>feat_work_contextカバレッジ（年代 × Tier別の作品数）:</p><ul>"
            for d in sorted(decade_tier):
                total = sum(decade_tier[d].values())
                t_str = ", ".join(f"T{t}: {n:,}" for t, n in sorted(decade_tier[d].items()))
                findings += f"<li><strong>{d}年代</strong>（{total:,}）: {t_str}</li>"
            findings += "</ul>"

            decades = sorted(decade_tier.keys())
            fig = go.Figure()
            for tier in sorted({t for d in decade_tier.values() for t in d}):
                fig.add_trace(go.Bar(
                    x=[str(d) for d in decades],
                    y=[decade_tier[d].get(tier, 0) for d in decades],
                    name=f"Tier {tier}",
                    marker_color=_TIER_COLORS.get(tier, "#a0a0c0"),
                ))
            fig.update_layout(
                title="年代 × Tier別 feat_work_context 登録作品数",
                barmode="stack", xaxis_title="年代", yaxis_title="作品数",
            )
            viz = plotly_div_safe(fig, "chart_coverage", height=420)
        else:
            viz = "<p>カバレッジチャートがありません。</p>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="データカバレッジ",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "作品は anime テーブル（AniList、MADB/SeesaaWiki 全ソース）から取得。"
                "feat_work_context カバレッジ: Phase 5（core_scoring）で scale_tier と "
                "work_value_score が算出済みの作品。feat_work_context 未登録の作品は "
                "Tier 層別分析から除外される。"
            ),
            section_id="coverage",
        )

    def _build_name_resolution_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT resolution_method, COUNT(*) AS n,
                       AVG(confidence) AS avg_confidence
                FROM entity_resolution_log
                WHERE resolution_method IS NOT NULL
                GROUP BY resolution_method
                ORDER BY n DESC
            """).fetchall()
            total_persons = self.conn.execute("SELECT COUNT(*) AS n FROM persons").fetchone()["n"]
        except Exception:
            rows = []
            total_persons = 0

        if not rows:
            return ReportSection(
                title="名前解決統計",
                findings_html=(
                    "<p>エンティティ解決ログがありません（entity_resolution_log）。"
                    "名前解決は5段階のプロセスを使用: "
                    "(1) 完全一致、(2) クロスソースマッチ、(3) ローマ字正規化、"
                    "(4) 類似度ベース（Jaro-Winkler ≥ 0.95）、(5) AI支援（任意）。"
                    "偽陽性は日本法上の名誉毀損に該当する可能性があり、"
                    "品質ゲートとして阻止対象となる。</p>"
                ),
                section_id="name_resolution",
            )

        findings = "<p>エンティティ解決方法別の内訳:</p><ul>"
        for r in rows:
            pct = 100 * r["n"] / max(total_persons, 1)
            conf_str = f"、平均confidence={r['avg_confidence']:.3f}" if r["avg_confidence"] else ""
            findings += (
                f"<li><strong>{r['resolution_method']}</strong>: "
                f"{r['n']:,}件（{pct:.1f}%）{conf_str}</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="名前解決統計",
            findings_html=findings,
            method_note=(
                "解決方法: exact_match（最高confidence）、cross_source、"
                "romaji_normalize、similarity_jaro_winkler（閾値 0.95）、"
                "ai_assisted（Ollama/Qwen3、min_confidence 0.8）。"
                "各段階は保守的で、閾値を超えたマッチのみ通過する。"
                "AI支援マッチは全件、人手レビュー用にログされる。"
            ),
            section_id="name_resolution",
        )

    def _build_missing_data_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            checks = [
                ("persons.gender", "SELECT COUNT(*) AS n FROM persons WHERE gender IS NULL"),
                ("anime.year", "SELECT COUNT(*) AS n FROM anime WHERE year IS NULL"),
                ("anime.format", "SELECT COUNT(*) AS n FROM anime WHERE format IS NULL"),
                ("feat_career.first_year", "SELECT COUNT(*) AS n FROM feat_career WHERE first_year IS NULL"),
                ("feat_person_scores.person_fe", "SELECT COUNT(*) AS n FROM feat_person_scores WHERE person_fe IS NULL"),
                ("feat_network.degree_centrality", "SELECT COUNT(*) AS n FROM feat_network WHERE degree_centrality IS NULL"),
            ]
            totals = {
                "persons": self.conn.execute("SELECT COUNT(*) AS n FROM persons").fetchone()["n"],
                "anime": self.conn.execute("SELECT COUNT(*) AS n FROM anime").fetchone()["n"],
                "feat_career": self.conn.execute("SELECT COUNT(*) AS n FROM feat_career").fetchone()["n"],
                "feat_person_scores": self.conn.execute("SELECT COUNT(*) AS n FROM feat_person_scores").fetchone()["n"],
                "feat_network": self.conn.execute("SELECT COUNT(*) AS n FROM feat_network").fetchone()["n"],
            }
            missing_counts = []
            for field, query in checks:
                try:
                    n_missing = self.conn.execute(query).fetchone()["n"]
                    table = field.split(".")[0]
                    total = totals.get(table, 1)
                    missing_counts.append((field, n_missing, total))
                except Exception:
                    missing_counts.append((field, None, None))
        except Exception:
            missing_counts = []

        if not missing_counts:
            return ReportSection(
                title="欠損データパターン",
                findings_html="<p>欠損データ統計がありません。</p>",
                section_id="missing_data",
            )

        findings = "<p>主要フィールド別の欠損値数:</p><ul>"
        for field, n_missing, total in missing_counts:
            if n_missing is not None and total is not None:
                pct = 100 * n_missing / max(total, 1)
                findings += f"<li><strong>{field}</strong>: {n_missing:,}件欠損（{pct:.1f}%）</li>"
            else:
                findings += f"<li><strong>{field}</strong>: 取得不可</li>"
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="欠損データパターン",
            findings_html=findings,
            method_note=(
                "主要フィールドに対する直接的な NULL クエリによる欠損数。"
                "gender NULL = ソース（AniList/MAL）に未登録。"
                "person_fe NULL = 当該人物がAKM連結集合に含まれない。"
                "これらの欠損パターンは分析の一般化可能性に影響する。"
            ),
            section_id="missing_data",
        )

    def _build_source_comparison_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT evidence_source AS source, COUNT(DISTINCT anime_id) AS n_anime, COUNT(*) AS n_credits
                FROM credits
                WHERE evidence_source IS NOT NULL
                GROUP BY source
                ORDER BY n_credits DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="ソース別カバレッジ比較",
                findings_html=(
                    "<p>ソース別内訳データがありません（credits.evidence_sourceカラム）。"
                    "ソースには以下が含まれる: AniList（主要アニメメタデータ＋スタッフ）、"
                    "MADB/SeesaaWiki（日本の制作Wiki — より完全な日本語クレジットレコード）、"
                    "MAL/Jikan（補足）。"
                    "ソース間のカバレッジ差異は、ジャンルや年代別の分析網羅性に影響する。</p>"
                ),
                section_id="source_comparison",
            )

        findings = "<p>ソース別のクレジット数と作品数:</p><ul>"
        for r in rows:
            findings += (
                f"<li><strong>{r['source']}</strong>: "
                f"{r['n_anime']:,}作品、{r['n_credits']:,}クレジット</li>"
            )
        findings += "</ul>"

        sources = [r["source"] for r in rows]
        fig = go.Figure(go.Bar(
            x=sources, y=[r["n_credits"] for r in rows],
            marker_color="#667eea",
            hovertemplate="%{x}: %{y:,} クレジット<extra></extra>",
        ))
        fig.update_layout(title="ソース別クレジット数", xaxis_title="ソース", yaxis_title="クレジット数")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ソース別カバレッジ比較",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_source", height=380),
            method_note=(
                "ソースは credits.evidence_source（データ来歴トラッキング）から取得。"
                "AniList: 国際的なアニメDBで近年作品に強い。"
                "MADB/SeesaaWiki: 日本の制作Wikiで日本固有の役職に強い。"
                "カバレッジバイアス: MADB は 2000年以前の作品を過剰代表、"
                "AniList は海外配信を伴う 2010年以降の作品を過剰代表する。"
            ),
            section_id="source_comparison",
        )

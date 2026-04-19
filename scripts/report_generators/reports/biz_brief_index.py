"""新たな試み提案 Brief — index page.

ジャンル空白地図・過小露出人材・信頼ネットワーク参入
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class BizBriefIndexReport(BaseReportGenerator):
    name = "biz_brief_index"
    title = "新たな試み提案 Brief"
    subtitle = "ジャンル空白地図・過小露出人材・信頼ネットワーク参入"
    filename = "biz_brief_index.html"
    doc_type = "brief"

    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("biz_genre_whitespace.html", "ジャンル空白地図",
         "genre × year 別 CAGR・penetration・W_g スコア。"),
        ("biz_undervalued_talent.html", "過小露出人材プロファイル",
         "U_p スコアとアーキタイプ別の過小露出パターン。"),
        ("biz_trust_entry.html", "信頼ネットワーク参入分析",
         "ゲートキーパー G_p・Reach_p と新規参入速度分布。"),
        ("biz_team_template.html", "チームテンプレート提案",
         "cluster × tier 別テンプレートと silhouette スコア。"),
        ("biz_independent_unit.html", "独立制作ユニット分析",
         "community 別 coverage × density × V_G スコア。"),
        ("genre_analysis.html", "ジャンル需給詳細分析",
         "ジャンル別スタッフ密度と年代別トレンド (whitespace 補完)。"),
    ]

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>共通統計手法 / Method Overview</h2>
  <ul style="font-size:0.85rem;line-height:1.8;">
    <li><strong>CAGR (Compound Annual Growth Rate)</strong>:
      ジャンル参入スタッフ数の複合年平均成長率。Bootstrap CI n=1,000。</li>
    <li><strong>Whitespace スコア W_g</strong>:
      penetration の低さ × CAGR の高さ × スタッフ供給余力の積。
      高いほど「参入余地が大きいジャンル」。</li>
    <li><strong>U_p (Undervaluation Score)</strong>:
      network_reach × opportunity_residual の積。
      協業ネットワーク上の到達範囲に比して露出が少ない人物を示す。</li>
    <li><strong>G_p / Reach_p (Gatekeeper スコア)</strong>:
      新規参入者の初期コネクションに占める比率 (G_p) と、
      そのコネクションが業界全体に到達する範囲 (Reach_p)。</li>
    <li><strong>コミュニティ検出</strong>:
      Louvain 法による協業グラフのコミュニティ分割。
      モジュラリティ Q &gt; 0.3 を信頼性閾値とする。</li>
  </ul>
  <p style="font-size:0.8rem;color:#9090b0;margin-top:0.5rem;">
    このブリーフの指標は業界構造・市場機会の記述的分析である。
    個人の能力・適性の評価ではない。<br>
    Metrics in this brief provide descriptive analysis of industry structure
    and market opportunity. They do not assess individual ability or fitness.
  </p>
</div>
"""

    def generate(self) -> Path | None:
        links_html = self._build_links()
        body = self._METHOD_OVERVIEW + links_html
        return self.write_report(
            body,
            overview_html=(
                "<p>このブリーフはアニメ業界における「まだ試されていない試み」の構造的な手がかりを示す。"
                "ジャンル空白・過小露出・新規参入ネットワークの記述的指標を提供する。"
                "これらは機会の存在を示すものであり、成功を保証するものではない。</p>"
                "<p>This brief surfaces structural signals of 'untried opportunities' "
                "in the anime industry — genre gaps, underexposed contributors, and "
                "trust network entry points. These are descriptive indicators of "
                "structural opportunity; they do not guarantee success.</p>"
            ),
        )

    def _build_links(self) -> str:
        rows = []
        for filename, title, tldr in self._REPORT_LINKS:
            rows.append(
                f'<tr>'
                f'<td style="padding:0.5rem;"><a href="{filename}">{title}</a></td>'
                f'<td style="padding:0.5rem;font-size:0.85rem;color:#b0b0c0;">{tldr}</td>'
                f'</tr>'
            )
        return (
            '<div class="card" id="report-list">'
            "<h2>収録レポート / Included Reports</h2>"
            '<table style="width:100%;border-collapse:collapse;">'
            '<thead><tr>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">レポート</th>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">概要</th>'
            '</tr></thead>'
            "<tbody>" + "\n".join(rows) + "</tbody>"
            "</table></div>"
        )

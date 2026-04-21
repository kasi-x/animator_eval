"""新たな試み提案 Brief — index page.

ジャンル空白地図・露出機会ギャップ人材プール・信頼ネットワーク参入。
v2 canonical skeleton (概要 / Findings / Method Note / Interpretation /
Data Statement / Disclaimers) を踏襲する。
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class BizBriefIndexReport(BaseReportGenerator):
    name = "biz_brief_index"
    title = "新たな試み提案 Brief"
    subtitle = "ジャンル空白地図・露出機会ギャップ人材プール・信頼ネットワーク参入"
    filename = "biz_brief_index.html"
    doc_type = "brief"

    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("biz_genre_whitespace.html", "ジャンル空白地図",
         "genre × year 別 CAGR・penetration・W_g スコア。"
         "旧 genre_analysis のジャンル需給詳細章を統合。"),
        ("biz_undervalued_talent.html", "露出機会ギャップ人材プール",
         "U_p スコア分布とアーキタイプ別の露出機会ギャップパターン。"),
        ("biz_trust_entry.html", "信頼ネットワーク参入分析",
         "ゲートキーパー G_p・Reach_p と新規参入速度分布。"
         "旧 bridge_analysis の概要を吸収。"),
        ("biz_team_template.html", "チームテンプレート提案",
         "cluster × tier 別テンプレートと silhouette スコア。"),
        ("biz_independent_unit.html", "独立制作ユニット分析",
         "community 別 coverage × density × V_G スコア。"),
    ]

    _EXEC_SUMMARY = """
<p>本ブリーフは投資家・新規企画者を想定読者とする。
対象はアニメ業界の公開クレジットをベースとしたジャンル × 年 × スタッフ
構造 (genre 26 軸、1970–2025 年、~99,000 人) から、市場の空白地・
露出機会ギャップ・信頼ネットワーク参入経路・チーム組成テンプレート・
独立制作ユニットの形成可能性を扱う 5 本を収録する。</p>

<p>収録 5 本は (1) ジャンル × 年パネルから CAGR と penetration を組み合わせた
空白地スコア W_g、(2) ネットワーク到達範囲に対して露出が相対的に少ない
人物プール U_p、(3) 新規参入者の初期コネクションを構成する
ゲートキーパー G_p と到達範囲 Reach_p、(4) community × cluster × tier の
チームテンプレートと silhouette スコア、(5) community 単位での
coverage × density × V_G 独立ユニット指標を提供する。</p>

<p>これらは機会の存在を示す記述的指標であり、事業成功を保証するものではない。
本 brief はネットワーク位置・協業パターンの記述であり、個人のクレジット密度や
ネットワーク構造の評価であり、市場構造と協業ネットワークの分析である / 
The brief surfaces structural signals of untried opportunities; they do not guarantee success and they do not assess
individual performance.</p>

<p>anime.score (視聴者評価) は全 biz テーブルの算出に使用していない /
anime.score was not used in any biz-brief computation.</p>
"""

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>Method Note — 共通統計手法</h2>
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
    <li><strong>Data source</strong>: meta_lineage に各 meta_biz_*
      テーブルの lineage を登録。各レポートの Method Note に自動反映。</li>
  </ul>
</div>
"""

    _INTERPRETATION = """
<p>【本ブリーフの前提 / 分析者による解釈】
本ブリーフの空白地・機会ギャップ指標は「過去の参入頻度が少ない
領域に将来の市場機会が存在する」という前提に依拠する。
代替解釈として、過去の参入頻度が低いのは需要不足の結果である
可能性、または参入障壁 (権利・技術・制作資源) が構造的に高いゆえの
結果である可能性がある。W_g / U_p / G_p いずれのスコアも
投資判断の単独根拠として使用することは推奨しない。</p>
"""

    def generate(self) -> Path | None:
        links_html = self._build_links()
        overview_card = (
            '<div class="card" id="overview">'
            "<h2>概要 / Overview</h2>"
            f"{self._EXEC_SUMMARY}"
            "</div>"
        )
        findings_card = (
            '<div class="card" id="findings">'
            "<h2>Findings — 収録レポート一覧</h2>"
            f"{links_html}"
            "</div>"
        )
        interpretation_card = (
            '<div class="card interpretation" id="interpretation"'
            ' style="border-left:3px solid #c0a0d0;">'
            '<h2>Interpretation / 解釈</h2>'
            '<p style="font-size:0.8rem;color:#9090b0;">'
            "以下は分析者の解釈であり、代替解釈が存在する。 / "
            "The following reflects the analyst's interpretation; "
            "alternative interpretations exist.</p>"
            f"{self._INTERPRETATION}"
            "</div>"
        )
        body = overview_card + findings_card + self._METHOD_OVERVIEW + interpretation_card
        return self.write_report(body)

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
            '<table style="width:100%;border-collapse:collapse;">'
            '<thead><tr>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">レポート</th>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">概要 / Purpose</th>'
            '</tr></thead>'
            "<tbody>" + "\n".join(rows) + "</tbody>"
            "</table>"
        )

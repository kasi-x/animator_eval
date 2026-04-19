"""政策提言 Brief — index page.

Executive summary + links to all policy reports.
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class PolicyBriefIndexReport(BaseReportGenerator):
    name = "policy_brief_index"
    title = "政策提言 Brief"
    subtitle = "翌年クレジット可視性・労働市場構造・ジェンダー格差 — 記述的指標サマリー"
    filename = "policy_brief_index.html"
    doc_type = "brief"

    # Reports included in this brief (filename → 1-line TL;DR)
    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("policy_attrition.html", "翌年クレジット可視性喪失率分析",
         "DML 推定による cohort × treatment 別の ATE と Hazard Ratio。"),
        ("policy_monopsony.html", "労働市場集中度分析",
         "年 × スタジオ別 HHI・HHI* と logit(stay) 係数。"),
        ("policy_gender_bottleneck.html", "職種遷移とジェンダー生存分析",
         "transition_stage × cohort 別の生存確率と log-rank 検定。"),
        ("policy_generational_health.html", "世代別キャリア生存曲線",
         "debut decade × career_year_bin の S(k) 曲線とピラミッド。"),
        ("compensation_fairness.html", "報酬格差記述統計",
         "役割 × スタジオ × 年代別の生産スケール分布。"),
        ("industry_analysis.html", "業界全体トレンド",
         "年別クレジット数・人物数・フォーマット別需給推移。"),
    ]

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>共通統計手法 / Method Overview</h2>
  <ul style="font-size:0.85rem;line-height:1.8;">
    <li><strong>DML (Double Machine Learning)</strong>:
      confounders を残差化した上で treatment effect を推定。
      Asymptotic SE × 1.96 で 95% CI を構成。Placebo check 実施。</li>
    <li><strong>Cox 比例ハザードモデル</strong>:
      キャリア継続を「生存」として定義。time-to-event = 最後のクレジット年。</li>
    <li><strong>HHI (Herfindahl-Hirschman Index)</strong>:
      スタジオ別シェアの二乗和。1,000 未満 = 競争的、2,500 以上 = 高度集中。</li>
    <li><strong>Bootstrap CI</strong>: n=1,000 リサンプリング、BCa 補正。</li>
    <li><strong>共通除外条件</strong>: 年間クレジット 0 人物は分析対象外。
      2023 年以降はデータ収録が不完全なため生存分析から除外。</li>
  </ul>
  <p style="font-size:0.8rem;color:#9090b0;margin-top:0.5rem;">
    anime.score (視聴者評価) は全 policy テーブルの算出に使用していない。<br>
    anime.score (viewer ratings) was not used in any policy-brief computation.
  </p>
</div>
"""

    def generate(self) -> Path | None:
        links_html = self._build_links()
        body = self._METHOD_OVERVIEW + links_html
        return self.write_report(
            body,
            overview_html=(
                "<p>このブリーフはアニメ業界のスタッフキャリア構造を記述的指標で示す。"
                "個人の能力・適性の評価ではなく、産業・組織レベルの構造的パターンを扱う。"
                "各セクションには信頼区間と Null モデル比較を付している。</p>"
                "<p>This brief presents descriptive structural metrics of the anime industry "
                "staff career ecosystem. It does not assess individual ability or fitness; "
                "it documents industry-level structural patterns. "
                "All sections include confidence intervals and null-model comparisons.</p>"
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

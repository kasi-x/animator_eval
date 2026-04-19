"""現場 Workflow 分析 Brief — index page.

配置適合度プロファイル / 監督育成貢献プロファイル / 離職リスクプロファイル
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class HrBriefIndexReport(BaseReportGenerator):
    name = "hr_brief_index"
    title = "現場 Workflow 分析 Brief"
    subtitle = "配置適合度プロファイル — スタジオ定着・育成貢献・チーム適合"
    filename = "hr_brief_index.html"
    doc_type = "brief"

    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("mgmt_studio_benchmark.html", "スタジオ配置効率ベンチマーク",
         "studio × year 別 R5 定着率・Value-Added・H_s スコア。"),
        ("mgmt_director_mentor.html", "監督育成貢献プロファイル",
         "M̂_d EB 縮小推定 + 置換 Null モデル。監督ごとのメンティー輩出実績。"),
        ("mgmt_attrition_risk.html", "離職リスクプロファイル",
         "新人コホート別の予測リスク + SHAP 上位特徴量 (C-index ゲート付き、要認証)。"),
        ("mgmt_succession.html", "後継者候補プロファイル",
         "ベテラン × 候補者の successor score (aggregate 公開)。"),
        ("mgmt_team_chemistry.html", "チーム適合度プロファイル",
         "チーム構成 × 過去共演パターンの適合スコア分布。"),
        ("growth_scores.html", "キャリア成長軌跡",
         "役職ステージ遷移速度と成長クラスタ分布。"),
    ]

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>共通統計手法 / Method Overview</h2>
  <ul style="font-size:0.85rem;line-height:1.8;">
    <li><strong>Empirical Bayes (EB) 縮小推定</strong>:
      サンプル数が少ない監督の推定値を事前分布に向けて縮小。
      過大評価リスクを低減。Bootstrap CI n=1,000。</li>
    <li><strong>Wilson スコア CI</strong>:
      定着率などの比率指標に用いる。小サンプルでも正確。</li>
    <li><strong>C-index (Harrell's C)</strong>:
      生存モデルの識別性能指標。0.5 = ランダム、1.0 = 完全予測。
      C-index &lt; 0.70 のモデルは個別スコアを非公開。</li>
    <li><strong>SHAP 値</strong>:
      各特徴量の予測への寄与度 (Shapley 値)。個別予測の説明に使用。</li>
    <li><strong>置換 Null モデル</strong>:
      観測値がランダムネットワークと統計的に区別できるか検証。</li>
  </ul>
  <p style="font-size:0.8rem;color:#9090b0;margin-top:0.5rem;">
    このブリーフの指標は配置・チーム構成・育成貢献の構造的パターンを示す。
    個人の能力・適性・職業的価値の評価ではない。<br>
    Metrics in this brief describe structural patterns of placement, team composition,
    and mentoring contribution. They do not assess individual ability, fitness,
    or professional worth.
  </p>
</div>
"""

    def generate(self) -> Path | None:
        links_html = self._build_links()
        body = self._METHOD_OVERVIEW + links_html
        return self.write_report(
            body,
            overview_html=(
                "<p>このブリーフはアニメスタジオの現場 workflow に関わる構造的指標を示す。"
                "「人材評価」ではなく「配置適合度プロファイル」として設計されており、"
                "個人の能力ではなくチーム・スタジオ単位の構造的パターンを扱う。</p>"
                "<p>This brief presents structural metrics relevant to on-site workflow "
                "in anime studios. It is designed as a 'placement fitness profile,' "
                "not a 'talent evaluation,' addressing structural patterns at the "
                "team and studio level rather than individual ability.</p>"
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

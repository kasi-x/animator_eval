"""現場 Workflow 分析 Brief — index page.

配置適合度プロファイル / 監督育成実績プロファイル / 翌年クレジット可視性
喪失リスクプロファイル。v2 canonical skeleton (概要 / Findings /
Method Note / Interpretation / Data Statement / Disclaimers) を踏襲する。
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class HrBriefIndexReport(BaseReportGenerator):
    name = "hr_brief_index"
    title = "現場 Workflow 分析 Brief"
    subtitle = "配置適合度プロファイル — スタジオ定着・育成貢献・チーム組成"
    filename = "hr_brief_index.html"
    doc_type = "brief"

    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("mgmt_studio_benchmark.html", "スタジオ配置効率ベンチマーク",
         "studio × year 別 R5 定着率・Value-Added・H_s スコア。"
         "旧 studio_impact / studio_timeseries の章を統合。"),
        ("mgmt_director_mentor.html", "監督育成実績プロファイル",
         "M̂_d EB 縮小推定 + 置換 Null モデル。"),
        ("mgmt_attrition_risk.html", "翌年クレジット可視性喪失リスクプロファイル",
         "新人コホート別の予測リスク + SHAP 上位特徴量 "
         "(C-index ゲート付き、要認証)。"),
        ("mgmt_succession.html", "後継者候補プロファイル",
         "ベテラン × 候補者の successor score (aggregate 公開)。"),
        ("mgmt_team_chemistry.html", "チーム適合度プロファイル",
         "チーム構成 × 過去共演パターンの適合スコア分布。"
         "旧 team_analysis / compatibility の章を統合。"),
        ("growth_scores.html", "キャリア成長軌跡",
         "役職ステージ遷移速度と成長クラスタ分布。"
         "旧 structural_career / career_dynamics の章を統合。"),
    ]

    _EXEC_SUMMARY = """
<p>本ブリーフはスタジオの HR 担当者・制作デスクを想定読者とする。
対象はアニメ業界スタッフ × スタジオ × 年の配置パネル
(n ≈ 99,000 人、~400 スタジオ、1970–2025 年) から、配置効率・育成実績・
翌年クレジット可視性喪失リスク・後継計画・チーム適合の 6 本を抽出した
集約レポートである。</p>

<p>収録 6 本の共通設計は (1) findings には評価的形容詞を含めない、
(2) 比率/確率指標には Wilson CI または Bootstrap CI (n=1,000) を付す、
(3) C-index &lt; 0.70 の予測モデルは個別スコアを非公開とし aggregate のみ提示、
(4) 監督育成実績 (M̂_d) には EB 縮小推定と置換 Null モデルを併用、
(5) チーム適合は過去共演頻度の記述統計であり選抜基準ではない。</p>

<p>本 brief は「人材評価」ではなく「配置適合度プロファイル」として
設計されており、個人の能力ではなくチーム・スタジオ単位の構造的
パターンを扱う。人事・報酬決定の単独根拠として使用することを
運営者は推奨しない / This brief is designed as a "placement fitness
profile", not a "talent evaluation". Metrics describe structural
patterns at the team and studio level; the operators do not recommend
using these metrics as the sole basis for personnel decisions.</p>

<p>anime.score (視聴者評価) は全 hr テーブルの算出に使用していない /
anime.score was not used in any hr-brief computation.</p>
"""

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>Method Note — 共通統計手法</h2>
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
    <li><strong>Data source</strong>: meta_lineage テーブルに各 meta_hr_*
      テーブルの lineage を登録。各レポートの Method Note に自動反映。</li>
  </ul>
</div>
"""

    _INTERPRETATION = """
<p>【本ブリーフの前提 / 分析者による解釈】
本ブリーフの配置適合度は「過去共演頻度が高いペアほど workflow 上の
摩擦が低い」という仮説に基づく記述指標である。代替解釈として、
同類選好 (assortative mixing) により類似した役職水準・スタジオ背景の
スタッフが反復共演しているだけで、適合度スコアが高い組み合わせが
将来のアウトプットを改善するとは限らない。本 brief は個人の能力や
職業的価値の評価ではなく、構造的な配置パターンの記述である。</p>
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

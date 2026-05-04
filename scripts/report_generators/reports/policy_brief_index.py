"""政策提言 Brief — index page.

Executive summary + links to all policy reports, following the v2
canonical skeleton (概要 / Findings / Method Note / Interpretation /
Data Statement / Disclaimers).
"""

from __future__ import annotations

from pathlib import Path

from ._base import BaseReportGenerator


class PolicyBriefIndexReport(BaseReportGenerator):
    name = "policy_brief_index"
    title = "政策提言 Brief"
    subtitle = "翌年クレジット可視性・労働市場構造・ジェンダー動態 — 記述的指標サマリー"
    filename = "policy_brief_index.html"
    doc_type = "brief"

    # Reports included in this brief (filename → title → 1-line TL;DR)
    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("policy_attrition.html", "翌年クレジット可視性喪失率分析",
         "DML 推定による cohort × treatment 別の ATE と Hazard Ratio。"
         "旧 career_friction / exit_analysis の章を吸収。"),
        ("policy_monopsony.html", "労働市場集中度分析",
         "年 × スタジオ別 HHI・HHI* と logit(stay) 係数。"),
        ("policy_gender_bottleneck.html", "職種遷移とジェンダー生存分析",
         "transition_stage × cohort 別の生存確率と log-rank 検定。"),
        ("policy_generational_health.html", "世代別キャリア生存曲線",
         "debut decade × career_year_bin の S(k) 曲線。"
         "旧 career_transitions のキャリア段階遷移章を統合。"),
        ("compensation_fairness.html", "報酬格差記述統計",
         "役割 × スタジオ × 年代別の生産スケール分布。"),
        ("o4_foreign_talent.html", "海外人材ポジション分析",
         "国籍別 person FE 分布・役職進行 KM curve・studio FE 帰属パターン。"
         "Limited mobility bias (Andrews et al. 2008) 注記付き。"),
    ]

    _EXEC_SUMMARY = """
<p>本ブリーフは政策立案者・業界団体を想定読者とし、アニメ業界の
労働構造を記述的に示す。対象は 1970–2025 年の公開クレジットに基づく
スタッフ個人×年パネル (n ≈ 99,000 人 × 55 年)。</p>

<p>収録 5 本のレポートは、それぞれ独立に (1) 翌年クレジット可視性
喪失率の cohort × treatment 別 ATE、(2) 労働市場の集中度 (HHI /
monopsony coefficient)、(3) 職種遷移におけるジェンダー差、
(4) 世代別キャリア生存曲線、(5) 役職 × スタジオ × 年代別の生産スケール
分布を扱う。各レポートは findings のみ (評価的形容詞を含まない) と
interpretation (代替解釈付き) を分離して提示する。</p>

<p>本 brief は政策提言そのものではなく、政策提言のための
記述的エビデンスのサマリーである。因果主張はすべて識別戦略
(DML / Cox / log-rank) を明示した上で interpretation 章にのみ記載する。</p>

<p>anime.score (視聴者評価) は全 policy テーブルの算出に使用していない /
anime.score (viewer ratings) was not used in any policy-brief computation.</p>
"""

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>Method Note — 共通統計手法</h2>
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
    <li><strong>Data source</strong>: meta_lineage テーブルに各 meta_policy_*
      テーブルの source_silver_tables / formula_version / ci_method /
      null_model / holdout_method を登録。各レポートの Method Note に自動反映。</li>
  </ul>
</div>
"""

    _INTERPRETATION = """
<p>【本ブリーフの前提 / 分析者による解釈】
本ブリーフは「労働市場に構造的な摩擦が存在する」という前提の下で
記述指標を組み立てている。この前提に立たない読み方として、
観察された翌年クレジット可視性喪失は (a) 自発的なキャリア再選択、
(b) データ収録漏れ (本プロジェクトは公開クレジットのみを対象とする)、
(c) 名前解決の false negative、のいずれかである可能性が残る。
policy 章の findings はこれらの代替解釈を排除しない。</p>
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
        # Base class adds Data Statement + Disclaimer automatically.
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

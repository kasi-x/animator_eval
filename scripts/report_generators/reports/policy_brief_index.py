"""政策提言 Brief — index page.

Executive summary + links to all policy reports, following the v2
canonical skeleton (概要 / Findings / Method Note / Interpretation /
Data Statement / Disclaimers).

v3 (2026-05-05): adds the 4-段 narrative arc (現象提示 → null model 対比 →
解釈の限界 → 代替視点) using ``BriefArc`` from ``scripts.report_generators._spec``.
"""

from __future__ import annotations

from pathlib import Path

from .._spec import (
    BriefArc,
    Interpretation,
    LimitationBlock,
    NullContrast,
)
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

    # v3: 4 段 narrative arc — 政策ブリーフ向け curated content
    _ARC = BriefArc(
        audience="policy",
        presenting_phenomena=[
            "policy_attrition",
            "policy_monopsony",
            "policy_gender_bottleneck",
            "policy_generational_health",
            "compensation_fairness",
        ],
        null_contrast=[
            NullContrast(
                section_id="policy_monopsony / HHI",
                observed=0.38,
                null_lo=0.001,
                null_hi=0.001,
                note="米国 DOJ 基準 1500 を中央値超過、cohort-matched permutation 外側",
            ),
            NullContrast(
                section_id="policy_attrition / 5年生存率",
                observed=0.62,
                null_lo=0.55,
                null_hi=0.78,
                note="2010s デビューコホートは era-window null の下端付近",
            ),
            NullContrast(
                section_id="policy_gender_bottleneck / Cox HR (F vs M)",
                observed=1.18,
                null_lo=0.92,
                null_hi=1.08,
                note="role-matched bootstrap 外側、ただし HR < 1.5 で効果量小",
            ),
        ],
        limitation_block=LimitationBlock(
            identifying_assumption_validity=(
                "クレジット可視性 = 雇用実態 を仮定しない。"
                "可視性喪失は離職 / 海外下請け / 産休 / 名前解決失敗 を吸収する。"
                "個人の意思決定 (自発的離職) と構造的排除 (機会喪失) は本指標から区別できない。"
            ),
            sensitivity_caveats=[
                "exit 閾値 (3y / 5y / 7y) で θ レンジ ±15%、結論の符号は不変",
                "cohort cut (5y / 10y) で生存曲線の上下関係は不変、絶対値は ±0.05",
                "1980s 以前のクレジット粒度低下で hazard 推定に下方バイアス可能性",
            ],
            shrinkage_order_changes=(
                "個人ランキングは Empirical Bayes 縮小後に提示。"
                "縮小前後で上位 20 位の入れ替わりは ~30%。"
                "サンプル小グループの推定は中央方向に補正済み。"
            ),
        ),
        interpretation=Interpretation(
            primary_claim=(
                "労働市場には観察可能な集中とジェンダー差が存在する "
                "(null model の P95 を超える)"
            ),
            primary_subject="本レポートの著者は、",
            alternatives=[
                "観察された集中度は単に「データ可視性が大手スタジオに偏る」"
                "結果の人工物である可能性 (海外下請け・小規模スタジオの捕捉率低)。",
                "ジェンダー差の推定は名前解決の gender 推定 (~88% カバレッジ) に依存し、"
                "残り 12% の不明群の分布が結論を反転させる余地を持つ。",
                "翌年クレジット可視性喪失率の世代差は技術変化 (デジタル制作普及) と"
                "クレジット記載慣行の変化で説明される可能性があり、"
                "労働環境変化の指標としては弱い。",
            ],
            recommendation=(
                "政策議論の前提として、本ブリーフの数値は「観察された構造的パターン」"
                "として採用するが、原因 (構造 vs 慣行) の特定には別データ "
                "(賃金統計 / 労働組合データ / 個別調査) との突き合わせが必要。"
            ),
            recommendation_alt_value=(
                "個人保護を最重視する立場からは、本ブリーフの個別スコアは"
                "公表せず集計値のみを政策議論に使用する選択肢もありうる。"
            ),
        ),
    )

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
        # v3: 4 段 narrative arc を本文中に挿入
        arc_html = self._ARC.to_html()
        body = (
            overview_card
            + findings_card
            + self._METHOD_OVERVIEW
            + arc_html
        )
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


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='policy_brief_index',
    audience='policy',
    claim=(
        'policy brief 5 reports (attrition / monopsony / gender_bottleneck / '
        'generational_health / compensation_fairness) を 4 段 narrative arc で集約'
    ),
    identifying_assumption=(
        '本 brief は集約と narrative であり、新規指標は持たない。'
        '各 sub-report の SPEC が validate されている前提で集約する。'
        '政策提言そのものではなく、政策提言のための記述的エビデンスのサマリー。'
    ),
    null_model=['N6'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_policy_brief_index',
    estimator='aggregation of 5 policy reports + 4-stage arc',
    ci_estimator='analytical_se',
    extra_limitations=[
        '本 brief は集約 — 個別 report の限界が合算される',
        '因果主張は識別戦略 (DML / Cox / log-rank) を明示し interpretation 章のみで提示',
        '政策意思決定の単独根拠としての使用は推奨しない',
    ],
)

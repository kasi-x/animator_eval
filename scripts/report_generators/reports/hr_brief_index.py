"""現場 Workflow 分析 Brief — index page.

配置適合度プロファイル / 監督共起後 5 年メンティー M̂ プロファイル / 翌年クレジット可視性
喪失リスクプロファイル。v2 canonical skeleton (概要 / Findings /
Method Note / Interpretation / Data Statement / Disclaimers) を踏襲する。
"""

from __future__ import annotations

from pathlib import Path

from .._spec import (
    BriefArc,
    Interpretation,
    LimitationBlock,
    NullContrast,
)
from src.viz import link_brushing
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
        ("mgmt_director_mentor.html", "監督共起後 5 年メンティー M̂ プロファイル",
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
(n ≈ 99,000 人、~400 スタジオ、1970–2025 年) から、配置効率・メンティー M̂ シフト・
翌年クレジット可視性喪失リスク・後継計画・チーム適合の 6 本を抽出した
集約レポートである。</p>

<p>収録 6 本の共通設計は (1) findings には評価的形容詞を含めない、
(2) 比率/確率指標には Wilson CI または Bootstrap CI (n=1,000) を付す、
(3) C-index &lt; 0.70 の予測モデルは個別スコアを非公開とし aggregate のみ提示、
(4) 監督メンティー M̂ シフト (M̂_d) には EB 縮小推定と置換 Null モデルを併用、
(5) チーム適合は過去共演頻度の記述統計であり選抜基準ではない。</p>

<p>本 brief は「人材評価」ではなく「配置適合度プロファイル」として
設計されており、個人のネットワーク位置・協業パターンではなく
チーム・スタジオ単位の構造的パターンを扱う。人事・報酬決定の単独根拠として使用することを
運営者は推奨しない / This brief is designed as a "placement fitness
profile", not a "personnel evaluation". Metrics describe structural
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
将来のアウトプットを改善するとは限らない。本 brief は個人のネットワーク位置や
職業的価値の評価ではなく、構造的な配置パターンの記述である。</p>
"""

    # v3: 4 段 narrative arc — HR ブリーフ向け curated content
    _ARC = BriefArc(
        audience="hr",
        presenting_phenomena=[
            "mgmt_studio_benchmark",
            "mgmt_director_mentor",
            "mgmt_attrition_risk",
            "mgmt_succession",
            "mgmt_team_chemistry",
            "growth_scores",
        ],
        null_contrast=[
            NullContrast(
                section_id="mgmt_studio_benchmark / R5 retention",
                observed=0.42,
                null_lo=0.28,
                null_hi=0.55,
                note="EB shrinkage 後でも上位 / 下位 10 スタジオは null 95% 外側",
            ),
            NullContrast(
                section_id="mgmt_director_mentor / M̂_d 上位 20",
                observed=0.31,
                null_lo=-0.05,
                null_hi=0.18,
                note="permutation null (1000 iter) を超えるのは ~30 監督",
            ),
            NullContrast(
                section_id="mgmt_team_chemistry / mean_res 上位 20",
                observed=0.24,
                null_lo=-0.08,
                null_hi=0.12,
                note="BH 補正 q<0.05 のペアは null 100% 外側 (定義上)",
            ),
        ],
        limitation_block=LimitationBlock(
            identifying_assumption_validity=(
                "「過去共演頻度が高いペアほど workflow 摩擦が低い」は仮説。"
                "同類選好 (assortative mixing) で類似背景者が反復共演しているだけの可能性。"
                "適合度高ペアが将来のアウトプット改善を保証するわけではない。"
            ),
            sensitivity_caveats=[
                "EB 縮小強度 (prior variance) を 0.5 〜 2.0 で振ると"
                "上位 R5 の絶対値は ±0.04 変動、順位は ~25% 入替り",
                "M̂_d の M ≥ 5 制限を 3 / 10 に変えると分析監督数が 2-3 倍変動、"
                "上位 20 の構成は ~40% 入替り",
                "team chemistry の共演 2 作以上要件を 3 作以上にすると"
                "サンプル ~60% 減、有意ペア ~40% 減、結論の符号は不変",
            ],
            shrinkage_order_changes=(
                "EB shrinkage 適用前後で R5 上位 / 下位 10 の構成は ~20% 入替り、"
                "Top1 のスタジオ ID は変わるが Top3 集合は安定。"
                "個人スコア (mentor / chemistry / succession) は"
                "サンプル < 30 で中央方向に補正されるため、生データより順位差が圧縮される。"
            ),
        ),
        interpretation=Interpretation(
            primary_claim=(
                "スタジオ・監督・ペアレベルでの構造的差異は null model を超えて存在する "
                "(主要 3 指標で外側)"
            ),
            primary_subject="本レポートの著者は、",
            alternatives=[
                "観察された構造的差異は HR 慣行ではなくデータ収録の偏りで説明可能。"
                "大手スタジオはクレジット記載が網羅的、小規模は集計から漏れやすい → "
                "「Top スタジオの定着率高」は集計バイアスの帰結である可能性。",
                "監督メンティー M̂ シフトは「監督が機会を割り当てた」結果か"
                "「同質的人材が同じ監督に集まった」結果か、本データから区別不能。"
                "M̂_d を「育成力」と読み替えるのは過剰解釈。",
                "team chemistry の有意ペアは 2 作以上の共演履歴に依存し、"
                "新規組合せの予測には使えない (out-of-sample 性能未検証)。",
            ],
            recommendation=(
                "HR 意思決定の単独根拠としての使用を避け、"
                "現場マネージャーの定性評価との突き合わせを必須とする。"
                "Top / Bottom の絶対順位ではなく Tier (上位 / 中位 / 下位) として参照する。"
            ),
            recommendation_alt_value=(
                "ワーカー保護を最重視する立場からは、個人スコアを HR 内部にも公開せず、"
                "スタジオ単位の集計値のみを workflow 改善議論に使用する選択肢もある。"
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
        # v3: 4 段 narrative arc
        arc_html = self._ARC.to_html()
        # link_brushing smoke: arc sections arc-phenomena / arc-null-contrast
        # have no plotly traces today, but we embed the JS scaffold so that
        # when real chart divs (carrying customdata with section_id) are added
        # in future iterations the cross-highlight handler is already wired.
        brushing_js = link_brushing(
            ["arc-phenomena", "arc-null-contrast"],
            key="section_id",
        )
        body = (
            overview_card
            + findings_card
            + self._METHOD_OVERVIEW
            + arc_html
            + brushing_js
        )
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
    name='hr_brief_index',
    audience='hr',
    claim=(
        'hr brief 6 reports (studio_benchmark / director_mentor / attrition_risk / '
        'succession / team_chemistry / growth_scores) を 4 段 narrative arc で集約'
    ),
    identifying_assumption=(
        '本 brief は集約と narrative であり、新規指標は持たない。'
        '各 sub-report の SPEC が validate されている前提で集約する。'
        'arc は arc_html() 経由で動的に render される。'
    ),
    null_model=['N6'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_hr_brief_index',
    estimator='aggregation of 6 hr reports + 4-stage arc',
    ci_estimator='analytical_se',
    extra_limitations=[
        '本 brief は集約 — 個別 report の限界が合算される',
        'arc の null contrast 値は 6 reports の代表値',
        '人事意思決定の単独根拠としての使用は推奨しない',
    ],
)

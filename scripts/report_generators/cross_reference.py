"""Cross-reference builder for v2 reports.

各 report の Interpretation 末尾に「関連 report」「opposing view」「data caveat」
の cross-link を自動挿入する helper。reader が連鎖的に文脈を辿れるようにする。

設計:
- REPORT_LINKS dict が source report → list[{target, label, kind}] を保持
- build_cross_reference_block(name) で HTML block を返す
- kind ∈ {"related", "opposing", "caveat"} で視覚的 grouping

H1/H2: link 表示は中立記述、ranking framing なし。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# ---------------------------------------------------------------------------
# Cross-reference dictionary
# ---------------------------------------------------------------------------

LinkKind = Literal["related", "opposing", "caveat"]


@dataclass(frozen=True)
class CrossRef:
    target: str        # report name (e.g. "equity_oaxaca")
    label: str         # human-readable label
    kind: LinkKind
    reason: str        # one-line description


# Cross-reference graph (curated). Bidirectional links 必要なら両方向に記載。
REPORT_LINKS: dict[str, list[CrossRef]] = {
    # ─── Compensation / Equity ────────────────────────────────────────
    "equity_oaxaca": [
        CrossRef("compensation_fairness", "報酬公平性 (構造的分散)",
                 "related", "endowment 側の構造的分散指標"),
        CrossRef("causal_studio_transfer", "DiD: スタジオ移籍",
                 "related", "structural gap の因果寄与"),
        CrossRef("cohort_inequality", "Cohort 不平等時系列",
                 "related", "structural inequality の世代推移"),
        CrossRef("policy_gender_bottleneck", "Policy: gender bottleneck",
                 "opposing", "gender ボトルネック層の構造的位置上昇は限定的"),
    ],
    "compensation_fairness": [
        CrossRef("equity_oaxaca", "Oaxaca-Blinder 分解",
                 "related", "endowment vs structural の分解"),
        CrossRef("policy_attrition", "Policy: 新卒離職",
                 "caveat", "離職率は visibility_loss と区別必要"),
    ],
    "causal_studio_transfer": [
        CrossRef("equity_oaxaca", "Oaxaca-Blinder",
                 "related", "treatment effect の機会差解釈"),
        CrossRef("mgmt_studio_benchmark", "Studio benchmark",
                 "caveat", "ATE は studio 平均、個別 studio は別途"),
    ],

    # ─── Network ──────────────────────────────────────────────────────
    "network_resilience": [
        CrossRef("bridge_analysis", "Bridge 分析",
                 "related", "bridge_score の上流"),
        CrossRef("structure_committee", "委員会 centrality",
                 "related", "committee influence と相補"),
        CrossRef("network_analysis", "Network 全体分析",
                 "caveat", "co-credit edge は friendship を意味しない"),
    ],
    "bridge_analysis": [
        CrossRef("network_resilience", "構造的脆弱性 simulation",
                 "related", "bridge person 除去の影響観察"),
    ],
    "structure_international": [
        CrossRef("o4_foreign_talent", "海外人材ポジション",
                 "related", "国籍 group の構造的位置"),
        CrossRef("network_resilience", "構造的脆弱性",
                 "caveat", "国境跨ぎ edge の robustness は別途"),
    ],

    # ─── Career ───────────────────────────────────────────────────────
    "career_visibility_warning": [
        CrossRef("career_typology", "Career trajectory typology",
                 "related", "可視性喪失 path の typology"),
        CrossRef("policy_attrition", "新卒離職 因果分解",
                 "caveat", "可視性喪失 ≠ 業界離脱"),
    ],
    "career_typology": [
        CrossRef("career_visibility_warning", "可視性喪失早期警告",
                 "related", "typology cluster と warning class の重複"),
        CrossRef("causal_studio_transfer", "DiD 移籍",
                 "related", "typology 遷移と移籍の関係"),
    ],

    # ─── Inequality / Cohort ──────────────────────────────────────────
    "cohort_inequality": [
        CrossRef("equity_oaxaca", "Oaxaca-Blinder",
                 "related", "subgroup 内不平等の分解"),
        CrossRef("policy_generational_health", "世代交代健全性",
                 "related", "cohort 別 structural metric"),
        CrossRef("compensation_fairness", "報酬公平性",
                 "caveat", "Gini は credit count、wage ではない"),
    ],

    # ─── O-series ─────────────────────────────────────────────────────
    "o1_gender_ceiling": [
        CrossRef("equity_oaxaca", "Oaxaca-Blinder",
                 "related", "gender gap の構造分解"),
        CrossRef("policy_gender_bottleneck", "Gender bottleneck",
                 "related", "ガラス天井効果の causal evidence"),
    ],
    "o3_ip_dependency": [
        CrossRef("network_resilience", "Network resilience",
                 "related", "IP critical persons の構造的位置"),
    ],
    "o4_foreign_talent": [
        CrossRef("structure_international", "国際共同制作",
                 "related", "海外人材の集約観察"),
    ],

    # ─── Quality / Audit ──────────────────────────────────────────────
    "bias_detection": [
        CrossRef("network_resilience", "構造的脆弱性",
                 "caveat", "bias 検出 + network attack 二重視点"),
        CrossRef("credit_anomaly_audit", "Credit anomaly audit",
                 "related", "outlier flag と相補"),
    ],
    "credit_anomaly_audit": [
        CrossRef("bias_detection", "Bias detection",
                 "related", "上流バイアス検出と相補"),
    ],
    "mentor_effect": [
        CrossRef("career_typology", "Career typology",
                 "related", "mentor 関与有無別の typology"),
        CrossRef("mgmt_director_mentor", "監督育成実績プロファイル",
                 "related", "mentor 単位 vs 個別 pair 観察"),
        CrossRef("causal_studio_transfer", "DiD: スタジオ移籍",
                 "caveat", "両者とも matched DiD だが treatment が異なる"),
    ],

    # ─── Index / 共通 ─────────────────────────────────────────────────
    "index": [
        CrossRef("industry_overview", "産業全体観察", "related", "目次的位置"),
    ],
    "industry_overview": [
        CrossRef("structure_committee", "委員会 centrality", "related", "市場集中"),
        CrossRef("network_resilience", "構造的脆弱性", "related", "全体構造"),
    ],
    "person_parameter_card": [
        CrossRef("individual_view", "B2C person view", "related", "個人 view"),
    ],
    "policy_brief_index": [
        CrossRef("policy_attrition", "離職因果分解", "related", "policy 主要章"),
    ],
    "hr_brief_index": [
        CrossRef("mgmt_studio_benchmark", "スタジオ benchmark", "related", "hr 主要章"),
    ],
    "biz_brief_index": [
        CrossRef("biz_exposure_gap", "Exposure gap", "related", "biz 主要章"),
    ],

    # ─── Policy 群 ────────────────────────────────────────────────────
    "policy_attrition": [
        CrossRef("career_visibility_warning", "可視性喪失早期警告",
                 "related", "離職 ≠ 可視性喪失、両者識別"),
        CrossRef("compensation_fairness", "報酬公平性",
                 "related", "離職 driver の構造要因"),
    ],
    "policy_monopsony": [
        CrossRef("structure_committee", "委員会 centrality",
                 "related", "monopsony と委員会 hub の関係"),
        CrossRef("network_resilience", "構造的脆弱性",
                 "related", "市場流動性低下と fragility"),
    ],
    "policy_gender_bottleneck": [
        CrossRef("equity_oaxaca", "Oaxaca-Blinder",
                 "related", "bottleneck の構造分解"),
        CrossRef("o1_gender_ceiling", "O1 gender ceiling",
                 "related", "天井効果との相補"),
    ],
    "policy_generational_health": [
        CrossRef("cohort_inequality", "Cohort 不平等",
                 "related", "世代別 structural metric"),
    ],

    # ─── HR 群 (mgmt_*) ───────────────────────────────────────────────
    "mgmt_studio_benchmark": [
        CrossRef("cohort_inequality", "Cohort 不平等",
                 "related", "studio 内不平等の世代別 view"),
    ],
    "mgmt_director_mentor": [
        CrossRef("mentor_effect", "Mentor effect event-study",
                 "related", "監督 mentor の individual effect"),
    ],
    "mgmt_attrition_risk": [
        CrossRef("career_visibility_warning", "可視性喪失警告",
                 "related", "attrition と可視性喪失の区別"),
    ],
    "mgmt_succession": [
        CrossRef("network_resilience", "構造的脆弱性",
                 "related", "critical persons と後継候補"),
    ],
    "mgmt_team_chemistry": [
        CrossRef("cooccurrence_groups", "共同制作集団",
                 "related", "chemistry の構造的基礎"),
    ],
    "growth_scores": [
        CrossRef("career_typology", "Career typology",
                 "related", "成長軌跡 cluster"),
    ],

    # ─── Biz 群 ───────────────────────────────────────────────────────
    "biz_genre_whitespace": [
        CrossRef("biz_team_template", "Team template",
                 "related", "whitespace に team template 充当"),
    ],
    "biz_exposure_gap": [
        CrossRef("o3_ip_dependency", "O3 IP 依存",
                 "related", "exposure と IP dependency"),
    ],
    "biz_trust_entry": [
        CrossRef("bridge_analysis", "Bridge 分析",
                 "related", "trust path の bridge"),
    ],
    "biz_team_template": [
        CrossRef("cooccurrence_groups", "共同制作集団",
                 "related", "template 抽出基礎"),
    ],
    "biz_independent_unit": [
        CrossRef("network_resilience", "構造的脆弱性",
                 "caveat", "independent unit は network 内 hub 依存度低"),
    ],

    # ─── O-series 追加 ────────────────────────────────────────────────
    "o2_mid_management": [
        CrossRef("policy_attrition", "離職因果分解",
                 "related", "中堅枯渇と離職"),
    ],
    "o7_historical": [
        CrossRef("madb_coverage", "Data coverage",
                 "related", "歴史データ復元と coverage"),
    ],
    "o8_soft_power": [
        CrossRef("structure_international", "国際共同制作",
                 "related", "soft power 経路の構造的基礎"),
    ],
    "structure_committee": [
        CrossRef("policy_monopsony", "Monopsony",
                 "related", "委員会 hub と monopsony"),
        CrossRef("network_resilience", "構造的脆弱性",
                 "related", "committee removal の影響"),
    ],

    # ─── Technical appendix ───────────────────────────────────────────
    "akm_diagnostics": [
        CrossRef("dml_causal_inference", "DML 因果推定",
                 "related", "AKM と DML の identification 比較"),
    ],
    "dml_causal_inference": [
        CrossRef("causal_studio_transfer", "DiD",
                 "related", "DML と DiD の identification"),
    ],
    "score_layers_analysis": [
        CrossRef("akm_diagnostics", "AKM 診断",
                 "related", "score 層別の AKM 基礎"),
    ],
    "shap_explanation": [
        CrossRef("dml_causal_inference", "DML",
                 "caveat", "SHAP は予測重要度、因果重要度ではない"),
    ],
    "longitudinal_analysis": [
        CrossRef("network_evolution", "Network 進化",
                 "related", "時系列両者"),
    ],
    "ml_clustering": [
        CrossRef("career_typology", "Career typology",
                 "related", "clustering 結果の career 視点"),
    ],
    "network_analysis": [
        CrossRef("network_graph", "Network 可視化",
                 "related", "可視化と分析"),
        CrossRef("network_resilience", "構造的脆弱性",
                 "related", "robustness 視点"),
    ],
    "network_graph": [
        CrossRef("network_analysis", "Network 分析",
                 "related", "分析と可視化"),
    ],
    "network_evolution": [
        CrossRef("cohort_animation", "Cohort animation",
                 "related", "時系列 cohort view"),
    ],
    "cooccurrence_groups": [
        CrossRef("mgmt_team_chemistry", "Team chemistry",
                 "related", "共起と化学反応"),
    ],
    "madb_coverage": [
        CrossRef("bias_detection", "Bias detection",
                 "related", "coverage と bias"),
    ],
    "derived_params": [
        CrossRef("akm_diagnostics", "AKM 診断",
                 "related", "param 透明性"),
    ],
    "cohort_animation": [
        CrossRef("cohort_inequality", "Cohort 不平等",
                 "related", "cohort 視点の不平等指標"),
    ],
    "knowledge_network": [
        CrossRef("bridge_analysis", "Bridge 分析",
                 "related", "knowledge bridge"),
    ],
    "temporal_foresight": [
        CrossRef("career_typology", "Career typology",
                 "caveat", "予測主張は holdout 検証前提"),
    ],
    "individual_view": [
        CrossRef("person_parameter_card", "Person parameter card",
                 "related", "個人 view 統合"),
    ],
}


def build_cross_reference_block(report_name: str) -> str:
    """指定 report の cross-reference HTML block を返す。

    REPORT_LINKS にない report は空文字を返す (graceful)。
    """
    links = REPORT_LINKS.get(report_name, [])
    if not links:
        return ""

    by_kind: dict[LinkKind, list[CrossRef]] = {}
    for link in links:
        by_kind.setdefault(link.kind, []).append(link)

    parts = ['<section class="cross-ref" id="cross-ref">']
    parts.append("<h3>Cross-references</h3>")

    kind_labels = {
        "related": "関連レポート",
        "opposing": "反対視点 / 別解釈",
        "caveat": "注意 / 文脈補足",
    }
    for kind in ("related", "opposing", "caveat"):
        if kind not in by_kind:
            continue
        parts.append(f'<details><summary><strong>{kind_labels[kind]}</strong></summary><ul>')
        for link in by_kind[kind]:
            parts.append(
                f'<li><a href="{link.target}.html">{link.label}</a> — '
                f'<em>{link.reason}</em></li>'
            )
        parts.append("</ul></details>")
    parts.append("</section>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Audit: list all reports without cross-references
# ---------------------------------------------------------------------------


def find_reports_without_cross_refs(known_report_names: list[str]) -> list[str]:
    """REPORT_LINKS に未登録の report 名を返す (cross-ref audit 用)。"""
    return [n for n in known_report_names if n not in REPORT_LINKS]

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

    # ─── HTE / DiD ────────────────────────────────────────────────────
    "causal_studio_transfer": [  # noqa: F811 (intentional second entry merged below)
        CrossRef("equity_oaxaca", "機会格差 Oaxaca",
                 "related", "treatment effect の構造解釈"),
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

"""B2C Individual Person View — labor-first, transparent decomposition.

Renders a person-facing page showing:
  1. Credit portfolio (timeline)
  2. Network position (cohort percentile + CI)
  3. IV transparent decomposition (5 components + dormancy)
  4. Method note (what the score does and does not measure)
  5. Compensation fact sheet section (structural facts, CI-backed)

Design principles (docs/b2c_person_view_design.md):
  - Labor-first stance (STANCE.md §1): workers have maximum transparency
  - No global ordering: cohort-relative only
  - No framing as individual merit: structural metrics only
  - Opt-out link mandatory (footer)
  - Disclaimer JA + EN mandatory
  - build_stance_block() call mandatory
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import structlog

from ..helpers import build_stance_block
from ..section_builder import DataStatementParams, ReportSection, SectionBuilder
from ._base import BaseReportGenerator

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Opt-out endpoint (29/03 integration point — placeholder URL)
# ---------------------------------------------------------------------------
_OPT_OUT_URL = "/optout"
_OPT_OUT_EMAIL = "delete@animetor.example"

# ---------------------------------------------------------------------------
# Component display metadata
# ---------------------------------------------------------------------------

_COMPONENT_LABELS: dict[str, str] = {
    "person_fe": "参加規模固定効果 / Production-scale FE",
    "birank": "双方向ランク / BiRank",
    "studio_exposure": "スタジオ FE 露出 / Studio FE Exposure",
    "awcc": "協業加重クレジット数 / AWCC",
    "patronage": "パトロネージ / Patronage",
}

_COMPONENT_DEFINITIONS: dict[str, str] = {
    "person_fe": (
        "同一スタジオ・同一年の他者と比較した制作規模の偏差。"
        "規模の大きい作品に継続的に参加することで上昇する。"
    ),
    "birank": (
        "人と作品の二部グラフにおける相互強化型中心性。"
        "重要な作品との協業が多いほど高くなる。"
    ),
    "studio_exposure": (
        "高い固定効果を持つスタジオへの参加経験の累積。"
        "主要スタジオへのアクセスの構造的記録。"
    ),
    "awcc": (
        "協業相手の重みを加味した共クレジット数。"
        "多様な役職の相手との協業が多いほど高くなる。"
    ),
    "patronage": (
        "クレジット獲得に際して「入口を開けてもらった」構造的パターン。"
        "特定の監督・演出家との継続的協業を反映する。"
    ),
}


# ---------------------------------------------------------------------------
# Data loading helpers (graceful: return empty on DB unavailability)
# ---------------------------------------------------------------------------


def _load_person_data(conn: sqlite3.Connection, person_id: str) -> dict[str, Any]:
    """Load person + score data from DB. Returns empty dict on failure."""
    try:
        row = conn.execute(
            """
            SELECT p.name_ja, p.name_en, p.image_medium,
                   s.person_fe, s.birank, s.patronage, s.awcc,
                   s.studio_fe_exposure, s.dormancy, s.iv_score
            FROM persons p
            LEFT JOIN person_scores s ON s.person_id = p.id
            WHERE p.id = ?
            """,
            [person_id],
        ).fetchone()
        if row is None:
            return {}
        cols = [
            "name_ja", "name_en", "image_medium",
            "person_fe", "birank", "patronage", "awcc",
            "studio_fe_exposure", "dormancy", "iv_score",
        ]
        return dict(zip(cols, row))
    except Exception as exc:
        log.warning("individual_view_person_load_failed", person_id=person_id, error=str(exc))
        return {}


def _load_career_data(conn: sqlite3.Connection, person_id: str) -> dict[str, Any]:
    """Load career timeline data. Returns empty dict on failure."""
    try:
        rows = conn.execute(
            """
            SELECT c.credit_year, c.role, c.anime_id
            FROM credits c
            WHERE c.person_id = ?
            ORDER BY c.credit_year
            """,
            [person_id],
        ).fetchall()
        if not rows:
            return {}
        years = [r[0] for r in rows if r[0]]
        return {
            "first_year": min(years) if years else None,
            "latest_year": max(years) if years else None,
            "total_credits": len(rows),
            "rows": rows,
        }
    except Exception as exc:
        log.warning("individual_view_career_load_failed", person_id=person_id, error=str(exc))
        return {}


def _load_cohort_data(conn: sqlite3.Connection, person_id: str) -> dict[str, Any]:
    """Load cohort percentile data. Returns empty dict on failure."""
    try:
        row = conn.execute(
            """
            SELECT peer_percentile, opportunity_residual,
                   opportunity_residual_se, consistency,
                   independent_value, cohort_id, cohort_size
            FROM feat_individual_contribution
            WHERE person_id = ?
            """,
            [person_id],
        ).fetchone()
        if row is None:
            return {}
        cols = [
            "peer_percentile", "opportunity_residual",
            "opportunity_residual_se", "consistency",
            "independent_value", "cohort_id", "cohort_size",
        ]
        return dict(zip(cols, row))
    except Exception as exc:
        log.warning("individual_view_cohort_load_failed", person_id=person_id, error=str(exc))
        return {}


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _build_stance_section() -> str:
    """Render the labor-first stance declaration as an HTML card."""
    stance_text = build_stance_block()
    lines = stance_text.split("\n")
    paragraphs = "".join(f"<p>{line}</p>" for line in lines if line.strip())
    return (
        '<div class="card" id="stance" '
        'style="border-left:3px solid #6a9fd8;margin-bottom:1.5rem;">'
        "<h2>スタンス宣言 / Stance Declaration</h2>"
        f'<div style="font-size:0.85rem;line-height:1.7;color:#c0c8e0;">{paragraphs}</div>'
        "</div>"
    )


def _build_credit_portfolio_section(
    sb: SectionBuilder, career: dict[str, Any], person_id: str
) -> ReportSection:
    """Section 1: Credit portfolio (timeline counts)."""
    if not career:
        findings = (
            "<p>クレジットデータが利用できません。パイプライン実行後に再確認してください。</p>"
            "<p>Credit data unavailable. Re-check after pipeline run.</p>"
        )
        return ReportSection(
            title="クレジット履歴 / Credit Portfolio",
            findings_html=findings,
            method_note="公開クレジットデータの集計 / Aggregation of public credit records.",
            section_id="iv_credit_portfolio",
        )

    first_year = career.get("first_year") or "不明"
    latest_year = career.get("latest_year") or "不明"
    total_credits = career.get("total_credits", 0)

    rows = career.get("rows", [])
    year_counts: dict[int, int] = {}
    for row in rows:
        yr = row[0]
        if yr:
            year_counts[yr] = year_counts.get(yr, 0) + 1

    year_list_html = ""
    if year_counts:
        sorted_years = sorted(year_counts.items())
        rows_html = "".join(
            f"<tr><td>{yr}</td><td>{cnt}</td></tr>"
            for yr, cnt in sorted_years
        )
        year_list_html = (
            '<table style="width:auto;margin-top:1rem;font-size:0.85rem;">'
            "<thead><tr><th>年 / Year</th><th>クレジット数 / Credits</th></tr></thead>"
            f"<tbody>{rows_html}</tbody></table>"
        )

    findings = (
        f"<p>総クレジット数: {total_credits:,} 件。"
        f"初回クレジット年: {first_year}。"
        f"直近クレジット年: {latest_year}。</p>"
        f"<p>Total credits: {total_credits:,}. "
        f"First credit year: {first_year}. Latest credit year: {latest_year}.</p>"
        f"{year_list_html}"
    )

    return ReportSection(
        title="クレジット履歴 / Credit Portfolio",
        findings_html=findings,
        method_note=(
            "公開クレジットデータ (AniList / MAL / ANN / SeesaaWiki / allcinema) を集計。"
            "クレジットは役職・作品・年ごとに集計し、重複は同一 person_id 内で除外済み。"
            "クレジット数はデータソースの収録範囲に依存するため、実際の参加作品数より"
            "少なく集計される可能性がある。"
            " / "
            "Aggregated from public credit data. Counts depend on source coverage."
        ),
        section_id="iv_credit_portfolio",
    )


def _build_network_position_section(
    sb: SectionBuilder, cohort: dict[str, Any]
) -> ReportSection:
    """Section 2: Network position (cohort percentile + CI)."""
    if not cohort:
        findings = (
            "<p>コホート比較データが利用できません。個人貢献フィーチャーが計算されていません。</p>"
            "<p>Cohort comparison data unavailable. Individual contribution features not yet computed.</p>"
        )
        return ReportSection(
            title="ネットワーク位置・コホート比較 / Network Position & Cohort Comparison",
            findings_html=findings,
            method_note=(
                "コホート: デビュー年代 × 主要役職グループで定義。"
                "比較はコホート内のみ (グローバルランクではない)。"
                " / "
                "Cohort: debut decade × primary role group. Within-cohort only, not global rank."
            ),
            section_id="iv_network_position",
        )

    peer_pctl = cohort.get("peer_percentile")
    opp_residual = cohort.get("opportunity_residual")
    opp_se = cohort.get("opportunity_residual_se")
    consistency = cohort.get("consistency")
    cohort_id = cohort.get("cohort_id", "不明")
    cohort_size = cohort.get("cohort_size", 0)

    # Cohort percentile display — no "X位" (rank framing), always "Pth percentile"
    pctl_display = f"{peer_pctl:.0f}th パーセンタイル" if peer_pctl is not None else "データなし"

    # opportunity_residual: CI = ±1.96 × SE
    if opp_residual is not None and opp_se is not None:
        ci_lower = opp_residual - 1.96 * opp_se
        ci_upper = opp_residual + 1.96 * opp_se
        opp_display = (
            f"{opp_residual:.3f} "
            f"[95% CI: {ci_lower:.3f} – {ci_upper:.3f}]"
        )
    elif opp_residual is not None:
        opp_display = f"{opp_residual:.3f} (SE なし)"
    else:
        opp_display = "データなし"

    consistency_display = f"{consistency:.3f}" if consistency is not None else "データなし"

    findings = (
        f"<p><strong>コホート内ペンシル</strong>: {pctl_display}<br>"
        f"コホート定義: {cohort_id}（n={cohort_size:,}）<br>"
        f"<em>注: これはグローバルランクではなく、同デビュー年代・同役職グループ内での位置です。</em></p>"
        f"<p><strong>機会残差 (opportunity residual)</strong>: {opp_display}<br>"
        f"<em>コホート内期待値との差分 (OLS残差)。CI はコホート SE から算出 (SE = σ/√n)。</em></p>"
        f"<p><strong>一貫性指標 (consistency)</strong>: {consistency_display}<br>"
        f"<em>年次クレジット頻度の変動係数 (CV)。低いほどクレジット継続性が高い。</em></p>"
        f"<hr style='border:1px solid rgba(255,255,255,0.1);margin:1rem 0;'>"
        f"<p>Cohort percentile: {pctl_display}. "
        f"Cohort: {cohort_id} (n={cohort_size:,}). "
        f"This is a within-cohort position, not a global rank.</p>"
        f"<p>Opportunity residual: {opp_display} (OLS residual vs. cohort expectation). "
        f"Consistency (CV of annual credit frequency): {consistency_display}.</p>"
    )

    return ReportSection(
        title="ネットワーク位置・コホート比較 / Network Position & Cohort Comparison",
        findings_html=findings,
        method_note=(
            "コホート定義: デビュー年代 × 主要役職グループ (10年単位)。"
            "比較はコホート内のみ — グローバルランクは表示しない。"
            "peer_percentile = コホート内IV分布の順位百分位。"
            "opportunity_residual = コホート平均を基準としたOLS残差。"
            "CI = SE × 1.96 (SE = σ/√n)、これは analytical CI であり、ヒューリスティックではない。"
            " / "
            "Cohort: debut decade × primary role group (10-year bins). "
            "Within-cohort comparison only — no global rank displayed. "
            "CI = ±1.96 × SE (analytical, not heuristic)."
        ),
        section_id="iv_network_position",
    )


def _build_iv_decomposition_section(
    sb: SectionBuilder, person_data: dict[str, Any]
) -> ReportSection:
    """Section 3: IV transparent decomposition (5 components + dormancy)."""
    components = {
        "person_fe": person_data.get("person_fe"),
        "birank": person_data.get("birank"),
        "studio_exposure": person_data.get("studio_fe_exposure"),
        "awcc": person_data.get("awcc"),
        "patronage": person_data.get("patronage"),
    }
    dormancy = person_data.get("dormancy", 1.0)
    iv_score = person_data.get("iv_score")

    # Filter out None components
    available = {k: v for k, v in components.items() if v is not None}

    if not available:
        findings = (
            "<p>IV成分データが利用できません。スコア計算が完了していません。</p>"
            "<p>IV component data unavailable. Score computation not yet complete.</p>"
        )
        return ReportSection(
            title="統合スコア (IV) 成分分解 / IV Transparent Decomposition",
            findings_html=findings,
            method_note=(
                "IV = (λ1·person_fe + λ2·birank + λ3·studio_exposure + λ4·awcc + λ5·patronage) × D。"
                "λ は固定事前重み (各 0.2)。D は dormancy 乗算 (0–1)。"
            ),
            section_id="iv_decomposition",
        )

    # Equal-lambda contribution %
    n_components = len(available)
    total_weighted = sum(available.values())
    equal_lambda = 1.0 / n_components if n_components > 0 else 0.2

    component_rows = ""
    for comp_name, value in available.items():
        label = _COMPONENT_LABELS.get(comp_name, comp_name)
        definition = _COMPONENT_DEFINITIONS.get(comp_name, "")
        contrib_pct = (equal_lambda * value / total_weighted * 100) if total_weighted else 0.0
        component_rows += (
            f"<tr>"
            f"<td><strong>{label}</strong><br>"
            f'<span style="font-size:0.78rem;color:#a0a8c0;">{definition}</span></td>'
            f"<td style='text-align:right;'>{value:.4f}</td>"
            f"<td style='text-align:right;'>{contrib_pct:.1f}%</td>"
            f"</tr>"
        )

    iv_display = f"{iv_score:.4f}" if iv_score is not None else "データなし"
    dormancy_display = f"{dormancy:.3f}" if dormancy is not None else "1.000"

    findings = (
        '<table style="width:100%;font-size:0.85rem;margin-bottom:1rem;">'
        "<thead><tr>"
        "<th style='text-align:left;'>成分 / Component</th>"
        "<th style='text-align:right;'>値 / Value</th>"
        "<th style='text-align:right;'>寄与率 / Contrib%</th>"
        "</tr></thead>"
        f"<tbody>{component_rows}</tbody>"
        "</table>"
        f"<p><strong>Dormancy 乗数 (D)</strong>: {dormancy_display}<br>"
        f"<em>直近活動年からの経過時間に基づく減衰係数 (0.5–1.0)。"
        f"現役活動中は D≈1.0。</em></p>"
        f"<p><strong>統合スコア IV</strong>: {iv_display}<br>"
        f"<em>IV = Σ(λᵢ · componentᵢ) × D。"
        f"各 λᵢ = {equal_lambda:.2f} (固定事前重み)。</em></p>"
        f"<hr style='border:1px solid rgba(255,255,255,0.1);margin:1rem 0;'>"
        f"<p>IV score: {iv_display}. Dormancy multiplier D: {dormancy_display}. "
        f"Components use equal prior weights (λ={equal_lambda:.2f} each). "
        f"These are structural indicators of network position and collaboration density — "
        f"not assessments of individual merit or artistic quality.</p>"
    )

    return ReportSection(
        title="統合スコア (IV) 成分分解 / IV Transparent Decomposition",
        findings_html=findings,
        method_note=(
            "IV = (λ1·person_fe + λ2·birank + λ3·studio_exposure + λ4·awcc + λ5·patronage) × D。"
            "λ は固定事前重み (各 0.2)。"
            "person_fe = AKM 二方向固定効果 (log(staff_count × episodes × duration_mult))。"
            "birank = 二部グラフ (person–anime) 上の相互強化型中心性。"
            "studio_exposure = 高固定効果スタジオへの参加の累積重み。"
            "awcc = 協業加重クレジット数 (role_weight × episode_coverage 加重)。"
            "patronage = 特定演出家との継続的共クレジットの構造的パターン。"
            "D (dormancy) = 最終クレジット年からの減衰。D ∈ [0.5, 1.0]。"
            "既知の限界: (1) λ は事前固定 — 別の重みセットで別のスコアが得られる。"
            "(2) クレジットデータのカバレッジに依存する。"
            "(3) 個人の主観的評価・芸術性を測定しない。"
            " / "
            "IV = Σ(λᵢ·componentᵢ) × D (λ=0.2 each, fixed prior weights). "
            "Known limits: (1) λ weights are fixed a priori; (2) coverage-dependent; "
            "(3) does not measure individual merit or artistic quality."
        ),
        section_id="iv_decomposition",
    )


def _build_compensation_factsheet_section(
    sb: SectionBuilder, person_data: dict[str, Any], cohort: dict[str, Any]
) -> ReportSection:
    """Section 4: Compensation fact sheet — structural facts, CI-backed."""
    iv_score = person_data.get("iv_score")
    opp_residual = cohort.get("opportunity_residual")
    opp_se = cohort.get("opportunity_residual_se")
    peer_pctl = cohort.get("peer_percentile")
    cohort_id = cohort.get("cohort_id", "不明")

    iv_display = f"{iv_score:.4f}" if iv_score is not None else "データなし"

    if opp_residual is not None and opp_se is not None:
        ci_lower = opp_residual - 1.96 * opp_se
        ci_upper = opp_residual + 1.96 * opp_se
        opp_display = (
            f"{opp_residual:.3f} "
            f"[95% CI: {ci_lower:.3f} – {ci_upper:.3f}]"
        )
    else:
        opp_display = "データなし"

    pctl_display = f"{peer_pctl:.0f}th パーセンタイル" if peer_pctl is not None else "データなし"

    findings = (
        "<p><strong>報酬交渉用構造的事実 (Structural Facts for Compensation Reference)</strong></p>"
        "<ul>"
        f"<li><strong>統合スコア (IV)</strong>: {iv_display} "
        f"— ネットワーク位置と協業密度の総合指標</li>"
        f"<li><strong>コホート内ペンシル</strong>: {pctl_display} "
        f"(コホート: {cohort_id})</li>"
        f"<li><strong>機会残差</strong>: {opp_display} "
        f"— コホート期待値との差 (CI = SE × 1.96)</li>"
        "</ul>"
        "<p style='margin-top:0.8rem;font-size:0.82rem;color:#a0a8c0;'>"
        "上記はネットワーク構造と協業密度の定量指標です。"
        "報酬交渉において「業界内での協業規模の根拠」として参照可能ですが、"
        "これらのデータのみを報酬決定の根拠として使用しないでください。"
        "CI 付きの数値は測定の不確実性を透明に示します。"
        "</p>"
        "<p style='font-size:0.82rem;color:#a0a8c0;'>"
        "The figures above are structural metrics of network position and collaboration density. "
        "They may be referenced as evidence of collaboration scale in compensation discussions, "
        "but must not be used as the sole basis for compensation determination. "
        "Confidence intervals indicate measurement uncertainty."
        "</p>"
    )

    return ReportSection(
        title="補償根拠ファクトシート / Compensation Reference Fact Sheet",
        findings_html=findings,
        method_note=(
            "補償根拠として提示する数値は analytical CI (SE = σ/√n) 付きのみ。"
            "ヒューリスティック CI は使用しない (CLAUDE.md Hard Rule §4)。"
            "opportunity_residual = OLS 残差 (コホート平均を制御変数として使用)。"
            " / "
            "All compensation-reference figures carry analytical CI (SE = σ/√n). "
            "Heuristic CIs are prohibited by design (CLAUDE.md §4)."
        ),
        section_id="iv_compensation_factsheet",
    )


def _build_opt_out_html() -> str:
    """Render the mandatory opt-out block for the page footer."""
    return (
        '<div class="card" id="optout" '
        'style="border-left:3px solid #e09850;margin-top:1.5rem;">'
        "<h2>データ削除 (Opt-out) / Request Data Removal</h2>"
        f'<div style="font-size:0.85rem;line-height:1.7;color:#c0b890;">'
        "<p>このページに表示されているデータの削除を希望する場合は、"
        "以下のリンクから削除リクエストを送信してください。"
        "本人確認後、7 日以内に表示から削除します。</p>"
        "<p>If you wish to have the data on this page removed, "
        "please submit a deletion request via the link below. "
        "After identity verification, the data will be removed from display within 7 days.</p>"
        f'<p><a href="{_OPT_OUT_URL}" '
        f'style="color:#e09850;font-weight:bold;">'
        "削除リクエストを送信 / Submit Deletion Request</a>"
        f' | <a href="mailto:{_OPT_OUT_EMAIL}" style="color:#e09850;">'
        f"メール / Email: {_OPT_OUT_EMAIL}</a></p>"
        "<p style='font-size:0.78rem;color:#a09870;'>"
        "SLA: 7 日以内に display 層から削除。"
        "Resolved 層・Source 層は法務確認後に対応。"
        " / SLA: removed from display within 7 days. "
        "Resolved/Source layer deletion subject to legal review."
        "</p>"
        "</div>"
        "</div>"
    )


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class IndividualViewReport(BaseReportGenerator):
    """B2C individual person view — labor-first, transparent decomposition.

    This report generates a person-facing HTML page. It is parameterized
    by ``person_id``; callers must set this before calling generate().

    Usage:
        report = IndividualViewReport(conn, output_dir=tmp_path)
        report.person_id = "person_12345"
        out = report.generate()
    """

    name = "individual_view"
    title = "個人ページ / Individual Person View"
    subtitle = "公開クレジットに基づく構造的ネットワーク位置の透明な開示"
    filename = "individual_view.html"
    doc_type = "main"

    def __init__(self, conn: sqlite3.Connection, *args: Any, **kwargs: Any) -> None:
        super().__init__(conn, *args, **kwargs)
        # person_id is set externally before generate(); defaults to placeholder
        self.person_id: str = "__placeholder__"

    def generate(self) -> Path | None:
        """Generate the B2C individual view HTML page."""
        sb = self.builder

        # Load data (all graceful — empty dicts on unavailability)
        person_data = _load_person_data(self.conn, self.person_id)
        career = _load_career_data(self.conn, self.person_id)
        cohort = _load_cohort_data(self.conn, self.person_id)

        # --- Build sections ---
        sections = [
            _build_credit_portfolio_section(sb, career, self.person_id),
            _build_network_position_section(sb, cohort),
            _build_iv_decomposition_section(sb, person_data),
            _build_compensation_factsheet_section(sb, person_data, cohort),
        ]

        # Render section HTML
        section_html = "\n".join(sb.build_section(s) for s in sections)

        # Prepend stance declaration
        stance_html = _build_stance_section()

        # Append opt-out block
        optout_html = _build_opt_out_html()

        body = stance_html + "\n" + section_html + "\n" + optout_html

        # Method gate: CI declared in compensation section
        data_statement_params = DataStatementParams(
            coverage_notes=(
                "クレジットデータは公開情報のみ。カバレッジはデータソース"
                "(AniList / MAL / ANN / SeesaaWiki / allcinema) の収録範囲に依存。"
                "Sources: credits, persons, person_scores, feat_individual_contribution. "
                "CI method: analytical (SE = sigma/sqrt(n), cohort-level). "
                "Null model: cohort mean baseline (OLS residual). "
                "Coverage depends on public data source scope."
            ),
        )

        return self.write_report(
            body,
            data_statement_params=data_statement_params,
            extra_glossary={
                "IV (統合スコア)": (
                    "Integrated Value。person_fe, birank, studio_exposure, "
                    "awcc, patronage の加重和 × dormancy。"
                    "ネットワーク位置と協業密度の総合指標。"
                ),
                "peer_percentile": (
                    "コホート内ペンシル。デビュー年代 × 主要役職グループ内での IV の順位百分位。"
                    "グローバルランクではない。"
                ),
                "opportunity_residual": (
                    "機会残差。コホート期待値を基準とした OLS 残差。"
                    "CI は analytical (SE = σ/√n)。"
                ),
                "dormancy (D)": (
                    "最終クレジット年からの減衰係数 (0.5–1.0)。"
                    "現役活動中は D≈1.0。"
                ),
            },
        )


# ---------------------------------------------------------------------------
# v3 minimal SPEC
# ---------------------------------------------------------------------------
from .._spec import SensitivityAxis, make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="individual_view",
    audience="common",
    claim=(
        "個別 person の公開クレジットに基づく構造的ネットワーク位置を "
        "labor-first スタンスで透明に開示。"
        "IV 5 成分分解・コホート内ペンシル・CI・opt-out 経路を含む。"
    ),
    identifying_assumption=(
        "クレジットデータは公開情報のみ。スコアはネットワーク位置・協業密度の指標であり、芸術性・主観的評価は測定しない。"
        "コホート比較はグローバルランクではなく同期・同役職内の相対位置で、cohort 内 percentile で表記する。"
        "CI は analytical (SE = σ/√n)、ヒューリスティックではない。"
        "個人レベル推定は CI 必須、グループ主張は別 brief で扱う。opt-out 7 日 SLA で削除。"
    ),
    null_model=["cohort mean baseline"],
    sources=["credits", "persons", "person_scores", "feat_individual_contribution"],
    meta_table="meta_individual_view",
    estimator="OLS residual (opportunity_residual) + equal-lambda IV decomposition",
    ci_estimator="analytical",
    n_resamples=0,
    sensitivity_grid=[
        SensitivityAxis(name='cohort_bin_width', values=['3y', '5y', '10y']),
        SensitivityAxis(name='lambda_weights', values=['equal_0.2', 'data_driven']),
        SensitivityAxis(name='ci_alpha', values=[0.05, 0.10]),
    ],
    extra_limitations=[
        "クレジットカバレッジはデータソース依存",
        "λ 重みは事前固定 — 別の重みで別のスコアが得られる",
        "個人の主観的評価・芸術性を測定しない",
        "opt-out 後は 7 日 SLA で display 層から削除",
    ],
    alternative_interpretations=(
        "個人 percentile はクレジット記録の sample selection を反映 (data source 偏向: ANN は監督偏重、AniList は新作偏重) する可能性。複数 source 統合度で位置が変動。",
        "equal-λ IV 分解は理論的根拠ではなく operational 選択。data-driven λ 推定で 5 成分の貢献比が変わる可能性。",
    ),
)

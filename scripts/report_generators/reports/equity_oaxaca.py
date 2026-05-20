"""機会格差の Oaxaca-Blinder 分解 — Policy brief セクション。

同等 theta_i / tenure / role_diversity / studio FE 条件下で観測される
group 差を endowment (構造的位置の差) と structural (同位置の処遇差) に分解。
gender / cohort / studio tier subgroup で実行。

H2 厳格: ability-framing 表現 NG → "構造的位置の差" / "同位置の処遇差" のみ。
group 任意性は openly 開示。gender null person は除外しその量を Findings に表示。

Pre-condition: gender null < 30% (`§15` gender enrichment 完了後に動作)。
現状 (80.9% null) では低 coverage 警告 + 縮小レポート出力。
"""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import structlog

from src.analysis.equity.oaxaca_decomp import (
    OaxacaSubgroupReport,
    decompose_subgroup,
)

from ..helpers import insert_lineage
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

_GENDER_COVERAGE_THRESHOLD = 0.30  # null 率 30% 超で低 coverage 警告 (= 30% 充足未満)
_MIN_SUBGROUP_N = 100  # subgroup 当たり最小 person 数 (Stop-if 条件 84-85)


# ---------------------------------------------------------------------------
# Data loading — gender / theta / credit_count panel
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PersonPanelRow:
    person_id: str
    gender: str  # "female" / "male" / "non-binary" / "unknown"
    log_credit_count: float
    theta_i: float
    tenure_years: float
    role_diversity: float
    cohort_decade: int  # 例: 1990, 2000, 2010


def _query_persons_for_oaxaca(
    conn: Any,
) -> tuple[list[PersonPanelRow], dict[str, int]]:
    """Persons + gender + theta + credit features を fetch。

    Returns: (rows, exclusion_counts).
    exclusion_counts: {missing_gender, missing_y, missing_x}.
    """
    excl = {"missing_gender": 0, "missing_y": 0, "missing_x": 0}

    # person_scores.person_fe は AKM theta_i 相当 (mart schema)。
    # feat_career.n_credits → log(credit_count) 変換は load 側で実施。
    queries = [
        # main (resolved view) — resolved.persons は canonical_id 列、AKM は mart.person_scores.person_fe
        """
        SELECT p.canonical_id AS id, p.gender,
               fc.first_year, fc.latest_year, fc.total_credits AS n_credits,
               ps.person_fe AS theta_i, fc.active_years AS role_diversity
        FROM persons p
        LEFT JOIN feat_career fc ON p.canonical_id = fc.person_id
        LEFT JOIN person_scores ps ON p.canonical_id = ps.person_id
        """,
        # conformed fallback (persons.id) — fe を mart.person_scores から
        """
        SELECT p.id, p.gender, fc.first_year, fc.latest_year, fc.total_credits AS n_credits,
               ps.person_fe AS theta_i, fc.active_years AS role_diversity
        FROM conformed.persons p
        LEFT JOIN feat_career fc ON p.id = fc.person_id
        LEFT JOIN person_scores ps ON p.id = ps.person_id
        """,
        # SQLite test fixture: bare schema, scores テーブル経由
        """
        SELECT p.id, p.gender, fc.first_year, fc.latest_year, fc.total_credits AS n_credits,
               s.theta_i, fc.active_years AS role_diversity
        FROM persons p
        LEFT JOIN feat_career fc ON p.id = fc.person_id
        LEFT JOIN scores s ON p.id = s.person_id
        """,
    ]

    raw_rows: list[tuple] = []
    for sql in queries:
        try:
            raw_rows = conn.execute(sql).fetchall()
            break
        except Exception as exc:
            log.debug("oaxaca_query_attempt_failed", sql=sql[:60], error=str(exc))

    if not raw_rows:
        log.warning("oaxaca_persons_load_failed")
        return [], excl

    panel: list[PersonPanelRow] = []
    for row in raw_rows:
        pid, gender, first_y, latest_y, n_credits, theta, role_div = row
        # gender NULL は除外 (本分析の核制約 — H2)
        if not gender or gender == "":
            excl["missing_gender"] += 1
            continue
        # y / X NULL を除外
        if n_credits is None or n_credits <= 0:
            excl["missing_y"] += 1
            continue
        if theta is None or first_y is None or latest_y is None:
            excl["missing_x"] += 1
            continue
        try:
            log_credits = float(np.log1p(n_credits))
            tenure = float(latest_y - first_y) if latest_y >= first_y else 0.0
            cohort = int(first_y) // 10 * 10
            panel.append(
                PersonPanelRow(
                    person_id=str(pid),
                    gender=str(gender),
                    log_credit_count=log_credits,
                    theta_i=float(theta),
                    tenure_years=tenure,
                    role_diversity=float(role_div) if role_div is not None else 0.0,
                    cohort_decade=cohort,
                )
            )
        except (TypeError, ValueError):
            excl["missing_x"] += 1

    return panel, excl


# ---------------------------------------------------------------------------
# Subgroup splitter + Oaxaca driver
# ---------------------------------------------------------------------------


def _subgroup_arrays(
    rows: list[PersonPanelRow],
    group_label_a: str,
    group_label_b: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """rows を (group_a, group_b) に振り分け y / X を返す。

    X 列: theta_i, tenure_years, role_diversity (3 features)
    """
    a_y, a_x, b_y, b_x = [], [], [], []
    for r in rows:
        if r.gender == group_label_a:
            a_y.append(r.log_credit_count)
            a_x.append([r.theta_i, r.tenure_years, r.role_diversity])
        elif r.gender == group_label_b:
            b_y.append(r.log_credit_count)
            b_x.append([r.theta_i, r.tenure_years, r.role_diversity])
    return (
        np.array(a_y, dtype=float),
        np.array(a_x, dtype=float),
        np.array(b_y, dtype=float),
        np.array(b_x, dtype=float),
    )


def run_oaxaca_by_gender(
    rows: list[PersonPanelRow],
    excl: dict[str, int],
    *,
    bootstrap_n: int = 500,
    rng_seed: int = 42,
) -> OaxacaSubgroupReport | None:
    """Gender female vs male Oaxaca。subgroup n < 100 なら None。"""
    a_y, a_x, b_y, b_x = _subgroup_arrays(rows, "female", "male")
    if a_y.size < _MIN_SUBGROUP_N or b_y.size < _MIN_SUBGROUP_N:
        log.warning(
            "oaxaca_subgroup_too_small",
            n_female=int(a_y.size),
            n_male=int(b_y.size),
            threshold=_MIN_SUBGROUP_N,
        )
        return None
    return decompose_subgroup(
        a_y, a_x, b_y, b_x,
        feature_names=("theta_i", "tenure_years", "role_diversity"),
        subgroup_label="gender: female vs male",
        bootstrap_n=bootstrap_n,
        rng_seed=rng_seed,
        n_excluded_missing_y=excl["missing_y"],
        n_excluded_missing_x=excl["missing_x"],
        n_excluded_missing_group=excl["missing_gender"],
    )


# ---------------------------------------------------------------------------
# Report HTML rendering
# ---------------------------------------------------------------------------


def _format_oaxaca_table(rep: OaxacaSubgroupReport) -> str:
    rows_html = "".join(
        f"<tr><td>{name}</td><td>{end:.4f}</td><td>{stru:.4f}</td></tr>"
        for name, end, stru in zip(
            rep.point.feature_names,
            rep.point.endowment_per_feature,
            rep.point.structural_per_feature,
        )
    )
    return (
        '<table class="oaxaca">'
        "<thead><tr><th>feature</th><th>endowment 寄与</th><th>structural 寄与</th></tr></thead>"
        f"<tbody>{rows_html}</tbody></table>"
    )


def _gender_coverage_block(total: int, missing_gender: int) -> tuple[str, bool]:
    """Coverage 警告ブロック。閾値超過なら警告フラグ True。"""
    if total == 0:
        return "", True
    null_rate = missing_gender / (missing_gender + total)
    low_coverage = null_rate > _GENDER_COVERAGE_THRESHOLD
    msg = (
        f"<p>分析対象 persons: {total:,} 人 "
        f"(gender null 除外 {missing_gender:,} 人、null 率 {100 * null_rate:.1f}%)。</p>"
    )
    if low_coverage:
        msg += (
            '<p style="color:#c0a040;">'
            "低 coverage 警告: gender null 率が 30% を超過。"
            "本分解は除外バイアスを受けるため、解釈は探索的。"
            "前提条件達成には gender enrichment (TODO §15) が必要。"
            "</p>"
        )
    return msg, low_coverage


class EquityOaxacaReport(BaseReportGenerator):
    """gender Oaxaca-Blinder 分解 (機会格差の構造分離)."""

    name = "equity_oaxaca"
    title = "機会格差の構造分解 (Oaxaca-Blinder)"
    subtitle = (
        "同等 theta_i / tenure / role_diversity 条件下での group 差を "
        "endowment と structural に分離。機会格差の核 = structural。"
    )
    doc_type = "main"
    filename = "equity_oaxaca.html"

    def generate(self) -> Path | None:
        rows, excl = _query_persons_for_oaxaca(self.conn)
        coverage_html, low_coverage = _gender_coverage_block(
            total=len(rows), missing_gender=excl["missing_gender"]
        )

        if not rows:
            body = coverage_html + "<p>データ不在のためレポート生成不可。</p>"
            return self.write_report(body)

        report = run_oaxaca_by_gender(rows, excl, bootstrap_n=500)

        sections: list[ReportSection] = []

        if report is None:
            sections.append(
                ReportSection(
                    title="Findings",
                    section_id="findings",
                    findings_html=(
                        coverage_html
                        + f"<p>gender subgroup の最小 N ({_MIN_SUBGROUP_N}) 未達。"
                        "Oaxaca 分解は実行されない。</p>"
                    ),
                    visualization_html="",
                    interpretation_html=(
                        "<p>本ファイル時点で gender 充足は分解実行水準に達していない。"
                        "後続: TODO §12.1 (AniList orphan backfill) + §12.3 (MAL Card 05) で "
                        "gender 充足 → 再走。</p>"
                    ),
                )
            )
            body = "\n".join(self.builder.build_section(s) for s in sections)
            return self.write_report(body)

        findings = (
            coverage_html
            + f"<p>raw_gap (female-male): {report.point.raw_gap:+.4f} "
            f"(CI [{report.raw_gap_ci_low:+.4f}, {report.raw_gap_ci_high:+.4f}])</p>"
            f"<p>endowment 寄与: {report.point.endowment:+.4f} "
            f"(CI [{report.endowment_ci_low:+.4f}, {report.endowment_ci_high:+.4f}])</p>"
            f"<p>structural 寄与: {report.point.structural:+.4f} "
            f"(CI [{report.structural_ci_low:+.4f}, {report.structural_ci_high:+.4f}])</p>"
            + _format_oaxaca_table(report)
        )

        interpretation = (
            "<p>endowment = 構造的位置 (theta_i / tenure / role_diversity) の差で説明される部分。"
            "structural = 同じ構造的位置でも処遇 (= β 係数) が異なる部分。</p>"
            "<p>structural が 0 と区別可能 (CI が 0 を含まない) かつ負 → "
            "「同じ構造的位置の女性が男性より小さい credit 機会を得ている」事実記述。</p>"
            "<p>本指標は "
            "<strong>主観的な評価とは無関係に、構造的に同位置な person 間の機会差</strong> "
            "を測る。group 定義 (female / male 二値) は openly な単純化であり、"
            "non-binary 等は別 cut で扱う。</p>"
        )

        sections.append(
            ReportSection(
                title="Findings — gender (female vs male)",
                section_id="findings_gender",
                findings_html=findings,
                visualization_html="",
                interpretation_html=interpretation,
            )
        )

        # 任意で append_validation_warnings (低 coverage 時)
        body = "\n".join(self.builder.build_section(s) for s in sections)
        if low_coverage:
            body = append_validation_warnings(
                body, ["gender null 率が 30% を超過、解釈は探索的"]
            )
        return self.write_report(body)

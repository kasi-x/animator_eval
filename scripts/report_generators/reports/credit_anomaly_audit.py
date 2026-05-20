"""Credit anomaly audit board (Technical appendix / quality 監視)。

3 detector (Poisson outlier / Role KL divergence / Source disagreement) の
audit board をまとめて表示。entity resolution drift 監視 (`28/01`) と相補。

H1: anime.score 非依存。
Flag は review priority のみ、自動修復はしない。
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import structlog

from src.analysis.quality.credit_anomaly import (
    detect_poisson_outliers,
    detect_role_divergence,
    detect_source_disagreement,
)

from ..section_builder import ReportSection
from ._base import BaseReportGenerator

log = structlog.get_logger(__name__)


_MAX_DISPLAY = 30


_MIN_CREDITS_FOR_AUDIT = 10
_MAX_AUDIT_PERSONS = 50000  # safety cap


def _query_credit_counts(conn: Any) -> dict[str, int]:
    """Per-person credit count (filtered: >= _MIN_CREDITS_FOR_AUDIT)."""
    queries = [
        f"""
        SELECT person_id, COUNT(*) AS n
        FROM credits
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        HAVING COUNT(*) >= {_MIN_CREDITS_FOR_AUDIT}
        ORDER BY n DESC
        LIMIT {_MAX_AUDIT_PERSONS}
        """,
        f"""
        SELECT person_id, COUNT(*) AS n
        FROM conformed.credits
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        HAVING COUNT(*) >= {_MIN_CREDITS_FOR_AUDIT}
        ORDER BY n DESC
        LIMIT {_MAX_AUDIT_PERSONS}
        """,
    ]
    for sql in queries:
        try:
            rows = conn.execute(sql).fetchall()
            return {r[0]: int(r[1]) for r in rows if r[0]}
        except Exception as exc:
            log.debug("credit_anomaly_count_attempt_failed", error=str(exc))
    return {}


def _query_role_distribution(conn: Any, person_ids: list[str]) -> dict[str, dict[str, int]]:
    """person_id × role → count (filtered by person_ids subset for speed)."""
    if not person_ids:
        return {}
    # DuckDB IN list cap (派手な数を避ける)
    person_ids = person_ids[:5000]
    ids_csv = ",".join("'" + p.replace("'", "''") + "'" for p in person_ids)
    queries = [
        f"""
        SELECT person_id, role, COUNT(*) AS n
        FROM credits
        WHERE person_id IN ({ids_csv}) AND role IS NOT NULL
        GROUP BY 1, 2
        """,
        f"""
        SELECT person_id, role, COUNT(*) AS n
        FROM conformed.credits
        WHERE person_id IN ({ids_csv}) AND role IS NOT NULL
        GROUP BY 1, 2
        """,
    ]
    for sql in queries:
        try:
            rows = conn.execute(sql).fetchall()
            d: dict[str, dict[str, int]] = defaultdict(dict)
            for pid, role, n in rows:
                if pid and role:
                    d[pid][role] = int(n)
            return dict(d)
        except Exception as exc:
            log.debug("credit_anomaly_role_attempt_failed", error=str(exc))
    return {}


def _query_source_counts(conn: Any, person_ids: list[str]) -> dict[str, dict[str, int]]:
    """canonical_id × source → count (filtered)."""
    if not person_ids:
        return {}
    person_ids = person_ids[:5000]
    ids_csv = ",".join("'" + p.replace("'", "''") + "'" for p in person_ids)
    queries = [
        f"""
        SELECT person_id, evidence_source, COUNT(*)
        FROM credits
        WHERE person_id IN ({ids_csv}) AND evidence_source IS NOT NULL
        GROUP BY 1, 2
        """,
        f"""
        SELECT person_id, evidence_source, COUNT(*)
        FROM conformed.credits
        WHERE person_id IN ({ids_csv}) AND evidence_source IS NOT NULL
        GROUP BY 1, 2
        """,
    ]
    for sql in queries:
        try:
            rows = conn.execute(sql).fetchall()
            d: dict[str, dict[str, int]] = defaultdict(dict)
            for pid, src, n in rows:
                d[pid][src] = int(n)
            return dict(d)
        except Exception as exc:
            log.debug("credit_anomaly_source_attempt_failed", error=str(exc))
    return {}


class CreditAnomalyAuditReport(BaseReportGenerator):
    """3 detector を統合した audit board (technical appendix)."""

    name = "credit_anomaly_audit"
    title = "Credit anomaly audit (品質監視)"
    subtitle = (
        "Poisson outlier / Role KL divergence / Source disagreement の "
        "3 detector で credit attribution 異常を review priority flag"
    )
    doc_type = "appendix"
    filename = "credit_anomaly_audit.html"

    def generate(self) -> Path | None:
        # 1. Load (top-N by credit count for speed)
        credit_counts = _query_credit_counts(self.conn)
        top_persons = list(credit_counts.keys())
        role_dist = _query_role_distribution(self.conn, top_persons)
        source_counts = _query_source_counts(self.conn, top_persons)

        # 2. Detect (with conservative thresholds for safety)
        poisson_outs = detect_poisson_outliers(
            credit_counts, z_threshold=4.0, min_expected=5.0,
        ) if credit_counts else []
        role_divs = detect_role_divergence(
            role_dist, kl_threshold=2.0, min_credits=20,
        ) if role_dist else []
        src_disagree = detect_source_disagreement(
            source_counts, spread_threshold=5.0, z_threshold=2.5, min_total=20,
        ) if source_counts else []

        # 3. Sections
        sections = []

        # --- Poisson outliers ---
        if poisson_outs:
            rows_html = "".join(
                f"<tr><td>{o.person_id}</td><td>{o.observed:,}</td>"
                f"<td>{o.expected:.1f}</td><td>{o.z_score:+.2f}</td>"
                f"<td>{o.direction}</td></tr>"
                for o in poisson_outs[:_MAX_DISPLAY]
            )
            findings = (
                f"<p>Poisson outlier 検出: {len(poisson_outs):,} 件 "
                f"(|z| >= 4σ, top {min(_MAX_DISPLAY, len(poisson_outs))} 表示)</p>"
                "<table><thead><tr><th>person_id</th><th>obs</th>"
                "<th>expected</th><th>z</th><th>dir</th></tr></thead>"
                f"<tbody>{rows_html}</tbody></table>"
            )
        else:
            findings = "<p>Poisson outlier 該当なし (or データ不在)。</p>"
        sections.append(self.builder.build_section(
            ReportSection(
                title="Poisson credit outliers",
                section_id="poisson_outliers",
                findings_html=findings,
                method_note=(
                    "Per-cohort credit count を Poisson(μ) 仮定で z = (obs - μ) / sqrt(μ)。"
                    "|z| >= 4 で flag (本実装は保守閾値)。direction = high / low 両方向。"
                ),
                interpretation_html=(
                    "<p>本稿の解釈: high outlier は過剰クレジット集中の signal、"
                    "low outlier は under-credit の可能性と考えられる。"
                    "review priority のみで自動修復は実施しない。</p>"
                ),
            )
        ))

        # --- Role divergence ---
        if role_divs:
            rows_html = "".join(
                f"<tr><td>{r.person_id}</td><td>{r.kl_divergence:.3f}</td>"
                f"<td>{r.n_credits:,}</td><td>{r.dominant_role}</td>"
                f"<td>{r.dominant_role_share:.2f}</td></tr>"
                for r in role_divs[:_MAX_DISPLAY]
            )
            findings = (
                f"<p>Role distribution divergence: {len(role_divs):,} 件 "
                f"(KL >= 2.0, top {min(_MAX_DISPLAY, len(role_divs))})</p>"
                "<table><thead><tr><th>person_id</th><th>KL</th>"
                "<th>n_credits</th><th>dominant_role</th><th>share</th></tr></thead>"
                f"<tbody>{rows_html}</tbody></table>"
            )
        else:
            findings = "<p>Role divergence 該当なし。</p>"
        sections.append(self.builder.build_section(
            ReportSection(
                title="Role distribution divergence",
                section_id="role_divergence",
                findings_html=findings,
                method_note=(
                    "KL(p_person || q_cohort_marginal) を計算。"
                    "KL >= 2.0 かつ min_credits = 20 で flag。"
                    "role taxonomy 整備度に依存することに注意。"
                ),
                interpretation_html=(
                    "<p>本稿の解釈: 高 KL は cohort norm から特殊な役職構成を持つ person を"
                    "意味する。特殊キャリア (e.g. director 一筋) か "
                    "データ不整合 (誤マッチ後の role 混入) の二択と考えられる。</p>"
                ),
            )
        ))

        # --- Source disagreement ---
        if src_disagree:
            rows_html = "".join(
                f"<tr><td>{s.canonical_id}</td><td>{s.spread_ratio:.1f}</td>"
                f"<td>{s.max_count:,}</td><td>{s.min_count:,}</td>"
                f"<td>{s.z_max:+.2f}</td></tr>"
                for s in src_disagree[:_MAX_DISPLAY]
            )
            findings = (
                f"<p>Source disagreement: {len(src_disagree):,} 件 "
                f"(spread >= 5x, top {min(_MAX_DISPLAY, len(src_disagree))})</p>"
                "<table><thead><tr><th>canonical_id</th><th>spread</th>"
                "<th>max_count</th><th>min_count</th><th>z_max</th></tr></thead>"
                f"<tbody>{rows_html}</tbody></table>"
            )
        else:
            findings = "<p>Source disagreement 該当なし。</p>"
        sections.append(self.builder.build_section(
            ReportSection(
                title="Multi-source disagreement",
                section_id="source_disagreement",
                findings_html=findings,
                method_note=(
                    "同 canonical_id に対する source 別 credit 数の spread と "
                    "per-source z-score。spread >= 5x かつ |z_max| >= 2.5 で flag。"
                    "source 自体のカバレッジ差で false positive が出る点に注意。"
                ),
                interpretation_html=(
                    "<p>本稿の解釈: 高 spread は片方の source が誤マッチしている可能性の"
                    "示唆と考えられる。entity resolution audit (28/01 drift) と相補的に使用する。</p>"
                ),
            )
        ))

        body = "\n".join(sections)
        return self.write_report(body)


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="credit_anomaly_audit",
    audience="appendix",
    claim=(
        "3 detector (Poisson outlier / Role KL / Source disagreement) を統合し、"
        "credit attribution の統計的異常を review priority として flag する。"
        "自動修復はせず、entity resolution audit と相補に使用する。"
    ),
    identifying_assumption=(
        "Poisson(μ) 仮定 (per-cohort)、role marginal 安定、source カバレッジ差は"
        "false positive 要因。本 detector は false positive 許容、false negative 最小化。"
    ),
    null_model=["per-cohort statistics (mean / sd) baseline"],
    sources=["credits", "persons"],
    meta_table="meta_credit_anomaly_audit",
    estimator="z-score + KL divergence + spread ratio",
    ci_estimator="bootstrap", n_resamples=200,
    extra_limitations=[
        "Poisson 単峰仮定: bipolar 分布で false positive リスク",
        "role taxonomy 整備度に依存",
        "source カバレッジ差で source_disagreement の false positive",
        "review priority のみ、誤マッチ確定ではない",
    ],
    alternative_interpretations=(
        "Poisson outlier は単に productive person (director / 大物 KA) を flag しているだけで、真の異常ではない可能性。手動 review で確認要。",
        "Role KL divergence は cohort 切り口次第で値が大きく変動。年代別 cohort に細分化すると flag 数が変わる可能性。",
        "Source disagreement は entity resolution の問題ではなく、source 別に scrape されたクレジットの実態的相違 (e.g. ANN は IP 関連クレジット、AniList は staff クレジット偏重) を反映している可能性。",
    ),
)

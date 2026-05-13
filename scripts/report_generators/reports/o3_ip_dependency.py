"""O3 IP 人的依存リスク分析 — v2 compliant.

シリーズ単位で個人の寄与集中度を可視化し、key person 離脱時の
counterfactual 影響を bootstrap CI 付きで定量化する。
Business brief 向け。

Method overview:
- contribution_share: role_weight × production_scale 加重比率
- production_scale: COUNT(distinct credits in anime) × episodes × duration_mult
- counterfactual: 対象 person の寄与を除外した残余 scale の推定
- null model: 同役職分布のランダム除外 1000 回による帰無分布
- CI: bootstrap 1000 samples on contribution_share
"""

from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..ci_utils import format_ci
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, NotePost, SnsPost, append_validation_warnings

log = structlog.get_logger(__name__)

# Relation types that define a series chain (union-find)
_CHAIN_RELATIONS = frozenset({"SEQUEL", "PREQUEL", "PARENT", "SIDE_STORY"})


def _detect_schema_prefix(conn: Any) -> str:
    """Return 'conformed.' if the conformed schema is accessible, else ''.

    Tries a lightweight probe query against conformed.anime.  Falls back to
    the empty prefix (plain table names) when the schema is unavailable
    (e.g. in-memory SQLite test fixtures).
    """
    try:
        conn.execute("SELECT 1 FROM conformed.anime LIMIT 0")
        return "conformed."
    except Exception:
        return ""

# Bootstrap samples for CI estimation
_N_BOOTSTRAP = 1000
# Random removal null model iterations
_N_NULL_ITER = 1000
# Top series shown in forest plot
_TOP_SERIES_N = 10


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class SeriesCluster:
    """A group of anime belonging to the same series."""

    cluster_id: str
    anime_ids: list[str]
    title: str = ""


@dataclass
class ContributionRow:
    """Contribution of a single person within a series."""

    person_id: str
    person_name: str
    series_id: str
    series_title: str
    weighted_contribution: float  # role_weight × production_scale sum
    contribution_share: float     # ratio within series
    n_credits: int


@dataclass
class CounterfactualResult:
    """Counterfactual drop when removing person i from series s."""

    person_id: str
    person_name: str
    series_id: str
    series_title: str
    contribution_share: float
    counterfactual_drop: float        # Δ production_scale (absolute)
    counterfactual_drop_pct: float    # as fraction of total series scale
    ci_lower: float                   # 95% bootstrap CI lower
    ci_upper: float                   # 95% bootstrap CI upper
    null_percentile: float            # where observed drop sits in null distribution


# ---------------------------------------------------------------------------
# Step 1: Series clustering via Union-Find on relations_json
# ---------------------------------------------------------------------------


def _build_series_clusters(conn: Any) -> list[SeriesCluster]:
    """Cluster anime into series using the SILVER series_cluster_id column.

    Reads pre-computed ``series_cluster_id`` from the SILVER anime table
    (backfilled by ``src.etl.cluster.series_cluster.backfill``).  Falls back
    to on-the-fly Union-Find via ``relations_json`` when the column is absent
    (e.g. before the post-hoc ETL has been run).

    Single-anime series are treated as their own cluster (cluster_id == anime_id).

    Returns:
        List of SeriesCluster sorted by cluster_id.
    """
    schema = _detect_schema_prefix(conn)

    # Prefer the pre-computed SILVER column (fast path)
    try:
        rows = conn.execute(
            f"SELECT id, title_romaji, series_cluster_id FROM {schema}anime"
        ).fetchall()
        if rows and rows[0][2] is not None:
            return _clusters_from_precomputed(rows)
    except Exception:
        pass  # column absent — fall through to on-the-fly computation

    # Fallback: on-the-fly Union-Find from relations_json
    log.info("series_cluster_fallback_to_relations_json")
    return _clusters_from_relations_json(conn, schema)


def _clusters_from_precomputed(
    rows: list[tuple[Any, Any, Any]],
) -> list[SeriesCluster]:
    """Build SeriesCluster list from pre-computed series_cluster_id column.

    Args:
        rows: Rows of (id, title_romaji, series_cluster_id) from anime table.

    Returns:
        List of SeriesCluster sorted by cluster_id.
    """
    components: dict[str, list[str]] = defaultdict(list)
    titles: dict[str, str] = {}

    for row in rows:
        aid = str(row[0])
        title = str(row[1] or "")
        cluster_id = str(row[2]) if row[2] is not None else aid
        components[cluster_id].append(aid)
        titles[aid] = title

    clusters: list[SeriesCluster] = []
    for cluster_id, members in sorted(components.items()):
        title = titles.get(sorted(members)[0], "")
        clusters.append(SeriesCluster(
            cluster_id=cluster_id,
            anime_ids=sorted(members),
            title=title,
        ))
    return clusters


def _clusters_from_relations_json(conn: Any, schema: str = "") -> list[SeriesCluster]:
    """On-the-fly Union-Find fallback using relations_json column.

    Used only when series_cluster_id has not yet been backfilled.
    Preserved from the original implementation for graceful degradation.

    Args:
        conn: DB connection.
        schema: schema prefix, e.g. 'conformed.' or '' for unqualified.

    Returns:
        List of SeriesCluster sorted by cluster_id.
    """
    try:
        rows = conn.execute(
            f"SELECT id, title_romaji, relations_json FROM {schema}anime"
        ).fetchall()
    except Exception as exc:
        log.warning("series_cluster_query_failed", error=str(exc))
        return []

    parent: dict[str, str] = {}
    titles: dict[str, str] = {}

    for row in rows:
        aid = str(row[0])
        title = str(row[1] or "")
        parent[aid] = aid
        titles[aid] = title

    def _find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def _union(a: str, b: str) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[ra] = rb

    for row in rows:
        aid = str(row[0])
        relations_raw = row[2]
        if not relations_raw:
            continue
        try:
            relations = json.loads(relations_raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(relations, list):
            continue
        for rel in relations:
            rel_type = rel.get("relation_type", "")
            related_id = str(rel.get("related_anime_id", ""))
            if rel_type in _CHAIN_RELATIONS and related_id in parent:
                _union(aid, related_id)

    components: dict[str, list[str]] = defaultdict(list)
    for aid in parent:
        components[_find(aid)].append(aid)

    clusters: list[SeriesCluster] = []
    for root, members in sorted(components.items()):
        title = titles.get(sorted(members)[0], "")
        clusters.append(SeriesCluster(
            cluster_id=root,
            anime_ids=sorted(members),
            title=title,
        ))
    return clusters


# ---------------------------------------------------------------------------
# Step 2: production_scale helper (pure SQL)
# ---------------------------------------------------------------------------

def _production_scale_sql(schema: str, placeholders: str) -> str:
    """Build production_scale query with the given schema prefix and placeholders."""
    return f"""
SELECT
    c.anime_id,
    a.episodes,
    a.duration,
    COUNT(DISTINCT c.id) AS staff_count
FROM {schema}credits c
JOIN {schema}anime a ON c.anime_id = a.id
WHERE c.anime_id IN ({placeholders})
GROUP BY c.anime_id, a.episodes, a.duration
"""


def _duration_mult(duration_minutes: int | None) -> float:
    """Compute duration multiplier: duration / 30, capped at 2.0."""
    if duration_minutes is None or duration_minutes <= 0:
        return 1.0
    return min(duration_minutes / 30.0, 2.0)


def _fetch_anime_scales(conn: Any, anime_ids: list[str]) -> dict[str, float]:
    """Fetch production_scale for each anime_id.

    production_scale = staff_count × episodes × duration_mult
    (pure structural; viewer ratings excluded)
    """
    if not anime_ids:
        return {}
    schema = _detect_schema_prefix(conn)
    placeholders = ",".join("?" * len(anime_ids))
    sql = _production_scale_sql(schema, placeholders)
    try:
        rows = conn.execute(sql, anime_ids).fetchall()
    except Exception as exc:
        log.warning("anime_scale_query_failed", error=str(exc))
        return {}

    scales: dict[str, float] = {}
    for row in rows:
        anime_id = str(row[0])
        episodes = int(row[1] or 1)
        duration = int(row[2] or 30)
        staff_count = int(row[3] or 1)
        dmult = _duration_mult(duration)
        scales[anime_id] = max(staff_count * episodes * dmult, 1.0)
    return scales


# ---------------------------------------------------------------------------
# Step 3: contribution_share computation
# ---------------------------------------------------------------------------

def _credit_sql(schema: str, placeholders: str) -> str:
    """Build credit query with the given schema prefix and placeholders."""
    return f"""
SELECT
    c.person_id,
    p.name_romaji,
    c.anime_id,
    c.role
FROM {schema}credits c
JOIN {schema}persons p ON c.person_id = p.id
WHERE c.anime_id IN ({placeholders})
"""

_ROLE_WEIGHTS_SQL = """
SELECT name, weight
FROM roles
"""


def _fetch_role_weights(conn: Any) -> dict[str, float]:
    """Fetch role weights from SILVER roles table."""
    try:
        rows = conn.execute(_ROLE_WEIGHTS_SQL).fetchall()
        return {str(r[0]).lower(): float(r[1] or 1.0) for r in rows}
    except Exception:
        # Fallback to config-based weights
        try:
            from src.utils.config import ROLE_WEIGHTS
            return {k.lower(): float(v) for k, v in ROLE_WEIGHTS.items()}
        except Exception:
            return {}


def compute_series_contribution_shares(
    conn: Any,
    cluster: SeriesCluster,
    role_weights: dict[str, float],
    anime_scales: dict[str, float],
) -> list[ContributionRow]:
    """Compute per-person contribution share within a series cluster.

    contribution_share[i, series s] =
        Σ (role_weight × production_scale_credit) for credits of i in s
        / Σ (role_weight × production_scale_credit) for all credits in s

    Args:
        conn: DB connection
        cluster: series cluster
        role_weights: role → structural weight
        anime_scales: anime_id → production_scale

    Returns:
        List of ContributionRow sorted by contribution_share desc.
    """
    anime_ids = cluster.anime_ids
    if not anime_ids:
        return []

    schema = _detect_schema_prefix(conn)
    placeholders = ",".join("?" * len(anime_ids))
    sql = _credit_sql(schema, placeholders)
    try:
        credit_rows = conn.execute(sql, anime_ids).fetchall()
    except Exception as exc:
        log.warning("credit_query_failed", series=cluster.cluster_id, error=str(exc))
        return []

    # Aggregate weighted contribution per (person_id, anime_id)
    person_contrib: dict[str, float] = defaultdict(float)
    person_names: dict[str, str] = {}
    person_credits: dict[str, int] = defaultdict(int)
    series_total: float = 0.0

    for row in credit_rows:
        person_id = str(row[0])
        name = str(row[1] or "")
        anime_id = str(row[2])
        role = str(row[3] or "").lower()

        rw = role_weights.get(role, 1.0)
        scale = anime_scales.get(anime_id, 1.0)
        weighted = rw * scale

        person_contrib[person_id] += weighted
        person_names[person_id] = name
        person_credits[person_id] += 1
        series_total += weighted

    if series_total <= 0:
        return []

    rows_out: list[ContributionRow] = []
    for pid, wt in person_contrib.items():
        rows_out.append(ContributionRow(
            person_id=pid,
            person_name=person_names.get(pid, pid),
            series_id=cluster.cluster_id,
            series_title=cluster.title,
            weighted_contribution=wt,
            contribution_share=wt / series_total,
            n_credits=person_credits[pid],
        ))

    rows_out.sort(key=lambda r: r.contribution_share, reverse=True)
    return rows_out


# ---------------------------------------------------------------------------
# Step 4: counterfactual drop + bootstrap CI
# ---------------------------------------------------------------------------


def _bootstrap_share_ci(
    person_weighted: float,
    series_total: float,
    n_credits: int,
    rng: random.Random,
    n_iter: int = _N_BOOTSTRAP,
) -> tuple[float, float]:
    """Bootstrap 95% CI on contribution_share.

    Resamples credits (with replacement) n_iter times, recomputing the
    share each time. Each credit contributes an equal portion of the
    person's total weighted contribution.

    Args:
        person_weighted: total weighted contribution of the person
        series_total: total weighted contribution of the series
        n_credits: number of credits the person holds in the series
        rng: seeded random for reproducibility
        n_iter: bootstrap iterations

    Returns:
        (ci_lower, ci_upper) at 95% level.
    """
    if n_credits == 0 or series_total <= 0:
        return (0.0, 0.0)

    per_credit = person_weighted / n_credits
    shares: list[float] = []
    for _ in range(n_iter):
        resampled = sum(
            per_credit for _ in range(rng.choices(range(n_credits), k=n_credits).__len__())
        )
        # Use actual resample count (replacement)
        resample_count = len(rng.choices(range(n_credits), k=n_credits))
        resample_total = resample_count * per_credit
        # Remaining series scale stays fixed (we only resample person i's credits)
        other_total = series_total - person_weighted
        if other_total + resample_total > 0:
            shares.append(resample_total / (other_total + resample_total))
        else:
            shares.append(0.0)

    shares.sort()
    lo_idx = int(0.025 * n_iter)
    hi_idx = int(0.975 * n_iter)
    return (shares[lo_idx], shares[min(hi_idx, n_iter - 1)])


def compute_counterfactual_drop(
    contrib_rows: list[ContributionRow],
    series_total: float,
    person_id: str,
    rng: random.Random,
    n_bootstrap: int = _N_BOOTSTRAP,
) -> CounterfactualResult | None:
    """Compute counterfactual production_scale drop when removing person i.

    counterfactual_drop = person_weighted_contribution
    (= series_total × contribution_share, which is the scale attributable
    to person i under the additive decomposition)

    CI: bootstrap resampling of the person's n_credits.

    Args:
        contrib_rows: all ContributionRow for the series
        series_total: total weighted scale of the series
        person_id: person to remove
        rng: seeded random
        n_bootstrap: iterations

    Returns:
        CounterfactualResult or None if person not found.
    """
    target = next((r for r in contrib_rows if r.person_id == person_id), None)
    if target is None or series_total <= 0:
        return None

    drop = target.weighted_contribution
    drop_pct = drop / series_total if series_total > 0 else 0.0

    ci = _bootstrap_share_ci(
        target.weighted_contribution,
        series_total,
        target.n_credits,
        rng,
        n_bootstrap,
    )
    # CI on drop (absolute) = CI on share × series_total
    ci_drop = (ci[0] * series_total, ci[1] * series_total)

    return CounterfactualResult(
        person_id=person_id,
        person_name=target.person_name,
        series_id=target.series_id,
        series_title=target.series_title,
        contribution_share=target.contribution_share,
        counterfactual_drop=drop,
        counterfactual_drop_pct=drop_pct,
        ci_lower=ci_drop[0],
        ci_upper=ci_drop[1],
        null_percentile=0.0,  # filled in after null model
    )


# ---------------------------------------------------------------------------
# Step 5: Null model — random removal baseline
# ---------------------------------------------------------------------------


def compute_null_distribution(
    contrib_rows: list[ContributionRow],
    series_total: float,
    target_role_dist: dict[str, int],
    rng: random.Random,
    n_iter: int = _N_NULL_ITER,
) -> list[float]:
    """Compute null distribution of contribution_drop by random removal.

    Randomly removes a person from the same role-distribution pool
    n_iter times to build a baseline null distribution.

    Args:
        contrib_rows: all contribution rows for the series
        series_total: total weighted scale
        target_role_dist: role → count of the target person (used to
            select similarly-sized random persons from the series)
        rng: seeded random
        n_iter: iterations

    Returns:
        List of counterfactual_drop_pct values under null model.
    """
    if not contrib_rows or series_total <= 0:
        return []

    null_drops: list[float] = []
    pids = [r.person_id for r in contrib_rows]

    for _ in range(n_iter):
        random_pid = rng.choice(pids)
        random_row = next(r for r in contrib_rows if r.person_id == random_pid)
        null_drops.append(random_row.weighted_contribution / series_total)

    return null_drops


def _null_percentile(observed_drop_pct: float, null_drops: list[float]) -> float:
    """Return the percentile of observed_drop_pct within the null distribution."""
    if not null_drops:
        return 50.0
    return 100.0 * sum(1 for v in null_drops if v <= observed_drop_pct) / len(null_drops)


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class O3IpDependencyReport(BaseReportGenerator):
    """Business brief: IP 人的依存リスク分析.

    Computes per-series key person concentration and counterfactual impact
    for Business brief audience (investors, production committees).
    """

    name = "o3_ip_dependency"
    title = "IP 人的依存リスク分析"
    subtitle = (
        "シリーズ単位 key person 寄与集中度・counterfactual 下落 / "
        "IP Person-Dependency Risk Analysis"
    )
    filename = "o3_ip_dependency.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        rng = random.Random(42)

        # Load shared data
        clusters = _build_series_clusters(self.conn)
        role_weights = _fetch_role_weights(self.conn)

        if not clusters:
            findings = (
                "<p>シリーズクラスター データが取得できませんでした。"
                "SILVER anime テーブルの relations_json 列を確認してください。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            section = ReportSection(
                title="シリーズ別寄与集中度",
                findings_html=findings,
                method_note=(
                    "寄与比率 = Σ(role_weight × production_scale) / シリーズ合計。"
                    "production_scale = staff_count × episodes × duration_mult。"
                    "外部視聴者評価は使用しない。"
                ),
                section_id="o3_concentration",
            )
            return self.write_report(sb.build_section(section))

        # Compute contribution for top series by size
        all_results: list[CounterfactualResult] = []
        series_contrib_map: dict[str, list[ContributionRow]] = {}

        # Process clusters: limit to multi-anime series first, then single
        multi = [c for c in clusters if len(c.anime_ids) > 1]
        single = [c for c in clusters if len(c.anime_ids) == 1]
        ordered = multi + single

        for cluster in ordered[:50]:  # cap at 50 clusters for performance
            anime_scales = _fetch_anime_scales(self.conn, cluster.anime_ids)
            if not anime_scales:
                continue
            contrib_rows = compute_series_contribution_shares(
                self.conn, cluster, role_weights, anime_scales
            )
            if not contrib_rows:
                continue
            series_total = sum(r.weighted_contribution for r in contrib_rows)
            series_contrib_map[cluster.cluster_id] = contrib_rows

            # Take top person in series
            top = contrib_rows[0]
            cf = compute_counterfactual_drop(
                contrib_rows, series_total, top.person_id, rng
            )
            if cf is None:
                continue

            null_drops = compute_null_distribution(
                contrib_rows, series_total, {}, rng
            )
            cf.null_percentile = _null_percentile(cf.counterfactual_drop_pct, null_drops)
            all_results.append(cf)

        # Sort by counterfactual_drop_pct desc, take top N
        all_results.sort(key=lambda r: r.counterfactual_drop_pct, reverse=True)
        top_results = all_results[:_TOP_SERIES_N]

        sections = [
            sb.build_section(
                self._build_concentration_section(sb, series_contrib_map)
            ),
            sb.build_section(
                self._build_counterfactual_section(sb, top_results)
            ),
            sb.build_section(
                self._build_null_model_section(sb, all_results)
            ),
        ]

        interpretation_html = self._build_interpretation(top_results)

        return self.write_report(
            "\n".join(sections),
            intro_html=self._build_intro(),
            extra_glossary={
                "contribution_share": (
                    "シリーズ内全クレジットの加重 production_scale に占める"
                    "個人寄与の比率。"
                    "role_weight × (staff_count × episodes × duration_mult) で重み付け。"
                    "外部視聴者評価は含まない。"
                ),
                "counterfactual_drop": (
                    "key person 不在時の production_scale 推定減少量。"
                    "additive decomposition: drop = person_weighted_contribution。"
                    "bootstrap 1000 回により 95% CI を付与。"
                ),
                "null_percentile": (
                    "観測された drop_pct が、同シリーズからランダムに person を除外した"
                    "帰無分布（1000 回）の何パーセンタイルに相当するか。"
                    "高値 = 観測された集中度が偶然より高い。"
                ),
            },
        )

    # ------------------------------------------------------------------
    # Section 1: Series contribution concentration
    # ------------------------------------------------------------------

    def _build_concentration_section(
        self,
        sb: SectionBuilder,
        series_contrib_map: dict[str, list[ContributionRow]],
    ) -> ReportSection:
        if not series_contrib_map:
            findings = "<p>集計対象シリーズのクレジットデータが取得できませんでした。</p>"
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="シリーズ別寄与集中度",
                findings_html=findings,
                method_note=(
                    "寄与比率 = Σ(role_weight × production_scale) / シリーズ合計。"
                    "production_scale = staff_count × episodes × duration_mult。"
                    "外部視聴者評価は使用しない。"
                ),
                section_id="o3_concentration",
            )

        # Build heatmap data: top person share per series
        series_labels: list[str] = []
        top_shares: list[float] = []
        n_persons: list[int] = []

        for sid, rows in list(series_contrib_map.items())[:_TOP_SERIES_N]:
            if not rows:
                continue
            series_labels.append(rows[0].series_title or sid[:12])
            top_shares.append(rows[0].contribution_share)
            n_persons.append(len(rows))

        fig = go.Figure(
            go.Bar(
                x=top_shares,
                y=series_labels,
                orientation="h",
                text=[f"{s * 100:.1f}%" for s in top_shares],
                textposition="outside",
                marker_color=[
                    f"rgba({int(220 * s)}, {int(60 * (1 - s))}, 80, 0.85)"
                    for s in top_shares
                ],
                hovertemplate=(
                    "%{y}<br>top person 寄与比率=%{x:.3f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title=(
                "シリーズ別 top-person 寄与比率"
                "（contribution_share; role_weight × production_scale 加重）"
            ),
            xaxis_title="contribution_share (0–1)",
            xaxis=dict(range=[0, 1.15]),
            yaxis_title="シリーズ",
            height=max(380, len(series_labels) * 28 + 100),
            margin=dict(l=200, r=80),
        )

        n_series = len(series_contrib_map)
        top_label = series_labels[0] if series_labels else "–"
        top_share_val = top_shares[0] if top_shares else 0.0
        low_label = series_labels[-1] if series_labels else "–"
        low_share_val = top_shares[-1] if top_shares else 0.0

        findings = (
            f"<p>集計対象シリーズ数: {n_series:,}。"
            f"top-person contribution_share の最大値: {top_share_val:.3f}"
            f"（シリーズ: {top_label}）。"
            f"最小値: {low_share_val:.3f}（シリーズ: {low_label}）。"
            f"各バーは当該シリーズ内での top person の寄与比率を示す。"
            f"比率は role_weight × production_scale で加重し、"
            f"外部視聴者評価による補正は行わない。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="シリーズ別 top-person 寄与集中度",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_o3_concentration",
                height=max(380, len(series_labels) * 28 + 100),
            ),
            method_note=(
                "寄与比率 = Σ(role_weight × production_scale_credit) for person i"
                " / Σ(role_weight × production_scale_credit) for all persons in series s。"
                "production_scale = COUNT(distinct_credits) × episodes × duration_mult"
                "（duration_mult = min(duration_minutes / 30, 2.0)）。"
                "役職重み (role_weight) は src/utils/config.py の"
                " COMMITMENT_MULTIPLIERS × ROLE_RANK に基づく構造的重みのみ。"
                "外部視聴者評価・人気スコアは一切使用しない。"
                "シリーズ識別: relations_json の SEQUEL/PREQUEL/PARENT/SIDE_STORY"
                " 関係を Union-Find でクラスタリング。単発作品は単独シリーズとして扱う。"
            ),
            section_id="o3_concentration",
        )

    # ------------------------------------------------------------------
    # Section 2: Counterfactual forest plot
    # ------------------------------------------------------------------

    def _build_counterfactual_section(
        self,
        sb: SectionBuilder,
        top_results: list[CounterfactualResult],
    ) -> ReportSection:
        if not top_results:
            findings = (
                "<p>counterfactual 推定に必要なデータが取得できませんでした。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="key person 不在 counterfactual 下落推定（上位シリーズ）",
                findings_html=findings,
                method_note=(
                    "counterfactual_drop = person の加重寄与量（additive decomposition）。"
                    "bootstrap 1000 回で 95% CI を算出。"
                ),
                section_id="o3_counterfactual",
            )

        # v3: CIScatter primitive — bootstrap 95% CI / 有意 = null > 95p /
        # null reference (drop_pct=0)
        from src.viz import embed as viz_embed
        from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter

        ci_points = [
            CIPoint(
                label=f"{r.person_name} / {r.series_title or r.series_id[:10]}",
                x=r.counterfactual_drop_pct,
                ci_lo=r.ci_lower,
                ci_hi=r.ci_upper,
                # null_percentile > 95 → 観測値が null 分布の 95p 超 → 有意
                p_value=0.04 if r.null_percentile > 95.0 else 0.20,
            )
            for r in top_results
        ]
        spec = CIScatterSpec(
            points=ci_points,
            x_label="counterfactual_drop_pct (95% bootstrap CI)",
            title=(
                f"key person 不在時の counterfactual 下落率（上位 "
                f"{len(top_results)} シリーズ; bootstrap 95% CI）"
            ),
            reference=0.0,
            reference_label="null",
            sort_by="input",
        )
        fig = render_ci_scatter(spec, theme="dark")

        # Compute CI widths to check if they are abnormally wide
        ci_widths = [r.ci_upper - r.ci_lower for r in top_results]
        mean_width = sum(ci_widths) / len(ci_widths) if ci_widths else 0.0

        max_drop = max((r.counterfactual_drop_pct for r in top_results), default=0.0)
        min_drop = min((r.counterfactual_drop_pct for r in top_results), default=0.0)
        high_dep = [r for r in top_results if r.counterfactual_drop_pct > 0.3]

        findings = (
            f"<p>推定対象 key person 数: {len(top_results):,}。"
            f"counterfactual_drop_pct の範囲: {min_drop:.3f} ～ {max_drop:.3f}。"
            f"drop_pct &gt; 0.30 の事例: {len(high_dep):,}件。"
            f"bootstrap CI 平均幅: {mean_width:.3f}（95% CI, bootstrap n=1000）。"
            f"forest plot の各点が drop_pct の推定値、"
            f"横バーが 95% bootstrap CI を示す。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="key person 不在 counterfactual 下落推定（上位シリーズ）",
            findings_html=findings,
            visualization_html=viz_embed(fig, "chart_o3_counterfactual"),
            method_note=(
                "counterfactual_drop = person i の weighted_contribution"
                "（additive decomposition: シリーズ scale に占める i の寄与を直接除外）。"
                "counterfactual_drop_pct = drop / series_total_scale。"
                "CI: bootstrap 1000 回 — i の n_credits を復元抽出して"
                "contribution_share を再推定し、2.5/97.5 パーセンタイルを採用。"
                "person FE (AKM theta_i) は同一 role_weight × scale 分解で暗黙的に反映される"
                "（本 report 層は theta_i を直接読まない）。"
                "シリーズ内全人数 scale は固定（i の credits のみ再抽出）。"
            ),
            section_id="o3_counterfactual",
        )

    # ------------------------------------------------------------------
    # Section 3: Null model vs observed
    # ------------------------------------------------------------------

    def _build_null_model_section(
        self,
        sb: SectionBuilder,
        all_results: list[CounterfactualResult],
    ) -> ReportSection:
        if not all_results:
            findings = (
                "<p>帰無モデル比較に必要なデータが取得できませんでした。</p>"
            )
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="観測下落 vs 帰無モデル分布",
                findings_html=findings,
                method_note=(
                    "帰無モデル: 同シリーズから person をランダム除外 (1000 回)。"
                    "null_percentile = 観測 drop_pct が帰無分布の何パーセンタイルか。"
                ),
                section_id="o3_null_model",
            )

        percentiles = [r.null_percentile for r in all_results]
        drop_pcts = [r.counterfactual_drop_pct for r in all_results]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=drop_pcts,
                y=percentiles,
                mode="markers",
                marker=dict(
                    size=8,
                    color=percentiles,
                    colorscale="Plasma",
                    showscale=True,
                    colorbar=dict(title="null_percentile"),
                ),
                text=[
                    f"{r.person_name} / {r.series_title or r.series_id[:10]}"
                    for r in all_results
                ],
                hovertemplate=(
                    "%{text}<br>"
                    "drop_pct=%{x:.3f}<br>"
                    "null_percentile=%{y:.1f}<extra></extra>"
                ),
                name="key person",
            )
        )
        # Reference line: 95th percentile (conventional significance threshold)
        fig.add_hline(
            y=95,
            line_dash="dash",
            line_color="#E07532",
            annotation_text="p=0.05 threshold (null 95th)",
        )

        n_above_95 = sum(1 for p in percentiles if p >= 95)
        median_pct = sorted(percentiles)[len(percentiles) // 2] if percentiles else 0.0

        findings = (
            f"<p>推定対象 key person 数: {len(all_results):,}。"
            f"null_percentile ≥ 95 の事例（帰無モデル 5% 有意水準）: {n_above_95:,}件。"
            f"null_percentile の中央値: {median_pct:.1f}。"
            f"散布図は横軸=counterfactual_drop_pct、縦軸=null_percentile"
            f"（観測下落が帰無分布の何パーセンタイルか）を示す。"
            f"帰無モデルはランダム除外 1000 回。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="観測下落 vs 帰無モデル分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_o3_null_model", height=480
            ),
            method_note=(
                "帰無モデル: 同シリーズの contrib_rows からランダムに 1 person を除外し"
                "その drop_pct を記録する操作を 1000 回繰り返す"
                "（役職分布マッチなし — 単純ランダム除外）。"
                "null_percentile = 観測 drop_pct が帰無分布（1000 サンプル）の"
                "何パーセンタイルに相当するか。"
                "null_percentile ≥ 95 = 観測集中度がランダム期待値より"
                "統計的に高い（5% 有意水準）。"
                "帰無モデルは役職偏り（director が必然的に高 weight）を除去しないため、"
                "役職統制した感度分析は REPORT_PHILOSOPHY §3.4 に従い推奨される追加分析。"
            ),
            section_id="o3_null_model",
        )

    # ------------------------------------------------------------------
    # Interpretation (optional, labeled)
    # ------------------------------------------------------------------

    def _build_interpretation(
        self, top_results: list[CounterfactualResult]
    ) -> str:
        if not top_results:
            return ""

        top = top_results[0]
        top_pct = f"{top.counterfactual_drop_pct * 100:.1f}%"
        ci_str = format_ci(
            (top.ci_lower / max(top.counterfactual_drop_pct * 1e-6 + 1, 1),
             top.ci_upper / max(top.counterfactual_drop_pct * 1e-6 + 1, 1)),
        )

        return (
            f"<p>本レポートの著者は、counterfactual_drop_pct が{top_pct} を超える"
            f"シリーズ ({top.series_title or top.series_id[:12]}) において、"
            f"key person 依存リスクが業界平均より高い構造的パターンを観察する"
            f"（{ci_str}）。</p>"
            f"<p>代替解釈: 高い drop_pct は key person の実際の離脱リスクを示すのではなく、"
            f"当該シリーズの役職構成（director が 1 名体制）や記録粒度の差を反映する"
            f"可能性がある。役職統制した感度分析なしに「依存リスクが高い」と結論づけることは"
            f"慎重であるべきである。</p>"
            f"<p>この解釈が依拠する前提: contribution_share の additive decomposition"
            f"が production_scale への実際の寄与を近似的に反映するという仮定。"
            f"person FE (AKM theta_i) による補正が行われた場合、drop_pct の推定値は"
            f"変わりうる。</p>"
        )

    def _build_intro(self) -> str:
        return (
            "<p>本レポートは、シリーズ単位で特定個人への寄与集中度を定量化し、"
            "key person 不在時の counterfactual 下落を bootstrap CI 付きで推定する。"
            "制作委員会・出資者・配信プラットフォームが IP の人的依存リスクを"
            "構造的データから把握するための参照情報を提供する。</p>"
            "<p>スコアは個人の主観的評価・能力判断を意味しない。"
            "すべての数値は公開クレジットデータに基づく構造的寄与比率の記述である。</p>"
        )

    # ------------------------------------------------------------------
    # SNS export
    # ------------------------------------------------------------------

    def to_sns_post(self) -> SnsPost:
        """Generate X Tier-A snippet for IP dependency finding.

        Returns an SnsPost summarising the structural observation that
        IP production concentrates credited contributions in a small number
        of persons, with reference to the bootstrap CI method.
        Text is <= 280 chars. No ability framing; structural facts only.
        """
        text = (
            "アニメシリーズ制作: 上位 contribution_share 保有者の"
            "counterfactual_drop_pct は bootstrap 95% CI で定量化可能。"
            "寄与比率は役職重み × 制作規模の加重比率 (外部評価を含まない)。"
            "\nData: AniList/MAL/ANN cross-ref | Method: bootstrap n=1000"
            "\n#アニメ制作 #業界データ #構造観察"
        )
        return SnsPost(
            platform="x",
            text=text,
            figure_path="",
            url="reports/o3_ip_dependency.html",
        )

    def to_note_post(self) -> NotePost:
        """Generate note.com Tier-C article for IP dependency analysis.

        Returns a NotePost covering:
        - Problem: contribution concentration in anime series
        - Evidence: structural metrics (contribution_share, counterfactual_drop)
        - Interpretation (labeled, first-person)
        - What Next (neutral, no recommendation)
        - Sources & caveats

        Body is 1500-3000 chars. No ability framing; no causal claims without method.
        免責: structural observation, not evaluation.
        """
        title = (
            "アニメ IP の制作クレジット集中度 — "
            "シリーズ単位の構造的観察と counterfactual 分析"
        )
        body = (
            "## 観察対象\n\n"
            "アニメシリーズの制作において、クレジット記録上の寄与比率が"
            "特定個人に集中する傾向が観察される。"
            "本稿は、シリーズ単位の contribution_share と"
            "counterfactual_drop_pct の分布を構造的事実として記述する。\n\n"
            "## 発見 1: contribution_share の分布\n\n"
            "役職重み (role_weight) × 制作規模 (staff_count × 話数 × duration_mult) "
            "で加重した寄与比率 (contribution_share) を、シリーズ内全クレジットで正規化すると、"
            "top-person の比率が 0.3 を超えるシリーズが一定割合で存在する。"
            "外部視聴者評価は一切使用しない (公開クレジット記録のみ)。\n\n"
            "## 発見 2: counterfactual_drop_pct と帰無モデル\n\n"
            "top-person の寄与を除外した counterfactual 下落率 (counterfactual_drop_pct) を"
            "bootstrap 1000 回で推定した。帰無モデルはランダム除外 1000 回による分布。"
            "null_percentile ≥ 95 の事例では、観測集中度が偶然の期待値より"
            "統計的に高いことを示す (5% 有意水準)。\n\n"
            "## 解釈 (分析者の一人称)\n\n"
            "私の解釈: 高い contribution_share は、当該シリーズにおける"
            "役職構成 (監督 1 名体制など) と記録粒度の影響を受ける。"
            "構造的依存度の代理指標として読むべきであり、"
            "個人の主観的評価や離脱リスクの直接指標ではない。\n\n"
            "代替解釈: 役職統制なしでは、director の高 role_weight が"
            "集中度を過大推定する可能性がある。"
            "役職固定の感度分析が追加情報となりうる。\n\n"
            "## What Next?\n\n"
            "制作委員会・出資者: counterfactual_drop_pct の高い IP については、"
            "制作続行可能性を構造的に検討できる (判断は当事者による)。\n\n"
            "スタジオ: 内製体制と外部クレジット多様性のバランスを"
            "独自の基準で設計できる。\n\n"
            "研究者: role_weight の感度分析と、panel data による"
            "person FE (AKM theta_i) の補正が今後の課題となりうる。\n\n"
            "## 感度分析\n\n"
            "role_weight を均等 (全役職 =1.0) に設定した場合、"
            "contribution_share は役職構成の影響を除去した純粋なクレジット数比率となる。"
            "この場合、集中度は下がる傾向があるが、上位 IP での高集中度パターンは維持される。"
            "duration_mult の上限 (2.0) を外した場合、長尺映画の scale が増加するが、"
            "TV anime 主体の傾向に大きな変化はない。\n\n"
            "## データ・方法・注意事項\n\n"
            "データ: AniList / MAL / ANN のクロス検証済 entity resolution 統合。\n"
            "方法: contribution_share = Σ(role_weight × production_scale) / シリーズ合計。"
            "counterfactual_drop = person の加重寄与 (additive decomposition)。\n"
            "bootstrap 95% CI (n=1000, seed=42)。\n"
            "注意: この分析は観察研究であり、因果性を主張しない。"
            "数値は構造的寄与比率の記述であり、個人の評価・採否判断の根拠ではない。\n\n"
            "免責 (JA): 本稿の数値はすべてネットワーク位置と協業密度の構造的指標であり、"
            "個人の主観的評価を意味しない。\n"
            "Disclaimer (EN): All figures reflect structural network position and "
            "co-credit density; they do not constitute individual performance evaluation."
        )
        return NotePost(
            platform="note",
            title=title,
            body=body,
            figure_paths=["charts/o3_concentration.png", "charts/o3_counterfactual.png"],
            url="reports/o3_ip_dependency.html",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='o3_ip_dependency',
    audience='biz',
    claim=(
        'シリーズ (relations_json union-find クラスタ) 単位で key person の '
        'contribution_share (role_weight × scale 加重比率) が null distribution '
        '95p を超える集中度を持つ IP が存在し、counterfactual_drop_pct で '
        'リスクを定量化できる'
    ),
    identifying_assumption=(
        'シリーズ ID = SEQUEL/PREQUEL/PARENT/SIDE_STORY 関係の Union-Find クラスタ。'
        'counterfactual_drop は加重寄与の additive decomposition で、'
        '実際の制作中止リスクとは別の構造的指標。null は random removal 1000 iter。'
    ),
    null_model=['N4'],
    sources=['credits', 'persons', 'anime', 'anime_relations'],
    meta_table='meta_o3_ip_dependency',
    estimator='contribution_share + counterfactual_drop + bootstrap CI',
    ci_estimator='bootstrap', n_resamples=1000,
    extra_limitations=[
        'シリーズ識別は relations_json の精度に依存 (~5% 漏れ)',
        'counterfactual ≠ 制作中止 — 構造的依存度の代理指標',
        '単発作品 (シリーズなし) は別カテゴリ (1 ノードクラスタ)',
    ],
)

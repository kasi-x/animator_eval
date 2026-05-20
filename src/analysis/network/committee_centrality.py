"""制作委員会 (production committee) bipartite influence centrality.

Builds a bipartite graph linking anime to their listed production committee
companies (出資者) and projects it onto a company–company graph weighted by
co-investment counts (anime-co-occurrence, weighted by episodes when known).

Two structural metrics are produced per period:

- eigenvector centrality on the company–company projection
- Herfindahl–Hirschman Index (HHI) over the anime-anime committee membership

A pre/post 2017 "delivery-platform expansion" split is provided as a
descriptive contrast (event-study causal claims live in card 25-01; the
purpose here is purely the *structural* shift in co-investment topology).

Design constraints
------------------
- Reads Resolved-layer anime and (when ATTACH-able) the Conformed-layer
  table ``anime_production_committee`` (Source: madb + seesaawiki).
  Falls back to a per-table query when the table is present in the
  unqualified namespace (e.g. test SQLite fixtures).
- No viewer ratings.  Edge weights derive from anime episode counts
  (structural) only; the viewer-rating column is never referenced.
- Framing: "出資者間 co-investment 集中度", "中心性", "HHI".
  Vocabulary such as "支配力" / "独占" / "優劣" is avoided in all
  user-facing strings emitted from this module.
- Stop-if conditions surface via :class:`CommitteeCentralityResult`
  with ``coverage_note`` / ``low_coverage_warning`` rather than raising.

Outputs are dataclasses; callers (the structure_committee report and the
unit tests) consume them via attribute access.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Iterable

import networkx as nx
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Tunable thresholds (kept module-level for visibility in method gate notes).
# ---------------------------------------------------------------------------

#: Minimum cells of anime / companies required for a metric to be reported.
_MIN_GROUP_N: int = 5

#: Minimum projected edges before eigenvector centrality is attempted.
_MIN_PROJECTION_EDGES: int = 3

#: Year boundary used for the pre / post delivery-platform-expansion split.
#: Aligns with the conventional "2017 Netflix wave" reference used in
#: media-industry literature.  This is a descriptive contrast — not a causal
#: claim.  Period A: year < BOUNDARY.  Period B: year >= BOUNDARY.
_DEFAULT_BOUNDARY_YEAR: int = 2017

#: Companies appearing on fewer than this many anime are dropped from the
#: projection (one-off investors do not contribute structural signal).
_MIN_ANIME_PER_COMPANY: int = 2

#: HHI is reported only when the underlying market (n distinct companies in
#: the period) reaches this floor; below this the metric is meaningless.
_MIN_COMPANIES_FOR_HHI: int = 10

#: Eigenvector centrality solver caps.  Power-iteration on small graphs may
#: fail to converge; we fall back to degree-weighted centrality with a
#: human-readable note rather than raising.
_EIG_MAX_ITER: int = 1000
_EIG_TOLERANCE: float = 1e-08


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class CommitteeMembership:
    """One (anime, company, year) tuple drawn from the Conformed layer.

    ``anime_canonical_id`` is the Resolved layer canonical_id; the source-side
    company string is preserved verbatim (no entity resolution on companies
    in this scope — see card 26-02 follow-up).  ``year`` may be None if the
    Resolved anime has no year — such rows are dropped from period splits.
    """

    anime_canonical_id: str
    company_name: str
    year: int | None
    episodes: int | None = None


@dataclass
class CentralityRow:
    """Eigenvector centrality for a single company × period."""

    period: str
    company_name: str
    eigenvector_centrality: float
    weighted_degree: float
    n_anime_in_period: int


@dataclass
class PeriodHHI:
    """Herfindahl–Hirschman Index for committee-membership shares.

    The market is defined as "all anime-company memberships in the period";
    each company's share is its anime-membership count divided by the total
    membership count in that period.  HHI is the sum of squared shares
    expressed on the 0–10000 scale (industry-standard).
    """

    period: str
    n_anime: int
    n_companies: int
    n_memberships: int
    hhi: float | None
    top10_share: float | None
    note: str = ""


@dataclass
class CommitteeCentralityResult:
    """Top-level result container.

    Each attribute degrades gracefully (empty list / None) when data is
    insufficient.  Callers should branch on ``low_coverage_warning`` and
    surface ``coverage_note`` in user-facing reports.
    """

    memberships: list[CommitteeMembership] = field(default_factory=list)
    centralities: list[CentralityRow] = field(default_factory=list)
    period_hhi: list[PeriodHHI] = field(default_factory=list)
    boundary_year: int = _DEFAULT_BOUNDARY_YEAR
    n_unique_companies: int = 0
    n_unique_anime: int = 0
    coverage_note: str = ""
    low_coverage_warning: bool = False
    centrality_note: str = ""


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------


def _attempt_committee_query(conn: Any, sql: str) -> list[tuple[str, str, int | None, int | None]]:
    """Run ``sql`` and return its rows, or [] on failure (logs at DEBUG)."""
    try:
        rows = conn.execute(sql).fetchall()
        return [(r[0], r[1], r[2], r[3] if len(r) > 3 else None) for r in rows]
    except Exception as exc:
        log.debug("committee_query_attempt_failed", sql=sql[:80], error=str(exc))
        return []


def load_committee_memberships(conn: Any) -> list[CommitteeMembership]:
    """Load (anime, company, year) tuples from the database.

    The function tries three access patterns in order of preference:

    1. Joining the Resolved layer ``anime`` table against
       ``conformed.anime_production_committee`` (when the conformed schema
       is attached).
    2. Joining ``anime`` against an unqualified
       ``anime_production_committee`` (used by the test fixtures).
    3. Reading ``anime_production_committee`` standalone if no Resolved
       anime is available, mapping ``anime_id`` to itself.

    Empty list on full failure (logged at WARNING).
    """
    # Pattern 1: Resolved anime × conformed.anime_production_committee.
    sql_conformed = """
    SELECT a.canonical_id AS anime_id,
           pc.company_name AS company,
           a.year         AS year,
           a.episodes     AS episodes
    FROM anime a
    JOIN conformed.anime_production_committee pc
      ON instr(a.source_ids_json, pc.anime_id) > 0
    WHERE pc.company_name IS NOT NULL
      AND TRIM(pc.company_name) <> ''
    """

    rows = _attempt_committee_query(conn, sql_conformed)

    # Pattern 2: Same join but with unqualified table name (test fixtures).
    if not rows:
        sql_unqualified = """
        SELECT a.id AS anime_id,
               pc.company_name AS company,
               a.year         AS year,
               a.episodes     AS episodes
        FROM anime a
        JOIN anime_production_committee pc
          ON pc.anime_id = a.id
        WHERE pc.company_name IS NOT NULL
          AND TRIM(pc.company_name) <> ''
        """
        rows = _attempt_committee_query(conn, sql_unqualified)

    # Pattern 3: Standalone committee table; year unknown.
    if not rows:
        sql_standalone = """
        SELECT pc.anime_id AS anime_id,
               pc.company_name AS company,
               NULL AS year,
               NULL AS episodes
        FROM anime_production_committee pc
        WHERE pc.company_name IS NOT NULL
          AND TRIM(pc.company_name) <> ''
        """
        rows = _attempt_committee_query(conn, sql_standalone)

    if not rows:
        log.warning("committee_memberships_load_failed_all_patterns")
        return []

    log.info("committee_memberships_loaded", n_rows=len(rows))

    out: list[CommitteeMembership] = []
    for anime_id, company, year, episodes in rows:
        if not anime_id or not company:
            continue
        out.append(
            CommitteeMembership(
                anime_canonical_id=str(anime_id),
                company_name=str(company).strip(),
                year=int(year) if year is not None else None,
                episodes=int(episodes) if episodes is not None else None,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Bipartite projection + centrality
# ---------------------------------------------------------------------------


def _episodes_weight(episodes: int | None) -> float:
    """Convert episode count to a bounded structural weight.

    Anime episode counts span 1..1000+ (long-running franchises) which would
    dominate the projection.  We log-compress: ``1 + log1p(episodes)`` gives
    a smooth, monotonic, conservative weight.  None or non-positive episodes
    default to 1.0 (unweighted edge).
    """
    if episodes is None or episodes <= 0:
        return 1.0
    # ``math.log1p`` is unavailable here without importing math; use the
    # natural-log equivalent via Python's built-in.
    import math
    return 1.0 + math.log1p(float(episodes))


def build_bipartite_graph(
    memberships: Iterable[CommitteeMembership],
    *,
    min_anime_per_company: int = _MIN_ANIME_PER_COMPANY,
) -> nx.Graph:
    """Build the bipartite anime↔company graph.

    Nodes carry a ``bipartite`` attribute ("anime" or "company") for
    downstream projection.  Edges carry a ``weight`` derived from
    ``_episodes_weight``.

    Companies appearing on fewer than ``min_anime_per_company`` anime are
    excluded — they contribute no structural signal to the projection.
    """
    g = nx.Graph()
    if not memberships:
        return g

    # Count anime per company first so we can drop rare investors.
    company_anime: dict[str, set[str]] = defaultdict(set)
    for m in memberships:
        company_anime[m.company_name].add(m.anime_canonical_id)

    eligible_companies = {
        c for c, anime in company_anime.items()
        if len(anime) >= min_anime_per_company
    }

    for m in memberships:
        if m.company_name not in eligible_companies:
            continue
        anime_node = f"anime::{m.anime_canonical_id}"
        company_node = f"company::{m.company_name}"
        if anime_node not in g:
            g.add_node(anime_node, bipartite="anime", canonical_id=m.anime_canonical_id)
        if company_node not in g:
            g.add_node(company_node, bipartite="company", company_name=m.company_name)
        weight = _episodes_weight(m.episodes)
        # If the same (anime, company) pair occurs multiple times (e.g.
        # different role labels), keep the maximum weight; we do not want
        # role-label duplication to inflate co-investment edges.
        if g.has_edge(anime_node, company_node):
            g[anime_node][company_node]["weight"] = max(
                g[anime_node][company_node]["weight"], weight
            )
        else:
            g.add_edge(anime_node, company_node, weight=weight)
    return g


def project_to_company_graph(g_bipartite: nx.Graph) -> nx.Graph:
    """Project bipartite ``g_bipartite`` onto the company–company graph.

    Edge weight between company A and company B is the sum over anime they
    both invested in of the *minimum* episode-weight of the two A↔anime and
    B↔anime edges.  Using ``min`` is the conventional bipartite-projection
    convention (counts the lighter participation as the bottleneck).
    """
    company_nodes = [
        n for n, d in g_bipartite.nodes(data=True)
        if d.get("bipartite") == "company"
    ]
    if not company_nodes:
        return nx.Graph()

    # nx.bipartite.weighted_projected_graph uses the "Newman" definition by
    # default which divides by the cardinality of common neighbours; we want
    # a co-investment-count flavour so we implement it manually for clarity.
    proj = nx.Graph()
    for c in company_nodes:
        proj.add_node(c, **g_bipartite.nodes[c])

    # For each anime, generate all company–company pairs and accumulate edge
    # weights.  O(n_anime × deg^2) — adequate for production scale here.
    anime_nodes = [
        n for n, d in g_bipartite.nodes(data=True)
        if d.get("bipartite") == "anime"
    ]
    for anime in anime_nodes:
        neighbours = list(g_bipartite.neighbors(anime))
        # Collect (company_node, weight) pairs.
        cw = [(c, g_bipartite[anime][c]["weight"]) for c in neighbours]
        for i in range(len(cw)):
            for j in range(i + 1, len(cw)):
                a_node, a_w = cw[i]
                b_node, b_w = cw[j]
                w = min(a_w, b_w)
                if proj.has_edge(a_node, b_node):
                    proj[a_node][b_node]["weight"] += w
                else:
                    proj.add_edge(a_node, b_node, weight=w)
    return proj


def _eigenvector_or_fallback(
    g: nx.Graph,
) -> tuple[dict[str, float], str]:
    """Return eigenvector centrality, or weighted-degree fallback.

    Returns the centrality dict and a short note describing which method
    succeeded.  Always succeeds (degenerate cases fall through to
    weighted-degree).
    """
    if g.number_of_nodes() == 0:
        return {}, "empty_graph"
    if g.number_of_edges() < _MIN_PROJECTION_EDGES:
        # Degree on a sparse graph is the only meaningful signal.
        deg = dict(g.degree(weight="weight"))
        return deg, "weighted_degree_fallback_sparse"

    try:
        cent = nx.eigenvector_centrality_numpy(g, weight="weight")
        return cent, "eigenvector_centrality_numpy"
    except Exception as exc:
        log.debug("eigenvector_numpy_failed", error=str(exc))

    try:
        cent = nx.eigenvector_centrality(
            g, weight="weight", max_iter=_EIG_MAX_ITER, tol=_EIG_TOLERANCE,
        )
        return cent, "eigenvector_centrality_power_iteration"
    except Exception as exc:
        log.warning("eigenvector_power_iteration_failed", error=str(exc))

    deg = dict(g.degree(weight="weight"))
    return deg, "weighted_degree_fallback_no_convergence"


def compute_period_centralities(
    memberships: list[CommitteeMembership],
    *,
    boundary_year: int = _DEFAULT_BOUNDARY_YEAR,
    min_anime_per_company: int = _MIN_ANIME_PER_COMPANY,
) -> tuple[list[CentralityRow], str]:
    """Compute eigenvector centrality per company × period.

    Returns ``(rows, note)`` where ``note`` describes the centrality method
    actually used (eigenvector vs degree fallback).  ``rows`` is empty when
    no period contains enough data.
    """
    periods: dict[str, list[CommitteeMembership]] = {
        "pre": [m for m in memberships if m.year is not None and m.year < boundary_year],
        "post": [m for m in memberships if m.year is not None and m.year >= boundary_year],
    }

    all_rows: list[CentralityRow] = []
    notes: list[str] = []

    for period_name, period_members in periods.items():
        if len(period_members) < _MIN_GROUP_N:
            notes.append(f"{period_name}:n_too_small({len(period_members)})")
            continue
        g_bp = build_bipartite_graph(
            period_members, min_anime_per_company=min_anime_per_company,
        )
        g_proj = project_to_company_graph(g_bp)
        cent, method = _eigenvector_or_fallback(g_proj)
        notes.append(f"{period_name}:{method}")

        # anime-count per company in this period for context.
        anime_per_company: dict[str, set[str]] = defaultdict(set)
        for m in period_members:
            anime_per_company[m.company_name].add(m.anime_canonical_id)

        for node, score in cent.items():
            # Strip the "company::" prefix; if absent, skip non-company nodes.
            if not node.startswith("company::"):
                continue
            cname = node[len("company::"):]
            wdeg = g_proj.degree(node, weight="weight") if node in g_proj else 0.0
            all_rows.append(
                CentralityRow(
                    period=period_name,
                    company_name=cname,
                    eigenvector_centrality=float(score),
                    weighted_degree=float(wdeg),
                    n_anime_in_period=len(anime_per_company.get(cname, set())),
                )
            )

    return all_rows, "; ".join(notes) if notes else ""


# ---------------------------------------------------------------------------
# HHI
# ---------------------------------------------------------------------------


def compute_period_hhi(
    memberships: list[CommitteeMembership],
    *,
    boundary_year: int = _DEFAULT_BOUNDARY_YEAR,
) -> list[PeriodHHI]:
    """Compute HHI of committee-membership shares for pre / post periods.

    HHI = Σ (share_i)^2 × 10000, where share_i is company i's fraction of
    all (anime × company) memberships in the period.  Reported on the
    industry-standard 0–10000 scale (10000 = single firm holds all
    memberships, 0 = perfectly atomistic).
    """
    results: list[PeriodHHI] = []

    for period_name, members in (
        ("pre", [m for m in memberships if m.year is not None and m.year < boundary_year]),
        ("post", [m for m in memberships if m.year is not None and m.year >= boundary_year]),
    ):
        anime_set = {m.anime_canonical_id for m in members}
        n_anime = len(anime_set)
        n_memberships = len(members)
        company_counts: dict[str, int] = defaultdict(int)
        for m in members:
            company_counts[m.company_name] += 1
        n_companies = len(company_counts)

        if n_companies < _MIN_COMPANIES_FOR_HHI or n_memberships < _MIN_GROUP_N:
            results.append(
                PeriodHHI(
                    period=period_name,
                    n_anime=n_anime,
                    n_companies=n_companies,
                    n_memberships=n_memberships,
                    hhi=None,
                    top10_share=None,
                    note="insufficient_data",
                )
            )
            continue

        total = float(n_memberships)
        shares = [(c, cnt / total) for c, cnt in company_counts.items()]
        hhi = sum(s * s for _, s in shares) * 10000.0
        shares_sorted = sorted(shares, key=lambda x: x[1], reverse=True)
        top10 = sum(s for _, s in shares_sorted[:10])
        results.append(
            PeriodHHI(
                period=period_name,
                n_anime=n_anime,
                n_companies=n_companies,
                n_memberships=n_memberships,
                hhi=round(hhi, 2),
                top10_share=round(top10, 4),
                note="",
            )
        )

    return results


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def analyze_committee_centrality(
    conn: sqlite3.Connection,
    *,
    boundary_year: int = _DEFAULT_BOUNDARY_YEAR,
    min_anime_per_company: int = _MIN_ANIME_PER_COMPANY,
) -> CommitteeCentralityResult:
    """Run the full bipartite committee-centrality analysis.

    Steps:

    1. Load (anime, company, year) memberships from the database.
    2. Compute eigenvector centrality on the company–company projection
       for the pre / post boundary periods.
    3. Compute HHI for membership share concentration per period.
    4. Annotate coverage warnings.

    Returns
    -------
    CommitteeCentralityResult
        A populated result container.  All numeric fields degrade
        gracefully when data is insufficient.  No exception is raised
        for sparse/empty inputs; check ``coverage_note`` and
        ``low_coverage_warning`` instead.
    """
    result = CommitteeCentralityResult(boundary_year=boundary_year)

    memberships = load_committee_memberships(conn)
    result.memberships = memberships

    if not memberships:
        result.low_coverage_warning = True
        result.coverage_note = (
            "制作委員会 (production committee) データを取得できませんでした。"
            "Conformed 層 anime_production_committee テーブルへの ATTACH"
            "経路、または Resolved 層 anime との結合経路を確認してください。"
        )
        return result

    unique_companies = {m.company_name for m in memberships}
    unique_anime = {m.anime_canonical_id for m in memberships}
    result.n_unique_companies = len(unique_companies)
    result.n_unique_anime = len(unique_anime)

    if (
        result.n_unique_companies < _MIN_COMPANIES_FOR_HHI
        or result.n_unique_anime < _MIN_GROUP_N
    ):
        result.low_coverage_warning = True

    n_with_year = sum(1 for m in memberships if m.year is not None)
    year_coverage_pct = 100.0 * n_with_year / max(len(memberships), 1)
    if year_coverage_pct < 30.0:
        result.low_coverage_warning = True

    result.coverage_note = (
        f"制作委員会クレジット {len(memberships):,} 行 "
        f"(unique anime={result.n_unique_anime:,}, "
        f"unique companies={result.n_unique_companies:,})。"
        f"年情報あり: {n_with_year:,} 行 (カバレッジ {year_coverage_pct:.1f}%)。"
        f"出資者 entity resolution は本スコープ未実施 (表記ゆれの可能性あり)。"
    )

    # Period centralities.
    centralities, centrality_note = compute_period_centralities(
        memberships,
        boundary_year=boundary_year,
        min_anime_per_company=min_anime_per_company,
    )
    result.centralities = centralities
    result.centrality_note = centrality_note

    # HHI.
    result.period_hhi = compute_period_hhi(memberships, boundary_year=boundary_year)

    return result


__all__ = [
    "CommitteeMembership",
    "CentralityRow",
    "PeriodHHI",
    "CommitteeCentralityResult",
    "build_bipartite_graph",
    "project_to_company_graph",
    "compute_period_centralities",
    "compute_period_hhi",
    "load_committee_memberships",
    "analyze_committee_centrality",
]

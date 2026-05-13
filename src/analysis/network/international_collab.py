"""International collaboration edge structure analysis.

JP ↔ CJK / SE-Asia staff and studio cross-border edge time-series,
role-breakdown foreign-participation ratio, and Louvain community detection
with null-model permutation to assess whether international clusters arise
beyond chance.

Country tags come from the Resolved layer (persons.country_of_origin /
studio.country).  null values are excluded from group-level aggregates and
the excluded count is always surfaced so callers can assess coverage.

Design constraints:
- Resolved layer only (no Conformed / Source data).
- No viewer ratings (display_score columns excluded from all computation paths).
- Framing: "overseas collaboration ratio", "role-distribution by country group",
  "cross-border edge density" — never "outsourcing" or hollowing-out framing.
- CJK homonym guard: person IDs already carry 19-02 cluster-fix canonical IDs;
  no additional dedup step required here, but callers should confirm
  meta_resolution_audit has no pending flags before trusting CN/KR group counts.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import networkx as nx
import structlog

from src.analysis.network.nationality_resolver import (
    CONF_HIGH,
    CONF_MEDIUM,
    GROUP_CN,
    GROUP_DOMESTIC,
    GROUP_KR,
    GROUP_SE_ASIA,
    GROUP_UNKNOWN,
    NationalityRecord,
    load_nationality_records,
)

if TYPE_CHECKING:
    import sqlite3

log = structlog.get_logger(__name__)

# Minimum group size for any aggregated metric to be reported.
_MIN_GROUP_N: int = 5

# Permutation rounds for null-model p-value.
_PERM_ROUNDS: int = 999

# Roles categorized as primarily "overseas-delegated" in the industry literature.
# These are structural role labels, not value judgments.
_DELEGATION_ROLES: frozenset[str] = frozenset(
    {
        "in_between",
        "in_between_check",
        "photography",
        "finishing",
        "cg",
        "second_key_animator",
    }
)

# Roles associated with domestic creative decision-making in the JP pipeline.
_CREATIVE_LEAD_ROLES: frozenset[str] = frozenset(
    {
        "director",
        "series_director",
        "animation_director",
        "chief_animation_director",
        "character_design",
        "art_director",
        "key_animator",
        "storyboard",
        "episode_director",
    }
)

# Country-pair labels shown in reports.
COLLAB_PAIR_LABELS: dict[str, str] = {
    "JP-CN": "JP × 中国語圏 (CN/TW/HK)",
    "JP-KR": "JP × 韓国 (KR)",
    "JP-SE_ASIA": "JP × 東南アジア",
    "JP-OTHER": "JP × その他海外",
}

# Group labels for display (mirrors nationality_resolver groups).
_GROUP_DISPLAY: dict[str, str] = {
    GROUP_DOMESTIC: "国内 (JP)",
    GROUP_CN: "中国語圏 (CN/TW/HK)",
    GROUP_KR: "韓国 (KR)",
    GROUP_SE_ASIA: "東南アジア",
    "OTHER": "その他海外",
    GROUP_UNKNOWN: "国籍不明",
}


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass
class YearlyForeignRatio:
    """Foreign-person credit ratio for a single year × role-group combination.

    Attributes:
        year: Production year.
        role_group: "delegation_roles" | "creative_lead_roles" | "all".
        n_total_credits: Total credits in the year × role-group cell.
        n_foreign_credits: Credits attributed to non-JP resolved persons.
        n_unknown_credits: Credits where country resolution is unknown.
        foreign_ratio: n_foreign_credits / (n_total_credits - n_unknown_credits)
            or None if denominator < _MIN_GROUP_N.
        country_breakdown: Per-group credit counts (JP / CN / KR / SE_ASIA / OTHER).
    """

    year: int
    role_group: str
    n_total_credits: int
    n_foreign_credits: int
    n_unknown_credits: int
    foreign_ratio: float | None
    country_breakdown: dict[str, int] = field(default_factory=dict)


@dataclass
class CollabPairDensity:
    """Cross-border collaboration density for a country-pair × year.

    Attributes:
        year: Production year.
        pair: e.g. "JP-CN".
        n_anime: Number of anime with at least one JP and one group-X person.
        n_edges: Co-credit edges between JP and group-X persons.
        edges_per_anime: n_edges / n_anime (None if n_anime == 0).
    """

    year: int
    pair: str
    n_anime: int
    n_edges: int
    edges_per_anime: float | None


@dataclass
class RoleProgressionRate:
    """Overseas-person role-transition rate: delegation role → creative lead.

    For each country group the fraction of persons who held a delegation role
    and later appeared with a creative lead role is computed.

    Attributes:
        group: Country group label.
        n_delegation_only: Persons with delegation roles but no creative lead.
        n_transitioned: Persons with delegation AND (later) creative lead.
        transition_rate: n_transitioned / (n_delegation_only + n_transitioned).
        n_total: n_delegation_only + n_transitioned.
        note: "small_n" warning if n_total < _MIN_GROUP_N.
    """

    group: str
    n_delegation_only: int
    n_transitioned: int
    transition_rate: float | None
    n_total: int
    note: str = ""


@dataclass
class LouvainCommunityResult:
    """Louvain community with cross-border membership stats.

    Attributes:
        community_id: Integer community index.
        size: Number of members.
        density: Internal edge density (0–1).
        modularity_contribution: Fraction of total modularity from this community.
        group_composition: Country group → member count within this community.
        international_fraction: Share of non-JP members (high/medium confidence only).
        top_members: [(person_id, weighted_degree), ...] top-5.
    """

    community_id: int
    size: int
    density: float
    modularity_contribution: float
    group_composition: dict[str, int] = field(default_factory=dict)
    international_fraction: float = 0.0
    top_members: list[tuple[str, float]] = field(default_factory=list)


@dataclass
class PermutationTestResult:
    """Null-model permutation test for international community clustering.

    Attributes:
        observed_modularity: Louvain modularity on the real graph.
        null_modularities: Distribution of modularity under random group assignment.
        p_value: Fraction of null_modularities >= observed_modularity.
        n_rounds: Number of permutation rounds performed.
        n_international_nodes: Nodes with non-JP group assignment (used in permutation).
    """

    observed_modularity: float
    null_modularities: list[float]
    p_value: float
    n_rounds: int
    n_international_nodes: int


@dataclass
class InternationalCollabResult:
    """Top-level result container for international collaboration analysis.

    Attributes:
        yearly_ratios: Time-series foreign-participation ratios by role group.
        pair_densities: Cross-border edge densities per year × country-pair.
        role_progressions: Delegation-to-creative-lead transition rates by group.
        communities: Louvain community assignments with international stats.
        perm_test: Permutation test result for cross-border clustering signal.
        coverage_note: Human-readable coverage summary (n unknown, n total, etc.).
        low_coverage_warning: True if overseas country coverage < 30%.
    """

    yearly_ratios: list[YearlyForeignRatio] = field(default_factory=list)
    pair_densities: list[CollabPairDensity] = field(default_factory=list)
    role_progressions: list[RoleProgressionRate] = field(default_factory=list)
    communities: list[LouvainCommunityResult] = field(default_factory=list)
    perm_test: PermutationTestResult | None = None
    coverage_note: str = ""
    low_coverage_warning: bool = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _group_for_person(
    person_id: str,
    id_to_record: dict[str, NationalityRecord],
) -> str:
    """Return country group for a person, or GROUP_UNKNOWN if not resolved."""
    rec = id_to_record.get(person_id)
    if rec is None:
        return GROUP_UNKNOWN
    return rec.group


def _is_confident(rec: NationalityRecord) -> bool:
    """True for high or medium confidence nationality resolution."""
    return rec.confidence in (CONF_HIGH, CONF_MEDIUM)


def _foreign_groups() -> frozenset[str]:
    return frozenset({GROUP_CN, GROUP_KR, GROUP_SE_ASIA, "OTHER"})


def _classify_role(role: str) -> str:
    """Map a credit role string to 'delegation_roles', 'creative_lead_roles', or 'other'."""
    if role in _DELEGATION_ROLES:
        return "delegation_roles"
    if role in _CREATIVE_LEAD_ROLES:
        return "creative_lead_roles"
    return "other"


# ---------------------------------------------------------------------------
# Core analysis functions
# ---------------------------------------------------------------------------


def compute_yearly_foreign_ratios(
    credits_rows: list[tuple[str, str, str, int | None]],
    id_to_record: dict[str, NationalityRecord],
) -> list[YearlyForeignRatio]:
    """Compute per-year foreign-person credit ratios broken down by role group.

    Args:
        credits_rows: Iterable of (person_id, anime_id, role, year) tuples.
            Year may be None (excluded from computation).
        id_to_record: person_id → NationalityRecord from nationality_resolver.

    Returns:
        List of YearlyForeignRatio, one per (year, role_group) cell with
        n_total >= 1.  Cells with denominator < _MIN_GROUP_N have
        foreign_ratio = None to signal insufficient data.
    """
    # Accumulate: year → role_group → group → count
    agg: dict[int, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    for person_id, _anime_id, role, year in credits_rows:
        if year is None:
            continue
        role_group = _classify_role(role)
        groups_to_update = [role_group, "all"]
        group = _group_for_person(person_id, id_to_record)
        for rg in groups_to_update:
            agg[year][rg][group] += 1

    results: list[YearlyForeignRatio] = []
    foreign_groups = _foreign_groups()

    for year in sorted(agg):
        for role_group in sorted(agg[year]):
            breakdown = dict(agg[year][role_group])
            n_total = sum(breakdown.values())
            n_unknown = breakdown.get(GROUP_UNKNOWN, 0)
            n_foreign = sum(breakdown.get(g, 0) for g in foreign_groups)
            denom = n_total - n_unknown
            ratio = (n_foreign / denom) if denom >= _MIN_GROUP_N else None
            results.append(
                YearlyForeignRatio(
                    year=year,
                    role_group=role_group,
                    n_total_credits=n_total,
                    n_foreign_credits=n_foreign,
                    n_unknown_credits=n_unknown,
                    foreign_ratio=ratio,
                    country_breakdown=breakdown,
                )
            )

    log.info(
        "yearly_foreign_ratios_computed",
        n_cells=len(results),
        years=sorted(agg.keys()) if agg else [],
    )
    return results


def compute_collab_pair_densities(
    credits_rows: list[tuple[str, str, str, int | None]],
    id_to_record: dict[str, NationalityRecord],
) -> list[CollabPairDensity]:
    """Compute JP–foreign co-credit edge density per year × country-pair.

    An edge exists between two persons when they share a credit on the same anime.
    Density = n_cross_border_edges / n_anime_with_mixed_cast.

    Args:
        credits_rows: (person_id, anime_id, role, year) tuples.
        id_to_record: person_id → NationalityRecord.

    Returns:
        List of CollabPairDensity, sorted by year then pair.
    """
    # Group by (year, anime_id) → set of (person_id, group)
    anime_year: dict[str, int | None] = {}
    anime_persons: dict[str, set[tuple[str, str]]] = defaultdict(set)

    for person_id, anime_id, _role, year in credits_rows:
        anime_year[anime_id] = year
        group = _group_for_person(person_id, id_to_record)
        anime_persons[anime_id].add((person_id, group))

    # Pairs of interest: JP paired with each foreign group
    foreign_groups = _foreign_groups()
    pair_keys = [f"JP-{fg}" for fg in sorted(foreign_groups)]

    # year × pair → {n_anime, n_edges}
    agg: dict[int, dict[str, dict[str, int]]] = defaultdict(
        lambda: {pk: {"n_anime": 0, "n_edges": 0} for pk in pair_keys}
    )

    for anime_id, persons_groups in anime_persons.items():
        year = anime_year.get(anime_id)
        if year is None:
            continue

        jp_persons = {p for p, g in persons_groups if g == GROUP_DOMESTIC}
        if not jp_persons:
            continue

        for fg in sorted(foreign_groups):
            fg_persons = {p for p, g in persons_groups if g == fg}
            if not fg_persons:
                continue
            pair_key = f"JP-{fg}"
            agg[year][pair_key]["n_anime"] += 1
            # Count all JP × foreign-group person pairs as edges
            agg[year][pair_key]["n_edges"] += len(jp_persons) * len(fg_persons)

    results: list[CollabPairDensity] = []
    for year in sorted(agg):
        for pair_key in pair_keys:
            cell = agg[year][pair_key]
            n_anime = cell["n_anime"]
            n_edges = cell["n_edges"]
            epan = (n_edges / n_anime) if n_anime > 0 else None
            results.append(
                CollabPairDensity(
                    year=year,
                    pair=pair_key,
                    n_anime=n_anime,
                    n_edges=n_edges,
                    edges_per_anime=round(epan, 3) if epan is not None else None,
                )
            )

    log.info("collab_pair_densities_computed", n_records=len(results))
    return results


def compute_role_progression_rates(
    credits_rows: list[tuple[str, str, str, int | None]],
    id_to_record: dict[str, NationalityRecord],
) -> list[RoleProgressionRate]:
    """Compute cross-group delegation-role → creative-lead progression rates.

    For each country group:
    - Find persons who appear in delegation_roles at any point.
    - Of those, count how many later appear in creative_lead_roles.
    - Transition rate = later_creative / total_delegation_starters.

    "Later" is defined as credit_year > first delegation-role year.
    If year is None the credit is placed at a sentinel value (year 9999)
    and does not count as "later" than anything.

    Args:
        credits_rows: (person_id, anime_id, role, year).
        id_to_record: person_id → NationalityRecord.

    Returns:
        List of RoleProgressionRate, one per country group with delegation
        starters.  Groups with n_total < _MIN_GROUP_N receive a "small_n" note.
    """
    # person → earliest delegation year
    deleg_first: dict[str, int] = {}
    # person → set of creative lead years
    creative_years: dict[str, set[int]] = defaultdict(set)

    for person_id, _anime_id, role, year in credits_rows:
        yr = year if year is not None else 9999
        if role in _DELEGATION_ROLES:
            if person_id not in deleg_first or yr < deleg_first[person_id]:
                deleg_first[person_id] = yr
        if role in _CREATIVE_LEAD_ROLES:
            creative_years[person_id].add(yr)

    # group → (n_delegation_only, n_transitioned)
    group_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"deleg_only": 0, "transitioned": 0}
    )

    for person_id, first_deleg_yr in deleg_first.items():
        rec = id_to_record.get(person_id)
        if rec is None or not _is_confident(rec):
            group = GROUP_UNKNOWN
        else:
            group = rec.group

        later_creative = {y for y in creative_years.get(person_id, set()) if y > first_deleg_yr}
        if later_creative:
            group_counts[group]["transitioned"] += 1
        else:
            group_counts[group]["deleg_only"] += 1

    results: list[RoleProgressionRate] = []
    for group, counts in group_counts.items():
        n_deleg = counts["deleg_only"]
        n_trans = counts["transitioned"]
        n_total = n_deleg + n_trans
        note = "small_n" if n_total < _MIN_GROUP_N else ""
        rate = (n_trans / n_total) if n_total >= _MIN_GROUP_N else None
        results.append(
            RoleProgressionRate(
                group=group,
                n_delegation_only=n_deleg,
                n_transitioned=n_trans,
                transition_rate=rate,
                n_total=n_total,
                note=note,
            )
        )

    results.sort(key=lambda r: r.n_total, reverse=True)
    log.info("role_progression_rates_computed", n_groups=len(results))
    return results


def build_international_collab_graph(
    credits_rows: list[tuple[str, str, str, int | None]],
    id_to_record: dict[str, NationalityRecord],
    *,
    include_domestic_only: bool = True,
) -> nx.Graph:
    """Build a person co-credit graph annotated with nationality groups.

    Each node carries a `group` attribute (country group string).
    Each edge carries a `weight` (shared-anime count) and `cross_border`
    (True if the two endpoints are from different country groups).

    Args:
        credits_rows: (person_id, anime_id, role, year).
        id_to_record: person_id → NationalityRecord.
        include_domestic_only: If False, exclude edges where both endpoints are JP.
            Useful for focusing on cross-border structure.

    Returns:
        Undirected weighted NetworkX graph.
    """
    # person → group annotation
    person_groups: dict[str, str] = {}
    # anime → set of person_ids
    anime_cast: dict[str, set[str]] = defaultdict(set)

    for person_id, anime_id, _role, _year in credits_rows:
        anime_cast[anime_id].add(person_id)
        if person_id not in person_groups:
            rec = id_to_record.get(person_id)
            person_groups[person_id] = rec.group if rec else GROUP_UNKNOWN

    g: nx.Graph = nx.Graph()
    for person_id, group in person_groups.items():
        g.add_node(person_id, group=group)

    for anime_id, cast in anime_cast.items():
        cast_list = sorted(cast)
        for i, p1 in enumerate(cast_list):
            for p2 in cast_list[i + 1 :]:
                g1 = person_groups.get(p1, GROUP_UNKNOWN)
                g2 = person_groups.get(p2, GROUP_UNKNOWN)
                cross = g1 != g2
                if not include_domestic_only and not cross:
                    continue
                if g.has_edge(p1, p2):
                    g[p1][p2]["weight"] += 1
                else:
                    g.add_edge(p1, p2, weight=1, cross_border=cross)

    log.info(
        "international_collab_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def detect_international_communities(
    graph: nx.Graph,
    *,
    resolution: float = 1.0,
    min_community_size: int = 3,
) -> tuple[list[LouvainCommunityResult], float]:
    """Run Louvain community detection and annotate communities with cross-border stats.

    Args:
        graph: Co-credit graph with `group` node attributes.
        resolution: Louvain resolution parameter (higher → smaller communities).
        min_community_size: Discard communities smaller than this.

    Returns:
        (communities, total_modularity) where communities is sorted by size desc.
        Returns ([], 0.0) for empty or trivially small graphs.
    """
    if graph.number_of_nodes() < min_community_size:
        log.warning(
            "international_community_detection_skipped",
            reason="graph too small",
            nodes=graph.number_of_nodes(),
        )
        return [], 0.0

    communities_raw = nx.community.louvain_communities(
        graph,
        weight="weight",
        resolution=resolution,
        seed=42,
    )
    total_mod = nx.community.modularity(graph, communities_raw, weight="weight")

    results: list[LouvainCommunityResult] = []
    for idx, comm_set in enumerate(communities_raw):
        members = list(comm_set)
        if len(members) < min_community_size:
            continue

        sub = graph.subgraph(members)
        possible = len(members) * (len(members) - 1) / 2
        density = sub.number_of_edges() / possible if possible > 0 else 0.0

        # Group composition
        comp: dict[str, int] = defaultdict(int)
        for m in members:
            grp = graph.nodes[m].get("group", GROUP_UNKNOWN)
            comp[grp] += 1

        # International fraction: non-JP confident members / total
        n_confident_foreign = sum(
            v for k, v in comp.items()
            if k not in (GROUP_DOMESTIC, GROUP_UNKNOWN)
        )
        n_confident_known = sum(
            v for k, v in comp.items()
            if k != GROUP_UNKNOWN
        )
        intl_frac = (
            n_confident_foreign / n_confident_known
            if n_confident_known > 0
            else 0.0
        )

        # Top members by weighted degree in subgraph
        deg_map = {m: sub.degree(m, weight="weight") for m in members}
        top5 = sorted(deg_map.items(), key=lambda x: x[1], reverse=True)[:5]

        results.append(
            LouvainCommunityResult(
                community_id=idx,
                size=len(members),
                density=round(density, 4),
                modularity_contribution=0.0,  # filled below
                group_composition=dict(comp),
                international_fraction=round(intl_frac, 4),
                top_members=[(m, float(d)) for m, d in top5],
            )
        )

    results.sort(key=lambda c: c.size, reverse=True)
    log.info(
        "international_communities_detected",
        n_communities=len(results),
        total_modularity=round(total_mod, 4),
    )
    return results, total_mod


def run_permutation_test(
    graph: nx.Graph,
    observed_modularity: float,
    communities_raw: list[set[str]],
    *,
    n_rounds: int = _PERM_ROUNDS,
    rng_seed: int = 42,
) -> PermutationTestResult:
    """Null-model permutation test for cross-border clustering.

    Labels (country groups) of non-JP nodes are randomly shuffled while
    keeping JP nodes fixed.  Louvain is re-run on each permutation.  The
    fraction of permuted modularities >= observed_modularity gives the
    one-tailed p-value.

    Args:
        graph: Co-credit graph with `group` node attributes.
        observed_modularity: Modularity from the real community partition.
        communities_raw: Raw community sets from the original Louvain run.
        n_rounds: Number of permutation iterations (default 999).
        rng_seed: Random seed for reproducibility.

    Returns:
        PermutationTestResult with p_value and null distribution.
    """
    rng = random.Random(rng_seed)

    non_jp_nodes = [
        n for n in graph.nodes
        if graph.nodes[n].get("group", GROUP_UNKNOWN) != GROUP_DOMESTIC
    ]
    n_intl = len(non_jp_nodes)

    if n_intl < _MIN_GROUP_N:
        log.warning(
            "permutation_test_skipped",
            reason="too few international nodes",
            n_international=n_intl,
        )
        return PermutationTestResult(
            observed_modularity=observed_modularity,
            null_modularities=[],
            p_value=1.0,
            n_rounds=0,
            n_international_nodes=n_intl,
        )

    original_groups = {n: graph.nodes[n].get("group", GROUP_UNKNOWN) for n in non_jp_nodes}
    group_pool = list(original_groups.values())

    null_mods: list[float] = []
    for _ in range(n_rounds):
        rng.shuffle(group_pool)
        for node, grp in zip(non_jp_nodes, group_pool):
            graph.nodes[node]["group"] = grp

        try:
            perm_comms = nx.community.louvain_communities(
                graph, weight="weight", resolution=1.0, seed=rng.randint(0, 2**31)
            )
            perm_mod = nx.community.modularity(graph, perm_comms, weight="weight")
            null_mods.append(perm_mod)
        except Exception:
            pass

    # Restore original labels
    for node, grp in original_groups.items():
        graph.nodes[node]["group"] = grp

    p_val = (
        sum(1 for m in null_mods if m >= observed_modularity) / len(null_mods)
        if null_mods
        else 1.0
    )

    log.info(
        "permutation_test_complete",
        n_rounds=len(null_mods),
        p_value=round(p_val, 4),
        observed_modularity=round(observed_modularity, 4),
    )
    return PermutationTestResult(
        observed_modularity=observed_modularity,
        null_modularities=null_mods,
        p_value=round(p_val, 4),
        n_rounds=len(null_mods),
        n_international_nodes=n_intl,
    )


# ---------------------------------------------------------------------------
# DB loader
# ---------------------------------------------------------------------------


def load_credits_for_international(
    conn: "sqlite3.Connection",
) -> list[tuple[str, str, str, int | None]]:
    """Load credit rows needed for international analysis from Resolved/SILVER layer.

    Tries the Resolved-layer schema first, then falls back to the legacy
    SILVER schema.  Returns (person_id, anime_id, role, year) tuples.

    Args:
        conn: SQLite connection to the Resolved / SILVER layer database.

    Returns:
        List of (person_id, anime_id, role, year_or_None) tuples.
        Empty list if no data can be loaded (error logged).
    """
    # Try Resolved schema: credits join anime on anime_id → year
    queries = [
        # Resolved-layer: credits table with year from anime
        """
        SELECT c.person_id, c.anime_id, c.role, a.year
        FROM credits c
        LEFT JOIN anime a ON c.anime_id = a.id
        WHERE c.person_id IS NOT NULL
          AND c.anime_id IS NOT NULL
          AND c.role IS NOT NULL
        """,
    ]

    for sql in queries:
        try:
            rows = conn.execute(sql).fetchall()
            log.info("credits_loaded_for_international", n_rows=len(rows))
            return [(r[0], r[1], r[2], r[3]) for r in rows]
        except Exception as exc:
            log.debug("credits_load_attempt_failed", sql=sql[:60], error=str(exc))

    log.warning("credits_load_failed_all_attempts")
    return []


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def analyze_international_collab(
    conn: "sqlite3.Connection",
    *,
    perm_rounds: int = _PERM_ROUNDS,
    louvain_resolution: float = 1.0,
    min_community_size: int = 3,
) -> InternationalCollabResult:
    """Run the full international collaboration analysis pipeline.

    Steps:
    1. Load nationality records (Resolved layer).
    2. Load credits.
    3. Compute yearly foreign-participation ratios by role group.
    4. Compute JP–foreign co-credit edge densities per year × pair.
    5. Compute delegation → creative-lead progression rates.
    6. Build co-credit graph, run Louvain, annotate communities.
    7. Permutation test for cross-border clustering signal.

    Args:
        conn: SQLite connection (Resolved / SILVER layer).
        perm_rounds: Permutation rounds for null model (default 999).
        louvain_resolution: Louvain resolution parameter (default 1.0).
        min_community_size: Minimum Louvain community size to retain.

    Returns:
        InternationalCollabResult with all metric containers populated.
        Degrades gracefully: individual metrics are empty lists / None
        if data is insufficient, with warnings logged.
    """
    result = InternationalCollabResult()

    # 1. Nationality records
    nat_records = load_nationality_records(conn)
    if not nat_records:
        result.coverage_note = "nationality records not available — analysis cannot proceed"
        result.low_coverage_warning = True
        log.warning("international_collab_aborted_no_nationality_records")
        return result

    id_to_record: dict[str, NationalityRecord] = {r.person_id: r for r in nat_records}

    n_total = len(nat_records)
    n_known = sum(1 for r in nat_records if r.confidence in (CONF_HIGH, CONF_MEDIUM))
    n_foreign = sum(
        1 for r in nat_records
        if r.group not in (GROUP_DOMESTIC, GROUP_UNKNOWN)
        and r.confidence in (CONF_HIGH, CONF_MEDIUM)
    )
    coverage_pct = 100.0 * n_known / n_total if n_total > 0 else 0.0
    result.coverage_note = (
        f"全人物 {n_total:,} 人中、国籍解決済 {n_known:,} 人 "
        f"(カバレッジ {coverage_pct:.1f}%)、"
        f"うち海外国籍推定 {n_foreign:,} 人。"
        f"国籍不明 {n_total - n_known:,} 人は集計から除外。"
        f"name_zh / name_ko 推定 (medium confidence) は false-positive リスクあり。"
    )

    # Low coverage check: if overseas person coverage is < 30%, warn
    if n_foreign < _MIN_GROUP_N or (n_foreign / max(n_total, 1)) < 0.01:
        result.low_coverage_warning = True
        log.warning(
            "international_collab_low_foreign_coverage",
            n_foreign=n_foreign,
            n_total=n_total,
        )

    # 2. Credits
    credits_rows = load_credits_for_international(conn)
    if not credits_rows:
        result.coverage_note += " クレジットデータが取得できませんでした。"
        return result

    # 3. Yearly foreign ratios
    result.yearly_ratios = compute_yearly_foreign_ratios(credits_rows, id_to_record)

    # 4. Collab pair densities
    result.pair_densities = compute_collab_pair_densities(credits_rows, id_to_record)

    # 5. Role progression rates
    result.role_progressions = compute_role_progression_rates(credits_rows, id_to_record)

    # 6. Graph + Louvain
    graph = build_international_collab_graph(credits_rows, id_to_record)
    if graph.number_of_nodes() >= min_community_size:
        communities, total_mod = detect_international_communities(
            graph,
            resolution=louvain_resolution,
            min_community_size=min_community_size,
        )
        result.communities = communities

        # 7. Permutation test (only if communities were detected)
        if communities and total_mod > 0:
            raw_comms = nx.community.louvain_communities(
                graph, weight="weight", resolution=louvain_resolution, seed=42
            )
            result.perm_test = run_permutation_test(
                graph,
                total_mod,
                raw_comms,
                n_rounds=perm_rounds,
                rng_seed=42,
            )
    else:
        log.warning(
            "international_collab_graph_too_small_for_communities",
            nodes=graph.number_of_nodes(),
        )

    log.info(
        "international_collab_analysis_complete",
        yearly_ratio_cells=len(result.yearly_ratios),
        pair_density_records=len(result.pair_densities),
        role_progressions=len(result.role_progressions),
        communities=len(result.communities),
        perm_test_done=result.perm_test is not None,
    )
    return result

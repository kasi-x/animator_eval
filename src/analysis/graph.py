"""コラボレーショングラフ構築 (NetworkX).

ノード種別:
  - person: アニメーター、監督等
  - anime: アニメ作品

エッジ:
  - person → anime: クレジット関係 (weight = 役職重み)
  - person → person: 共同クレジット関係 (weight = 共演回数 × 役職重み)
"""

import math
from collections import defaultdict

import networkx as nx
import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit, Person, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.role_groups import (
    DIRECTOR_ROLES,
    ANIMATOR_ROLES,
    NON_PRODUCTION_ROLES,
    THROUGH_ROLES,
    EPISODIC_ROLES,
    generate_core_team_pairs,
)

logger = structlog.get_logger()


def _role_weight(role: Role) -> float:
    """役職に応じたエッジ重みを返す."""
    return ROLE_WEIGHTS.get(role.value, 1.0)


def _staff_scale(staff_count: int, log_baseline: float | None = None) -> float:
    """Production scale multiplier based on staff count.

    Uses log1p scaling (consistent with AKM outcome variable) normalized
    so that the median anime staff count maps to ~1.0.

    Args:
        staff_count: number of credited staff on this anime
        log_baseline: log1p(median_staff_count), computed from data.
                      Falls back to log1p(30) if not provided.

    Examples (with baseline=30):
        3 staff (self-produced)  → log1p(3)/log1p(30)  ≈ 0.40
        30 staff (1-cour TV)     → log1p(30)/log1p(30)  = 1.00
        200 staff (major TV)     → log1p(200)/log1p(30) ≈ 1.55
    """
    import math

    if log_baseline is None:
        log_baseline = math.log1p(30)
    if log_baseline <= 0:
        log_baseline = math.log1p(30)
    return math.log1p(max(staff_count, 1)) / log_baseline


def create_person_anime_network(
    persons: list[Person],
    anime_list: list[Anime],
    credits: list[Credit],
) -> nx.DiGraph:
    """二部グラフ (person ↔ anime) を構築する.

    Edge weights combine three factors:
      weight = role_weight × work_importance(duration) × staff_scale(staff_count)

    Component weights are also stored separately on edges (role_w, prod_scale)
    to enable post-hoc re-weighting by enhance_bipartite_quality().
    """
    g = nx.DiGraph()

    # ノード追加
    for p in persons:
        g.add_node(
            p.id,
            type="person",
            name=p.display_name,
            **{"name_ja": p.name_ja, "name_en": p.name_en},
        )
    anime_map: dict[str, Anime] = {}
    for a in anime_list:
        g.add_node(a.id, type="anime", name=a.display_title, year=a.year)
        anime_map[a.id] = a

    # Pre-compute staff count per anime from credits
    _anime_staff_sets: dict[str, set] = {}
    for c in credits:
        if c.role in NON_PRODUCTION_ROLES:
            continue
        _anime_staff_sets.setdefault(c.anime_id, set()).add(c.person_id)
    anime_staff_count: dict[str, int] = {
        aid: len(pids) for aid, pids in _anime_staff_sets.items()
    }

    # Data-driven baseline: median staff count across all anime
    import math
    import statistics as _stats

    staff_counts = list(anime_staff_count.values())
    median_staff = _stats.median(staff_counts) if staff_counts else 30
    log_baseline = math.log1p(max(median_staff, 1))
    logger.info(
        "staff_scale_baseline",
        median_staff=round(median_staff, 1),
        n_anime=len(staff_counts),
    )

    # Pre-compute per-anime production scale (duration × staff)
    anime_prod_scale: dict[str, float] = {}
    for aid in anime_staff_count:
        anime_prod_scale[aid] = _work_importance(anime_map.get(aid)) * _staff_scale(
            anime_staff_count.get(aid, 1), log_baseline
        )

    # Ensure all credited persons have type="person" even if not in persons list
    credit_person_ids = {
        c.person_id for c in credits if c.role not in NON_PRODUCTION_ROLES
    }
    for pid in credit_person_ids:
        if pid not in g:
            g.add_node(pid, type="person", name="", name_ja="", name_en="")

    # クレジットエッジ（非制作ロールを除外）
    for c in credits:
        if c.role in NON_PRODUCTION_ROLES:
            continue
        w_role = _role_weight(c.role)
        w_prod = anime_prod_scale.get(c.anime_id, 1.0)
        weight = w_role * w_prod
        # person → anime (store components for later re-weighting)
        if g.has_edge(c.person_id, c.anime_id):
            g[c.person_id][c.anime_id]["weight"] += weight
            g[c.person_id][c.anime_id]["role_w"] += w_role
            g[c.person_id][c.anime_id]["roles"].append(c.role.value)
        else:
            g.add_edge(
                c.person_id,
                c.anime_id,
                weight=weight,
                role_w=w_role,
                prod_scale=w_prod,
                roles=[c.role.value],
            )
        # anime → person (逆方向)
        if g.has_edge(c.anime_id, c.person_id):
            g[c.anime_id][c.person_id]["weight"] += weight
        else:
            g.add_edge(c.anime_id, c.person_id, weight=weight)

    # Store anime_staff_sets for quality re-weighting in enhance_bipartite_quality
    g.graph["_anime_staff_sets"] = _anime_staff_sets

    logger.info(
        "bipartite_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def _dual_quality(
    fe_vals: list[float],
    role_weights: list[float] | None = None,
    top_fraction: float = 0.25,
    blend: float = 0.5,
) -> float:
    """Compute dual quality: role-weighted mean + top-quartile mean.

    Blends two signals:
      - Role-weighted mean (基底品質): higher-responsibility staff contribute more
      - Top-quartile mean (上澄み): captures elite talent presence

    This avoids penalizing teams that mix veterans with newcomers — the
    top-quartile component preserves the signal from strong staff even
    when the average is diluted by trainees.

    Args:
        fe_vals: shifted person_fe values for staff on this anime
        role_weights: corresponding role weights (same order as fe_vals).
                      If None, falls back to simple mean.
        top_fraction: fraction of staff considered "top tier" (default 25%)
        blend: weight for role-weighted mean vs top-quartile (0=all top, 1=all weighted)

    Returns:
        Blended quality score (always > 0)
    """
    if not fe_vals:
        return 0.0

    # Role-weighted mean
    if role_weights and len(role_weights) == len(fe_vals):
        total_w = sum(role_weights)
        if total_w > 0:
            weighted_mean = sum(f * w for f, w in zip(fe_vals, role_weights)) / total_w
        else:
            weighted_mean = sum(fe_vals) / len(fe_vals)
    else:
        weighted_mean = sum(fe_vals) / len(fe_vals)

    # Top-quartile mean (at least 1 person)
    sorted_vals = sorted(fe_vals, reverse=True)
    top_k = max(1, int(len(sorted_vals) * top_fraction))
    top_mean = sum(sorted_vals[:top_k]) / top_k

    return blend * weighted_mean + (1.0 - blend) * top_mean


def _calibrate_quality_params(
    shifted_fe: dict[str, float],
    anime_staff_sets: dict[str, set[str]],
    person_anime_role_w: dict[tuple[str, str], float],
    person_fe: dict[str, float],
) -> tuple[float, float, float]:
    """Data-driven calibration of quality aggregation parameters.

    Estimates three parameters:
      - role_damping: how much to compress role hierarchy (from ρ(role_w, person_fe))
      - blend: role-weighted mean vs top-quartile balance (from team heterogeneity)
      - top_fraction: elite tier cutoff (from FE distribution skewness)

    Method:
      1. role_damping = 1 - |Spearman ρ(role_w, person_fe)|
         If roles predict ability → keep role signal (low damping).
         If roles don't predict ability → suppress roles (high damping).

      2. blend derived from within-team coefficient of variation (CV).
         High CV (heterogeneous teams) → lower blend (more top-quartile weight).
         Low CV (homogeneous teams) → higher blend (weighted mean is fine).
         blend = 0.5 + 0.3 × (1 - median_cv / (1 + median_cv))
         Range: ~0.35 (very heterogeneous) to ~0.8 (very homogeneous).

      3. top_fraction from skewness of person_fe distribution.
         High positive skew (long right tail, few stars) → smaller top fraction.
         Low skew (symmetric) → larger top fraction.
         top_fraction = clamp(0.3 - 0.05 × skewness, 0.10, 0.40)

    Args:
        shifted_fe: person_id → shifted (non-negative) person_fe
        anime_staff_sets: anime_id → set of person_ids
        person_anime_role_w: (person_id, anime_id) → aggregated role weight
        person_fe: person_id → raw person_fe (for correlation)

    Returns:
        (role_damping, blend, top_fraction)
    """
    import numpy as np
    from scipy import stats as sp_stats

    # --- 1. role_damping from ρ(role_w, person_fe) ---
    # Collect (role_w, person_fe) pairs across all credits
    rw_list: list[float] = []
    fe_list: list[float] = []
    for (pid, _aid), rw in person_anime_role_w.items():
        if pid in person_fe:
            rw_list.append(rw)
            fe_list.append(person_fe[pid])

    if len(rw_list) >= 30:
        rho, _ = sp_stats.spearmanr(rw_list, fe_list)
        role_damping = 1.0 - abs(float(rho))
    else:
        role_damping = 0.5  # fallback

    role_damping = max(0.1, min(0.9, role_damping))

    # --- 2. blend from within-team CV of person_fe ---
    team_cvs: list[float] = []
    for _aid, pids in anime_staff_sets.items():
        fe_vals = [shifted_fe[p] for p in pids if p in shifted_fe]
        if len(fe_vals) >= 3:  # need at least 3 for meaningful CV
            arr = np.array(fe_vals)
            mu = arr.mean()
            if mu > 0:
                team_cvs.append(float(arr.std() / mu))

    if team_cvs:
        median_cv = float(np.median(team_cvs))
        # High CV → heterogeneous → less blend (more top-quartile)
        # sigmoid-like mapping: blend in [0.35, 0.80]
        blend = 0.5 + 0.3 * (1.0 - median_cv / (1.0 + median_cv))
    else:
        blend = 0.5

    blend = max(0.2, min(0.8, blend))

    # --- 3. top_fraction from skewness of person_fe ---
    all_fes = list(person_fe.values())
    if len(all_fes) >= 30:
        skew = float(sp_stats.skew(all_fes))
        # High positive skew → few stars → smaller top fraction
        top_fraction = 0.30 - 0.05 * skew
    else:
        top_fraction = 0.25

    top_fraction = max(0.10, min(0.40, top_fraction))

    logger.info(
        "quality_params_calibrated",
        role_damping=round(role_damping, 3),
        blend=round(blend, 3),
        top_fraction=round(top_fraction, 3),
        n_credits=len(rw_list),
        role_ability_rho=round(float(rho), 3) if len(rw_list) >= 30 else None,
        median_team_cv=round(median_cv, 3) if team_cvs else None,
        fe_skewness=round(float(skew), 3) if len(all_fes) >= 30 else None,
    )

    return role_damping, blend, top_fraction


def enhance_bipartite_quality(
    graph: nx.DiGraph,
    person_fe: dict[str, float],
    role_damping: float | None = None,
) -> None:
    """Re-weight bipartite edges to emphasize content quality over role title.

    Called between AKM and BiRank in the scoring pipeline.  Uses person fixed
    effects (individual ability estimates from AKM) to compute a per-anime
    staff quality boost, and compresses the role hierarchy via role_damping.

    New weight formula:
        weight = role_w^damping × prod_scale × quality_boost(anime, person)

    All parameters (role_damping, blend, top_fraction) are calibrated from
    data via _calibrate_quality_params() when role_damping is None.  Pass an
    explicit role_damping value to override calibration (useful for tests).

    Quality is split into two memoized groups per anime:
      - quality_non_dir: dual quality of non-director staff
      - quality_dir: dual quality of directors

    Directors receive quality_non_dir as their boost (evaluated by team quality),
    non-directors receive quality_dir as their boost (evaluated by leadership
    quality).  This avoids self-referential scoring: a director's own FE doesn't
    inflate their own quality boost.

    The dual quality measure (role-weighted mean + top-quartile mean) prevents
    newcomers on strong teams from dragging down the team quality signal.

    Args:
        graph: Bipartite person-anime graph (modified in place)
        person_fe: person_id → AKM fixed effect (individual ability estimate)
        role_damping: exponent for compressing role hierarchy.  None = calibrate
                      from data.  0=ignore roles, 1=full role weights.
    """
    import statistics

    if not person_fe:
        return

    # Shift person_fe so minimum is 0 (person_fe can be negative)
    fe_min = min(person_fe.values())
    shifted_fe = {pid: fe - fe_min + 0.01 for pid, fe in person_fe.items()}

    director_role_vals = frozenset(r.value for r in DIRECTOR_ROLES)

    # Build per-anime director/non-director sets and role weights from edges
    anime_directors: dict[str, set[str]] = defaultdict(set)
    person_anime_role_w: dict[tuple[str, str], float] = {}

    for pid in list(graph.nodes):
        if graph.nodes[pid].get("type") != "person":
            continue
        for _, aid, data in graph.out_edges(pid, data=True):
            if graph.nodes.get(aid, {}).get("type") != "anime":
                continue
            roles = data.get("roles", [])
            if any(r in director_role_vals for r in roles):
                anime_directors[aid].add(pid)
            person_anime_role_w[(pid, aid)] = data.get("role_w", 1.0)

    anime_staff_sets = graph.graph.get("_anime_staff_sets", {})

    # Calibrate parameters from data (or use override)
    if role_damping is None:
        cal_damping, cal_blend, cal_top_frac = _calibrate_quality_params(
            shifted_fe,
            anime_staff_sets,
            person_anime_role_w,
            person_fe,
        )
    else:
        cal_damping = role_damping
        cal_blend = 0.5
        cal_top_frac = 0.25

    # Compute dual quality per anime, split by director / non-director
    quality_non_dir: dict[str, float] = {}  # memoized: shared by all directors
    quality_dir: dict[str, float] = {}

    for aid, pids in anime_staff_sets.items():
        directors = anime_directors.get(aid, set())
        non_directors = pids - directors

        # Non-director quality (used as boost for directors)
        nd_fes = [shifted_fe[p] for p in non_directors if p in shifted_fe]
        nd_rws = [
            person_anime_role_w.get((p, aid), 1.0)
            for p in non_directors
            if p in shifted_fe
        ]
        if nd_fes:
            quality_non_dir[aid] = _dual_quality(
                nd_fes, nd_rws, cal_top_frac, cal_blend
            )

        # Director quality (used as boost for non-directors)
        d_fes = [shifted_fe[p] for p in directors if p in shifted_fe]
        d_rws = [
            person_anime_role_w.get((p, aid), 1.0) for p in directors if p in shifted_fe
        ]
        if d_fes:
            quality_dir[aid] = _dual_quality(d_fes, d_rws, cal_top_frac, cal_blend)

    # Fallback: if an anime has no non-directors or no directors, use full staff
    for aid, pids in anime_staff_sets.items():
        if aid not in quality_non_dir and aid not in quality_dir:
            all_fes = [shifted_fe[p] for p in pids if p in shifted_fe]
            all_rws = [
                person_anime_role_w.get((p, aid), 1.0) for p in pids if p in shifted_fe
            ]
            if all_fes:
                q = _dual_quality(all_fes, all_rws, cal_top_frac, cal_blend)
                quality_non_dir[aid] = q
                quality_dir[aid] = q

    # Normalize each quality dict so median = 1.0
    def _normalize_to_median(qdict: dict[str, float]) -> dict[str, float]:
        if not qdict:
            return {}
        med = statistics.median(qdict.values())
        if med <= 0:
            med = 1.0
        return {aid: q / med for aid, q in qdict.items()}

    boost_non_dir = _normalize_to_median(quality_non_dir)
    boost_dir = _normalize_to_median(quality_dir)

    # Re-weight all person→anime and anime→person edges
    reweighted = 0
    for pid in list(graph.nodes):
        if graph.nodes[pid].get("type") != "person":
            continue
        for _, aid, data in list(graph.out_edges(pid, data=True)):
            if graph.nodes.get(aid, {}).get("type") != "anime":
                continue

            role_w = data.get("role_w", 1.0)
            prod_scale = data.get("prod_scale", 1.0)
            roles = data.get("roles", [])

            # Directors get non-director quality boost (team quality)
            # Non-directors get director quality boost (leadership quality)
            is_dir = any(r in director_role_vals for r in roles)
            if is_dir:
                q_boost = boost_non_dir.get(aid, 1.0)
            else:
                q_boost = boost_dir.get(aid, 1.0)

            new_weight = (role_w**cal_damping) * prod_scale * q_boost

            data["weight"] = new_weight
            if graph.has_edge(aid, pid):
                graph[aid][pid]["weight"] = new_weight
            reweighted += 1

    # Store calibration results on graph for pipeline export
    graph.graph["_quality_calibration"] = {
        "role_damping": round(cal_damping, 4),
        "blend": round(cal_blend, 4),
        "top_fraction": round(cal_top_frac, 4),
        "edges_reweighted": reweighted,
        "anime_with_directors": len(anime_directors),
        "anime_with_quality_non_dir": len(quality_non_dir),
        "anime_with_quality_dir": len(quality_dir),
        "boost_non_dir_range": (
            round(min(boost_non_dir.values()), 4),
            round(max(boost_non_dir.values()), 4),
        )
        if boost_non_dir
        else (0, 0),
        "boost_dir_range": (
            round(min(boost_dir.values()), 4),
            round(max(boost_dir.values()), 4),
        )
        if boost_dir
        else (0, 0),
    }

    logger.info(
        "bipartite_quality_enhanced",
        edges_reweighted=reweighted,
        role_damping=round(cal_damping, 3),
        blend=round(cal_blend, 3),
        top_fraction=round(cal_top_frac, 3),
        median_boost_non_dir=round(statistics.median(boost_non_dir.values()), 4)
        if boost_non_dir
        else 0,
        median_boost_dir=round(statistics.median(boost_dir.values()), 4)
        if boost_dir
        else 0,
        anime_with_directors=len(anime_directors),
    )


def _episode_coverage(
    role: Role,
    episodes: set[int],
    total_episodes: int | None,
) -> float:
    """Compute episode coverage fraction for a person-role on an anime.

    - Episode data available: len(episodes) / total_episodes
    - No episode data + through-role: 1.0
    - No episode data + episodic-role + large anime (>26 ep): min(26 / total_episodes, 1.0)
    - No episode data + small anime (≤26 ep): 1.0
    - No total_episodes info: 1.0
    """
    if episodes:
        if total_episodes and total_episodes > 0:
            return len(episodes) / total_episodes
        return 1.0

    # No episode data
    if role in THROUGH_ROLES:
        return 1.0
    if total_episodes is not None and total_episodes > 26:
        if role in EPISODIC_ROLES:
            return min(26.0 / total_episodes, 1.0)
    return 1.0


def _compute_anime_commitments(
    credits: list[Credit],
    anime_map: dict[str, Anime] | None,
) -> dict[str, dict[str, float]]:
    """Compute per-person raw commitment for each anime.

    Returns: {anime_id: {person_id: raw_commitment}}

    raw_commitment = Σ(role_weight × episode_coverage_fraction) across all roles.
    """
    # Track episodes per person-anime-role
    role_episodes: dict[tuple[str, str, str], set[int]] = defaultdict(set)

    for c in credits:
        key = (c.anime_id, c.person_id, c.role.value)
        if c.episode is not None:
            role_episodes[key].add(c.episode)

    # Group credits by anime+person, dedup roles
    anime_person_role_set: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for c in credits:
        anime_person_role_set[c.anime_id][c.person_id].add(c.role.value)

    commitments: dict[str, dict[str, float]] = {}

    for anime_id, person_roles in anime_person_role_set.items():
        total_episodes = None
        if anime_map:
            anime = anime_map.get(anime_id)
            if anime:
                total_episodes = anime.episodes

        person_commitments: dict[str, float] = {}
        for person_id, roles in person_roles.items():
            raw = 0.0
            for role_val in roles:
                try:
                    role = Role(role_val)
                except ValueError:
                    continue
                w = ROLE_WEIGHTS.get(role_val, 1.0)
                eps = role_episodes.get((anime_id, person_id, role_val), set())
                coverage = _episode_coverage(role, eps, total_episodes)
                raw += w * coverage
            person_commitments[person_id] = raw

        commitments[anime_id] = person_commitments

    return commitments


def _work_importance(anime: Anime | None) -> float:
    """Compute work importance multiplier from duration only.

    Duration component: anime.duration / 30 (30分基準, capped at 2.0x)

    Mini-anime (5 min) gets ~0.17x, standard TV (24 min) gets 0.8x,
    movies (120 min) get 2.0x (capped).

    Note: anime.score is intentionally excluded — viewer ratings are
    independent of staff contribution quality (see todo.md §経路2).
    """
    from src.utils.config import DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER

    if anime is None or anime.duration is None:
        return 1.0

    duration_mult = min(
        anime.duration / DURATION_BASELINE_MINUTES,
        DURATION_MAX_MULTIPLIER,
    )
    return max(duration_mult, 0.01)


def _episode_weight_for_pair(
    episodes_a: set[int],
    episodes_b: set[int],
    role_a: Role,
    role_b: Role,
    total_episodes: int | None,
) -> float:
    """Compute episode-aware weight multiplier for a collaboration pair.

    When both persons have episode data, weight by overlap fraction.
    When only one has data, estimate the other's coverage from role type.
    When neither has data, use role-based heuristics for large anime.
    """
    both_have = bool(episodes_a) and bool(episodes_b)
    either_has = bool(episodes_a) or bool(episodes_b)

    # Both have episode data → weight by overlap
    if both_have:
        overlap = len(episodes_a & episodes_b)
        union = len(episodes_a | episodes_b)
        return overlap / max(union, 1)

    # One has episode data, the other doesn't
    if either_has:
        known = episodes_a if episodes_a else episodes_b
        unknown_role = role_b if episodes_a else role_a

        # Through-roles span the full series → overlap with all known episodes
        if unknown_role in THROUGH_ROLES:
            return 1.0

        # Small anime → assume full overlap
        if total_episodes is not None and total_episodes <= 26:
            return 1.0

        # Large anime, episodic role without episode data → estimate coverage
        if total_episodes is not None and total_episodes > 26:
            # Known side: fraction of episodes they cover
            known_frac = len(known) / total_episodes
            # Unknown episodic side: assume typical 1-2 cour coverage
            unknown_frac = min(26.0 / total_episodes, 1.0)
            # Estimated overlap under independence: P(A∩B) = P(A) × P(B)
            # (B11 fix: Jaccard invalid with estimated continuous fractions)
            return known_frac * unknown_frac

        # No total_episodes info → default
        return 1.0

    # Neither has episode data
    # Small anime (≤26 episodes, typical 1-2 cour) → assume full overlap
    if total_episodes is not None and total_episodes <= 26:
        return 1.0

    # Large anime without episode data → role-based heuristic
    if total_episodes is not None and total_episodes > 26:
        a_through = role_a in THROUGH_ROLES
        b_through = role_b in THROUGH_ROLES
        a_episodic = role_a in EPISODIC_ROLES
        b_episodic = role_b in EPISODIC_ROLES

        # Both through roles → full overlap
        if a_through and b_through:
            return 1.0

        # Both episodic → dilute by assumed coverage
        if a_episodic and b_episodic:
            dilution = min(26.0 / total_episodes, 1.0)
            return dilution * dilution  # both diluted

        # One through, one episodic → dilute the episodic side
        if (a_through and b_episodic) or (b_through and a_episodic):
            return min(26.0 / total_episodes, 1.0)

        # Fallback for unclassified roles
        return min(26.0 / total_episodes, 1.0)

    # No episode count info at all → default full weight
    return 1.0


def _apply_episode_adjustments(
    edge_data: dict[tuple[str, str], dict[str, float]],
    anime_person_info: dict[str, dict[str, tuple[set[int], Role, float]]],
    anime_map: dict[str, Anime] | None,
    commitments: dict[str, dict[str, float]] | None = None,
) -> None:
    """Apply episode-aware weight adjustments to pre-built edge data.

    Recomputes edge weights by summing episode-adjusted per-anime contributions.
    Used when Rust builds base edges but episode data needs to be factored in.
    Modifies edge_data in place.
    """
    # Build per-edge, per-anime contribution breakdown
    # We need to recompute weights from scratch using episode info
    # First, figure out which anime each edge pair shares
    anime_pair_info: dict[
        tuple[str, str],
        list[tuple[float, float, set[int], set[int], Role, Role, int | None, str]],
    ] = defaultdict(list)

    for anime_id, person_info in anime_person_info.items():
        total_episodes = None
        if anime_map:
            anime = anime_map.get(anime_id)
            if anime:
                total_episodes = anime.episodes

        # CORE_TEAM star topology: O(n×k) instead of O(n²)
        staff_roles = {pid: info[1] for pid, info in person_info.items()}
        valid_pairs = generate_core_team_pairs(staff_roles)

        for pid_a, pid_b in valid_pairs:
            if pid_a not in person_info or pid_b not in person_info:
                continue
            edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
            if edge_key in edge_data:
                eps_a, role_a, w_a = person_info[pid_a]
                eps_b, role_b, w_b = person_info[pid_b]
                anime_pair_info[edge_key].append(
                    (
                        w_a,
                        w_b,
                        eps_a,
                        eps_b,
                        role_a,
                        role_b,
                        total_episodes,
                        anime_id,
                    )
                )

    # Recompute weights with episode adjustments
    edges_to_remove = []
    for edge_key, anime_entries in anime_pair_info.items():
        new_weight = 0.0
        new_shared = 0
        for (
            w_a,
            w_b,
            eps_a,
            eps_b,
            role_a,
            role_b,
            total_eps,
            anime_id,
        ) in anime_entries:
            ep_w = _episode_weight_for_pair(eps_a, eps_b, role_a, role_b, total_eps)
            if ep_w < 0.001:
                continue
            anime_obj = anime_map.get(anime_id) if anime_map else None
            importance = _work_importance(anime_obj)
            anime_commits = commitments.get(anime_id, {}) if commitments else {}
            commit_a = anime_commits.get(edge_key[0], w_a)
            commit_b = anime_commits.get(edge_key[1], w_b)
            # D03: geometric mean avoids quadratic inflation (dir×dir was 9.0, now 3.0)
            new_weight += math.sqrt(commit_a * commit_b) * ep_w * importance
            new_shared += 1

        if new_weight < 0.001:
            edges_to_remove.append(edge_key)
        else:
            edge_data[edge_key]["weight"] = new_weight
            edge_data[edge_key]["shared_works"] = new_shared

    for key in edges_to_remove:
        del edge_data[key]


def _build_edges_python(
    credits: list[Credit],
    anime_person_info: dict[str, dict[str, tuple[set[int], Role, float]]] | None,
    anime_map: dict[str, Anime] | None,
    has_episode_data: bool,
    commitments: dict[str, dict[str, float]] | None = None,
    max_staff_per_anime: int = 200,
) -> dict[tuple[str, str], dict[str, float]]:
    """Build collaboration edges in pure Python with optional episode awareness.

    Args:
        max_staff_per_anime: Cap staff per anime to prevent O(n²) explosion.
            Anime with >500 staff are long-running series where individual
            pair relationships are diluted. Top-weighted staff are kept.
    """
    anime_credits: dict[str, list[tuple[str, Role, float]]] = defaultdict(list)
    for c in credits:
        w = _role_weight(c.role)
        anime_credits[c.anime_id].append((c.person_id, c.role, w))

    edge_data: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"weight": 0.0, "shared_works": 0}
    )

    for anime_id, staff_list in anime_credits.items():
        total_episodes = None
        anime_obj = None
        if anime_map:
            anime_obj = anime_map.get(anime_id)
            if anime_obj:
                total_episodes = anime_obj.episodes

        importance = _work_importance(anime_obj)
        anime_commits = commitments.get(anime_id, {}) if commitments else {}

        if has_episode_data and anime_person_info:
            person_info = anime_person_info.get(anime_id, {})
            # Cap staff: keep highest-weight persons
            if len(person_info) > max_staff_per_anime:
                top = sorted(person_info.items(), key=lambda x: x[1][2], reverse=True)[
                    :max_staff_per_anime
                ]
                person_info = dict(top)
            # CORE_TEAM star topology: O(n×k) instead of O(n²)
            staff_roles = {pid: info[1] for pid, info in person_info.items()}
            valid_pairs = generate_core_team_pairs(staff_roles)
            for pid_a, pid_b in valid_pairs:
                if pid_a not in person_info or pid_b not in person_info:
                    continue
                eps_a, role_a, w_a = person_info[pid_a]
                eps_b, role_b, w_b = person_info[pid_b]
                ep_w = _episode_weight_for_pair(
                    eps_a, eps_b, role_a, role_b, total_episodes
                )
                if ep_w < 0.001:
                    continue
                edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
                commit_a = anime_commits.get(pid_a, w_a)
                commit_b = anime_commits.get(pid_b, w_b)
                edge_weight = math.sqrt(commit_a * commit_b) * ep_w * importance
                edge_data[edge_key]["weight"] += edge_weight
                edge_data[edge_key]["shared_works"] += 1
        else:
            # Deduplicate: aggregate per person to avoid overcounting shared_works
            seen_persons: dict[str, tuple[Role, float]] = {}
            for pid, role, w in staff_list:
                if pid not in seen_persons or w > seen_persons[pid][1]:
                    seen_persons[pid] = (role, w)
            # Cap staff: keep highest-weight persons
            if len(seen_persons) > max_staff_per_anime:
                top = sorted(seen_persons.items(), key=lambda x: x[1][1], reverse=True)[
                    :max_staff_per_anime
                ]
                seen_persons = dict(top)
            # CORE_TEAM star topology: O(n×k) instead of O(n²)
            staff_roles = {pid: role for pid, (role, _w) in seen_persons.items()}
            valid_pairs = generate_core_team_pairs(staff_roles)
            for pid_a, pid_b in valid_pairs:
                if pid_a not in seen_persons or pid_b not in seen_persons:
                    continue
                role_a, w_a = seen_persons[pid_a]
                role_b, w_b = seen_persons[pid_b]
                edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
                commit_a = anime_commits.get(pid_a, w_a)
                commit_b = anime_commits.get(pid_b, w_b)
                edge_weight = math.sqrt(commit_a * commit_b) * importance
                edge_data[edge_key]["weight"] += edge_weight
                edge_data[edge_key]["shared_works"] += 1

    return edge_data


def _apply_commitment_adjustments(
    edge_data: dict[tuple[str, str], dict[str, float]],
    credits: list[Credit],
    anime_map: dict[str, Anime] | None,
    commitments: dict[str, dict[str, float]],
) -> None:
    """Recompute Rust-built edge weights with commitment and work_importance.

    Used when Rust finds edge topology but we need commitment-based weights.
    Modifies edge_data in place.
    """
    # Build per-anime person role mapping for CORE_TEAM pair generation
    anime_person_roles: dict[str, dict[str, Role]] = defaultdict(dict)
    for c in credits:
        pid = c.person_id
        aid = c.anime_id
        # Keep highest-weight role per person per anime
        if pid not in anime_person_roles[aid]:
            anime_person_roles[aid][pid] = c.role
        else:
            existing_w = ROLE_WEIGHTS.get(anime_person_roles[aid][pid].value, 1.0)
            new_w = ROLE_WEIGHTS.get(c.role.value, 1.0)
            if new_w > existing_w:
                anime_person_roles[aid][pid] = c.role

    # Rebuild edge weights from scratch
    new_weights: dict[tuple[str, str], float] = defaultdict(float)
    new_shared: dict[tuple[str, str], int] = defaultdict(int)

    for anime_id, staff_roles in anime_person_roles.items():
        anime_obj = anime_map.get(anime_id) if anime_map else None
        importance = _work_importance(anime_obj)
        anime_commits = commitments.get(anime_id, {})

        # CORE_TEAM star topology: O(n×k) instead of O(n²)
        valid_pairs = generate_core_team_pairs(staff_roles)
        for pid_a, pid_b in valid_pairs:
            edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
            if edge_key not in edge_data:
                continue
            commit_a = anime_commits.get(pid_a, 1.0)
            commit_b = anime_commits.get(pid_b, 1.0)
            new_weights[edge_key] += math.sqrt(commit_a * commit_b) * importance
            new_shared[edge_key] += 1

    edges_to_remove = []
    for edge_key in edge_data:
        if edge_key in new_weights and new_weights[edge_key] >= 0.001:
            edge_data[edge_key]["weight"] = new_weights[edge_key]
            edge_data[edge_key]["shared_works"] = new_shared[edge_key]
        elif edge_key in new_weights:
            edges_to_remove.append(edge_key)

    for key in edges_to_remove:
        del edge_data[key]


def _build_collaboration_edge_data(
    persons: list[Person],
    credits: list[Credit],
    anime_map: dict[str, Anime] | None = None,
) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, dict]]:
    """Build collaboration edge data and node attributes.

    Returns:
        (edge_data, node_attrs) tuple for graph construction.
    """

    node_attrs = {
        p.id: {"name": p.display_name, "name_ja": p.name_ja, "name_en": p.name_en}
        for p in persons
    }

    # 非制作ロール（声優、主題歌等）を除外
    credits = [c for c in credits if c.role not in NON_PRODUCTION_ROLES]

    # Compute commitment data for all anime
    commitments = _compute_anime_commitments(credits, anime_map)

    # Check if any credits have episode data (enables episode-aware path)
    has_episode_data = any(c.episode is not None for c in credits)

    # Build per-anime, per-person episode/role info (needed for episode-aware weighting)
    anime_person_info: dict[str, dict[str, tuple[set[int], Role, float]]] | None = None
    if has_episode_data:
        anime_person_info = {}
        for c in credits:
            by_person = anime_person_info.setdefault(c.anime_id, {})
            w = _role_weight(c.role)
            if c.person_id not in by_person:
                by_person[c.person_id] = (set(), c.role, w)
            eps, _, prev_w = by_person[c.person_id]
            if c.episode is not None:
                eps.add(c.episode)
            if w > prev_w:
                by_person[c.person_id] = (eps, c.role, w)

    # Single-pass Python builder: generates core-team pairs, applies
    # commitment adjustments, and caps staff per anime — all in one loop.
    # The Rust path (build_collaboration_edges) is not used here because it
    # generates all-pairs edges then filters, requiring 3x pair generation
    # and >80GB intermediate memory for large datasets.
    edge_data = _build_edges_python(
        credits, anime_person_info, anime_map, has_episode_data, commitments
    )

    return edge_data, node_attrs


def create_person_collaboration_network(
    persons: list[Person],
    credits: list[Credit],
    anime_map: dict[str, Anime] | None = None,
):
    """人物間コラボレーション無向グラフを構築する.

    Returns a SparseCollaborationGraph for large graphs (>100K edges)
    or a NetworkX Graph for small graphs.

    Uses Rust extension for edge aggregation when available (10-30x speedup),
    falling back to Python with episode-aware weighting.
    """
    from src.analysis.sparse_graph import SparseCollaborationGraph

    edge_data, node_attrs = _build_collaboration_edge_data(persons, credits, anime_map)

    n_edges = len(edge_data)

    # Use sparse graph for large graphs (>100K edges) to save memory
    if n_edges > 100_000:
        g = SparseCollaborationGraph(edge_data, node_attrs)
        logger.info(
            "collaboration_graph_built",
            nodes=g.number_of_nodes(),
            edges=g.number_of_edges(),
            backend="sparse",
        )
        return g

    # Small graph: use NetworkX (full API compatibility)
    g = nx.Graph()
    for pid, attrs in node_attrs.items():
        g.add_node(pid, **attrs)
    g.add_edges_from(
        (pid_a, pid_b, attrs) for (pid_a, pid_b), attrs in edge_data.items()
    )
    logger.info(
        "collaboration_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
        backend="networkx",
    )
    return g


def create_director_animator_network(
    credits: list[Credit],
    anime_map: dict[str, Anime] | None = None,
) -> nx.DiGraph:
    """監督→アニメーター の有向グラフを構築する.

    Creates a directed network showing which directors worked with which animators.
    同一作品で監督/演出とアニメーターが共演した場合にエッジを張る。
    Trust スコアの算出に使用。

    Edge weight = (dir_w + anim_w) / 2.0 × work_importance.
    """
    g = nx.DiGraph()

    # anime_id → directors/animators
    anime_directors: dict[str, list[tuple[str, float]]] = defaultdict(list)
    anime_animators: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for c in credits:
        w = _role_weight(c.role)
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].append((c.person_id, w))
        if c.role in ANIMATOR_ROLES:
            anime_animators[c.anime_id].append((c.person_id, w))

    for anime_id in anime_directors:
        if anime_id not in anime_animators:
            continue
        importance = _work_importance(anime_map.get(anime_id) if anime_map else None)
        for dir_id, dir_w in anime_directors[anime_id]:
            for anim_id, anim_w in anime_animators[anime_id]:
                if dir_id == anim_id:
                    continue
                edge_w = (dir_w + anim_w) / 2.0 * importance
                if g.has_edge(dir_id, anim_id):
                    g[dir_id][anim_id]["weight"] += edge_w
                    g[dir_id][anim_id]["works"].append(anime_id)
                else:
                    g.add_edge(
                        dir_id,
                        anim_id,
                        weight=edge_w,
                        works=[anime_id],
                    )

    logger.info(
        "director_animator_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def determine_primary_role_for_each_person(
    credits: list[Credit],
) -> dict[str, dict[str, int | str]]:
    """各人物の役職分布と主要カテゴリを算出する.

    Determines each person's primary role category based on their credit distribution.
    Returns:
        {person_id: {"primary_category": "animator"|"director"|...,
                      "role_counts": {role: count}, "total_credits": int}}
    """
    CATEGORY_MAP = {
        Role.DIRECTOR: "director",
        Role.EPISODE_DIRECTOR: "director",
        Role.ANIMATION_DIRECTOR: "animator",
        Role.KEY_ANIMATOR: "animator",
        Role.IN_BETWEEN: "animator",
        Role.LAYOUT: "animator",
        Role.CHARACTER_DESIGNER: "designer",
        Role.BACKGROUND_ART: "designer",
        Role.FINISHING: "designer",
        Role.CGI_DIRECTOR: "technical",
        Role.PHOTOGRAPHY_DIRECTOR: "technical",
        Role.PRODUCER: "production",
        Role.PRODUCTION_MANAGER: "production",
        Role.SOUND_DIRECTOR: "production",
        Role.MUSIC: "production",
        Role.SCREENPLAY: "writing",
        Role.ORIGINAL_CREATOR: "writing",
    }

    person_roles: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for c in credits:
        if c.role in NON_PRODUCTION_ROLES:
            continue
        person_roles[c.person_id][c.role.value] += 1

    result: dict[str, dict[str, int | str]] = {}
    for pid, role_counts in person_roles.items():
        # カテゴリ別の集計
        category_counts: dict[str, int] = defaultdict(int)
        total = 0
        for role_str, count in role_counts.items():
            total += count
            try:
                role = Role(role_str)
                cat = CATEGORY_MAP.get(role, "other")
            except ValueError:
                cat = "other"
            category_counts[cat] += count

        primary = (
            max(category_counts, key=category_counts.get)
            if category_counts
            else "other"
        )

        result[pid] = {
            "primary_category": primary,
            "role_counts": dict(role_counts),
            "total_credits": total,
        }

    logger.info("role_classification_complete", persons=len(result))
    return result


LARGE_GRAPH_THRESHOLD = 500  # nodes


def calculate_network_centrality_scores(
    graph: nx.Graph,
    person_ids: set[str] | None = None,
) -> dict[str, dict[str, float]]:
    """各種中心性指標を算出する.

    Calculates how central each person is to the collaboration network.
    大規模グラフ (>500ノード) の場合は近似アルゴリズムを使用する。
    Uses Rust extension for betweenness/degree/eigenvector when available (50-100x speedup).

    Args:
        graph: 無向コラボレーショングラフ
        person_ids: 対象ノードの限定（None の場合は全ノード）

    Returns:
        {person_id: {"betweenness": ..., "closeness": ..., "degree": ..., "eigenvector": ...}}
    """
    from src.analysis.graph_rust import RUST_AVAILABLE
    from src.analysis import graph_rust
    from src.analysis.sparse_graph import SparseCollaborationGraph

    if graph.number_of_nodes() == 0:
        return {}

    n_nodes = graph.number_of_nodes()
    n_edges = graph.number_of_edges()
    is_large = n_nodes > LARGE_GRAPH_THRESHOLD
    is_sparse = isinstance(graph, SparseCollaborationGraph)

    if is_large:
        logger.info(
            "large_graph_detected",
            nodes=n_nodes,
            edges=n_edges,
            using_approximation=True,
            rust_available=RUST_AVAILABLE,
            sparse=is_sparse,
        )

    metrics: dict[str, dict[str, float]] = {}

    # 次数中心性 (O(V) — SparseCollaborationGraph + NetworkX互換)
    deg_raw = graph.degree()
    s = 1.0 / (n_nodes - 1) if n_nodes > 1 else 1.0
    if isinstance(deg_raw, dict):
        # SparseCollaborationGraph returns dict[str, int]
        degree = {v: d * s for v, d in deg_raw.items()}
    else:
        # NetworkX DegreeView — iterable of (node, degree)
        degree = {v: d * s for v, d in deg_raw}

    # 媒介中心性 — 大規模グラフでは近似版を使用
    # For large graphs: use k-sample approximation (k=200 balances accuracy vs speed).
    # For sparse graphs >5M edges, graph_rust uses the memory-efficient edge-list
    # interface (no adjacency dict OOM).
    betweenness: dict = {}
    if is_large or is_sparse:
        k = min(200, n_nodes)
        betweenness = graph_rust.betweenness_centrality(graph, k=k, seed=42)
    else:
        betweenness = graph_rust.betweenness_centrality(graph)

    # 近接中心性 — 大規模グラフではスキップ（O(V*(V+E))で高コスト）
    # No Rust acceleration for closeness (rarely used on large graphs)
    closeness: dict = {}
    if not is_large and not is_sparse:
        for component in nx.connected_components(graph):
            subg = graph.subgraph(component)
            if subg.number_of_nodes() > 1:
                # Fix B05: Edge weights are similarity (higher = closer), but
                # closeness_centrality(distance=) treats values as distances.
                # Invert weights so strong connections = short distance.
                inv_subg = subg.copy()
                for u_node, v_node, d in inv_subg.edges(data=True):
                    d["distance"] = 1.0 / max(d.get("weight", 1.0), 0.001)
                c = nx.closeness_centrality(inv_subg, distance="distance")
                closeness.update(c)
            else:
                for n in component:
                    closeness[n] = 0.0

    # 固有ベクトル中心性（最大連結成分のみ）
    # Skip on sparse graphs (requires NetworkX conversion) and very large components
    eigenvector: dict = {}
    if is_sparse:
        logger.info("eigenvector_centrality_skipped", reason="sparse graph")
    elif n_nodes > 1:
        largest_cc = max(nx.connected_components(graph), key=len)
        subg = graph.subgraph(largest_cc)
        cc_nodes = subg.number_of_nodes()
        cc_edges = subg.number_of_edges()
        if cc_nodes > 50_000 or cc_edges > 10_000_000:
            logger.info(
                "eigenvector_centrality_skipped",
                nodes=cc_nodes,
                edges=cc_edges,
                reason="graph too large for eigenvector iteration",
            )
        else:
            eigenvector = graph_rust.eigenvector_centrality(subg, max_iter=1000)

    target_nodes = person_ids if person_ids else set(graph.nodes())
    for node in target_nodes:
        if node not in graph:
            continue
        metrics[node] = {
            "degree": degree.get(node, 0.0),
            "betweenness": betweenness.get(node, 0.0),
            "closeness": closeness.get(node, 0.0),
            "eigenvector": eigenvector.get(node, 0.0),
        }

    logger.info("centrality_metrics_computed", nodes=len(metrics))
    return metrics


def compute_graph_summary(graph) -> dict:
    """グラフレベルの統計サマリーを算出する.

    Works with both NetworkX Graph and SparseCollaborationGraph.

    Returns:
        {nodes, edges, density, avg_degree, components, largest_component_size}
    """
    from src.analysis.sparse_graph import SparseCollaborationGraph

    n_nodes = graph.number_of_nodes()
    n_edges = graph.number_of_edges()

    if n_nodes == 0:
        return {
            "nodes": 0,
            "edges": 0,
            "density": 0.0,
            "avg_degree": 0.0,
            "components": 0,
            "largest_component_size": 0,
        }

    is_sparse = isinstance(graph, SparseCollaborationGraph)

    if is_sparse:
        import scipy.sparse as sp

        density = 2.0 * n_edges / (n_nodes * (n_nodes - 1)) if n_nodes > 1 else 0.0
        degree_arr = np.diff(graph.weight_matrix.indptr)
        avg_degree = float(degree_arr.mean())
        # Connected components via scipy
        n_components, labels = sp.csgraph.connected_components(
            graph.weight_matrix, directed=False
        )
        from collections import Counter

        comp_sizes = Counter(labels)
        largest = max(comp_sizes.values()) if comp_sizes else 0
    else:
        density = nx.density(graph)
        degrees = [d for _, d in graph.degree()]
        avg_degree = sum(degrees) / len(degrees) if degrees else 0.0
        components = list(nx.connected_components(graph))
        n_components = len(components)
        largest = max(len(c) for c in components) if components else 0

    summary = {
        "nodes": n_nodes,
        "edges": n_edges,
        "density": round(density, 6),
        "avg_degree": round(avg_degree, 2),
        "components": n_components,
        "largest_component_size": largest,
    }

    # Clustering coefficient (skip for sparse/large graphs)
    if not is_sparse and n_nodes <= 5000 and n_edges <= 100_000 and avg_degree <= 100:
        try:
            avg_clustering = nx.average_clustering(graph, weight="weight")
            summary["avg_clustering"] = round(avg_clustering, 4)
        except Exception:
            pass
    elif not is_sparse and n_nodes <= 10_000 and n_edges <= 500_000:
        try:
            avg_clustering = nx.average_clustering(graph)
            summary["avg_clustering"] = round(avg_clustering, 4)
        except Exception:
            pass

    logger.info("graph_summary", **summary)
    return summary


def main() -> None:
    """エントリーポイント: DBからデータを読み込みグラフを構築して保存."""
    import json

    from src.database import (
        get_connection,
        init_db,
        load_all_anime,
        load_all_credits,
        load_all_persons,
    )
    from src.log import setup_logging
    from src.utils.config import JSON_DIR

    setup_logging()

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)
    conn.close()

    if not credits:
        logger.warning("No credits found in DB. Run scraper first.")
        return

    # 二部グラフ
    bp_graph = create_person_anime_network(persons, anime_list, credits)

    # コラボレーショングラフ
    anime_map = {a.id: a for a in anime_list}
    collab_graph = create_person_collaboration_network(
        persons, credits, anime_map=anime_map
    )

    # 監督→アニメーターグラフ
    da_graph = create_director_animator_network(credits, anime_map=anime_map)

    # 統計出力
    stats = {
        "bipartite": {
            "nodes": bp_graph.number_of_nodes(),
            "edges": bp_graph.number_of_edges(),
        },
        "collaboration": {
            "nodes": collab_graph.number_of_nodes(),
            "edges": collab_graph.number_of_edges(),
        },
        "director_animator": {
            "nodes": da_graph.number_of_nodes(),
            "edges": da_graph.number_of_edges(),
        },
    }

    JSON_DIR.mkdir(parents=True, exist_ok=True)
    stats_path = JSON_DIR / "graph_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logger.info("graph_stats_saved", path=str(stats_path))
    logger.info("graph_stats", stats=stats)


if __name__ == "__main__":
    main()

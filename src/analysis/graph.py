"""コラボレーショングラフ構築 (NetworkX).

ノード種別:
  - person: アニメーター、監督等
  - anime: アニメ作品

エッジ:
  - person → anime: クレジット関係 (weight = 役職重み)
  - person → person: 共同クレジット関係 (weight = 共演回数 × 役職重み)
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Anime, Credit, Person, Role
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


def create_person_anime_network(
    persons: list[Person],
    anime_list: list[Anime],
    credits: list[Credit],
) -> nx.DiGraph:
    """二部グラフ (person ↔ anime) を構築する.

    Creates a bipartite network connecting people to the anime works they contributed to.
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
    for a in anime_list:
        g.add_node(a.id, type="anime", name=a.display_title, year=a.year, score=a.score)

    # クレジットエッジ（非制作ロールを除外）
    for c in credits:
        if c.role in NON_PRODUCTION_ROLES:
            continue
        weight = _role_weight(c.role)
        # person → anime
        if g.has_edge(c.person_id, c.anime_id):
            g[c.person_id][c.anime_id]["weight"] += weight
            g[c.person_id][c.anime_id]["roles"].append(c.role.value)
        else:
            g.add_edge(c.person_id, c.anime_id, weight=weight, roles=[c.role.value])
        # anime → person (逆方向、PageRank 伝播用)
        if g.has_edge(c.anime_id, c.person_id):
            g[c.anime_id][c.person_id]["weight"] += weight
        else:
            g.add_edge(c.anime_id, c.person_id, weight=weight)

    logger.info(
        "bipartite_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


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
    """Compute work importance multiplier from score and duration.

    Score component: anime.score / 10.0 (range 0.1-1.0)
    Duration component: anime.duration / 30 (30分基準, capped at 2.0x)

    Mini-anime (5 min) gets ~0.17x, standard TV (24 min) gets 0.8x,
    movies (120 min) get 2.0x (capped).
    """
    from src.utils.config import DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER

    # Score-based importance
    if anime is None or anime.score is None:
        score_mult = 0.5
    else:
        score_mult = max(anime.score / 10.0, 0.1)

    # Duration-based importance (30分基準)
    if anime is None or anime.duration is None:
        return score_mult

    duration_mult = min(
        anime.duration / DURATION_BASELINE_MINUTES,
        DURATION_MAX_MULTIPLIER,
    )
    return max(score_mult * duration_mult, 0.01)


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
            # Estimated overlap = known_frac × unknown_frac × total_episodes
            # Normalized by union ≈ (known_frac + unknown_frac) × total_episodes
            return (known_frac * unknown_frac) / max(
                known_frac + unknown_frac - known_frac * unknown_frac, 0.001
            )

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
            new_weight += commit_a * commit_b * ep_w * importance
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
) -> dict[tuple[str, str], dict[str, float]]:
    """Build collaboration edges in pure Python with optional episode awareness."""
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
                edge_weight = commit_a * commit_b * ep_w * importance
                edge_data[edge_key]["weight"] += edge_weight
                edge_data[edge_key]["shared_works"] += 1
        else:
            # Deduplicate: aggregate per person to avoid overcounting shared_works
            seen_persons: dict[str, tuple[Role, float]] = {}
            for pid, role, w in staff_list:
                if pid not in seen_persons or w > seen_persons[pid][1]:
                    seen_persons[pid] = (role, w)
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
                edge_weight = commit_a * commit_b * importance
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
            new_weights[edge_key] += commit_a * commit_b * importance
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


def create_person_collaboration_network(
    persons: list[Person],
    credits: list[Credit],
    anime_map: dict[str, Anime] | None = None,
) -> nx.Graph:
    """人物間コラボレーション無向グラフを構築する.

    Creates a network of people who worked together on the same anime.
    同じ作品に参加した人物同士にエッジを張る。
    エッジ重み = Σ(commitment_a × commitment_b × episode_overlap × work_importance)

    Commitment = sum of role_weight × episode_coverage for each role a person holds.
    Work importance = anime score / 10.0 (0.1-1.0, default 0.5).

    Episode-aware weighting reduces spurious edges on long-running anime by
    considering actual episode overlap when available, and applying role-based
    heuristics when episode data is missing.

    Uses Rust extension for edge aggregation when available (10-30x speedup),
    falling back to Python with episode-aware weighting.
    """
    from src.analysis.graph_rust import RUST_AVAILABLE, build_collaboration_edges

    g = nx.Graph()

    for p in persons:
        g.add_node(p.id, name=p.display_name, name_ja=p.name_ja, name_en=p.name_en)

    # 非制作ロール（声優、主題歌等）を除外
    credits = [c for c in credits if c.role not in NON_PRODUCTION_ROLES]

    # Compute commitment data for all anime
    commitments = _compute_anime_commitments(credits, anime_map)

    # Check if any credits have episode data (enables episode-aware path)
    has_episode_data = any(c.episode is not None for c in credits)

    # Build per-anime, per-person episode/role info (needed for episode-aware weighting)
    # Structure: {anime_id: {person_id: (episodes, primary_role, max_weight)}}
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

    if RUST_AVAILABLE and not has_episode_data:
        # Rust-accelerated edge aggregation, then recompute with commitments
        edge_data = build_collaboration_edges(persons, credits)
        _apply_commitment_adjustments(edge_data, credits, anime_map, commitments)
    elif RUST_AVAILABLE and has_episode_data:
        # Rust builds base edges, then apply episode + commitment weight adjustments
        edge_data = build_collaboration_edges(persons, credits)
        _apply_episode_adjustments(edge_data, anime_person_info, anime_map, commitments)
    else:
        # Pure Python path with episode-aware edge aggregation
        edge_data = _build_edges_python(
            credits, anime_person_info, anime_map, has_episode_data, commitments
        )

    # Batch add all edges to graph (single pass, no has_edge() calls)
    g.add_edges_from(
        (pid_a, pid_b, attrs) for (pid_a, pid_b), attrs in edge_data.items()
    )

    logger.info(
        "collaboration_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
        episode_aware=has_episode_data,
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
        Role.CHIEF_ANIMATION_DIRECTOR: "director",
        Role.EPISODE_DIRECTOR: "director",
        Role.STORYBOARD: "director",
        Role.ANIMATION_DIRECTOR: "animator",
        Role.KEY_ANIMATOR: "animator",
        Role.SECOND_KEY_ANIMATOR: "animator",
        Role.IN_BETWEEN: "animator",
        Role.LAYOUT: "animator",
        Role.EFFECTS: "animator",
        Role.CHARACTER_DESIGNER: "designer",
        Role.MECHANICAL_DESIGNER: "designer",
        Role.ART_DIRECTOR: "designer",
        Role.COLOR_DESIGNER: "designer",
        Role.BACKGROUND_ART: "designer",
        Role.CGI_DIRECTOR: "technical",
        Role.PHOTOGRAPHY_DIRECTOR: "technical",
        Role.PRODUCER: "production",
        Role.SOUND_DIRECTOR: "production",
        Role.MUSIC: "production",
        Role.SERIES_COMPOSITION: "writing",
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

    if graph.number_of_nodes() == 0:
        return {}

    n_nodes = graph.number_of_nodes()
    is_large = n_nodes > LARGE_GRAPH_THRESHOLD

    if is_large:
        logger.info(
            "large_graph_detected",
            nodes=n_nodes,
            edges=graph.number_of_edges(),
            using_approximation=True,
            rust_available=RUST_AVAILABLE,
        )

    metrics: dict[str, dict[str, float]] = {}

    # 次数中心性 (O(V) — NetworkX直接の方が変換オーバーヘッドなく速い)
    degree = nx.degree_centrality(graph)

    # 媒介中心性 — 大規模グラフでは近似版を使用
    if is_large:
        k = min(100, n_nodes)
        betweenness = graph_rust.betweenness_centrality(graph, k=k, seed=42)
    else:
        betweenness = graph_rust.betweenness_centrality(graph)

    # 近接中心性 — 大規模グラフではスキップ（O(V*(V+E))で高コスト）
    # No Rust acceleration for closeness (rarely used on large graphs)
    closeness: dict = {}
    if not is_large:
        for component in nx.connected_components(graph):
            subg = graph.subgraph(component)
            if subg.number_of_nodes() > 1:
                c = nx.closeness_centrality(subg, distance="weight")
                closeness.update(c)
            else:
                for n in component:
                    closeness[n] = 0.0

    # 固有ベクトル中心性（最大連結成分のみ）
    # Skip on very large components — eigenvector iteration is O(V*E) per iteration
    eigenvector: dict = {}
    if n_nodes > 1:
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


def compute_graph_summary(graph: nx.Graph) -> dict:
    """グラフレベルの統計サマリーを算出する.

    Returns:
        {nodes, edges, density, avg_degree, components, largest_component_size}
    """
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

    density = nx.density(graph)
    degrees = [d for _, d in graph.degree()]
    avg_degree = sum(degrees) / len(degrees) if degrees else 0.0
    components = list(nx.connected_components(graph))
    largest = max(len(c) for c in components) if components else 0

    summary = {
        "nodes": n_nodes,
        "edges": n_edges,
        "density": round(density, 6),
        "avg_degree": round(avg_degree, 2),
        "components": len(components),
        "largest_component_size": largest,
    }

    # Clustering coefficient (skip for very large/dense graphs)
    # Weighted clustering is O(n * d^2) where d = avg_degree
    # Skip if: nodes > 5000 OR edges > 100K OR avg_degree > 100
    if n_nodes <= 5000 and n_edges <= 100_000 and avg_degree <= 100:
        try:
            avg_clustering = nx.average_clustering(graph, weight="weight")
            summary["avg_clustering"] = round(avg_clustering, 4)
        except Exception:
            pass
    elif n_nodes <= 10_000 and n_edges <= 500_000:
        # For moderately large graphs, use unweighted clustering (much faster)
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

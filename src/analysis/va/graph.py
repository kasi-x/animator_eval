"""Voice Actor Graph Construction — bipartite, collaboration, and sound director graphs.

Three graphs:
1. VA-Anime bipartite: VA ↔ anime (for BiRank)
2. VA collaboration: VA ↔ VA (shared anime, co-main bonus)
3. VA-Sound Director: VA ↔ sound_director (casting relationship)

Character role weights: MAIN=3.0, SUPPORTING=1.5, BACKGROUND=0.5
Franchise bonus: 1.0 + 0.1*(entries-1), capped at 1.5
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Anime, CharacterVoiceActor, Credit, Role

logger = structlog.get_logger()

# Character role weights (higher = more prominent casting)
CHARACTER_ROLE_WEIGHTS: dict[str, float] = {
    "MAIN": 3.0,
    "SUPPORTING": 1.5,
    "BACKGROUND": 0.5,
}

# Default weight for unknown character roles
_DEFAULT_CHAR_ROLE_WEIGHT = 0.5


def _char_role_weight(role: str) -> float:
    """Get weight for a character role string."""
    return CHARACTER_ROLE_WEIGHTS.get(role.upper(), _DEFAULT_CHAR_ROLE_WEIGHT)


def _duration_mult(anime: Anime | None) -> float:
    """Duration-based importance multiplier (same as production pipeline)."""
    if not anime or not anime.duration:
        return 1.0
    return min(anime.duration / 30, 2.0)


def _compute_franchise_bonus(
    va_credits: list[CharacterVoiceActor],
) -> dict[tuple[str, str], float]:
    """Compute franchise bonus for (person_id, character_id) pairs.

    Same character across multiple anime = franchise commitment.
    Bonus: 1.0 + 0.1 * (entries - 1), capped at 1.5.
    """
    # Count anime per (person, character)
    pc_anime: dict[tuple[str, str], set[str]] = defaultdict(set)
    for cva in va_credits:
        pc_anime[(cva.person_id, cva.character_id)].add(cva.anime_id)

    return {
        key: min(1.0 + 0.1 * (len(anime_ids) - 1), 1.5)
        for key, anime_ids in pc_anime.items()
    }


def build_va_anime_graph(
    va_credits: list[CharacterVoiceActor],
    anime_map: dict[str, Anime],
) -> nx.Graph:
    """Build VA-Anime bipartite graph for BiRank.

    Edge weight = Σ(character_role_weight × franchise_bonus) × duration_mult

    Args:
        va_credits: all character_voice_actor records
        anime_map: anime_id → Anime

    Returns:
        Bipartite graph with VA and anime nodes.
    """
    g = nx.Graph()
    franchise_bonus = _compute_franchise_bonus(va_credits)

    # Aggregate weights per (VA, anime)
    edge_weights: dict[tuple[str, str], float] = defaultdict(float)
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        dur_m = _duration_mult(anime)
        cr_w = _char_role_weight(cva.character_role)
        fb = franchise_bonus.get((cva.person_id, cva.character_id), 1.0)
        edge_weights[(cva.person_id, cva.anime_id)] += cr_w * fb * dur_m

    # Add nodes and edges
    va_ids: set[str] = set()
    anime_ids: set[str] = set()
    for (va_id, anime_id), weight in edge_weights.items():
        va_ids.add(va_id)
        anime_ids.add(anime_id)
        g.add_edge(va_id, anime_id, weight=weight)

    # Mark bipartite sets
    for va_id in va_ids:
        g.nodes[va_id]["bipartite"] = 0  # VA
    for anime_id in anime_ids:
        g.nodes[anime_id]["bipartite"] = 1  # anime

    logger.info(
        "va_anime_graph_built",
        va_nodes=len(va_ids),
        anime_nodes=len(anime_ids),
        edges=g.number_of_edges(),
    )
    return g


def build_va_collaboration_graph(
    va_credits: list[CharacterVoiceActor],
    anime_map: dict[str, Anime],
) -> nx.Graph:
    """Build VA collaboration graph (VA ↔ VA).

    Edge weight = Σ_shared_anime(min(role_w_a, role_w_b) × duration_mult × co_main_bonus)

    Uses MAIN-role VAs as hubs (star topology) to avoid O(n²) for large casts.

    Args:
        va_credits: all character_voice_actor records
        anime_map: anime_id → Anime

    Returns:
        Undirected collaboration graph.
    """
    # Group VAs per anime with their best character role weight
    anime_vas: dict[str, dict[str, float]] = defaultdict(dict)
    for cva in va_credits:
        w = _char_role_weight(cva.character_role)
        key = cva.anime_id
        cur = anime_vas[key].get(cva.person_id, 0.0)
        anime_vas[key][cva.person_id] = max(cur, w)

    # Build edges using star topology: MAIN VAs are hubs
    edge_weights: dict[tuple[str, str], float] = defaultdict(float)
    main_threshold = CHARACTER_ROLE_WEIGHTS["MAIN"]

    for anime_id, va_weights in anime_vas.items():
        if len(va_weights) < 2:
            continue

        dur_m = _duration_mult(anime_map.get(anime_id))
        main_vas = [vid for vid, w in va_weights.items() if w >= main_threshold]
        other_vas = [vid for vid, w in va_weights.items() if w < main_threshold]

        # If no main VAs, use all pairs (small cast)
        if not main_vas:
            vas = sorted(va_weights.keys())
            for i, a in enumerate(vas):
                for b in vas[i + 1 :]:
                    w = min(va_weights[a], va_weights[b]) * dur_m
                    key = (a, b) if a < b else (b, a)
                    edge_weights[key] += w
            continue

        # Main ↔ Main: all pairs with co-main bonus
        for i, a in enumerate(main_vas):
            for b in main_vas[i + 1 :]:
                co_main = 1.5  # co-main bonus
                w = min(va_weights[a], va_weights[b]) * dur_m * co_main
                key = (a, b) if a < b else (b, a)
                edge_weights[key] += w

        # Main ↔ Other: star edges
        for other in other_vas:
            for main in main_vas:
                w = min(va_weights[main], va_weights[other]) * dur_m
                key = (main, other) if main < other else (other, main)
                edge_weights[key] += w

    # Build graph
    g = nx.Graph()
    for (a, b), weight in edge_weights.items():
        g.add_edge(a, b, weight=weight)

    logger.info(
        "va_collaboration_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def build_va_sound_director_graph(
    va_credits: list[CharacterVoiceActor],
    production_credits: list[Credit],
    anime_map: dict[str, Anime],
) -> nx.Graph:
    """Build VA-Sound Director bipartite graph.

    Sound directors make casting decisions — they are to VAs what
    directors are to animators.

    Edge weight = Σ(max_char_role_weight × duration_mult)

    Args:
        va_credits: character_voice_actor records
        production_credits: production credit records (to find sound directors)
        anime_map: anime_id → Anime

    Returns:
        Bipartite graph with VA and sound_director nodes.
    """
    # Find sound directors per anime
    anime_sound_directors: dict[str, set[str]] = defaultdict(set)
    for c in production_credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sound_directors[c.anime_id].add(c.person_id)

    # Compute best character role per (VA, anime)
    va_anime_best: dict[tuple[str, str], float] = {}
    for cva in va_credits:
        key = (cva.person_id, cva.anime_id)
        w = _char_role_weight(cva.character_role)
        va_anime_best[key] = max(va_anime_best.get(key, 0.0), w)

    # Build edges: VA ↔ sound_director
    edge_weights: dict[tuple[str, str], float] = defaultdict(float)
    for (va_id, anime_id), best_w in va_anime_best.items():
        dur_m = _duration_mult(anime_map.get(anime_id))
        for sd_id in anime_sound_directors.get(anime_id, set()):
            edge_weights[(va_id, sd_id)] += best_w * dur_m

    # Build graph
    g = nx.Graph()
    va_ids: set[str] = set()
    sd_ids: set[str] = set()
    for (va_id, sd_id), weight in edge_weights.items():
        va_ids.add(va_id)
        sd_ids.add(sd_id)
        g.add_edge(va_id, sd_id, weight=weight)

    for va_id in va_ids:
        g.nodes[va_id]["bipartite"] = 0
    for sd_id in sd_ids:
        g.nodes[sd_id]["bipartite"] = 1

    logger.info(
        "va_sd_graph_built",
        va_nodes=len(va_ids),
        sd_nodes=len(sd_ids),
        edges=g.number_of_edges(),
    )
    return g

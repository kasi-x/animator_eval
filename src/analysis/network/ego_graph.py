"""Ego graph — extract the local network centred on a specific person.

指定した人物のN-hop近傍のコラボレーターを抽出し、
サブグラフとして返す。プロフィール表示やネットワーク図に使用。
"""

from collections import defaultdict

import structlog

from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def extract_ego_graph(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    hops: int = 1,
    person_scores: dict[str, float] | None = None,
) -> dict:
    """Extract the ego graph (local network) of the specified person.

    Args:
        person_id: 中心人物ID
        credits: 全クレジット
        anime_map: anime_id → Anime
        hops: 何ホップ先まで含めるか (default: 1)
        person_scores: {person_id: composite_score}

    Returns:
        {
            "center": person_id,
            "nodes": [{id, distance, shared_works, score}],
            "edges": [{source, target, shared_works, anime_ids}],
            "total_nodes": int,
            "total_edges": int,
        }
    """
    # Build person → anime and anime → person mappings
    person_anime: dict[str, set[str]] = defaultdict(set)
    anime_persons: dict[str, set[str]] = defaultdict(set)

    for c in credits:
        person_anime[c.person_id].add(c.anime_id)
        anime_persons[c.anime_id].add(c.person_id)

    if person_id not in person_anime:
        return {
            "center": person_id,
            "nodes": [],
            "edges": [],
            "total_nodes": 0,
            "total_edges": 0,
        }

    # BFS to find N-hop neighbors
    visited: dict[str, int] = {person_id: 0}
    frontier = {person_id}

    for hop in range(1, hops + 1):
        next_frontier: set[str] = set()
        for pid in frontier:
            for aid in person_anime.get(pid, set()):
                for neighbor in anime_persons.get(aid, set()):
                    if neighbor not in visited:
                        visited[neighbor] = hop
                        next_frontier.add(neighbor)
        frontier = next_frontier

    # Build edges between all visited persons
    edges: dict[tuple[str, str], set[str]] = {}
    for aid, persons in anime_persons.items():
        local_persons = [p for p in persons if p in visited]
        for i, a in enumerate(local_persons):
            for b in local_persons[i + 1 :]:
                key = (min(a, b), max(a, b))
                if key not in edges:
                    edges[key] = set()
                edges[key].add(aid)

    # Format nodes
    nodes = []
    for pid, distance in sorted(visited.items(), key=lambda x: (x[1], x[0])):
        node: dict = {
            "id": pid,
            "distance": distance,
            "shared_works": len(
                person_anime.get(pid, set()) & person_anime.get(person_id, set())
            ),
        }
        if person_scores and pid in person_scores:
            node["score"] = person_scores[pid]
        nodes.append(node)

    # Format edges
    formatted_edges = [
        {
            "source": a,
            "target": b,
            "shared_works": len(anime_ids),
            "anime_ids": sorted(anime_ids),
        }
        for (a, b), anime_ids in sorted(edges.items(), key=lambda x: -len(x[1]))
    ]

    return {
        "center": person_id,
        "nodes": nodes,
        "edges": formatted_edges,
        "total_nodes": len(nodes),
        "total_edges": len(formatted_edges),
    }

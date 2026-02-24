"""コラボレーション強度分析 — 人物ペア間の協力関係の深さを定量化する.

ペア単位で:
- shared_works: 共同作品数
- shared_roles: ロールの組み合わせ頻度
- first_collab / latest_collab: 最初/最新の共同年
- longevity: コラボレーション期間（年数）
- strength_score: 総合強度 (0-100)
"""

from collections import defaultdict

import structlog

import networkx as nx

from src.models import Anime, Credit

logger = structlog.get_logger()


def _from_collaboration_graph(
    graph: nx.Graph,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared: int,
    person_scores: dict[str, float] | None,
) -> list[dict]:
    """Fast path: extract collaboration strength from existing graph edges.

    The collaboration_graph already stores shared_works counts per edge.
    We only need to enrich with year/role data for qualifying pairs (min_shared).
    """
    # Pre-build per-pair anime lookup for enrichment (only if needed)
    anime_person_roles: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    anime_years: dict[str, int | None] = {}
    for c in credits:
        anime_person_roles[c.anime_id][c.person_id].add(c.role.value)
    for anime_id, anime in anime_map.items():
        anime_years[anime_id] = anime.year

    # Build pair → shared anime_ids index (lightweight: only track anime_id per pair)
    pair_anime: dict[tuple[str, str], list[str]] = defaultdict(list)
    for anime_id, person_roles in anime_person_roles.items():
        persons = list(person_roles.keys())
        if len(persons) > 200:
            # For very large casts, skip pair enumeration (handled by graph edges)
            continue
        for i in range(len(persons)):
            for j in range(i + 1, len(persons)):
                a, b = persons[i], persons[j]
                key = (a, b) if a < b else (b, a)
                pair_anime[key].append(anime_id)

    # Extract qualifying edges from graph
    max_shared = 1
    qualifying: list[tuple[str, str, int]] = []
    for u, v, data in graph.edges(data=True):
        shared = int(data.get("shared_works", 1))
        if shared >= min_shared:
            key = (u, v) if u < v else (v, u)
            qualifying.append((key[0], key[1], shared))
            if shared > max_shared:
                max_shared = shared

    results = []
    for pid_a, pid_b, shared_count in qualifying:
        key = (pid_a, pid_b)
        anime_ids = pair_anime.get(key, [])

        # Compute years
        years = sorted(
            {
                anime_years.get(aid)
                for aid in anime_ids
                if anime_years.get(aid) is not None
            }
        )
        first_year = years[0] if years else None
        latest_year = years[-1] if years else None
        longevity = (latest_year - first_year + 1) if first_year and latest_year else 0

        # Role pairs (top 5)
        role_pairs: list[str] = []
        for aid in anime_ids[:10]:  # Limit to avoid excessive iteration
            roles_a = anime_person_roles[aid].get(pid_a, set())
            roles_b = anime_person_roles[aid].get(pid_b, set())
            for ra in roles_a:
                for rb in roles_b:
                    role_pairs.append(f"{ra}+{rb}")
        role_pair_counts: dict[str, int] = defaultdict(int)
        for rp in role_pairs:
            role_pair_counts[rp] += 1
        top_role_pairs = sorted(role_pair_counts.items(), key=lambda x: -x[1])[:5]

        # Strength score
        freq_component = min(shared_count / max(max_shared, 1), 1.0) * 50
        longevity_component = min(longevity / 10, 1.0) * 30
        recency_component = (
            20
            if latest_year and latest_year >= 2020
            else 10
            if latest_year and latest_year >= 2015
            else 0
        )
        strength = round(freq_component + longevity_component + recency_component, 1)

        entry: dict = {
            "person_a": pid_a,
            "person_b": pid_b,
            "shared_works": shared_count,
            "shared_anime": sorted(set(anime_ids)),
            "top_role_pairs": [
                {"pair": rp, "count": cnt} for rp, cnt in top_role_pairs
            ],
            "first_year": first_year,
            "latest_year": latest_year,
            "longevity": longevity,
            "strength_score": strength,
        }

        if person_scores:
            score_a = person_scores.get(pid_a)
            score_b = person_scores.get(pid_b)
            if score_a is not None and score_b is not None:
                entry["combined_score"] = round((score_a + score_b) / 2, 2)

        results.append(entry)

    results.sort(key=lambda x: x["strength_score"], reverse=True)
    logger.info(
        "collaboration_strength_computed",
        pairs=len(results),
        min_shared=min_shared,
        fast_path=True,
    )
    return results


def compute_collaboration_strength(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared: int = 2,
    person_scores: dict[str, float] | None = None,
    collaboration_graph: "nx.Graph | None" = None,
) -> list[dict]:
    """人物ペア間のコラボレーション強度を算出する.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        min_shared: 最低共同作品数 (これ未満のペアは除外)
        person_scores: {person_id: composite_score} (optional)
        collaboration_graph: 既存のコラボグラフ (再利用で高速化)

    Returns:
        list of {person_a, person_b, shared_works, shared_anime, role_pairs,
                 first_year, latest_year, longevity, strength_score}
    """
    # Fast path: reuse existing collaboration_graph edges
    if collaboration_graph is not None:
        return _from_collaboration_graph(
            collaboration_graph,
            credits,
            anime_map,
            min_shared,
            person_scores,
        )

    # Build anime → {person_id: set(roles)} mapping (pre-compute for O(1) lookup)
    anime_person_roles: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for c in credits:
        anime_person_roles[c.anime_id][c.person_id].add(c.role.value)

    # Pass 1: Count shared works per pair (lightweight — no role data yet)
    pair_shared_anime: dict[tuple[str, str], list[str]] = defaultdict(list)
    pair_years: dict[tuple[str, str], list[int]] = defaultdict(list)

    for anime_id, person_roles in anime_person_roles.items():
        anime = anime_map.get(anime_id)
        year = anime.year if anime else None
        persons = list(person_roles.keys())

        for i in range(len(persons)):
            pid_a = persons[i]
            for j in range(i + 1, len(persons)):
                pid_b = persons[j]
                key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
                pair_shared_anime[key].append(anime_id)
                if year:
                    pair_years[key].append(year)

    # Pass 2: Collect role pairs only for qualifying pairs (min_shared filter)
    pair_data: dict[tuple[str, str], dict] = {}
    for key, anime_ids in pair_shared_anime.items():
        if len(anime_ids) < min_shared:
            continue
        pid_a, pid_b = key
        # Collect role pairs from all shared anime
        role_pairs: list[str] = []
        for anime_id in anime_ids:
            roles_a = anime_person_roles[anime_id].get(pid_a, set())
            roles_b = anime_person_roles[anime_id].get(pid_b, set())
            for ra in roles_a:
                for rb in roles_b:
                    role_pairs.append(f"{ra}+{rb}")

        pair_data[key] = {
            "anime_ids": set(anime_ids),
            "years": pair_years.get(key, []),
            "role_pairs": role_pairs,
        }

    # Filter by min_shared and compute metrics
    results = []
    max_shared = max((len(d["anime_ids"]) for d in pair_data.values()), default=1)

    for (pid_a, pid_b), data in pair_data.items():
        shared_count = len(data["anime_ids"])
        if shared_count < min_shared:
            continue

        years = sorted(set(data["years"]))
        first_year = years[0] if years else None
        latest_year = years[-1] if years else None
        longevity = (latest_year - first_year + 1) if first_year and latest_year else 0

        # Role pair frequency
        role_pair_counts: dict[str, int] = defaultdict(int)
        for rp in data["role_pairs"]:
            role_pair_counts[rp] += 1
        top_role_pairs = sorted(role_pair_counts.items(), key=lambda x: -x[1])[:5]

        # Strength score: combination of frequency, longevity, recency
        freq_component = min(shared_count / max(max_shared, 1), 1.0) * 50
        longevity_component = min(longevity / 10, 1.0) * 30
        recency_component = (
            20
            if latest_year and latest_year >= 2020
            else 10
            if latest_year and latest_year >= 2015
            else 0
        )
        strength = round(freq_component + longevity_component + recency_component, 1)

        entry: dict = {
            "person_a": pid_a,
            "person_b": pid_b,
            "shared_works": shared_count,
            "shared_anime": sorted(data["anime_ids"]),
            "top_role_pairs": [
                {"pair": rp, "count": cnt} for rp, cnt in top_role_pairs
            ],
            "first_year": first_year,
            "latest_year": latest_year,
            "longevity": longevity,
            "strength_score": strength,
        }

        if person_scores:
            score_a = person_scores.get(pid_a)
            score_b = person_scores.get(pid_b)
            if score_a is not None and score_b is not None:
                entry["combined_score"] = round((score_a + score_b) / 2, 2)

        results.append(entry)

    results.sort(key=lambda x: x["strength_score"], reverse=True)

    logger.info(
        "collaboration_strength_computed",
        pairs=len(results),
        min_shared=min_shared,
    )
    return results

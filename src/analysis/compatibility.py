"""Compatibility Groups — 全作品横断で相性グループを検出.

共演3回以上のペアを抽出し、共演時 vs 非共演時の制作規模残差比較で
相性スコアを算出。正の相性ペアからグループとブリッジ人材を検出。
anime.score は使用しない（構造指標のみ）。
"""

from collections import defaultdict
from dataclasses import asdict, dataclass
import random

import numpy as np
import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class CompatibilityPair:
    """A pair of persons with measured compatibility."""

    person_a: str
    person_b: str
    shared_works: int
    compatibility_score: float  # 共演時 - 非共演時の残差平均
    avg_shared_score: float


@dataclass
class CompatibleGroup:
    """A group of mutually compatible persons."""

    members: list[str]
    group_compatibility: float  # avg pair compatibility × cohesion
    shared_works: list[str]


@dataclass
class GroupCompatibilityResult:
    """Full result of compatibility group detection."""

    compatible_pairs: list[dict]
    compatible_groups: list[dict]
    bridge_persons: dict[str, float]  # person_id → bridge_compatibility_score
    person_compatibility_boost: dict[str, float]  # person_id → aggregated boost
    total_pairs_analyzed: int


def compute_compatibility_groups(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    iv_scores: dict[str, float],
    collaboration_graph=None,
    community_map: dict[str, int] | None = None,
    min_shared_works: int = 3,
) -> GroupCompatibilityResult:
    """Detect compatibility groups from co-occurrence patterns.

    Algorithm:
    1. Find all pairs with min_shared_works co-appearances
    2. For each pair, compute compatibility as project-quality-controlled
       score difference (with vs without each other)
    3. Extract groups via triangle enumeration on positive-compatibility edges
    4. Identify bridge persons spanning multiple groups

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        iv_scores: person_id → IV score
        collaboration_graph: NetworkX collaboration graph (optional)
        community_map: person_id → community_id (optional)
        min_shared_works: minimum co-appearances for pair analysis

    Returns:
        GroupCompatibilityResult
    """
    # Build anime → staff set and person → anime set
    # Uses staff_count as structural outcome (not anime.score — viewer ratings excluded)
    anime_staff: dict[str, set[str]] = defaultdict(set)
    person_anime: dict[str, set[str]] = defaultdict(set)
    person_anime_scale: dict[str, dict[str, float]] = defaultdict(dict)

    for c in credits:
        if c.person_id not in iv_scores:
            continue
        anime_staff[c.anime_id].add(c.person_id)
        person_anime[c.person_id].add(c.anime_id)

    # Compute per-anime production scale (log staff count) as structural outcome
    for c in credits:
        if c.person_id not in iv_scores:
            continue
        aid = c.anime_id
        if aid not in person_anime_scale[c.person_id]:
            staff_count = len(anime_staff.get(aid, set()))
            if staff_count > 0:
                person_anime_scale[c.person_id][aid] = float(np.log1p(staff_count))

    # Find pairs with sufficient shared works
    # Use anime_staff to count co-occurrences efficiently
    pair_shared: dict[tuple[str, str], set[str]] = defaultdict(set)
    for aid, staff in anime_staff.items():
        staff_list = sorted(staff)
        # D13 fix: Sample up to 50 persons from large casts instead of skipping.
        # Keeps the O(n²) pair count bounded (50² = 2500 pairs/anime) while
        # including large productions (blockbusters, long-running TV series).
        # Deterministic seed based on anime_id ensures reproducibility across runs.
        if len(staff_list) > 50:
            rng = random.Random(hash(aid) & 0x7FFFFFFF)
            staff_list = sorted(rng.sample(staff_list, 50))
        for i, a in enumerate(staff_list):
            for b in staff_list[i + 1:]:
                pair_shared[(a, b)].add(aid)

    # Filter to pairs with enough shared works
    candidate_pairs = {
        pair: shared for pair, shared in pair_shared.items()
        if len(shared) >= min_shared_works
    }

    if not candidate_pairs:
        logger.info("compatibility_no_pairs", min_shared=min_shared_works)
        return GroupCompatibilityResult(
            compatible_pairs=[],
            compatible_groups=[],
            bridge_persons={},
            person_compatibility_boost={},
            total_pairs_analyzed=0,
        )

    # Compute compatibility for each pair
    compatible_pairs: list[CompatibilityPair] = []

    for (pid_a, pid_b), shared_anime in candidate_pairs.items():
        a_anime = person_anime_scale.get(pid_a, {})
        b_anime = person_anime_scale.get(pid_b, {})

        # Shared works production scale
        shared_scores = []
        for aid in shared_anime:
            if aid in a_anime:
                shared_scores.append(a_anime[aid])
            elif aid in b_anime:
                shared_scores.append(b_anime[aid])

        if not shared_scores:
            continue

        avg_shared = float(np.mean(shared_scores))

        # Project-quality controlled comparison for person A
        a_with = []
        a_without = []
        for aid, score in a_anime.items():
            # Project quality: avg IV of other staff
            other_ivs = [
                iv_scores.get(p, 0.0)
                for p in anime_staff.get(aid, set())
                if p != pid_a and p in iv_scores
            ]
            proj_q = float(np.mean(other_ivs)) if other_ivs else 0.0
            resid = score - proj_q
            if aid in shared_anime:
                a_with.append(resid)
            else:
                a_without.append(resid)

        # Same for person B
        b_with = []
        b_without = []
        for aid, score in b_anime.items():
            other_ivs = [
                iv_scores.get(p, 0.0)
                for p in anime_staff.get(aid, set())
                if p != pid_b and p in iv_scores
            ]
            proj_q = float(np.mean(other_ivs)) if other_ivs else 0.0
            resid = score - proj_q
            if aid in shared_anime:
                b_with.append(resid)
            else:
                b_without.append(resid)

        # Compatibility = avg of both persons' with-vs-without differences
        diffs = []
        if a_with and a_without:
            diffs.append(float(np.mean(a_with) - np.mean(a_without)))
        if b_with and b_without:
            diffs.append(float(np.mean(b_with) - np.mean(b_without)))

        if not diffs:
            continue

        compat_score = float(np.mean(diffs))

        compatible_pairs.append(CompatibilityPair(
            person_a=pid_a,
            person_b=pid_b,
            shared_works=len(shared_anime),
            compatibility_score=round(compat_score, 4),
            avg_shared_score=round(avg_shared, 2),
        ))

    # Sort by compatibility score descending
    compatible_pairs.sort(key=lambda p: p.compatibility_score, reverse=True)

    # Build positive-compatibility adjacency for group detection
    pos_adj: dict[str, set[str]] = defaultdict(set)
    pair_compat: dict[tuple[str, str], float] = {}
    for pair in compatible_pairs:
        if pair.compatibility_score > 0:
            pos_adj[pair.person_a].add(pair.person_b)
            pos_adj[pair.person_b].add(pair.person_a)
            key = (min(pair.person_a, pair.person_b), max(pair.person_a, pair.person_b))
            pair_compat[key] = pair.compatibility_score

    # Triangle enumeration → groups
    triangles: list[frozenset[str]] = []
    seen_triangles: set[frozenset[str]] = set()
    for a in pos_adj:
        for b in pos_adj[a]:
            if b <= a:
                continue
            common = pos_adj[a] & pos_adj[b]
            for c in common:
                if c <= b:
                    continue
                tri = frozenset({a, b, c})
                if tri not in seen_triangles:
                    seen_triangles.add(tri)
                    triangles.append(tri)

    # Merge overlapping triangles into groups using Union-Find
    # O(triangles × α(n)) instead of O(triangles²)
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])  # path compression
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for tri in triangles:
        tri_list = list(tri)
        for i in range(len(tri_list)):
            if tri_list[i] not in parent:
                parent[tri_list[i]] = tri_list[i]
            union(tri_list[0], tri_list[i])

    # Collect groups from Union-Find
    groups_by_root: dict[str, set[str]] = defaultdict(set)
    for node in parent:
        groups_by_root[find(node)].add(node)

    compatible_groups: list[CompatibleGroup] = []
    for group_members_set in groups_by_root.values():
        members = sorted(group_members_set)
        # Compute group compatibility (avg pairwise)
        pair_scores = []
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                key = (min(a, b), max(a, b))
                if key in pair_compat:
                    pair_scores.append(pair_compat[key])

        group_compat = float(np.mean(pair_scores)) if pair_scores else 0.0

        # Find shared works across all members
        member_anime_sets = [person_anime.get(m, set()) for m in members]
        if member_anime_sets:
            group_shared = sorted(set.intersection(*member_anime_sets))
        else:
            group_shared = []

        compatible_groups.append(CompatibleGroup(
            members=members,
            group_compatibility=round(group_compat, 4),
            shared_works=group_shared[:20],
        ))

    compatible_groups.sort(key=lambda g: g.group_compatibility, reverse=True)

    # Bridge persons: appear in multiple groups with high compatibility
    person_group_count: dict[str, int] = defaultdict(int)
    person_group_compat: dict[str, list[float]] = defaultdict(list)
    for group in compatible_groups:
        for m in group.members:
            person_group_count[m] += 1
            person_group_compat[m].append(group.group_compatibility)

    bridge_persons: dict[str, float] = {}
    for pid, count in person_group_count.items():
        if count >= 2:
            avg_compat = float(np.mean(person_group_compat[pid]))
            bridge_persons[pid] = round(avg_compat * np.log1p(count), 4)

    # Person-level compatibility boost (from all positive pairs)
    person_boost: dict[str, float] = defaultdict(float)
    person_boost_count: dict[str, int] = defaultdict(int)
    for pair in compatible_pairs:
        if pair.compatibility_score > 0:
            person_boost[pair.person_a] += pair.compatibility_score
            person_boost[pair.person_b] += pair.compatibility_score
            person_boost_count[pair.person_a] += 1
            person_boost_count[pair.person_b] += 1

    person_compatibility_boost = {
        pid: round(total / person_boost_count[pid], 4)
        for pid, total in person_boost.items()
        if person_boost_count[pid] > 0
    }

    logger.info(
        "compatibility_groups_computed",
        pairs=len(compatible_pairs),
        positive_pairs=sum(1 for p in compatible_pairs if p.compatibility_score > 0),
        groups=len(compatible_groups),
        bridges=len(bridge_persons),
    )

    return GroupCompatibilityResult(
        compatible_pairs=[asdict(p) for p in compatible_pairs[:500]],
        compatible_groups=[asdict(g) for g in compatible_groups[:100]],
        bridge_persons=bridge_persons,
        person_compatibility_boost=person_compatibility_boost,
        total_pairs_analyzed=len(candidate_pairs),
    )

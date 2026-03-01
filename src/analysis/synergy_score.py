"""シナジースコア — シークエルチェーン内の上級職繰り返しペアリング効果.

上級職（COOCCURRENCE_ROLES: 10ロール）が同一フランチャイズ（シークエルチェーン）内で
繰り返し共同制作する場合の品質向上を定量化する。

スコアリングルール:
- 1回目: 0.0（後にペア継続確認時、遡及フラグ付与）
- 2回目+: log1p(n) / log1p(2) × 0.3 × quality_factor（統一対数公式、n=2で0.3×q）
- quality_factor: チェーン内のアニメスコア推移 1.0 + (latest - first) / max(first, 1.0), clamp [0.0, 2.0]
- グループシナジー: トリオ→1.1×、カルテット+→1.2×
"""

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field

import structlog

from src.analysis.cooccurrence_groups import COOCCURRENCE_ROLES
from src.models import Anime, Credit

logger = structlog.get_logger()

# Relation types that link sequel chains
_CHAIN_RELATIONS = frozenset({"SEQUEL", "PREQUEL", "PARENT", "SIDE_STORY"})

# Normalization constant for unified synergy formula: log1p(n) / _LOG1P_2 * 0.3 * quality
# At n=2: log1p(2)/log1p(2) * 0.3 = 0.3 (matches previous special case exactly)
# At n=3: log1p(3)/log1p(2) ≈ 1.262 → 0.379 (smooth, no discontinuity)
_LOG1P_2 = math.log1p(2)


@dataclass
class PairHistory:
    """Tracks collaboration history for a senior staff pair within a chain."""

    anime_ids: list[str] = field(default_factory=list)
    collab_count: int = 0


@dataclass
class PairSynergyResult:
    """Synergy result for a single pair."""

    members: list[str]
    synergy_value: float
    collaboration_count: int
    chain_anime_ids: list[str]
    franchise_title: str = ""


@dataclass
class PersonSynergyBoost:
    """Aggregated synergy boost for a single person."""

    total_synergy: float
    pair_count: int
    top_pairs: list[dict]


def _build_sequel_chains(anime_map: dict[str, Anime]) -> list[list[str]]:
    """Build sequel chains using Union-Find on anime relations.

    Parses relations_json for each anime and groups connected anime
    (via SEQUEL, PREQUEL, PARENT, SIDE_STORY) into chains sorted by year.

    Args:
        anime_map: anime_id → Anime

    Returns:
        List of chains, each chain is a list of anime_ids sorted by year.
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Initialize all anime
    for aid in anime_map:
        parent[aid] = aid

    # Parse relations and union connected anime
    for aid, anime in anime_map.items():
        if not anime.relations_json:
            continue
        try:
            relations = json.loads(anime.relations_json)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(relations, list):
            continue

        for rel in relations:
            rel_type = rel.get("relation_type", "")
            related_id = rel.get("related_anime_id", "")
            if rel_type in _CHAIN_RELATIONS and related_id in anime_map:
                union(aid, related_id)

    # Group by root
    components: dict[str, list[str]] = defaultdict(list)
    for aid in anime_map:
        components[find(aid)].append(aid)

    # Filter to chains with 2+ anime and sort by year
    chains = []
    for members in components.values():
        if len(members) < 2:
            continue
        sorted_chain = sorted(
            members,
            key=lambda a: anime_map[a].year or 9999,
        )
        chains.append(sorted_chain)

    logger.info("sequel_chains_built", chains=len(chains))
    return chains


def _extract_senior_staff(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict[str, set[str]]]:
    """Extract senior staff (COOCCURRENCE_ROLES) per anime.

    Args:
        credits: all credits
        anime_map: anime_id → Anime

    Returns:
        {anime_id: {person_id: set of role values}}
    """
    staff: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for c in credits:
        if c.role in COOCCURRENCE_ROLES and c.anime_id in anime_map:
            staff[c.anime_id][c.person_id].add(c.role.value)
    return dict(staff)


def _track_pair_occurrences(
    chains: list[list[str]],
    staff_map: dict[str, dict[str, set[str]]],
) -> dict[frozenset[str], list[PairHistory]]:
    """Track how many times each senior staff pair co-occurs within each chain.

    Args:
        chains: list of sequel chains (sorted anime_id lists)
        staff_map: anime_id → {person_id → roles}

    Returns:
        {frozenset(pid_a, pid_b) → [PairHistory per chain]}
    """
    pair_histories: dict[frozenset[str], list[PairHistory]] = defaultdict(list)

    for chain in chains:
        # Track pairs within this chain
        chain_pair_anime: dict[frozenset[str], list[str]] = defaultdict(list)

        for anime_id in chain:
            persons_in_anime = staff_map.get(anime_id, {})
            person_ids = sorted(persons_in_anime.keys())

            for i, pid_a in enumerate(person_ids):
                for pid_b in person_ids[i + 1 :]:
                    pair_key = frozenset({pid_a, pid_b})
                    chain_pair_anime[pair_key].append(anime_id)

        # Only keep pairs with 2+ collaborations in this chain
        for pair_key, anime_ids in chain_pair_anime.items():
            if len(anime_ids) >= 2:
                history = PairHistory(
                    anime_ids=anime_ids,
                    collab_count=len(anime_ids),
                )
                pair_histories[pair_key].append(history)

    return dict(pair_histories)


def _compute_quality_factor(
    anime_ids: list[str],
    anime_map: dict[str, Anime],
) -> float:
    """Compute quality factor from anime score trajectory within a chain.

    quality_factor = 1.0 + (latest_score - first_score) / max(first_score, 1.0)
    Clamped to [0.0, 2.0].

    Args:
        anime_ids: ordered anime IDs in the chain
        anime_map: anime_id → Anime

    Returns:
        Quality factor (0.0 to 2.0)
    """
    scores = []
    for aid in anime_ids:
        anime = anime_map.get(aid)
        if anime and anime.score is not None:
            scores.append(anime.score)

    if len(scores) < 2:
        return 1.0

    first = scores[0]
    latest = scores[-1]
    raw = 1.0 + (latest - first) / max(first, 1.0)
    return max(0.0, min(2.0, raw))


def _compute_pair_synergy(
    history: PairHistory,
    anime_map: dict[str, Anime],
) -> float:
    """Compute synergy score for a single pair within one chain.

    Uses a unified formula for all n >= 2:
        log1p(n) / log1p(2) × 0.3 × quality_factor

    At n=2 this yields exactly 0.3 × quality (matching the original n=2 case).
    At n=3 this yields ~0.379 × quality (smooth transition, no discontinuity).

    Args:
        history: PairHistory for the pair in one chain
        anime_map: anime_id → Anime

    Returns:
        Synergy score for this pair-chain combination
    """
    n = history.collab_count
    if n < 2:
        return 0.0

    quality = _compute_quality_factor(history.anime_ids, anime_map)

    return math.log1p(n) / _LOG1P_2 * 0.3 * quality


def _compute_group_synergy(
    members: frozenset[str],
    pair_synergies: dict[frozenset[str], float],
) -> float:
    """Apply group multiplier when 3+ members share synergy.

    Trio (3 members): 1.1× multiplier on sum of constituent pair synergies
    Quartet+ (4+ members): 1.2× multiplier

    Args:
        members: set of person IDs in the group
        pair_synergies: all pair synergy scores

    Returns:
        Group synergy bonus (sum of pair synergies × group multiplier)
    """
    member_list = sorted(members)
    total = 0.0
    for i, a in enumerate(member_list):
        for b in member_list[i + 1 :]:
            pair_key = frozenset({a, b})
            total += pair_synergies.get(pair_key, 0.0)

    n = len(members)
    if n >= 4:
        return total * 1.2
    if n >= 3:
        return total * 1.1
    return total


def _aggregate_person_synergy(
    pair_synergies: dict[frozenset[str], float],
    anime_map: dict[str, Anime],
    pair_histories: dict[frozenset[str], list[PairHistory]],
) -> dict[str, PersonSynergyBoost]:
    """Aggregate pair-level synergy into per-person boosts.

    Each person's total_synergy = sum of all pair synergies they participate in.

    Args:
        pair_synergies: frozenset(pid_a, pid_b) → synergy value
        anime_map: anime_id → Anime (for franchise titles)
        pair_histories: pair → chain histories (for collab counts)

    Returns:
        {person_id → PersonSynergyBoost}
    """
    person_pairs: dict[str, list[tuple[frozenset[str], float]]] = defaultdict(list)

    for pair_key, synergy in pair_synergies.items():
        if synergy <= 0:
            continue
        for pid in pair_key:
            person_pairs[pid].append((pair_key, synergy))

    result: dict[str, PersonSynergyBoost] = {}
    for pid, pairs in person_pairs.items():
        # Sort by synergy value descending
        pairs.sort(key=lambda x: x[1], reverse=True)

        top_pairs = []
        for pair_key, synergy in pairs[:5]:
            partner_id = [p for p in pair_key if p != pid][0]
            # Get first chain's anime for franchise title
            histories = pair_histories.get(pair_key, [])
            franchise = ""
            total_collabs = 0
            if histories:
                first_anime_id = histories[0].anime_ids[0]
                anime = anime_map.get(first_anime_id)
                if anime:
                    franchise = anime.display_title
                total_collabs = sum(h.collab_count for h in histories)
            top_pairs.append({
                "partner_id": partner_id,
                "synergy_value": round(synergy, 4),
                "collaboration_count": total_collabs,
                "franchise_title": franchise,
            })

        result[pid] = PersonSynergyBoost(
            total_synergy=round(sum(s for _, s in pairs), 4),
            pair_count=len(pairs),
            top_pairs=top_pairs,
        )

    return result


def compute_synergy_scores(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """Compute synergy scores for senior staff repeated pairings in sequel chains.

    Main API for the synergy score module. Returns a dict suitable for JSON export.

    Args:
        credits: all credits
        anime_map: anime_id → Anime

    Returns:
        {
            "person_synergy_boosts": {pid: {total_synergy, pair_count, top_pairs}},
            "top_synergy_pairs": [...],
            "summary": {total_pairs_tracked, synergy_active_pairs, franchise_count},
        }
    """
    # Step 1: Build sequel chains
    chains = _build_sequel_chains(anime_map)
    if not chains:
        logger.info("synergy_no_chains_found")
        return {
            "person_synergy_boosts": {},
            "top_synergy_pairs": [],
            "summary": {
                "total_pairs_tracked": 0,
                "synergy_active_pairs": 0,
                "franchise_count": 0,
            },
        }

    # Step 2: Extract senior staff per anime
    staff_map = _extract_senior_staff(credits, anime_map)

    # Step 3: Track pair occurrences across chains
    pair_histories = _track_pair_occurrences(chains, staff_map)

    # Step 4: Compute pair synergy scores
    pair_synergies: dict[frozenset[str], float] = {}
    for pair_key, histories in pair_histories.items():
        # Sum synergy across all chains where the pair co-occurs
        total = sum(
            _compute_pair_synergy(h, anime_map) for h in histories
        )
        pair_synergies[pair_key] = total

    # Step 5: Detect groups (3+ members with mutual pairwise synergy)
    # Build adjacency from synergy pairs
    adjacency: dict[str, set[str]] = defaultdict(set)
    for pair_key, synergy in pair_synergies.items():
        if synergy > 0:
            members = list(pair_key)
            adjacency[members[0]].add(members[1])
            adjacency[members[1]].add(members[0])

    # Find cliques of size 3+ (simple greedy: check triangles)
    groups_found: list[frozenset[str]] = []
    seen: set[frozenset[str]] = set()
    for pid_a, neighbors_a in adjacency.items():
        for pid_b in neighbors_a:
            if pid_b <= pid_a:
                continue
            # Find common neighbors → triangles
            common = neighbors_a & adjacency.get(pid_b, set())
            for pid_c in common:
                if pid_c <= pid_b:
                    continue
                trio = frozenset({pid_a, pid_b, pid_c})
                if trio not in seen:
                    seen.add(trio)
                    groups_found.append(trio)
                    # Check for quartet extension
                    common_abc = common & adjacency.get(pid_c, set())
                    for pid_d in common_abc:
                        if pid_d <= pid_c:
                            continue
                        quartet = frozenset({pid_a, pid_b, pid_c, pid_d})
                        if quartet not in seen:
                            seen.add(quartet)
                            groups_found.append(quartet)

    # Apply group multipliers to constituent pairs
    boosted_pairs: set[frozenset[str]] = set()
    for group in groups_found:
        multiplier = 1.1 if len(group) == 3 else 1.2
        member_list = sorted(group)
        for i, a in enumerate(member_list):
            for b in member_list[i + 1 :]:
                pair_key = frozenset({a, b})
                if pair_key in pair_synergies and pair_key not in boosted_pairs:
                    pair_synergies[pair_key] *= multiplier
                    boosted_pairs.add(pair_key)

    # Step 6: Aggregate person-level synergy
    person_boosts = _aggregate_person_synergy(
        pair_synergies, anime_map, pair_histories
    )

    # Step 7: Build top synergy pairs output
    active_pairs = {k: v for k, v in pair_synergies.items() if v > 0}
    top_pairs_list = []
    for pair_key, synergy in sorted(
        active_pairs.items(), key=lambda x: x[1], reverse=True
    )[:100]:
        members = sorted(pair_key)
        histories = pair_histories.get(pair_key, [])
        total_collabs = sum(h.collab_count for h in histories)
        franchise = ""
        chain_ids = []
        if histories:
            chain_ids = histories[0].anime_ids
            first_anime = anime_map.get(chain_ids[0])
            if first_anime:
                franchise = first_anime.display_title

        top_pairs_list.append({
            "members": members,
            "synergy_value": round(synergy, 4),
            "collaboration_count": total_collabs,
            "franchise_title": franchise,
            "chain_anime_ids": chain_ids,
        })

    # Convert PersonSynergyBoost to dicts for JSON
    person_boosts_dict = {}
    for pid, boost in person_boosts.items():
        person_boosts_dict[pid] = {
            "total_synergy": boost.total_synergy,
            "pair_count": boost.pair_count,
            "top_pairs": boost.top_pairs,
        }

    result = {
        "person_synergy_boosts": person_boosts_dict,
        "top_synergy_pairs": top_pairs_list,
        "summary": {
            "total_pairs_tracked": len(pair_histories),
            "synergy_active_pairs": len(active_pairs),
            "franchise_count": len(chains),
        },
    }

    logger.info(
        "synergy_scores_computed",
        franchises=len(chains),
        pairs_tracked=len(pair_histories),
        active_pairs=len(active_pairs),
        persons_with_synergy=len(person_boosts),
    )

    return result

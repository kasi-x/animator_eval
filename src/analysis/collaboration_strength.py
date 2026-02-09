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

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_collaboration_strength(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared: int = 2,
    person_scores: dict[str, float] | None = None,
) -> list[dict]:
    """人物ペア間のコラボレーション強度を算出する.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        min_shared: 最低共同作品数 (これ未満のペアは除外)
        person_scores: {person_id: composite_score} (optional)

    Returns:
        list of {person_a, person_b, shared_works, shared_anime, role_pairs,
                 first_year, latest_year, longevity, strength_score}
    """
    # Build anime → [(person_id, role)] mapping
    anime_staff: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for c in credits:
        anime_staff[c.anime_id].append((c.person_id, c.role.value))

    # Count shared works per pair
    pair_data: dict[tuple[str, str], dict] = {}

    for anime_id, staff in anime_staff.items():
        anime = anime_map.get(anime_id)
        year = anime.year if anime else None

        persons = list({pid for pid, _ in staff})
        for i, pid_a in enumerate(persons):
            for pid_b in persons[i + 1:]:
                key = (min(pid_a, pid_b), max(pid_a, pid_b))
                if key not in pair_data:
                    pair_data[key] = {
                        "anime_ids": set(),
                        "years": [],
                        "role_pairs": [],
                    }
                pair_data[key]["anime_ids"].add(anime_id)
                if year:
                    pair_data[key]["years"].append(year)

                # Record role combinations for this anime
                roles_a = {r for p, r in staff if p == pid_a}
                roles_b = {r for p, r in staff if p == pid_b}
                for ra in roles_a:
                    for rb in roles_b:
                        pair_data[key]["role_pairs"].append(f"{ra}+{rb}")

    # Filter by min_shared and compute metrics
    results = []
    max_shared = max(
        (len(d["anime_ids"]) for d in pair_data.values()), default=1
    )

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
        top_role_pairs = sorted(
            role_pair_counts.items(), key=lambda x: -x[1]
        )[:5]

        # Strength score: combination of frequency, longevity, recency
        freq_component = min(shared_count / max(max_shared, 1), 1.0) * 50
        longevity_component = min(longevity / 10, 1.0) * 30
        recency_component = 20 if latest_year and latest_year >= 2020 else 10 if latest_year and latest_year >= 2015 else 0
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

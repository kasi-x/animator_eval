"""Team composition analysis — analyse staff composition patterns for anime works.

成功作品のチーム構成パターンを特定し、
個人の貢献がどのようなチーム文脈で発揮されるかを可視化する。
"""

from collections import defaultdict

import structlog

from src.runtime.models import AnimeAnalysis as Anime, Credit
from src.utils.role_groups import CORE_TEAM_ROLES as CORE_ROLES

logger = structlog.get_logger()


def analyze_team_patterns(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
    min_score: float = 7.0,
    min_staff: int = 15,
) -> dict:
    """Analyse team composition patterns for large-scale productions.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: {person_id: composite_score}
        min_score: 未使用 (後方互換のため残存)
        min_staff: 「大規模制作」判定の最低スタッフ数

    Returns:
        {
            "large_teams": [...],  # 大規模制作のチーム構成
            "role_combinations": {...},  # 頻出パターンの役職組み合わせ
            "recommended_pairs": [...],  # 推薦ペア
            "team_size_stats": {...},    # チームサイズの統計
        }
    """
    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    # Classify anime by team size
    large_teams = []
    all_teams = []

    for anime_id, team_credits in anime_credits.items():
        anime = anime_map.get(anime_id)
        if not anime:
            continue

        # Team composition
        roles: dict[str, list[str]] = defaultdict(list)
        for c in team_credits:
            roles[c.role.value].append(c.person_id)

        team_size = len({c.person_id for c in team_credits})
        core_count = len({c.person_id for c in team_credits if c.role in CORE_ROLES})

        team_entry = {
            "anime_id": anime_id,
            "title": anime.display_title,
            "year": anime.year,
            "team_size": team_size,
            "core_roles": core_count,
            "roles": {r: sorted(set(pids)) for r, pids in roles.items()},
        }

        if person_scores:
            team_scores = [
                person_scores[c.person_id]
                for c in team_credits
                if c.person_id in person_scores
            ]
            if team_scores:
                team_entry["avg_person_score"] = round(
                    sum(team_scores) / len(team_scores), 2
                )

        all_teams.append(team_entry)

        if team_size >= min_staff:
            large_teams.append(team_entry)

    # Sort by team size
    large_teams.sort(key=lambda x: x["team_size"], reverse=True)

    # Team size statistics
    sizes = [t["team_size"] for t in all_teams]
    team_size_stats = {}
    if sizes:
        team_size_stats = {
            "min": min(sizes),
            "max": max(sizes),
            "avg": round(sum(sizes) / len(sizes), 1),
            "high_score_avg": round(
                sum(t["team_size"] for t in large_teams) / max(len(large_teams), 1),
                1,
            ),
        }

    # Find frequent role co-occurrences in large-scale works
    role_pair_freq: dict[str, int] = defaultdict(int)
    for team in large_teams:
        role_keys = sorted(team["roles"].keys())
        for i, r1 in enumerate(role_keys):
            for r2 in role_keys[i + 1 :]:
                role_pair_freq[f"{r1}+{r2}"] += 1

    top_combos = sorted(role_pair_freq.items(), key=lambda x: -x[1])[:20]

    # Find recommended pairs (persons who frequently appear together in large teams)
    pair_count: dict[tuple[str, str], int] = defaultdict(int)
    for team in large_teams:
        all_pids = sorted(
            {c.person_id for c in anime_credits.get(team["anime_id"], [])}
        )
        for i, a in enumerate(all_pids):
            for b in all_pids[i + 1 :]:
                pair_count[(a, b)] += 1

    recommended_pairs = []
    for (a, b), count in sorted(pair_count.items(), key=lambda x: -x[1])[:30]:
        if count >= 2:
            entry: dict = {
                "person_a": a,
                "person_b": b,
                "shared_large_team_works": count,
            }
            if person_scores:
                sa = person_scores.get(a)
                sb = person_scores.get(b)
                if sa is not None and sb is not None:
                    entry["combined_score"] = round((sa + sb) / 2, 2)
            recommended_pairs.append(entry)

    top_large_teams = large_teams[:50]
    total_large = len(large_teams)
    result = {
        # New naming (size-based semantics)
        "large_teams": top_large_teams,
        "total_large_teams": total_large,
        # Backward-compatible naming used by API/CLI/reports/tests
        "high_score_teams": top_large_teams,
        "total_high_score": total_large,
        "role_combinations": [
            {"roles": combo, "count": cnt} for combo, cnt in top_combos
        ],
        "recommended_pairs": recommended_pairs,
        "team_size_stats": team_size_stats,
    }

    logger.info(
        "team_patterns_analyzed",
        total_teams=len(all_teams),
        large_teams=len(large_teams),
    )
    return result

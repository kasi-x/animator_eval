"""Recommendation engine — recommend personnel based on team composition.

既存チームメンバーのスコアプロファイルを分析し、
相補的な人材を推薦する。
"""

from collections import defaultdict

import structlog

from src.models import Credit

logger = structlog.get_logger()


def recommend_for_team(
    team_person_ids: list[str],
    results: list[dict],
    credits: list[Credit],
    top_n: int = 10,
) -> list[dict]:
    """Recommend complementary personnel for a team.

    Args:
        team_person_ids: 既存チームメンバーのID
        results: 全スコア結果リスト
        credits: 全クレジット
        top_n: 推薦件数

    Returns:
        [{person_id, name, score, compatibility_score, reasons}]
    """
    if not team_person_ids or not results:
        return []

    scores_map = {r["person_id"]: r for r in results}
    team_set = set(team_person_ids)

    # Team profile: average scores per axis
    team_scores = [scores_map[pid] for pid in team_person_ids if pid in scores_map]
    if not team_scores:
        return []

    axes = ("person_fe", "birank", "patronage")
    team_avg = {
        axis: sum(r.get(axis, 0) for r in team_scores) / len(team_scores)
        for axis in axes
    }

    # Find shared anime between team members
    person_anime: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        person_anime[c.person_id].add(c.anime_id)

    team_anime = set()
    for pid in team_person_ids:
        team_anime.update(person_anime.get(pid, set()))

    # Candidate scoring
    candidates = []
    for r in results:
        pid = r["person_id"]
        if pid in team_set:
            continue

        # Complementarity: higher in team's weakest axis
        weakest_axis = min(axes, key=lambda a: team_avg[a])
        complement_bonus = r.get(weakest_axis, 0) / 100 * 30

        # Collaboration history: has worked with team members before
        cand_anime = person_anime.get(pid, set())
        shared = len(cand_anime & team_anime)
        collab_bonus = min(shared * 5, 30)

        # Base quality
        quality = r.get("iv_score", 0) / 100 * 40

        compatibility = round(quality + complement_bonus + collab_bonus, 2)

        reasons = []
        if complement_bonus > 15:
            reasons.append(f"Strong in team's weakest axis ({weakest_axis})")
        if shared > 0:
            reasons.append(f"Collaborated on {shared} shared projects")
        if r.get("growth", {}).get("trend") == "rising":
            reasons.append("Rising trend")

        candidates.append(
            {
                "person_id": pid,
                "name": r.get("name", "") or r.get("name_ja", "") or pid,
                "iv_score": r.get("iv_score", 0),
                "compatibility_score": compatibility,
                "shared_projects": shared,
                "reasons": reasons,
            }
        )

    candidates.sort(key=lambda x: x["compatibility_score"], reverse=True)

    logger.info(
        "recommendations_computed",
        team_size=len(team_person_ids),
        candidates=len(candidates[:top_n]),
    )
    return candidates[:top_n]

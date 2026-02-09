"""監督サークル分析 — 監督ごとの常連アニメーターグループを特定する.

「監督サークル」とは、特定の監督と頻繁に共演するアニメーターの集合。
アニメ業界では、監督が信頼するスタッフを繰り返し起用する傾向があり、
これがネットワーク上の密なクラスターとして現れる。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit, Role

logger = structlog.get_logger()

DIRECTOR_ROLES = {
    Role.DIRECTOR,
    Role.EPISODE_DIRECTOR,
    Role.CHIEF_ANIMATION_DIRECTOR,
}

ANIMATOR_ROLES = {
    Role.ANIMATION_DIRECTOR,
    Role.KEY_ANIMATOR,
    Role.SECOND_KEY_ANIMATOR,
    Role.IN_BETWEEN,
    Role.CHARACTER_DESIGNER,
    Role.STORYBOARD,
    Role.LAYOUT,
    Role.EFFECTS,
}


def find_director_circles(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
    min_director_works: int = 3,
) -> dict[str, dict]:
    """監督サークルを特定する.

    Args:
        credits: クレジットデータ
        anime_map: anime_id → Anime
        min_shared_works: サークルメンバー認定に必要な最低共演作品数
        min_director_works: 分析対象となる監督の最低作品数

    Returns:
        {director_id: {
            "director_name": str,
            "total_works": int,
            "members": [{
                "person_id": str,
                "shared_works": int,
                "hit_rate": float,  # 共演率 (shared / director's total)
                "roles": [str],
                "latest_year": int | None,
            }]
        }}
    """
    # 監督ごとの作品を特定
    director_works: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            director_works[c.person_id].add(c.anime_id)

    # 作品ごとのスタッフ
    anime_staff: dict[str, list[tuple[str, Role]]] = defaultdict(list)
    for c in credits:
        if c.role in ANIMATOR_ROLES:
            anime_staff[c.anime_id].append((c.person_id, c.role))

    circles: dict[str, dict] = {}

    for dir_id, dir_anime_ids in director_works.items():
        if len(dir_anime_ids) < min_director_works:
            continue

        # この監督の各作品に参加したアニメーター
        member_stats: dict[str, dict] = defaultdict(
            lambda: {"shared_works": 0, "roles": set(), "latest_year": None}
        )

        for anime_id in dir_anime_ids:
            anime = anime_map.get(anime_id)
            year = anime.year if anime else None

            for person_id, role in anime_staff.get(anime_id, []):
                if person_id == dir_id:
                    continue
                stats = member_stats[person_id]
                stats["shared_works"] += 1
                stats["roles"].add(role.value)
                if year and (stats["latest_year"] is None or year > stats["latest_year"]):
                    stats["latest_year"] = year

        # min_shared_works 以上の共演者をサークルメンバーとする
        members = []
        total = len(dir_anime_ids)
        for pid, stats in member_stats.items():
            if stats["shared_works"] >= min_shared_works:
                members.append({
                    "person_id": pid,
                    "shared_works": stats["shared_works"],
                    "hit_rate": round(stats["shared_works"] / total, 3),
                    "roles": sorted(stats["roles"]),
                    "latest_year": stats["latest_year"],
                })

        if members:
            members.sort(key=lambda m: m["shared_works"], reverse=True)
            circles[dir_id] = {
                "total_works": total,
                "members": members,
            }

    logger.info(
        "director_circles_found",
        directors=len(circles),
        total_members=sum(len(c["members"]) for c in circles.values()),
    )
    return circles


def get_person_circles(
    person_id: str,
    circles: dict[str, dict],
) -> list[dict]:
    """特定人物が属する監督サークルを返す.

    Returns:
        [{"director_id": str, "shared_works": int, "hit_rate": float, ...}]
    """
    result = []
    for dir_id, circle in circles.items():
        for member in circle["members"]:
            if member["person_id"] == person_id:
                result.append({
                    "director_id": dir_id,
                    "director_total_works": circle["total_works"],
                    **member,
                })
                break

    result.sort(key=lambda x: x["shared_works"], reverse=True)
    return result

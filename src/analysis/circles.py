"""監督サークル分析 — 監督ごとの常連アニメーターグループを特定する.

「監督サークル」とは、特定の監督と頻繁に共演するアニメーターの集合。
アニメ業界では、監督が信頼するスタッフを繰り返し起用する傾向があり、
これがネットワーク上の密なクラスターとして現れる。
"""

from collections import defaultdict
from dataclasses import dataclass, field

import structlog

from src.models import Anime, Credit, Role
from src.utils.role_groups import DIRECTOR_ROLES, ANIMATOR_ROLES

logger = structlog.get_logger()


@dataclass
class AnimatorInDirectorsCircle:
    """監督サークルに属するアニメーターのメンバー情報.

    Represents an animator who frequently collaborates with a specific director.
    """

    person_id: str
    shared_works: int
    hit_rate: float  # 共演率 (shared / director's total)
    roles: list[str]
    latest_year: int | None


@dataclass
class DirectorCircle:
    """監督とその常連メンバー集団.

    Represents a director and their circle of frequent collaborators.
    """

    total_works: int
    members: list[AnimatorInDirectorsCircle] = field(default_factory=list)


@dataclass
class PersonCircleMembership:
    """特定人物が属する監督サークルの情報.

    Represents a person's membership in a director's circle.
    """

    director_id: str
    director_total_works: int
    person_id: str
    shared_works: int
    hit_rate: float
    roles: list[str]
    latest_year: int | None


def find_director_circles(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
    min_director_works: int = 3,
) -> dict[str, DirectorCircle]:
    """監督サークルを特定する.

    Identifies circles of animators who frequently collaborate with specific directors.

    Args:
        credits: クレジットデータ / Credit records
        anime_map: anime_id → Anime
        min_shared_works: サークルメンバー認定に必要な最低共演作品数 / Min shared works for membership
        min_director_works: 分析対象となる監督の最低作品数 / Min director works for analysis

    Returns:
        {director_id: DirectorCircle}
    """
    # 監督ごとの作品を特定
    works_by_each_director: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            works_by_each_director[c.person_id].add(c.anime_id)

    # 作品ごとのスタッフ
    staff_per_anime: dict[str, list[tuple[str, Role]]] = defaultdict(list)
    for c in credits:
        if c.role in ANIMATOR_ROLES:
            staff_per_anime[c.anime_id].append((c.person_id, c.role))

    circles_by_director_id: dict[str, DirectorCircle] = {}

    for director_id, anime_ids_directed in works_by_each_director.items():
        if len(anime_ids_directed) < min_director_works:
            continue

        # この監督の各作品に参加したアニメーター
        collaborator_statistics: dict[str, dict] = defaultdict(
            lambda: {"shared_works": 0, "roles": set(), "latest_year": None}
        )

        for anime_id in anime_ids_directed:
            anime = anime_map.get(anime_id)
            year = anime.year if anime else None

            for person_id, role in staff_per_anime.get(anime_id, []):
                if person_id == director_id:
                    continue
                stats = collaborator_statistics[person_id]
                stats["shared_works"] += 1
                stats["roles"].add(role.value)
                if year and (
                    stats["latest_year"] is None or year > stats["latest_year"]
                ):
                    stats["latest_year"] = year

        # min_shared_works 以上の共演者をサークルメンバーとする
        circle_members = []
        total_director_works = len(anime_ids_directed)
        for person_id, stats in collaborator_statistics.items():
            if stats["shared_works"] >= min_shared_works:
                member = AnimatorInDirectorsCircle(
                    person_id=person_id,
                    shared_works=stats["shared_works"],
                    hit_rate=round(stats["shared_works"] / total_director_works, 3),
                    roles=sorted(stats["roles"]),
                    latest_year=stats["latest_year"],
                )
                circle_members.append(member)

        if circle_members:
            circle_members.sort(key=lambda m: m.shared_works, reverse=True)
            circles_by_director_id[director_id] = DirectorCircle(
                total_works=total_director_works,
                members=circle_members,
            )

    logger.info(
        "director_circles_found",
        directors=len(circles_by_director_id),
        total_members=sum(
            len(circle.members) for circle in circles_by_director_id.values()
        ),
    )
    return circles_by_director_id


def get_person_circles(
    person_id: str,
    circles: dict[str, DirectorCircle],
) -> list[PersonCircleMembership]:
    """特定人物が属する監督サークルを返す.

    Returns the list of director circles a person belongs to.

    Args:
        person_id: 人物ID / Person ID to look up
        circles: 監督サークル辞書 / Director circles dictionary

    Returns:
        List of PersonCircleMembership, sorted by shared_works descending
    """
    memberships = []
    for director_id, circle in circles.items():
        for member in circle.members:
            if member.person_id == person_id:
                membership = PersonCircleMembership(
                    director_id=director_id,
                    director_total_works=circle.total_works,
                    person_id=member.person_id,
                    shared_works=member.shared_works,
                    hit_rate=member.hit_rate,
                    roles=member.roles,
                    latest_year=member.latest_year,
                )
                memberships.append(membership)
                break

    memberships.sort(key=lambda x: x.shared_works, reverse=True)
    return memberships

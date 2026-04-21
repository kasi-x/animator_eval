"""共同制作集団分析 — コアスタッフの繰り返し共同制作パターン検出.

ペアワイズ（2人）の協業エッジでは見えない「事実上のチーム」を検出する。
3人以上が複数作品で繰り返し共同制作するパターンから、非公式の固定チーム・
企業集団・監督のコアスタッフを可視化する。
"""

from collections import defaultdict
from itertools import combinations

import structlog

from src.models import AnimeAnalysis as Anime, Credit, Role

logger = structlog.get_logger()

# 分析対象ロール: コアスタッフのみ（変動の大きい役職は除外）
COOCCURRENCE_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.SCREENPLAY,
        Role.CHARACTER_DESIGNER,
        Role.ANIMATION_DIRECTOR,
        Role.BACKGROUND_ART,
        Role.FINISHING,
        Role.SOUND_DIRECTOR,
        Role.PHOTOGRAPHY_DIRECTOR,
        Role.CGI_DIRECTOR,
    }
)

# temporal_slices の区切り
_PERIODS = [
    ("〜1999", None, 1999),
    ("2000-2004", 2000, 2004),
    ("2005-2009", 2005, 2009),
    ("2010-2014", 2010, 2014),
    ("2015-2019", 2015, 2019),
    ("2020-", 2020, None),
]

_ACTIVE_THRESHOLD_YEAR = 2022  # この年以降に活動があれば is_active=True


def compute_cooccurrence_groups(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    iv_scores: dict[str, float] | None = None,
    min_shared_works: int = 3,
    max_group_size: int = 5,
) -> dict:
    """コアスタッフの共同制作グループを検出する.

    Args:
        credits: 全クレジットリスト
        anime_map: {anime_id: Anime}
        iv_scores: {person_id: composite_score} (任意)
        min_shared_works: グループが共参加する最低作品数
        max_group_size: 検出するグループの最大サイズ（3〜max）

    Returns:
        {
            "groups": [...],
            "summary": {...},
            "temporal_slices": [...],
            "params": {...},
        }
    """
    if iv_scores is None:
        iv_scores = {}

    # Step 1: コアスタッフのクレジットのみ抽出
    core_credits = [c for c in credits if c.role in COOCCURRENCE_ROLES]
    logger.info(
        "cooccurrence_filter",
        total_credits=len(credits),
        core_credits=len(core_credits),
    )

    if not core_credits:
        return _empty_result(min_shared_works, max_group_size)

    # Step 2: anime_id → {person_id → set[role]} のマップ構築
    anime_to_staff: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for c in core_credits:
        anime_to_staff[c.anime_id][c.person_id].add(c.role.value)

    # Step 3: 全k-組み合わせ (k=3..max_group_size) を共起カウント
    # group_counts: frozenset[person_id] → list[anime_id]
    group_counts: dict[frozenset, list[str]] = defaultdict(list)

    # Cap staff per anime to avoid combinatorial explosion:
    # C(n,5) grows as O(n^5), so n=30 → 142K combos, n=60 → 5.5M.
    # Keep top-scored staff when exceeding the cap.
    _MAX_STAFF_PER_ANIME = 20

    for anime_id, staff_roles in anime_to_staff.items():
        persons = list(staff_roles.keys())
        if len(persons) < 3:
            continue
        if len(persons) > _MAX_STAFF_PER_ANIME:
            # Keep top staff by IV score (or arbitrary if no scores)
            persons.sort(key=lambda p: iv_scores.get(p, 0.0), reverse=True)
            persons = persons[:_MAX_STAFF_PER_ANIME]
        for k in range(3, min(max_group_size, len(persons)) + 1):
            for combo in combinations(persons, k):
                group_counts[frozenset(combo)].append(anime_id)

    # Step 4: min_shared_works でフィルタ
    filtered: list[tuple[frozenset, list[str]]] = [
        (group, anime_ids)
        for group, anime_ids in group_counts.items()
        if len(anime_ids) >= min_shared_works
    ]

    logger.info(
        "cooccurrence_groups_found",
        total_before_filter=len(group_counts),
        total_after_filter=len(filtered),
        min_shared_works=min_shared_works,
    )

    # Step 5: shared_works 降順ソート + メタデータ付与
    filtered.sort(key=lambda x: (-len(x[1]), -len(x[0])))

    groups = []
    for group_set, anime_ids in filtered:
        member_list = sorted(group_set)

        # 各メンバーの役割を集約（全共参加作品を通じた役割セット）
        roles: dict[str, list[str]] = {}
        for pid in member_list:
            member_roles: set[str] = set()
            for aid in anime_ids:
                member_roles.update(anime_to_staff[aid].get(pid, set()))
            roles[pid] = sorted(member_roles)

        # 活動期間
        years = [
            anime_map[aid].year
            for aid in anime_ids
            if aid in anime_map and anime_map[aid].year is not None
        ]
        first_year = min(years) if years else None
        last_year = max(years) if years else None
        is_active = last_year is not None and last_year >= _ACTIVE_THRESHOLD_YEAR

        # 平均IVスコア
        scores = [iv_scores[pid] for pid in member_list if pid in iv_scores]
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0

        # タイトルリスト
        shared_anime_titles = [
            anime_map[aid].display_title for aid in anime_ids if aid in anime_map
        ]

        groups.append(
            {
                "members": member_list,
                "member_names": [],  # export_and_viz で付与
                "size": len(member_list),
                "shared_works": len(anime_ids),
                "shared_anime": list(anime_ids),
                "shared_anime_titles": shared_anime_titles,
                "roles": roles,
                "first_year": first_year,
                "last_year": last_year,
                "is_active": is_active,
                "avg_iv_score": avg_score,
            }
        )

    # Step 6: サマリー
    by_size: dict[str, int] = defaultdict(int)
    for g in groups:
        by_size[str(g["size"])] += 1
    active_groups = sum(1 for g in groups if g["is_active"])

    summary = {
        "total_groups": len(groups),
        "by_size": dict(by_size),
        "active_groups": active_groups,
    }

    # Step 7: temporal_slices
    temporal_slices = _build_temporal_slices(groups)

    return {
        "groups": groups,
        "summary": summary,
        "temporal_slices": temporal_slices,
        "params": {
            "min_shared_works": min_shared_works,
            "max_group_size": max_group_size,
        },
    }


def _build_temporal_slices(groups: list[dict]) -> list[dict]:
    """5年区切りのピリオドごとにアクティブなグループ数をまとめる."""
    slices = []
    for period_label, year_from, year_to in _PERIODS:
        active_in_period = []
        for g in groups:
            fy = g["first_year"]
            ly = g["last_year"]
            if fy is None or ly is None:
                continue
            # ピリオド内に活動期間が重なるグループ
            period_start = year_from if year_from is not None else 0
            period_end = year_to if year_to is not None else 9999
            if fy <= period_end and ly >= period_start:
                active_in_period.append(g)

        # top_groups: shared_works 降順で上位5件
        top_groups = sorted(active_in_period, key=lambda g: -g["shared_works"])[:5]

        slices.append(
            {
                "period": period_label,
                "active_group_count": len(active_in_period),
                "top_groups": [
                    {
                        "members": g["members"],
                        "member_names": g["member_names"],
                        "shared_works": g["shared_works"],
                        "size": g["size"],
                    }
                    for g in top_groups
                ],
            }
        )
    return slices


def _empty_result(min_shared_works: int, max_group_size: int) -> dict:
    """空データ時のデフォルト戻り値."""
    return {
        "groups": [],
        "summary": {"total_groups": 0, "by_size": {}, "active_groups": 0},
        "temporal_slices": [
            {"period": label, "active_group_count": 0, "top_groups": []}
            for label, _, _ in _PERIODS
        ],
        "params": {
            "min_shared_works": min_shared_works,
            "max_group_size": max_group_size,
        },
    }

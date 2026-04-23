"""Role flow analysis — generate role transition flow data for Sankey diagrams.

キャリアの中で role A → role B へ遷移した人数をカウントし、
フロー（Sankey）図の入力データとして出力する。
"""

from collections import defaultdict

import structlog

from src.models import AnimeAnalysis as Anime, Credit, Role

logger = structlog.get_logger()

# Stage mapping for grouping
ROLE_STAGE = {
    Role.IN_BETWEEN: "Stage 1: 動画",
    Role.SECOND_KEY_ANIMATOR: "Stage 2: 第二原画",
    Role.LAYOUT: "Stage 2: レイアウト",
    Role.KEY_ANIMATOR: "Stage 3: 原画",
    Role.PHOTOGRAPHY_DIRECTOR: "Stage 3: 撮影",
    Role.CHARACTER_DESIGNER: "Stage 4: キャラデザ",
    Role.ANIMATION_DIRECTOR: "Stage 5: 作画監督",
    Role.EPISODE_DIRECTOR: "Stage 5: 演出",
    Role.DIRECTOR: "Stage 6: 監督",
}


def compute_role_flow(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """Generate role transition flow data.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        {
            "nodes": [{"id": str, "label": str}],
            "links": [{"source": str, "target": str, "value": int}],
            "total_transitions": int,
        }
    """
    # Build per-person yearly role data
    person_year_roles: dict[str, dict[int, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        stage = ROLE_STAGE.get(c.role)
        if stage:
            person_year_roles[c.person_id][anime.year].add(stage)

    # Count transitions
    flow_counts: dict[tuple[str, str], int] = defaultdict(int)
    total_transitions = 0

    for pid, year_roles in person_year_roles.items():
        years = sorted(year_roles.keys())
        for i in range(len(years) - 1):
            current_roles = year_roles[years[i]]
            next_roles = year_roles[years[i + 1]]

            # Take highest stage role for each year (extract stage number for robust ordering)
            def _stage_num(s: str) -> int:
                return int(s.split(":")[0].split()[-1]) if ":" in s else 0

            current_best = max(current_roles, key=_stage_num)
            next_best = max(next_roles, key=_stage_num)

            if current_best != next_best:
                flow_counts[(current_best, next_best)] += 1
                total_transitions += 1

    # Build nodes and links
    all_nodes = set()
    for src, tgt in flow_counts:
        all_nodes.add(src)
        all_nodes.add(tgt)

    nodes = [{"id": n, "label": n} for n in sorted(all_nodes)]
    links = [
        {"source": src, "target": tgt, "value": cnt}
        for (src, tgt), cnt in sorted(flow_counts.items(), key=lambda x: -x[1])
    ]

    result = {
        "nodes": nodes,
        "links": links,
        "total_transitions": total_transitions,
    }

    logger.info("role_flow_computed", transitions=total_transitions, nodes=len(nodes))
    return result

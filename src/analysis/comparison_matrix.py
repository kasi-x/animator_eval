"""Comparison matrix — compare multiple persons' scores in matrix form.

N人の人物に対して、各軸のスコアを比較行列として出力。
"""

import structlog

logger = structlog.get_logger()


def build_comparison_matrix(
    person_ids: list[str],
    results: list[dict],
    axes: tuple[str, ...] = ("iv_score", "person_fe", "birank", "patronage"),
) -> dict:
    """Build a comparison matrix for multiple persons.

    Args:
        person_ids: 比較対象の人物ID
        results: スコア結果リスト
        axes: 比較する軸

    Returns:
        {
            "persons": [{person_id, name, ...scores}],
            "axis_rankings": {axis: [ranked person_ids]},
            "pairwise_dominance": {pid_a: {pid_b: {"wins": int, "ties": int, "losses": int}}},
        }
    """
    # Filter results to requested persons
    scores_map = {r["person_id"]: r for r in results}
    persons = []
    for pid in person_ids:
        if pid in scores_map:
            r = scores_map[pid]
            persons.append(
                {
                    "person_id": pid,
                    "name": r.get("name", "") or r.get("name_ja", "") or pid,
                    **{axis: r.get(axis, 0) for axis in axes},
                }
            )

    if not persons:
        return {"persons": [], "axis_rankings": {}, "pairwise_dominance": {}}

    # Rankings per axis
    axis_rankings = {}
    for axis in axes:
        ranked = sorted(persons, key=lambda x: x.get(axis, 0), reverse=True)
        axis_rankings[axis] = [p["person_id"] for p in ranked]

    # Pairwise dominance
    pairwise: dict[str, dict[str, dict]] = {}
    for i, pa in enumerate(persons):
        pid_a = pa["person_id"]
        pairwise[pid_a] = {}
        for j, pb in enumerate(persons):
            if i == j:
                continue
            pid_b = pb["person_id"]
            wins = sum(1 for ax in axes if pa.get(ax, 0) > pb.get(ax, 0))
            ties = sum(1 for ax in axes if pa.get(ax, 0) == pb.get(ax, 0))
            losses = len(axes) - wins - ties
            pairwise[pid_a][pid_b] = {"wins": wins, "ties": ties, "losses": losses}

    return {
        "persons": persons,
        "axis_rankings": axis_rankings,
        "pairwise_dominance": pairwise,
    }

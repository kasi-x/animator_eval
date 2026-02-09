"""バッチ比較 — グループ間のスコア比較.

2つの人物グループ (例: スタジオA vs スタジオB、ベテラン vs 新人) を
統計的に比較する。
"""

import structlog

logger = structlog.get_logger()


def compare_groups(
    group_a: list[dict],
    group_b: list[dict],
    group_a_label: str = "Group A",
    group_b_label: str = "Group B",
    axes: list[str] | None = None,
) -> dict:
    """2つのグループのスコアを比較する.

    Args:
        group_a: グループAの結果リスト
        group_b: グループBの結果リスト
        group_a_label: グループA名
        group_b_label: グループB名
        axes: 比較軸

    Returns:
        {group_a, group_b, comparison_by_axis, summary}
    """
    if axes is None:
        axes = ["authority", "trust", "skill", "composite"]

    def _stats(group: list[dict], axis: str) -> dict:
        vals = [r.get(axis, 0) for r in group]
        if not vals:
            return {"mean": 0, "min": 0, "max": 0, "count": 0}
        return {
            "mean": round(sum(vals) / len(vals), 2),
            "min": round(min(vals), 2),
            "max": round(max(vals), 2),
            "count": len(vals),
        }

    comparison: dict[str, dict] = {}
    a_wins = 0
    b_wins = 0

    for axis in axes:
        stats_a = _stats(group_a, axis)
        stats_b = _stats(group_b, axis)
        diff = round(stats_a["mean"] - stats_b["mean"], 2)
        winner = group_a_label if diff > 0 else group_b_label if diff < 0 else "tie"
        if diff > 0:
            a_wins += 1
        elif diff < 0:
            b_wins += 1

        comparison[axis] = {
            group_a_label: stats_a,
            group_b_label: stats_b,
            "mean_diff": diff,
            "winner": winner,
        }

    overall_winner = (
        group_a_label if a_wins > b_wins
        else group_b_label if b_wins > a_wins
        else "tie"
    )

    logger.info(
        "groups_compared",
        group_a=group_a_label,
        group_b=group_b_label,
        winner=overall_winner,
    )

    return {
        "group_a": {"label": group_a_label, "count": len(group_a)},
        "group_b": {"label": group_b_label, "count": len(group_b)},
        "comparison_by_axis": comparison,
        "summary": {
            "a_wins": a_wins,
            "b_wins": b_wins,
            "ties": len(axes) - a_wins - b_wins,
            "overall_winner": overall_winner,
        },
    }

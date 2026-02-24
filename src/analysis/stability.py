"""スコア安定性検出 — 前回実行との差分を検出する.

新データ追加時にスコアが大きく変動した人物を検出し、
データ品質問題の早期発見に役立てる。
"""

import json
from pathlib import Path

import structlog

logger = structlog.get_logger()


def compare_scores(
    current: list[dict],
    previous_path: Path,
    threshold: float = 10.0,
) -> dict:
    """現在のスコアと前回の scores.json を比較する.

    Args:
        current: 今回のパイプライン結果
        previous_path: 前回の scores.json パス
        threshold: 変動アラート閾値（composite の絶対差）

    Returns:
        {
            "new_persons": [person_id, ...],
            "removed_persons": [person_id, ...],
            "significant_changes": [{person_id, old_composite, new_composite, delta}, ...],
            "rank_changes": [{person_id, old_rank, new_rank, delta}, ...],
            "summary": {total_compared, avg_delta, max_delta},
        }
    """
    if not previous_path.exists():
        return {
            "new_persons": [r["person_id"] for r in current],
            "removed_persons": [],
            "significant_changes": [],
            "rank_changes": [],
            "summary": {"total_compared": 0, "avg_delta": 0.0, "max_delta": 0.0},
        }

    previous = json.loads(previous_path.read_text())
    prev_map = {r["person_id"]: r for r in previous}
    curr_map = {r["person_id"]: r for r in current}

    prev_ids = set(prev_map)
    curr_ids = set(curr_map)

    new_persons = sorted(curr_ids - prev_ids)
    removed_persons = sorted(prev_ids - curr_ids)

    # ランク計算
    prev_ranked = {pid: i + 1 for i, pid in enumerate(prev_map)}
    curr_ranked = {pid: i + 1 for i, pid in enumerate(curr_map)}

    significant_changes = []
    rank_changes = []
    deltas = []

    common_ids = prev_ids & curr_ids
    for pid in common_ids:
        old_c = prev_map[pid].get("composite", 0)
        new_c = curr_map[pid].get("composite", 0)
        delta = new_c - old_c
        deltas.append(abs(delta))

        if abs(delta) >= threshold:
            significant_changes.append(
                {
                    "person_id": pid,
                    "name": curr_map[pid].get("name", pid),
                    "old_composite": round(old_c, 2),
                    "new_composite": round(new_c, 2),
                    "delta": round(delta, 2),
                }
            )

        old_rank = prev_ranked.get(pid, 0)
        new_rank = curr_ranked.get(pid, 0)
        rank_delta = old_rank - new_rank  # positive = moved up
        if abs(rank_delta) >= 5:
            rank_changes.append(
                {
                    "person_id": pid,
                    "name": curr_map[pid].get("name", pid),
                    "old_rank": old_rank,
                    "new_rank": new_rank,
                    "delta": rank_delta,
                }
            )

    significant_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)
    rank_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)

    summary = {
        "total_compared": len(common_ids),
        "avg_delta": round(sum(deltas) / len(deltas), 2) if deltas else 0.0,
        "max_delta": round(max(deltas), 2) if deltas else 0.0,
    }

    logger.info(
        "score_stability",
        new=len(new_persons),
        removed=len(removed_persons),
        significant_changes=len(significant_changes),
        avg_delta=summary["avg_delta"],
    )

    return {
        "new_persons": new_persons,
        "removed_persons": removed_persons,
        "significant_changes": significant_changes,
        "rank_changes": rank_changes,
        "summary": summary,
    }

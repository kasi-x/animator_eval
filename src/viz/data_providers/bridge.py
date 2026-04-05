"""ブリッジ分析レポート用データプロバイダ."""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from src.utils.json_io import load_json_file_or_return_default


@dataclass(frozen=True)
class BridgePerson:
    """ブリッジ人材1人分."""

    person_id: str
    name: str
    bridge_score: int
    communities_connected: int
    cross_community_edges: int


@dataclass(frozen=True)
class CommunityPair:
    """コミュニティペアとエッジ数."""

    label: str
    edge_count: int


@dataclass(frozen=True)
class BridgeData:
    """bridges.json + scores.json から抽出した構造化データ."""

    # 統計
    total_persons: int
    total_communities: int
    total_cross_edges: int
    bridge_person_count: int
    bridge_ratio_pct: float

    # ブリッジ人材リスト (bridge_score 降順)
    bridge_persons: tuple[BridgePerson, ...]

    # スコア分布用
    bridge_scores: tuple[int, ...]

    # Bridge vs Non-Bridge IV比較
    bridge_ivs: tuple[float, ...]
    nonbridge_ivs: tuple[float, ...]

    # コミュニティ接続数別スコア
    scores_by_communities: dict[int, tuple[float, ...]]

    # トップコミュニティペア
    top_community_pairs: tuple[CommunityPair, ...]

    # K-Means用の raw features (bridge_score, communities_connected, cross_community_edges)
    bridge_features: tuple[tuple[float, float, float], ...] = ()

    # scores.json から取得するブリッジ人材の役職分布
    bridge_role_counts: dict[str, int] = field(default_factory=dict)

    # コミュニティペア間のエッジ数マトリクス（上位コミュニティのみ）
    community_matrix_labels: tuple[str, ...] = ()
    community_matrix: tuple[tuple[int, ...], ...] = ()


def load_bridge_data(json_dir: Path) -> BridgeData | None:
    """bridges.json + scores.json を読み込み BridgeData を返す."""
    raw = load_json_file_or_return_default(json_dir / "bridges.json", {})
    if not raw or not isinstance(raw, dict):
        return None

    stats = raw.get("stats", {})
    bridge_persons_raw = raw.get("bridge_persons", [])
    cross_edges_raw = raw.get("cross_community_edges", [])

    total_persons = stats.get("total_persons", 0)
    bridge_count = stats.get("bridge_person_count", 0)

    # Bridge persons
    persons = tuple(
        BridgePerson(
            person_id=bp["person_id"],
            name=bp.get("name", bp["person_id"]),
            bridge_score=bp["bridge_score"],
            communities_connected=bp["communities_connected"],
            cross_community_edges=bp["cross_community_edges"],
        )
        for bp in bridge_persons_raw
    )

    bridge_scores = tuple(bp.bridge_score for bp in persons)

    # Community-grouped scores
    scores_by_comm: dict[int, list[float]] = {}
    for bp in persons:
        scores_by_comm.setdefault(bp.communities_connected, []).append(
            float(bp.bridge_score)
        )
    scores_by_communities = {
        k: tuple(v) for k, v in scores_by_comm.items()
    }

    # Bridge vs Non-Bridge IV (from scores.json) + role counts
    bridge_ivs: list[float] = []
    nonbridge_ivs: list[float] = []
    bridge_role_counts: dict[str, int] = {}
    scores_raw = load_json_file_or_return_default(json_dir / "scores.json", [])
    if scores_raw and isinstance(scores_raw, list) and persons:
        bridge_pids = {bp.person_id for bp in persons}
        for p in scores_raw:
            iv = p.get("iv_score", 0)
            if p.get("person_id", "") in bridge_pids:
                bridge_ivs.append(float(iv))
                role = p.get("primary_role", "unknown")
                bridge_role_counts[role] = bridge_role_counts.get(role, 0) + 1
            else:
                nonbridge_ivs.append(float(iv))
        # Subsample non-bridge for performance
        if len(nonbridge_ivs) > 5000:
            rng = random.Random(42)
            nonbridge_ivs = rng.sample(nonbridge_ivs, 5000)

    # Top community pairs
    pair_counts: Counter[tuple[int, int]] = Counter()
    for edge in cross_edges_raw:
        pair = tuple(sorted([edge["community_a"], edge["community_b"]]))
        pair_counts[pair] += 1

    top_pairs = tuple(
        CommunityPair(label=f"C{a}-C{b}", edge_count=cnt)
        for (a, b), cnt in pair_counts.most_common(30)
    )

    # K-Means用 raw features
    bridge_features = tuple(
        (float(bp.bridge_score), float(bp.communities_connected), float(bp.cross_community_edges))
        for bp in persons
    )

    # コミュニティペア間エッジ数マトリクス（上位10コミュニティ）
    comm_counts: Counter[int] = Counter()
    for edge in cross_edges_raw:
        comm_counts[edge["community_a"]] += 1
        comm_counts[edge["community_b"]] += 1
    top_comms = [c for c, _ in comm_counts.most_common(10)]

    community_matrix_labels = tuple(f"C{c}" for c in top_comms)
    matrix: list[tuple[int, ...]] = []
    for ca in top_comms:
        row: list[int] = []
        for cb in top_comms:
            pair = tuple(sorted([ca, cb]))
            row.append(pair_counts.get(pair, 0))
        matrix.append(tuple(row))
    community_matrix = tuple(matrix)

    return BridgeData(
        total_persons=total_persons,
        total_communities=stats.get("total_communities", 0),
        total_cross_edges=stats.get("total_cross_edges", 0),
        bridge_person_count=bridge_count,
        bridge_ratio_pct=bridge_count / max(total_persons, 1) * 100,
        bridge_persons=persons,
        bridge_scores=bridge_scores,
        bridge_ivs=tuple(bridge_ivs),
        nonbridge_ivs=tuple(nonbridge_ivs),
        scores_by_communities=scores_by_communities,
        top_community_pairs=top_pairs,
        bridge_features=bridge_features,
        bridge_role_counts=bridge_role_counts,
        community_matrix_labels=community_matrix_labels,
        community_matrix=community_matrix,
    )

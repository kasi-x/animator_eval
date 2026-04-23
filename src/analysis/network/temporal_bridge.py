"""Temporal Bridge Analysis — bridge analysis on the temporal network.

5年スライディングウィンドウでコラボレーショングラフを再構築し、
ブリッジ人物の時間変化・寿命・世代交代を分析する。

静的ネットワークでは見えない「時期特異的なブリッジ現象」を可視化する。
キャリアアーク（どの時代にブリッジとして機能していたか）、
ブリッジ寿命の分布、世代別トップブリッジを提供する。

References:
    - Holme, P., & Saramäki, J. (2012). Temporal networks.
      Physics Reports, 519(3), 97-125.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


# =============================================================================
# Dataclasses
# =============================================================================


@dataclass
class TemporalBridgeSnapshot:
    """Bridge analysis result per time window.

    Attributes:
        window_start: ウィンドウ開始年
        window_end: ウィンドウ終了年（含む）
        bridge_person_ids: ブリッジと判定された person_id のセット
        bridge_count: ブリッジ人物数
        top_bridges: [(person_id, cross_community_edges), ...] 上位 N 人
        total_persons: このウィンドウの総人物数
        total_communities: 検出されたコミュニティ数
    """

    window_start: int
    window_end: int
    bridge_person_ids: set[str] = field(default_factory=set)
    bridge_count: int = 0
    top_bridges: list[tuple[str, int]] = field(default_factory=list)
    total_persons: int = 0
    total_communities: int = 0


@dataclass
class BridgeLifespanStats:
    """Bridge lifespan statistics.

    Attributes:
        person_id: person_id
        lifespan_windows: 連続してブリッジであったウィンドウ数の最大値
        total_windows_as_bridge: ブリッジとして出現したウィンドウ数（連続でなくてもよい）
        first_bridge_year: 最初にブリッジとして出現した年
        last_bridge_year: 最後にブリッジとして出現した年
        bridge_windows: ブリッジであったウィンドウの開始年リスト
    """

    person_id: str
    lifespan_windows: int = 0
    total_windows_as_bridge: int = 0
    first_bridge_year: int | None = None
    last_bridge_year: int | None = None
    bridge_windows: list[int] = field(default_factory=list)


# =============================================================================
# Core Computation
# =============================================================================


def compute_temporal_bridges(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    window_size: int = 5,
    step: int = 2,
    start_year: int = 1980,
    end_year: int = 2025,
    min_community_size: int = 5,
    top_n_bridges: int = 30,
) -> list[TemporalBridgeSnapshot]:
    """Run bridge analysis using a sliding window.

    各ウィンドウ [window_start, window_start + window_size) の期間のクレジットのみを
    使ってコラボレーショングラフを再構築し、コミュニティ検出 → ブリッジ判定を行う。

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        window_size: ウィンドウサイズ（年数）
        step: ウィンドウの移動幅（年数）
        start_year: 分析開始年
        end_year: 分析終了年
        min_community_size: コミュニティ最小サイズ
        top_n_bridges: 各ウィンドウで上位何人を返すか

    Returns:
        TemporalBridgeSnapshot のリスト（時系列順）
    """
    # deferred import (avoid circular dependency)
    from src.analysis.network.community_detection import (
        analyze_community_overlap,
        detect_communities,
    )

    # Cache: anime_id → year
    anime_year: dict[str, int | None] = {aid: a.year for aid, a in anime_map.items()}

    snapshots: list[TemporalBridgeSnapshot] = []

    windows = list(range(start_year, end_year - window_size + 2, step))
    logger.info(
        "temporal_bridge_start",
        windows=len(windows),
        window_size=window_size,
        step=step,
    )

    for w_start in windows:
        w_end = w_start + window_size - 1

        # extract credits within the window
        window_credits = [
            c
            for c in credits
            if (
                c.anime_id in anime_year
                and anime_year[c.anime_id] is not None
                and w_start <= anime_year[c.anime_id] <= w_end
            )  # type: ignore[operator]
        ]

        if len(window_credits) < 10:
            # skip windows with too little data
            logger.debug(
                "temporal_window_skipped_insufficient_data",
                window_start=w_start,
                credits=len(window_credits),
            )
            continue

        # build collaboration graph
        # Aggregate anime_id → [person_id]
        anime_persons: dict[str, list[str]] = defaultdict(list)
        for c in window_credits:
            anime_persons[c.anime_id].append(c.person_id)

        import networkx as nx

        G = nx.Graph()
        for anime_id, person_ids in anime_persons.items():
            unique_persons = list(set(person_ids))
            n = len(unique_persons)
            if n < 2:
                continue
            for i in range(n):
                for j in range(i + 1, n):
                    a, b = unique_persons[i], unique_persons[j]
                    if G.has_edge(a, b):
                        G[a][b]["weight"] += 1
                    else:
                        G.add_edge(a, b, weight=1)

        if G.number_of_nodes() < min_community_size * 2:
            continue

        # community detection (use fixed Louvain from Phase 0)
        try:
            communities = detect_communities(G, min_community_size=min_community_size)
        except Exception as e:
            logger.warning(
                "community_detection_failed_in_window",
                window_start=w_start,
                error=str(e),
            )
            continue

        if len(communities) < 2:
            # if ≤1 community, no bridges exist
            snapshots.append(
                TemporalBridgeSnapshot(
                    window_start=w_start,
                    window_end=w_end,
                    bridge_count=0,
                    total_persons=G.number_of_nodes(),
                    total_communities=len(communities),
                )
            )
            continue

        # bridge check (persons connected to multiple communities)
        bridges = analyze_community_overlap(communities, G)
        bridge_ids = set(bridges.keys())

        # top_n_bridges: sort by cross_community_edges count descending
        # bridges[pid] = [(community_id, connections_count), ...]
        bridge_scores: list[tuple[str, int]] = [
            (pid, sum(count for _, count in comm_list))
            for pid, comm_list in bridges.items()
        ]
        bridge_scores.sort(key=lambda x: x[1], reverse=True)

        snapshots.append(
            TemporalBridgeSnapshot(
                window_start=w_start,
                window_end=w_end,
                bridge_person_ids=bridge_ids,
                bridge_count=len(bridge_ids),
                top_bridges=bridge_scores[:top_n_bridges],
                total_persons=G.number_of_nodes(),
                total_communities=len(communities),
            )
        )

        logger.debug(
            "temporal_window_complete",
            window_start=w_start,
            window_end=w_end,
            persons=G.number_of_nodes(),
            communities=len(communities),
            bridges=len(bridge_ids),
        )

    logger.info(
        "temporal_bridge_complete",
        windows_computed=len(snapshots),
    )
    return snapshots


# =============================================================================
# Lifespan Analysis
# =============================================================================


def compute_bridge_lifespan(
    snapshots: list[TemporalBridgeSnapshot],
) -> dict[str, BridgeLifespanStats]:
    """Compute bridge lifespan for each person.

    「寿命」= 連続してブリッジであったウィンドウ数の最大値
    「総ウィンドウ数」= ブリッジとして出現したウィンドウ数（連続不問）

    Args:
        snapshots: compute_temporal_bridges の出力

    Returns:
        person_id → BridgeLifespanStats
    """
    # person_id → list of window start years when they were a bridge (chronological)
    person_bridge_windows: dict[str, list[int]] = defaultdict(list)

    for snap in sorted(snapshots, key=lambda s: s.window_start):
        for pid in snap.bridge_person_ids:
            person_bridge_windows[pid].append(snap.window_start)

    stats: dict[str, BridgeLifespanStats] = {}

    for pid, windows in person_bridge_windows.items():
        windows_sorted = sorted(windows)
        total = len(windows_sorted)

        # compute maximum length of consecutive windows
        # windows advance by step, so consecutive = difference ≤ step
        if len(windows_sorted) == 1:
            max_consecutive = 1
        else:
            # estimate window step (most frequent difference)
            diffs = [
                windows_sorted[i + 1] - windows_sorted[i]
                for i in range(len(windows_sorted) - 1)
            ]
            if diffs:
                from collections import Counter

                step_estimate = Counter(diffs).most_common(1)[0][0]
            else:
                step_estimate = 2

            # detect consecutive groups
            max_consecutive = 1
            current = 1
            for i in range(1, len(windows_sorted)):
                if windows_sorted[i] - windows_sorted[i - 1] <= step_estimate:
                    current += 1
                    max_consecutive = max(max_consecutive, current)
                else:
                    current = 1

        stats[pid] = BridgeLifespanStats(
            person_id=pid,
            lifespan_windows=max_consecutive,
            total_windows_as_bridge=total,
            first_bridge_year=windows_sorted[0],
            last_bridge_year=windows_sorted[-1],
            bridge_windows=windows_sorted,
        )

    logger.info(
        "bridge_lifespan_computed",
        persons=len(stats),
        avg_lifespan=round(
            sum(s.lifespan_windows for s in stats.values()) / max(len(stats), 1), 2
        ),
    )
    return stats


def get_person_temporal_trajectory(
    snapshots: list[TemporalBridgeSnapshot],
    person_ids: list[str],
) -> dict[str, list[int | None]]:
    """Return an in/out matrix of target persons × time windows.

    各ウィンドウで当該人物がブリッジであれば cross_community_edges 数、
    そうでなければ None を返す（ヒートマップ用）。

    Args:
        snapshots: compute_temporal_bridges の出力
        person_ids: 対象の person_id リスト

    Returns:
        person_id → [cross_edges_or_none, ...] のリスト（snapshots と同じ長さ）
    """
    # Pre-build lookup for top_bridges
    snap_bridge_scores: list[dict[str, int]] = []
    for snap in snapshots:
        score_map = {pid: score for pid, score in snap.top_bridges}
        snap_bridge_scores.append(score_map)

    trajectories: dict[str, list[int | None]] = {pid: [] for pid in person_ids}

    for snap, score_map in zip(snapshots, snap_bridge_scores):
        for pid in person_ids:
            if pid in snap.bridge_person_ids:
                # score if in top_bridges, else 1
                trajectories[pid].append(score_map.get(pid, 1))
            else:
                trajectories[pid].append(None)

    return trajectories


def get_era_top_bridges(
    snapshots: list[TemporalBridgeSnapshot],
    era_boundaries: list[int] | None = None,
    top_n: int = 5,
) -> dict[str, list[tuple[str, int]]]:
    """Aggregate top bridge persons by era.

    Args:
        snapshots: compute_temporal_bridges の出力
        era_boundaries: 時代の境界年リスト（例: [1990, 2000, 2010, 2020]）
        top_n: 各時代で返す上位人物数

    Returns:
        era_label → [(person_id, total_cross_edges), ...] の辞書
    """
    if era_boundaries is None:
        era_boundaries = [1990, 2000, 2010, 2020, 2030]

    # define era labels
    era_labels = []
    for i, boundary in enumerate(era_boundaries[:-1]):
        label = f"{boundary}s"
        era_labels.append((boundary, era_boundaries[i + 1], label))

    era_scores: dict[str, dict[str, int]] = {
        label: defaultdict(int) for _, _, label in era_labels
    }

    for snap in snapshots:
        center_year = (snap.window_start + snap.window_end) // 2
        for boundary_start, boundary_end, label in era_labels:
            if boundary_start <= center_year < boundary_end:
                for pid, score in snap.top_bridges:
                    era_scores[label][pid] += score
                break

    result: dict[str, list[tuple[str, int]]] = {}
    for _, _, label in era_labels:
        scores = sorted(era_scores[label].items(), key=lambda x: x[1], reverse=True)
        result[label] = scores[:top_n]

    return result


# =============================================================================
# Entry Point
# =============================================================================


def main() -> None:
    """Standalone entry point."""
    from src.analysis.silver_reader import (
        load_anime_silver,
        load_credits_silver,
        load_persons_silver,
    )

    persons = load_persons_silver()
    anime_list = load_anime_silver()
    credits = load_credits_silver()

    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}

    logger.info("computing_temporal_bridges")
    snapshots = compute_temporal_bridges(
        credits, anime_map, window_size=5, step=2, start_year=1990, end_year=2025
    )

    print(f"\n=== 時間ウィンドウ別ブリッジ分析 ({len(snapshots)} ウィンドウ) ===\n")
    for snap in snapshots:
        print(
            f"{snap.window_start}-{snap.window_end}: "
            f"ブリッジ {snap.bridge_count}人 / "
            f"総人数 {snap.total_persons}人 / "
            f"コミュニティ {snap.total_communities}"
        )

    logger.info("computing_bridge_lifespan")
    lifespan = compute_bridge_lifespan(snapshots)

    print("\n=== ブリッジ寿命 トップ10 ===\n")
    sorted_lifespan = sorted(
        lifespan.values(), key=lambda s: s.lifespan_windows, reverse=True
    )[:10]
    for s in sorted_lifespan:
        name = person_names.get(s.person_id, s.person_id)
        print(
            f"{name}: 最大連続 {s.lifespan_windows} ウィンドウ "
            f"(総計 {s.total_windows_as_bridge} ウィンドウ, "
            f"{s.first_bridge_year}-{s.last_bridge_year})"
        )

    era_tops = get_era_top_bridges(snapshots)
    print("\n=== 時代別トップブリッジ ===\n")
    for era_label, top_persons in era_tops.items():
        if top_persons:
            print(f"{era_label}:")
            for pid, score in top_persons[:3]:
                name = person_names.get(pid, pid)
                print(f"  - {name} (score: {score})")


if __name__ == "__main__":
    main()

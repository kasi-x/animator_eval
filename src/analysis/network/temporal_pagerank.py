"""時系列PageRank — 年次スナップショットによる動的BiRank評価.

年ごとの累積グラフでPageRankを実行し、BiRank推移・先見スコア・抜擢検出を算出する。
Phase 9 分析モジュールとして動作（コアスコアリングには影響しない独立した補足分析）。

Components:
1. 年次スナップショットPageRank (warm start)
2. 同僚エッジ (Peer Edges) — 同役職カテゴリの人同士をグラフに追加
3. 先見スコア (Foresight) — 無名時の共演者が後に成長した場合のボーナス
4. 抜擢検出 (Promotion Detection) — 格上げ起用の検出と信頼度付き帰属
"""

import pickle  # noqa: S403 — self-generated local files only, not network input
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

import networkx as nx
import numpy as np
import structlog

from src.analysis.career import CAREER_STAGE
from src.analysis.scoring.pagerank import normalize_scores, weighted_pagerank
from src.models import Anime, Credit, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.role_groups import ROLE_CATEGORY

logger = structlog.get_logger()

# =============================================================================
# Data Structures
# =============================================================================

PEER_EDGE_CAP = 15  # Max persons per category per anime for peer edges


@dataclass
class StreamingCheckpoint:
    """年次グラフストリーミングのチェックポイント.

    累積グラフの状態を保存し、次回実行時に差分年だけ再計算できるようにする。
    pickle で result/json/ 以下に保存される（自己生成ファイルのみ使用）。
    """

    last_year: int  # このグラフが表現する最終年（その年まで累積済み）
    graph: nx.DiGraph  # last_year 時点の累積グラフ
    prev_scores: dict[str, float]  # last_year の PageRank スコア（warm start 用）


def save_streaming_checkpoint(path: Path, checkpoint: StreamingCheckpoint) -> None:
    """チェックポイントを pickle として保存する."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        pickle.dump(checkpoint, fh, protocol=pickle.HIGHEST_PROTOCOL)  # noqa: S301
    logger.info(
        "birank_checkpoint_saved", last_year=checkpoint.last_year, path=str(path)
    )


def load_streaming_checkpoint(path: Path) -> StreamingCheckpoint | None:
    """チェックポイントを読み込む. 存在しない/壊れている場合は None."""
    if not path.exists():
        return None
    try:
        with open(path, "rb") as fh:
            obj = pickle.load(fh)  # noqa: S301
        if not isinstance(obj, StreamingCheckpoint):
            logger.warning("birank_checkpoint_invalid_type", path=str(path))
            return None
        logger.info("birank_checkpoint_loaded", last_year=obj.last_year, path=str(path))
        return obj
    except Exception as exc:
        logger.warning("birank_checkpoint_load_failed", path=str(path), error=str(exc))
        return None


def _work_importance(anime: Anime | None) -> float:
    """Duration-based work importance (no anime.score)."""
    from src.utils.config import DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER

    if anime is None or anime.duration is None:
        return 1.0
    duration_mult = min(
        anime.duration / DURATION_BASELINE_MINUTES,
        DURATION_MAX_MULTIPLIER,
    )
    return max(duration_mult, 0.01)


@dataclass(frozen=True)
class YearlyBirankSnapshot:
    """1年分のBiRank評価."""

    year: int
    birank: float  # 0-100 normalized within year
    raw_pagerank: float
    graph_size: int  # person nodes count
    n_credits_cumulative: int


@dataclass
class BirankTimeline:
    """人物の年次BiRank推移."""

    person_id: str
    name: str = ""  # display name resolved from persons list
    snapshots: list[YearlyBirankSnapshot] = field(default_factory=list)
    peak_year: int | None = None
    peak_birank: float = 0.0
    career_start_year: int | None = None
    latest_year: int | None = None
    trajectory: str = "stable"  # "rising" | "stable" | "declining" | "peaked"
    is_censored: bool = False  # True if latest_year >= RIGHT_CENSOR_CUTOFF (peak may not yet be reached)


@dataclass
class ForesightScore:
    """先見スコア — 無名時の共演者が後に成長した場合のボーナス.

    Note: This is a retrospective pattern metric. "Foresight" here means
    a person historically co-appeared with persons who later rose.
    Predictive validity requires holdout_validation (see TemporalPageRankResult).
    """

    person_id: str
    name: str = ""  # display name resolved from persons list
    foresight_raw: float = 0.0
    foresight_normalized: float = 0.0  # 0-100
    discoveries: list[dict] = field(default_factory=list)  # top 20 for export
    n_discoveries: int = 0
    confidence_lower: float = 0.0  # bootstrap 95% CI
    confidence_upper: float = 0.0


@dataclass
class PromotionEvent:
    """1回の抜擢イベント."""

    promotee_id: str
    anime_id: str
    year: int
    previous_max_stage: int  # CAREER_STAGE値
    new_stage: int
    stage_jump: int
    attributed_to: str | None = None
    confidence: float = 0.0  # 0-1


@dataclass
class PromotionCredit:
    """抜擢クレジット — ある上位者が何人を格上げしたか.

    Note: "Promotion" is attributed to the highest-stage person on the same
    production. This captures co-credit structural patterns, not verified
    mentoring relationships. vs_cohort_baseline is a true lift ratio
    (e.g., 2.3 means 2.3× the stage-cohort baseline promotion success rate).
    """

    person_id: str
    name: str = ""  # display name resolved from persons list
    promotion_count: int = 0
    successful_promotions: int = 0
    promotion_success_rate: float = 0.0
    shrunk_success_rate: float = (
        0.0  # Beta-Binomial posterior mean (preferred for ranking)
    )
    vs_cohort_baseline: float = (
        0.0  # true lift vs stage-cohort baseline (not 0-1 compressed)
    )
    exclusivity_score: float = 0.0  # 他作品での同時昇格がない度合い
    studio_adjusted_rate: float = 0.0  # スタジオ効果を統制後
    confidence: float = 0.0  # 4因子の幾何平均
    events: list[PromotionEvent] = field(default_factory=list)


@dataclass
class TemporalPageRankResult:
    """時系列PageRankの全結果."""

    birank_timelines: dict = field(default_factory=dict)  # person_id -> dict
    foresight_scores: dict = field(default_factory=dict)  # person_id -> dict
    promotion_credits: dict = field(default_factory=dict)  # person_id -> dict
    years_computed: list[int] = field(default_factory=list)
    total_persons: int = 0
    computation_time_seconds: float = 0.0
    holdout_validation: dict = field(
        default_factory=dict
    )  # foresight holdout eval results


# =============================================================================
# Step 1: Cumulative Graph Construction + Peer Edges
# =============================================================================


def _add_peer_edges(
    graph: nx.DiGraph,
    anime_credits: dict[str, list[tuple[str, Role]]],
    anime_map: dict[str, Anime],
    peer_edge_weight: float,
) -> int:
    """同役職カテゴリの人同士に双方向エッジを追加する.

    Args:
        graph: 二部グラフ (person ↔ anime)
        anime_credits: {anime_id: [(person_id, role), ...]}
        anime_map: anime_id -> Anime
        peer_edge_weight: 同僚エッジの基本重み

    Returns:
        追加したエッジ数
    """
    added = 0

    for anime_id, staff in anime_credits.items():
        anime = anime_map.get(anime_id)
        importance = _work_importance(anime)

        # Group by ROLE_CATEGORY
        category_persons: dict[str, list[str]] = defaultdict(list)
        for person_id, role in staff:
            cat = ROLE_CATEGORY.get(role, "other")
            category_persons[cat].append(person_id)

        for cat, persons in category_persons.items():
            # Cap to prevent O(n^2) explosion
            capped = persons[:PEER_EDGE_CAP]
            w = peer_edge_weight * importance

            for i, pid_a in enumerate(capped):
                for pid_b in capped[i + 1 :]:
                    # Bidirectional peer edges
                    if graph.has_edge(pid_a, pid_b):
                        graph[pid_a][pid_b]["weight"] += w
                    else:
                        graph.add_edge(pid_a, pid_b, weight=w, edge_type="peer")
                    if graph.has_edge(pid_b, pid_a):
                        graph[pid_b][pid_a]["weight"] += w
                    else:
                        graph.add_edge(pid_b, pid_a, weight=w, edge_type="peer")
                    added += 2

    return added


def _build_yearly_cumulative_graphs(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    persons: list,
    peer_edge_weight: float,
) -> dict[int, nx.DiGraph]:
    """年ごとの累積二部グラフをインクリメンタルに構築する.

    前年のグラフを copy() して今年の差分だけ追加する。
    O(Y × C_year) — 毎年全クレジットを再走査する O(Y × C_total) に比べて高速。

    Returns:
        {year: DiGraph}
    """
    # Group credits by year
    credits_by_year: dict[int, list[Credit]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        year = anime.year if anime and anime.year else None
        if year:
            credits_by_year[year].append(c)

    if not credits_by_year:
        return {}

    years = sorted(credits_by_year.keys())
    person_map = {p.id: p for p in persons}

    yearly_graphs: dict[int, nx.DiGraph] = {}
    prev_graph: nx.DiGraph | None = None

    for year in years:
        year_credits = credits_by_year[year]

        # Incremental: copy previous year's graph, add delta
        if prev_graph is None:
            g = nx.DiGraph()
        else:
            g = prev_graph.copy()

        # Add new nodes and bipartite edges from this year's credits only
        for c in year_credits:
            if c.person_id not in g:
                p = person_map.get(c.person_id)
                if p:
                    g.add_node(
                        c.person_id,
                        type="person",
                        name=p.display_name,
                        name_ja=p.name_ja,
                        name_en=p.name_en,
                    )
                else:
                    g.add_node(c.person_id, type="person", name=c.person_id)

            if c.anime_id not in g:
                anime = anime_map.get(c.anime_id)
                if anime:
                    g.add_node(
                        c.anime_id,
                        type="anime",
                        name=anime.display_title,
                        year=anime.year,
                        score=anime.score,
                    )
                else:
                    g.add_node(c.anime_id, type="anime", name=c.anime_id)

            weight = ROLE_WEIGHTS.get(c.role.value, 1.0)
            # person -> anime
            if g.has_edge(c.person_id, c.anime_id):
                g[c.person_id][c.anime_id]["weight"] += weight
            else:
                g.add_edge(c.person_id, c.anime_id, weight=weight)
            # anime -> person
            if g.has_edge(c.anime_id, c.person_id):
                g[c.anime_id][c.person_id]["weight"] += weight
            else:
                g.add_edge(c.anime_id, c.person_id, weight=weight)

        # Add peer edges for this year's new anime credits only
        # (previous years' peer edges are already in the copied graph)
        year_anime_credits: dict[str, list[tuple[str, Role]]] = defaultdict(list)
        for c in year_credits:
            year_anime_credits[c.anime_id].append((c.person_id, c.role))
        _add_peer_edges(g, dict(year_anime_credits), anime_map, peer_edge_weight)

        yearly_graphs[year] = g
        prev_graph = g

    logger.info(
        "yearly_cumulative_graphs_built",
        years=len(yearly_graphs),
        year_range=f"{years[0]}-{years[-1]}",
    )
    return yearly_graphs


# =============================================================================
# Step 1+2 Combined: Streaming Build + PageRank (memory-optimized)
# =============================================================================


def _build_and_score_yearly_streaming(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    persons: list,
    peer_edge_weight: float,
    resume_checkpoint: StreamingCheckpoint | None = None,
) -> tuple[
    dict[int, dict[str, float]],
    dict[int, set[str]],
    dict[int, int],
    StreamingCheckpoint | None,
]:
    """Build yearly cumulative graphs and run PageRank in streaming fashion.

    チェックポイントが渡された場合、そのグラフ状態を起点にして
    resume_checkpoint.last_year より後の年だけ計算する。これにより
    新規データが少数年分しかない場合の再計算コストを最小化できる。

    Returns:
        (yearly_scores, yearly_person_nodes, yearly_person_count, final_checkpoint)
        - yearly_scores: {year: {node_id: raw_pagerank_score}}  ← 新規年のみ
        - yearly_person_nodes: {year: set of person node IDs}
        - yearly_person_count: {year: number of person nodes}
        - final_checkpoint: 最終年のグラフ状態（None = 計算なし）
    """
    credits_by_year: dict[int, list[Credit]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        year = anime.year if anime and anime.year else None
        if year:
            credits_by_year[year].append(c)

    if not credits_by_year:
        return {}, {}, {}, None

    all_years = sorted(credits_by_year.keys())

    # チェックポイントがある場合はその年以降だけ処理する
    if resume_checkpoint is not None:
        skip_up_to = resume_checkpoint.last_year
        years = [y for y in all_years if y > skip_up_to]
        prev_graph: nx.DiGraph = resume_checkpoint.graph.copy()
        prev_scores: dict[str, float] | None = dict(resume_checkpoint.prev_scores)
        logger.info(
            "birank_streaming_resumed",
            checkpoint_year=skip_up_to,
            new_years=len(years),
            first_new=years[0] if years else None,
        )
    else:
        years = all_years
        prev_graph = None
        prev_scores = None

    if not years:
        return {}, {}, {}, None

    yearly_scores: dict[int, dict[str, float]] = {}
    yearly_person_nodes: dict[int, set[str]] = {}
    yearly_person_count: dict[int, int] = {}

    for year in years:
        year_credits = credits_by_year[year]

        g = nx.DiGraph() if prev_graph is None else prev_graph.copy()

        for c in year_credits:
            if c.person_id not in g:
                g.add_node(c.person_id, type="person")
            if c.anime_id not in g:
                g.add_node(c.anime_id, type="anime")
            weight = ROLE_WEIGHTS.get(c.role.value, 1.0)
            if g.has_edge(c.person_id, c.anime_id):
                g[c.person_id][c.anime_id]["weight"] += weight
            else:
                g.add_edge(c.person_id, c.anime_id, weight=weight)
            if g.has_edge(c.anime_id, c.person_id):
                g[c.anime_id][c.person_id]["weight"] += weight
            else:
                g.add_edge(c.anime_id, c.person_id, weight=weight)

        person_nodes = {n for n in g.nodes() if g.nodes[n].get("type") == "person"}
        yearly_person_nodes[year] = person_nodes
        yearly_person_count[year] = len(person_nodes)

        if g.number_of_nodes() == 0:
            yearly_scores[year] = {}
        else:
            nstart = None
            if prev_scores:
                nodes = set(g.nodes())
                n_nodes = len(nodes)
                nstart = {node: prev_scores.get(node, 1.0 / n_nodes) for node in nodes}
                total = sum(nstart.values())
                if total > 0:
                    nstart = {k: v / total for k, v in nstart.items()}
            scores = weighted_pagerank(g, nstart=nstart)
            yearly_scores[year] = scores
            prev_scores = scores

        prev_graph = g

    # Build checkpoint from final state for potential reuse next run
    final_checkpoint = StreamingCheckpoint(
        last_year=years[-1],
        graph=prev_graph,
        prev_scores=prev_scores or {},
    )
    del prev_graph

    logger.info(
        "yearly_streaming_pagerank_complete",
        years=len(years),
        year_range=f"{years[0]}-{years[-1]}",
    )
    return yearly_scores, yearly_person_nodes, yearly_person_count, final_checkpoint


def _run_yearly_pagerank_with_warm_start(
    yearly_graphs: dict[int, nx.DiGraph],
) -> dict[int, dict[str, float]]:
    """Run PageRank per year with warm start (legacy — used by tests)."""
    years = sorted(yearly_graphs.keys())
    yearly_scores: dict[int, dict[str, float]] = {}
    prev_scores: dict[str, float] | None = None

    for year in years:
        graph = yearly_graphs[year]
        if graph.number_of_nodes() == 0:
            yearly_scores[year] = {}
            continue

        nstart = None
        if prev_scores:
            nodes = set(graph.nodes())
            nstart = {}
            for node in nodes:
                if node in prev_scores:
                    nstart[node] = prev_scores[node]
                else:
                    nstart[node] = 1.0 / len(nodes)
            total = sum(nstart.values())
            if total > 0:
                nstart = {k: v / total for k, v in nstart.items()}

        scores = weighted_pagerank(graph, nstart=nstart)
        yearly_scores[year] = scores
        prev_scores = scores

    return yearly_scores


# =============================================================================
# Step 3: BiRank Timeline Construction
# =============================================================================


def _build_birank_timelines_from_counts(
    yearly_scores: dict[int, dict[str, float]],
    yearly_person_count: dict[int, int],
    yearly_normalized: dict[int, dict[str, float]],
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, BirankTimeline]:
    """年次PageRankスコアからBiRankタイムラインを構築する (graph-free version).

    Args:
        yearly_scores: {year: {node_id: raw_pagerank}}
        yearly_person_count: {year: person node count} — pre-computed
        yearly_normalized: {year: {person_id: 0-100 normalized score}}
        credits: 全クレジット
        anime_map: anime_id -> Anime

    Returns:
        {person_id: BirankTimeline}
    """
    # Count cumulative credits per year
    credits_by_year: dict[int, int] = defaultdict(int)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        yr = anime.year if anime and anime.year else None
        if yr:
            credits_by_year[yr] += 1

    cumulative_credit_count = 0
    cumulative_by_year: dict[int, int] = {}
    for yr in sorted(credits_by_year.keys()):
        cumulative_credit_count += credits_by_year[yr]
        cumulative_by_year[yr] = cumulative_credit_count

    # Build timelines
    timelines: dict[str, BirankTimeline] = {}
    years = sorted(yearly_scores.keys())

    # Collect all person IDs
    all_person_ids: set[str] = set()
    for year, scores in yearly_normalized.items():
        all_person_ids.update(scores.keys())

    for person_id in all_person_ids:
        snapshots = []
        for year in years:
            normalized = yearly_normalized.get(year, {})
            raw = yearly_scores.get(year, {})
            if person_id not in normalized:
                continue
            snapshots.append(
                YearlyBirankSnapshot(
                    year=year,
                    birank=normalized[person_id],
                    raw_pagerank=raw.get(person_id, 0.0),
                    graph_size=yearly_person_count.get(year, 0),
                    n_credits_cumulative=cumulative_by_year.get(year, 0),
                )
            )

        if not snapshots:
            continue

        # Find peak
        peak_snap = max(snapshots, key=lambda s: s.birank)
        # Determine trajectory
        trajectory = _classify_trajectory(snapshots)

        timelines[person_id] = BirankTimeline(
            person_id=person_id,
            snapshots=snapshots,
            peak_year=peak_snap.year,
            peak_birank=peak_snap.birank,
            career_start_year=snapshots[0].year,
            latest_year=snapshots[-1].year,
            trajectory=trajectory,
        )

    return timelines


def _build_birank_timelines(
    yearly_scores: dict[int, dict[str, float]],
    yearly_graphs: dict[int, nx.DiGraph],
    yearly_normalized: dict[int, dict[str, float]],
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, BirankTimeline]:
    """Legacy wrapper — extracts person counts from graphs, delegates to _from_counts."""
    yearly_person_count = {}
    for year, graph in yearly_graphs.items():
        yearly_person_count[year] = sum(
            1 for n in graph.nodes() if graph.nodes[n].get("type") == "person"
        )
    return _build_birank_timelines_from_counts(
        yearly_scores, yearly_person_count, yearly_normalized, credits, anime_map
    )


def _classify_trajectory(snapshots: list[YearlyBirankSnapshot]) -> str:
    """スナップショット列からキャリア軌道を分類する.

    Returns: "rising" | "stable" | "declining" | "peaked"
    """
    if len(snapshots) < 2:
        return "stable"

    biranks = [s.birank for s in snapshots]
    n = len(biranks)

    # Use last third vs first third comparison
    third = max(n // 3, 1)
    early_avg = sum(biranks[:third]) / third
    late_avg = sum(biranks[-third:]) / third
    peak = max(biranks)
    peak_idx = biranks.index(peak)

    # Threshold for "significant" change
    threshold = 10.0

    if late_avg - early_avg > threshold:
        return "rising"
    elif early_avg - late_avg > threshold:
        # Check if peaked in the middle (not at the start)
        if 0 < peak_idx < n * 0.7 and peak - late_avg > threshold:
            return "peaked"
        return "declining"
    else:
        # Check peaked pattern: significant peak in middle, lower at both ends
        if peak - early_avg > threshold and peak - late_avg > threshold:
            if 0 < peak_idx < n - 1:
                return "peaked"
        return "stable"


# =============================================================================
# Step 4: Foresight Score
# =============================================================================


def _compute_foresight_scores(
    birank_timelines: dict[str, BirankTimeline],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    yearly_normalized: dict[int, dict[str, float]],
    foresight_horizon_years: int = 10,
    unknown_threshold_percentile: float = 25.0,
) -> dict[str, ForesightScore]:
    """先見スコアを計算する.

    Algorithm:
    1. Year T: find "unknown" Y (birank < 25th percentile) and "established" X (> 25th)
       who co-appear in the same anime
    2. If Y's birank grows by 10+ points within T+5 years -> X gets foresight credit
    3. foresight(X) = sum(growth(Y) * 1/(birank_T(Y) + epsilon))
    4. Bootstrap CI (200 iterations)

    Returns:
        {person_id: ForesightScore}
    """
    # Build co-appearance index: {(year, anime_id): set[person_id]}
    year_anime_persons: dict[tuple[int, str], set[str]] = defaultdict(set)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        yr = anime.year if anime and anime.year else None
        if yr:
            year_anime_persons[(yr, c.anime_id)].add(c.person_id)

    # Compute percentile thresholds per year
    year_thresholds: dict[int, float] = {}
    for year, scores in yearly_normalized.items():
        if scores:
            vals = list(scores.values())
            year_thresholds[year] = float(
                np.percentile(vals, unknown_threshold_percentile)
            )

    # Build per-person birank by year lookup
    person_year_birank: dict[str, dict[int, float]] = defaultdict(dict)
    for pid, timeline in birank_timelines.items():
        for snap in timeline.snapshots:
            person_year_birank[pid][snap.year] = snap.birank

    years = sorted(yearly_normalized.keys())

    # Collect discovery events: {established_person: [(growth, unknown_birank, year, unknown_id)]}
    discoveries_raw: dict[str, list[tuple[float, float, int, str]]] = defaultdict(list)

    for year in years:
        threshold = year_thresholds.get(year, 25.0)
        year_scores = yearly_normalized.get(year, {})

        # Find all (year, anime_id) pairs for this year
        for (yr, anime_id), persons in year_anime_persons.items():
            if yr != year:
                continue
            if len(persons) < 2:
                continue

            # Split into established (X) and unknown (Y)
            established = [p for p in persons if year_scores.get(p, 0) >= threshold]
            unknown = [p for p in persons if year_scores.get(p, 0) < threshold]

            if not established or not unknown:
                continue

            for y_id in unknown:
                y_birank_t = year_scores.get(y_id, 0.0)
                # Check future growth within horizon
                future_birank = 0.0
                for future_year in range(year + 1, year + foresight_horizon_years + 1):
                    fa = person_year_birank.get(y_id, {}).get(future_year)
                    if fa is not None:
                        future_birank = max(future_birank, fa)

                growth = future_birank - y_birank_t
                if growth < 5.0:
                    continue

                # Credit all established co-workers
                for x_id in established:
                    discoveries_raw[x_id].append((growth, y_birank_t, year, y_id))

    # Compute raw foresight scores
    epsilon = 1.0
    foresight_raw_scores: dict[str, float] = {}
    foresight_discoveries: dict[str, list[tuple[float, float, int, str]]] = {}

    for x_id, events in discoveries_raw.items():
        # Deduplicate: unique (x_id, y_id) pairs — take the max growth
        best_per_y: dict[str, tuple[float, float, int]] = {}
        for growth, y_auth, year, y_id in events:
            if y_id not in best_per_y or growth > best_per_y[y_id][0]:
                best_per_y[y_id] = (growth, y_auth, year)

        raw = 0.0
        deduped_events = []
        for y_id, (growth, y_auth, year) in best_per_y.items():
            raw += growth * (1.0 / (y_auth + epsilon))
            deduped_events.append((growth, y_auth, year, y_id))

        foresight_raw_scores[x_id] = raw
        foresight_discoveries[x_id] = deduped_events

    # Normalize foresight scores 0-100
    normalized = normalize_scores(foresight_raw_scores)

    # Bootstrap CI
    rng = np.random.default_rng(42)
    n_bootstrap = 200

    results: dict[str, ForesightScore] = {}
    for x_id in foresight_raw_scores:
        events = foresight_discoveries.get(x_id, [])
        raw = foresight_raw_scores[x_id]
        norm = normalized.get(x_id, 0.0)

        # Bootstrap
        if len(events) >= 2:
            bootstrap_scores = []
            events_arr = np.array(
                [(g * (1.0 / (a + epsilon))) for g, a, _, _ in events]
            )
            for _ in range(n_bootstrap):
                sample = rng.choice(events_arr, size=len(events_arr), replace=True)
                bootstrap_scores.append(float(sample.sum()))
            ci_lower = float(np.percentile(bootstrap_scores, 2.5))
            ci_upper = float(np.percentile(bootstrap_scores, 97.5))
        else:
            ci_lower = raw
            ci_upper = raw

        # Top 20 discoveries for export
        top_discoveries = sorted(events, key=lambda x: x[0], reverse=True)[:20]
        discoveries_export = [
            {
                "person_id": y_id,
                "growth": round(growth, 2),
                "birank_at_discovery": round(y_auth, 2),
                "year": year,
            }
            for growth, y_auth, year, y_id in top_discoveries
        ]

        results[x_id] = ForesightScore(
            person_id=x_id,
            foresight_raw=round(raw, 4),
            foresight_normalized=round(norm, 2),
            discoveries=discoveries_export,
            n_discoveries=len(events),
            confidence_lower=round(ci_lower, 4),
            confidence_upper=round(ci_upper, 4),
        )

    return results


# =============================================================================
# Step 4b: Foresight Holdout Validation
# =============================================================================

# Persons still active in recent years haven't reached their peak yet.
RIGHT_CENSOR_CUTOFF = 2022


def _validate_foresight_holdout(
    credits: list,
    anime_map: dict,
    yearly_normalized: dict[int, dict[str, float]],
    holdout_year: int = 2018,
) -> dict:
    """ホールドアウト検証: holdout_year 以前のデータで先見スコアを計算し、
    2019〜 のブレイク人材をどれだけ予測できるかを評価する.

    先見スコアと2つの素朴ベースライン（活動量、共演者平均birank）を比較。
    ROC-AUC と precision@k を返す。

    重要: このメトリックは「事前」ではなく「事後」のパターン検出を評価するもの。
    AUC < 0.6 の場合、指標に予測力はなく活動量やネットワーク位置で説明可能。
    """
    # Pre-holdout credits only
    pre_credits = [
        c
        for c in credits
        if (a := anime_map.get(c.anime_id)) and a.year and a.year <= holdout_year
    ]
    if not pre_credits:
        return {"error": "no_pre_holdout_credits"}

    pre_norm = {yr: sc for yr, sc in yearly_normalized.items() if yr <= holdout_year}
    post_norm = {yr: sc for yr, sc in yearly_normalized.items() if yr > holdout_year}
    if not pre_norm or not post_norm:
        return {"error": "insufficient_year_span"}

    birank_at_holdout = yearly_normalized.get(holdout_year, {})
    if len(birank_at_holdout) < 10:
        return {"error": "insufficient_data_at_holdout", "n": len(birank_at_holdout)}

    vals = list(birank_at_holdout.values())
    low_thr = float(np.percentile(vals, 40))
    high_thr = float(np.percentile(vals, 65))

    unknowns: set[str] = {pid for pid, br in birank_at_holdout.items() if br < low_thr}

    # Ground truth: unknowns who broke out post-holdout
    breakout_persons: set[str] = set()
    for scores in post_norm.values():
        for pid, br in scores.items():
            if pid in unknowns and br >= high_thr:
                breakout_persons.add(pid)

    if len(unknowns) < 20 or len(breakout_persons) < 5:
        return {
            "error": "too_few_breakouts",
            "n_unknowns": len(unknowns),
            "n_breakouts": len(breakout_persons),
        }

    # Compute signals over pre-holdout co-appearances
    credit_count: dict[str, int] = defaultdict(int)
    partner_biranks: dict[str, list[float]] = defaultdict(list)
    foresight_signal: dict[str, float] = defaultdict(float)  # keyed by unknown Y

    # Group pre-holdout credits by (year, anime)
    year_anime_persons: dict[tuple[int, str], list[str]] = defaultdict(list)
    for c in pre_credits:
        a = anime_map.get(c.anime_id)
        yr = a.year if a else None
        if yr:
            year_anime_persons[(yr, c.anime_id)].append(c.person_id)

    for (yr, anime_id), persons in year_anime_persons.items():
        yr_scores = pre_norm.get(yr, {})
        if not yr_scores:
            continue
        yr_vals = list(yr_scores.values())
        yr_low = float(np.percentile(yr_vals, 40)) if yr_vals else 25.0

        for pid in persons:
            credit_count[pid] += 1
            partners = [yr_scores.get(p, 0.0) for p in persons if p != pid]
            if partners:
                partner_biranks[pid].extend(partners)

        estabs = [p for p in persons if yr_scores.get(p, 0.0) >= yr_low]
        unks_here = [
            p for p in persons if yr_scores.get(p, 0.0) < yr_low and p in unknowns
        ]

        for x_id in estabs:
            for y_id in unks_here:
                y_br = yr_scores.get(y_id, 0.0)
                foresight_signal[y_id] += 1.0 / (y_br + 1.0)

    all_unknowns = sorted(unknowns)
    y_true = [1 if pid in breakout_persons else 0 for pid in all_unknowns]

    fs_scores = [foresight_signal.get(pid, 0.0) for pid in all_unknowns]
    bl_activity = [float(credit_count.get(pid, 0)) for pid in all_unknowns]
    bl_partner = [
        float(np.mean(partner_biranks[pid])) if partner_biranks.get(pid) else 0.0
        for pid in all_unknowns
    ]

    def _roc_auc(scores: list[float], labels: list[int]) -> float:
        """Wilcoxon-Mann-Whitney U statistic = AUC."""
        pos = [s for s, lbl in zip(scores, labels) if lbl == 1]
        neg = [s for s, lbl in zip(scores, labels) if lbl == 0]
        if not pos or not neg:
            return 0.5
        concordant = sum(p > n for p in pos for n in neg)
        tied = sum(p == n for p in pos for n in neg)
        total = len(pos) * len(neg)
        return (concordant + 0.5 * tied) / total

    def _precision_at_k(scores: list[float], labels: list[int], k: int) -> float:
        paired = sorted(zip(scores, labels), reverse=True)
        return sum(lbl for _, lbl in paired[:k]) / k if k > 0 else 0.0

    n = len(all_unknowns)
    ks = sorted({k for k in [10, 20, 50, 100] if k <= n // 2})
    if not ks:
        ks = [max(1, n // 5)]

    return {
        "holdout_year": holdout_year,
        "n_unknowns": len(unknowns),
        "n_breakouts": len(breakout_persons),
        "breakout_rate": round(len(breakout_persons) / max(len(unknowns), 1), 3),
        "roc_auc": {
            "foresight": round(_roc_auc(fs_scores, y_true), 3),
            "baseline_activity": round(_roc_auc(bl_activity, y_true), 3),
            "baseline_partner_birank": round(_roc_auc(bl_partner, y_true), 3),
        },
        "precision_at_k": {
            str(k): {
                "foresight": round(_precision_at_k(fs_scores, y_true, k), 3),
                "baseline_activity": round(_precision_at_k(bl_activity, y_true, k), 3),
                "baseline_partner_birank": round(
                    _precision_at_k(bl_partner, y_true, k), 3
                ),
                "random_baseline": round(
                    len(breakout_persons) / max(len(unknowns), 1), 3
                ),
            }
            for k in ks
        },
        "interpretation": (
            "retrospective_pattern_detection: AUC > 0.6 suggests the co-appearance signal "
            "carries information beyond activity counts alone. AUC near 0.5 indicates no "
            "predictive value beyond random."
        ),
        "caveats": [
            "Breakout definition: birank < 40th pct at holdout_year, > 65th pct thereafter",
            "Foresight signal: sum of 1/(y_birank+1) for each pre-holdout co-appearance",
            "Recent foresight scorers systematically underscored (discovery-verification lag)",
        ],
    }


# =============================================================================
# Step 4c: Beta-Binomial Shrinkage
# =============================================================================


def _beta_binomial_shrinkage(
    successes: list[int],
    trials: list[int],
) -> list[float]:
    """Beta-Binomial 経験ベイズ縮約.

    素の成功率 s/n ではなく posterior mean = (alpha0+s)/(alpha0+beta0+n) を返す。
    試行回数が少ない人物の極端な成功率を母集団平均方向に縮小する。

    Prior Beta(alpha0, beta0) はモーメント法で母集団から推定する。
    """
    if not trials or all(n == 0 for n in trials):
        return [0.0] * len(trials)

    rates = [s / n for s, n in zip(successes, trials) if n > 0]
    if len(rates) < 2:
        return [s / max(n, 1) for s, n in zip(successes, trials)]

    mu = float(np.mean(rates))
    var = float(np.var(rates))

    # Method of moments: alpha = mu * precision, beta = (1-mu) * precision
    # where precision = mu*(1-mu)/var - 1
    if var <= 1e-9 or mu <= 0 or mu >= 1:
        alpha0, beta0 = 1.0, 1.0
    else:
        precision = mu * (1.0 - mu) / var - 1.0
        if precision <= 0:
            alpha0, beta0 = 1.0, 1.0
        else:
            alpha0 = max(mu * precision, 0.1)
            beta0 = max((1.0 - mu) * precision, 0.1)

    return [
        (alpha0 + s) / (alpha0 + beta0 + n) if n > 0 else alpha0 / (alpha0 + beta0)
        for s, n in zip(successes, trials)
    ]


# =============================================================================
# Step 5: Promotion Detection
# =============================================================================


def _detect_promotions(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    birank_timelines: dict[str, BirankTimeline],
    min_promotions: int = 2,
) -> dict[str, PromotionCredit]:
    """抜擢イベントを検出し、上位者に帰属する.

    Algorithm:
    1. Build CAREER_STAGE timeline per person
    2. Detect stage jumps (new_stage > previous_max_stage)
    3. Attribute to highest-stage person on same anime
    4. Compute 4-factor confidence (geometric mean):
       - repeated_pattern: min(events/5, 1.0)
       - baseline_ratio: vs cohort average promotion rate
       - exclusivity: not promoted elsewhere same year
       - studio_effect: controlled for studio promotion rate

    Returns:
        {person_id: PromotionCredit} for persons meeting min_promotions threshold
    """
    # Build per-person career stage timeline: {person_id: {year: max_stage}}
    person_year_stage: dict[str, dict[int, int]] = defaultdict(dict)
    # Track credits per anime: {anime_id: [(person_id, role, stage)]}
    anime_staff: dict[str, list[tuple[str, Role, int]]] = defaultdict(list)
    # Track studios per anime
    anime_studios: dict[str, str | None] = {}

    for c in credits:
        anime = anime_map.get(c.anime_id)
        yr = anime.year if anime and anime.year else None
        if yr is None:
            continue

        stage = CAREER_STAGE.get(c.role, 0)
        if stage == 0:
            continue

        current = person_year_stage[c.person_id].get(yr, 0)
        person_year_stage[c.person_id][yr] = max(current, stage)
        anime_staff[c.anime_id].append((c.person_id, c.role, stage))

        if c.anime_id not in anime_studios:
            studio = None
            if anime and hasattr(anime, "studio"):
                studio = getattr(anime, "studio", None)
            anime_studios[c.anime_id] = studio

    # Pre-build credit index: {(person_id, year): [(anime_id, stage)]}
    # Replaces O(events × C) full-credit scan with O(1) lookup
    person_year_credits: dict[tuple[str, int], list[tuple[str, int]]] = defaultdict(
        list
    )
    for c in credits:
        anime = anime_map.get(c.anime_id)
        yr = anime.year if anime and anime.year else None
        if yr is None:
            continue
        stage = CAREER_STAGE.get(c.role, 0)
        if stage > 0:
            person_year_credits[(c.person_id, yr)].append((c.anime_id, stage))

    # Detect promotion events
    all_events: list[PromotionEvent] = []
    # Track per-person promotions per year for exclusivity check
    person_year_promotions: dict[str, dict[int, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for person_id, year_stages in person_year_stage.items():
        max_stage_so_far = 0
        for year in sorted(year_stages.keys()):
            stage = year_stages[year]
            if stage > max_stage_so_far and max_stage_so_far > 0:
                # Find anime where this promotion happened — O(1) index lookup
                for anime_id, c_stage in person_year_credits.get((person_id, year), []):
                    if c_stage == stage:
                        event = PromotionEvent(
                            promotee_id=person_id,
                            anime_id=anime_id,
                            year=year,
                            previous_max_stage=max_stage_so_far,
                            new_stage=stage,
                            stage_jump=stage - max_stage_so_far,
                        )
                        all_events.append(event)
                        person_year_promotions[person_id][year].append(anime_id)
                        break  # One event per year per person

            max_stage_so_far = max(max_stage_so_far, stage)

    # Attribute events to supervisors
    for event in all_events:
        staff = anime_staff.get(event.anime_id, [])
        # Find highest-stage person who is not the promotee
        best_supervisor = None
        best_stage = 0
        for pid, role, stage in staff:
            if pid == event.promotee_id:
                continue
            if stage >= event.new_stage and stage > best_stage:
                best_stage = stage
                best_supervisor = pid
        event.attributed_to = best_supervisor

    # Group events by attributed supervisor
    supervisor_events: dict[str, list[PromotionEvent]] = defaultdict(list)
    for event in all_events:
        if event.attributed_to:
            supervisor_events[event.attributed_to].append(event)

    # Compute confidence factors and build PromotionCredits
    # Precompute: stage-based promotion rates for baseline comparison
    stage_promotion_counts: dict[int, int] = defaultdict(int)
    stage_person_counts: dict[int, int] = defaultdict(int)
    for person_id, year_stages in person_year_stage.items():
        max_so_far = 0
        for year in sorted(year_stages.keys()):
            stage = year_stages[year]
            if max_so_far > 0:
                stage_person_counts[max_so_far] += 1
                if stage > max_so_far:
                    stage_promotion_counts[max_so_far] += 1
            max_so_far = max(max_so_far, stage)

    stage_baseline_rate: dict[int, float] = {}
    for stage, count in stage_person_counts.items():
        if count > 0:
            stage_baseline_rate[stage] = stage_promotion_counts.get(stage, 0) / count

    # Precompute: studio promotion rates
    studio_promotion_counts: dict[str, int] = defaultdict(int)
    studio_opportunity_counts: dict[str, int] = defaultdict(int)
    for event in all_events:
        studio = anime_studios.get(event.anime_id)
        if studio:
            studio_promotion_counts[studio] += 1
    for anime_id, staff in anime_staff.items():
        studio = anime_studios.get(anime_id)
        if studio:
            studio_opportunity_counts[studio] += len(staff)

    studio_rates: dict[str, float] = {}
    for studio, opp in studio_opportunity_counts.items():
        if opp > 0:
            studio_rates[studio] = studio_promotion_counts.get(studio, 0) / opp

    results: dict[str, PromotionCredit] = {}

    for supervisor_id, events in supervisor_events.items():
        if len(events) < min_promotions:
            continue

        # Successful promotions: promotee continued to higher stages after promotion.
        # Computed first — needed for vs_cohort_baseline (true lift).
        successful = 0
        for event in events:
            promotee_stages = person_year_stage.get(event.promotee_id, {})
            future_stages = [s for yr, s in promotee_stages.items() if yr > event.year]
            if future_stages and max(future_stages) >= event.new_stage:
                successful += 1

        actual_success_rate = successful / len(events) if events else 0.0

        # Stage-cohort baseline: average population success rate for the stages promoted FROM.
        baseline_rates = []
        for event in events:
            br = stage_baseline_rate.get(event.previous_max_stage, 0.1)
            baseline_rates.append(br)
        avg_baseline = (
            sum(baseline_rates) / len(baseline_rates) if baseline_rates else 0.1
        )

        # True lift: supervisor actual success rate / stage-cohort baseline.
        # e.g., 2.3 means 2.3× the population rate for those stage transitions.
        vs_cohort_lift = actual_success_rate / max(avg_baseline, 0.01)

        # Factor 1: Repeated pattern — min(events/5, 1.0)
        repeated_factor = min(len(events) / 5.0, 1.0)

        # Factor 2: Baseline factor for confidence — normalize lift to [0, 1].
        # Floor at 0.05: successful=0 may reflect data truncation (no future credits),
        # not confirmed failure. Keeps the geometric mean non-zero when other factors
        # carry signal.
        baseline_factor = max(min(vs_cohort_lift / 3.0, 1.0), 0.05)

        # Factor 3: Exclusivity — promotees not promoted elsewhere in same year
        exclusive_count = 0
        for event in events:
            year_promos = person_year_promotions.get(event.promotee_id, {}).get(
                event.year, []
            )
            if len(year_promos) <= 1:
                exclusive_count += 1
        exclusivity = exclusive_count / len(events) if events else 0.0

        # Factor 4: Studio effect — supervisor success rate vs studio baseline
        studio_factors = []
        for event in events:
            studio = anime_studios.get(event.anime_id)
            if studio and studio in studio_rates:
                sr = studio_rates[studio]
                studio_factors.append(
                    min(actual_success_rate / max(sr, 0.01), 3.0) / 3.0
                )
            else:
                studio_factors.append(0.5)  # neutral when no studio data
        studio_adjusted = (
            sum(studio_factors) / len(studio_factors) if studio_factors else 0.5
        )

        # Geometric mean of 4 factors → confidence in [0, 1]
        factors = [repeated_factor, baseline_factor, exclusivity, studio_adjusted]
        confidence = float(np.prod(factors) ** (1.0 / len(factors)))

        # Set per-event confidence
        for event in events:
            event.confidence = confidence

        results[supervisor_id] = PromotionCredit(
            person_id=supervisor_id,
            promotion_count=len(events),
            successful_promotions=successful,
            promotion_success_rate=round(actual_success_rate, 4),
            vs_cohort_baseline=round(vs_cohort_lift, 3),
            exclusivity_score=exclusivity,
            studio_adjusted_rate=studio_adjusted,
            confidence=round(confidence, 4),
            events=events,
        )

    return results


# =============================================================================
# Entry Point
# =============================================================================


def compute_temporal_pagerank(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    persons: list,
    peer_edge_weight: float = 0.3,
    foresight_horizon_years: int = 10,
    unknown_threshold_percentile: float = 25.0,
    min_promotions_for_credit: int = 2,
    resume_checkpoint: StreamingCheckpoint | None = None,
) -> tuple[TemporalPageRankResult, StreamingCheckpoint | None]:
    """時系列PageRankの全計算を実行する.

    Args:
        credits: 全クレジットデータ
        anime_map: anime_id -> Anime
        persons: 人物リスト
        peer_edge_weight: 同僚エッジの基本重み (default: 0.3)
        foresight_horizon_years: 先見スコアの観測期間 (default: 5)
        unknown_threshold_percentile: 「無名」の閾値パーセンタイル (default: 25.0)
        min_promotions_for_credit: 抜擢クレジットの最小回数 (default: 2)
        resume_checkpoint: ストリーミングチェックポイント。渡された場合は
            checkpoint.last_year 以降の年だけ計算する。

    Returns:
        (TemporalPageRankResult, final_checkpoint)
        final_checkpoint は次回呼び出し時の resume_checkpoint として使える。
    """
    start_time = time.monotonic()

    if not credits:
        return TemporalPageRankResult(), None

    # Step 1+2: Build yearly graphs incrementally and run PageRank on each.
    yearly_scores, yearly_person_nodes, yearly_person_count, final_checkpoint = (
        _build_and_score_yearly_streaming(
            credits,
            anime_map,
            persons,
            peer_edge_weight,
            resume_checkpoint=resume_checkpoint,
        )
    )
    if not yearly_scores:
        return TemporalPageRankResult()

    # Build normalized scores per year (person nodes only) for foresight
    yearly_normalized: dict[int, dict[str, float]] = {}
    for year, scores in yearly_scores.items():
        person_nodes = yearly_person_nodes[year]
        person_scores = {k: v for k, v in scores.items() if k in person_nodes}
        yearly_normalized[year] = normalize_scores(person_scores)

    # Step 3: Build birank timelines
    birank_timelines = _build_birank_timelines_from_counts(
        yearly_scores, yearly_person_count, yearly_normalized, credits, anime_map
    )

    # Step 4: Compute foresight scores
    foresight_scores = _compute_foresight_scores(
        birank_timelines,
        credits,
        anime_map,
        yearly_normalized,
        foresight_horizon_years=foresight_horizon_years,
        unknown_threshold_percentile=unknown_threshold_percentile,
    )

    # Step 5: Detect promotions
    promotion_credits = _detect_promotions(
        credits,
        anime_map,
        birank_timelines,
        min_promotions=min_promotions_for_credit,
    )

    # Step 6: Name resolution — attach display names to all result objects.
    person_name_map: dict[str, str] = {p.id: p.display_name for p in persons}
    for pid, tl in birank_timelines.items():
        tl.name = person_name_map.get(pid, "")
    for pid, fs in foresight_scores.items():
        fs.name = person_name_map.get(pid, "")
        for disc in fs.discoveries:
            disc["name"] = person_name_map.get(disc.get("person_id", ""), "")
    for pid, pc in promotion_credits.items():
        pc.name = person_name_map.get(pid, "")

    # Step 7: Beta-Binomial shrinkage on promotion success rates.
    if promotion_credits:
        pc_list = list(promotion_credits.values())
        shrunk = _beta_binomial_shrinkage(
            [pc.successful_promotions for pc in pc_list],
            [pc.promotion_count for pc in pc_list],
        )
        for pc, s in zip(pc_list, shrunk):
            pc.shrunk_success_rate = round(s, 4)

    # Step 8: Mark right-censored timelines (person still active — peak not yet reached).
    for tl in birank_timelines.values():
        tl.is_censored = (
            tl.latest_year is not None and tl.latest_year >= RIGHT_CENSOR_CUTOFF
        )

    # Step 9: Foresight holdout validation (train ≤ 2018, test 2019+).
    holdout_result = _validate_foresight_holdout(
        credits, anime_map, yearly_normalized, holdout_year=2018
    )

    elapsed = time.monotonic() - start_time
    years_computed = sorted(yearly_scores.keys())

    logger.info(
        "temporal_pagerank_computed",
        years=len(years_computed),
        timelines=len(birank_timelines),
        foresight_persons=len(foresight_scores),
        promotions=sum(pc.promotion_count for pc in promotion_credits.values()),
        holdout_roc=holdout_result.get("roc_auc", {}).get("foresight"),
        elapsed_seconds=round(elapsed, 2),
    )

    result = TemporalPageRankResult(
        birank_timelines={pid: asdict(tl) for pid, tl in birank_timelines.items()},
        foresight_scores={pid: asdict(fs) for pid, fs in foresight_scores.items()},
        promotion_credits={pid: asdict(pc) for pid, pc in promotion_credits.items()},
        years_computed=years_computed,
        total_persons=len(birank_timelines),
        computation_time_seconds=round(elapsed, 2),
        holdout_validation=holdout_result,
    )
    return result, final_checkpoint

"""時系列PageRank — 年次スナップショットによる動的Authority評価.

年ごとの累積グラフでPageRankを実行し、Authority推移・先見スコア・抜擢検出を算出する。
Phase 9 分析モジュールとして動作（コアスコアリングには影響しない独立した補足分析）。

Components:
1. 年次スナップショットPageRank (warm start)
2. 同僚エッジ (Peer Edges) — 同役職カテゴリの人同士をグラフに追加
3. 先見スコア (Foresight) — 無名時の共演者が後に成長した場合のボーナス
4. 抜擢検出 (Promotion Detection) — 格上げ起用の検出と信頼度付き帰属
"""

import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field

import networkx as nx
import numpy as np
import structlog

from src.analysis.career import CAREER_STAGE
from src.analysis.pagerank import normalize_scores, weighted_pagerank
from src.models import Anime, Credit, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.role_groups import ROLE_CATEGORY

logger = structlog.get_logger()

# =============================================================================
# Data Structures
# =============================================================================

PEER_EDGE_CAP = 15  # Max persons per category per anime for peer edges


@dataclass(frozen=True)
class YearlyAuthoritySnapshot:
    """1年分のAuthority評価."""

    year: int
    authority: float  # 0-100 normalized within year
    raw_pagerank: float
    graph_size: int  # person nodes count
    n_credits_cumulative: int


@dataclass
class AuthorityTimeline:
    """人物の年次Authority推移."""

    person_id: str
    snapshots: list[YearlyAuthoritySnapshot] = field(default_factory=list)
    peak_year: int | None = None
    peak_authority: float = 0.0
    career_start_year: int | None = None
    latest_year: int | None = None
    trajectory: str = "stable"  # "rising" | "stable" | "declining" | "peaked"


@dataclass
class ForesightScore:
    """先見スコア — 無名時の共演者が後に成長した場合のボーナス."""

    person_id: str
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
    """抜擢クレジット — ある上位者が何人を格上げしたか."""

    person_id: str
    promotion_count: int = 0
    successful_promotions: int = 0
    promotion_success_rate: float = 0.0
    vs_cohort_baseline: float = 0.0  # 同コホートの平均昇格率との比
    exclusivity_score: float = 0.0  # 他作品での同時昇格がない度合い
    studio_adjusted_rate: float = 0.0  # スタジオ効果を統制後
    confidence: float = 0.0  # 4因子の幾何平均
    events: list[PromotionEvent] = field(default_factory=list)


@dataclass
class TemporalPageRankResult:
    """時系列PageRankの全結果."""

    authority_timelines: dict = field(default_factory=dict)  # person_id -> dict
    foresight_scores: dict = field(default_factory=dict)  # person_id -> dict
    promotion_credits: dict = field(default_factory=dict)  # person_id -> dict
    years_computed: list[int] = field(default_factory=list)
    total_persons: int = 0
    computation_time_seconds: float = 0.0


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
        importance = max(anime.score / 10.0, 0.1) if (anime and anime.score) else 0.5

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
# Step 2: Warm Start PageRank
# =============================================================================


def _run_yearly_pagerank_with_warm_start(
    yearly_graphs: dict[int, nx.DiGraph],
) -> dict[int, dict[str, float]]:
    """年ごとにPageRankを実行し、前年のスコアを初期値に使う (warm start).

    Returns:
        {year: {node_id: raw_pagerank_score}}
    """
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
            # Build nstart from previous year's scores
            nodes = set(graph.nodes())
            nstart = {}
            n_new = 0
            for node in nodes:
                if node in prev_scores:
                    nstart[node] = prev_scores[node]
                else:
                    n_new += 1
                    nstart[node] = 1.0 / len(nodes)

            # Re-normalize so sum = 1
            total = sum(nstart.values())
            if total > 0:
                nstart = {k: v / total for k, v in nstart.items()}

        scores = weighted_pagerank(graph, nstart=nstart)
        yearly_scores[year] = scores
        prev_scores = scores

    return yearly_scores


# =============================================================================
# Step 3: Authority Timeline Construction
# =============================================================================


def _build_authority_timelines(
    yearly_scores: dict[int, dict[str, float]],
    yearly_graphs: dict[int, nx.DiGraph],
    yearly_normalized: dict[int, dict[str, float]],
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, AuthorityTimeline]:
    """年次PageRankスコアからAuthorityタイムラインを構築する.

    Args:
        yearly_scores: {year: {node_id: raw_pagerank}}
        yearly_graphs: {year: DiGraph}
        yearly_normalized: {year: {person_id: 0-100 normalized score}} — 事前計算済み
        credits: 全クレジット
        anime_map: anime_id -> Anime

    Returns:
        {person_id: AuthorityTimeline}
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

    # Precompute person node count per year — O(Y×V) total, not O(P×Y×V)
    yearly_person_count: dict[int, int] = {}
    for year, graph in yearly_graphs.items():
        yearly_person_count[year] = sum(
            1 for n in graph.nodes() if graph.nodes[n].get("type") == "person"
        )

    # Build timelines
    timelines: dict[str, AuthorityTimeline] = {}
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
                YearlyAuthoritySnapshot(
                    year=year,
                    authority=normalized[person_id],
                    raw_pagerank=raw.get(person_id, 0.0),
                    graph_size=yearly_person_count.get(year, 0),
                    n_credits_cumulative=cumulative_by_year.get(year, 0),
                )
            )

        if not snapshots:
            continue

        # Find peak
        peak_snap = max(snapshots, key=lambda s: s.authority)
        # Determine trajectory
        trajectory = _classify_trajectory(snapshots)

        timelines[person_id] = AuthorityTimeline(
            person_id=person_id,
            snapshots=snapshots,
            peak_year=peak_snap.year,
            peak_authority=peak_snap.authority,
            career_start_year=snapshots[0].year,
            latest_year=snapshots[-1].year,
            trajectory=trajectory,
        )

    return timelines


def _classify_trajectory(snapshots: list[YearlyAuthoritySnapshot]) -> str:
    """スナップショット列からキャリア軌道を分類する.

    Returns: "rising" | "stable" | "declining" | "peaked"
    """
    if len(snapshots) < 2:
        return "stable"

    authorities = [s.authority for s in snapshots]
    n = len(authorities)

    # Use last third vs first third comparison
    third = max(n // 3, 1)
    early_avg = sum(authorities[:third]) / third
    late_avg = sum(authorities[-third:]) / third
    peak = max(authorities)
    peak_idx = authorities.index(peak)

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
    authority_timelines: dict[str, AuthorityTimeline],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    yearly_normalized: dict[int, dict[str, float]],
    foresight_horizon_years: int = 10,
    unknown_threshold_percentile: float = 25.0,
) -> dict[str, ForesightScore]:
    """先見スコアを計算する.

    Algorithm:
    1. Year T: find "unknown" Y (authority < 25th percentile) and "established" X (> 25th)
       who co-appear in the same anime
    2. If Y's authority grows by 10+ points within T+5 years -> X gets foresight credit
    3. foresight(X) = sum(growth(Y) * 1/(authority_T(Y) + epsilon))
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
            year_thresholds[year] = float(np.percentile(vals, unknown_threshold_percentile))

    # Build per-person authority by year lookup
    person_year_authority: dict[str, dict[int, float]] = defaultdict(dict)
    for pid, timeline in authority_timelines.items():
        for snap in timeline.snapshots:
            person_year_authority[pid][snap.year] = snap.authority

    years = sorted(yearly_normalized.keys())

    # Collect discovery events: {established_person: [(growth, unknown_authority, year, unknown_id)]}
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
                y_authority_t = year_scores.get(y_id, 0.0)
                # Check future growth within horizon
                future_authority = 0.0
                for future_year in range(year + 1, year + foresight_horizon_years + 1):
                    fa = person_year_authority.get(y_id, {}).get(future_year)
                    if fa is not None:
                        future_authority = max(future_authority, fa)

                growth = future_authority - y_authority_t
                if growth < 5.0:
                    continue

                # Credit all established co-workers
                for x_id in established:
                    discoveries_raw[x_id].append((growth, y_authority_t, year, y_id))

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
                "authority_at_discovery": round(y_auth, 2),
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
# Step 5: Promotion Detection
# =============================================================================


def _detect_promotions(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    authority_timelines: dict[str, AuthorityTimeline],
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
    person_year_credits: dict[tuple[str, int], list[tuple[str, int]]] = defaultdict(list)
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
                for anime_id, c_stage in person_year_credits.get(
                    (person_id, year), []
                ):
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

        # Factor 1: Repeated pattern — min(events/5, 1.0)
        repeated_factor = min(len(events) / 5.0, 1.0)

        # Factor 2: Baseline comparison
        # Average promotion rate for the stages the supervisor promoted FROM
        baseline_rates = []
        for event in events:
            br = stage_baseline_rate.get(event.previous_max_stage, 0.1)
            baseline_rates.append(br)
        avg_baseline = sum(baseline_rates) / len(baseline_rates) if baseline_rates else 0.1
        supervisor_rate = len(events) / max(len(events) + 5, 1)  # smoothed rate
        baseline_ratio = min(supervisor_rate / max(avg_baseline, 0.01), 3.0) / 3.0

        # Factor 3: Exclusivity — promotees not promoted elsewhere in same year
        exclusive_count = 0
        for event in events:
            year_promos = person_year_promotions.get(event.promotee_id, {}).get(
                event.year, []
            )
            if len(year_promos) <= 1:
                exclusive_count += 1
        exclusivity = exclusive_count / len(events) if events else 0.0

        # Factor 4: Studio effect — supervisor promotes more than studio average
        studio_factors = []
        for event in events:
            studio = anime_studios.get(event.anime_id)
            if studio and studio in studio_rates:
                sr = studio_rates[studio]
                studio_factors.append(min(supervisor_rate / max(sr, 0.01), 3.0) / 3.0)
            else:
                studio_factors.append(0.5)  # neutral when no studio data
        studio_adjusted = (
            sum(studio_factors) / len(studio_factors) if studio_factors else 0.5
        )

        # Geometric mean of 4 factors
        factors = [repeated_factor, baseline_ratio, exclusivity, studio_adjusted]
        confidence = float(np.prod(factors) ** (1.0 / len(factors)))

        # Set per-event confidence
        for event in events:
            event.confidence = confidence

        # Successful promotions: promotee continued to higher stages after promotion
        successful = 0
        for event in events:
            promotee_stages = person_year_stage.get(event.promotee_id, {})
            future_stages = [
                s for yr, s in promotee_stages.items() if yr > event.year
            ]
            if future_stages and max(future_stages) >= event.new_stage:
                successful += 1

        results[supervisor_id] = PromotionCredit(
            person_id=supervisor_id,
            promotion_count=len(events),
            successful_promotions=successful,
            promotion_success_rate=successful / len(events) if events else 0.0,
            vs_cohort_baseline=baseline_ratio,
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
) -> TemporalPageRankResult:
    """時系列PageRankの全計算を実行する.

    Args:
        credits: 全クレジットデータ
        anime_map: anime_id -> Anime
        persons: 人物リスト
        peer_edge_weight: 同僚エッジの基本重み (default: 0.3)
        foresight_horizon_years: 先見スコアの観測期間 (default: 5)
        unknown_threshold_percentile: 「無名」の閾値パーセンタイル (default: 25.0)
        min_promotions_for_credit: 抜擢クレジットの最小回数 (default: 2)

    Returns:
        TemporalPageRankResult with all computed data
    """
    start_time = time.monotonic()

    if not credits:
        return TemporalPageRankResult()

    # Step 1: Build yearly cumulative graphs with peer edges
    yearly_graphs = _build_yearly_cumulative_graphs(
        credits, anime_map, persons, peer_edge_weight
    )
    if not yearly_graphs:
        return TemporalPageRankResult()

    # Step 2: Run warm-start PageRank per year
    yearly_scores = _run_yearly_pagerank_with_warm_start(yearly_graphs)

    # Build normalized scores per year (person nodes only) for foresight
    yearly_normalized: dict[int, dict[str, float]] = {}
    for year, scores in yearly_scores.items():
        graph = yearly_graphs[year]
        person_scores = {
            k: v for k, v in scores.items() if graph.nodes[k].get("type") == "person"
        }
        yearly_normalized[year] = normalize_scores(person_scores)

    # Step 3: Build authority timelines (reuses yearly_normalized — no recomputation)
    authority_timelines = _build_authority_timelines(
        yearly_scores, yearly_graphs, yearly_normalized, credits, anime_map
    )

    # Step 4: Compute foresight scores
    foresight_scores = _compute_foresight_scores(
        authority_timelines,
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
        authority_timelines,
        min_promotions=min_promotions_for_credit,
    )

    elapsed = time.monotonic() - start_time
    years_computed = sorted(yearly_graphs.keys())

    logger.info(
        "temporal_pagerank_computed",
        years=len(years_computed),
        timelines=len(authority_timelines),
        foresight_persons=len(foresight_scores),
        promotions=sum(pc.promotion_count for pc in promotion_credits.values()),
        elapsed_seconds=round(elapsed, 2),
    )

    return TemporalPageRankResult(
        authority_timelines={
            pid: asdict(tl) for pid, tl in authority_timelines.items()
        },
        foresight_scores={pid: asdict(fs) for pid, fs in foresight_scores.items()},
        promotion_credits={
            pid: asdict(pc) for pid, pc in promotion_credits.items()
        },
        years_computed=years_computed,
        total_persons=len(authority_timelines),
        computation_time_seconds=round(elapsed, 2),
    )

"""Multilayer Network Analysis — 役職レイヤー別ネットワーク分析.

アニメクレジットを役職カテゴリで分離し、4つのレイヤーグラフを構築する。
各レイヤーでの betweenness / degree centrality を計算することで
「原画レイヤーでは中心だが演出レイヤーでは周辺」といった役職固有の位置を可視化する。

加えて、各人物の初期クレジットからキャリア畑（career_track）を推定する。
アニメーター出身の監督と脚本・演出畑出身の監督は異なるノード属性として扱い、
Gould-Fernandez ブローカー役割分析の group 定義に使用する。

References:
    - Mucha et al. (2010). Community structure in time-dependent, multiscale,
      and multiplex networks. Science, 328(5980), 876-878.
    - De Domenico et al. (2014). Mathematical formulation of multilayer networks.
      Physical Review X, 3(4), 041022.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import networkx as nx
import structlog

from src.utils.role_groups import ROLE_CATEGORY

if TYPE_CHECKING:
    from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


# =============================================================================
# Layer Definitions
# =============================================================================

#: role_category → layer_name の対応表（4レイヤー粗粒化）
ROLE_CATEGORY_TO_LAYER: dict[str, str] = {
    # Direction layer
    "direction": "direction",
    # Animation layer（作画・デザイン・仕上げ全般）
    "animation_supervision": "animation",
    "animation": "animation",
    "design": "animation",
    "layout": "animation",
    "finishing": "animation",
    "editing": "animation",
    "settings": "animation",
    # Production layer
    "production": "production",
    "production_management": "production",
    # Technical layer（撮影・CG・美術・音楽・脚本）
    "technical": "technical",
    "art": "technical",
    "sound": "technical",
    "writing": "technical",
}

LAYER_NAMES: list[str] = ["direction", "animation", "production", "technical"]

#: career track names (values of career_track)
#: animator_director = アニメーター出身で後に監督となった人物（宮崎駿・庵野秀明・山田尚子など）
CAREER_TRACKS: list[str] = [
    "animator",  # 原画・作監など、アニメーター工程が主体
    "animator_director",  # アニメーター出身で演出・監督へ転向した人物
    "director",  # 脚本・演出畑からのディレクター（アニメーター経験なし）
    "production",  # 制作進行・プロデューサー畑
    "technical",  # 撮影・CG・美術・音楽・脚本畑
    "multi_track",  # 複数畑・分類困難
]

#: role_category → career_track の基本マッピング（初期クレジット判定用）
ROLE_CATEGORY_TO_TRACK: dict[str, str] = {
    "animation_supervision": "animator",
    "animation": "animator",
    "design": "animator",
    "layout": "animator",
    "finishing": "animator",
    "editing": "animator",
    "settings": "animator",
    "direction": "director",
    "production": "production",
    "production_management": "production",
    "technical": "technical",
    "art": "technical",
    "sound": "technical",
    "writing": "technical",
    "non_production": "multi_track",  # 声優・原作者等は分類困難
}


# =============================================================================
# Dataclass
# =============================================================================


@dataclass
class MultilayerCentrality:
    """Centrality metrics per layer.

    Attributes:
        person_id: person_id
        direction_betweenness: direction レイヤーの betweenness centrality
        animation_betweenness: animation レイヤーの betweenness centrality
        production_betweenness: production レイヤーの betweenness centrality
        technical_betweenness: technical レイヤーの betweenness centrality
        direction_degree: direction レイヤーの normalized degree
        animation_degree: animation レイヤーの normalized degree
        production_degree: production レイヤーの normalized degree
        technical_degree: technical レイヤーの normalized degree
        layer_count: 何レイヤーに出現するか（0-4）
        career_track: キャリア畑の推定値
        aggregate_betweenness: 全レイヤー betweenness の平均（ランキング用）
    """

    person_id: str
    direction_betweenness: float = 0.0
    animation_betweenness: float = 0.0
    production_betweenness: float = 0.0
    technical_betweenness: float = 0.0
    direction_degree: float = 0.0
    animation_degree: float = 0.0
    production_degree: float = 0.0
    technical_degree: float = 0.0
    layer_count: int = 0
    career_track: str = "multi_track"
    aggregate_betweenness: float = 0.0
    layers_active: list[str] = field(default_factory=list)


# =============================================================================
# Career Track Inference
# =============================================================================


def infer_career_track(
    person_id: str,
    person_credits: list[Credit],
    anime_map: dict[str, Anime],
) -> str:
    """Infer career track from early credits, distinguishing animator_director explicitly.

    判定ロジック:
    1. 初期3年クレジット（少ない場合はキャリア全体）の最頻役職カテゴリ → 基本 track
    2. 基本 track が 'animator' かつ、後半クレジットに direction カテゴリが出現する場合
       → 'animator_director' に昇格（アニメーター出身の監督）
    3. 過半数を占めるカテゴリがない → 'multi_track'

    Args:
        person_id: 対象の person_id
        person_credits: この人物の全クレジット（絞り込み済み）
        anime_map: anime_id → Anime

    Returns:
        career_track 文字列
        ('animator'/'animator_director'/'director'/'production'/'technical'/'multi_track')
    """
    if not person_credits:
        return "multi_track"

    # identify debut year
    years = []
    for c in person_credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            years.append(anime.year)

    if not years:
        return "multi_track"

    debut_year = min(years)
    latest_year = max(years)
    career_span = max(latest_year - debut_year, 1)
    early_cutoff = debut_year + 3

    # get early credits (first 3 years)
    early_credits = [
        c
        for c in person_credits
        if (
            anime_map.get(c.anime_id)
            and anime_map[c.anime_id].year
            and anime_map[c.anime_id].year <= early_cutoff
        )
    ]

    # if early credits are sparse, use entire career
    target_credits = early_credits if len(early_credits) >= 3 else person_credits

    # count role categories
    track_counts: Counter[str] = Counter()
    for c in target_credits:
        cat = ROLE_CATEGORY.get(c.role, "non_production")
        track = ROLE_CATEGORY_TO_TRACK.get(cat, "multi_track")
        if track != "multi_track":
            track_counts[track] += 1

    if not track_counts:
        return "multi_track"

    total = sum(track_counts.values())
    top_track, top_count = track_counts.most_common(1)[0]

    # if no category is a majority, assign multi_track
    if top_count / total < 0.5:
        return "multi_track"

    base_track = top_track

    # animator 畑で、かつキャリア後半に direction クレジットが出現するか確認
    # (animator_director determination)
    if base_track == "animator" and career_span >= 5:
        # second half of career = career_span/2 years after debut
        mid_year = debut_year + career_span // 2
        late_credits = [
            c
            for c in person_credits
            if (
                anime_map.get(c.anime_id)
                and anime_map[c.anime_id].year
                and anime_map[c.anime_id].year >= mid_year
            )
        ]
        late_cats = {ROLE_CATEGORY.get(c.role, "non_production") for c in late_credits}
        if "direction" in late_cats:
            return "animator_director"

    return base_track


def infer_all_career_tracks(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, str]:
    """Infer career tracks for all persons in batch.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        person_id → career_track の辞書
    """
    # person_id → credits のマッピングを構築
    person_credits_map: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits_map[c.person_id].append(c)

    career_tracks: dict[str, str] = {}
    for person_id, person_creds in person_credits_map.items():
        career_tracks[person_id] = infer_career_track(
            person_id, person_creds, anime_map
        )

    track_dist = Counter(career_tracks.values())
    logger.info(
        "career_tracks_inferred",
        total_persons=len(career_tracks),
        distribution=dict(track_dist),
    )
    return career_tracks


# =============================================================================
# Layer Graph Construction
# =============================================================================


def build_layer_graphs(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, nx.Graph]:
    """Build four layer graphs filtered by role category.

    各レイヤーのグラフは、そのレイヤーに属する役職でクレジットされた人物同士の
    コラボレーションエッジのみを含む。

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        layer_name → nx.Graph の辞書
    """
    # classify credits by layer
    layer_credits: dict[str, list[Credit]] = {name: [] for name in LAYER_NAMES}

    for c in credits:
        cat = ROLE_CATEGORY.get(c.role, "non_production")
        layer = ROLE_CATEGORY_TO_LAYER.get(cat)
        if layer:
            layer_credits[layer].append(c)

    # build collaboration graph per layer
    # connect persons who participated in the same work within the same layer
    layer_graphs: dict[str, nx.Graph] = {}

    for layer_name, layer_creds in layer_credits.items():
        G = nx.Graph()

        # anime_id → [person_id] の集約
        anime_persons: dict[str, list[str]] = defaultdict(list)
        for c in layer_creds:
            if c.anime_id in anime_map:
                anime_persons[c.anime_id].append(c.person_id)

        # add edges for person pairs on the same work (weight = shared work count)
        for anime_id, person_ids in anime_persons.items():
            # deduplicate
            unique_persons = list(set(person_ids))
            n = len(unique_persons)
            if n < 2:
                continue

            # all pairwise combinations (low density because within a layer)
            for i in range(n):
                for j in range(i + 1, n):
                    a, b = unique_persons[i], unique_persons[j]
                    if G.has_edge(a, b):
                        G[a][b]["weight"] += 1
                    else:
                        G.add_edge(a, b, weight=1)

        layer_graphs[layer_name] = G
        logger.info(
            "layer_graph_built",
            layer=layer_name,
            nodes=G.number_of_nodes(),
            edges=G.number_of_edges(),
        )

    return layer_graphs


# =============================================================================
# Multilayer Centrality Computation
# =============================================================================


def compute_multilayer_centrality(
    layer_graphs: dict[str, nx.Graph],
    career_tracks: dict[str, str],
    top_n: int = 500,
) -> dict[str, MultilayerCentrality]:
    """Compute betweenness + degree centrality per layer.

    大規模グラフ対策: グラフのノード数 > 500 の場合は k=100 近似 betweenness を使用
    （CLAUDE.md パフォーマンスガイドライン準拠）。

    最終的に全レイヤーでの aggregate_betweenness 上位 top_n 人物のみ返す。

    Args:
        layer_graphs: build_layer_graphs の出力
        career_tracks: person_id → career_track
        top_n: 返す人物数（aggregate_betweenness 上位）

    Returns:
        person_id → MultilayerCentrality
    """
    # compute betweenness + degree centrality per layer
    layer_betweenness: dict[str, dict[str, float]] = {}
    layer_degree: dict[str, dict[str, float]] = {}

    for layer_name, G in layer_graphs.items():
        n_nodes = G.number_of_nodes()
        n_edges = G.number_of_edges()
        if n_nodes == 0:
            layer_betweenness[layer_name] = {}
            layer_degree[layer_name] = {}
            continue

        deg = nx.degree_centrality(G)
        layer_degree[layer_name] = deg

        # Guard: skip betweenness for large graphs (>10K nodes or >1M edges)
        # Even k=100 approximate betweenness is O(k*(V+E)) which can take 30+ min
        if n_nodes > 10_000 or n_edges > 1_000_000:
            # Use degree centrality as proxy for betweenness
            layer_betweenness[layer_name] = deg
            logger.info(
                "layer_centrality_computed",
                layer=layer_name,
                nodes=n_nodes,
                edges=n_edges,
                approximate=True,
                betweenness_skipped=True,
                note="graph too large, using degree centrality as proxy",
            )
            continue

        # k=100 近似 betweenness（大グラフ）または完全計算（小グラフ）
        k = 100 if n_nodes > 500 else None
        btw = nx.betweenness_centrality(
            G, k=k, weight="weight", normalized=True, seed=42
        )

        layer_betweenness[layer_name] = btw

        logger.info(
            "layer_centrality_computed",
            layer=layer_name,
            nodes=n_nodes,
            edges=n_edges,
            approximate=(k is not None),
        )

    # collect person_ids that appear in all layers
    all_persons: set[str] = set()
    for btw in layer_betweenness.values():
        all_persons.update(btw.keys())

    # MultilayerCentrality を組み立て
    results: dict[str, MultilayerCentrality] = {}

    for pid in all_persons:
        btw_vals = {
            layer: layer_betweenness[layer].get(pid, 0.0) for layer in LAYER_NAMES
        }
        deg_vals = {layer: layer_degree[layer].get(pid, 0.0) for layer in LAYER_NAMES}
        layers_active = [
            layer for layer in LAYER_NAMES if pid in layer_betweenness.get(layer, {})
        ]
        aggregate = sum(btw_vals.values()) / max(len(layers_active), 1)

        results[pid] = MultilayerCentrality(
            person_id=pid,
            direction_betweenness=round(btw_vals["direction"], 6),
            animation_betweenness=round(btw_vals["animation"], 6),
            production_betweenness=round(btw_vals["production"], 6),
            technical_betweenness=round(btw_vals["technical"], 6),
            direction_degree=round(deg_vals["direction"], 4),
            animation_degree=round(deg_vals["animation"], 4),
            production_degree=round(deg_vals["production"], 4),
            technical_degree=round(deg_vals["technical"], 4),
            layer_count=len(layers_active),
            career_track=career_tracks.get(pid, "multi_track"),
            aggregate_betweenness=round(aggregate, 6),
            layers_active=layers_active,
        )

    # top_n に絞る
    sorted_results = sorted(
        results.values(),
        key=lambda m: m.aggregate_betweenness,
        reverse=True,
    )[:top_n]

    final = {m.person_id: m for m in sorted_results}

    logger.info(
        "multilayer_centrality_computed",
        total_persons=len(all_persons),
        top_n=len(final),
    )
    return final


# =============================================================================
# Entry Point for Standalone Execution
# =============================================================================


def main() -> None:
    """Standalone entry point."""
    from src.analysis.graph import create_person_collaboration_network  # noqa: F401
    from src.database import (
        get_connection,
        init_db,
        load_all_anime,
        load_all_credits,
        load_all_persons,
    )

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)
    conn.close()

    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}

    logger.info("inferring_career_tracks")
    career_tracks = infer_all_career_tracks(credits, anime_map)

    logger.info("building_layer_graphs")
    layer_graphs = build_layer_graphs(credits, anime_map)

    logger.info("computing_multilayer_centrality")
    centrality = compute_multilayer_centrality(layer_graphs, career_tracks, top_n=50)

    print("\n=== マルチレイヤー中心性 トップ20 ===\n")
    sorted_persons = sorted(
        centrality.values(),
        key=lambda m: m.aggregate_betweenness,
        reverse=True,
    )[:20]

    for m in sorted_persons:
        name = person_names.get(m.person_id, m.person_id)
        print(
            f"{name} [{m.career_track}] "
            f"(dir={m.direction_betweenness:.4f}, "
            f"anim={m.animation_betweenness:.4f}, "
            f"prod={m.production_betweenness:.4f}, "
            f"tech={m.technical_betweenness:.4f}, "
            f"layers={m.layer_count})"
        )

    # distribution by career track
    track_dist = Counter(career_tracks.values())
    print("\n=== キャリア畑分布 ===")
    for track, count in track_dist.most_common():
        print(f"  {track}: {count:,}人")


if __name__ == "__main__":
    main()

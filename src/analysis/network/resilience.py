"""Network resilience — node 除去 simulation で構造的脆弱性を測定。

collaboration graph (person/studio) から key node (bridge / hub) を順次除去し、
以下の global metric の劣化曲線を観測する:

    - LCC: largest connected component の persons 数
    - PCC: pair connectivity = sum_{C in components} |C|*(|C|-1)/2 (Latora-Marchiori 風)
    - mean_authority: 残存 node の eigenvector centrality 平均

3 つの removal strategy:

    - "random":   無作為 (基準線)
    - "degree":   degree centrality 降順 (古典的 attack シナリオ)
    - "bridge":   bridge_score (knowledge_spanners 由来) 降順

実応用シナリオ:
- 中間管理職 (bridge 役) が一斉離職した場合の連結性劣化
- 制作委員会 hub が抜けた時の affiliation 構造の robust 度

H1: anime.score 不参入 (構造的指標のみ)。
H2: 主観的評価 / 役割語 frame NG → "structural position" のみ。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import networkx as nx
import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def largest_connected_component_size(g: nx.Graph) -> int:
    """LCC: 残存 graph の最大連結成分の node 数。"""
    if g.number_of_nodes() == 0:
        return 0
    return max((len(c) for c in nx.connected_components(g)), default=0)


def pair_connectivity(g: nx.Graph) -> int:
    """PCC: sum over components of n_c * (n_c - 1) / 2。

    全ペアが可達なら N*(N-1)/2、完全に分断されたら 0。
    全体スケールが large でも比較は ratio で行う。
    """
    if g.number_of_nodes() == 0:
        return 0
    return sum(len(c) * (len(c) - 1) // 2 for c in nx.connected_components(g))


def mean_eigenvector_authority(g: nx.Graph) -> float:
    """残存 graph の eigenvector centrality 平均。

    収束失敗 (degenerate / disconnected) は graceful: 0.0 を返す。
    """
    if g.number_of_nodes() < 2 or g.number_of_edges() == 0:
        return 0.0
    try:
        ec = nx.eigenvector_centrality_numpy(g)
    except Exception:
        try:
            ec = nx.eigenvector_centrality(g, max_iter=500, tol=1e-4)
        except Exception:
            return 0.0
    if not ec:
        return 0.0
    return float(np.mean(list(ec.values())))


# ---------------------------------------------------------------------------
# Removal strategies
# ---------------------------------------------------------------------------


def removal_order_random(
    g: nx.Graph, *, rng_seed: int = 42, k: int | None = None
) -> list:
    """無作為 (置換無し) で除去順を返す。"""
    rng = np.random.default_rng(rng_seed)
    nodes = list(g.nodes())
    rng.shuffle(nodes)
    if k is not None:
        nodes = nodes[:k]
    return nodes


def removal_order_by_degree(g: nx.Graph, *, k: int | None = None) -> list:
    """Degree 降順 (tie-break: node id)。"""
    nodes_sorted = sorted(
        g.nodes(), key=lambda n: (-int(g.degree(n)), str(n))
    )
    if k is not None:
        nodes_sorted = nodes_sorted[:k]
    return nodes_sorted


def removal_order_by_attribute(
    g: nx.Graph,
    attribute: str,
    *,
    descending: bool = True,
    k: int | None = None,
) -> list:
    """node 属性 (e.g. bridge_score) 降順。属性 missing は最後尾。"""
    def _score(n):
        v = g.nodes[n].get(attribute)
        if v is None:
            return float("-inf") if descending else float("inf")
        return float(v)

    nodes_sorted = sorted(
        g.nodes(),
        key=lambda n: (-_score(n) if descending else _score(n), str(n)),
    )
    if k is not None:
        nodes_sorted = nodes_sorted[:k]
    return nodes_sorted


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResilienceStep:
    """1 step の除去後 metric snapshot。"""

    step: int
    removed_node: str | None
    lcc_size: int
    pair_connectivity: int
    mean_authority: float
    n_remaining: int


@dataclass(frozen=True)
class ResilienceCurve:
    """Removal sequence + metric trajectory。"""

    strategy: str
    n_initial: int
    steps: tuple[ResilienceStep, ...]
    # 比較用 normalized 指標 (0 = baseline 全 intact)
    lcc_ratio_curve: tuple[float, ...] = field(default_factory=tuple)
    pcc_ratio_curve: tuple[float, ...] = field(default_factory=tuple)
    auth_ratio_curve: tuple[float, ...] = field(default_factory=tuple)


def simulate_resilience(
    g: nx.Graph,
    removal_order: Sequence,
    *,
    strategy_name: str = "custom",
    metric_authority: bool = True,
    snapshot_stride: int = 1,
) -> ResilienceCurve:
    """順次 node を除去し metric trajectory を返す。

    Args:
        g: 元 graph (非破壊)。複製してから除去する。
        removal_order: 除去対象 node の順序 (前から除去)。
        strategy_name: ログ・report 用ラベル。
        metric_authority: True なら mean_eigenvector_authority も計測 (高コスト)。
        snapshot_stride: N step に 1 回だけ snapshot を取る (大規模 graph 用)。

    Returns:
        ResilienceCurve.
    """
    work = g.copy()
    n_initial = work.number_of_nodes()
    if n_initial == 0:
        return ResilienceCurve(
            strategy=strategy_name, n_initial=0, steps=()
        )

    # baseline (step=0)
    base_lcc = largest_connected_component_size(work)
    base_pcc = pair_connectivity(work)
    base_auth = mean_eigenvector_authority(work) if metric_authority else 0.0

    steps: list[ResilienceStep] = [
        ResilienceStep(
            step=0,
            removed_node=None,
            lcc_size=base_lcc,
            pair_connectivity=base_pcc,
            mean_authority=base_auth,
            n_remaining=n_initial,
        )
    ]

    for i, node in enumerate(removal_order, start=1):
        if node not in work:
            continue
        work.remove_node(node)
        if i % snapshot_stride != 0 and i != len(removal_order):
            continue
        steps.append(
            ResilienceStep(
                step=i,
                removed_node=str(node),
                lcc_size=largest_connected_component_size(work),
                pair_connectivity=pair_connectivity(work),
                mean_authority=(
                    mean_eigenvector_authority(work) if metric_authority else 0.0
                ),
                n_remaining=work.number_of_nodes(),
            )
        )

    base_lcc_f = float(base_lcc) if base_lcc > 0 else 1.0
    base_pcc_f = float(base_pcc) if base_pcc > 0 else 1.0
    base_auth_f = float(base_auth) if base_auth > 0 else 1.0

    return ResilienceCurve(
        strategy=strategy_name,
        n_initial=n_initial,
        steps=tuple(steps),
        lcc_ratio_curve=tuple(s.lcc_size / base_lcc_f for s in steps),
        pcc_ratio_curve=tuple(s.pair_connectivity / base_pcc_f for s in steps),
        auth_ratio_curve=tuple(s.mean_authority / base_auth_f for s in steps),
    )


# ---------------------------------------------------------------------------
# AUC summary (lower = more fragile)
# ---------------------------------------------------------------------------


def resilience_auc(curve: ResilienceCurve, *, metric: str = "lcc") -> float:
    """Removal curve の AUC (trapezoidal、normalized)。0 - 1。

    値が高い = node 除去耐性が高い (graph robust)。
    値が低い = 少数除去で大幅劣化 (fragile)。

    metric: "lcc" / "pcc" / "auth"
    """
    if metric == "lcc":
        ys = curve.lcc_ratio_curve
    elif metric == "pcc":
        ys = curve.pcc_ratio_curve
    elif metric == "auth":
        ys = curve.auth_ratio_curve
    else:
        raise ValueError(f"Unknown metric: {metric}")
    if len(ys) < 2:
        return 0.0
    # Use step index normalized 0-1 as x
    n = len(ys) - 1
    xs = np.linspace(0.0, 1.0, n + 1)
    return float(np.trapezoid(ys, xs))


# ---------------------------------------------------------------------------
# Critical node identification (top-k by single-removal impact)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CriticalNode:
    """単独除去で graph の global metric を大きく低下させる node。"""

    node_id: str
    lcc_drop: float       # baseline_lcc - lcc_after, 単位 = persons
    pcc_drop_ratio: float  # (baseline_pcc - pcc_after) / baseline_pcc
    auth_drop_ratio: float # (baseline_auth - auth_after) / baseline_auth


def find_critical_nodes(
    g: nx.Graph,
    candidates: Sequence | None = None,
    *,
    top_k: int = 10,
    score_metric: str = "pcc",  # "lcc" / "pcc" / "auth"
) -> list[CriticalNode]:
    """各 candidate node を 1 つだけ除去した時の global drop を測り top-k を返す。

    Args:
        candidates: 評価対象 (None なら全 node)。
        top_k: 上位件数。
        score_metric: ランキングに使う drop metric。

    Returns:
        score_metric の drop が大きい順 (= critical 順)。
    """
    base_lcc = largest_connected_component_size(g)
    base_pcc = pair_connectivity(g)
    base_auth = mean_eigenvector_authority(g)

    base_pcc_f = float(base_pcc) if base_pcc > 0 else 1.0
    base_auth_f = float(base_auth) if base_auth > 0 else 1.0

    cands = list(candidates) if candidates is not None else list(g.nodes())
    results: list[CriticalNode] = []

    for node in cands:
        if node not in g:
            continue
        sub = g.copy()
        sub.remove_node(node)
        lcc = largest_connected_component_size(sub)
        pcc = pair_connectivity(sub)
        auth = mean_eigenvector_authority(sub)
        results.append(
            CriticalNode(
                node_id=str(node),
                lcc_drop=float(base_lcc - lcc),
                pcc_drop_ratio=float((base_pcc - pcc) / base_pcc_f),
                auth_drop_ratio=float((base_auth - auth) / base_auth_f),
            )
        )

    if score_metric == "lcc":
        results.sort(key=lambda c: -c.lcc_drop)
    elif score_metric == "pcc":
        results.sort(key=lambda c: -c.pcc_drop_ratio)
    elif score_metric == "auth":
        results.sort(key=lambda c: -c.auth_drop_ratio)
    else:
        raise ValueError(f"Unknown score_metric: {score_metric}")
    return results[:top_k]


# ---------------------------------------------------------------------------
# Strategy comparator (random vs targeted)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StrategyComparison:
    """random vs targeted の AUC 差分 (= fragility ratio)。"""

    random_auc: float
    degree_auc: float
    bridge_auc: float | None
    relative_fragility: float  # 1 - (degree_auc / random_auc)
    interpretation: str


def compare_strategies(
    g: nx.Graph,
    *,
    bridge_attribute: str | None = "bridge_score",
    k_removals: int | None = None,
    rng_seed: int = 42,
    metric: str = "pcc",
) -> StrategyComparison:
    """random / degree / bridge_score を比較し fragility 指標を返す。

    fragility_ratio = 1 - degree_auc / random_auc:
        - 0 に近い: random と targeted が同等 = robust
        - 1 に近い: targeted で大幅劣化 = fragile (hub 依存型 network)
    """
    rand_order = removal_order_random(g, rng_seed=rng_seed, k=k_removals)
    deg_order = removal_order_by_degree(g, k=k_removals)

    rand_curve = simulate_resilience(g, rand_order, strategy_name="random")
    deg_curve = simulate_resilience(g, deg_order, strategy_name="degree")

    rand_auc = resilience_auc(rand_curve, metric=metric)
    deg_auc = resilience_auc(deg_curve, metric=metric)

    bridge_auc: float | None = None
    if bridge_attribute is not None:
        # 属性持ち node が >= 1 件あれば実行
        n_with_attr = sum(
            1 for _, d in g.nodes(data=True) if bridge_attribute in d
        )
        if n_with_attr > 0:
            br_order = removal_order_by_attribute(
                g, bridge_attribute, k=k_removals
            )
            br_curve = simulate_resilience(
                g, br_order, strategy_name="bridge"
            )
            bridge_auc = resilience_auc(br_curve, metric=metric)

    rand_auc_f = rand_auc if rand_auc > 1e-9 else 1.0
    rel_fragility = float(1.0 - deg_auc / rand_auc_f)

    interp = (
        "robust (random と targeted がほぼ同等)"
        if rel_fragility < 0.1
        else "moderate fragility"
        if rel_fragility < 0.3
        else "high fragility (hub 集中型構造)"
    )

    return StrategyComparison(
        random_auc=float(rand_auc),
        degree_auc=float(deg_auc),
        bridge_auc=bridge_auc,
        relative_fragility=rel_fragility,
        interpretation=interp,
    )

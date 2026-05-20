"""Network resilience report — 構造的脆弱性の可視化 (Policy brief セクション)。

collaboration graph (person) の hub / bridge を順次除去 → LCC / pair_connectivity
劣化曲線を描画。「中堅 N 人が一斉離職した時の業界 connectivity 劣化」を
counterfactual シミュレーション。

H1: anime.score 非依存。
H2: 主観的評価 frame NG → "structural fragility"。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import networkx as nx
import plotly.graph_objects as go
import structlog

from src.analysis.network.resilience import (
    compare_strategies,
    find_critical_nodes,
    removal_order_by_degree,
    removal_order_random,
    simulate_resilience,
)

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection
from ._base import BaseReportGenerator

log = structlog.get_logger(__name__)


_MIN_GRAPH_NODES = 100  # below this, report skips computation
_DEFAULT_K_REMOVALS = 20  # cap simulation; large graph では時間優先で短くする


_SAMPLE_TOP_N_PERSONS = 5000  # graph 構築用 person sample 上限
_PER_ANIME_CAP = 80
_MIN_CREDITS_FOR_GRAPH = 5


def _build_collaboration_graph_from_conn(conn: Any) -> nx.Graph:
    """credit 共起から co-credit person graph を構築 (sample top-N)。

    50 万 person 規模では O(n²) 爆発するため、上位 _SAMPLE_TOP_N_PERSONS のみで
    construct する。これは sample-based fragility 推定であり、population-level
    解釈は別途。
    """
    # Step 1: top-N persons by credit count
    person_queries = [
        f"""
        SELECT person_id FROM credits
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        HAVING COUNT(*) >= {_MIN_CREDITS_FOR_GRAPH}
        ORDER BY COUNT(*) DESC
        LIMIT {_SAMPLE_TOP_N_PERSONS}
        """,
        f"""
        SELECT person_id FROM conformed.credits
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        HAVING COUNT(*) >= {_MIN_CREDITS_FOR_GRAPH}
        ORDER BY COUNT(*) DESC
        LIMIT {_SAMPLE_TOP_N_PERSONS}
        """,
    ]
    top_persons: set[str] = set()
    for sql in person_queries:
        try:
            rows = conn.execute(sql).fetchall()
            top_persons = {r[0] for r in rows if r[0]}
            break
        except Exception as exc:
            log.debug("graph_top_persons_attempt_failed", error=str(exc))
    if not top_persons:
        return nx.Graph()

    # Step 2: co-credit edges within sample only
    persons_csv = ",".join("'" + p.replace("'", "''") + "'" for p in top_persons)
    edge_queries = [
        f"""
        SELECT person_id, anime_id FROM credits
        WHERE person_id IN ({persons_csv}) AND anime_id IS NOT NULL
        """,
        f"""
        SELECT person_id, anime_id FROM conformed.credits
        WHERE person_id IN ({persons_csv}) AND anime_id IS NOT NULL
        """,
    ]
    rows: list[tuple] = []
    for sql in edge_queries:
        try:
            rows = conn.execute(sql).fetchall()
            break
        except Exception as exc:
            log.debug("graph_edges_attempt_failed", error=str(exc))
    if not rows:
        return nx.Graph()

    from collections import defaultdict
    by_anime: dict[str, list[str]] = defaultdict(list)
    for pid, aid in rows:
        if pid and aid:
            by_anime[str(aid)].append(str(pid))

    g = nx.Graph()
    for aid, persons in by_anime.items():
        persons = list(set(persons))
        if len(persons) > _PER_ANIME_CAP:
            continue
        for i in range(len(persons)):
            for j in range(i + 1, len(persons)):
                a, b = persons[i], persons[j]
                if g.has_edge(a, b):
                    g[a][b]["w"] = g[a][b]["w"] + 1
                else:
                    g.add_edge(a, b, w=1)
    return g


class NetworkResilienceReport(BaseReportGenerator):
    """Network resilience — 構造的脆弱性測定 (Policy brief)。"""

    name = "network_resilience"
    title = "Network 構造的脆弱性"
    subtitle = (
        "collaboration graph の hub / bridge 順次除去 simulation で network "
        "connectivity の劣化を測定。「中堅 N 人離職」counterfactual。"
    )
    doc_type = "main"
    filename = "network_resilience.html"

    def generate(self) -> Path | None:
        g = _build_collaboration_graph_from_conn(self.conn)
        n_nodes = g.number_of_nodes()
        n_edges = g.number_of_edges()

        if n_nodes < _MIN_GRAPH_NODES:
            body = (
                f"<p>collaboration graph 構築失敗 or node 数不足 "
                f"(n_nodes={n_nodes}, min={_MIN_GRAPH_NODES})。"
                "credits schema fallback 経路と Resolved 層完成度に依存。</p>"
            )
            return self.write_report(body)

        log.info("resilience_graph_built", n_nodes=n_nodes, n_edges=n_edges)

        # Strategy comparison (degree vs random, capped k)
        k = min(_DEFAULT_K_REMOVALS, n_nodes // 2)
        cmp_ = compare_strategies(
            g, bridge_attribute=None, k_removals=k, rng_seed=42, metric="pcc"
        )

        # Build resilience curves (LCC ratio over removal step)
        rand_order = removal_order_random(g, rng_seed=42, k=k)
        deg_order = removal_order_by_degree(g, k=k)
        # 実 graph 大規模時は metric_authority=False (eigenvector centrality 計算スキップ)
        large = g.number_of_edges() > 100_000
        rand_curve = simulate_resilience(
            g, rand_order, strategy_name="random", metric_authority=not large,
        )
        deg_curve = simulate_resilience(
            g, deg_order, strategy_name="degree", metric_authority=not large,
        )

        # Critical persons (top 10 by single-node pair_connectivity drop)
        # Limit candidates to degree-top 200 for speed
        top_deg = removal_order_by_degree(g, k=200)
        criticals = find_critical_nodes(g, candidates=top_deg, top_k=10, score_metric="pcc")

        # ── Findings HTML ────────────────────────────────────────────────
        findings = (
            f"<p>graph 規模: persons={n_nodes:,}, co-credit edges={n_edges:,}</p>"
            f"<p>random removal AUC (PCC): {cmp_.random_auc:.3f}</p>"
            f"<p>degree-targeted removal AUC (PCC): {cmp_.degree_auc:.3f}</p>"
            f"<p>relative fragility (= 1 - degree/random): "
            f"<strong>{cmp_.relative_fragility:.3f}</strong> "
            f"({cmp_.interpretation})</p>"
            "<p>top-10 critical persons (単独除去で pair_connectivity 最大 drop):</p>"
            "<table><thead><tr><th>person_id</th><th>pcc_drop_ratio</th>"
            "<th>lcc_drop</th></tr></thead><tbody>"
            + "".join(
                f"<tr><td>{c.node_id}</td><td>{c.pcc_drop_ratio:.4f}</td>"
                f"<td>{c.lcc_drop:.0f}</td></tr>"
                for c in criticals
            )
            + "</tbody></table>"
        )

        # ── Visualization ────────────────────────────────────────────────
        fig = go.Figure()
        rand_steps = [s.step for s in rand_curve.steps]
        deg_steps = [s.step for s in deg_curve.steps]
        fig.add_trace(go.Scatter(
            x=rand_steps, y=rand_curve.pcc_ratio_curve,
            mode="lines+markers", name="random removal", line={"color": "#88aacc"},
        ))
        fig.add_trace(go.Scatter(
            x=deg_steps, y=deg_curve.pcc_ratio_curve,
            mode="lines+markers", name="degree-targeted", line={"color": "#cc6655"},
        ))
        fig.update_layout(
            title="Pair connectivity vs nodes removed",
            xaxis_title="nodes removed",
            yaxis_title="pair_connectivity / baseline",
            yaxis_range=[0, 1.05],
            template="plotly_white",
            height=480,
        )
        viz_html = plotly_div_safe(fig, "resilience_curve", height=480)

        # ── Interpretation ───────────────────────────────────────────────
        interpretation = (
            "<p>relative_fragility が大きい = network が hub 集中型構造。"
            "少数 person の離脱で連結性が大幅劣化する。</p>"
            "<p>これは個人の主観的評価ではなく、"
            "構造的に bridge 役を担う person が居る事実記述。"
            "bridge 役の離職リスクが業界全体の collaboration "
            "持続性に影響する政策的含意を持つ。</p>"
            "<p>top-10 critical persons は単独除去で最大の pair_connectivity "
            "drop となる person。"
            "interpretation: そこに人材が偏在している構造的観察であり、"
            "個人の主観的評価とは独立。</p>"
        )

        section = ReportSection(
            title="Findings",
            section_id="resilience_findings",
            findings_html=findings,
            visualization_html=viz_html,
            interpretation_html=interpretation,
        )
        body = self.builder.build_section(section)
        return self.write_report(body)


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="network_resilience",
    audience="policy",
    claim=(
        "collaboration graph の hub / bridge person を順次除去する simulation で、"
        "fragility_ratio = 1 - degree_auc / random_auc を構造的脆弱性指標として "
        "公開する。high fragility (> 0.3) の場合、bridge person の離職リスクが "
        "業界全体の collaboration 持続性に影響することを警告する。"
    ),
    identifying_assumption=(
        "co-credit edges は協業関係を測る構造的代理。per-anime cap 80 persons で"
        "O(n²) 爆発回避。bridge_score は src/analysis/network/bridges.py 出力前提。"
        "entity resolution 信頼性 (19/01 / 35/01 完了) に依存。"
    ),
    null_model=["random removal baseline (rng_seed-fixed)"],
    sources=["credits", "persons"],
    meta_table="meta_network_resilience",
    estimator="trapezoidal AUC over LCC / PCC / authority ratio curve",
    ci_estimator="bootstrap", n_resamples=200,  # k_removals scaling
    extra_limitations=[
        "co-credit edge は friendship を意味しない",
        "long-running series の per-anime cap で truncation",
        "eigenvector_centrality 収束失敗時 graceful 0 fallback",
        "critical person flag は構造的観察、個人評価ではない",
    ],
)

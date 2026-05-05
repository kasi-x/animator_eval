"""P7: SankeyFlow — staged transitions (career stage / studio movement).

Wraps go.Sankey with consistent palette mapping, null-baseline tooltip
overlay, and small-link aggregation into an "Other" node so the diagram
remains readable.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import plotly.graph_objects as go

from ..palettes import OKABE_ITO_DARK, hex_to_rgba
from ..theme import apply_theme


@dataclass(frozen=True)
class SankeyNode:
    id: str
    label: str
    layer: int
    color: str | None = None


@dataclass(frozen=True)
class SankeyLink:
    source_id: str
    target_id: str
    value: float
    color: str | None = None
    null_baseline: float | None = None  # observed vs null comparison


@dataclass(frozen=True)
class SankeyFlowSpec:
    nodes: list[SankeyNode]
    links: list[SankeyLink]
    layer_labels: list[str]
    title: str = ""
    min_link_value: int = 5
    other_label: str = "Other (集約)"
    height: int = 540
    default_palette: tuple[str, ...] = field(default_factory=lambda: OKABE_ITO_DARK)


def _aggregate_small_links(spec: SankeyFlowSpec) -> tuple[list[SankeyNode], list[SankeyLink]]:
    if spec.min_link_value <= 0:
        return spec.nodes, spec.links

    keep, small = [], []
    for link in spec.links:
        (keep if link.value >= spec.min_link_value else small).append(link)
    if not small:
        return spec.nodes, spec.links

    nodes = list(spec.nodes)
    new_links = list(keep)
    nodes_by_id = {n.id: n for n in nodes}

    # 1 "Other" node per layer (target side of small links)
    other_by_layer: dict[int, SankeyNode] = {}
    for link in small:
        target = nodes_by_id.get(link.target_id)
        if target is None:
            continue
        layer = target.layer
        if layer not in other_by_layer:
            other_id = f"__other_{layer}"
            other_node = SankeyNode(id=other_id, label=spec.other_label,
                                    layer=layer, color="#7a7a7a")
            other_by_layer[layer] = other_node
            nodes.append(other_node)
            nodes_by_id[other_id] = other_node
        new_links.append(
            SankeyLink(
                source_id=link.source_id,
                target_id=other_by_layer[layer].id,
                value=link.value,
                color=link.color,
                null_baseline=link.null_baseline,
            )
        )
    return nodes, new_links


def render_sankey_flow(spec: SankeyFlowSpec, *, theme: str = "dark") -> go.Figure:
    if not spec.nodes or not spec.links:
        fig = go.Figure()
        fig.update_layout(title=spec.title or "(no data)")
        return apply_theme(fig, theme=theme, height=spec.height)

    nodes, links = _aggregate_small_links(spec)
    id_to_idx = {n.id: i for i, n in enumerate(nodes)}

    node_colors = [
        n.color or spec.default_palette[n.layer % len(spec.default_palette)]
        for n in nodes
    ]

    link_sources = [id_to_idx[link.source_id] for link in links if link.source_id in id_to_idx]
    link_targets = [id_to_idx[link.target_id] for link in links if link.target_id in id_to_idx]
    link_values = [link.value for link in links
                   if link.source_id in id_to_idx and link.target_id in id_to_idx]
    link_colors = [
        link.color or hex_to_rgba(node_colors[id_to_idx[link.source_id]], 0.35)
        for link in links if link.source_id in id_to_idx
    ]
    customdata = []
    for link in links:
        if link.source_id not in id_to_idx:
            continue
        if link.null_baseline is not None:
            customdata.append([link.null_baseline])
        else:
            customdata.append([None])

    sankey = go.Sankey(
        arrangement="snap",
        node=dict(
            label=[n.label for n in nodes],
            color=node_colors,
            pad=18,
            thickness=18,
            line=dict(color="rgba(255,255,255,0.15)", width=0.5),
        ),
        link=dict(
            source=link_sources,
            target=link_targets,
            value=link_values,
            color=link_colors,
            customdata=customdata,
            hovertemplate=(
                "%{source.label} → %{target.label}<br>"
                "obs = %{value:,.0f}<br>"
                "null = %{customdata[0]}<extra></extra>"
            ),
        ),
    )
    fig = go.Figure(data=[sankey])
    fig.update_layout(title=spec.title)
    return apply_theme(fig, theme=theme, height=spec.height)

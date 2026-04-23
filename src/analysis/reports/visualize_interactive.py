"""Interactive visualizations using Plotly.

Plotly によるインタラクティブな可視化機能。
matplotlib の静的チャートを補完し、ズーム・ホバー・フィルタリング機能を提供。
"""

from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import structlog
from plotly.subplots import make_subplots

logger = structlog.get_logger()


def plot_interactive_score_distribution(
    scores_data: list[dict],
    output_path: Path | None = None,
) -> None:
    """Interactive histogram of score distribution.

    Interactive histogram of score distributions with hover details.

    Args:
        scores_data: List of score dicts with person_id, person_fe, birank, patronage, iv_score
        output_path: Output HTML file path
    """
    if not scores_data:
        return

    import pandas as pd

    df = pd.DataFrame(scores_data)

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "IV Score",
            "Person FE Score",
            "BiRank Score",
            "Patronage Score",
        ),
    )

    # IV Score
    fig.add_trace(
        go.Histogram(
            x=df["iv_score"],
            name="IV Score",
            marker_color="rgba(171, 99, 250, 0.7)",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Person FE
    fig.add_trace(
        go.Histogram(
            x=df["person_fe"],
            name="Person FE",
            marker_color="rgba(0, 204, 150, 0.7)",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ),
        row=1,
        col=2,
    )

    # BiRank
    fig.add_trace(
        go.Histogram(
            x=df["birank"],
            name="BiRank",
            marker_color="rgba(99, 110, 250, 0.7)",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Patronage
    fig.add_trace(
        go.Histogram(
            x=df["patronage"],
            name="Patronage",
            marker_color="rgba(239, 85, 59, 0.7)",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ),
        row=2,
        col=2,
    )

    fig.update_layout(
        title_text="Score Distribution (Interactive)",
        showlegend=False,
        height=800,
        template="plotly_white",
    )

    if output_path:
        fig.write_html(str(output_path))
        logger.info("interactive_score_distribution_saved", path=str(output_path))
    else:
        fig.show()


def plot_interactive_radar(
    top_persons: list[dict],
    top_n: int = 10,
    output_path: Path | None = None,
) -> None:
    """Interactive radar chart of the top-N persons.

    Interactive radar chart for top N persons with toggleable traces.

    Args:
        top_persons: List of top person dicts with name, person_fe, birank, patronage
        top_n: Number of top persons to display
        output_path: Output HTML file path
    """
    if not top_persons:
        return

    selected = top_persons[:top_n]
    categories = ["Person FE", "BiRank", "Patronage"]

    fig = go.Figure()

    for person in selected:
        name = person.get("name") or person.get("person_id", "Unknown")
        values = [
            person.get("person_fe", 0),
            person.get("birank", 0),
            person.get("patronage", 0),
        ]
        # Close the radar by appending first value
        values_closed = values + [values[0]]
        categories_closed = categories + [categories[0]]

        fig.add_trace(
            go.Scatterpolar(
                r=values_closed,
                theta=categories_closed,
                fill="toself",
                name=name,
                hovertemplate="%{theta}: %{r:.1f}<extra></extra>",
            )
        )

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title=f"Top {len(selected)} Persons — Radar Chart (Click legend to toggle)",
        template="plotly_white",
        height=700,
    )

    if output_path:
        fig.write_html(str(output_path))
        logger.info("interactive_radar_saved", path=str(output_path))
    else:
        fig.show()


def plot_interactive_scatter(
    scores_data: list[dict],
    x_axis: str = "birank",
    y_axis: str = "patronage",
    output_path: Path | None = None,
) -> None:
    """Interactive scatter plot of two score axes.

    Interactive scatter plot with hover details and zoom.

    Args:
        scores_data: List of score dicts
        x_axis: Score type for X axis
        y_axis: Score type for Y axis
        output_path: Output HTML file path
    """
    if not scores_data:
        return

    import pandas as pd

    df = pd.DataFrame(scores_data)
    df["name_display"] = df.apply(
        lambda row: row.get("name") or row.get("person_id", "Unknown"), axis=1
    )
    # Clamp size column — px.scatter rejects negative marker sizes
    df["_size"] = df["iv_score"].clip(lower=0.001)

    fig = px.scatter(
        df,
        x=x_axis,
        y=y_axis,
        color="iv_score",
        size="_size",
        hover_data={
            "name_display": True,
            x_axis: ":.1f",
            y_axis: ":.1f",
            "iv_score": ":.1f",
        },
        labels={
            "birank": "BiRank Score",
            "patronage": "Patronage Score",
            "person_fe": "Person FE Score",
            "iv_score": "IV Score",
            "name_display": "Name",
        },
        title=f"{x_axis.capitalize()} vs {y_axis.capitalize()} (Interactive)",
        template="plotly_white",
        color_continuous_scale="Viridis",
    )

    fig.update_traces(marker=dict(line=dict(width=0.5, color="DarkSlateGrey")))
    fig.update_layout(height=700)

    if output_path:
        fig.write_html(str(output_path))
        logger.info(
            "interactive_scatter_saved", path=str(output_path), x=x_axis, y=y_axis
        )
    else:
        fig.show()


def plot_interactive_timeline(
    timeline_data: dict,
    output_path: Path | None = None,
) -> None:
    """Interactive trend of annual credit counts.

    Interactive timeline of credit counts over years.

    Args:
        timeline_data: Dict with "years" and "credit_counts" lists
        output_path: Output HTML file path
    """
    if not timeline_data or not timeline_data.get("years"):
        return

    years = timeline_data["years"]
    counts = timeline_data.get("credit_counts", [])

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=years,
            y=counts,
            mode="lines+markers",
            name="Credit Count",
            line=dict(color="rgb(99, 110, 250)", width=2),
            marker=dict(size=8),
            hovertemplate="Year: %{x}<br>Credits: %{y}<extra></extra>",
        )
    )

    fig.update_layout(
        title="Credit Timeline (Interactive - Drag to zoom)",
        xaxis_title="Year",
        yaxis_title="Credit Count",
        template="plotly_white",
        height=500,
        hovermode="x unified",
    )

    if output_path:
        fig.write_html(str(output_path))
        logger.info("interactive_timeline_saved", path=str(output_path))
    else:
        fig.show()


def plot_interactive_network(
    collaboration_data: list[dict],
    top_n: int = 50,
    output_path: Path | None = None,
) -> None:
    """Interactive collaboration network.

    Interactive network graph of collaboration relationships.

    Args:
        collaboration_data: List of collaboration edge dicts with person1_id, person2_id, weight
        top_n: Number of top collaborations to display
        output_path: Output HTML file path
    """
    if not collaboration_data:
        return

    import networkx as nx

    # Build NetworkX graph
    G = nx.Graph()
    for edge in collaboration_data[:top_n]:
        p1 = edge.get("person1_id", "")
        p2 = edge.get("person2_id", "")
        weight = edge.get("weight", 1)
        p1_name = edge.get("person1_name", p1)
        p2_name = edge.get("person2_name", p2)

        if p1 and p2:
            G.add_edge(p1, p2, weight=weight)
            G.nodes[p1]["name"] = p1_name
            G.nodes[p2]["name"] = p2_name

    if G.number_of_nodes() == 0:
        return

    # Use spring layout
    pos = nx.spring_layout(G, k=0.5, iterations=50)

    # Create edge traces
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.5, color="#888"),
        hoverinfo="none",
        mode="lines",
    )

    # Create node traces
    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        name = G.nodes[node].get("name", node)
        degree = G.degree(node)
        node_text.append(f"{name}<br>Connections: {degree}")

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        hoverinfo="text",
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale="YlGnBu",
            size=10,
            colorbar=dict(
                thickness=15, title=dict(text="Node Connections"), xanchor="left"
            ),
            line_width=2,
        ),
    )

    # Color nodes by degree
    node_adjacencies = []
    for node in G.nodes():
        node_adjacencies.append(len(list(G.neighbors(node))))

    node_trace.marker.color = node_adjacencies

    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title=dict(
                text=f"Collaboration Network (Top {top_n} pairs - Interactive)",
                font=dict(size=16),
            ),
            showlegend=False,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            template="plotly_white",
            height=800,
        ),
    )

    if output_path:
        fig.write_html(str(output_path))
        logger.info("interactive_network_saved", path=str(output_path))
    else:
        fig.show()


def generate_interactive_dashboard(
    scores_data: list[dict],
    timeline_data: dict | None = None,
    output_dir: Path | None = None,
) -> None:
    """Generate all interactive visualisations.

    Generates all interactive visualizations in HTML format.

    Args:
        scores_data: Score data for all persons
        timeline_data: Timeline data (optional)
        output_dir: Output directory for HTML files
    """
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    # Score distribution
    if scores_data:
        plot_interactive_score_distribution(
            scores_data,
            output_path=output_dir / "interactive_scores.html" if output_dir else None,
        )

    # Radar chart (top 10)
    if scores_data:
        plot_interactive_radar(
            scores_data,
            top_n=10,
            output_path=output_dir / "interactive_radar.html" if output_dir else None,
        )

    # Scatter plots (multiple combinations)
    if scores_data:
        for x_axis, y_axis in [
            ("birank", "patronage"),
            ("birank", "person_fe"),
            ("patronage", "person_fe"),
        ]:
            plot_interactive_scatter(
                scores_data,
                x_axis=x_axis,
                y_axis=y_axis,
                output_path=output_dir / f"interactive_scatter_{x_axis}_{y_axis}.html"
                if output_dir
                else None,
            )

    # Timeline
    if timeline_data:
        plot_interactive_timeline(
            timeline_data,
            output_path=output_dir / "interactive_timeline.html"
            if output_dir
            else None,
        )

    logger.info(
        "interactive_dashboard_generated",
        output_dir=str(output_dir) if output_dir else "display",
    )


def main() -> None:
    """Entry point: generate interactive visualisations from JSON data."""
    import json

    from src.utils.config import JSON_DIR
    from src.infra.logging import setup_logging

    setup_logging()

    # Load scores
    scores_path = JSON_DIR / "scores.json"
    if not scores_path.exists():
        logger.error("scores_not_found", path=str(scores_path))
        return

    with open(scores_path) as f:
        scores_data = json.load(f)

    # Load timeline if available
    timeline_data = None
    timeline_path = JSON_DIR / "time_series.json"
    if timeline_path.exists():
        with open(timeline_path) as f:
            timeline_data = json.load(f)

    # Generate all interactive visualizations
    interactive_dir = JSON_DIR.parent / "interactive"
    generate_interactive_dashboard(
        scores_data, timeline_data, output_dir=interactive_dir
    )

    logger.info("interactive_visualizations_complete", directory=str(interactive_dir))


if __name__ == "__main__":
    main()

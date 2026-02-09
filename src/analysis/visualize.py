"""グラフ可視化 (matplotlib)."""

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import structlog

from src.utils.config import JSON_DIR

matplotlib.use("Agg")

# 日本語フォント設定
matplotlib.rcParams["font.family"] = ["Noto Serif CJK JP", "Noto Sans CJK JP", "DejaVu Sans"]

logger = structlog.get_logger()


def plot_score_distribution(
    scores: dict[str, dict],
    output_path: Path | None = None,
) -> None:
    """3軸スコアの分布をヒストグラムで可視化."""
    if not scores:
        logger.warning("No scores to plot")
        return

    fig, axes = plt.subplots(1, 4, figsize=(20, 5))

    score_types = [
        ("authority", "Authority (PageRank)", "#2196F3"),
        ("trust", "Trust (継続起用)", "#4CAF50"),
        ("skill", "Skill (OpenSkill)", "#FF9800"),
        ("composite", "Composite (総合)", "#9C27B0"),
    ]

    for ax, (key, label, color) in zip(axes, score_types):
        values = [s[key] for s in scores.values() if key in s]
        if values:
            ax.hist(values, bins=30, color=color, alpha=0.7, edgecolor="white")
            ax.set_title(label, fontsize=12)
            ax.set_xlabel("Score")
            ax.set_ylabel("Count")
            ax.axvline(np.mean(values), color="red", linestyle="--", alpha=0.5, label=f"Mean: {np.mean(values):.1f}")
            ax.legend()

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "score_distribution.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("score_distribution_saved", path=str(output_path))


def plot_top_persons_radar(
    results: list[dict],
    top_n: int = 10,
    output_path: Path | None = None,
) -> None:
    """上位人物の3軸レーダーチャート."""
    if not results:
        return

    top = results[:top_n]
    categories = ["Authority", "Trust", "Skill"]
    n_cats = len(categories)
    angles = np.linspace(0, 2 * np.pi, n_cats, endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))

    colors = plt.cm.tab10(np.linspace(0, 1, top_n))

    for i, r in enumerate(top):
        values = [r.get("authority", 0), r.get("trust", 0), r.get("skill", 0)]
        values += values[:1]
        name = r.get("name", r.get("person_id", ""))
        ax.plot(angles, values, "o-", color=colors[i], label=name, linewidth=2)
        ax.fill(angles, values, color=colors[i], alpha=0.1)

    ax.set_thetagrids([a * 180 / np.pi for a in angles[:-1]], categories)
    ax.set_ylim(0, 100)
    ax.set_title(f"Top {top_n} — 3軸評価レーダー", fontsize=14, pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.0), fontsize=8)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "top_radar.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("radar_chart_saved", path=str(output_path))


def plot_collaboration_network(
    graph: nx.Graph,
    scores: dict[str, float] | None = None,
    top_n: int = 50,
    output_path: Path | None = None,
) -> None:
    """コラボレーションネットワークの可視化."""
    if graph.number_of_nodes() == 0:
        return

    # 上位ノードに絞る
    if scores and len(graph.nodes) > top_n:
        top_nodes = sorted(scores, key=scores.get, reverse=True)[:top_n]
        subgraph = graph.subgraph(top_nodes).copy()
    else:
        subgraph = graph

    if subgraph.number_of_nodes() == 0:
        return

    fig, ax = plt.subplots(figsize=(16, 16))

    pos = nx.spring_layout(subgraph, k=2.0, iterations=50, seed=42)

    # ノードサイズ = スコア
    if scores:
        node_sizes = [scores.get(n, 10) * 5 + 50 for n in subgraph.nodes()]
    else:
        node_sizes = [100] * subgraph.number_of_nodes()

    # エッジの太さ = weight
    edge_weights = [subgraph[u][v].get("weight", 1) for u, v in subgraph.edges()]
    max_w = max(edge_weights) if edge_weights else 1
    edge_widths = [w / max_w * 3 + 0.5 for w in edge_weights]

    nx.draw_networkx_edges(
        subgraph, pos, ax=ax, width=edge_widths, alpha=0.3, edge_color="#888"
    )
    nx.draw_networkx_nodes(
        subgraph, pos, ax=ax, node_size=node_sizes, node_color="#2196F3", alpha=0.7
    )

    labels = {
        n: subgraph.nodes[n].get("name", n)[:15] for n in subgraph.nodes()
    }
    nx.draw_networkx_labels(subgraph, pos, labels, ax=ax, font_size=7)

    ax.set_title(f"Collaboration Network (Top {top_n})", fontsize=14)
    ax.axis("off")

    if output_path is None:
        output_path = JSON_DIR.parent / "collaboration_network.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("collaboration_network_saved", path=str(output_path))


# キャリアステージの数値→ラベルマッピング
_STAGE_LABELS = {
    1: "In-Between",
    2: "2nd Key",
    3: "Key Animator",
    4: "Anim. Director",
    5: "Chief AD",
    6: "Director",
}


def plot_person_timeline(
    person_id: str,
    credits_by_year: dict[int, list[dict]],
    career_stages: dict[int, int] | None = None,
    person_name: str = "",
    output_path: Path | None = None,
) -> None:
    """人物のキャリアタイムラインを可視化.

    Args:
        person_id: 人物ID
        credits_by_year: {year: [{anime_title, role, score}]}
        career_stages: {year: stage_number} (optional)
        person_name: 表示名
        output_path: 出力先
    """
    if not credits_by_year:
        return

    years = sorted(credits_by_year.keys())
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [2, 1]})

    # Panel 1: Works per year (stacked by role type)
    role_groups = {
        "Director": {"director", "chief_animation_director", "episode_director", "storyboard"},
        "Animation": {"animation_director", "key_animator", "second_key_animator", "in_between"},
        "Design": {"character_designer", "mechanical_designer", "art_director", "color_designer"},
        "Other": set(),
    }
    group_colors = {"Director": "#E91E63", "Animation": "#2196F3", "Design": "#4CAF50", "Other": "#9E9E9E"}

    yearly_counts: dict[str, list[int]] = {g: [] for g in role_groups}
    for y in years:
        group_count: dict[str, int] = {g: 0 for g in role_groups}
        for c in credits_by_year[y]:
            role = c.get("role", "other")
            placed = False
            for group_name, roles in role_groups.items():
                if group_name != "Other" and role in roles:
                    group_count[group_name] += 1
                    placed = True
                    break
            if not placed:
                group_count["Other"] += 1
        for g in role_groups:
            yearly_counts[g].append(group_count[g])

    bottom = np.zeros(len(years))
    for group_name in ["Other", "Design", "Animation", "Director"]:
        vals = np.array(yearly_counts[group_name])
        ax1.bar(years, vals, bottom=bottom, label=group_name, color=group_colors[group_name], alpha=0.8)
        bottom += vals

    ax1.set_ylabel("Credits")
    ax1.set_title(f"Career Timeline: {person_name or person_id}", fontsize=14)
    ax1.legend(loc="upper left", fontsize=8)

    # Panel 2: Career stage progression
    if career_stages:
        stage_years = sorted(career_stages.keys())
        stage_vals = [career_stages[y] for y in stage_years]
        ax2.step(stage_years, stage_vals, where="post", color="#FF9800", linewidth=2)
        ax2.fill_between(stage_years, stage_vals, step="post", alpha=0.2, color="#FF9800")
        ax2.set_yticks(list(_STAGE_LABELS.keys()))
        ax2.set_yticklabels(list(_STAGE_LABELS.values()), fontsize=8)
        ax2.set_ylim(0.5, 6.5)
    else:
        ax2.text(0.5, 0.5, "No stage data", ha="center", va="center", transform=ax2.transAxes)

    ax2.set_xlabel("Year")
    ax2.set_ylabel("Career Stage")

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / f"timeline_{person_id.replace(':', '_')}.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("timeline_saved", path=str(output_path), person=person_id)


def plot_growth_trends(
    growth_data: dict,
    output_path: Path | None = None,
) -> None:
    """成長トレンド分布をバーチャートで可視化.

    Args:
        growth_data: growth.json の内容 (trend_summary キー含む)
        output_path: 出力先
    """
    summary = growth_data.get("trend_summary", {})
    if not summary:
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    labels = list(summary.keys())
    values = list(summary.values())

    colors = {
        "rising": "#4CAF50",
        "stable": "#2196F3",
        "declining": "#FF5722",
        "inactive": "#9E9E9E",
        "new": "#00BCD4",
    }
    bar_colors = [colors.get(label, "#757575") for label in labels]

    bars = ax.bar(labels, values, color=bar_colors, alpha=0.85, edgecolor="white", linewidth=1.5)

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            str(val),
            ha="center", va="bottom", fontsize=12, fontweight="bold",
        )

    ax.set_title("Career Growth Trend Distribution", fontsize=14)
    ax.set_xlabel("Trend")
    ax.set_ylabel("Number of Persons")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "growth_trends.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("growth_trends_chart_saved", path=str(output_path))


def plot_network_evolution(
    evolution_data: dict,
    output_path: Path | None = None,
) -> None:
    """ネットワーク進化をラインチャートで可視化.

    Args:
        evolution_data: network_evolution.json の内容
        output_path: 出力先
    """
    years = evolution_data.get("years", [])
    snapshots = evolution_data.get("snapshots", {})
    if not years:
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [1, 1]})

    cum_persons = [snapshots.get(str(y), snapshots.get(y, {})).get("cumulative_persons", 0) for y in years]
    cum_edges = [snapshots.get(str(y), snapshots.get(y, {})).get("cumulative_edges", 0) for y in years]
    new_persons = [snapshots.get(str(y), snapshots.get(y, {})).get("new_persons", 0) for y in years]
    density = [snapshots.get(str(y), snapshots.get(y, {})).get("density", 0) for y in years]

    # Top: Cumulative persons & edges
    ax1.plot(years, cum_persons, "o-", color="#2196F3", linewidth=2, label="Cumulative Persons")
    ax1.set_ylabel("Persons", color="#2196F3")
    ax1.tick_params(axis="y", labelcolor="#2196F3")

    ax1b = ax1.twinx()
    ax1b.plot(years, cum_edges, "s-", color="#FF9800", linewidth=2, label="Cumulative Edges")
    ax1b.set_ylabel("Edges", color="#FF9800")
    ax1b.tick_params(axis="y", labelcolor="#FF9800")

    ax1.set_title("Network Evolution", fontsize=14)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1b.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    # Bottom: New persons per year & density
    ax2.bar(years, new_persons, color="#4CAF50", alpha=0.7, label="New Persons")
    ax2.set_ylabel("New Persons", color="#4CAF50")
    ax2.tick_params(axis="y", labelcolor="#4CAF50")

    ax2b = ax2.twinx()
    ax2b.plot(years, density, "D-", color="#E91E63", linewidth=2, label="Density")
    ax2b.set_ylabel("Density", color="#E91E63")
    ax2b.tick_params(axis="y", labelcolor="#E91E63")

    ax2.set_xlabel("Year")
    lines3, labels3 = ax2.get_legend_handles_labels()
    lines4, labels4 = ax2b.get_legend_handles_labels()
    ax2.legend(lines3 + lines4, labels3 + labels4, loc="upper left")

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "network_evolution.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("network_evolution_chart_saved", path=str(output_path))


def plot_decade_comparison(
    decade_data: dict,
    output_path: Path | None = None,
) -> None:
    """年代別比較をグループバーチャートで可視化.

    Args:
        decade_data: decades.json の内容
        output_path: 出力先
    """
    decades = decade_data.get("decades", {})
    if not decades:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    labels = sorted(decades.keys())
    credits_vals = [decades[d].get("credit_count", 0) for d in labels]
    persons_vals = [decades[d].get("unique_persons", 0) for d in labels]
    anime_vals = [decades[d].get("unique_anime", 0) for d in labels]

    x = np.arange(len(labels))
    width = 0.25

    ax1.bar(x - width, credits_vals, width, label="Credits", color="#2196F3", alpha=0.8)
    ax1.bar(x, persons_vals, width, label="Persons", color="#4CAF50", alpha=0.8)
    ax1.bar(x + width, anime_vals, width, label="Anime", color="#FF9800", alpha=0.8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_title("Volume by Decade", fontsize=12)
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Avg anime score by decade
    avg_scores = [decades[d].get("avg_anime_score", 0) for d in labels]
    bars = ax2.bar(labels, avg_scores, color="#9C27B0", alpha=0.8)
    ax2.set_title("Average Anime Score by Decade", fontsize=12)
    ax2.set_ylim(0, 10)
    for bar, val in zip(bars, avg_scores):
        if val > 0:
            ax2.text(
                bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                f"{val:.1f}", ha="center", va="bottom", fontsize=10,
            )
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "decade_comparison.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("decade_comparison_chart_saved", path=str(output_path))


def plot_role_flow_sankey(
    role_flow_data: dict,
    output_path: Path | None = None,
) -> None:
    """役職遷移フローを簡易サンキー風チャートで可視化.

    matplotlib のみで表現する (plotly なし)。
    """
    links = role_flow_data.get("links", [])
    if not links:
        return

    fig, ax = plt.subplots(figsize=(12, 8))

    # Collect unique stages and their positions
    sources = sorted(set(lk["source"] for lk in links))
    targets = sorted(set(lk["target"] for lk in links))
    all_stages = sorted(set(sources) | set(targets))

    if len(all_stages) < 2:
        plt.close(fig)
        return

    # Position stages vertically
    stage_y = {s: i for i, s in enumerate(all_stages)}
    max_val = max(lk.get("value", 1) for lk in links)

    # Draw flows as arrows
    for link in sorted(links, key=lambda lk: -lk.get("value", 0))[:30]:
        src_y = stage_y.get(link["source"], 0)
        tgt_y = stage_y.get(link["target"], 0)
        val = link.get("value", 1)
        alpha = min(0.8, val / max_val * 0.8 + 0.1)
        lw = max(1, val / max_val * 10)

        ax.annotate(
            "", xy=(1, tgt_y), xytext=(0, src_y),
            arrowprops=dict(
                arrowstyle="->", color="#2196F3", alpha=alpha,
                lw=lw, connectionstyle="arc3,rad=0.2",
            ),
        )
        # Label the flow
        mid_y = (src_y + tgt_y) / 2
        ax.text(0.5, mid_y, str(val), ha="center", va="center", fontsize=8, alpha=0.6)

    # Label stages
    for stage, y in stage_y.items():
        ax.text(-0.1, y, stage, ha="right", va="center", fontsize=10, fontweight="bold")
        ax.text(1.1, y, stage, ha="left", va="center", fontsize=10, fontweight="bold")

    ax.set_xlim(-0.5, 1.5)
    ax.set_ylim(-0.5, len(all_stages) - 0.5)
    ax.set_title("Role Transition Flow", fontsize=14)
    ax.axis("off")

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "role_flow.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("role_flow_chart_saved", path=str(output_path))


def plot_time_series(
    ts_data: dict,
    output_path: Path | None = None,
) -> None:
    """年別クレジット数と人数の時系列チャート.

    Args:
        ts_data: time_series.json の内容 (years, series キー含む)
        output_path: 出力先
    """
    years = ts_data.get("years", [])
    series = ts_data.get("series", {})
    if not years or not series:
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    cc = series.get("credit_count", {})
    ap = series.get("active_persons", {})
    ua = series.get("unique_anime", {})
    credit_counts = [cc.get(str(y), cc.get(y, 0)) for y in years]
    person_counts = [ap.get(str(y), ap.get(y, 0)) for y in years]
    anime_counts = [ua.get(str(y), ua.get(y, 0)) for y in years]

    # Top panel: credit and anime count
    ax1.bar(years, credit_counts, color="#2196F3", alpha=0.7, label="Credits")
    ax1.set_ylabel("Credits", color="#2196F3")
    ax1.tick_params(axis="y", labelcolor="#2196F3")

    ax1b = ax1.twinx()
    ax1b.plot(years, anime_counts, "D-", color="#FF9800", linewidth=2, label="Anime")
    ax1b.set_ylabel("Anime", color="#FF9800")
    ax1b.tick_params(axis="y", labelcolor="#FF9800")

    ax1.set_title("Industry Activity Over Time", fontsize=14)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1b.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    # Bottom panel: unique persons
    ax2.fill_between(years, person_counts, color="#4CAF50", alpha=0.4)
    ax2.plot(years, person_counts, "o-", color="#4CAF50", linewidth=2)
    ax2.set_ylabel("Unique Persons")
    ax2.set_xlabel("Year")
    ax2.set_title("Active Persons per Year", fontsize=12)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "time_series.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("time_series_chart_saved", path=str(output_path))


def plot_productivity_distribution(
    prod_data: dict,
    output_path: Path | None = None,
) -> None:
    """生産性分布の可視化.

    Args:
        prod_data: productivity.json の内容 ({person_id: {credits_per_year, ...}})
        output_path: 出力先
    """
    if not prod_data:
        return

    cpy_values = [v["credits_per_year"] for v in prod_data.values() if "credits_per_year" in v]
    consistency_values = [v["consistency_score"] for v in prod_data.values() if "consistency_score" in v]

    if not cpy_values:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: credits per year histogram
    ax1.hist(cpy_values, bins=25, color="#2196F3", alpha=0.7, edgecolor="white")
    ax1.axvline(np.mean(cpy_values), color="red", linestyle="--", label=f"Mean: {np.mean(cpy_values):.1f}")
    ax1.set_title("Credits per Year Distribution", fontsize=12)
    ax1.set_xlabel("Credits / Year")
    ax1.set_ylabel("Count")
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: consistency score histogram
    if consistency_values:
        ax2.hist(consistency_values, bins=20, color="#4CAF50", alpha=0.7, edgecolor="white")
        ax2.axvline(np.mean(consistency_values), color="red", linestyle="--", label=f"Mean: {np.mean(consistency_values):.2f}")
        ax2.set_title("Consistency Score Distribution", fontsize=12)
        ax2.set_xlabel("Consistency (active years / career span)")
        ax2.set_ylabel("Count")
        ax2.set_xlim(0, 1.05)
        ax2.legend()
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "productivity.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("productivity_chart_saved", path=str(output_path))


def plot_influence_tree(
    influence_data: dict,
    output_path: Path | None = None,
) -> None:
    """影響力ツリーの可視化.

    Args:
        influence_data: influence.json の内容
        output_path: 出力先
    """
    mentors = influence_data.get("mentors", {})
    if not mentors:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Mentee count distribution
    mentee_counts = [len(m.get("mentees", [])) for m in mentors.values()]
    if mentee_counts:
        bins = range(0, max(mentee_counts) + 2)
        ax1.hist(mentee_counts, bins=bins, color="#E91E63", alpha=0.7, edgecolor="white")
        ax1.set_title("Mentees per Mentor", fontsize=12)
        ax1.set_xlabel("Number of Mentees")
        ax1.set_ylabel("Number of Mentors")
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)

    # Right: Top mentors bar chart
    sorted_mentors = sorted(mentors.items(), key=lambda x: len(x[1].get("mentees", [])), reverse=True)[:15]
    if sorted_mentors:
        names = [m[1].get("name", m[0])[:20] for m in sorted_mentors]
        counts = [len(m[1].get("mentees", [])) for m in sorted_mentors]
        y_pos = np.arange(len(names))
        ax2.barh(y_pos, counts, color="#9C27B0", alpha=0.8)
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(names, fontsize=8)
        ax2.set_title("Top 15 Mentors by Mentee Count", fontsize=12)
        ax2.set_xlabel("Mentees")
        ax2.invert_yaxis()
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "influence_tree.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("influence_tree_chart_saved", path=str(output_path))


def plot_milestone_summary(
    milestones_data: dict[str, list[dict]],
    output_path: Path | None = None,
) -> None:
    """マイルストーン種別分布の可視化.

    Args:
        milestones_data: milestones.json の内容 ({person_id: [milestone, ...]})
        output_path: 出力先
    """
    if not milestones_data:
        return

    # Count milestones by type
    type_counts: dict[str, int] = {}
    year_counts: dict[int, int] = {}
    for person_milestones in milestones_data.values():
        for ms in person_milestones:
            mtype = ms.get("type", "unknown")
            type_counts[mtype] = type_counts.get(mtype, 0) + 1
            if "year" in ms:
                year_counts[ms["year"]] = year_counts.get(ms["year"], 0) + 1

    if not type_counts:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: milestone type bar chart
    colors = {
        "career_start": "#4CAF50",
        "new_role": "#2196F3",
        "promotion": "#FF9800",
        "first_director": "#E91E63",
        "top_anime": "#9C27B0",
        "prolific": "#00BCD4",
    }
    labels = sorted(type_counts.keys(), key=lambda k: -type_counts[k])
    vals = [type_counts[k] for k in labels]
    bar_colors = [colors.get(k, "#757575") for k in labels]
    bars = ax1.barh(labels, vals, color=bar_colors, alpha=0.85)
    for bar, val in zip(bars, vals):
        ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                 str(val), va="center", fontsize=10)
    ax1.set_title("Milestone Types", fontsize=12)
    ax1.set_xlabel("Count")
    ax1.invert_yaxis()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: milestones per year
    if year_counts:
        years = sorted(year_counts.keys())
        counts = [year_counts[y] for y in years]
        ax2.bar(years, counts, color="#FF9800", alpha=0.7)
        ax2.set_title("Milestones per Year", fontsize=12)
        ax2.set_xlabel("Year")
        ax2.set_ylabel("Milestones")
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "milestones.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("milestones_chart_saved", path=str(output_path))


def plot_seasonal_trends(
    seasonal_data: dict,
    output_path: Path | None = None,
) -> None:
    """シーズン別トレンドの可視化.

    Args:
        seasonal_data: seasonal.json の内容
        output_path: 出力先
    """
    by_season = seasonal_data.get("by_season", {})
    if not by_season:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    season_order = ["winter", "spring", "summer", "fall"]
    seasons = [s for s in season_order if s in by_season]
    if not seasons:
        seasons = sorted(by_season.keys())

    season_colors = {
        "winter": "#2196F3",
        "spring": "#4CAF50",
        "summer": "#FF9800",
        "fall": "#E91E63",
    }

    # Left: credit count and person count per season
    x = np.arange(len(seasons))
    width = 0.35
    credit_vals = [by_season[s].get("credit_count", 0) for s in seasons]
    person_vals = [by_season[s].get("person_count", 0) for s in seasons]

    ax1.bar(x - width / 2, credit_vals, width, label="Credits",
            color=[season_colors.get(s, "#757575") for s in seasons], alpha=0.8)
    ax1.bar(x + width / 2, person_vals, width, label="Persons",
            color=[season_colors.get(s, "#757575") for s in seasons], alpha=0.5)
    ax1.set_xticks(x)
    ax1.set_xticklabels([s.capitalize() for s in seasons])
    ax1.set_title("Activity by Season", fontsize=12)
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: avg anime score per season
    avg_scores = [by_season[s].get("avg_anime_score", 0) for s in seasons]
    bars = ax2.bar([s.capitalize() for s in seasons], avg_scores,
                   color=[season_colors.get(s, "#757575") for s in seasons], alpha=0.8)
    ax2.set_title("Average Anime Score by Season", fontsize=12)
    ax2.set_ylim(0, 10)
    for bar, val in zip(bars, avg_scores):
        if val > 0:
            ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                     f"{val:.1f}", ha="center", va="bottom", fontsize=10)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "seasonal.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("seasonal_chart_saved", path=str(output_path))


def plot_bridge_analysis(
    bridge_data: dict,
    output_path: Path | None = None,
) -> None:
    """ブリッジパーソンのスコア分布と上位者の可視化.

    Args:
        bridge_data: bridges.json の内容
        output_path: 出力先
    """
    bridge_persons = bridge_data.get("bridge_persons", [])
    if not bridge_persons:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: bridge score distribution
    scores = [bp["bridge_score"] for bp in bridge_persons]
    ax1.hist(scores, bins=20, color="#E91E63", alpha=0.7, edgecolor="white")
    ax1.axvline(np.mean(scores), color="red", linestyle="--", label=f"Mean: {np.mean(scores):.1f}")
    ax1.set_title("Bridge Score Distribution", fontsize=12)
    ax1.set_xlabel("Bridge Score")
    ax1.set_ylabel("Count")
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: top bridge persons
    top = sorted(bridge_persons, key=lambda x: -x["bridge_score"])[:15]
    if top:
        names = [bp.get("person_id", "?")[:20] for bp in top]
        bscores = [bp["bridge_score"] for bp in top]
        communities = [bp.get("communities_connected", 0) for bp in top]
        y_pos = np.arange(len(names))
        ax2.barh(y_pos, bscores, color="#2196F3", alpha=0.8, label="Bridge Score")
        # Annotate with communities connected
        for i, (bs, cc) in enumerate(zip(bscores, communities)):
            ax2.text(bs + 1, i, f"{cc} comm.", va="center", fontsize=8, color="#666")
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(names, fontsize=8)
        ax2.set_title("Top Bridge Persons", fontsize=12)
        ax2.set_xlabel("Bridge Score")
        ax2.invert_yaxis()
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "bridges.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("bridge_chart_saved", path=str(output_path))


def plot_collaboration_strength(
    collab_pairs: list[dict],
    output_path: Path | None = None,
) -> None:
    """コラボレーション強度の分布と上位ペアの可視化.

    Args:
        collab_pairs: collaborations.json の内容 (list of pair dicts)
        output_path: 出力先
    """
    if not collab_pairs:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: strength score distribution
    strength_scores = [p.get("strength_score", 0) for p in collab_pairs]
    ax1.hist(strength_scores, bins=25, color="#2196F3", alpha=0.7, edgecolor="white")
    ax1.axvline(np.mean(strength_scores), color="red", linestyle="--",
                label=f"Mean: {np.mean(strength_scores):.1f}")
    ax1.set_title("Collaboration Strength Distribution", fontsize=12)
    ax1.set_xlabel("Strength Score")
    ax1.set_ylabel("Pairs")
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: longevity vs shared works scatter
    longevity = [p.get("longevity", 0) for p in collab_pairs]
    shared = [p.get("shared_works", 0) for p in collab_pairs]
    ax2.scatter(shared, longevity, c=strength_scores, cmap="viridis", alpha=0.6, s=30)
    ax2.set_title("Shared Works vs Longevity", fontsize=12)
    ax2.set_xlabel("Shared Works")
    ax2.set_ylabel("Longevity (years)")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "collaborations.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("collaboration_strength_chart_saved", path=str(output_path))


def plot_tag_summary(
    tags_data: dict,
    output_path: Path | None = None,
) -> None:
    """タグ分布の可視化.

    Args:
        tags_data: tags.json の内容 (tag_summary キー含む)
        output_path: 出力先
    """
    tag_summary = tags_data.get("tag_summary", {})
    if not tag_summary:
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    # Sort by count descending, take top 20
    sorted_tags = sorted(tag_summary.items(), key=lambda x: -x[1])[:20]
    labels = [t[0] for t in sorted_tags]
    counts = [t[1] for t in sorted_tags]

    colors = plt.cm.tab20(np.linspace(0, 1, len(labels)))
    y_pos = np.arange(len(labels))
    bars = ax.barh(y_pos, counts, color=colors, alpha=0.85)

    for bar, val in zip(bars, counts):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", fontsize=9)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_title("Person Tag Distribution (Top 20)", fontsize=14)
    ax.set_xlabel("Count")
    ax.invert_yaxis()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "tags.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("tag_summary_chart_saved", path=str(output_path))


def plot_studio_comparison(
    studio_data: dict[str, dict],
    output_path: Path | None = None,
) -> None:
    """スタジオ間の人材比較チャート.

    Args:
        studio_data: studios.json の内容 ({studio_name: {...}})
        output_path: 出力先
    """
    if not studio_data:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))

    # Sort studios by person count
    sorted_studios = sorted(studio_data.items(), key=lambda x: -x[1].get("person_count", 0))[:15]
    if not sorted_studios:
        plt.close(fig)
        return

    names = [s[0][:25] for s in sorted_studios]
    person_counts = [s[1].get("person_count", 0) for s in sorted_studios]
    avg_scores = [s[1].get("avg_person_score", 0) or 0 for s in sorted_studios]

    # Left: person count per studio
    y_pos = np.arange(len(names))
    ax1.barh(y_pos, person_counts, color="#2196F3", alpha=0.8)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(names, fontsize=8)
    ax1.set_title("Staff Count by Studio (Top 15)", fontsize=12)
    ax1.set_xlabel("Persons")
    ax1.invert_yaxis()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: avg person score per studio
    ax2.barh(y_pos, avg_scores, color="#4CAF50", alpha=0.8)
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(names, fontsize=8)
    ax2.set_title("Avg Person Score by Studio", fontsize=12)
    ax2.set_xlabel("Avg Composite Score")
    ax2.invert_yaxis()
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "studios.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("studio_chart_saved", path=str(output_path))


def plot_outlier_summary(
    outlier_data: dict,
    output_path: Path | None = None,
) -> None:
    """外れ値の軸別分布を可視化.

    Args:
        outlier_data: outliers.json の内容
        output_path: 出力先
    """
    axis_outliers = outlier_data.get("axis_outliers", {})
    if not axis_outliers:
        return

    axes = sorted(axis_outliers.keys())
    if not axes:
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    x = np.arange(len(axes))
    width = 0.35
    high_counts = [len(axis_outliers[a].get("high", [])) for a in axes]
    low_counts = [len(axis_outliers[a].get("low", [])) for a in axes]

    ax.bar(x - width / 2, high_counts, width, label="High Outliers", color="#E91E63", alpha=0.8)
    ax.bar(x + width / 2, low_counts, width, label="Low Outliers", color="#2196F3", alpha=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([a.capitalize() for a in axes])
    ax.set_title("Outliers by Score Axis", fontsize=14)
    ax.set_ylabel("Count")
    ax.legend()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "outliers.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("outlier_chart_saved", path=str(output_path))


def plot_transition_heatmap(
    transition_data: dict,
    output_path: Path | None = None,
) -> None:
    """役職遷移のヒートマップを可視化.

    Args:
        transition_data: transitions.json の内容
        output_path: 出力先
    """
    transitions = transition_data.get("transitions", [])
    if not transitions:
        return

    # Build stage labels
    stage_labels = {
        1: "In-Between",
        2: "2nd Key",
        3: "Key Anim.",
        4: "Anim. Dir.",
        5: "Chief AD",
        6: "Director",
    }

    # Find all stages used
    stages_used = set()
    for t in transitions:
        stages_used.add(t["from_stage"])
        stages_used.add(t["to_stage"])
    stages = sorted(stages_used)

    if len(stages) < 2:
        return

    # Build transition matrix
    stage_idx = {s: i for i, s in enumerate(stages)}
    n = len(stages)
    matrix = np.zeros((n, n))
    for t in transitions:
        fi = stage_idx.get(t["from_stage"])
        ti = stage_idx.get(t["to_stage"])
        if fi is not None and ti is not None:
            matrix[fi][ti] = t.get("count", 0)

    fig, ax = plt.subplots(figsize=(10, 8))

    im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
    labels = [stage_labels.get(s, str(s)) for s in stages]
    ax.set_xticks(np.arange(n))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.set_yticks(np.arange(n))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("To Stage")
    ax.set_ylabel("From Stage")
    ax.set_title("Role Transition Heatmap", fontsize=14)

    # Annotate cells
    for i in range(n):
        for j in range(n):
            val = int(matrix[i][j])
            if val > 0:
                ax.text(j, i, str(val), ha="center", va="center",
                        color="white" if val > matrix.max() / 2 else "black", fontsize=10)

    fig.colorbar(im, ax=ax, label="Transition Count")
    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "transitions.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("transition_heatmap_saved", path=str(output_path))


def plot_anime_stats(
    anime_data: dict[str, dict],
    output_path: Path | None = None,
) -> None:
    """アニメ統計の可視化 — スタッフ数 vs スコアの散布図.

    Args:
        anime_data: anime_stats.json の内容 ({anime_id: {...}})
        output_path: 出力先
    """
    if not anime_data:
        return

    entries = list(anime_data.values())
    scores = [e.get("score", 0) or 0 for e in entries]
    persons = [e.get("unique_persons", 0) for e in entries]
    avg_person_scores = [e.get("avg_person_score", 0) or 0 for e in entries]

    if not any(s > 0 for s in scores):
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: unique persons vs anime score
    sc = ax1.scatter(persons, scores, c=avg_person_scores, cmap="viridis",
                     alpha=0.6, s=40, edgecolors="white", linewidth=0.5)
    ax1.set_title("Staff Count vs Anime Score", fontsize=12)
    ax1.set_xlabel("Unique Persons")
    ax1.set_ylabel("Anime Score")
    fig.colorbar(sc, ax=ax1, label="Avg Person Score")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: histogram of anime scores
    valid_scores = [s for s in scores if s > 0]
    if valid_scores:
        ax2.hist(valid_scores, bins=20, color="#9C27B0", alpha=0.7, edgecolor="white")
        ax2.axvline(np.mean(valid_scores), color="red", linestyle="--",
                    label=f"Mean: {np.mean(valid_scores):.1f}")
        ax2.set_title("Anime Score Distribution", fontsize=12)
        ax2.set_xlabel("Score")
        ax2.set_ylabel("Count")
        ax2.legend()
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "anime_stats.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("anime_stats_chart_saved", path=str(output_path))


def plot_genre_affinity(
    genre_data: dict[str, dict],
    output_path: Path | None = None,
) -> None:
    """ジャンル親和性の可視化 — スコア層・時代分布.

    Args:
        genre_data: genre_affinity.json の内容 ({person_id: {primary_tier, primary_era, ...}})
        output_path: 出力先
    """
    if not genre_data:
        return

    # Count tier/era distributions
    tier_counts: dict[str, int] = {}
    era_counts: dict[str, int] = {}
    for entry in genre_data.values():
        tier = entry.get("primary_tier", "unknown")
        era = entry.get("primary_era", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        era_counts[era] = era_counts.get(era, 0) + 1

    if not tier_counts and not era_counts:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: score tier distribution (pie chart)
    tier_colors = {"high": "#4CAF50", "mid": "#FF9800", "low": "#F44336", "unknown": "#9E9E9E"}
    if tier_counts:
        labels = sorted(tier_counts.keys())
        values = [tier_counts[k] for k in labels]
        colors = [tier_colors.get(k, "#757575") for k in labels]
        ax1.pie(values, labels=labels, colors=colors, autopct="%1.0f%%", startangle=90)
        ax1.set_title("Score Tier Distribution", fontsize=12)

    # Right: era distribution
    era_order = ["classic", "2000s", "2010s", "modern", "unknown"]
    era_colors_map = {"classic": "#9C27B0", "2000s": "#2196F3", "2010s": "#4CAF50", "modern": "#FF9800", "unknown": "#9E9E9E"}
    if era_counts:
        eras = [e for e in era_order if e in era_counts]
        if not eras:
            eras = sorted(era_counts.keys())
        vals = [era_counts[e] for e in eras]
        colors = [era_colors_map.get(e, "#757575") for e in eras]
        bars = ax2.bar(eras, vals, color=colors, alpha=0.85)
        for bar, val in zip(bars, vals):
            ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                     str(val), ha="center", va="bottom", fontsize=10)
        ax2.set_title("Era Distribution", fontsize=12)
        ax2.set_xlabel("Era")
        ax2.set_ylabel("Persons")
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "genre_affinity.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("genre_affinity_chart_saved", path=str(output_path))


def plot_crossval_stability(
    crossval_data: dict,
    output_path: Path | None = None,
) -> None:
    """交差検証安定性の可視化.

    Args:
        crossval_data: crossval.json の内容
        output_path: 出力先
    """
    folds = crossval_data.get("fold_results", [])
    if not folds:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    fold_ids = list(range(1, len(folds) + 1))
    correlations = [f.get("correlation", 0) for f in folds]
    overlaps = [f.get("top10_overlap", 0) for f in folds]

    # Left: rank correlation per fold
    ax1.bar(fold_ids, correlations, color="#2196F3", alpha=0.8)
    avg_corr = crossval_data.get("avg_rank_correlation", 0)
    ax1.axhline(avg_corr, color="red", linestyle="--", label=f"Avg: {avg_corr:.3f}")
    ax1.set_title("Rank Correlation by Fold", fontsize=12)
    ax1.set_xlabel("Fold")
    ax1.set_ylabel("Spearman Correlation")
    ax1.set_ylim(0, 1.05)
    ax1.legend()
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    # Right: top-10 overlap per fold
    ax2.bar(fold_ids, overlaps, color="#4CAF50", alpha=0.8)
    avg_overlap = crossval_data.get("avg_top10_overlap", 0)
    ax2.axhline(avg_overlap, color="red", linestyle="--", label=f"Avg: {avg_overlap:.1f}%")
    ax2.set_title("Top-10 Overlap by Fold", fontsize=12)
    ax2.set_xlabel("Fold")
    ax2.set_ylabel("Overlap (%)")
    ax2.set_ylim(0, 105)
    ax2.legend()
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout()

    if output_path is None:
        output_path = JSON_DIR.parent / "crossval.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("crossval_chart_saved", path=str(output_path))

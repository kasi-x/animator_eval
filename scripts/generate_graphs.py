#!/usr/bin/env python3
"""静的グラフ生成スクリプト (matplotlib).

Usage:
    pixi run python scripts/generate_graphs.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.visualize import (
    plot_anime_stats,
    plot_bridge_analysis,
    plot_collaboration_network,
    plot_collaboration_strength,
    plot_crossval_stability,
    plot_decade_comparison,
    plot_genre_affinity,
    plot_growth_trends,
    plot_influence_tree,
    plot_milestone_summary,
    plot_network_evolution,
    plot_outlier_summary,
    plot_performance_metrics,
    plot_productivity_distribution,
    plot_role_flow_sankey,
    plot_score_distribution,
    plot_seasonal_trends,
    plot_studio_comparison,
    plot_tag_summary,
    plot_time_series,
    plot_top_persons_radar,
    plot_transition_heatmap,
)

JSON_DIR = Path("result/json")
GRAPHS_DIR = Path("result/graphs")


def main():
    """全グラフを生成."""
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    # グラフ生成関数リスト（JSONファイルが必要なもののみ）
    plot_functions = [
        ("performance_metrics.png", plot_performance_metrics),
        ("score_distribution.png", plot_score_distribution),
        ("top_persons_radar.png", plot_top_persons_radar),
        ("collaboration_network.png", plot_collaboration_network),
        ("growth_trends.png", plot_growth_trends),
        ("network_evolution.png", plot_network_evolution),
        ("decade_comparison.png", plot_decade_comparison),
        ("role_flow_sankey.png", plot_role_flow_sankey),
        ("time_series.png", plot_time_series),
        ("productivity_distribution.png", plot_productivity_distribution),
        ("influence_tree.png", plot_influence_tree),
        ("milestone_summary.png", plot_milestone_summary),
        ("seasonal_trends.png", plot_seasonal_trends),
        ("bridge_analysis.png", plot_bridge_analysis),
        ("collaboration_strength.png", plot_collaboration_strength),
        ("tag_summary.png", plot_tag_summary),
        ("studio_comparison.png", plot_studio_comparison),
        ("outlier_summary.png", plot_outlier_summary),
        ("transition_heatmap.png", plot_transition_heatmap),
        ("anime_stats.png", plot_anime_stats),
        ("genre_affinity.png", plot_genre_affinity),
        ("crossval_stability.png", plot_crossval_stability),
    ]

    print(f"📊 Generating {len(plot_functions)} static graphs...")
    print()

    success_count = 0
    skip_count = 0
    error_count = 0

    for filename, plot_func in plot_functions:
        try:
            print(f"  • {filename}...", end=" ", flush=True)
            plot_func(JSON_DIR, GRAPHS_DIR)
            print("✅")
            success_count += 1
        except FileNotFoundError as e:
            print(f"⏭️  (missing data: {e.filename})")
            skip_count += 1
        except Exception as e:
            print(f"❌ {type(e).__name__}: {e}")
            error_count += 1

    print()
    print(f"✅ Generated {success_count} graphs")
    if skip_count > 0:
        print(f"⏭️  Skipped {skip_count} graphs (missing data)")
    if error_count > 0:
        print(f"❌ Failed {error_count} graphs")
    print()
    print(f"Output directory: {GRAPHS_DIR.absolute()}")


if __name__ == "__main__":
    main()

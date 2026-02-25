#!/usr/bin/env python3
"""インタラクティブグラフ生成スクリプト (Plotly HTML).

Usage:
    pixi run python scripts/generate_interactive_graphs.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.visualize_interactive import (
    generate_interactive_dashboard,
    plot_interactive_network,
    plot_interactive_radar,
    plot_interactive_scatter,
    plot_interactive_score_distribution,
    plot_interactive_timeline,
)

JSON_DIR = Path("result/json")
GRAPHS_DIR = Path("result/graphs")


def main():
    """全インタラクティブグラフを生成."""
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    # グラフ生成関数リスト
    plot_functions = [
        ("interactive_score_distribution.html", plot_interactive_score_distribution),
        ("interactive_radar.html", plot_interactive_radar),
        ("interactive_scatter.html", plot_interactive_scatter),
        ("interactive_timeline.html", plot_interactive_timeline),
        ("interactive_network.html", plot_interactive_network),
        ("interactive_dashboard.html", generate_interactive_dashboard),
    ]

    print(f"📈 Generating {len(plot_functions)} interactive graphs (Plotly HTML)...")
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
    print(f"✅ Generated {success_count} interactive graphs")
    if skip_count > 0:
        print(f"⏭️  Skipped {skip_count} graphs (missing data)")
    if error_count > 0:
        print(f"❌ Failed {error_count} graphs")
    print()
    print(f"Output directory: {GRAPHS_DIR.absolute()}")


if __name__ == "__main__":
    main()

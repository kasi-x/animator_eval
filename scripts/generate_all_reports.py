#!/usr/bin/env python3
"""全レポート・ビジュアライゼーション一括生成スクリプト (v1 slim).

このスクリプトは v2 に移行済みの generate_* 関数をすべて削除した薄いラッパー。
HTML レポート生成は generate_reports_v2.py に委譲する。
このファイルが直接担当するのは:
  - explorer_data (v2 対応なし)
  - matplotlib 静的グラフ
  - Plotly インタラクティブグラフ

Usage:
    pixi run python scripts/generate_all_reports.py
    pixi run python scripts/generate_all_reports.py --only explorer
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

# Import modular report generators
from report_generators import helpers, html_templates

JSON_DIR = Path("result/json")
REPORTS_DIR = Path("result/reports")
GRAPHS_DIR = Path("result/graphs")

# Configure modules with global directories
helpers.JSON_DIR = JSON_DIR
helpers.EXPLORER_URL = "http://localhost:3000"
html_templates.JSON_DIR = JSON_DIR
html_templates.REPORTS_DIR = REPORTS_DIR

from report_generators.helpers import (  # noqa: E402
    load_json,
    compute_iv_percentiles,
    get_feat_person_scores,
)

# Compute IV percentiles once at import
IV_PCTILES = compute_iv_percentiles()



# ============================================================
# Explorer Data (no v2 equivalent)
# ============================================================


def generate_explorer_data():
    """Goエクスプローラー用軽量データを出力."""
    print("  Generating explorer data...")
    scores = get_feat_person_scores()
    if not scores or not isinstance(scores, list):
        print("    [SKIP] scores.json not available")
        return

    light = []
    for p in scores:
        entry = {k: v for k, v in p.items() if k not in ("breakdown", "score_range")}
        light.append(entry)

    out = JSON_DIR / "explorer_data.json"
    with open(out, "w") as f:
        json.dump(light, f, ensure_ascii=False)
    print(f"    -> {out} ({len(light)} persons)")



# ============================================================
# Visualization runners
# ============================================================


def run_matplotlib_visualizations():
    """既存のmatplotlib可視化を実行."""
    print("  Generating matplotlib static charts...")
    try:
        from src.analysis.visualize import (
            plot_score_distribution,
            plot_top_persons_radar,
            plot_time_series,
            plot_growth_trends,
            plot_decade_comparison,
            plot_seasonal_trends,
            plot_bridge_analysis,
            plot_transition_heatmap,
            plot_role_flow_sankey,
            plot_studio_comparison,
        )
    except ImportError as e:
        print(f"    [SKIP] Could not import visualize: {e}")
        return

    viz_dir = Path("result")
    funcs = [
        ("score_distribution", plot_score_distribution, "scores.json"),
        ("top_radar", plot_top_persons_radar, "scores.json"),
        ("time_series", plot_time_series, "time_series.json"),
        ("growth_trends", plot_growth_trends, "growth.json"),
        ("decade_comparison", plot_decade_comparison, "decades.json"),
        ("seasonal_trends", plot_seasonal_trends, "seasonal.json"),
        ("bridge_analysis", plot_bridge_analysis, "bridges.json"),
        ("transition_heatmap", plot_transition_heatmap, "transitions.json"),
        ("role_flow_sankey", plot_role_flow_sankey, "role_flow.json"),
        ("studio_comparison", plot_studio_comparison, "studios.json"),
    ]

    for name, func, data_file in funcs:
        try:
            data_path = JSON_DIR / data_file
            if not data_path.exists():
                print(f"    [SKIP] {name} (missing {data_file})")
                continue

            with open(data_path) as f:
                data = json.load(f)

            out_path = viz_dir / f"{name}.png"
            func(data, output_path=out_path)
            print(f"    -> {out_path}")
        except Exception as e:
            print(f"    [ERROR] {name}: {type(e).__name__}: {e}")


def run_interactive_visualizations():
    """Plotlyインタラクティブ可視化を実行."""
    print("  Generating Plotly interactive charts...")
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from src.analysis.visualize_interactive import (
            plot_interactive_score_distribution,
            plot_interactive_radar,
            plot_interactive_scatter,
            plot_interactive_timeline,
            plot_interactive_network,
        )
    except ImportError as e:
        print(f"    [SKIP] Could not import visualize_interactive: {e}")
        return

    # Load data
    scores_data = get_feat_person_scores()
    time_series = load_json("time_series.json")
    collabs = load_json("collaborations.json")

    if scores_data and isinstance(scores_data, list):
        try:
            plot_interactive_score_distribution(scores_data, output_path=GRAPHS_DIR / "score_distribution.html")
            print(f"    -> {GRAPHS_DIR / 'score_distribution.html'}")
        except Exception as e:
            print(f"    [ERROR] score_distribution: {e}")

        try:
            plot_interactive_radar(scores_data, top_n=15, output_path=GRAPHS_DIR / "radar_top15.html")
            print(f"    -> {GRAPHS_DIR / 'radar_top15.html'}")
        except Exception as e:
            print(f"    [ERROR] radar: {e}")

        for x, y in [("birank", "patronage"), ("birank", "person_fe"), ("patronage", "person_fe")]:
            try:
                plot_interactive_scatter(scores_data, x, y, output_path=GRAPHS_DIR / f"scatter_{x}_{y}.html")
                print(f"    -> {GRAPHS_DIR / f'scatter_{x}_{y}.html'}")
            except Exception as e:
                print(f"    [ERROR] scatter_{x}_{y}: {e}")

    if time_series:
        try:
            plot_interactive_timeline(time_series, output_path=GRAPHS_DIR / "timeline.html")
            print(f"    -> {GRAPHS_DIR / 'timeline.html'}")
        except Exception as e:
            print(f"    [ERROR] timeline: {e}")

    if collabs and isinstance(collabs, list):
        try:
            plot_interactive_network(collabs, top_n=80, output_path=GRAPHS_DIR / "network.html")
            print(f"    -> {GRAPHS_DIR / 'network.html'}")
        except Exception as e:
            print(f"    [ERROR] network: {e}")


# ============================================================
# Main
# ============================================================


def main(only: str | None = None):
    """メインエントリーポイント.

    HTML レポート生成は v2 (generate_reports_v2.py) に委譲。
    このスクリプトは explorer_data + matplotlib/Plotly 可視化のみ担当。
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    # Locally handled generators (no v2 equivalent)
    local_generators = [
        ("explorer", generate_explorer_data),
    ]

    print("=" * 60)
    print("Animetor Eval — Report Generator (v1 slim)")
    if only:
        print(f"  Target: {only}")
    print("=" * 60)
    print()

    if only:
        targets = [t.strip() for t in only.split(",")]
        generators = [(n, fn) for n, fn in local_generators if n in targets]
        if generators:
            print("[Local] Generating local-only reports...")
            for name, fn in generators:
                fn()
            print()
        else:
            # Delegate unknown names to v2 orchestrator
            import subprocess
            import sys
            cmd = [sys.executable, "scripts/generate_reports_v2.py", "--only", only]
            subprocess.run(cmd, check=False)
    else:
        # Run all local generators
        print("[Local] Generating local-only reports...")
        for name, fn in local_generators:
            fn()
        print()

        # Delegate all HTML reports to v2
        print("[v2] Delegating HTML report generation to generate_reports_v2.py...")
        import subprocess
        import sys
        result = subprocess.run(
            [sys.executable, "scripts/generate_reports_v2.py"],
            check=False,
        )
        if result.returncode != 0:
            print(f"  [WARN] generate_reports_v2.py exited with code {result.returncode}")
        print()

        # Phase 2: matplotlib static charts
        print("[Phase 2] Generating matplotlib static charts...")
        run_matplotlib_visualizations()
        print()

        # Phase 3: Plotly interactive charts
        print("[Phase 3] Generating Plotly interactive charts...")
        run_interactive_visualizations()
        print()

    # Summary
    reports = list(REPORTS_DIR.glob("*.html"))
    graphs = list(GRAPHS_DIR.glob("*.html"))
    pngs = list(Path("result").glob("*.png"))

    print("=" * 60)
    print("Generation Complete!")
    print(f"  HTML Reports:      {len(reports)} files in {REPORTS_DIR}/")
    print(f"  Interactive Charts: {len(graphs)} files in {GRAPHS_DIR}/")
    print(f"  Static Charts:     {len(pngs)} files in result/")
    print(f"  Total:             {len(reports) + len(graphs) + len(pngs)} files")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Animetor Eval — Report Generator")
    parser.add_argument("--only", help="カンマ区切りのレポート名 (例: explorer,person_ranking)")
    args = parser.parse_args()
    main(only=args.only)

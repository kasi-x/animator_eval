#!/usr/bin/env python3
"""インタラクティブグラフ生成スクリプト (Plotly HTML).

Usage:
    pixi run python scripts/generate_interactive_graphs.py
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analysis.reports.visualize_interactive import (
    generate_interactive_dashboard,
    plot_interactive_network,
    plot_interactive_radar,
    plot_interactive_scatter,
    plot_interactive_score_distribution,
    plot_interactive_timeline,
)

JSON_DIR = Path("result/json")
GRAPHS_DIR = Path("result/graphs")


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _load_scores() -> list[dict]:
    """Load scores.json as list[dict]."""
    path = JSON_DIR / "scores.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return data


def _load_timeline() -> dict:
    """Load time_series.json and normalise to {years, credit_counts}.

    time_series.json schema: {years: [...], series: {<key>: [...]}, ...}
    The visualisation function expects {years: [...], credit_counts: [...]}.
    We prefer the 'total_credits' series when available; fall back to the
    first numeric series found.
    """
    path = JSON_DIR / "time_series.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        raw = json.load(f)

    years = raw.get("years", [])
    series: dict = raw.get("series", {})

    # Prefer an explicit total-credits series
    credit_counts: list = []
    for key in ("total_credits", "credits", "credit_count", "count"):
        if key in series and isinstance(series[key], list):
            credit_counts = series[key]
            break

    # Fall back to first numeric list in series
    if not credit_counts and series:
        for v in series.values():
            if isinstance(v, list) and v and isinstance(v[0], (int, float)):
                credit_counts = v
                break

    return {"years": years, "credit_counts": credit_counts}


def _load_collaborations() -> list[dict]:
    """Load collaborations.json and normalise field names.

    collaborations.json uses person_a / person_b / shared_works.
    plot_interactive_network expects person1_id / person2_id / weight.
    """
    path = JSON_DIR / "collaborations.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        return []

    normalised = []
    for edge in raw:
        normalised.append(
            {
                "person1_id": edge.get("person1_id") or edge.get("person_a", ""),
                "person2_id": edge.get("person2_id") or edge.get("person_b", ""),
                "weight": edge.get("weight") or edge.get("shared_works", 1),
                "person1_name": edge.get("person1_name") or edge.get("person_a", ""),
                "person2_name": edge.get("person2_name") or edge.get("person_b", ""),
            }
        )
    return normalised


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """全インタラクティブグラフを生成."""
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    # Load data once (shared across multiple plots)
    scores_data: list[dict] = []
    timeline_data: dict = {}
    collab_data: list[dict] = []

    print("Loading JSON data...")
    try:
        scores_data = _load_scores()
        print(f"  scores.json: {len(scores_data)} persons")
    except FileNotFoundError as e:
        print(f"  scores.json not found ({e.filename}) — some graphs will be skipped")

    try:
        timeline_data = _load_timeline()
        n_years = len(timeline_data.get("years", []))
        print(f"  time_series.json: {n_years} years")
    except FileNotFoundError as e:
        print(f"  time_series.json not found ({e.filename}) — timeline will be skipped")

    try:
        collab_data = _load_collaborations()
        print(f"  collaborations.json: {len(collab_data)} edges")
    except FileNotFoundError as e:
        print(
            f"  collaborations.json not found ({e.filename}) — network will be skipped"
        )

    print()

    # Graph definitions: (filename, callable, args, kwargs)
    plot_tasks = [
        (
            "interactive_score_distribution.html",
            plot_interactive_score_distribution,
            (scores_data,),
            {"output_path": GRAPHS_DIR / "interactive_score_distribution.html"},
        ),
        (
            "interactive_radar.html",
            plot_interactive_radar,
            (scores_data,),
            {"output_path": GRAPHS_DIR / "interactive_radar.html"},
        ),
        (
            "interactive_scatter.html",
            plot_interactive_scatter,
            (scores_data,),
            {"output_path": GRAPHS_DIR / "interactive_scatter.html"},
        ),
        (
            "interactive_timeline.html",
            plot_interactive_timeline,
            (timeline_data,),
            {"output_path": GRAPHS_DIR / "interactive_timeline.html"},
        ),
        (
            "interactive_network.html",
            plot_interactive_network,
            (collab_data,),
            {"output_path": GRAPHS_DIR / "interactive_network.html"},
        ),
        (
            "interactive_dashboard.html",
            generate_interactive_dashboard,
            (scores_data,),
            {
                "timeline_data": timeline_data if timeline_data.get("years") else None,
                "output_dir": GRAPHS_DIR,
            },
        ),
    ]

    print(f"Generating {len(plot_tasks)} interactive graphs (Plotly HTML)...")
    print()

    success_count = 0
    skip_count = 0
    error_count = 0

    for filename, plot_func, args, kwargs in plot_tasks:
        try:
            print(f"  • {filename}...", end=" ", flush=True)
            plot_func(*args, **kwargs)
            print("OK")
            success_count += 1
        except FileNotFoundError as e:
            print(f"SKIP (missing data: {e.filename})")
            skip_count += 1
        except Exception as e:
            print(f"FAIL {type(e).__name__}: {e}")
            error_count += 1

    # ------------------------------------------------------------------
    # Regenerate result/dashboard.html (PNG-embedded visual dashboard)
    # ------------------------------------------------------------------
    print()
    print("Regenerating result/dashboard.html...")
    try:
        from src.runtime.report import generate_visual_dashboard

        generate_visual_dashboard(
            results=scores_data,
            png_dir=Path("result"),
            output_path=Path("result/dashboard.html"),
        )
        print("  result/dashboard.html updated")
    except Exception as e:
        print(f"  dashboard.html FAIL {type(e).__name__}: {e}")

    print()
    print(f"Generated {success_count} interactive graphs")
    if skip_count > 0:
        print(f"Skipped {skip_count} graphs (missing data)")
    if error_count > 0:
        print(f"Failed {error_count} graphs")
    print()
    print(f"Output directory: {GRAPHS_DIR.absolute()}")


if __name__ == "__main__":
    main()

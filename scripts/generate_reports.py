#!/usr/bin/env python3
"""Animetor Eval — v2 Report Generator.

Generates REPORT_PHILOSOPHY v2-compliant reports using the class-based
report architecture.

Usage:
    # Generate all reports
    pixi run python scripts/generate_reports_v2.py

    # List available reports
    pixi run python scripts/generate_reports_v2.py --list

    # Generate specific reports
    pixi run python scripts/generate_reports_v2.py --only person_ranking,exit_analysis

    # Recompute DB features before generating
    pixi run python scripts/generate_reports_v2.py --recompute career_gaps,work_context

    # Recompute ALL DB features
    pixi run python scripts/generate_reports_v2.py --recompute all

    # Override exit thresholds
    pixi run python scripts/generate_reports_v2.py --exit-years 5 --semi-exit-years 3

    # Skip reports whose HTML already exists
    pixi run python scripts/generate_reports_v2.py --skip-existing

    # Force regeneration (ignore errors, continue to next report)
    pixi run python scripts/generate_reports_v2.py --force

    # Dry run: show what would be generated
    pixi run python scripts/generate_reports_v2.py --dry-run

    # Exclude specific reports
    pixi run python scripts/generate_reports_v2.py --exclude index,derived_params

    # Show report categories
    pixi run python scripts/generate_reports_v2.py --list --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from src.analysis.io.gold_writer import gold_connect
from scripts.report_generators.reports import V2_REPORT_CLASSES

# Import for visualization functions
from report_generators import helpers, html_templates

REPORTS_DIR = Path("result/reports")
JSON_DIR = Path("result/json")
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

# ── Report categories (for --category filter) ─────────────────────

REPORT_CATEGORIES: dict[str, list[str]] = {
    # Phase 3-5 (2026-04-19): 12 reports moved to archived/ and removed
    # from V2_REPORT_CLASSES. See docs/REPORT_INVENTORY.md for the
    # consolidate_into map. Only live, audience-aligned categories below.
    "overview": [
        "index_page", "industry_overview", "person_parameter_card",
        "bias_detection",
    ],
    "brief_index": [
        "policy_brief_index", "hr_brief_index", "biz_brief_index",
    ],
    "policy": [
        "policy_attrition", "policy_monopsony", "policy_gender_bottleneck",
        "policy_generational_health", "compensation_fairness",
    ],
    "hr": [
        "mgmt_studio_benchmark", "mgmt_director_mentor",
        "mgmt_attrition_risk", "mgmt_succession", "mgmt_team_chemistry",
        "growth_scores",
    ],
    "biz": [
        "biz_genre_whitespace", "biz_undervalued_talent", "biz_trust_entry",
        "biz_team_template", "biz_independent_unit",
    ],
    "technical": [
        "akm_diagnostics", "dml_causal_inference", "score_layers_analysis",
        "shap_explanation", "longitudinal_analysis", "ml_clustering",
        "network_analysis", "network_graph", "network_evolution",
        "cooccurrence_groups", "madb_coverage", "derived_params",
        "cohort_animation", "knowledge_network", "temporal_foresight",
        "bridge_analysis",
    ],
}

# Inverse: report_name -> category
_REPORT_TO_CATEGORY = {
    name: cat for cat, names in REPORT_CATEGORIES.items() for name in names
}

# ── Recomputable DB features ──────────────────────────────────────

RECOMPUTABLE_FEATURES: dict[str, dict] = {
    "career_gaps": {
        "description": "Career gaps (exit/semi-exit/return statistics)",
        "function": None,
        "module": None,
    },
    "work_context": {
        "description": "Work context (scale_tier, production_scale)",
        "function": None,
        "module": None,
    },
    "studio_affiliation": {
        "description": "Person × studio × year affiliations",
        "function": None,
        "module": None,
    },
    "career_annual": {
        "description": "Career annual metrics (iv_score_year etc.)",
        "function": None,
        "module": None,
    },
    "cluster_membership": {
        "description": "Person cluster memberships (community, career_track, studio_cluster)",
        "function": None,  # filled by pipeline
        "module": None,
    },
}


def _recompute_features(
    conn,
    features: list[str],
    *,
    exit_years: int = 5,
    semi_exit_years: int = 3,
) -> None:
    """Recompute selected DB features."""
    import importlib

    for feat_name in features:
        spec = RECOMPUTABLE_FEATURES.get(feat_name)
        if not spec:
            print(f"  [WARN] Unknown feature: {feat_name}")
            continue

        func_name = spec.get("function")
        mod_name = spec.get("module")
        if not func_name or not mod_name:
            print(f"  [SKIP] {feat_name}: no standalone compute function (pipeline-only)")
            continue

        print(f"  Recomputing {feat_name}...", end=" ", flush=True)
        t0 = time.monotonic()
        try:
            mod = importlib.import_module(mod_name)
            func = getattr(mod, func_name)
            if feat_name == "career_gaps":
                result = func(
                    conn,
                    exit_years=exit_years,
                    semi_exit_years=semi_exit_years,
                )
            else:
                result = func(conn)
            elapsed = time.monotonic() - t0
            print(f"OK ({result} rows, {elapsed:.1f}s)")
        except Exception as exc:
            elapsed = time.monotonic() - t0
            print(f"ERROR: {type(exc).__name__}: {exc} ({elapsed:.1f}s)")


# ── Explorer Data (from v1 generate_all_reports.py) ──────────────────


def generate_explorer_data():
    """Go エクスプローラー用軽量データを出力."""
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
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(light, f, ensure_ascii=False)
    print(f"    -> {out} ({len(light)} persons)")


# ── Visualization runners (from v1 generate_all_reports.py) ──────────


def run_matplotlib_visualizations():
    """既存の matplotlib 可視化を実行."""
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
    """Plotly インタラクティブ可視化を実行."""
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


def main(
    *,
    only: str | None = None,
    exclude: str | None = None,
    category: str | None = None,
    list_reports: bool = False,
    verbose: bool = False,
    recompute: str | None = None,
    skip_existing: bool = False,
    force: bool = False,
    dry_run: bool = False,
    exit_years: int = 5,
    semi_exit_years: int = 3,
    explorer: bool = False,
    viz: bool = False,
) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Handle explorer/viz requests (from v1 shim)
    if explorer:
        generate_explorer_data()
        return
    if viz:
        print("=" * 60)
        print("Animetor Eval — Visualization Generator")
        print("=" * 60)
        print()
        run_matplotlib_visualizations()
        print()
        run_interactive_visualizations()
        print()
        print("=" * 60)
        print("Visualization generation complete!")
        print("=" * 60)
        return

    # Build name -> class map
    report_map = {cls.__dict__.get("name", cls.__name__): cls for cls in V2_REPORT_CLASSES}

    # ── --list ─────────────────────────────────────────────────────
    if list_reports:
        print("Available v2 reports:")
        if verbose:
            for cat_name, cat_reports in REPORT_CATEGORIES.items():
                print(f"\n  [{cat_name}]")
                for name in cat_reports:
                    cls = report_map.get(name)
                    if cls:
                        exists = (REPORTS_DIR / cls.filename).exists()
                        marker = "✓" if exists else " "
                        print(f"    {marker} {name:30s} → {cls.filename}")
                    else:
                        print(f"      {name:30s} (not implemented)")
            print(f"\n  Recomputable features: {', '.join(RECOMPUTABLE_FEATURES)}")
            print(f"  Categories: {', '.join(REPORT_CATEGORIES)}")
        else:
            for name, cls in report_map.items():
                cat = _REPORT_TO_CATEGORY.get(name, "?")
                print(f"  {name:30s} [{cat:10s}] → {cls.filename}")
        return

    # ── Resolve target reports ─────────────────────────────────────
    if only:
        targets = [t.strip() for t in only.split(",")]
        classes_to_run = [report_map[t] for t in targets if t in report_map]
        missing = [t for t in targets if t not in report_map]
        if missing:
            print(f"Unknown report(s): {', '.join(missing)}")
            print(f"Available: {', '.join(report_map)}")
    elif category:
        cat_names = [c.strip() for c in category.split(",")]
        target_names: list[str] = []
        for cn in cat_names:
            if cn in REPORT_CATEGORIES:
                target_names.extend(REPORT_CATEGORIES[cn])
            else:
                print(f"Unknown category: {cn}")
                print(f"Available: {', '.join(REPORT_CATEGORIES)}")
                return
        classes_to_run = [report_map[n] for n in target_names if n in report_map]
    else:
        classes_to_run = list(V2_REPORT_CLASSES)

    if exclude:
        exclude_set = {t.strip() for t in exclude.split(",")}
        classes_to_run = [
            cls for cls in classes_to_run
            if getattr(cls, "name", cls.__name__) not in exclude_set
        ]

    if skip_existing:
        before = len(classes_to_run)
        classes_to_run = [
            cls for cls in classes_to_run
            if not (REPORTS_DIR / cls.filename).exists()
        ]
        skipped = before - len(classes_to_run)
        if skipped:
            print(f"  Skipping {skipped} reports that already exist (--skip-existing)")

    # ── Header ─────────────────────────────────────────────────────
    print("=" * 60)
    print("Animetor Eval — v2 Report Generator")
    print(f"  Exit threshold:      {exit_years} years")
    print(f"  Semi-exit threshold:  {semi_exit_years} years")
    print(f"  Reports to generate:  {len(classes_to_run)}")
    if recompute:
        print(f"  Recompute features:   {recompute}")
    if dry_run:
        print("  Mode: DRY RUN")
    print("=" * 60)

    if dry_run:
        print("\nWould generate:")
        for cls in classes_to_run:
            name = getattr(cls, "name", cls.__name__)
            out_path = REPORTS_DIR / cls.filename
            exists = out_path.exists()
            marker = "[exists]" if exists else "[new]"
            print(f"  {name:30s} {marker} → {out_path}")
        if recompute:
            print("\nWould recompute:")
            feats = list(RECOMPUTABLE_FEATURES) if recompute == "all" else recompute.split(",")
            for f in feats:
                spec = RECOMPUTABLE_FEATURES.get(f, {})
                print(f"  {f}: {spec.get('description', '?')}")
        return

    # ── Connect to GOLD DuckDB ────────────────────────────────────
    # Inject exit thresholds before opening connection
    try:
        from scripts.report_generators.reports import industry_overview as io_mod
        io_mod.EXIT_CUTOFF_YEAR = io_mod.RELIABLE_MAX_YEAR - exit_years
        io_mod.SEMI_EXIT_CUTOFF_YEAR = io_mod.RELIABLE_MAX_YEAR - semi_exit_years
    except Exception:
        pass

    with gold_connect() as conn:
        # ── Recompute DB features ──────────────────────────────────────
        if recompute:
            print("\n--- Recomputing DB features ---")
            if recompute == "all":
                feats = list(RECOMPUTABLE_FEATURES)
            else:
                feats = [f.strip() for f in recompute.split(",")]
            _recompute_features(
                conn, feats,
                exit_years=exit_years,
                semi_exit_years=semi_exit_years,
            )
            print()

        # ── Generate reports ───────────────────────────────────────────
        n_ok = 0
        n_fail = 0
        n_skip = 0
        t_start = time.monotonic()

        for cls in classes_to_run:
            name = getattr(cls, "name", cls.__name__)
            print(f"  [{name}]", end=" ", flush=True)
            t0 = time.monotonic()
            try:
                report = cls(conn)
                out = report.generate()
                elapsed = time.monotonic() - t0
                if out:
                    print(f"-> {out} ({elapsed:.1f}s)")
                    n_ok += 1
                else:
                    print(f"[SKIP] no output ({elapsed:.1f}s)")
                    n_skip += 1
            except Exception as exc:
                elapsed = time.monotonic() - t0
                print(f"[ERROR] {type(exc).__name__}: {exc} ({elapsed:.1f}s)")
                n_fail += 1
                if not force:
                    print("  (use --force to continue after errors)")
                    break

    total = time.monotonic() - t_start
    print()
    print("=" * 60)
    print(f"Done in {total:.1f}s — {n_ok} OK, {n_skip} skipped, {n_fail} errors")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Animetor Eval v2 Report Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list --verbose              Show all reports with categories
  %(prog)s --only exit_analysis          Generate one report
  %(prog)s --category exit,scoring       Generate by category
  %(prog)s --exclude index,derived_params  Skip specific reports
  %(prog)s --recompute career_gaps       Recompute gaps then generate all
  %(prog)s --recompute all               Recompute all features
  %(prog)s --exit-years 5 --semi-exit-years 3  Override thresholds
  %(prog)s --skip-existing               Only generate missing reports
  %(prog)s --dry-run                     Preview without generating
  %(prog)s --force                       Continue after errors
""",
    )

    # Report selection
    select = parser.add_argument_group("Report selection")
    select.add_argument("--only", help="Comma-separated report names to generate")
    select.add_argument("--exclude", help="Comma-separated report names to skip")
    select.add_argument(
        "--category",
        help="Generate reports in category(s): "
             + ", ".join(REPORT_CATEGORIES),
    )
    select.add_argument("--list", action="store_true", dest="list_reports",
                        help="List available reports and exit")
    select.add_argument("--verbose", "-v", action="store_true",
                        help="Show categories and existence status in --list")

    # DB recomputation
    db_group = parser.add_argument_group("DB feature recomputation")
    db_group.add_argument(
        "--recompute",
        metavar="FEATURES",
        help="Recompute DB features before generating. "
             "'all' or comma-separated: "
             + ", ".join(RECOMPUTABLE_FEATURES),
    )

    # Thresholds
    thresh = parser.add_argument_group("Analysis thresholds")
    thresh.add_argument("--exit-years", type=int, default=5,
                        help="Years without credit to classify as exit (default: 5)")
    thresh.add_argument("--semi-exit-years", type=int, default=3,
                        help="Years without credit for semi-exit (default: 3)")

    # Behavior
    behav = parser.add_argument_group("Behavior")
    behav.add_argument("--skip-existing", action="store_true",
                       help="Skip reports whose HTML already exists")
    behav.add_argument("--force", action="store_true",
                       help="Continue generating after errors")
    behav.add_argument("--dry-run", action="store_true",
                       help="Show what would be generated without running")

    # Visualization (from v1 shim)
    viz_group = parser.add_argument_group("Visualization (v1 legacy)")
    viz_group.add_argument("--explorer", action="store_true",
                           help="Generate explorer data JSON and exit")
    viz_group.add_argument("--viz", action="store_true",
                           help="Generate matplotlib and Plotly visualizations and exit")

    args = parser.parse_args()
    main(
        only=args.only,
        exclude=args.exclude,
        category=args.category,
        list_reports=args.list_reports,
        verbose=args.verbose,
        recompute=args.recompute,
        skip_existing=args.skip_existing,
        force=args.force,
        dry_run=args.dry_run,
        exit_years=args.exit_years,
        semi_exit_years=args.semi_exit_years,
        explorer=args.explorer,
        viz=args.viz,
    )

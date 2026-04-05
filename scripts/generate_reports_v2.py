"""新レポート生成エントリポイント（v2アーキテクチャ）.

Usage:
    pixi run python scripts/generate_reports_v2.py
    pixi run python scripts/generate_reports_v2.py --only=bridge_analysis
    pixi run python scripts/generate_reports_v2.py --backend=both
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# プロジェクトルートをsys.pathに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.viz.assemblers.html_assembler import HtmlReportAssembler  # noqa: E402
from src.viz.renderers.plotly_renderer import PlotlyRenderer  # noqa: E402
from src.viz.reports.registry import get_all_reports  # noqa: E402

JSON_DIR = PROJECT_ROOT / "result" / "json"
REPORTS_DIR = PROJECT_ROOT / "result" / "reports"
MAKIE_DIR = PROJECT_ROOT / "result" / "makie"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate v2 reports")
    parser.add_argument("--only", help="Generate only this report (comma-separated names)")
    parser.add_argument(
        "--backend",
        choices=["js", "makie", "both"],
        default="js",
        help="Output backend (default: js)",
    )
    parser.add_argument("--json-dir", type=Path, default=JSON_DIR)
    parser.add_argument("--output-dir", type=Path, default=REPORTS_DIR)
    args = parser.parse_args()

    json_dir = args.json_dir
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    only_names = set(args.only.split(",")) if args.only else None

    all_reports = get_all_reports()
    renderer = PlotlyRenderer()
    assembler = HtmlReportAssembler(renderer)

    # Makie assembler (遅延初期化 — 必要な場合のみ Julia を起動)
    makie_assembler = None
    if args.backend in ("makie", "both"):
        try:
            from src.viz.assemblers.makie_assembler import MakieAssembler
            from src.viz.renderers.makie_renderer import MakieRenderer

            makie_renderer = MakieRenderer()
            if makie_renderer.is_available:
                makie_assembler = MakieAssembler(makie_renderer)
                print("[v2] Makie backend: available")
            else:
                print("[v2] Makie backend: juliacall not installed (skipping)")
        except Exception as e:
            print(f"[v2] Makie backend: initialization failed ({e})")

    print(f"[v2] Generating reports from {json_dir}")
    print(f"[v2] Output: {output_dir}")
    print(f"[v2] Backend: {args.backend}")
    print(f"[v2] Reports: {len(all_reports)} registered")
    print()

    generated = 0
    for name, builder in all_reports:
        if only_names and name not in only_names:
            continue

        t0 = time.time()
        print(f"  Building {name}...", end=" ", flush=True)

        spec = builder(json_dir)
        if spec is None:
            print("SKIP (no data)")
            continue

        # JS/Plotly backend
        if args.backend in ("js", "both"):
            html = assembler.assemble(spec)
            out_path = output_dir / f"{name}_v2.html"
            out_path.write_text(html, encoding="utf-8")
            print(f"-> {out_path.name}", end=" ", flush=True)

        # Makie/CairoMakie SVG backend
        if args.backend in ("makie", "both") and makie_assembler is not None:
            makie_out = MAKIE_DIR / name
            saved = makie_assembler.assemble(spec, makie_out)
            print(f"-> {len(saved)} SVGs", end=" ", flush=True)
        elif args.backend in ("makie", "both"):
            print("[makie: unavailable]", end=" ", flush=True)

        elapsed = time.time() - t0
        print(f"({elapsed:.1f}s)")
        generated += 1

    print(f"\n[v2] Done: {generated} reports generated.")


if __name__ == "__main__":
    main()

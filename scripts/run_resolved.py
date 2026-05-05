"""Resolved 層 ETL の CLI entry point。

Usage:
    pixi run python scripts/run_resolved.py [--anime] [--persons] [--studios]
        [--decisions-sample-rate 0.001]
        [--conformed result/animetor.duckdb]
        [--resolved result/resolved.duckdb]
        [--bronze-root result/bronze]
        [--decisions result/merge_decisions.jsonl]
        [--fast-only/--no-fast-only]

デフォルト: 3 entity 全部、decisions サンプリングなし。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.etl.resolved.resolve_anime import build_resolved_anime  # noqa: E402
from src.etl.resolved.resolve_persons import build_resolved_persons  # noqa: E402
from src.etl.resolved.resolve_studios import build_resolved_studios  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Resolved 層 ETL")
    p.add_argument("--anime", action="store_true", help="anime のみ")
    p.add_argument("--persons", action="store_true", help="persons のみ")
    p.add_argument("--studios", action="store_true", help="studios のみ")
    p.add_argument("--conformed", default="result/animetor.duckdb")
    p.add_argument("--resolved", default="result/resolved.duckdb")
    p.add_argument("--bronze-root", default="result/bronze")
    p.add_argument("--decisions", default="result/merge_decisions.jsonl")
    p.add_argument("--decisions-sample-rate", type=float, default=0.0,
                   help="0.0 = log なし、1.0 = 全件、0.001 = 0.1%%")
    p.add_argument("--fast-only", action=argparse.BooleanOptionalAction, default=True,
                   help="persons cluster: stage 1+2 のみ (default) / 全 stage")
    args = p.parse_args()

    run_all = not (args.anime or args.persons or args.studios)
    sample = float(args.decisions_sample_rate)
    decisions_path = Path(args.decisions) if sample > 0.0 else None

    # 全 entity の decisions を 1 つの jsonl に append するため、最初に truncate
    if decisions_path is not None and decisions_path.exists():
        decisions_path.unlink()

    if run_all or args.anime:
        n = build_resolved_anime(
            args.conformed, args.resolved,
            bronze_root=args.bronze_root,
            decisions_path=decisions_path,
            decisions_sample_rate=sample,
        )
        print(f"resolved.anime: {n:,} rows")

    if run_all or args.persons:
        n = build_resolved_persons(
            args.conformed, args.resolved,
            fast_only=args.fast_only,
            decisions_path=decisions_path,
            decisions_sample_rate=sample,
        )
        print(f"resolved.persons: {n:,} rows (fast_only={args.fast_only})")

    if run_all or args.studios:
        n = build_resolved_studios(
            args.conformed, args.resolved,
            decisions_path=decisions_path,
            decisions_sample_rate=sample,
        )
        print(f"resolved.studios: {n:,} rows")


if __name__ == "__main__":
    main()

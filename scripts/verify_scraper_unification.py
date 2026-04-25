"""Verify scraper unification (11_scraper_unification series).

Checks:
  - All scraper modules import OK
  - Common bases (Checkpoint, BronzeWriterGroup, RateLimitedHttpClient,
    scrape_progress) exist and are wired to client classes
  - Every scraper CLI exposes --limit / --quiet / --progress
  - bronze_compaction CLI loads

No live HTTP calls.
"""
from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRAPERS = [
    ("ann", "src.scrapers.ann_scraper", ["scrape-anime", "scrape-persons", "scrape-all"]),
    ("allcinema", "src.scrapers.allcinema_scraper", ["cinema", "persons", "run"]),
    ("bangumi", "src.scrapers.bangumi_main", ["relations", "persons", "characters", "run"]),
    ("mal", "src.scrapers.mal_scraper", []),
    ("mediaarts", "src.scrapers.mediaarts_scraper", []),
    ("seesaawiki", "src.scrapers.seesaawiki_scraper", ["scrape", "reparse"]),
    ("keyframe", "src.scrapers.keyframe_scraper", []),
    ("jvmg", "src.scrapers.jvmg_fetcher", []),
    ("anilist", "src.scrapers.anilist_scraper", []),
]

REQUIRED_FLAGS = ["--limit", "--quiet", "--progress"]


def check_imports() -> list[str]:
    failures = []
    for _, modpath, _ in SCRAPERS:
        try:
            importlib.import_module(modpath)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"import {modpath}: {exc}")
    return failures


def check_common_bases() -> list[str]:
    failures = []

    try:
        from src.scrapers.checkpoint import Checkpoint, atomic_write_json, load_json_or  # noqa: F401
    except ImportError as exc:
        failures.append(f"Checkpoint module: {exc}")

    try:
        from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup  # noqa: F401
    except ImportError as exc:
        failures.append(f"BronzeWriterGroup: {exc}")

    try:
        from src.scrapers.http_base import RateLimitedHttpClient
        from src.scrapers.ann_scraper import AnnClient
        from src.scrapers.allcinema_scraper import AllcinemaClient
        from src.scrapers.mal_scraper import JikanClient
        for cls in (AnnClient, AllcinemaClient, JikanClient):
            if not issubclass(cls, RateLimitedHttpClient):
                failures.append(f"{cls.__name__} not subclass of RateLimitedHttpClient")
    except ImportError as exc:
        failures.append(f"http_base: {exc}")

    try:
        from src.scrapers.progress import scrape_progress, progress_enabled  # noqa: F401
    except ImportError as exc:
        failures.append(f"scrape_progress: {exc}")

    try:
        from src.scrapers.cli_common import (  # noqa: F401
            LimitOpt,
            DryRunOpt,
            ResumeOpt,
            ForceOpt,
            QuietOpt,
            ProgressOpt,
            DelayOpt,
            DataDirOpt,
            CheckpointIntervalOpt,
        )
    except ImportError as exc:
        failures.append(f"cli_common: {exc}")

    return failures


def check_cli_flags() -> list[str]:
    failures = []
    for name, modpath, commands in SCRAPERS:
        if commands:
            for cmd in commands:
                help_text = _run_help([sys.executable, "-m", modpath, cmd, "--help"])
                missing = [f for f in REQUIRED_FLAGS if f not in help_text]
                if missing:
                    failures.append(f"{name} {cmd}: missing {missing}")
        else:
            help_text = _run_help([sys.executable, "-m", modpath, "--help"])
            missing = [f for f in REQUIRED_FLAGS if f not in help_text]
            if missing:
                failures.append(f"{name}: missing {missing}")

    return failures


def _run_help(cmd: list[str]) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
        return out.stdout + out.stderr
    except subprocess.TimeoutExpired:
        return ""


def check_bronze_compaction_cli() -> list[str]:
    out = _run_help([sys.executable, "-m", "src.scrapers.bronze_compaction", "--help"])
    failures = []
    for token in ["--all", "--source", "--dry-run"]:
        if token not in out:
            failures.append(f"bronze_compaction CLI missing {token}")
    return failures


def main() -> int:
    print("== 1. import check ==")
    f1 = check_imports()
    for x in f1:
        print("  FAIL:", x)
    print("  OK" if not f1 else f"  {len(f1)} failure(s)")

    print("== 2. common bases ==")
    f2 = check_common_bases()
    for x in f2:
        print("  FAIL:", x)
    print("  OK" if not f2 else f"  {len(f2)} failure(s)")

    print("== 3. CLI flags ==")
    f3 = check_cli_flags()
    for x in f3:
        print("  FAIL:", x)
    print("  OK" if not f3 else f"  {len(f3)} failure(s)")

    print("== 4. bronze_compaction CLI ==")
    f4 = check_bronze_compaction_cli()
    for x in f4:
        print("  FAIL:", x)
    print("  OK" if not f4 else f"  {len(f4)} failure(s)")

    total = len(f1) + len(f2) + len(f3) + len(f4)
    print(f"\n=== Total failures: {total} ===")
    return 0 if total == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

# Task: 全 scraper の統一機能を verify

**ID**: `11_scraper_unification/04_verification`
**Priority**: 🟢
**Estimated changes**: 約 +50 / -0 lines, 1 file (新規 verify script のみ)
**Requires senior judgment**: no
**Blocks**: (なし — 完了タスク)
**Blocked by**: `01_anilist_cli_unify`, `02_seesaawiki_progress`, `03_http_client_base`

---

## Goal

11_scraper_unification 群の最終 verify。全 scraper で以下が動くことをコード/CLI レベルで確認する verify script を `scripts/verify_scraper_unification.py` に置く。

- import 全部通る
- CLI に `--limit` / `--quiet` / `--progress` がある
- 共通基底 (`Checkpoint` / `BronzeWriterGroup` / `RateLimitedHttpClient` / `scrape_progress`) が使われている
- BRONZE compaction が動く
- `--dry-run` 系 (持つ scraper のみ) が動く

---

## Hard constraints

- 実 scrape を起動しない (smoke test は 1 req のみ)
- 既存テストを変更しない
- verify script は idempotent (何度走らせても安全)

---

## Pre-conditions

- [ ] `01_anilist_cli_unify` 完了
- [ ] `02_seesaawiki_progress` 完了
- [ ] `03_http_client_base` 完了
- [ ] `git status` clean

---

## Files to modify / create

| File | 変更内容 |
|------|---------|
| `scripts/verify_scraper_unification.py` | **新規**: verify script |

---

## Steps

### Step 1: verify script を作成

`scripts/verify_scraper_unification.py`:

```python
"""Verify scraper unification (11_scraper_unification series).

Runs lightweight static + CLI-help checks to confirm:
  - common bases (Checkpoint, BronzeWriterGroup, RateLimitedHttpClient,
    scrape_progress) are wired into every scraper module.
  - every scraper CLI exposes --limit / --quiet / --progress (or aliases).
  - bronze compaction CLI loads.

No live HTTP calls (use 03_http_client_base smoke tests for those).
"""
from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SCRAPERS = [
    ("ann", "src.scrapers.ann_scraper", ["scrape-anime", "scrape-persons", "scrape-all"]),
    ("allcinema", "src.scrapers.allcinema_scraper", ["cinema", "persons", "run"]),
    ("mal", "src.scrapers.mal_scraper", []),  # single command
    ("mediaarts", "src.scrapers.mediaarts_scraper", []),
    ("seesaawiki", "src.scrapers.seesaawiki_scraper", ["scrape", "reparse"]),
    ("keyframe", "src.scrapers.keyframe_scraper", []),
    ("jvmg", "src.scrapers.jvmg_fetcher", []),
    ("anilist", "src.scrapers.anilist_scraper", []),
]

BANGUMI_SCRIPTS = [
    REPO_ROOT / "scripts/scrape_bangumi_persons.py",
    REPO_ROOT / "scripts/scrape_bangumi_characters.py",
    REPO_ROOT / "scripts/scrape_bangumi_relations.py",
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
    # Checkpoint
    try:
        from src.scrapers.checkpoint import Checkpoint, atomic_write_json, load_json_or  # noqa: F401
    except ImportError as exc:
        failures.append(f"Checkpoint module: {exc}")

    # BronzeWriterGroup
    try:
        from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup  # noqa: F401
    except ImportError as exc:
        failures.append(f"BronzeWriterGroup: {exc}")

    # RateLimitedHttpClient + 5 subclasses (Card 03 完了後)
    try:
        from src.scrapers.http_base import RateLimitedHttpClient
        from src.scrapers.bangumi_scraper import BangumiClient
        from src.scrapers.ann_scraper import AnnClient
        from src.scrapers.allcinema_scraper import AllcinemaClient
        from src.scrapers.mal_scraper import JikanClient
        for cls in (BangumiClient, AnnClient, AllcinemaClient, JikanClient):
            if not issubclass(cls, RateLimitedHttpClient):
                failures.append(f"{cls.__name__} not subclass of RateLimitedHttpClient")
    except ImportError as exc:
        failures.append(f"http_base: {exc}")

    # scrape_progress
    try:
        from src.scrapers.progress import scrape_progress, progress_enabled  # noqa: F401
    except ImportError as exc:
        failures.append(f"scrape_progress: {exc}")

    # cli_common
    try:
        from src.scrapers.cli_common import (  # noqa: F401
            LimitOpt, DryRunOpt, ResumeOpt, ForceOpt,
            QuietOpt, ProgressOpt, DelayOpt, DataDirOpt, CheckpointIntervalOpt,
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

    for script in BANGUMI_SCRIPTS:
        help_text = _run_help([sys.executable, str(script), "--help"])
        missing = [f for f in REQUIRED_FLAGS if f not in help_text]
        if missing:
            failures.append(f"{script.name}: missing {missing}")
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
    for x in f1: print("  FAIL:", x)
    print("  OK" if not f1 else f"  {len(f1)} failure(s)")

    print("== 2. common bases ==")
    f2 = check_common_bases()
    for x in f2: print("  FAIL:", x)
    print("  OK" if not f2 else f"  {len(f2)} failure(s)")

    print("== 3. CLI flags ==")
    f3 = check_cli_flags()
    for x in f3: print("  FAIL:", x)
    print("  OK" if not f3 else f"  {len(f3)} failure(s)")

    print("== 4. bronze_compaction CLI ==")
    f4 = check_bronze_compaction_cli()
    for x in f4: print("  FAIL:", x)
    print("  OK" if not f4 else f"  {len(f4)} failure(s)")

    total = len(f1) + len(f2) + len(f3) + len(f4)
    print(f"\n=== Total failures: {total} ===")
    return 0 if total == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
```

### Step 2: verify script を実行し全 pass にする

```bash
pixi run python scripts/verify_scraper_unification.py
```

→ 期待: `Total failures: 0`

### Step 3: 失敗したら該当 card に戻って修正

| 失敗内容 | 戻る Card |
|---|---|
| `anilist: missing ['--limit']` 等 | `01_anilist_cli_unify` |
| `seesaawiki ...: missing ['--quiet', '--progress']` | `02_seesaawiki_progress` |
| `BangumiClient not subclass of RateLimitedHttpClient` | `03_http_client_base` |
| `Checkpoint module: ...` 等 | (基盤 module 既完了のはず → 環境問題) |

---

## Verification

```bash
pixi run python scripts/verify_scraper_unification.py
echo "exit code: $?"

# 既存テスト rerun
pixi run test-scoped tests/scrapers/
```

---

## Stop-if conditions

- verify script が 0 でない exit code
- 既存テスト fail
- `git diff --stat` が +100/-0 を超える (verify script は新規追加のみのはず)

---

## Rollback

```bash
rm scripts/verify_scraper_unification.py
```

---

## Completion signal

- [ ] `pixi run python scripts/verify_scraper_unification.py` が exit 0
- [ ] `pixi run test-scoped tests/scrapers/` 全 pass
- [ ] git log message: `verify: scraper unification CLI + base class checks (11_scraper_unification/04)`
- [ ] `DONE.md` に `11_scraper_unification` 1-4 完了を記録

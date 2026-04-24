# Task: 共通 utils + dry-run skeleton

**ID**: `07_json_to_parquet/01_common_utils`
**Priority**: 🟠
**Estimated changes**: 約 +80 lines, 1 file (新規)
**Requires senior judgment**: no
**Blocks**: `02_seesaawiki`, `03_allcinema`, `04_ann`, `05_madb`
**Blocked by**: なし

---

## Goal

`scripts/_migrate_common.py` (新規) を作り、全 migration script で共有する helper を定義する。

---

## Hard constraints

- H1 anime.score を scoring に使わない (BRONZE 保持は可)
- **破壊的操作禁止**: JSON ソースを削除・移動しない

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `result/bronze/` ディレクトリ存在 (なければ空でよい、`bronze_writer` が自動作成)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/_migrate_common.py` | 新規作成 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bronze_writer.py` | 既存動作中、仕様固定 |
| `src/runtime/models.py` | BronzeAnime は現状で十分、フィールド追加しない |

---

## Steps

### Step 1: helper ファイル新規作成

`scripts/_migrate_common.py` を以下内容で作成:

```python
"""Shared helpers for one-shot JSON → Bronze Parquet migration scripts.

Used by scripts/migrate_<source>_to_parquet.py. Not a module for runtime use.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any, Iterator

import structlog

log = structlog.get_logger()


def infer_date_from_mtime(path: Path) -> _dt.date:
    """ファイル mtime を date に変換 (scrape 実施日の approximation)."""
    return _dt.date.fromtimestamp(path.stat().st_mtime)


def group_files_by_date(
    files: list[Path],
    override_date: _dt.date | None = None,
) -> dict[_dt.date, list[Path]]:
    """date ごとに JSON ファイルを bucket 化 (Hive partition 用)."""
    if override_date is not None:
        return {override_date: list(files)}
    buckets: dict[_dt.date, list[Path]] = {}
    for p in files:
        d = infer_date_from_mtime(p)
        buckets.setdefault(d, []).append(p)
    return buckets


def iter_json_files(root: Path, pattern: str = "*.json") -> Iterator[Path]:
    """再帰的に JSON ファイルを yield."""
    yield from root.rglob(pattern)


def load_json(path: Path) -> Any:
    """JSON 読み込み。失敗時は None を返しログ。"""
    import json

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("migrate_json_load_failed", path=str(path), error=str(e))
        return None


def report_summary(source: str, counts: dict[str, int], dry_run: bool) -> None:
    """件数サマリを構造化ログで吐く."""
    log.info(
        "migrate_summary",
        source=source,
        dry_run=dry_run,
        **counts,
    )
```

### Step 2: smoke test

```bash
pixi run python -c "
from scripts._migrate_common import infer_date_from_mtime, group_files_by_date, iter_json_files
from pathlib import Path
files = list(iter_json_files(Path('data/allcinema')))
print('found files:', len(files))
if files:
    print('first date:', infer_date_from_mtime(files[0]))
    buckets = group_files_by_date(files)
    print('date buckets:', {str(k): len(v) for k, v in buckets.items()})
"
```

期待: エラーなく件数出力。

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. import 確認
pixi run python -c "from scripts._migrate_common import group_files_by_date; print('OK')"

# 3. 既存 test に影響しない
pixi run test-scoped tests/ -k "bronze_writer"
```

---

## Stop-if conditions

- [ ] lint 失敗
- [ ] import 失敗

---

## Rollback

```bash
rm scripts/_migrate_common.py
```

---

## Completion signal

- [ ] `scripts/_migrate_common.py` 存在
- [ ] smoke test pass
- [ ] 作業ログに `DONE: 07_json_to_parquet/01_common_utils` 記録

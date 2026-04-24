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

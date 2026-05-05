"""Atomic JSON checkpoint helper for scrapers.

Replaces 6+ duplicate `_load_checkpoint` / `_save_checkpoint` pairs scattered
across scrapers. Uses tmp-file → rename for crash safety so a Ctrl+C mid-write
never corrupts the checkpoint.

Usage:
    cp = Checkpoint.load(path)              # never raises; returns empty if missing
    cp["completed_ids"] = sorted(done)
    cp.save()                               # atomic; auto-stamps last_run_at

    # Set/list helpers for the common scrape-loop pattern:
    cp.completed_set                        # set[Hashable]
    cp.failed_ids                           # list[dict] with "id" + status info
    cp.mark_completed(item_id)              # add to set, sync into "completed_ids"
    cp.mark_failed(item_id, status="404")   # append to failed_ids list
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Hashable, Iterable

import structlog

logger = structlog.get_logger()


def atomic_write_json(path: Path, data: Any, *, ensure_ascii: bool = False, indent: int | None = None) -> None:
    """Write JSON to path atomically (tmp file → rename).

    Crash-safe: a Ctrl+C or power loss mid-write leaves the original file intact.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.tmp-")
    try:
        os.write(fd, json.dumps(data, ensure_ascii=ensure_ascii, indent=indent).encode("utf-8"))
        os.close(fd)
        Path(tmp_str).rename(path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        Path(tmp_str).unlink(missing_ok=True)
        raise


def load_json_or(path: Path, default: Any) -> Any:
    """Load JSON from path; return `default` if file missing or unparsable."""
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 — keep going with default on any read error
        logger.warning("checkpoint_load_error", path=str(path), error=str(exc))
        return default


_DEFAULT_SCHEMA: dict[str, Any] = {
    "completed_ids": [],
    "failed_ids": [],
    "last_run_at": None,
}


class Checkpoint:
    """Atomic JSON checkpoint with completed/failed-id helpers.

    Storage shape (default):
        {
            "completed_ids": [...],
            "failed_ids":    [{"id": X, "status": "...", "detail": "..."}],
            "last_run_at":   "2026-04-25T11:30:00+00:00",
            ... any other fields the caller stores ...
        }

    `Checkpoint(path)` auto-loads from disk if file exists.
    Pass `data=` to override (e.g. start fresh, or use `force_empty=True`).
    """

    def __init__(
        self,
        path: Path | str,
        data: dict[str, Any] | None = None,
        *,
        force_empty: bool = False,
    ) -> None:
        self.path = Path(path)
        if data is not None:
            self.data: dict[str, Any] = data
        elif force_empty or not self.path.exists():
            self.data = dict(_DEFAULT_SCHEMA)
        else:
            loaded = load_json_or(self.path, dict(_DEFAULT_SCHEMA))
            self.data = loaded if isinstance(loaded, dict) else dict(_DEFAULT_SCHEMA)

    # ── Construction / persistence ─────────────────────────────────────────

    @classmethod
    def load(cls, path: Path | str) -> "Checkpoint":
        """Load checkpoint or return an empty one if missing/unparsable.

        Equivalent to `Checkpoint(path)` since the constructor now auto-loads.
        Retained for explicit-intent call sites.
        """
        return cls(path)

    def save(self, *, stamp_time: bool = True) -> None:
        """Atomic write. Stamps `last_run_at` (UTC ISO) by default."""
        if stamp_time:
            self.data["last_run_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
        atomic_write_json(self.path, self.data)

    def delete(self) -> None:
        """Remove the checkpoint file (no-op if missing)."""
        self.path.unlink(missing_ok=True)

    # ── Dict-like passthrough ──────────────────────────────────────────────

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def setdefault(self, key: str, default: Any) -> Any:
        return self.data.setdefault(key, default)

    # ── Resume-loop helpers ────────────────────────────────────────────────

    @property
    def completed_set(self) -> set[Hashable]:
        """Mutable set view backed by `completed_ids`. Call `sync_completed()` after mutating."""
        return set(self.data.get("completed_ids") or [])

    @property
    def failed_ids(self) -> list[dict[str, Any]]:
        return self.data.setdefault("failed_ids", [])

    @property
    def failed_set(self) -> set[Hashable]:
        return {f["id"] for f in self.failed_ids if "id" in f}

    def pending(self, all_ids: Iterable[Hashable], limit: int = 0) -> list[Hashable]:
        """Return ids not yet completed and not in the failed list.

        Args:
            all_ids: Full candidate set to filter.
            limit:   Cap result to first N items (0 = no cap).
        """
        completed = self.completed_set
        failed = self.failed_set
        result = [i for i in all_ids if i not in completed and i not in failed]
        return result[:limit] if limit > 0 else result

    def sync_completed(self, completed: Iterable[Hashable]) -> None:
        """Replace completed_ids with sorted list of given iterable."""
        self.data["completed_ids"] = sorted(completed)  # type: ignore[type-var]

    def mark_completed(self, item_id: Hashable) -> None:
        """Add item_id to completed_ids (no-op if already present)."""
        ids: list = self.data.setdefault("completed_ids", [])
        if item_id not in ids:
            ids.append(item_id)

    def mark_failed(self, item_id: Hashable, *, status: str | int, detail: str | None = None) -> None:
        entry: dict[str, Any] = {"id": item_id, "status": status}
        if detail is not None:
            entry["detail"] = detail
        self.failed_ids.append(entry)


def resolve_checkpoint(path: Path | str, *, force: bool = False, resume: bool = True) -> Checkpoint:
    """Return a Checkpoint for `path` according to force/resume flags.

    - ``resume=True, force=False`` (default): load existing checkpoint.
    - ``force=True`` or ``resume=False``: start fresh (ignore any existing file).
    """
    if resume and not force:
        return Checkpoint(path)
    return Checkpoint(path, force_empty=True)


def prepare_checkpoint_run(
    all_ids: Iterable[Hashable],
    path: Path | str,
    *,
    limit: int = 0,
    force: bool = False,
    resume: bool = True,
) -> tuple[Checkpoint, list[Hashable], set[Hashable]]:
    """Resolve checkpoint and compute pending IDs in one call.

    Combines ``resolve_checkpoint`` + ``cp.pending(limit)`` + completed snapshot.

    Returns:
        (cp, pending_ids, completed_set)
    """
    cp = resolve_checkpoint(path, force=force, resume=resume)
    completed = set(cp.completed_set)
    pending = cp.pending(all_ids, limit=limit)
    return cp, pending, completed

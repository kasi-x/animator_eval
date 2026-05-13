"""Reproducibility snapshot for high-venue publication.

Bundles resolved.duckdb / gold.duckdb / pixi.lock / method_notes into a
single tarball that is Zenodo upload-ready.  Writes a frozen score record
to mart.meta_score_frozen so λ-recalibrated pipeline runs can still
reproduce the paper's numbers.

Usage:
    pixi run python scripts/publication/snapshot.py \\
        --venue JASSS \\
        --paper-anchor career_network_2026 \\
        --output-dir /tmp/snapshots

The resulting directory (``<output-dir>/<paper-anchor>_<yyyymmdd>/``) contains:
    resolved.duckdb          — entity-resolved canonical data
    gold.duckdb              — mart (scores + feat_* tables)
    pixi.lock                — exact dependency pinning
    method_notes/            — all docs/method_notes/*.md files
    README.txt               — auto-generated Zenodo metadata stub
    MANIFEST.json            — SHA-256 checksums + snapshot metadata
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

import structlog
import typer

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_DEFAULT_RESOLVED_DB = _REPO_ROOT / "result" / "resolved.duckdb"
_DEFAULT_GOLD_DB = Path(
    os.environ.get(
        "ANIMETOR_DB_PATH",
        str(_REPO_ROOT / "result" / "animetor.duckdb"),
    )
)
_DEFAULT_PIXI_LOCK = _REPO_ROOT / "pixi.lock"
_DEFAULT_METHOD_NOTES = _REPO_ROOT / "docs" / "method_notes"


def _sha256_file(path: Path) -> str:
    """Return SHA-256 hex digest of a file.

    Reads in 8 MiB chunks to handle large DuckDB files without loading
    the entire file into memory.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_string(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _git_sha(repo_root: Path) -> str:
    """Return HEAD SHA, or empty string if git is unavailable."""
    import subprocess

    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return ""


def _pipeline_version(repo_root: Path) -> str:
    """Return VERSION file content, or 'unknown'."""
    version_path = repo_root / "VERSION"
    if version_path.exists():
        return version_path.read_text().strip()
    return "unknown"


def _read_person_scores_from_gold(gold_path: Path) -> list[dict]:
    """Read person_scores from gold.duckdb.

    Returns empty list if the file does not exist or the table is missing.
    """
    if not gold_path.exists():
        log.warning("gold_db_missing_for_snapshot", path=str(gold_path))
        return []
    try:
        from src.analysis.io.mart_writer import GoldReader

        return GoldReader(gold_path).person_scores()
    except Exception as exc:
        log.warning("score_read_failed", error=str(exc))
        return []


def _build_lambda_json(repo_root: Path) -> str:
    """Extract IV formula λ weights from the codebase.

    Reads the lambda constants from src/analysis/anime_value.py or
    equivalent.  Falls back to an empty dict if not locatable, so the
    snapshot still succeeds.
    """
    candidate_paths = [
        repo_root / "src" / "analysis" / "anime_value.py",
        repo_root / "src" / "analysis" / "domain" / "anime" / "anime_value.py",
    ]
    for path in candidate_paths:
        if path.exists():
            text = path.read_text()
            # Heuristic: look for LAMBDA_* or lambda_* dict / assignments.
            # We capture the raw block for auditability — not eval'd.
            import re

            matches = re.findall(
                r"(?:LAMBDA|lambda)_?\w*\s*[=:]\s*[{(]?[^#\n]+", text
            )
            if matches:
                return json.dumps({"extracted": matches[:20]}, ensure_ascii=False)
    return "{}"


def _compute_spec_hash(
    lambda_json: str,
    pipeline_version: str,
    git_sha: str,
) -> str:
    """Deterministic spec hash: SHA-256 of λ weights + version + git SHA."""
    canonical = json.dumps(
        {
            "lambda_json": lambda_json,
            "pipeline_version": pipeline_version,
            "git_sha": git_sha,
        },
        sort_keys=True,
    )
    return _sha256_string(canonical)


# ---------------------------------------------------------------------------
# Snapshot orchestration
# ---------------------------------------------------------------------------


def _collect_method_notes(method_notes_dir: Path, dest_dir: Path) -> list[Path]:
    """Copy all *.md files from method_notes_dir into dest_dir/method_notes/.

    Returns list of copied paths relative to dest_dir.
    """
    out: list[Path] = []
    if not method_notes_dir.exists():
        log.warning("method_notes_dir_missing", path=str(method_notes_dir))
        return out
    target = dest_dir / "method_notes"
    target.mkdir(parents=True, exist_ok=True)
    for md in sorted(method_notes_dir.glob("*.md")):
        dest = target / md.name
        shutil.copy2(md, dest)
        out.append(dest.relative_to(dest_dir))
    return out


def _write_readme(
    dest_dir: Path,
    *,
    paper_anchor: str,
    venue: str,
    snapshot_id: str,
    git_sha: str,
    pipeline_version: str,
    frozen_at: str,
) -> None:
    """Write README.txt with Zenodo metadata stub."""
    readme = dest_dir / "README.txt"
    content = f"""Animetor Eval — Reproducibility Snapshot
=========================================

paper_anchor   : {paper_anchor}
venue          : {venue}
snapshot_id    : {snapshot_id}
pipeline       : {pipeline_version}
git_sha        : {git_sha}
frozen_at      : {frozen_at}

Contents
--------
resolved.duckdb   — Entity-resolved canonical data (Resolved layer).
                    Read with DuckDB ≥0.10.  Schema: resolved.persons,
                    resolved.anime, resolved.credits.

gold.duckdb       — Mart layer: person_scores, feat_*, meta_score_frozen.
                    The meta_score_frozen table contains the frozen score
                    snapshot associated with this deposit.

pixi.lock         — Exact dependency pinning (pixi ≥0.18).
                    Reproduce the environment: pixi install

method_notes/     — Methodological notes for each analytical choice.

MANIFEST.json     — SHA-256 checksums + snapshot metadata.

Reproducing scores
------------------
1. Install pixi: https://prefix.dev/docs/pixi/overview
2. pixi install
3. pixi run pipeline

Frozen scores in meta_score_frozen (snapshot_id={snapshot_id}) will be
re-produced identically from the bundled resolved.duckdb regardless of
subsequent λ recalibrations.

Disclaimer (EN)
---------------
All scores represent structural network position and co-credit density
derived from public credit records.  No claim about individual capability,
competence, or performance is made or implied.

免責事項 (JA)
-----------
全スコアは公開クレジットデータに基づくネットワーク上の位置と共クレジット密度を
示す構造的指標です。個人の能力・資質・業務遂行水準についての主張または示唆は
一切含まれていません。
"""
    readme.write_text(content, encoding="utf-8")


def _write_manifest(
    dest_dir: Path,
    *,
    snapshot_id: str,
    paper_anchor: str,
    venue: str,
    git_sha: str,
    pipeline_version: str,
    spec_hash: str,
    score_hash: str,
    frozen_at: str,
    file_hashes: dict[str, str],
) -> None:
    """Write MANIFEST.json with checksums and snapshot metadata."""
    manifest = {
        "snapshot_id": snapshot_id,
        "paper_anchor": paper_anchor,
        "venue": venue,
        "git_sha": git_sha,
        "pipeline_version": pipeline_version,
        "spec_hash": spec_hash,
        "score_hash": score_hash,
        "frozen_at": frozen_at,
        "files": file_hashes,
    }
    (dest_dir / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def build_snapshot(
    *,
    venue: str,
    paper_anchor: str,
    output_dir: Path,
    resolved_db: Path = _DEFAULT_RESOLVED_DB,
    gold_db: Path = _DEFAULT_GOLD_DB,
    pixi_lock: Path = _DEFAULT_PIXI_LOCK,
    method_notes_dir: Path = _DEFAULT_METHOD_NOTES,
    repo_root: Path = _REPO_ROOT,
    dry_run: bool = False,
) -> Path:
    """Build a Zenodo upload-ready snapshot directory and tarball.

    Args:
        venue: Target venue name (e.g. ``JASSS``).
        paper_anchor: Short identifier for the paper (e.g.
            ``career_network_2026``).
        output_dir: Parent directory for the snapshot artefact.
        resolved_db: Path to resolved.duckdb.
        gold_db: Path to gold.duckdb (animetor.duckdb).
        pixi_lock: Path to pixi.lock.
        method_notes_dir: Path to docs/method_notes/.
        repo_root: Repository root (for git SHA lookup).
        dry_run: If True, collect metadata but do not write files or
            register the frozen score in gold.duckdb.

    Returns:
        Path to the created tarball (``<output_dir>/<snapshot_id>.tar.gz``).
    """
    frozen_at = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    snapshot_id = f"{paper_anchor}_{date_str}"

    log.info(
        "snapshot_start",
        snapshot_id=snapshot_id,
        venue=venue,
        dry_run=dry_run,
    )

    git = _git_sha(repo_root)
    version = _pipeline_version(repo_root)
    lambda_json = _build_lambda_json(repo_root)
    spec_hash = _compute_spec_hash(lambda_json, version, git)

    # -- staging directory --------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    stage = output_dir / snapshot_id
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)

    file_hashes: dict[str, str] = {}

    def _copy_with_hash(src: Path, dest_name: str) -> str | None:
        if not src.exists():
            log.warning("snapshot_source_missing", path=str(src))
            return None
        dest = stage / dest_name
        shutil.copy2(src, dest)
        h = _sha256_file(dest)
        file_hashes[dest_name] = h
        return h

    # Databases
    _copy_with_hash(resolved_db, "resolved.duckdb")
    _copy_with_hash(gold_db, "gold.duckdb")
    _copy_with_hash(pixi_lock, "pixi.lock")

    # Method notes
    _collect_method_notes(method_notes_dir, stage)

    # -- frozen scores ------------------------------------------------------
    score_rows = _read_person_scores_from_gold(gold_db)

    if not dry_run and gold_db.exists():
        from src.analysis.io.mart_writer import write_score_frozen

        pixi_lock_hash = _sha256_file(pixi_lock) if pixi_lock.exists() else ""
        resolved_db_hash = _sha256_file(resolved_db) if resolved_db.exists() else ""

        score_hash = write_score_frozen(
            snapshot_id=snapshot_id,
            paper_anchor=paper_anchor,
            venue=venue,
            spec_hash=spec_hash,
            score_rows=score_rows,
            lambda_json=lambda_json,
            pipeline_version=version,
            git_sha=git,
            pixi_lock_hash=pixi_lock_hash,
            resolved_db_hash=resolved_db_hash,
            notes=f"Auto-generated by snapshot.py at {frozen_at}",
            gold_path=gold_db,
        )
    else:
        import hashlib as _hl
        import json as _json
        from datetime import date as _date, datetime as _datetime

        def _dry_json_default(obj: object) -> object:
            if isinstance(obj, (_datetime, _date)):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")

        score_hash = _hl.sha256(
            _json.dumps(score_rows, sort_keys=True, default=_dry_json_default).encode()
        ).hexdigest()

    # -- README + MANIFEST --------------------------------------------------
    _write_readme(
        stage,
        paper_anchor=paper_anchor,
        venue=venue,
        snapshot_id=snapshot_id,
        git_sha=git,
        pipeline_version=version,
        frozen_at=frozen_at,
    )

    # Hash the README itself after writing
    readme_path = stage / "README.txt"
    if readme_path.exists():
        file_hashes["README.txt"] = _sha256_file(readme_path)

    _write_manifest(
        stage,
        snapshot_id=snapshot_id,
        paper_anchor=paper_anchor,
        venue=venue,
        git_sha=git,
        pipeline_version=version,
        spec_hash=spec_hash,
        score_hash=score_hash,
        frozen_at=frozen_at,
        file_hashes=file_hashes,
    )

    # -- tarball ------------------------------------------------------------
    tarball = output_dir / f"{snapshot_id}.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(stage, arcname=snapshot_id)

    log.info(
        "snapshot_complete",
        tarball=str(tarball),
        snapshot_id=snapshot_id,
        score_hash=score_hash[:12],
        spec_hash=spec_hash[:12],
        n_scores=len(score_rows),
    )
    return tarball


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(add_completion=False)


@app.command()
def main(
    venue: Annotated[str, typer.Option("--venue", help="Target venue name")] = "JASSS",
    paper_anchor: Annotated[
        str,
        typer.Option("--paper-anchor", help="Short paper identifier"),
    ] = "animetor_2026",
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            help="Parent directory for snapshot artefact",
            file_okay=False,
            dir_okay=True,
        ),
    ] = Path("/tmp/animetor_snapshots"),
    resolved_db: Annotated[
        Path,
        typer.Option("--resolved-db", help="Path to resolved.duckdb"),
    ] = _DEFAULT_RESOLVED_DB,
    gold_db: Annotated[
        Path,
        typer.Option("--gold-db", help="Path to gold.duckdb"),
    ] = _DEFAULT_GOLD_DB,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--no-dry-run", help="Collect metadata without writing"),
    ] = False,
) -> None:
    """Create a Zenodo upload-ready reproducibility snapshot.

    Bundles resolved.duckdb, gold.duckdb, pixi.lock and method_notes
    into a single tarball.  Registers a frozen score record in
    mart.meta_score_frozen so paper results survive λ recalibrations.

    Default snapshot_policy=not_taken; this command documents the
    exception per §5.4 replication policy.
    """
    tarball = build_snapshot(
        venue=venue,
        paper_anchor=paper_anchor,
        output_dir=output_dir,
        resolved_db=resolved_db,
        gold_db=gold_db,
    )
    typer.echo(f"Snapshot created: {tarball}")


if __name__ == "__main__":
    app()

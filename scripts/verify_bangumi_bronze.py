"""bangumi BRONZE parquet 検証スクリプト.

BRONZE パーティション (result/bronze/source=bangumi/) を監査し、
整合性チェック結果を Rich テーブルで表示 + オプションで JSON 出力。

Usage:
    pixi run python scripts/verify_bangumi_bronze.py
    pixi run python scripts/verify_bangumi_bronze.py --bronze-root result/bronze/ --json-out /tmp/report.json

Exit code: 0 = all OK/WARN/INFO, 1 = any ERROR.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import duckdb
import typer
from rich.console import Console
from rich.table import Table

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLES = [
    "subjects",
    "subject_persons",
    "subject_characters",
    "person_characters",
    "persons",
    "characters",
]

SENTINEL_LAST_MODIFIED = "0001-01-01T00:00:00Z"
SENTINEL_WARN_THRESHOLD = 0.50  # > 50% sentinel → WARN
POSITION_CARDINALITY_WARN = 200
SUBJECTS_REQUIRED_COLUMNS = {"score", "rank", "score_details"}

SEV_OK = "OK"
SEV_INFO = "INFO"
SEV_WARN = "WARN"
SEV_ERROR = "ERROR"

SEV_COLOR = {
    SEV_OK: "green",
    SEV_INFO: "cyan",
    SEV_WARN: "yellow",
    SEV_ERROR: "red",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

console = Console()


def _parquet_glob(bronze_root: Path, table: str) -> str:
    """Return DuckDB read_parquet glob for a given table."""
    return str(bronze_root / "source=bangumi" / f"table={table}" / "**" / "*.parquet")


def _table_exists(bronze_root: Path, table: str) -> bool:
    table_dir = bronze_root / "source=bangumi" / f"table={table}"
    return table_dir.is_dir()


def _read_count(con: duckdb.DuckDBPyConnection, glob: str) -> int:
    return con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]  # type: ignore[index]


def _make_row(
    check: str,
    severity: str,
    detail: str,
) -> dict[str, str]:
    return {"check": check, "severity": severity, "detail": detail}


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_row_counts(con: duckdb.DuckDBPyConnection, bronze_root: Path) -> list[dict[str, str]]:
    """Check 1: row count sanity for all six tables."""
    rows = []
    for table in TABLES:
        if not _table_exists(bronze_root, table):
            rows.append(_make_row(
                f"row_count/{table}",
                SEV_WARN,
                "table directory absent — table not yet populated",
            ))
            continue
        glob = _parquet_glob(bronze_root, table)
        try:
            count = _read_count(con, glob)
        except Exception as exc:
            rows.append(_make_row(
                f"row_count/{table}",
                SEV_ERROR,
                f"read_parquet failed: {exc}",
            ))
            continue

        if count == 0:
            sev = SEV_WARN if table == "subjects" else SEV_ERROR
            rows.append(_make_row(f"row_count/{table}", sev, "0 rows"))
        else:
            rows.append(_make_row(f"row_count/{table}", SEV_OK, f"{count:,} rows"))
    return rows


def check_subject_filter_integrity(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 2: every subject_id in relation tables must exist in subjects (type=2)."""
    if not all(_table_exists(bronze_root, t) for t in ["subjects", "subject_persons", "subject_characters", "person_characters"]):
        return [_make_row("subject_filter_integrity", SEV_WARN, "skipped — one or more tables absent")]

    subj_glob = _parquet_glob(bronze_root, "subjects")
    sp_glob = _parquet_glob(bronze_root, "subject_persons")
    sc_glob = _parquet_glob(bronze_root, "subject_characters")
    pc_glob = _parquet_glob(bronze_root, "person_characters")

    try:
        violations = con.execute(f"""
            WITH valid_subjects AS (
                SELECT id FROM read_parquet('{subj_glob}')
            ),
            all_refs AS (
                SELECT DISTINCT subject_id AS sid FROM read_parquet('{sp_glob}')
                UNION
                SELECT DISTINCT subject_id FROM read_parquet('{sc_glob}')
                UNION
                SELECT DISTINCT subject_id FROM read_parquet('{pc_glob}')
            )
            SELECT count(*) FROM all_refs
            WHERE sid NOT IN (SELECT id FROM valid_subjects)
        """).fetchone()[0]  # type: ignore[index]
    except Exception as exc:
        return [_make_row("subject_filter_integrity", SEV_ERROR, f"query failed: {exc}")]

    if violations > 0:
        return [_make_row("subject_filter_integrity", SEV_ERROR, f"{violations:,} orphan subject_ids")]
    return [_make_row("subject_filter_integrity", SEV_OK, "all relation subject_ids present in subjects")]


def check_person_reference_integrity(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 3: person_id in subject_persons/person_characters should appear in persons."""
    for t in ["subject_persons", "persons"]:
        if not _table_exists(bronze_root, t):
            return [_make_row("person_ref_integrity", SEV_WARN, f"skipped — {t} absent")]

    sp_glob = _parquet_glob(bronze_root, "subject_persons")
    pc_glob = _parquet_glob(bronze_root, "person_characters")
    p_glob = _parquet_glob(bronze_root, "persons")

    try:
        persons_count = _read_count(con, p_glob)
        if persons_count == 0:
            return [_make_row("person_ref_integrity", SEV_WARN, "persons table is empty — backfill pending")]

        referenced = con.execute(f"""
            SELECT count(DISTINCT pid) FROM (
                SELECT DISTINCT person_id AS pid FROM read_parquet('{sp_glob}')
                UNION
                SELECT DISTINCT person_id FROM read_parquet('{pc_glob}')
            ) t
        """).fetchone()[0]  # type: ignore[index]

        missing = con.execute(f"""
            WITH ref AS (
                SELECT DISTINCT person_id AS pid FROM read_parquet('{sp_glob}')
                UNION
                SELECT DISTINCT person_id FROM read_parquet('{pc_glob}')
            ),
            known AS (
                SELECT DISTINCT id FROM read_parquet('{p_glob}')
            )
            SELECT count(*) FROM ref WHERE pid NOT IN (SELECT id FROM known)
        """).fetchone()[0]  # type: ignore[index]

    except Exception as exc:
        return [_make_row("person_ref_integrity", SEV_ERROR, f"query failed: {exc}")]

    if missing == 0:
        return [_make_row("person_ref_integrity", SEV_OK, f"all {referenced:,} person_ids resolved")]

    # Determine whether persons table is "fully populated" heuristically:
    # if persons.count < 50% of referenced person_ids, treat as partial.
    coverage = persons_count / max(referenced, 1)
    if coverage < 0.50:
        return [_make_row(
            "person_ref_integrity",
            SEV_WARN,
            f"{missing:,} missing person_ids (persons coverage {coverage:.0%} — backfill in progress)",
        )]
    return [_make_row(
        "person_ref_integrity",
        SEV_ERROR,
        f"{missing:,} missing person_ids despite persons table being {coverage:.0%} populated",
    )]


def check_character_reference_integrity(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 4: character_id in relation tables should appear in characters."""
    for t in ["subject_characters", "characters"]:
        if not _table_exists(bronze_root, t):
            return [_make_row("character_ref_integrity", SEV_WARN, f"skipped — {t} absent")]

    sc_glob = _parquet_glob(bronze_root, "subject_characters")
    pc_glob = _parquet_glob(bronze_root, "person_characters")
    c_glob = _parquet_glob(bronze_root, "characters")

    try:
        chars_count = _read_count(con, c_glob)
        if chars_count == 0:
            return [_make_row("character_ref_integrity", SEV_WARN, "characters table is empty — backfill pending")]

        referenced = con.execute(f"""
            SELECT count(DISTINCT cid) FROM (
                SELECT DISTINCT character_id AS cid FROM read_parquet('{sc_glob}')
                UNION
                SELECT DISTINCT character_id FROM read_parquet('{pc_glob}')
            ) t
        """).fetchone()[0]  # type: ignore[index]

        missing = con.execute(f"""
            WITH ref AS (
                SELECT DISTINCT character_id AS cid FROM read_parquet('{sc_glob}')
                UNION
                SELECT DISTINCT character_id FROM read_parquet('{pc_glob}')
            ),
            known AS (
                SELECT DISTINCT id FROM read_parquet('{c_glob}')
            )
            SELECT count(*) FROM ref WHERE cid NOT IN (SELECT id FROM known)
        """).fetchone()[0]  # type: ignore[index]

    except Exception as exc:
        return [_make_row("character_ref_integrity", SEV_ERROR, f"query failed: {exc}")]

    if missing == 0:
        return [_make_row("character_ref_integrity", SEV_OK, f"all {referenced:,} character_ids resolved")]

    coverage = chars_count / max(referenced, 1)
    if coverage < 0.50:
        return [_make_row(
            "character_ref_integrity",
            SEV_WARN,
            f"{missing:,} missing character_ids (characters coverage {coverage:.0%} — backfill in progress)",
        )]
    return [_make_row(
        "character_ref_integrity",
        SEV_ERROR,
        f"{missing:,} missing character_ids despite characters table being {coverage:.0%} populated",
    )]


def check_person_character_actor_consistency(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 5: every (person_id, character_id, subject_id) in person_characters
    should be derivable from subject_characters."""
    for t in ["person_characters", "subject_characters"]:
        if not _table_exists(bronze_root, t):
            return [_make_row("actor_consistency", SEV_WARN, f"skipped — {t} absent")]

    pc_glob = _parquet_glob(bronze_root, "person_characters")
    sc_glob = _parquet_glob(bronze_root, "subject_characters")

    try:
        violations = con.execute(f"""
            SELECT count(*) FROM read_parquet('{pc_glob}') pc
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{sc_glob}') sc
                WHERE sc.subject_id = pc.subject_id
                  AND sc.character_id = pc.character_id
            )
        """).fetchone()[0]  # type: ignore[index]
    except Exception as exc:
        return [_make_row("actor_consistency", SEV_ERROR, f"query failed: {exc}")]

    if violations > 0:
        return [_make_row("actor_consistency", SEV_ERROR, f"{violations:,} person_characters rows with no matching subject_characters origin")]
    return [_make_row("actor_consistency", SEV_OK, "all person_characters derivable from subject_characters")]


def check_sentinel_timestamps(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 6: scan persons.last_modified for sentinel '0001-01-01T00:00:00Z'."""
    if not _table_exists(bronze_root, "persons"):
        return [_make_row("sentinel_timestamps", SEV_WARN, "skipped — persons absent")]

    p_glob = _parquet_glob(bronze_root, "persons")
    try:
        total = _read_count(con, p_glob)
        if total == 0:
            return [_make_row("sentinel_timestamps", SEV_WARN, "persons empty — nothing to check")]

        sentinel_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{p_glob}') WHERE last_modified = '{SENTINEL_LAST_MODIFIED}'"
        ).fetchone()[0]  # type: ignore[index]
    except Exception as exc:
        return [_make_row("sentinel_timestamps", SEV_ERROR, f"query failed: {exc}")]

    pct = sentinel_count / total
    detail = f"{sentinel_count:,}/{total:,} rows have sentinel ({pct:.1%})"
    if pct > SENTINEL_WARN_THRESHOLD:
        return [_make_row("sentinel_timestamps", SEV_WARN, detail + " — systemic issue suspected")]
    return [_make_row("sentinel_timestamps", SEV_INFO, detail)]


def check_position_cardinality(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 7: count distinct position labels in subject_persons."""
    if not _table_exists(bronze_root, "subject_persons"):
        return [_make_row("position_cardinality", SEV_WARN, "skipped — subject_persons absent")]

    sp_glob = _parquet_glob(bronze_root, "subject_persons")
    try:
        cardinality = con.execute(
            f"SELECT count(DISTINCT position) FROM read_parquet('{sp_glob}')"
        ).fetchone()[0]  # type: ignore[index]
    except Exception as exc:
        return [_make_row("position_cardinality", SEV_ERROR, f"query failed: {exc}")]

    detail = f"{cardinality} distinct position labels"
    if cardinality > POSITION_CARDINALITY_WARN:
        return [_make_row("position_cardinality", SEV_WARN, detail + " (> 200 — possible parser drift)")]
    return [_make_row("position_cardinality", SEV_OK, detail)]


def check_h1_columns(
    con: duckdb.DuckDBPyConnection, bronze_root: Path
) -> list[dict[str, str]]:
    """Check 8 (Hard Rule H1): score/rank/score_details columns present in subjects."""
    if not _table_exists(bronze_root, "subjects"):
        return [_make_row("H1_columns", SEV_WARN, "skipped — subjects absent")]

    subj_glob = _parquet_glob(bronze_root, "subjects")
    try:
        col_names = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{subj_glob}') LIMIT 0"
            ).fetchall()
        }
    except Exception as exc:
        return [_make_row("H1_columns", SEV_ERROR, f"DESCRIBE failed: {exc}")]

    missing = SUBJECTS_REQUIRED_COLUMNS - col_names
    if missing:
        return [_make_row(
            "H1_columns",
            SEV_ERROR,
            f"Missing columns stripped from subjects: {sorted(missing)} — H1 violation",
        )]
    return [_make_row("H1_columns", SEV_OK, "score, rank, score_details all present in subjects")]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_all_checks(bronze_root: Path) -> list[dict[str, str]]:
    con = duckdb.connect()  # in-memory, no network
    results: list[dict[str, str]] = []
    results.extend(check_row_counts(con, bronze_root))
    results.extend(check_subject_filter_integrity(con, bronze_root))
    results.extend(check_person_reference_integrity(con, bronze_root))
    results.extend(check_character_reference_integrity(con, bronze_root))
    results.extend(check_person_character_actor_consistency(con, bronze_root))
    results.extend(check_sentinel_timestamps(con, bronze_root))
    results.extend(check_position_cardinality(con, bronze_root))
    results.extend(check_h1_columns(con, bronze_root))
    con.close()
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(add_completion=False)


@app.command()
def main(
    bronze_root: Path = typer.Option(
        Path("result/bronze/"),
        "--bronze-root",
        help="Root directory for BRONZE parquet partitions",
        exists=False,  # allow non-existent for graceful error
    ),
    json_out: Optional[Path] = typer.Option(
        None,
        "--json-out",
        help="Write JSON report to this path (omit to print to stdout only)",
    ),
) -> None:
    """Audit bangumi BRONZE parquet partitions. No network calls made."""
    if not bronze_root.is_dir():
        console.print(f"[red]ERROR[/red]: bronze_root not found: {bronze_root}")
        raise typer.Exit(code=1)

    bangumi_root = bronze_root / "source=bangumi"
    if not bangumi_root.is_dir():
        console.print(f"[yellow]WARN[/yellow]: no bangumi data found at {bangumi_root}")
        console.print("Run the bangumi scraper pipeline first.")
        raise typer.Exit(code=0)

    console.print(f"\n[bold]bangumi BRONZE audit[/bold] — {bangumi_root}\n")

    results = run_all_checks(bronze_root)

    # Rich table
    table = Table(show_header=True, header_style="bold")
    table.add_column("Check", style="dim", no_wrap=True)
    table.add_column("Severity", no_wrap=True)
    table.add_column("Detail")

    for row in results:
        sev = row["severity"]
        color = SEV_COLOR.get(sev, "white")
        table.add_row(
            row["check"],
            f"[{color}]{sev}[/{color}]",
            row["detail"],
        )

    console.print(table)

    # Summary line
    error_count = sum(1 for r in results if r["severity"] == SEV_ERROR)
    warn_count = sum(1 for r in results if r["severity"] == SEV_WARN)
    ok_count = sum(1 for r in results if r["severity"] == SEV_OK)

    if error_count:
        console.print(f"\n[red]FAIL[/red]: {error_count} error(s), {warn_count} warning(s), {ok_count} ok")
    elif warn_count:
        console.print(f"\n[yellow]PASS with warnings[/yellow]: {warn_count} warning(s), {ok_count} ok")
    else:
        console.print(f"\n[green]PASS[/green]: {ok_count} checks OK")

    # JSON output
    report = {
        "bronze_root": str(bronze_root),
        "checks": results,
        "summary": {
            "errors": error_count,
            "warnings": warn_count,
            "ok": ok_count,
        },
    }

    if json_out is not None:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        console.print(f"JSON report written to {json_out}")
    else:
        # Print to stdout (pipe-friendly)
        print(json.dumps(report, ensure_ascii=False, indent=2))

    raise typer.Exit(code=1 if error_count else 0)


if __name__ == "__main__":
    app()

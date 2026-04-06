"""CLI for the declarative report architecture.

Commands::

    python -m src.reporting.cli validate <slug>
    python -m src.reporting.cli validate-all
    python -m src.reporting.cli generate <slug>
    python -m src.reporting.cli generate-all
    python -m src.reporting.cli list

Usage via pixi::

    pixi run reports-new generate compensation_fairness
    pixi run reports-new validate-all
    pixi run reports-new list
"""

from __future__ import annotations

import importlib
from pathlib import Path

import structlog
import typer

from src.reporting.registry import all_slugs, get_entry

logger = structlog.get_logger()

app = typer.Typer(
    name="reporting",
    help="Declarative report generation and validation.",
    no_args_is_help=True,
)

# Default directories
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_JSON_DIR = _REPO_ROOT / "result" / "json"
_DEFAULT_REPORTS_DIR = _REPO_ROOT / "result" / "reports"


def _discover_definitions() -> None:
    """Import all ``src.reporting.definitions.*`` modules to trigger registration."""
    defs_dir = Path(__file__).resolve().parent / "definitions"
    if not defs_dir.is_dir():
        return
    for p in sorted(defs_dir.glob("*.py")):
        if p.name.startswith("_"):
            continue
        module_name = f"src.reporting.definitions.{p.stem}"
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            logger.warning("definition_import_failed", module=module_name, error=str(exc))


@app.command()
def list_reports() -> None:
    """List all registered report slugs."""
    _discover_definitions()
    slugs = all_slugs()
    if not slugs:
        typer.echo("No reports registered yet.")
        raise typer.Exit(0)
    for slug in slugs:
        typer.echo(slug)


@app.command()
def validate(
    slug: str = typer.Argument(help="Report slug to validate"),
) -> None:
    """Validate a single report's spec."""
    _discover_definitions()
    entry = get_entry(slug)
    spec = entry.build_spec()

    from src.reporting.specs.validation import errors_only, validate as run_validate, warnings_only

    results = run_validate(spec)
    errors = errors_only(results)
    warnings = warnings_only(results)

    for w in warnings:
        typer.echo(f"  WARN [{w.rule}] {w.message}")
    for e in errors:
        typer.echo(f"  ERROR [{e.rule}] {e.message}")

    if errors:
        typer.echo(f"\n{len(errors)} error(s), {len(warnings)} warning(s).")
        raise typer.Exit(1)
    typer.echo(f"OK — 0 errors, {len(warnings)} warning(s).")


@app.command()
def validate_all() -> None:
    """Validate all registered reports."""
    _discover_definitions()
    slugs = all_slugs()
    if not slugs:
        typer.echo("No reports registered.")
        raise typer.Exit(0)

    from src.reporting.specs.validation import errors_only, validate as run_validate

    total_errors = 0
    for slug in slugs:
        entry = get_entry(slug)
        spec = entry.build_spec()
        results = run_validate(spec)
        errors = errors_only(results)
        status = "FAIL" if errors else "OK"
        typer.echo(f"  {status}  {slug}" + (f"  ({len(errors)} errors)" if errors else ""))
        total_errors += len(errors)

    if total_errors:
        raise typer.Exit(1)


@app.command()
def generate(
    slug: str = typer.Argument(help="Report slug to generate"),
    json_dir: Path = typer.Option(_DEFAULT_JSON_DIR, help="Input JSON directory"),
    output_dir: Path = typer.Option(_DEFAULT_REPORTS_DIR, help="Output directory"),
) -> None:
    """Generate a single report as HTML."""
    from src.reporting.assemblers.html import assemble
    from src.reporting.renderers.html_primitives import configure_legacy_dirs

    _discover_definitions()
    configure_legacy_dirs(json_dir, output_dir)

    entry = get_entry(slug)
    spec = entry.build_spec()
    data = entry.provide(json_dir)
    html = assemble(spec, data)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{slug}.html"
    out_path.write_text(html, encoding="utf-8")
    typer.echo(f"Generated: {out_path}")


@app.command()
def generate_all(
    json_dir: Path = typer.Option(_DEFAULT_JSON_DIR, help="Input JSON directory"),
    output_dir: Path = typer.Option(_DEFAULT_REPORTS_DIR, help="Output directory"),
) -> None:
    """Generate all registered reports."""
    from src.reporting.assemblers.html import assemble
    from src.reporting.renderers.html_primitives import configure_legacy_dirs

    _discover_definitions()
    configure_legacy_dirs(json_dir, output_dir)

    slugs = all_slugs()
    if not slugs:
        typer.echo("No reports registered.")
        raise typer.Exit(0)

    output_dir.mkdir(parents=True, exist_ok=True)
    ok_count = 0
    fail_count = 0

    for slug in slugs:
        try:
            entry = get_entry(slug)
            spec = entry.build_spec()
            data = entry.provide(json_dir)
            html = assemble(spec, data)
            out_path = output_dir / f"{slug}.html"
            out_path.write_text(html, encoding="utf-8")
            typer.echo(f"  OK  {slug}")
            ok_count += 1
        except Exception as exc:
            typer.echo(f"  FAIL  {slug}: {exc}")
            fail_count += 1

    typer.echo(f"\n{ok_count} generated, {fail_count} failed.")
    if fail_count:
        raise typer.Exit(1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()

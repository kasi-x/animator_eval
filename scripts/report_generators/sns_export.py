"""SNS post 自動生成 orchestrator.

各 report の to_sns_post() / to_note_post() を呼び出し、
X (280 char) と note (1500-3000 char) 用の short-form を生成して
output dir に MD / JSON 形式で書き出す。

Usage (CLI)
-----------
    pixi run python scripts/report_generators/sns_export.py --report o3_ip_dependency
    pixi run python scripts/report_generators/sns_export.py --all

Output
------
    <output_dir>/x/<report_name>.md          — X post (Markdown)
    <output_dir>/note/<report_name>.md        — note article (Markdown)
    <output_dir>/x/<report_name>.json         — X post (JSON)
    <output_dir>/note/<report_name>.json      — note article (JSON)
    <output_dir>/sns_export_index.json        — index of all generated posts

Compliance
----------
    * Text char limits enforced by Pydantic v2 validators in SnsPost / NotePost.
    * forbidden_vocab lint is run on generated text at export time.
    * Structural facts only; no ability framing; no ranking / hiring framing.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

# Allow both `python scripts/report_generators/sns_export.py` (direct script)
# and `from scripts.report_generators import sns_export` (package import).
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import structlog  # noqa: E402

from scripts.report_generators.lint_vocab import (  # noqa: E402
    CONTEXTUAL_BIGRAMS,
    _compile_patterns,
    _scan_text,
    load_replacements,
    load_vocab,
)
from scripts.report_generators.reports._base import NotePost, SnsPost  # noqa: E402

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Registry: report_name → (module_path, class_name)
# ---------------------------------------------------------------------------

#: Reports that have implemented to_sns_post() / to_note_post().
_SNS_CAPABLE_REPORTS: dict[str, tuple[str, str]] = {
    "o2_mid_management": (
        "scripts.report_generators.reports.o2_mid_management",
        "O2MidManagementReport",
    ),
    "o3_ip_dependency": (
        "scripts.report_generators.reports.o3_ip_dependency",
        "O3IpDependencyReport",
    ),
    "studio_pipeline": (
        "scripts.report_generators.reports.studio_pipeline",
        "StudioPipelineReport",
    ),
}

_DEFAULT_OUTPUT_DIR = Path(__file__).parents[2] / "result" / "sns"


# ---------------------------------------------------------------------------
# Vocab lint (inline — no subprocess)
# ---------------------------------------------------------------------------


def _lint_text(text: str, label: str) -> list[str]:
    """Run forbidden_vocab lint on text.

    Returns list of violation messages (empty = clean).
    """
    terms = load_vocab()
    replacements = load_replacements()
    patterns = _compile_patterns(terms)
    dummy_path = Path(f"<{label}>")

    violations: list[str] = []
    findings = _scan_text(text, dummy_path, patterns, replacements)
    for f in findings:
        violations.append(
            f"[{f.term.category}] {f.term.term!r} at col {f.col}: {f.context.strip()[:80]}"
        )

    # Contextual bigrams
    from scripts.report_generators.lint_vocab import _scan_bigrams
    bigram_findings = _scan_bigrams(text, dummy_path, CONTEXTUAL_BIGRAMS)
    for bf in bigram_findings:
        violations.append(
            f"[bigram] {bf.term_a!r}+{bf.term_b!r} at col {bf.col}: {bf.context.strip()[:80]}"
        )
    return violations


# ---------------------------------------------------------------------------
# Report instantiation
# ---------------------------------------------------------------------------


def _instantiate_report(
    report_name: str,
    conn: sqlite3.Connection,
    output_dir: Path,
) -> Any:
    """Import and instantiate a report class by registry name.

    Args:
        report_name: key in _SNS_CAPABLE_REPORTS.
        conn: DB connection (may be :memory: for SNS-only run).
        output_dir: output dir for report HTML (not used here but required by base).

    Returns:
        Instantiated report object.

    Raises:
        KeyError: report_name not in registry.
        ImportError: module or class not found.
    """
    if report_name not in _SNS_CAPABLE_REPORTS:
        raise KeyError(
            f"Report {report_name!r} not in SNS-capable registry. "
            f"Available: {sorted(_SNS_CAPABLE_REPORTS)}"
        )
    module_path, class_name = _SNS_CAPABLE_REPORTS[report_name]
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(conn, output_dir=output_dir)


# ---------------------------------------------------------------------------
# Single-report export
# ---------------------------------------------------------------------------


def export_report_sns(
    report_name: str,
    *,
    conn: sqlite3.Connection | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Generate X + note posts for one report and write to output_dir.

    Args:
        report_name: key in _SNS_CAPABLE_REPORTS.
        conn: DB connection. If None, uses in-memory SQLite (SNS only, no real data).
        output_dir: where to write .md and .json files.

    Returns:
        dict with keys "report", "x_post", "note_post", "lint_violations",
        "x_path", "note_path" — suitable for inclusion in the index.
    """
    out_dir = output_dir or _DEFAULT_OUTPUT_DIR
    _conn = conn if conn is not None else sqlite3.connect(":memory:")

    report_obj = _instantiate_report(report_name, _conn, out_dir)

    # Generate posts
    sns_post: SnsPost = report_obj.to_sns_post()
    note_post: NotePost = report_obj.to_note_post()

    # Lint both texts
    x_violations = _lint_text(sns_post.text, f"{report_name}/x")
    note_violations = _lint_text(note_post.body, f"{report_name}/note")
    all_violations = x_violations + note_violations

    if all_violations:
        log.warning(
            "sns_export_lint_violations",
            report=report_name,
            violations=all_violations,
        )

    # Write output
    x_dir = out_dir / "x"
    note_dir = out_dir / "note"
    x_dir.mkdir(parents=True, exist_ok=True)
    note_dir.mkdir(parents=True, exist_ok=True)

    # X post — Markdown
    x_md_path = x_dir / f"{report_name}.md"
    x_md_content = _format_x_markdown(report_name, sns_post)
    x_md_path.write_text(x_md_content, encoding="utf-8")

    # X post — JSON
    x_json_path = x_dir / f"{report_name}.json"
    x_json_path.write_text(
        json.dumps(sns_post.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # note post — Markdown
    note_md_path = note_dir / f"{report_name}.md"
    note_md_content = _format_note_markdown(report_name, note_post)
    note_md_path.write_text(note_md_content, encoding="utf-8")

    # note post — JSON
    note_json_path = note_dir / f"{report_name}.json"
    note_json_path.write_text(
        json.dumps(note_post.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log.info(
        "sns_export_complete",
        report=report_name,
        x_chars=len(sns_post.text),
        note_chars=len(note_post.body),
        lint_violations=len(all_violations),
        x_path=str(x_md_path),
        note_path=str(note_md_path),
    )

    return {
        "report": report_name,
        "x_post": sns_post.model_dump(),
        "note_post": note_post.model_dump(),
        "lint_violations": all_violations,
        "x_path": str(x_md_path),
        "note_path": str(note_md_path),
        "x_chars": len(sns_post.text),
        "note_chars": len(note_post.body),
    }


# ---------------------------------------------------------------------------
# Markdown formatters
# ---------------------------------------------------------------------------


def _format_x_markdown(report_name: str, post: SnsPost) -> str:
    """Format SnsPost as a Markdown document for archive / review."""
    lines = [
        f"# X Post — {report_name}",
        "",
        f"**Platform**: {post.platform}  ",
        f"**Chars**: {len(post.text)} / 280  ",
        f"**URL**: {post.url}  ",
    ]
    if post.figure_path:
        lines.append(f"**Figure**: {post.figure_path}  ")
    lines += [
        "",
        "---",
        "",
        post.text,
        "",
    ]
    return "\n".join(lines)


def _format_note_markdown(report_name: str, post: NotePost) -> str:
    """Format NotePost as a Markdown document for archive / review."""
    lines = [
        f"# {post.title}",
        "",
        f"<!-- platform: {post.platform} | chars: {len(post.body)} | url: {post.url} -->",
        "",
        post.body,
        "",
    ]
    if post.figure_paths:
        lines += [
            "---",
            "",
            "**Figures**:",
        ]
        for fp in post.figure_paths:
            lines.append(f"- {fp}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Bulk export
# ---------------------------------------------------------------------------


def export_all_sns(
    *,
    conn: sqlite3.Connection | None = None,
    output_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Export SNS posts for all SNS-capable reports.

    Args:
        conn: DB connection shared across all reports.
        output_dir: output directory.

    Returns:
        List of result dicts from export_report_sns().
    """
    results: list[dict[str, Any]] = []
    for report_name in sorted(_SNS_CAPABLE_REPORTS):
        try:
            result = export_report_sns(report_name, conn=conn, output_dir=output_dir)
            results.append(result)
        except Exception as exc:
            log.error("sns_export_failed", report=report_name, error=str(exc))
            results.append({
                "report": report_name,
                "error": str(exc),
                "lint_violations": [],
            })

    # Write index
    out_dir = output_dir or _DEFAULT_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "sns_export_index.json"
    index_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("sns_export_index_written", path=str(index_path), count=len(results))
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate X / note SNS posts from v3 report modules. "
            "Writes .md and .json files to result/sns/."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--report",
        metavar="REPORT_NAME",
        help=(
            "Generate SNS posts for one report. "
            f"Available: {', '.join(sorted(_SNS_CAPABLE_REPORTS))}"
        ),
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Generate SNS posts for all SNS-capable reports.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: result/sns/).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    out_dir: Path | None = args.output_dir

    if args.all:
        results = export_all_sns(output_dir=out_dir)
    else:
        try:
            result = export_report_sns(args.report, output_dir=out_dir)
            results = [result]
        except KeyError as exc:
            print(f"[sns_export] ERROR: {exc}", file=sys.stderr)
            return 1

    total_violations = sum(len(r.get("lint_violations", [])) for r in results)
    errors = [r for r in results if "error" in r]

    print(
        f"[sns_export] reports={len(results)} "
        f"errors={len(errors)} "
        f"lint_violations={total_violations}"
    )

    for result in results:
        if "error" in result:
            print(f"  ERROR {result['report']}: {result['error']}", file=sys.stderr)
            continue
        vio = result.get("lint_violations", [])
        print(
            f"  {result['report']}: "
            f"x={result.get('x_chars', '?')}c "
            f"note={result.get('note_chars', '?')}c "
            f"violations={len(vio)}"
        )
        for v in vio:
            print(f"    VIOLATION: {v}", file=sys.stderr)

    # Print sample posts for first successful result
    for result in results:
        if "error" not in result:
            print("\n--- Sample X post ---")
            print(result["x_post"]["text"])
            print(f"({result['x_chars']} chars)")
            print("\n--- Sample note post (first 300 chars) ---")
            body = result["note_post"]["body"]
            print(body[:300] + ("..." if len(body) > 300 else ""))
            print(f"({result['note_chars']} chars total)")
            break

    return 0 if (total_violations == 0 and not errors) else 1


if __name__ == "__main__":
    sys.exit(main())

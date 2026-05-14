#!/usr/bin/env python
"""Labor-first audit: run forbidden-vocab lint over all v3 report generators.

Wraps ``scripts/report_generators/lint_vocab.py`` internals and produces a
machine-readable JSON summary suitable for downstream triage.

Usage
-----
::

    pixi run python scripts/maintenance/audit_reports_labor_first.py
    pixi run python scripts/maintenance/audit_reports_labor_first.py --output reports/audit_2026_05_15.json
    pixi run python scripts/maintenance/audit_reports_labor_first.py --quiet

Output schema
-------------
::

    {
        "audit_date": "2026-05-15",
        "lint_version": "forbidden_vocab.yaml categories: ...",
        "reports_dir": "scripts/report_generators/reports",
        "files_scanned": N,
        "files_flagged": M,
        "total_violations": V,
        "fixed_in_this_run": 0,          # always 0 — this script is read-only
        "categories": {
            "ability_framing": {"count": ..., "files": [...]},
            "causal_verbs": {...},
            "evaluative_adjectives": {...},
            "ranking_framing": {...},
            "hiring_framing": {...},
        },
        "per_file": [
            {
                "file": "relative/path.py",
                "violations": [
                    {
                        "category": "ability_framing",
                        "severity": "error",
                        "term": "...",
                        "line": N,
                        "col": N,
                        "context": "...",
                        "suggestion": "..."
                    },
                    ...
                ]
            },
            ...
        ]
    }
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: resolve project root and add to sys.path so that
# scripts/report_generators/lint_vocab.py is importable regardless of cwd.
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LINT_DIR = PROJECT_ROOT / "scripts" / "report_generators"
REPORTS_DIR = LINT_DIR / "reports"

if str(LINT_DIR) not in sys.path:
    sys.path.insert(0, str(LINT_DIR))

# Now import the linter internals.
from lint_vocab import (  # noqa: E402
    CONTEXTUAL_BIGRAMS,
    ENFORCED_CATEGORIES,
    _is_definitional,
    _is_excepted,
    iter_target_files,
    lint_file,
    lint_file_bigrams,
    load_exceptions,
    load_replacements,
    load_vocab,
    _compile_patterns,
)


def _collect(reports_dir: Path) -> dict:
    """Run the linter over all files in *reports_dir* and return a summary."""
    terms = load_vocab()
    replacements = load_replacements()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)

    per_file: list[dict] = []
    category_counts: dict[str, int] = {cat: 0 for cat in ENFORCED_CATEGORIES}
    category_files: dict[str, list[str]] = {cat: [] for cat in ENFORCED_CATEGORIES}
    files_scanned = 0
    files_flagged = 0
    total_violations = 0

    for file_path in sorted(iter_target_files([reports_dir])):
        files_scanned += 1

        # Standard single-term scan.
        file_findings = lint_file(file_path, patterns, replacements)
        file_findings = [
            f for f in file_findings
            if not _is_definitional(f) and not _is_excepted(f, exceptions)
        ]

        # Contextual 2-gram scan.
        bigram_findings = lint_file_bigrams(file_path, CONTEXTUAL_BIGRAMS)

        violations: list[dict] = []
        for f in file_findings:
            violations.append(
                {
                    "category": f.term.category,
                    "severity": f.term.severity,
                    "term": f.term.term,
                    "line": f.line,
                    "col": f.col,
                    "context": f.context.strip()[:200],
                    "suggestion": f.replacement or "",
                }
            )
            category_counts[f.term.category] = category_counts.get(f.term.category, 0) + 1
            rel = str(file_path.relative_to(PROJECT_ROOT))
            if rel not in category_files.get(f.term.category, []):
                category_files.setdefault(f.term.category, []).append(rel)

        for bf in bigram_findings:
            violations.append(
                {
                    "category": "ability_framing_bigram",
                    "severity": "error",
                    "term": f"{bf.term_a}+{bf.term_b}",
                    "line": bf.line,
                    "col": bf.col,
                    "context": bf.context.strip()[:200],
                    "suggestion": bf.replacement,
                }
            )
            category_counts["ability_framing"] = category_counts.get("ability_framing", 0) + 1
            rel = str(file_path.relative_to(PROJECT_ROOT))
            if rel not in category_files.get("ability_framing", []):
                category_files.setdefault("ability_framing", []).append(rel)

        if violations:
            files_flagged += 1
            total_violations += len(violations)
            per_file.append(
                {
                    "file": str(file_path.relative_to(PROJECT_ROOT)),
                    "violation_count": len(violations),
                    "violations": violations,
                }
            )

    categories_summary = {
        cat: {
            "count": category_counts.get(cat, 0),
            "files": sorted(set(category_files.get(cat, []))),
        }
        for cat in ENFORCED_CATEGORIES
    }

    return {
        "audit_date": str(date.today()),
        "lint_version": f"forbidden_vocab.yaml enforced_categories={list(ENFORCED_CATEGORIES)}",
        "reports_dir": str(reports_dir.relative_to(PROJECT_ROOT)),
        "files_scanned": files_scanned,
        "files_flagged": files_flagged,
        "total_violations": total_violations,
        "fixed_in_this_run": 0,
        "categories": categories_summary,
        "per_file": per_file,
    }


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Labor-first audit: lint all v3 report generators for forbidden vocabulary.",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write JSON result to this file in addition to stdout.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-file detail; print only the summary.",
    )
    p.add_argument(
        "--reports-dir",
        type=Path,
        default=REPORTS_DIR,
        help="Path to the report generators directory (default: scripts/report_generators/reports/).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    result = _collect(args.reports_dir)

    payload = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
        if not args.quiet:
            print(f"[audit] written to {args.output}")

    if not args.quiet:
        print(payload)
    else:
        # Print summary line only.
        print(
            f"[audit] date={result['audit_date']} "
            f"scanned={result['files_scanned']} "
            f"flagged={result['files_flagged']} "
            f"violations={result['total_violations']}"
        )
        for cat, info in result["categories"].items():
            if info["count"] > 0:
                print(f"  {cat}: {info['count']} violation(s) in {len(info['files'])} file(s)")

    return 0 if result["total_violations"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

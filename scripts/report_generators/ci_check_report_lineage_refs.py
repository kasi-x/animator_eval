#!/usr/bin/env python3
"""CI gate: every findings-bearing report generator must reference meta_lineage.

Static source-code check complementing ``ci_check_lineage.py`` (runtime DB
check) and ``ci_check_method_notes.py`` (post-pipeline row check).

What it enforces
----------------
For each report module under ``scripts/report_generators/reports/`` that
defines a subclass of ``BaseReportGenerator`` with a non-trivial
``generate()`` method (i.e. produces findings), the file body must contain
at least one of the following markers:

* ``insert_lineage(``                — writes a lineage row
* ``render_unified_structure(.. meta_table=..)``
* ``meta_lineage`` / ``ops_lineage`` — reads the lineage table

Rationale
---------
``meta_lineage`` is the single source of truth for formula version, CI
method, null model, and hold-out method (see REPORT_PHILOSOPHY.md § Method
Gate). A report that produces findings without a lineage reference cannot
be audited and violates the v2 unified structure (Method Note is mandatory
for every findings-bearing brief).

Allowlist policy
----------------
Hard-gate rule: all 5 mandatory reports listed in
``ci_check_method_notes.REQUIRED_TABLE_NAMES`` MUST reference lineage; any
regression there fails CI.

Soft-gate rule: reports not yet on the mandatory list are checked and
reported, but only fail CI when ``--strict`` is passed. This lets us land
the gate now without cascading failures on the 30 legacy reports that have
not yet been migrated to the method-gate structure.

Usage
-----
::

    pixi run python scripts/report_generators/ci_check_report_lineage_refs.py
    pixi run python scripts/report_generators/ci_check_report_lineage_refs.py --strict
"""
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# Reports that already emit meta_lineage — regressions here must fail CI.
# Keep in sync with ci_check_method_notes.REQUIRED_TABLE_NAMES (by report name,
# not lineage table_name).
MANDATORY_REPORT_STEMS = frozenset({
    "policy_attrition",
    "policy_monopsony",
    "policy_gender_bottleneck",
    "mgmt_studio_benchmark",
    "biz_genre_whitespace",
})

# Files skipped even from the soft gate (base classes, index/landing pages
# that do not emit findings of their own).
IGNORED_STEMS = frozenset({
    "__init__",
    "_base",
    "index_page",            # landing page, just links out
    "policy_brief_index",    # index of briefs
    "hr_brief_index",
    "biz_brief_index",
})

# Tokens that, if found anywhere in the file, satisfy the lineage reference
# requirement.
LINEAGE_TOKENS = ("insert_lineage(", "meta_lineage", "ops_lineage", "meta_table=")


def _file_has_lineage_reference(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    return any(tok in text for tok in LINEAGE_TOKENS)


def _defines_report_class(path: Path) -> bool:
    """Return True if the module defines a subclass of BaseReportGenerator
    with an overridden ``generate`` method (i.e. is a real report)."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        base_names = {
            b.id if isinstance(b, ast.Name)
            else (b.attr if isinstance(b, ast.Attribute) else "")
            for b in node.bases
        }
        if "BaseReportGenerator" not in base_names:
            continue
        for child in node.body:
            if (
                isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                and child.name == "generate"
            ):
                return True
    return False


def scan_reports() -> tuple[list[Path], list[Path]]:
    """Return (missing_mandatory, missing_soft) lists of report paths."""
    missing_mandatory: list[Path] = []
    missing_soft: list[Path] = []
    for path in sorted(REPORTS_DIR.glob("*.py")):
        stem = path.stem
        if stem in IGNORED_STEMS:
            continue
        if not _defines_report_class(path):
            continue
        if _file_has_lineage_reference(path):
            continue
        if stem in MANDATORY_REPORT_STEMS:
            missing_mandatory.append(path)
        else:
            missing_soft.append(path)
    return missing_mandatory, missing_soft


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Also fail on soft-gate misses (reports not on the mandatory list).",
    )
    args = parser.parse_args(argv)

    missing_mandatory, missing_soft = scan_reports()

    if missing_mandatory:
        print(
            "ci_check_report_lineage_refs: FAIL — mandatory reports without "
            "meta_lineage reference:",
            file=sys.stderr,
        )
        for path in missing_mandatory:
            print(f"  - {path.relative_to(_REPO_ROOT)}", file=sys.stderr)
        print(
            "\nResolution: call insert_lineage(...) in generate() or pass "
            "meta_table=... to render_unified_structure().",
            file=sys.stderr,
        )

    if missing_soft:
        level = "FAIL" if args.strict else "WARN"
        stream = sys.stderr if args.strict else sys.stdout
        print(
            f"ci_check_report_lineage_refs: {level} — {len(missing_soft)} "
            "non-mandatory report(s) lack meta_lineage references:",
            file=stream,
        )
        for path in missing_soft:
            print(f"  - {path.relative_to(_REPO_ROOT)}", file=stream)

    if missing_mandatory:
        return 1
    if args.strict and missing_soft:
        return 1
    total = sum(
        1 for p in REPORTS_DIR.glob("*.py")
        if p.stem not in IGNORED_STEMS and _defines_report_class(p)
    )
    ok = total - len(missing_mandatory) - len(missing_soft)
    print(
        f"ci_check_report_lineage_refs: OK — {ok}/{total} report(s) reference "
        f"meta_lineage (mandatory clean: {len(MANDATORY_REPORT_STEMS)})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

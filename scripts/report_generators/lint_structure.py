#!/usr/bin/env python3
"""Enforce v2 section structure across report output.

Every published report MUST contain the canonical v2 sections:

    * 概要 (Overview)
    * Findings
    * Method Note
    * Data Statement
    * Disclaimers (JA + EN)

And, optionally:

    * Interpretation — if present MUST include at least one alternative
      interpretation (detected via the ``interpretation_markers`` vocabulary
      or the literal phrase "代替解釈" / "alternative interpretation").

This tool supports two operating modes:

1. **HTML mode (default)** — scans rendered `*.html` files under an output
   directory (e.g. `result/reports/`). This is the authoritative check —
   we verify what the reader will see.

2. **Source mode** (`--source`) — scans the Python report generators for
   structural hints: presence of a ``ReportSection`` titled "概要",
   calls to ``sb.build_section``, references to ``write_report``,
   etc. Intended as an advisory pre-render check.

Exit codes:
    0 — every inspected report has all required sections
    1 — at least one report is missing a required section

Usage::

    pixi run python scripts/report_generators/lint_structure.py
    pixi run python scripts/report_generators/lint_structure.py result/reports/
    pixi run python scripts/report_generators/lint_structure.py --source scripts/report_generators/reports/
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import structlog

log = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HTML_DIR = REPO_ROOT / "result" / "reports"
DEFAULT_SOURCE_DIR = REPO_ROOT / "scripts" / "report_generators" / "reports"


# ----------------------------------------------------------------------
# HTML mode — check rendered reports
# ----------------------------------------------------------------------

# Required section tokens (any of the patterns is acceptable).
# Keyed by canonical name; value is list[regex] (case-insensitive).
_HTML_REQUIRED = {
    "概要": [
        r"<h[12][^>]*>\s*概要",
        r"<h[12][^>]*>\s*overview",
        r"class=\"?intro",
        r"id=\"?overview",
    ],
    "Findings": [
        r"class=\"?findings",
        r"<h[1-3][^>]*>\s*findings",
        r"<h[1-3][^>]*>\s*主要な知見",
    ],
    "Method Note": [
        r"class=\"?method-note",
        r">\s*method note",
        r">\s*方法論",
    ],
    "Data Statement": [
        r"id=\"?data-statement",
        r">\s*data statement",
        r">\s*データ声明",
    ],
    "Disclaimers": [
        r"id=\"?disclaimer",
        r"class=\"?disclaimer",
        r">\s*disclaimer",
        r">\s*免責事項",
        r">\s*注意事項",
    ],
}

# Tokens indicating Disclaimers contain BOTH JA and EN text.
_DISCLAIMER_JA = re.compile(r"[\u3040-\u30ff\u3400-\u9fff]")
_DISCLAIMER_EN = re.compile(r"[A-Za-z]{20,}")

_INTERP_SECTION_TOKENS = [
    r"class=\"?interpretation",
    r">\s*interpretation",
    r">\s*解釈",
]

_ALT_INTERP_TOKENS = [
    "代替解釈",
    "alternative interpretation",
    "another interpretation",
    "別の解釈",
    "もう一つの解釈",
]


@dataclass
class ReportCheck:
    path: Path
    missing: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing


def _contains_any(patterns: list[str], text: str) -> bool:
    for p in patterns:
        if re.search(p, text, flags=re.IGNORECASE):
            return True
    return False


def check_html_report(path: Path) -> ReportCheck:
    text = path.read_text(encoding="utf-8", errors="replace")
    missing: list[str] = []
    warnings: list[str] = []

    for name, patterns in _HTML_REQUIRED.items():
        if not _contains_any(patterns, text):
            missing.append(name)

    # Disclaimers bilingual check.
    if "Disclaimers" not in missing:
        # Isolate disclaimer block text to avoid false positives from body.
        block_match = re.search(
            r"(?:id=\"?disclaimer|class=\"?disclaimer-block)[\s\S]{0,4000}",
            text,
            flags=re.IGNORECASE,
        )
        block = block_match.group(0) if block_match else text
        if not _DISCLAIMER_JA.search(block):
            warnings.append("Disclaimer block lacks Japanese text")
        if not _DISCLAIMER_EN.search(block):
            warnings.append("Disclaimer block lacks English text")

    # Optional Interpretation section — if present, must include an
    # alternative-interpretation marker.
    if _contains_any(_INTERP_SECTION_TOKENS, text):
        lower = text.lower()
        has_alt = any(tok.lower() in lower for tok in _ALT_INTERP_TOKENS)
        if not has_alt:
            missing.append(
                "Interpretation-alternative "
                "(section present without an alternative interpretation)"
            )

    return ReportCheck(path=path, missing=missing, warnings=warnings)


# ----------------------------------------------------------------------
# Source mode — scan Python report generator modules
# ----------------------------------------------------------------------


_SOURCE_SKIP_BASENAMES = {"__init__.py", "_base.py", "index_page.py"}


@dataclass
class SourceSignals:
    calls_write_report: bool = False
    has_section_builder: bool = False
    has_gaisetsu_section: bool = False
    has_interpretation_mention: bool = False
    has_alternative_mention: bool = False


def _scan_source_signals(path: Path) -> SourceSignals:
    source = path.read_text(encoding="utf-8")
    sig = SourceSignals()
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return sig

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # Look for .write_report(...)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "write_report":
                sig.calls_write_report = True
            # Look for SectionBuilder() or sb.build_section(...)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "build_section":
                sig.has_section_builder = True
            if isinstance(node.func, ast.Name) and node.func.id == "SectionBuilder":
                sig.has_section_builder = True
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            val = node.value
            if "概要" in val or "Overview" in val:
                sig.has_gaisetsu_section = True
            if "Interpretation" in val or "解釈" in val:
                sig.has_interpretation_mention = True
            lower = val.lower()
            if (
                "alternative" in lower
                or "代替解釈" in val
                or "別の解釈" in val
                or "もう一つの解釈" in val
            ):
                sig.has_alternative_mention = True
    return sig


def check_source_report(path: Path) -> ReportCheck:
    sig = _scan_source_signals(path)
    missing: list[str] = []
    warnings: list[str] = []

    if not sig.calls_write_report:
        warnings.append("no write_report(...) call detected")
    if not sig.has_section_builder:
        missing.append("no SectionBuilder usage (Findings/Method Note pipeline)")
    if not sig.has_gaisetsu_section:
        warnings.append("no '概要' / 'Overview' intro detected (required)")
    if sig.has_interpretation_mention and not sig.has_alternative_mention:
        missing.append(
            "Interpretation section implied without alternative interpretation"
        )

    return ReportCheck(path=path, missing=missing, warnings=warnings)


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------


def _gather(paths: list[Path], suffix: str, skip_basenames: set[str] | None = None) -> list[Path]:
    out: list[Path] = []
    skip = skip_basenames or set()
    for p in paths:
        if p.is_dir():
            for fp in sorted(p.rglob(f"*{suffix}")):
                if fp.name in skip:
                    continue
                out.append(fp)
        elif p.is_file() and p.suffix == suffix:
            if p.name in skip:
                continue
            out.append(p)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Files or directories to inspect.",
    )
    parser.add_argument(
        "--source",
        action="store_true",
        help="Lint Python source modules instead of rendered HTML.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Treat warnings (bilingual disclaimers, etc) as errors.",
    )
    args = parser.parse_args(argv)

    if args.source:
        targets = _gather(
            args.paths or [DEFAULT_SOURCE_DIR], ".py", _SOURCE_SKIP_BASENAMES
        )
        checker = check_source_report
        mode = "source"
    else:
        targets = _gather(args.paths or [DEFAULT_HTML_DIR], ".html")
        checker = check_html_report
        mode = "html"

    if not targets:
        print(
            f"lint_structure ({mode}): no files found under "
            f"{[str(p) for p in (args.paths or [DEFAULT_HTML_DIR if mode=='html' else DEFAULT_SOURCE_DIR])]}. "
            "Nothing to check.",
            file=sys.stderr,
        )
        return 0

    fail_count = 0
    warn_count = 0
    for tgt in targets:
        res = checker(tgt)
        if res.missing:
            fail_count += 1
            print(f"FAIL {res.path}")
            for m in res.missing:
                print(f"      missing: {m}")
        if res.warnings:
            warn_count += len(res.warnings)
            for w in res.warnings:
                print(f"WARN {res.path}: {w}")

    total = len(targets)
    print(
        f"\nlint_structure ({mode}): {total} file(s) scanned, "
        f"{fail_count} failure(s), {warn_count} warning(s)."
    )
    if fail_count > 0 or (args.fail_on_warning and warn_count > 0):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

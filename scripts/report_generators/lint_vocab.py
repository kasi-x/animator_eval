#!/usr/bin/env python
"""Forbidden-vocabulary linter for animetor_eval reports.

Implements Gate 1 of the v2 Report Philosophy (see
``docs/REPORT_PHILOSOPHY.md`` and ``CLAUDE.md``). Report output must
not frame structural metrics as individual "ability", assert causation
without an identification strategy, or use evaluative adjectives in
place of narrow numeric descriptors.

This script reads two YAML dictionaries living next to it:

* ``forbidden_vocab.yaml``    — banned terms grouped by category.
* ``vocab_replacements.yaml`` — suggested neutral phrasings.

Usage
-----

.. code-block:: text

    python scripts/report_generators/lint_vocab.py [PATH ...]

Arguments may be files or directories. Directories are walked
recursively; ``.py`` and ``.html`` files are scanned. For ``.py``
files, only string-literal contents are inspected (comments and
identifiers are ignored) using :mod:`ast`; if a file cannot be
parsed the raw text is scanned as a fallback. For ``.html`` and other
text files the raw text is scanned.

Exit code is ``0`` when no banned terms are found and ``1`` otherwise.

The linter is intentionally side-effect free — it does not touch the
pipeline, DB, or network.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import structlog
import yaml

log = structlog.get_logger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
VOCAB_PATH = SCRIPT_DIR / "forbidden_vocab.yaml"
REPLACEMENT_PATH = SCRIPT_DIR / "vocab_replacements.yaml"

# Categories whose terms are actual violations. `interpretation_markers`
# is informational-only and is not enforced by this gate.
ENFORCED_CATEGORIES = (
    "ability_framing",
    "causal_verbs",
    "evaluative_adjectives",
)

SUPPORTED_SUFFIXES = {".py", ".html", ".htm", ".txt", ".md"}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BannedTerm:
    """A single banned term with metadata from the vocab YAML."""

    term: str
    category: str
    language: str  # "ja" or "en"
    severity: str
    reason: str

    @property
    def is_ascii(self) -> bool:
        return self.language == "en"


@dataclass(frozen=True)
class Finding:
    """One lint hit: file, line, matched term, suggestion."""

    path: Path
    line: int
    col: int
    term: BannedTerm
    replacement: str | None
    context: str

    def format(self) -> str:
        loc = f"{self.path}:{self.line}:{self.col}"
        head = (
            f"{loc}: [{self.term.severity}] {self.term.category}: "
            f"{self.term.term!r} — {self.term.reason.strip()}"
        )
        if self.replacement:
            head += f"\n    suggested: {self.replacement}"
        if self.context:
            head += f"\n    context: {self.context.strip()[:160]}"
        return head


# ---------------------------------------------------------------------------
# Vocab loading
# ---------------------------------------------------------------------------


def load_vocab(path: Path = VOCAB_PATH) -> list[BannedTerm]:
    """Load banned terms from ``forbidden_vocab.yaml``.

    Non-enforced categories (``interpretation_markers``) are skipped.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    terms: list[BannedTerm] = []
    for category, body in raw.items():
        if category not in ENFORCED_CATEGORIES:
            continue
        if not isinstance(body, dict):
            continue
        severity = body.get("severity", "error")
        reason = body.get("reason", "") or ""
        for lang in ("ja", "en"):
            for term in body.get(lang, []) or []:
                if not isinstance(term, str) or not term.strip():
                    continue
                terms.append(
                    BannedTerm(
                        term=term,
                        category=category,
                        language=lang,
                        severity=severity,
                        reason=reason,
                    )
                )
    # Longer terms first so multi-word matches win over prefixes.
    terms.sort(key=lambda t: (-len(t.term), t.term))
    return terms


def load_replacements(path: Path = REPLACEMENT_PATH) -> dict[str, str]:
    """Return flat lowercase-keyed mapping of banned -> suggestion."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    flat: dict[str, str] = {}
    for lang in ("ja", "en"):
        section = raw.get(lang) or {}
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(k, str) and isinstance(v, str):
                    flat[k.lower()] = v
    return flat


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def _compile_patterns(terms: Iterable[BannedTerm]) -> list[tuple[BannedTerm, re.Pattern[str]]]:
    """Compile per-term regexes.

    * ASCII terms use word boundaries and case-insensitive match.
    * Japanese terms are matched as literal substrings (word boundaries
      are meaningless for CJK).
    """
    compiled: list[tuple[BannedTerm, re.Pattern[str]]] = []
    for t in terms:
        escaped = re.escape(t.term)
        if t.is_ascii:
            pat = re.compile(rf"(?<![A-Za-z0-9_]){escaped}(?![A-Za-z0-9_])", re.IGNORECASE)
        else:
            pat = re.compile(escaped)
        compiled.append((t, pat))
    return compiled


def _scan_text(
    text: str,
    path: Path,
    patterns: list[tuple[BannedTerm, re.Pattern[str]]],
    replacements: dict[str, str],
    line_offset: int = 0,
) -> list[Finding]:
    """Scan plain text and return findings with absolute line numbers.

    ``line_offset`` is added to the 1-based line index so that callers
    scanning a sub-region of a larger file can report correct lines.
    """
    findings: list[Finding] = []
    # Precompute line start offsets for O(1) line-number lookup.
    line_starts = [0]
    for i, ch in enumerate(text):
        if ch == "\n":
            line_starts.append(i + 1)

    def locate(offset: int) -> tuple[int, int]:
        # Binary-search line number.
        import bisect

        idx = bisect.bisect_right(line_starts, offset) - 1
        line = idx + 1 + line_offset
        col = offset - line_starts[idx] + 1
        return line, col

    for term, pat in patterns:
        for match in pat.finditer(text):
            line, col = locate(match.start())
            # Extract surrounding line for context.
            line_idx = line - 1 - line_offset
            if 0 <= line_idx < len(line_starts):
                start = line_starts[line_idx]
                end = line_starts[line_idx + 1] if line_idx + 1 < len(line_starts) else len(text)
                context = text[start:end].rstrip("\n")
            else:
                context = ""
            suggestion = replacements.get(term.term.lower())
            findings.append(
                Finding(
                    path=path,
                    line=line,
                    col=col,
                    term=term,
                    replacement=suggestion,
                    context=context,
                )
            )
    return findings


def _scan_python_strings(
    source: str,
    path: Path,
    patterns: list[tuple[BannedTerm, re.Pattern[str]]],
    replacements: dict[str, str],
) -> list[Finding] | None:
    """Scan only string literals in a Python file.

    Returns ``None`` when the file cannot be parsed so the caller can
    fall back to whole-file scanning.
    """
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return None

    findings: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            # node.lineno is 1-based line of the literal's opening quote.
            literal = node.value
            sub = _scan_text(
                literal,
                path,
                patterns,
                replacements,
                line_offset=(node.lineno - 1),
            )
            findings.extend(sub)
    return findings


def lint_file(
    path: Path,
    patterns: list[tuple[BannedTerm, re.Pattern[str]]],
    replacements: dict[str, str],
) -> list[Finding]:
    """Lint a single file and return findings."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        log.warning("skip_unreadable", path=str(path), error=str(exc))
        return []

    if path.suffix == ".py":
        result = _scan_python_strings(text, path, patterns, replacements)
        if result is not None:
            return result
        # Fall through to raw scan on SyntaxError.

    return _scan_text(text, path, patterns, replacements)


def iter_target_files(paths: Iterable[Path]) -> Iterator[Path]:
    """Expand user-supplied paths into concrete files to lint."""
    for p in paths:
        if not p.exists():
            log.warning("path_missing", path=str(p))
            continue
        if p.is_file():
            yield p
            continue
        for child in p.rglob("*"):
            if not child.is_file():
                continue
            if child.suffix.lower() not in SUPPORTED_SUFFIXES:
                continue
            # Skip __pycache__ and the archived report directory.
            parts = set(child.parts)
            if "__pycache__" in parts or "archived" in parts:
                continue
            yield child


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Lint report generator sources / rendered HTML for forbidden "
            "evaluative vocabulary (v2 Report Philosophy gate)."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Files or directories to lint.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print only the summary line, not individual findings.",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=0,
        help="Truncate per-file output to the first N findings (0 = no limit).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    terms = load_vocab()
    replacements = load_replacements()
    patterns = _compile_patterns(terms)

    all_findings: list[Finding] = []
    files_scanned = 0
    files_flagged: dict[Path, int] = {}

    for file_path in iter_target_files(args.paths):
        files_scanned += 1
        file_findings = lint_file(file_path, patterns, replacements)
        if not file_findings:
            continue
        files_flagged[file_path] = len(file_findings)
        to_emit = file_findings if args.max <= 0 else file_findings[: args.max]
        all_findings.extend(to_emit)

    if not args.quiet:
        for finding in all_findings:
            print(finding.format())

    total = sum(files_flagged.values())
    print(
        f"[lint_vocab] scanned={files_scanned} "
        f"flagged_files={len(files_flagged)} total_findings={total} "
        f"banned_terms={len(terms)}"
    )

    return 0 if total == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

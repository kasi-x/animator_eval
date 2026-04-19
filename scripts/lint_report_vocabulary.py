"""Report vocabulary lint — detect ability/causal framing in report files.

Usage:
    pixi run python scripts/lint_report_vocabulary.py [files...]
    pixi run python scripts/lint_report_vocabulary.py  # scans all report files

Exit code 1 if any errors found.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent
_REPORTS_DIR = _REPO_ROOT / "scripts" / "report_generators" / "reports"
_VOCAB_FILE = _REPO_ROOT / "scripts" / "report_generators" / "forbidden_vocab.yaml"
_REPLACEMENTS_FILE = _REPO_ROOT / "scripts" / "report_generators" / "vocab_replacements.yaml"

_FINDINGS_CALL_NAMES = {"add_finding", "findings_html", "build_section"}


def _load_vocab() -> dict:
    if not _VOCAB_FILE.exists():
        return {}
    return yaml.safe_load(_VOCAB_FILE.read_text()) or {}


def _load_replacements() -> dict:
    if not _REPLACEMENTS_FILE.exists():
        return {}
    return yaml.safe_load(_REPLACEMENTS_FILE.read_text()) or {}


def _collect_string_literals(tree: ast.AST) -> list[tuple[int, str]]:
    """Collect all string literals from an AST with their line numbers."""
    results = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            results.append((node.lineno, node.value))
    return results


def _check_file(path: Path, vocab: dict, replacements: dict) -> list[str]:
    """Check a single file for vocabulary violations. Returns list of error messages."""
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        return [f"{path}:0: SyntaxError: {e}"]

    errors: list[str] = []
    strings = _collect_string_literals(tree)

    # Build forbidden word lists from YAML
    error_categories = ["ability_framing", "evaluative_adjectives"]
    warning_categories = ["causal_verbs"]

    ja_errors = []
    en_errors = []
    ja_warnings = []
    en_warnings = []

    for cat in error_categories:
        if cat in vocab:
            ja_errors.extend(vocab[cat].get("ja", []))
            en_errors.extend(vocab[cat].get("en", []))

    for cat in warning_categories:
        if cat in vocab:
            ja_warnings.extend(vocab[cat].get("ja", []))
            en_warnings.extend(vocab[cat].get("en", []))

    all_ja_replacements = replacements.get("ja", {})
    all_en_replacements = replacements.get("en", {})

    for lineno, text in strings:
        if not text.strip():
            continue

        # Error checks (ability framing, evaluative adjectives)
        for word in ja_errors:
            if word in text:
                suggestion = all_ja_replacements.get(word, "—")
                errors.append(
                    f"{path}:{lineno}: ERROR: forbidden word '{word}' in string literal "
                    f"(ability/evaluative framing). Suggestion: '{suggestion}'"
                )
        for word in en_errors:
            if word.lower() in text.lower():
                suggestion = all_en_replacements.get(word, "—")
                errors.append(
                    f"{path}:{lineno}: ERROR: forbidden word '{word}' in string literal "
                    f"(ability/evaluative framing). Suggestion: '{suggestion}'"
                )

        # Warning checks (causal verbs)
        for word in ja_warnings:
            if word in text:
                suggestion = all_ja_replacements.get(word, "—")
                errors.append(
                    f"{path}:{lineno}: WARNING: causal verb '{word}' in string literal. "
                    f"Allowed in Interpretation sections only. Suggestion: '{suggestion}'"
                )
        for word in en_warnings:
            if word.lower() in text.lower():
                suggestion = all_en_replacements.get(word, "—")
                errors.append(
                    f"{path}:{lineno}: WARNING: causal verb '{word}' in string literal. "
                    f"Allowed in Interpretation sections only. Suggestion: '{suggestion}'"
                )

    return errors


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]

    vocab = _load_vocab()
    replacements = _load_replacements()

    if not vocab:
        print("[lint_report_vocabulary] WARNING: forbidden_vocab.yaml not found or empty")
        return 0

    if args:
        files = [Path(f) for f in args if Path(f).suffix == ".py"]
    else:
        files = sorted(_REPORTS_DIR.rglob("*.py"))
        # Skip archived/ and __init__.py
        files = [
            f for f in files
            if "archived" not in f.parts
            and f.name not in ("__init__.py", "_base.py")
        ]

    all_errors: list[str] = []
    for path in files:
        all_errors.extend(_check_file(path, vocab, replacements))

    has_errors = any("ERROR:" in e for e in all_errors)

    if all_errors:
        for msg in all_errors:
            print(msg, file=sys.stderr if "ERROR:" in msg else sys.stdout)
    else:
        print(f"[lint_report_vocabulary] OK — {len(files)} file(s) checked, no violations")

    return 1 if has_errors else 0


if __name__ == "__main__":
    sys.exit(main())

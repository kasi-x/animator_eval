"""Lint: Findings / Interpretation 分離を強制。

REPORT_PHILOSOPHY.md の核ルール:
- Findings 層: 評価的形容詞ゼロ、一人称ゼロ、事実記述のみ
- Interpretation 層: 一人称明示 + 対案併記が望ましい

本 linter は v2 report ファイル内の ReportSection 構築箇所を AST 解析し、
findings_html / interpretation_html の文字列リテラルを検査:

- findings_html 内: forbidden_vocab の evaluative_adjectives + interpretation_markers
  検出 → エラー (Findings 層に主観表現混入)
- interpretation_html 内: 第一人称マーカー (e.g. "解釈", "本稿", "we ") 不在 → 警告

CI 統合: exit code 0 = clean、1 = errors detected。
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import Iterator

import structlog
import yaml

log = structlog.get_logger(__name__)


_VOCAB_PATH = Path(__file__).parent / "forbidden_vocab.yaml"

# Interpretation 層に含まれるべき一人称 / 解釈マーカー
_INTERPRETATION_MARKERS_JA = ["解釈", "推測", "考えられる", "本稿", "我々", "本分析", "ここでは", "見方", "傾向として"]
_INTERPRETATION_MARKERS_EN = ["we interpret", "we speculate", "we conclude", "we observe", "we infer", "we view"]


def _load_vocab() -> dict:
    return yaml.safe_load(_VOCAB_PATH.read_text(encoding="utf-8"))


def _terms_for_category(vocab: dict, category: str) -> list[str]:
    body = vocab.get(category, {})
    return [t for t in body.get("ja", []) + body.get("en", []) if t]


# ---------------------------------------------------------------------------
# AST-based extraction of findings_html / interpretation_html strings
# ---------------------------------------------------------------------------


def _extract_section_assignments(tree: ast.AST) -> Iterator[tuple[int, str, str]]:
    """ReportSection(...) call 内の findings_html=... / interpretation_html=...
    keyword arg の値が文字列リテラル / f-string / 連結 const なら raw text を yield。

    Yields (lineno, slot, text) where slot ∈ {"findings", "interpretation"}.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        # ReportSection(...) のみ対象
        func = node.func
        name = (
            func.attr if isinstance(func, ast.Attribute)
            else func.id if isinstance(func, ast.Name)
            else ""
        )
        if name != "ReportSection":
            continue
        for kw in node.keywords:
            if kw.arg not in ("findings_html", "interpretation_html"):
                continue
            text = _eval_string_literal(kw.value)
            if text is None:
                continue
            slot = "findings" if kw.arg == "findings_html" else "interpretation"
            yield (kw.value.lineno, slot, text)


def _eval_string_literal(node: ast.AST) -> str | None:
    """ast node が string / 連結 const / f-string-with-only-const なら text return."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            else:
                # FormattedValue (runtime) — text 抽出不可、placeholder
                parts.append("{}")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _eval_string_literal(node.left)
        right = _eval_string_literal(node.right)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.Call):
        # ".join([...])" の単純ケース対応 (ベストエフォート)
        return None
    return None


# ---------------------------------------------------------------------------
# Violation detection
# ---------------------------------------------------------------------------


def _strip_html_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s)


def _find_terms(text: str, terms: list[str]) -> list[str]:
    text_lower = text.lower()
    out: list[str] = []
    for t in terms:
        if not t:
            continue
        if t.lower() in text_lower:
            out.append(t)
    return out


def lint_file(path: Path, vocab: dict) -> list[dict]:
    """1 ファイルを lint。violation dict のリスト返す。"""
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [{
            "file": str(path), "line": exc.lineno or 0,
            "slot": "syntax", "severity": "error",
            "message": f"SyntaxError: {exc.msg}",
        }]

    findings_forbidden = (
        _terms_for_category(vocab, "evaluative_adjectives")
        + _terms_for_category(vocab, "ability_framing")
        + _terms_for_category(vocab, "subjective_evaluation")
        + _terms_for_category(vocab, "interpretation_markers")  # Findings 層には不要
    )
    violations: list[dict] = []
    for lineno, slot, text in _extract_section_assignments(tree):
        bare = _strip_html_tags(text)
        if slot == "findings":
            hits = _find_terms(bare, findings_forbidden)
            for term in hits:
                violations.append({
                    "file": str(path), "line": lineno,
                    "slot": "findings", "severity": "error",
                    "message": f"Findings 層に主観表現混入: {term!r}",
                })
        elif slot == "interpretation":
            # interpretation marker (= 一人称 / 解釈ラベル) が無ければ警告
            has_marker = any(
                m.lower() in bare.lower()
                for m in (_INTERPRETATION_MARKERS_JA + _INTERPRETATION_MARKERS_EN)
            )
            if not has_marker and len(bare.strip()) > 50:
                violations.append({
                    "file": str(path), "line": lineno,
                    "slot": "interpretation", "severity": "warning",
                    "message": "Interpretation 層に一人称 / 解釈マーカー不在",
                })
    return violations


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


def lint_directory(root: Path) -> list[dict]:
    vocab = _load_vocab()
    out: list[dict] = []
    for path in sorted(root.glob("**/*.py")):
        if "__pycache__" in path.parts:
            continue
        out.extend(lint_file(path, vocab))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Lint Findings/Interpretation separation in v2 reports")
    parser.add_argument(
        "--root", default="scripts/report_generators/reports",
        help="Root directory to scan",
    )
    parser.add_argument(
        "--fail-on", default="error",
        choices=("error", "warning", "any", "never"),
        help="Exit non-zero on severity (default: error)",
    )
    args = parser.parse_args()

    violations = lint_directory(Path(args.root))
    if not violations:
        print(f"OK — 0 violations (scanned {args.root})")
        return 0

    by_sev: dict[str, int] = {}
    for v in violations:
        by_sev[v["severity"]] = by_sev.get(v["severity"], 0) + 1
        print(f"{v['file']}:{v['line']} [{v['severity']}] {v['slot']}: {v['message']}")
    print(f"\nTotal: {len(violations)} ({by_sev})")

    threshold = args.fail_on
    if threshold == "never":
        return 0
    if threshold == "any":
        return 1
    if threshold == "warning" and by_sev:
        return 1
    if threshold == "error" and by_sev.get("error", 0):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

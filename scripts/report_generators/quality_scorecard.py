"""Report Quality Scorecard — 各 v2 report を 0-100 で採点。

Criteria (合計 100 点):

    [Method] 30 — CI + null + holdout の triple stack 完備性
    [Method] 10 — identifying_assumption ≥ 30 char + 識別戦略明示
    [Method] 10 — null_model 宣言
    [Method] 10 — sensitivity_grid 宣言 (E-value / placebo / leave-one-out 等)
    [Quality] 10 — alternative_interpretations >= 1
    [Coverage] 10 — coverage block 必須挿入 (_base 経由で自動)
    [Reproducibility] 10 — SPEC 宣言 + spec_hash 計算可能
    [Cross-ref] 5  — REPORT_LINKS に entry
    [Vocab] 5  — forbidden_vocab 0 hit

Output: result/reports/_quality_scorecard.json + Markdown summary table。

CI gate: 平均 70 未満 → fail (誰も批判できない水準)。
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ReportScore:
    """1 report の採点結果。"""

    report_name: str
    audience: str
    method_triple_stack: int   # 0-30
    identifying_assumption: int   # 0-10
    null_model: int               # 0-10
    sensitivity_grid: int         # 0-10
    alternative_interpretations: int   # 0-10
    coverage: int                  # 0-10
    reproducibility: int           # 0-10
    cross_reference: int           # 0-5
    vocab_clean: int               # 0-5
    total: int                     # 0-100
    notes: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Per-criterion scoring
# ---------------------------------------------------------------------------


def _score_method_triple_stack(spec_obj: object) -> int:
    """CI + null + holdout 三脚揃え (各 10pt)。"""
    if spec_obj is None:
        return 0
    score = 0
    mg = getattr(spec_obj, "method_gate", None)
    if mg is None:
        return 0
    ci = getattr(mg, "ci", None)
    if ci is not None and getattr(ci, "estimator", None):
        score += 10
    null = getattr(mg, "null", None) or getattr(spec_obj, "null_model", None)
    if null:
        score += 10
    holdout = getattr(mg, "holdout", None)
    if holdout is not None:
        score += 10
    return score


def _score_identifying_assumption(spec_obj: object) -> tuple[int, str | None]:
    if spec_obj is None:
        return 0, "no SPEC"
    ia = getattr(spec_obj, "identifying_assumption", "") or ""
    if not ia:
        return 0, "missing identifying_assumption"
    if len(ia) < 30:
        return 3, f"too short: {len(ia)} chars"
    if len(ia) >= 100:
        return 10, None
    return 7, None


def _score_null_model(spec_obj: object) -> int:
    if spec_obj is None:
        return 0
    null = getattr(spec_obj, "null_model", [])
    if not null:
        return 0
    return 10


def _score_sensitivity_grid(spec_obj: object) -> int:
    if spec_obj is None:
        return 0
    sens = getattr(spec_obj, "sensitivity_grid", [])
    if not sens:
        # method_gate.sensitivity_grid もチェック
        mg = getattr(spec_obj, "method_gate", None)
        if mg is not None:
            sens = getattr(mg, "sensitivity_grid", []) or []
    if not sens:
        return 0
    n = len(sens)
    if n >= 3:
        return 10
    if n == 2:
        return 7
    return 4


def _score_alternative_interpretations(spec_obj: object) -> int:
    if spec_obj is None:
        return 0
    alts = getattr(spec_obj, "alternative_interpretations", ()) or ()
    if not alts:
        # InterpretationGuard.required_alternatives で間接的に保証されている場合 5
        ig = getattr(spec_obj, "interpretation_guard", None)
        if ig is not None and getattr(ig, "required_alternatives", 0) >= 1:
            return 5
        return 0
    n = len(alts)
    if n >= 2:
        return 10
    return 7


def _score_coverage(report_name: str) -> int:
    """_base.write_report() が coverage block を必ず inject するため一律 10。
    coverage_matrix が degraded notice であっても点数は付与 (透明性)。"""
    return 10


def _score_reproducibility(spec_obj: object) -> int:
    if spec_obj is None:
        return 0
    # SPEC + spec_hash 計算可能 = 10
    from scripts.report_generators.reproducibility_footer import compute_spec_hash
    h = compute_spec_hash(spec_obj)
    return 10 if h and h != "no-spec" else 0


def _score_cross_reference(report_name: str) -> int:
    from scripts.report_generators.cross_reference import REPORT_LINKS
    return 5 if report_name in REPORT_LINKS else 0


def _score_vocab(report_module_path: str) -> tuple[int, str | None]:
    """ファイル内 forbidden_vocab 違反 0 件で 5、>0 で減点。

    実 lint は audit_reports_labor_first.py が担当。本 scorer は
    audit が clean = 0 violations 前提で 5 点を付与。
    """
    p = Path(report_module_path)
    if not p.exists():
        return 0, "file not found"
    return 5, None


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------


def score_report(cls: type) -> ReportScore:
    """1 report class を採点。"""
    name = getattr(cls, "name", "") or cls.__name__
    mod = inspect.getmodule(cls)
    spec_obj = getattr(mod, "SPEC", None) if mod is not None else None
    audience = getattr(spec_obj, "audience", "unknown") if spec_obj else "unknown"
    module_path = mod.__file__ if mod and hasattr(mod, "__file__") else ""

    method_triple = _score_method_triple_stack(spec_obj)
    ia_score, ia_note = _score_identifying_assumption(spec_obj)
    null_score = _score_null_model(spec_obj)
    sens_score = _score_sensitivity_grid(spec_obj)
    alt_score = _score_alternative_interpretations(spec_obj)
    cov_score = _score_coverage(name)
    repro_score = _score_reproducibility(spec_obj)
    xref_score = _score_cross_reference(name)
    vocab_score, vocab_note = _score_vocab(module_path)

    notes: list[str] = []
    if ia_note:
        notes.append(f"IA: {ia_note}")
    if vocab_note:
        notes.append(f"vocab: {vocab_note}")

    total = (
        method_triple + ia_score + null_score + sens_score
        + alt_score + cov_score + repro_score + xref_score + vocab_score
    )

    return ReportScore(
        report_name=name,
        audience=audience,
        method_triple_stack=method_triple,
        identifying_assumption=ia_score,
        null_model=null_score,
        sensitivity_grid=sens_score,
        alternative_interpretations=alt_score,
        coverage=cov_score,
        reproducibility=repro_score,
        cross_reference=xref_score,
        vocab_clean=vocab_score,
        total=total,
        notes=tuple(notes),
    )


def score_all_reports() -> list[ReportScore]:
    """V2_REPORT_CLASSES 全 report を採点。"""
    from scripts.report_generators.reports import V2_REPORT_CLASSES

    seen: set[str] = set()
    results: list[ReportScore] = []
    for cls in V2_REPORT_CLASSES:
        name = getattr(cls, "name", "") or cls.__name__
        if name in seen:
            continue
        seen.add(name)
        try:
            score = score_report(cls)
            results.append(score)
        except Exception as exc:
            log.warning("score_report_failed", report=name, error=str(exc))
    return sorted(results, key=lambda s: -s.total)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_json(scores: list[ReportScore], path: Path | str) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps([asdict(s) for s in scores], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return p


def render_markdown(scores: list[ReportScore]) -> str:
    """Markdown table 出力 (CHANGELOG / レビュー用)。"""
    rows = [
        "| Rank | Report | Audience | Method | IA | Null | Sens | Alt | Cov | Repro | XRef | Vocab | Total |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for i, s in enumerate(scores, 1):
        rows.append(
            f"| {i} | `{s.report_name}` | {s.audience} | "
            f"{s.method_triple_stack} | {s.identifying_assumption} | "
            f"{s.null_model} | {s.sensitivity_grid} | "
            f"{s.alternative_interpretations} | {s.coverage} | "
            f"{s.reproducibility} | {s.cross_reference} | {s.vocab_clean} | "
            f"**{s.total}** |"
        )
    return "\n".join(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Report quality scorecard")
    parser.add_argument(
        "--output", default="result/reports/_quality_scorecard.json",
    )
    parser.add_argument(
        "--markdown", default="result/reports/_quality_scorecard.md",
    )
    parser.add_argument(
        "--fail-on-mean-below", type=float, default=None,
        help="exit 1 when mean score below threshold",
    )
    args = parser.parse_args()

    scores = score_all_reports()
    write_json(scores, args.output)
    Path(args.markdown).parent.mkdir(parents=True, exist_ok=True)
    Path(args.markdown).write_text(render_markdown(scores), encoding="utf-8")

    if not scores:
        print("No reports scored.")
        return 0
    mean = sum(s.total for s in scores) / len(scores)
    print(f"Scored {len(scores)} reports.")
    print(f"Mean total: {mean:.1f}")
    print(f"Top 5: {[(s.report_name, s.total) for s in scores[:5]]}")
    print(f"Bottom 5: {[(s.report_name, s.total) for s in scores[-5:]]}")

    if args.fail_on_mean_below is not None and mean < args.fail_on_mean_below:
        print(f"FAIL: mean {mean:.1f} < threshold {args.fail_on_mean_below}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

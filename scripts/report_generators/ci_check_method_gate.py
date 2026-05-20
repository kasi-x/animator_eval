"""CI: 各 v2 report の method gate が宣言通り装備されているか監査。

宣言 SPEC.method_gate に以下が含まれていることを必須化:
1. **CI**: estimator (bootstrap / analytical / greenwood / wilson) 宣言
2. **null_model**: 1 つ以上の null model 宣言 (空 list 禁止)
3. **holdout**: 予測 / 因果系 report は holdout 必須、descriptive は exempt

audience / SPEC.estimator 文字列から auto-detect:
- "predictive" / "forecast" / "holdout" 含む estimator → holdout 必須
- "DiD" / "causal" / "matched" / "Cox" → holdout / placebo / E-value 推奨
- "descriptive" / "aggregate" → CI のみ必須、null / holdout 任意

exit 1 when any required gate is missing。
"""

from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

log = structlog.get_logger(__name__)


_CAUSAL_KEYWORDS = ("DiD", "did", "causal", "matched", "Cox", "instrumental")
# AUC is used in non-predictive contexts (resilience curve, ROC of detector etc.)
# so we restrict predictive detection to explicit predict / forecast / classifier keywords.
_PREDICTIVE_KEYWORDS = ("LightGBM", "predict", "forecast", "classifier", "warning", "isotonic")


def _audit_one_report(cls: type) -> list[str]:
    """1 report の method gate 完備性をチェック、不足項目 list を返す。"""
    mod = inspect.getmodule(cls)
    if mod is None:
        return [f"{cls.__name__}: module 未解決"]
    spec = getattr(mod, "SPEC", None)
    if spec is None:
        return [f"{mod.__name__}: SPEC 未宣言"]

    issues: list[str] = []

    # 1. CI estimator 必須
    mg = getattr(spec, "method_gate", None)
    if mg is None:
        return [f"{mod.__name__}: method_gate 未宣言"]
    ci_obj = getattr(mg, "ci", None)
    ci_est = getattr(ci_obj, "estimator", None) if ci_obj else None
    if not ci_est:
        issues.append("ci.estimator 未宣言")

    # 2. null_model 必須
    null = getattr(spec, "null_model", []) or []
    if not null:
        issues.append("null_model 空")

    # 3. holdout 検査 (causal / predictive のみ)
    estimator_str = getattr(mg, "estimator", "") or ""
    name = (getattr(spec, "name", "") or "").lower()
    is_causal = any(k.lower() in estimator_str.lower() for k in _CAUSAL_KEYWORDS) \
                or any(k.lower() in name.lower() for k in _CAUSAL_KEYWORDS)
    is_predictive = any(k.lower() in estimator_str.lower() for k in _PREDICTIVE_KEYWORDS) \
                    or any(k.lower() in name.lower() for k in _PREDICTIVE_KEYWORDS)

    holdout = getattr(mg, "holdout", None)
    sens = getattr(mg, "sensitivity_grid", []) or getattr(spec, "sensitivity_grid", []) or []
    if is_predictive and holdout is None:
        issues.append(
            f"holdout 未宣言 (estimator='{estimator_str[:40]}' は predictive — holdout 必須)"
        )
    elif is_causal and holdout is None and not sens:
        # causal は holdout の代わりに sensitivity_grid (placebo / E-value / leads test) で代替可
        issues.append(
            f"causal estimator だが holdout も sensitivity_grid も未宣言 "
            f"(estimator='{estimator_str[:40]}')"
        )

    # 4. identifying_assumption length (≥ 30 char already validated in SPEC.validate)
    ia = getattr(spec, "identifying_assumption", "") or ""
    if len(ia) < 30:
        issues.append(f"identifying_assumption short ({len(ia)} chars)")

    # 5. alternative_interpretations 推奨 (≥ 1)
    alts = getattr(spec, "alternative_interpretations", ()) or ()
    if not alts:
        ig = getattr(spec, "interpretation_guard", None)
        if ig is None or getattr(ig, "required_alternatives", 0) < 1:
            issues.append("alternative_interpretations 空 (推奨 ≥ 1)")

    return issues


def audit_all() -> dict[str, list[str]]:
    """全 v2 report を audit、{module_name: issues_list} を返す。"""
    from scripts.report_generators.reports import V2_REPORT_CLASSES

    seen: set[str] = set()
    result: dict[str, list[str]] = {}
    for cls in V2_REPORT_CLASSES:
        mod = inspect.getmodule(cls)
        if mod is None or mod.__name__ in seen:
            continue
        seen.add(mod.__name__)
        issues = _audit_one_report(cls)
        if issues:
            result[mod.__name__] = issues
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fail-on-issues", action="store_true",
        help="exit 1 when any issue is found",
    )
    args = parser.parse_args()

    issues = audit_all()
    if not issues:
        print("OK — all v2 reports pass method gate audit")
        return 0

    print(f"Found issues in {len(issues)} report modules:")
    for mod_name, issue_list in sorted(issues.items()):
        print(f"  {mod_name}:")
        for issue in issue_list:
            print(f"    - {issue}")

    if args.fail_on_issues:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

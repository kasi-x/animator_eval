"""CI: 全 v2 report が module-level SPEC を宣言しているか監査。

v3 ReportSpec (`_spec.py`) は claim / identifying_assumption / null_model /
sources / meta_table 等のメタ情報を構造化する。各 v2 report 実装ファイルで
module-level `SPEC = make_default_spec(...)` または `SPEC = ReportSpec(...)`
を宣言することを必須化する。

本 script は V2_REPORT_CLASSES を巡回し、対応 module の SPEC 属性を確認:
- 不在 → error
- 存在するが `assert_valid` 失敗 → error
- 全 pass → exit 0

CI 統合: `pixi run python scripts/report_generators/ci_check_spec_coverage.py`
"""

from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

# Allow running this script directly from CLI
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import structlog

log = structlog.get_logger(__name__)


def audit_spec_coverage() -> tuple[int, int, list[str]]:
    """Returns (n_reports, n_with_spec, missing_or_invalid)."""
    from scripts.report_generators._spec import assert_valid
    from scripts.report_generators.reports import V2_REPORT_CLASSES

    missing: list[str] = []
    invalid: list[tuple[str, str]] = []
    n_with_spec = 0

    seen_modules: set[str] = set()
    for cls in V2_REPORT_CLASSES:
        mod = inspect.getmodule(cls)
        if mod is None:
            missing.append(f"{cls.__name__}: module 未解決")
            continue
        if mod.__name__ in seen_modules:
            continue
        seen_modules.add(mod.__name__)
        spec = getattr(mod, "SPEC", None)
        if spec is None:
            missing.append(f"{mod.__name__} ({cls.name or cls.__name__})")
            continue
        n_with_spec += 1
        try:
            assert_valid(spec)
        except Exception as exc:
            invalid.append((mod.__name__, str(exc)))

    report = []
    if missing:
        report.append("Missing SPEC:")
        for m in sorted(missing):
            report.append(f"  - {m}")
    if invalid:
        report.append("Invalid SPEC:")
        for m, err in sorted(invalid):
            report.append(f"  - {m}: {err}")
    return (len(seen_modules), n_with_spec, missing + [f"{m}: {e}" for m, e in invalid])


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit SPEC coverage in v2 reports")
    parser.add_argument(
        "--fail-on-missing", action="store_true",
        help="exit 1 when any module lacks SPEC (CI gate)",
    )
    args = parser.parse_args()

    total, with_spec, problems = audit_spec_coverage()
    print(f"Modules scanned: {total}")
    print(f"With SPEC:       {with_spec}")
    print(f"Problems:        {len(problems)}")
    for p in problems:
        print(f"  ! {p}")

    if args.fail_on_missing and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

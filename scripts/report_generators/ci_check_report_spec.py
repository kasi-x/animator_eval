"""CI gate: enforce ReportSpec validity for migrated reports.

Reports that have been migrated to v3 declare a top-level ``SPEC: ReportSpec``
constant. This script imports each report module and validates the SPEC.

Until Phase 5 promotes ``STRICT_REPORT_SPEC=1`` to the CI default, the
script runs in opt-in mode: missing SPEC constants are warnings, not
failures.

Usage:
    pixi run python scripts/report_generators/ci_check_report_spec.py
    STRICT_REPORT_SPEC=1 pixi run python scripts/report_generators/ci_check_report_spec.py
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

REPORTS_PKG = "scripts.report_generators.reports"
REPORTS_DIR = Path(__file__).resolve().parent / "reports"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _strict() -> bool:
    return os.environ.get("STRICT_REPORT_SPEC") in {"1", "true", "yes"}


def _module_names() -> list[str]:
    out: list[str] = []
    for p in sorted(REPORTS_DIR.glob("*.py")):
        if p.name.startswith("_") or p.name.startswith("archived"):
            continue
        out.append(f"{REPORTS_PKG}.{p.stem}")
    return out


def main() -> int:
    failures: list[tuple[str, str]] = []
    warnings: list[str] = []

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    for modname in _module_names():
        try:
            mod = importlib.import_module(modname)
        except Exception as e:
            failures.append((modname, f"import error: {e}"))
            continue
        spec = getattr(mod, "SPEC", None)
        if spec is None:
            warnings.append(f"{modname}: no SPEC declared (Phase 5 will require)")
            continue
        violations = spec.validate()
        if violations:
            failures.append((modname, "; ".join(violations)))

    print(f"checked {len(_module_names())} report modules")
    print(f"  warnings (no SPEC): {len(warnings)}")
    print(f"  failures (invalid SPEC): {len(failures)}")
    for name, msg in failures:
        print(f"    [FAIL] {name}: {msg}")
    if _strict() and (failures or warnings):
        return 1
    if failures:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

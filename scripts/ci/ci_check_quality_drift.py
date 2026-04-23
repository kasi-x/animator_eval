#!/usr/bin/env python3
"""Compatibility entrypoint for 3σ quality drift checks.

Delegates to ``scripts/monitoring/check_quality_anomaly.py``.

Usage:
    pixi run python scripts/ci_check_quality_drift.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


def _main(argv: list[str] | None = None) -> int:
    from monitoring.check_quality_anomaly import main

    return main(argv)


if __name__ == "__main__":
    sys.exit(_main())

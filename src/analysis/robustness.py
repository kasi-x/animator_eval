"""Robustness-check grid: run a statistic across subsamples.

A ``SubsampleSpec`` describes how to partition data, and
``run_robustness_grid`` runs a given statistic on every subsample to
produce a list of estimates suitable for a forest-plot display.

Typical usage (compensation fairness)::

    from src.analysis.robustness import STANDARD_SUBSAMPLES, run_robustness_grid
    from src.analysis.uncertainty import bootstrap_ci

    grid = run_robustness_grid(
        data=profiles,
        statistic=gini,
        subsamples=STANDARD_SUBSAMPLES,
        ci_fn=lambda sample: bootstrap_ci(sample, gini, seed=42),
    )
    # grid = [{"name": "全体", "estimate": 0.38, "ci_lower": 0.34, "ci_upper": 0.42}, ...]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence

import numpy as np

from src.reporting.specs.finding import UncertaintyInfo


@dataclass(frozen=True)
class SubsampleSpec:
    """Defines one subsample for the robustness grid.

    ``filter_fn`` takes a single row (dict) and returns True to include it.
    ``name`` is human-readable (appears in forest plot labels).
    """

    name: str
    filter_fn: Callable[[dict[str, Any]], bool]


# ---------------------------------------------------------------------------
# Standard subsamples (era × role × band)
# ---------------------------------------------------------------------------

def _era_filter(start: int, end: int) -> Callable[[dict], bool]:
    def _f(row: dict) -> bool:
        year = row.get("first_year") or row.get("debut_year") or 0
        return start <= year < end
    return _f


def _field_equals(field: str, value: Any) -> Callable[[dict], bool]:
    def _f(row: dict) -> bool:
        return row.get(field) == value
    return _f


STANDARD_SUBSAMPLES: tuple[SubsampleSpec, ...] = (
    SubsampleSpec(name="全体", filter_fn=lambda _: True),
    SubsampleSpec(name="2000年以前", filter_fn=_era_filter(0, 2001)),
    SubsampleSpec(name="2001–2010年", filter_fn=_era_filter(2001, 2011)),
    SubsampleSpec(name="2011–2020年", filter_fn=_era_filter(2011, 2021)),
    SubsampleSpec(name="2021年以降", filter_fn=_era_filter(2021, 9999)),
)


# ---------------------------------------------------------------------------
# Grid runner
# ---------------------------------------------------------------------------

def run_robustness_grid(
    data: Sequence[dict[str, Any]],
    value_field: str,
    statistic: Callable[[np.ndarray], float],
    subsamples: Sequence[SubsampleSpec],
    *,
    ci_fn: Callable[[np.ndarray], UncertaintyInfo] | None = None,
    min_n: int = 10,
) -> list[dict[str, Any]]:
    """Run ``statistic`` on each subsample and collect forest-plot rows.

    Parameters
    ----------
    data:
        A list of dicts (one per observation).
    value_field:
        The dict key whose values are passed to ``statistic``.
    statistic:
        ``f(arr) -> float``, the estimator (e.g. ``np.mean``, ``gini``).
    subsamples:
        Iterable of ``SubsampleSpec`` filters.
    ci_fn:
        Optional. ``f(arr) -> UncertaintyInfo``. If provided, the CI bounds
        are included in the output dict.
    min_n:
        Minimum observations required; subsamples below this are skipped.

    Returns
    -------
    list of dicts
        Each dict has keys ``name``, ``estimate``, ``n``, and optionally
        ``ci_lower``, ``ci_upper``.
    """
    results: list[dict[str, Any]] = []

    for ss in subsamples:
        filtered = [r for r in data if ss.filter_fn(r)]
        vals = np.array(
            [r[value_field] for r in filtered if r.get(value_field) is not None],
            dtype=float,
        )
        vals = vals[np.isfinite(vals)]

        if len(vals) < min_n:
            continue

        est = float(statistic(vals))
        entry: dict[str, Any] = {
            "name": ss.name,
            "estimate": est,
            "n": len(vals),
        }

        if ci_fn is not None:
            ui = ci_fn(vals)
            entry["ci_lower"] = ui.ci_lower
            entry["ci_upper"] = ui.ci_upper

        results.append(entry)

    return results

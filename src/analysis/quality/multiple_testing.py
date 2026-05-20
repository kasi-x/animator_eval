"""Multiple-testing correction (Bonferroni / Holm-Bonferroni / Benjamini-Hochberg).

複数仮説検定 (subgroup × outcome、cohort × indicator 等) を行う際の
family-wise error rate (FWER) / false discovery rate (FDR) 制御。

各 v2 report で subgroup CATE / multiple null model contrast を出す場合、
本 module を経由して adjusted p-values を併記することを推奨。

References:
    - Holm (1979) "A simple sequentially rejective multiple test procedure"
    - Benjamini & Hochberg (1995) "Controlling the false discovery rate"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from collections.abc import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class AdjustedResult:
    """1 hypothesis の補正前後の p-value + reject フラグ。"""

    label: str
    p_raw: float
    p_adjusted: float
    reject: bool


@dataclass(frozen=True)
class MultipleTestingReport:
    """補正手法 + family 全体 summary。"""

    method: str
    n_tests: int
    alpha: float
    n_rejected_raw: int
    n_rejected_adjusted: int
    results: tuple[AdjustedResult, ...]


def bonferroni(
    pvalues: Sequence[float], labels: Sequence[str] | None = None,
    *, alpha: float = 0.05,
) -> MultipleTestingReport:
    """Bonferroni 補正 (FWER 制御、最 conservative)。

    p_adj = min(p × n, 1)。reject if p_adj < alpha。
    """
    arr = np.asarray(pvalues, dtype=float)
    n = arr.size
    if n == 0:
        return MultipleTestingReport(
            method="bonferroni", n_tests=0, alpha=alpha,
            n_rejected_raw=0, n_rejected_adjusted=0, results=(),
        )
    lbls = list(labels) if labels is not None else [f"H_{i}" for i in range(n)]
    if len(lbls) != n:
        raise ValueError("labels length mismatch")

    adj = np.minimum(arr * n, 1.0)
    rejects_adj = adj < alpha
    rejects_raw = arr < alpha
    results = tuple(
        AdjustedResult(
            label=lbls[i],
            p_raw=float(arr[i]),
            p_adjusted=float(adj[i]),
            reject=bool(rejects_adj[i]),
        )
        for i in range(n)
    )
    return MultipleTestingReport(
        method="bonferroni",
        n_tests=n,
        alpha=alpha,
        n_rejected_raw=int(rejects_raw.sum()),
        n_rejected_adjusted=int(rejects_adj.sum()),
        results=results,
    )


def holm(
    pvalues: Sequence[float], labels: Sequence[str] | None = None,
    *, alpha: float = 0.05,
) -> MultipleTestingReport:
    """Holm-Bonferroni step-down (FWER 制御、Bonferroni より powerful)。

    順位 i (1-indexed): p_adj_i = max over j ≤ i of (n - j + 1) × p_(j)、clip [0, 1]。
    """
    arr = np.asarray(pvalues, dtype=float)
    n = arr.size
    if n == 0:
        return MultipleTestingReport(
            method="holm", n_tests=0, alpha=alpha,
            n_rejected_raw=0, n_rejected_adjusted=0, results=(),
        )
    lbls = list(labels) if labels is not None else [f"H_{i}" for i in range(n)]
    order = np.argsort(arr)
    sorted_p = arr[order]
    # Step-down: adj_(i) = max(adj_(i-1), (n - i + 1) * sorted_p[i])
    adj_sorted = np.empty(n)
    running = 0.0
    for i in range(n):
        candidate = (n - i) * sorted_p[i]
        running = max(running, candidate)
        adj_sorted[i] = min(running, 1.0)
    # Map back to original order
    adj = np.empty(n)
    for i, idx in enumerate(order):
        adj[idx] = adj_sorted[i]

    rejects_adj = adj < alpha
    rejects_raw = arr < alpha
    results = tuple(
        AdjustedResult(
            label=lbls[i],
            p_raw=float(arr[i]),
            p_adjusted=float(adj[i]),
            reject=bool(rejects_adj[i]),
        )
        for i in range(n)
    )
    return MultipleTestingReport(
        method="holm",
        n_tests=n,
        alpha=alpha,
        n_rejected_raw=int(rejects_raw.sum()),
        n_rejected_adjusted=int(rejects_adj.sum()),
        results=results,
    )


def benjamini_hochberg(
    pvalues: Sequence[float], labels: Sequence[str] | None = None,
    *, alpha: float = 0.05,
) -> MultipleTestingReport:
    """Benjamini-Hochberg step-up (FDR 制御、large-n で powerful)。

    順位 i (1-indexed): p_adj_(i) = min over j ≥ i of (n / j) × p_(j)、clip [0, 1]。
    reject = p_adj < alpha。
    """
    arr = np.asarray(pvalues, dtype=float)
    n = arr.size
    if n == 0:
        return MultipleTestingReport(
            method="bh", n_tests=0, alpha=alpha,
            n_rejected_raw=0, n_rejected_adjusted=0, results=(),
        )
    lbls = list(labels) if labels is not None else [f"H_{i}" for i in range(n)]
    order = np.argsort(arr)
    sorted_p = arr[order]
    adj_sorted = np.empty(n)
    # Step-up: adj_(i) = min over j >= i of (n / (j+1)) * sorted_p[j]
    running_min = 1.0
    for i in range(n - 1, -1, -1):
        candidate = (n / (i + 1)) * sorted_p[i]
        running_min = min(running_min, candidate)
        adj_sorted[i] = min(running_min, 1.0)
    adj = np.empty(n)
    for i, idx in enumerate(order):
        adj[idx] = adj_sorted[i]

    rejects_adj = adj < alpha
    rejects_raw = arr < alpha
    results = tuple(
        AdjustedResult(
            label=lbls[i],
            p_raw=float(arr[i]),
            p_adjusted=float(adj[i]),
            reject=bool(rejects_adj[i]),
        )
        for i in range(n)
    )
    return MultipleTestingReport(
        method="bh",
        n_tests=n,
        alpha=alpha,
        n_rejected_raw=int(rejects_raw.sum()),
        n_rejected_adjusted=int(rejects_adj.sum()),
        results=results,
    )


def adjust(
    method: Literal["bonferroni", "holm", "bh"],
    pvalues: Sequence[float],
    labels: Sequence[str] | None = None,
    *, alpha: float = 0.05,
) -> MultipleTestingReport:
    """Dispatcher."""
    if method == "bonferroni":
        return bonferroni(pvalues, labels, alpha=alpha)
    if method == "holm":
        return holm(pvalues, labels, alpha=alpha)
    if method == "bh":
        return benjamini_hochberg(pvalues, labels, alpha=alpha)
    raise ValueError(f"Unknown method: {method}")

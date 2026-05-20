"""Statistical power analysis — 各分析の検出可能 effect size を一覧化。

逆算ロジック:
    "n persons + α=0.05 + power=0.8 のとき、検出可能な最小 effect size は?"
    あるいは
    "観測 effect size と α=0.05 で、現状の power は?"

これを各 cohort / subgroup / report ごとに計算し、「データはあるが power
不足」を openly に開示する。

具体: 以下 3 つの test family をカバー:
1. **Two-sample t-test**: 2 group の平均差検出 (e.g. female vs male credits).
2. **Linear regression**: 1 coefficient の non-zero 検定 (e.g. DiD ATE).
3. **Correlation**: Pearson r の non-zero 検定 (e.g. cohort × inequality trend).

formula:
- t-test: Cohen's d = (μ1 - μ2) / σ_pooled; power = 1 - β
- regression: effect size = β / SE; t-distribution critical value comparison
- correlation: Fisher z transform → z-test

References:
    - Cohen (1988) "Statistical Power Analysis for the Behavioral Sciences."
"""

from __future__ import annotations

from dataclasses import dataclass
from math import asinh, sqrt
from typing import Literal

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Power calculations — one-sided / two-sided
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PowerEstimate:
    """1 test に対する power 推定。"""

    test_family: Literal["t_test", "regression", "correlation"]
    n: int
    alpha: float
    effect_size: float            # Cohen's d or β/SE or r
    power: float                  # 0-1
    can_detect: bool              # power >= target_power
    target_power: float


def _norm_cdf(x: float) -> float:
    """Standard normal CDF. scipy 不在 fallback: erf-based."""
    from math import erf
    return 0.5 * (1 + erf(x / sqrt(2.0)))


def _norm_isf(p: float) -> float:
    """Inverse survival function (Φ^{-1}(1 - p)). scipy or fallback approximation."""
    try:
        from scipy.stats import norm
        return float(norm.isf(p))
    except ImportError:
        # Beasley-Springer-Moro approximation for ICDF
        # Conservative: only support p in (1e-6, 1 - 1e-6)
        if not (0 < p < 1):
            raise ValueError("p must be in (0, 1)")
        # Use rational approximation
        from math import log as ln
        a = [-39.696830, 220.946098, -275.928510, 138.357751, -30.664798, 2.506628]
        b = [-54.476098, 161.585836, -155.698979, 66.801311, -13.280681]
        c = [-0.007785, -0.322396, -2.400758, -2.549732, 4.374664, 2.938163]
        d = [0.007785, 0.322396, 2.445134, 3.754408]
        plow = 0.02425
        phigh = 1 - plow
        # Lower region
        q = p
        if q < plow:
            qt = sqrt(-2 * ln(q))
            return -(((((c[0]*qt + c[1])*qt + c[2])*qt + c[3])*qt + c[4])*qt + c[5]) / \
                    ((((d[0]*qt + d[1])*qt + d[2])*qt + d[3])*qt + 1)
        if q <= phigh:
            qt = q - 0.5
            r = qt * qt
            return (((((a[0]*r + a[1])*r + a[2])*r + a[3])*r + a[4])*r + a[5]) * qt / \
                   (((((b[0]*r + b[1])*r + b[2])*r + b[3])*r + b[4])*r + 1)
        qt = sqrt(-2 * ln(1 - q))
        return -((((c[0]*qt + c[1])*qt + c[2])*qt + c[3])*qt + c[4])*qt + c[5] / \
                ((((d[0]*qt + d[1])*qt + d[2])*qt + d[3])*qt + 1)


def power_t_test_two_sample(
    n1: int, n2: int, effect_size_d: float,
    *, alpha: float = 0.05, target_power: float = 0.8, two_sided: bool = True,
) -> PowerEstimate:
    """Two-sample t-test の検出 power (Cohen's d input)。

    power ≈ Φ(d · sqrt(harmonic_mean(n1, n2) / 4) - z_{α/2 or α}).
    """
    if n1 < 2 or n2 < 2:
        return PowerEstimate(
            test_family="t_test", n=int(n1 + n2), alpha=alpha,
            effect_size=float(effect_size_d), power=0.0,
            can_detect=False, target_power=target_power,
        )
    # Effective sample size (harmonic mean × 2 for two-sample equivalence)
    n_eff = 2.0 / (1.0 / n1 + 1.0 / n2)
    # critical z
    z_crit = _norm_isf(alpha / 2.0 if two_sided else alpha)
    # noncentrality
    delta = abs(effect_size_d) * sqrt(n_eff / 2.0)
    # power = 1 - Φ(z_crit - delta) + (two_sided: + Φ(-z_crit - delta))
    p = 1.0 - _norm_cdf(z_crit - delta)
    if two_sided:
        p += _norm_cdf(-z_crit - delta)
    p = max(0.0, min(1.0, p))
    return PowerEstimate(
        test_family="t_test", n=int(n1 + n2), alpha=alpha,
        effect_size=float(effect_size_d), power=float(p),
        can_detect=p >= target_power, target_power=target_power,
    )


def power_regression_coefficient(
    n: int, beta: float, se_beta: float,
    *, alpha: float = 0.05, target_power: float = 0.8, two_sided: bool = True,
) -> PowerEstimate:
    """Regression coefficient β ≠ 0 検定の power (β / SE_β input)。

    t-stat = β / SE。large n では z 近似。
    """
    if n < 3 or se_beta <= 0:
        return PowerEstimate(
            test_family="regression", n=int(n), alpha=alpha,
            effect_size=float(abs(beta / se_beta) if se_beta > 0 else 0.0),
            power=0.0, can_detect=False, target_power=target_power,
        )
    z_crit = _norm_isf(alpha / 2.0 if two_sided else alpha)
    es = abs(beta) / se_beta
    p = 1.0 - _norm_cdf(z_crit - es)
    if two_sided:
        p += _norm_cdf(-z_crit - es)
    p = max(0.0, min(1.0, p))
    return PowerEstimate(
        test_family="regression", n=int(n), alpha=alpha,
        effect_size=float(es), power=float(p),
        can_detect=p >= target_power, target_power=target_power,
    )


def power_correlation(
    n: int, r: float,
    *, alpha: float = 0.05, target_power: float = 0.8, two_sided: bool = True,
) -> PowerEstimate:
    """Pearson correlation r ≠ 0 検定の power (Fisher z transform)。

    z_r = atanh(r); var(z_r) = 1 / (n - 3).
    """
    if n < 4:
        return PowerEstimate(
            test_family="correlation", n=int(n), alpha=alpha,
            effect_size=float(r), power=0.0,
            can_detect=False, target_power=target_power,
        )
    # Fisher z
    from math import atanh
    z_r = atanh(min(0.9999, max(-0.9999, abs(r))))
    se = 1.0 / sqrt(n - 3)
    z_crit = _norm_isf(alpha / 2.0 if two_sided else alpha)
    delta = z_r / se
    p = 1.0 - _norm_cdf(z_crit - delta)
    if two_sided:
        p += _norm_cdf(-z_crit - delta)
    p = max(0.0, min(1.0, p))
    return PowerEstimate(
        test_family="correlation", n=int(n), alpha=alpha,
        effect_size=float(abs(r)), power=float(p),
        can_detect=p >= target_power, target_power=target_power,
    )


# ---------------------------------------------------------------------------
# MDE (Minimum Detectable Effect) — inverse problem
# ---------------------------------------------------------------------------


def mde_t_test_two_sample(
    n1: int, n2: int,
    *, alpha: float = 0.05, target_power: float = 0.8, two_sided: bool = True,
) -> float:
    """与えられた n / α / power で検出可能な最小 Cohen's d を返す。

    d = (z_{α/2} + z_{β}) · sqrt(2 / n_eff)
    """
    if n1 < 2 or n2 < 2:
        return float("inf")
    z_alpha = _norm_isf(alpha / 2.0 if two_sided else alpha)
    z_beta = _norm_isf(1.0 - target_power)
    n_eff = 2.0 / (1.0 / n1 + 1.0 / n2)
    return float((z_alpha + z_beta) * sqrt(2.0 / n_eff))


def mde_correlation(
    n: int, *, alpha: float = 0.05, target_power: float = 0.8, two_sided: bool = True,
) -> float:
    """検出可能な最小 |r|。Fisher z 逆変換。"""
    if n < 4:
        return 1.0
    z_alpha = _norm_isf(alpha / 2.0 if two_sided else alpha)
    z_beta = _norm_isf(1.0 - target_power)
    z_r = (z_alpha + z_beta) / sqrt(n - 3)
    # invert Fisher z: r = tanh(z_r)
    from math import tanh
    return float(tanh(z_r))


# ---------------------------------------------------------------------------
# Audit: scan a list of (report_name, test_spec) → power table
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PowerAuditRow:
    """1 report × test の power audit row。"""

    report_name: str
    test_label: str
    test_family: str
    n: int
    alpha: float
    target_power: float
    observed_effect: float
    power: float
    mde: float
    verdict: str       # "ok" | "underpowered" | "borderline"


def audit_report_power(
    audit_inputs: list[dict],
    *,
    target_power: float = 0.8,
    alpha: float = 0.05,
) -> list[PowerAuditRow]:
    """各 report の主要 test の power を一覧化。

    audit_inputs: list of dicts, each with keys:
        report_name (str): レポート名
        test_label (str): test の説明
        test_family ("t_test"|"regression"|"correlation"): test 種別
        n (int) or (n1, n2)
        observed_effect (float): 観測 effect size (d / β/SE / r)

    Returns:
        PowerAuditRow list.
    """
    rows: list[PowerAuditRow] = []
    for spec in audit_inputs:
        report_name = str(spec["report_name"])
        test_label = str(spec.get("test_label", "unspecified"))
        family = spec.get("test_family", "t_test")
        eff = float(spec.get("observed_effect", 0.0))

        if family == "t_test":
            n1 = int(spec.get("n1", spec.get("n", 0)))
            n2 = int(spec.get("n2", spec.get("n", 0)))
            est = power_t_test_two_sample(
                n1, n2, eff, alpha=alpha, target_power=target_power,
            )
            mde = mde_t_test_two_sample(n1, n2, alpha=alpha, target_power=target_power)
            total_n = n1 + n2
        elif family == "regression":
            n = int(spec.get("n", 0))
            beta = float(spec.get("beta", eff))
            se = float(spec.get("se_beta", 1.0))
            est = power_regression_coefficient(
                n, beta, se, alpha=alpha, target_power=target_power,
            )
            mde = (
                _norm_isf(alpha / 2.0) + _norm_isf(1.0 - target_power)
            )  # MDE in units of SE
            total_n = n
        elif family == "correlation":
            n = int(spec.get("n", 0))
            est = power_correlation(n, eff, alpha=alpha, target_power=target_power)
            mde = mde_correlation(n, alpha=alpha, target_power=target_power)
            total_n = n
        else:
            log.warning("power_audit_unknown_family", family=family)
            continue

        if est.power >= target_power:
            verdict = "ok"
        elif est.power >= target_power - 0.1:
            verdict = "borderline"
        else:
            verdict = "underpowered"

        rows.append(
            PowerAuditRow(
                report_name=report_name,
                test_label=test_label,
                test_family=family,
                n=int(total_n),
                alpha=alpha,
                target_power=target_power,
                observed_effect=float(eff),
                power=float(est.power),
                mde=float(mde),
                verdict=verdict,
            )
        )
    return rows

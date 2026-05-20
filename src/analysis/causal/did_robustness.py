"""DiD robustness checks — placebo, E-value, joint leads test.

既存 `did_studio_transfer.py` の estimate_did() / estimate_event_study() に対する
追加検証 3 種:

1. **Placebo test**: fake_event_year を引いて DiD 推定。
   p_value 非有意 = 真の treatment year でなくとも ATE 出る "noise" でないことを示す。
2. **E-value** (VanderWeele & Ding 2017): unobserved confounder の最小 strength。
   小さい E-value = 観測結果が confounding に弱い (= 因果解釈不安定)。
3. **Joint leads F-test**: event-study spec の leads (-3, -2, -1) が
   joint で 0 と区別不能か。parallel_trends の代替確認。

H1: anime.score 非依存。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Placebo test
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlaceboResult:
    """Placebo DiD: fake event_year に対する ATE 推定。"""

    n_placebo_runs: int
    placebo_ate_mean: float
    placebo_ate_sd: float
    placebo_p_below_observed: float   # |placebo| >= |observed| の比率
    observed_ate: float
    passes: bool                       # placebo |ATE| < observed |ATE| が >95%


def placebo_did(
    panel_y: np.ndarray,
    panel_treated: np.ndarray,
    panel_year: np.ndarray,
    panel_person: np.ndarray,
    real_event_year: int,
    *,
    observed_ate: float,
    placebo_year_offsets: Sequence[int] = (-5, -4, -3, 3, 4, 5),
) -> PlaceboResult:
    """偽 event_year で DiD を計算し、真 ATE と比較。

    各 placebo_year_offset を真の event_year に加算して fake event を作り、
    naive DiD 推定 (within-mean post - pre) を比較。

    Args:
        panel_y: 結果変数 array (1-row-per-person-year).
        panel_treated: 0/1 treated indicator (person-level fixed).
        panel_year: year of observation.
        panel_person: person id (str/int).
        real_event_year: 観測上の treatment 開始年。
        observed_ate: 真の estimate_did() の ATE point estimate。
        placebo_year_offsets: 真の event_year に加える offset list。

    Returns:
        PlaceboResult.
    """
    if panel_y.shape != panel_treated.shape or panel_y.shape != panel_year.shape:
        raise ValueError("panel arrays shape mismatch")

    ates: list[float] = []
    for off in placebo_year_offsets:
        fake_year = real_event_year + off
        post = panel_year >= fake_year
        treated_post = (panel_treated == 1) & post
        control_post = (panel_treated == 0) & post
        treated_pre = (panel_treated == 1) & ~post
        control_pre = (panel_treated == 0) & ~post

        if not (treated_post.any() and control_post.any()
                and treated_pre.any() and control_pre.any()):
            continue

        t_post = float(panel_y[treated_post].mean())
        t_pre = float(panel_y[treated_pre].mean())
        c_post = float(panel_y[control_post].mean())
        c_pre = float(panel_y[control_pre].mean())
        ate = (t_post - t_pre) - (c_post - c_pre)
        ates.append(ate)

    if not ates:
        return PlaceboResult(
            n_placebo_runs=0, placebo_ate_mean=0.0, placebo_ate_sd=0.0,
            placebo_p_below_observed=0.0, observed_ate=float(observed_ate),
            passes=False,
        )

    arr = np.array(ates, dtype=float)
    abs_obs = abs(float(observed_ate))
    n_exceed = int((np.abs(arr) >= abs_obs).sum())
    p_exceed = n_exceed / arr.size

    return PlaceboResult(
        n_placebo_runs=arr.size,
        placebo_ate_mean=float(arr.mean()),
        placebo_ate_sd=float(arr.std(ddof=1)) if arr.size > 1 else 0.0,
        placebo_p_below_observed=float(p_exceed),
        observed_ate=float(observed_ate),
        passes=(p_exceed < 0.05),
    )


# ---------------------------------------------------------------------------
# E-value (VanderWeele & Ding 2017)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EValueResult:
    """E-value: unobserved confounder の最小 strength。"""

    observed_rr: float           # observed risk ratio (or HR)
    e_value_point: float          # point estimate E-value
    e_value_ci: float             # CI bound E-value (lower bound if RR > 1, upper if < 1)
    interpretation: str


def compute_e_value(rr_point: float, ci_lower: float, ci_upper: float) -> EValueResult:
    """E-value 計算。Risk Ratio / Hazard Ratio 入力。

    Formula (VanderWeele 2017):
        E = RR + sqrt(RR * (RR - 1))    if RR > 1
        E = 1/RR + sqrt(1/RR * (1/RR - 1))  if RR < 1

    interpretation: 観測 RR を null (=1) に押し戻すために必要な
    unobserved confounder の strength。E-value が大きい = 因果解釈頑健。

    Args:
        rr_point: 観測 RR/HR の point estimate.
        ci_lower / ci_upper: 95% CI bounds (on RR scale).

    Returns:
        EValueResult.
    """
    def _e(rr: float) -> float:
        rr = max(rr, 1e-9)
        if rr >= 1:
            return rr + np.sqrt(rr * (rr - 1.0))
        # rr < 1: invert
        inv = 1.0 / rr
        return inv + np.sqrt(inv * (inv - 1.0))

    e_point = float(_e(rr_point))
    # CI bound nearest to null (1) determines E-value for CI
    if rr_point >= 1:
        # use lower CI (closer to null)
        bound = max(ci_lower, 1.0)
        e_ci = float(_e(bound)) if ci_lower > 1.0 else 1.0
    else:
        bound = min(ci_upper, 1.0)
        e_ci = float(_e(bound)) if ci_upper < 1.0 else 1.0

    if e_point >= 3.0:
        interp = "頑健: 観測 effect を null に押し戻すには強い unobserved confounder 必要"
    elif e_point >= 1.5:
        interp = "中程度: moderate confounder で null 化可能"
    else:
        interp = "脆弱: 弱い confounder で null 化可能、因果解釈慎重"

    return EValueResult(
        observed_rr=float(rr_point),
        e_value_point=e_point,
        e_value_ci=e_ci,
        interpretation=interp,
    )


def e_value_from_continuous(beta: float, se_beta: float, sd_y: float) -> EValueResult:
    """連続結果変数 β を擬似 RR に変換して E-value 計算。

    Chinn (2000) approximation: RR ≈ exp(0.91 × β × sd_y / sd_y) for continuous → binary proxy.
    本実装は **conservative approximation** (Cohen's d → RR 換算)。
    厳密な解釈には binary outcome での E-value 推奨。

    Args:
        beta: continuous coefficient.
        se_beta: SE of beta.
        sd_y: outcome SD (for d standardization).

    Returns:
        EValueResult based on approximate RR.
    """
    if sd_y <= 0:
        return EValueResult(
            observed_rr=1.0, e_value_point=1.0, e_value_ci=1.0,
            interpretation="sd_y=0 のため近似不可",
        )
    d = beta / sd_y          # Cohen's d
    d_lo = (beta - 1.96 * se_beta) / sd_y
    d_hi = (beta + 1.96 * se_beta) / sd_y
    # Approximate RR (Chinn 2000): RR ≈ exp(0.91 × d) for moderately rare outcome
    rr = float(np.exp(0.91 * d))
    rr_lo = float(np.exp(0.91 * d_lo))
    rr_hi = float(np.exp(0.91 * d_hi))
    return compute_e_value(rr, min(rr_lo, rr_hi), max(rr_lo, rr_hi))


# ---------------------------------------------------------------------------
# Joint leads test
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JointLeadsResult:
    """Pre-treatment leads (k < 0) joint test."""

    n_leads_tested: int
    leads_coefficients: tuple[float, ...]
    chi2_stat: float
    p_value: float
    parallel_trends_holds: bool


def joint_leads_test(
    leads_coefs: Sequence[float],
    leads_ses: Sequence[float],
    *,
    alpha: float = 0.05,
) -> JointLeadsResult:
    """Pre-treatment leads (k=-3, -2, -1 等) が全て 0 と区別不能かの joint test。

    Simplified Wald: each lead z = β_k / SE_k, joint chi2 = Σ z_k^2 (under indep).
    Independence 仮定は overstate 気味だが、conservative anchor として使用。

    Returns:
        JointLeadsResult with chi2 stat + p-value.
    """
    if len(leads_coefs) != len(leads_ses):
        raise ValueError("leads_coefs / leads_ses length mismatch")
    coefs = np.array(leads_coefs, dtype=float)
    ses = np.array(leads_ses, dtype=float)
    if coefs.size == 0:
        return JointLeadsResult(
            n_leads_tested=0, leads_coefficients=(),
            chi2_stat=0.0, p_value=1.0,
            parallel_trends_holds=True,
        )

    # Use safe SE (0 → 1e-9 to avoid divide error)
    ses_safe = np.where(ses > 0, ses, 1e-9)
    zs = coefs / ses_safe
    chi2 = float(np.sum(zs ** 2))

    try:
        from scipy.stats import chi2 as chi2_dist
        p = float(1.0 - chi2_dist.cdf(chi2, df=coefs.size))
    except ImportError:
        # Crude approximation
        p = float(np.exp(-chi2 / 2.0)) if coefs.size <= 1 else 0.0

    return JointLeadsResult(
        n_leads_tested=int(coefs.size),
        leads_coefficients=tuple(float(c) for c in coefs),
        chi2_stat=chi2,
        p_value=p,
        parallel_trends_holds=(p >= alpha),
    )

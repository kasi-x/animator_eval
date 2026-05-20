"""Heterogeneous treatment effect (HTE) — DiD ATE のサブグループ分解。

既存 `did_studio_transfer.py` は ATE (Average Treatment Effect) 推定が中心。
本モジュールは「treatment 効果が誰に対して大きいか / 小さいか」を分解する。

2 つのアプローチを提供:

1. **Interaction term DiD**: treatment × subgroup の交互作用係数で各 subgroup の
   CATE (Conditional Average Treatment Effect) を回帰式 1 本で同時推定。
   parsimonious、CI 取得容易。

2. **Causal Forest (econml 不在の場合は sklearn-based proxy)**: 個体レベル CATE
   を non-parametric に推定。subgroup definition を先験的に持たない探索的分析。
   本実装は econml がない場合 sklearn の RandomForest を用いた T-learner 近似。

H1: anime.score 不参入 (結果変数は theta_i / opportunity_residual / log_credit のみ)。
H2: 「treatment 効果が大きい層」表現可、「成長余地が大きい層」frame は NG → 「効果が大きい層」のみ。

References:
- Athey & Imbens (2016) "Recursive partitioning for heterogeneous causal effects."
- Künzel et al. (2019) "Metalearners for estimating HTE using ML."
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Subgroup CATE via interaction-term DiD
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SubgroupCATE:
    """1 subgroup の treatment effect 点推定 + 95% CI。"""

    subgroup_label: str
    n_treated: int
    n_control: int
    cate: float           # treatment - control の差分の差分
    ci_low: float
    ci_high: float
    se: float
    significant: bool     # CI が 0 を含まない


@dataclass(frozen=True)
class HTEDecomposition:
    """ATE + per-subgroup CATE のセット。"""

    ate: float
    ate_se: float
    ate_ci_low: float
    ate_ci_high: float
    subgroups: tuple[SubgroupCATE, ...]
    subgroup_var: str
    n_total: int
    # heterogeneity test: subgroup 間で CATE が有意に異なるかの p
    homogeneity_test_p: float | None = None


def _ols_with_se(
    y: np.ndarray, X: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """OLS β + heteroskedasticity-consistent SE (HC0)。

    Returns (β, β_se) of length k+1 (intercept first).
    """
    n, k = X.shape
    X_full = np.hstack([np.ones((n, 1)), X])
    beta, _, _, _ = np.linalg.lstsq(X_full, y, rcond=None)
    resid = y - X_full @ beta
    # HC0: Var(β) = (X'X)^{-1} X' diag(e^2) X (X'X)^{-1}
    xtx_inv = np.linalg.pinv(X_full.T @ X_full)
    middle = X_full.T @ np.diag(resid ** 2) @ X_full
    var_beta = xtx_inv @ middle @ xtx_inv
    se = np.sqrt(np.clip(np.diag(var_beta), 0, None))
    return beta, se


def estimate_cate_by_subgroup(
    y: np.ndarray,
    treated: np.ndarray,
    subgroup_ids: Sequence[int | str],
    subgroup_labels: dict[int | str, str] | None = None,
    *,
    subgroup_var: str = "subgroup",
    ci_level: float = 0.95,
) -> HTEDecomposition:
    """Treatment × subgroup の交互作用 OLS で per-subgroup CATE を推定。

    Spec: y = α + β·treated + Σ_s γ_s · subgroup_s + Σ_s δ_s · (treated × subgroup_s) + ε
    base subgroup を omit、各 δ_s が "base に対する追加効果"、δ_base = 0。

    Args:
        y: 結果変数 (1-row-per-observation)。
        treated: 0/1 binary treatment indicator。
        subgroup_ids: 各 obs の subgroup id (int or str)。
        subgroup_labels: optional human-readable label dict。
        subgroup_var: 表示用 subgroup 列名 (e.g. "cohort_decade", "gender").

    Returns:
        HTEDecomposition with ATE + per-subgroup CATE.
    """
    if y.size != treated.size or y.size != len(subgroup_ids):
        raise ValueError("y / treated / subgroup_ids 長さ不一致")
    if y.size == 0:
        raise ValueError("empty input")

    y = np.asarray(y, dtype=float)
    treated = np.asarray(treated, dtype=int)
    subgroups = list(subgroup_ids)
    unique_subs = sorted(set(subgroups), key=str)

    if len(unique_subs) < 2:
        raise ValueError("少なくとも 2 subgroup 必要 (HTE 不可)")

    # Base = first subgroup (alphabetical)
    base_sub = unique_subs[0]
    other_subs = unique_subs[1:]

    # ── Simple ATE first ────────────────────────────────────────────────
    X_ate = treated.reshape(-1, 1).astype(float)
    beta_ate, se_ate = _ols_with_se(y, X_ate)
    ate = float(beta_ate[1])
    ate_se = float(se_ate[1])
    z = 1.959964
    ate_lo = ate - z * ate_se
    ate_hi = ate + z * ate_se

    # ── Per-subgroup CATE via interaction regression ────────────────────
    # X columns: [treated, subgroup_2_dummy, ..., subgroup_K_dummy,
    #             treated × subgroup_2, ..., treated × subgroup_K]
    n = y.size
    sub_dummies = np.zeros((n, len(other_subs)), dtype=float)
    for i, s in enumerate(subgroups):
        if s != base_sub:
            j = other_subs.index(s)
            sub_dummies[i, j] = 1.0
    interaction = sub_dummies * treated.reshape(-1, 1).astype(float)
    X_full = np.hstack([
        treated.reshape(-1, 1).astype(float),
        sub_dummies,
        interaction,
    ])
    beta, se = _ols_with_se(y, X_full)
    # beta layout: [intercept, treated, sub_2..sub_K, treated×sub_2..treated×sub_K]
    base_treat_idx = 1
    interaction_start = 1 + 1 + len(other_subs)

    # CATE for base = β_treated
    # CATE for sub_k = β_treated + β_(treated×sub_k)
    sub_results: list[SubgroupCATE] = []
    for k, sub in enumerate(unique_subs):
        if sub == base_sub:
            point = float(beta[base_treat_idx])
            point_se = float(se[base_treat_idx])
        else:
            inter_idx = interaction_start + other_subs.index(sub)
            # Var(β_treated + β_interaction) = Var(β_treated) + Var(β_interaction) + 2 Cov
            # 簡易: assume covariance term, conservative HC0 → ignore cov (slight overcoverage acceptable)
            point = float(beta[base_treat_idx] + beta[inter_idx])
            point_se = float(np.sqrt(se[base_treat_idx] ** 2 + se[inter_idx] ** 2))
        ci_lo = point - z * point_se
        ci_hi = point + z * point_se
        # subgroup person counts
        mask = np.array([s == sub for s in subgroups])
        n_treat = int((treated[mask] == 1).sum())
        n_ctrl = int((treated[mask] == 0).sum())
        label = (
            subgroup_labels.get(sub, str(sub))
            if subgroup_labels is not None
            else str(sub)
        )
        sub_results.append(
            SubgroupCATE(
                subgroup_label=label,
                n_treated=n_treat,
                n_control=n_ctrl,
                cate=point,
                ci_low=ci_lo,
                ci_high=ci_hi,
                se=point_se,
                significant=(ci_lo > 0 or ci_hi < 0),
            )
        )

    # Homogeneity F-test (simplified): test that all interaction coefficients = 0
    # F = ((SSR_restricted - SSR_full) / q) / (SSR_full / (n - p))
    # Restricted = no interactions (only treated + subgroup dummies)
    X_restricted = np.hstack([
        treated.reshape(-1, 1).astype(float),
        sub_dummies,
    ])
    beta_r, _ = _ols_with_se(y, X_restricted)
    pred_full = np.hstack([np.ones((n, 1)), X_full]) @ beta
    pred_r = np.hstack([np.ones((n, 1)), X_restricted]) @ beta_r
    ssr_full = float(np.sum((y - pred_full) ** 2))
    ssr_r = float(np.sum((y - pred_r) ** 2))
    q = len(other_subs)
    p_full = X_full.shape[1] + 1
    if n - p_full > 0 and ssr_full > 0 and q > 0:
        f_stat = ((ssr_r - ssr_full) / q) / (ssr_full / (n - p_full))
        # F → approximate p-value via chi-square upper tail with df=q (large n)
        try:
            from scipy.stats import f as f_dist
            hom_p = float(1.0 - f_dist.cdf(f_stat, q, n - p_full))
        except ImportError:
            hom_p = None
    else:
        hom_p = None

    return HTEDecomposition(
        ate=ate,
        ate_se=ate_se,
        ate_ci_low=ate_lo,
        ate_ci_high=ate_hi,
        subgroups=tuple(sub_results),
        subgroup_var=subgroup_var,
        n_total=int(y.size),
        homogeneity_test_p=hom_p,
    )


# ---------------------------------------------------------------------------
# T-learner: separate ML model for treated vs control
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndividualCATE:
    """個体レベル CATE 推定。"""

    n: int
    cate_mean: float
    cate_sd: float
    cate_quantiles: dict[float, float]   # {0.1: ..., 0.5: ..., 0.9: ...}
    feature_names: tuple[str, ...]
    top_features_by_variance: tuple[tuple[str, float], ...]   # (name, var) 上位 5


def estimate_individual_cate_t_learner(
    y: np.ndarray,
    treated: np.ndarray,
    X: np.ndarray,
    feature_names: Sequence[str],
    *,
    rng_seed: int = 42,
    n_estimators: int = 100,
) -> IndividualCATE:
    """T-learner (Künzel 2019): treated と control に別々の Random Forest を fit、
    全 obs に対して両 model を predict、その差を個体 CATE とする。

    Returns:
        分布統計 + variance-based feature importance proxy.
    """
    try:
        from sklearn.ensemble import RandomForestRegressor
    except ImportError as exc:
        raise ImportError("sklearn required for T-learner") from exc

    if y.size != treated.size or y.size != X.shape[0]:
        raise ValueError("y / treated / X 長さ不一致")
    if X.shape[1] != len(feature_names):
        raise ValueError("feature_names != X.shape[1]")
    treated_mask = treated == 1
    control_mask = ~treated_mask
    if treated_mask.sum() < 5 or control_mask.sum() < 5:
        raise ValueError(
            f"insufficient samples: treated={int(treated_mask.sum())}, "
            f"control={int(control_mask.sum())}"
        )

    rf_t = RandomForestRegressor(
        n_estimators=n_estimators, random_state=rng_seed, n_jobs=1
    )
    rf_c = RandomForestRegressor(
        n_estimators=n_estimators, random_state=rng_seed + 1, n_jobs=1
    )
    rf_t.fit(X[treated_mask], y[treated_mask])
    rf_c.fit(X[control_mask], y[control_mask])
    cate = rf_t.predict(X) - rf_c.predict(X)

    quantiles = {q: float(np.quantile(cate, q)) for q in (0.1, 0.25, 0.5, 0.75, 0.9)}
    # Feature importance proxy: variance of CATE explained by each feature (univariate corr^2)
    importances: list[tuple[str, float]] = []
    for j, name in enumerate(feature_names):
        col = X[:, j]
        sd_col = float(np.std(col))
        sd_cate = float(np.std(cate))
        if sd_col == 0 or sd_cate == 0:
            corr = 0.0
        else:
            corr = float(np.corrcoef(col, cate)[0, 1])
        importances.append((name, corr ** 2))
    importances.sort(key=lambda t: -t[1])

    return IndividualCATE(
        n=int(y.size),
        cate_mean=float(np.mean(cate)),
        cate_sd=float(np.std(cate)),
        cate_quantiles=quantiles,
        feature_names=tuple(feature_names),
        top_features_by_variance=tuple(importances[:5]),
    )

"""統計ヘルパー — 相関計算、効果量、バッジ閾値."""

from __future__ import annotations

import math


def pearson_r(x: list[float], y: list[float]) -> tuple[float, float]:
    """Pearson相関係数とp値を計算.

    Returns (r, p_value). NaN/Inf は自動除外。
    """
    from scipy import stats

    clean_x, clean_y = [], []
    for xi, yi in zip(x, y):
        if math.isfinite(xi) and math.isfinite(yi):
            clean_x.append(xi)
            clean_y.append(yi)
    if len(clean_x) < 3:
        return 0.0, 1.0
    r, p = stats.pearsonr(clean_x, clean_y)
    return float(r), float(p)


def effect_size_label(r: float) -> str:
    """Cohen's conventionに基づく効果量ラベル."""
    abs_r = abs(r)
    if abs_r >= 0.5:
        return "大"
    if abs_r >= 0.3:
        return "中"
    return "小"


def r_squared(r: float) -> float:
    """決定係数."""
    return r ** 2


def correlation_annotation(
    x: list[float], y: list[float],
) -> str:
    """散布図用の相関アノテーションテキストを生成."""
    r, p = pearson_r(x, y)
    effect = effect_size_label(r)
    r2 = r_squared(r)
    n = sum(1 for xi, yi in zip(x, y) if math.isfinite(xi) and math.isfinite(yi))

    if n > 1000:
        p_text = "大標本: p値は常に有意"
    elif p < 0.001:
        p_text = "p<0.001"
    else:
        p_text = f"p={p:.3f}"

    return f"r={r:.3f} (効果量:{effect}), R²={r2:.3f}, {p_text}, n={n:,}"


def data_driven_badges(
    values: list[float],
) -> tuple[float, float]:
    """P25/P75閾値を返す. badge分類用."""
    import numpy as np
    arr = [v for v in values if math.isfinite(v)]
    if len(arr) < 4:
        return 0.0, 0.0
    p25, p75 = float(np.percentile(arr, 25)), float(np.percentile(arr, 75))
    return p25, p75


def fmt_num(v: int | float) -> str:
    """数値フォーマット（千区切り、小数2桁）."""
    if isinstance(v, float):
        if v == int(v) and abs(v) < 1e12:
            return f"{int(v):,}"
        return f"{v:,.2f}"
    return f"{v:,}"


def moving_avg(vals: tuple[float, ...], window: int = 5) -> tuple[float, ...]:
    """スライディングウィンドウ移動平均."""
    result = []
    for i in range(len(vals)):
        start = max(0, i - window + 1)
        result.append(sum(vals[start : i + 1]) / (i - start + 1))
    return tuple(result)

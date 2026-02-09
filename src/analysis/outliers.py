"""外れ値検出 — スコア分布における統計的外れ値を特定する.

IQR 法と Z-score 法の両方を使用して、
極端に高い/低いスコアの人物を検出する。
"""

import structlog

logger = structlog.get_logger()


def detect_outliers(
    results: list[dict],
    axes: tuple[str, ...] = ("authority", "trust", "skill", "composite"),
    iqr_multiplier: float = 1.5,
    zscore_threshold: float = 2.5,
) -> dict:
    """スコア分布における外れ値を検出する.

    Args:
        results: スコア結果リスト ({person_id, authority, trust, ...})
        axes: 検査する軸
        iqr_multiplier: IQR 外れ値判定の倍数 (default: 1.5)
        zscore_threshold: Z-score の閾値 (default: 2.5)

    Returns:
        {
            "axis_outliers": {axis: {"high": [...], "low": [...]}},
            "total_outliers": int,
            "outlier_person_ids": [str],
        }
    """
    if len(results) < 5:
        return {
            "axis_outliers": {},
            "total_outliers": 0,
            "outlier_person_ids": [],
        }

    axis_outliers: dict[str, dict[str, list]] = {}
    all_outlier_ids: set[str] = set()

    for axis in axes:
        values = [r.get(axis, 0) for r in results]
        if not values:
            continue

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[(3 * n) // 4]
        iqr = q3 - q1

        lower_bound = q1 - iqr_multiplier * iqr
        upper_bound = q3 + iqr_multiplier * iqr

        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = variance ** 0.5 if variance > 0 else 0.0

        high_outliers = []
        low_outliers = []

        for r in results:
            val = r.get(axis, 0)
            pid = r.get("person_id", "")
            name = r.get("name", "") or r.get("name_ja", "") or pid

            is_iqr_outlier = val > upper_bound or val < lower_bound
            zscore = (val - mean) / std if std > 0 else 0
            is_zscore_outlier = abs(zscore) > zscore_threshold

            if is_iqr_outlier or is_zscore_outlier:
                entry = {
                    "person_id": pid,
                    "name": name,
                    "value": round(val, 2),
                    "zscore": round(zscore, 2),
                    "iqr_outlier": is_iqr_outlier,
                    "zscore_outlier": is_zscore_outlier,
                }
                if val > upper_bound or zscore > zscore_threshold:
                    high_outliers.append(entry)
                else:
                    low_outliers.append(entry)
                all_outlier_ids.add(pid)

        high_outliers.sort(key=lambda x: x["value"], reverse=True)
        low_outliers.sort(key=lambda x: x["value"])

        axis_outliers[axis] = {
            "high": high_outliers,
            "low": low_outliers,
            "bounds": {
                "iqr_lower": round(lower_bound, 2),
                "iqr_upper": round(upper_bound, 2),
                "mean": round(mean, 2),
                "std": round(std, 2),
            },
        }

    result = {
        "axis_outliers": axis_outliers,
        "total_outliers": len(all_outlier_ids),
        "outlier_person_ids": sorted(all_outlier_ids),
    }

    logger.info(
        "outliers_detected",
        total=len(all_outlier_ids),
        axes=len(axes),
    )
    return result

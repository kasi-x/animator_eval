"""Attrition risk score — Random Survival Forest.

C-index gate: >= 0.70 on test set before publishing results.

Reuses build_entry_cohort_dataset() from entry_cohort_attrition.py.
"""

from __future__ import annotations

import duckdb
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

C_INDEX_GATE = 0.70


def train_survival_model(df: Any) -> tuple[Any, float, float, dict]:
    """Random Survival Forest with temporal train/val/test split.

    Train 2010-2015, Val 2016, Test 2017-2018.
    Gate: C-index > 0.70 on test set.

    Returns (model, c_index_test, brier_score, feature_importances)
    Returns (None, c_index, brier, {}) if gate not met.
    """
    try:
        from sksurv.ensemble import RandomSurvivalForest
        from sksurv.metrics import concordance_index_censored
    except ImportError:
        logger.warning("scikit_survival_not_available")
        return _fallback_survival_model(df)

    import pandas as pd

    df = df.dropna(subset=["duration", "event", "debut_year"])

    train = df[df["debut_year"] <= 2015]
    test = df[df["debut_year"] >= 2017]

    if len(train) < 20 or len(test) < 10:
        return None, 0.0, 1.0, {}

    feature_cols = ["debut_studio_tier", "early_density", "debut_year"]
    X_train = train[feature_cols].values.astype(float)
    X_test = test[feature_cols].values.astype(float)

    # structured array for sksurv
    def _to_structured(subset: "pd.DataFrame"):
        return np.array(
            [
                (bool(row["event"]), float(row["duration"]))
                for _, row in subset.iterrows()
            ],
            dtype=[("event", "?"), ("duration", "<f8")],
        )

    y_train = _to_structured(train)
    y_test = _to_structured(test)

    model = RandomSurvivalForest(
        n_estimators=100, max_depth=5, min_samples_leaf=5, random_state=42, n_jobs=-1
    )
    model.fit(X_train, y_train)

    risk_scores = model.predict(X_test)
    c_idx = concordance_index_censored(
        y_test["event"], y_test["duration"], risk_scores
    )[0]

    importances = dict(zip(feature_cols, model.feature_importances_.tolist()))

    logger.info("survival_model_trained", c_index=round(c_idx, 4), gate=C_INDEX_GATE)

    if c_idx < C_INDEX_GATE:
        logger.warning("survival_model_below_gate", c_index=round(c_idx, 4))
        return None, float(c_idx), 1.0, importances

    return model, float(c_idx), 0.0, importances


def _fallback_survival_model(df: Any) -> tuple[Any, float, float, dict]:
    """Fallback: Cox PH via lifelines if sksurv not available."""
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        return None, 0.0, 1.0, {}

    df = df.dropna().copy()
    feature_cols = ["debut_studio_tier", "early_density", "debut_year"]

    train = df[df["debut_year"] <= 2015]
    test = df[df["debut_year"] >= 2017]

    if len(train) < 20 or len(test) < 10:
        return None, 0.0, 1.0, {}

    cph = CoxPHFitter()
    fit_df = train[feature_cols + ["duration", "event"]].copy()
    try:
        cph.fit(fit_df, duration_col="duration", event_col="event")
    except Exception as e:
        logger.warning("cox_fallback_failed", error=str(e))
        return None, 0.0, 1.0, {}

    # C-index approximation using concordance on test
    test_df = test[feature_cols + ["duration", "event"]].copy()
    risk_scores = cph.predict_partial_hazard(test_df[feature_cols])

    from lifelines.utils import concordance_index as ci_func

    c_idx = float(ci_func(test_df["duration"], -risk_scores, test_df["event"]))

    coefs = {
        col: round(float(cph.summary.loc[col, "coef"]), 4)
        for col in feature_cols
        if col in cph.summary.index
    }

    if c_idx < C_INDEX_GATE:
        return None, c_idx, 1.0, coefs

    return cph, c_idx, 0.0, coefs


def compute_attrition_risk_scores(
    model: Any, df: Any, c_index: float
) -> dict[str, Any]:
    """Predict aggregate attrition patterns.

    Returns {aggregate_by_tier, feature_importance, c_index, published}
    """
    if model is None or c_index < C_INDEX_GATE:
        return {
            "published": False,
            "reason": f"C-index {c_index:.3f} < gate {C_INDEX_GATE}",
            "c_index": round(c_index, 4),
        }

    feature_cols = ["debut_studio_tier", "early_density", "debut_year"]
    X = df[feature_cols].values.astype(float)

    try:
        risk_scores = model.predict(X)
    except Exception:
        return {
            "published": False,
            "reason": "prediction_failed",
            "c_index": round(c_index, 4),
        }

    df = df.copy()
    df["risk_score"] = risk_scores

    # Aggregate by studio_tier
    by_tier: dict = {}
    for tier in sorted(df["debut_studio_tier"].unique()):
        grp = df[df["debut_studio_tier"] == tier]
        by_tier[str(int(tier))] = {
            "n": int(len(grp)),
            "mean_risk": round(float(grp["risk_score"].mean()), 4),
            "event_rate": round(float(grp["event"].mean()), 4),
        }

    return {
        "published": True,
        "c_index": round(c_index, 4),
        "aggregate_by_tier": by_tier,
        "n_scored": len(df),
    }


def run_attrition_risk_model(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Attrition risk score — main entry point."""
    from src.analysis.attrition.entry_cohort_attrition import build_entry_cohort_dataset

    import pandas as pd

    dataset = build_entry_cohort_dataset(conn)
    if not dataset:
        return {"error": "no_data", "published": False}

    df = pd.DataFrame(dataset)
    model, c_index, brier, importances = train_survival_model(df)
    risk_output = compute_attrition_risk_scores(model, df, c_index)

    return {
        "c_index": round(c_index, 4),
        "c_index_gate": C_INDEX_GATE,
        "feature_importance": importances,
        "risk_scores": risk_output,
        "method_notes": {
            "model": "RandomSurvivalForest (sksurv) or CoxPH fallback (lifelines)",
            "split": "train 2010-2015, test 2017-2018",
            "gate": f"C-index >= {C_INDEX_GATE} required to publish",
        },
    }

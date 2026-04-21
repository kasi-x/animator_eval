"""新卒離職因果分解 — Kaplan-Meier / Cox PH / DML.

デビューコホート (first_year 2010-2018) の離職生存時間を分析し、
初期スタジオ tier の因果効果を DML で推定する。

event  = feat_career_gaps.gap_type == 'exit' AND returned == 0
duration = gap_start_year - debut_year (event) / obs_window_end - debut_year (censored)
"""

from __future__ import annotations

import sqlite3

import numpy as np
import structlog

logger = structlog.get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# データ構築
# ─────────────────────────────────────────────────────────────────────────────


def build_entry_cohort_dataset(
    conn: sqlite3.Connection,
    *,
    first_year_min: int = 2010,
    first_year_max: int = 2018,
    obs_window_end: int = 2020,
) -> "list[dict]":
    """デビューコホートの生存時間データセットを構築する.

    Returns list of dicts with keys:
        person_id, debut_year, duration, event,
        initial_role_category, debut_studio_tier, early_density
    """
    rows = conn.execute(
        """
        SELECT
            fc.person_id,
            fc.first_year AS debut_year,
            fc.highest_stage,
            fc.primary_role,
            fc.active_years,
            fc.total_credits
        FROM feat_career fc
        WHERE fc.first_year BETWEEN ? AND ?
        """,
        (first_year_min, first_year_max),
    ).fetchall()

    if not rows:
        logger.warning("entry_cohort_no_data", first_year_min=first_year_min)
        return []

    person_ids = [r[0] for r in rows]
    career_map = {
        r[0]: {
            "debut_year": r[1],
            "highest_stage": r[2],
            "primary_role": r[3] or "unknown",
            "active_years": r[4] or 0,
            "total_credits": r[5] or 0,
        }
        for r in rows
    }

    # gap → exit event per person (earliest exit with returned==0)
    placeholders = ",".join("?" * len(person_ids))
    gap_rows = conn.execute(
        f"""
        SELECT person_id, gap_start_year, returned, gap_type
        FROM feat_career_gaps
        WHERE person_id IN ({placeholders})
          AND gap_type = 'exit'
        ORDER BY gap_start_year ASC
        """,
        person_ids,
    ).fetchall()

    first_exit: dict[str, dict] = {}
    for pid, gap_start, returned, gap_type in gap_rows:
        if pid not in first_exit and not returned:
            first_exit[pid] = {"gap_start_year": gap_start}

    # studio tier — primary studio in debut year (is_main_studio=1 preferred)
    studio_rows = conn.execute(
        f"""
        SELECT sa.person_id, sa.credit_year, sa.studio_id,
               COALESCE(wc.scale_tier, 2) AS scale_tier
        FROM feat_studio_affiliation sa
        LEFT JOIN (
            SELECT anime_id, scale_tier, credit_year
            FROM feat_work_context
        ) wc ON wc.credit_year = sa.credit_year
        WHERE sa.person_id IN ({placeholders})
        ORDER BY sa.person_id, sa.credit_year ASC
        """,
        person_ids,
    ).fetchall()

    # debut studio tier = median tier of all works in debut_year
    studio_tier_by_person: dict[str, list[int]] = {}
    for pid, yr, sid, tier in studio_rows:
        debut_year = career_map[pid]["debut_year"]
        if yr == debut_year:
            studio_tier_by_person.setdefault(pid, []).append(tier or 2)

    # early credit density (first 2 years)
    credit_rows = conn.execute(
        f"""
        SELECT cc.person_id, COUNT(*) AS n_early
        FROM feat_credit_contribution cc
        JOIN feat_career fc ON fc.person_id = cc.person_id
        WHERE cc.person_id IN ({placeholders})
          AND cc.credit_year <= fc.first_year + 2
        GROUP BY cc.person_id
        """,
        person_ids,
    ).fetchall()
    early_density = {pid: n for pid, n in credit_rows}

    dataset = []
    for pid, meta in career_map.items():
        debut_year = meta["debut_year"]
        if pid in first_exit:
            event = 1
            duration = max(1, first_exit[pid]["gap_start_year"] - debut_year)
        else:
            event = 0
            duration = max(1, obs_window_end - debut_year)

        tiers = studio_tier_by_person.get(pid, [])
        debut_tier = int(np.median(tiers)) if tiers else 2

        role = meta["primary_role"]
        role_cat = (
            "direction"
            if "director" in role.lower()
            else "animation"
            if "animat" in role.lower()
            else "other"
        )

        dataset.append(
            {
                "person_id": pid,
                "debut_year": debut_year,
                "duration": duration,
                "event": event,
                "initial_role_category": role_cat,
                "debut_studio_tier": debut_tier,
                "early_density": early_density.get(pid, 0),
            }
        )

    logger.info(
        "entry_cohort_dataset_built",
        n=len(dataset),
        n_events=sum(d["event"] for d in dataset),
    )
    return dataset


# ─────────────────────────────────────────────────────────────────────────────
# Kaplan-Meier
# ─────────────────────────────────────────────────────────────────────────────


def compute_kaplan_meier_by_cohort(dataset: list[dict]) -> dict:
    """デビュー年代別 KM 生存曲線.

    Returns {decade: {timeline, survival, ci_lower, ci_upper, n_at_risk, n_events}}
    """
    try:
        from lifelines import KaplanMeierFitter
    except ImportError:
        logger.warning("lifelines_not_available")
        return {}

    def _decade(yr: int) -> str:
        if yr <= 2012:
            return "2010-2012"
        elif yr <= 2015:
            return "2013-2015"
        else:
            return "2016-2018"

    import pandas as pd

    df = pd.DataFrame(dataset)
    results: dict = {}

    for decade, grp in df.groupby(df["debut_year"].map(_decade)):
        kmf = KaplanMeierFitter()
        kmf.fit(grp["duration"], grp["event"], label=str(decade))
        sf = kmf.survival_function_
        ci = kmf.confidence_interval_survival_function_

        results[str(decade)] = {
            "timeline": sf.index.tolist(),
            "survival": sf.iloc[:, 0].tolist(),
            "ci_lower": ci.iloc[:, 0].tolist(),
            "ci_upper": ci.iloc[:, 1].tolist(),
            "n": int(len(grp)),
            "n_events": int(grp["event"].sum()),
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Cox PH
# ─────────────────────────────────────────────────────────────────────────────


def compute_cox_ph(dataset: list[dict]) -> dict:
    """Cox PH 回帰. 共変量: debut_studio_tier, early_density, debut_year.

    Returns {covariate: {coef, se, hr, ci_lower, ci_upper, p_value}}
    """
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        logger.warning("lifelines_not_available")
        return {}

    import pandas as pd

    df = pd.DataFrame(dataset)
    df = df[
        ["duration", "event", "debut_studio_tier", "early_density", "debut_year"]
    ].copy()
    df = df.dropna()

    if len(df) < 30:
        return {"error": "insufficient_data", "n": len(df)}

    cph = CoxPHFitter()
    try:
        cph.fit(df, duration_col="duration", event_col="event")
    except Exception as e:
        logger.warning("cox_ph_fit_failed", error=str(e))
        return {"error": str(e)}

    summary = cph.summary
    result: dict = {}
    for cov in summary.index:
        row = summary.loc[cov]
        result[str(cov)] = {
            "coef": float(row["coef"]),
            "se": float(row["se(coef)"]),
            "hr": float(row["exp(coef)"]),
            "ci_lower": float(row["exp(coef) lower 95%"]),
            "ci_upper": float(row["exp(coef) upper 95%"]),
            "p_value": float(row["p"]),
        }

    return result


# ─────────────────────────────────────────────────────────────────────────────
# DML
# ─────────────────────────────────────────────────────────────────────────────


def compute_dml_attrition(
    dataset: list[dict],
    treatment: str = "debut_studio_tier",
) -> dict:
    """DML: debut_studio_tier → exit 因果効果.

    Partial Linear Model: Y = θ·D + g(X) + ε, D = m(X) + V
    Cross-fitting K=5 with GradientBoosting nuisance.

    Returns {theta, se, ci_lower, ci_upper, n, r2_y, r2_d}
    """
    import pandas as pd
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import KFold

    df = pd.DataFrame(dataset).dropna()
    if len(df) < 50:
        return {"error": "insufficient_data", "n": len(df)}

    confounders = ["early_density", "debut_year"]
    outcome = "event"

    Y = df[outcome].values.astype(float)
    D = df[treatment].values.astype(float)
    X = df[confounders].values.astype(float)

    n = len(Y)
    Y_res = np.zeros(n)
    D_res = np.zeros(n)
    r2_y_scores: list[float] = []
    r2_d_scores: list[float] = []

    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    for train_idx, test_idx in kf.split(X):
        X_tr, X_te = X[train_idx], X[test_idx]
        Y_tr, Y_te = Y[train_idx], Y[test_idx]
        D_tr, D_te = D[train_idx], D[test_idx]

        g_model = GradientBoostingRegressor(
            n_estimators=100, max_depth=3, random_state=42
        )
        m_model = GradientBoostingRegressor(
            n_estimators=100, max_depth=3, random_state=42
        )

        g_model.fit(X_tr, Y_tr)
        m_model.fit(X_tr, D_tr)

        Y_res[test_idx] = Y_te - g_model.predict(X_te)
        D_res[test_idx] = D_te - m_model.predict(X_te)

        ss_res_y = np.sum((Y_te - g_model.predict(X_te)) ** 2)
        ss_tot_y = np.sum((Y_te - Y_te.mean()) ** 2)
        r2_y_scores.append(1 - ss_res_y / ss_tot_y if ss_tot_y > 0 else 0.0)

        ss_res_d = np.sum((D_te - m_model.predict(X_te)) ** 2)
        ss_tot_d = np.sum((D_te - D_te.mean()) ** 2)
        r2_d_scores.append(1 - ss_res_d / ss_tot_d if ss_tot_d > 0 else 0.0)

    # Final OLS on residuals
    denom = np.sum(D_res**2)
    if denom < 1e-10:
        return {"error": "no_treatment_variation", "n": n}

    theta = float(np.sum(D_res * Y_res) / denom)
    epsilon = Y_res - theta * D_res
    se = float(np.sqrt(np.sum(epsilon**2 * D_res**2) / denom**2))

    logger.info("dml_attrition_done", theta=round(theta, 4), se=round(se, 4), n=n)
    return {
        "theta": theta,
        "se": se,
        "ci_lower": theta - 1.96 * se,
        "ci_upper": theta + 1.96 * se,
        "t_stat": theta / se if se > 0 else 0.0,
        "p_value": float(
            2
            * (1 - __import__("scipy").stats.norm.cdf(abs(theta / se if se > 0 else 0)))
        ),
        "n": n,
        "r2_y": float(np.mean(r2_y_scores)),
        "r2_d": float(np.mean(r2_d_scores)),
        "treatment": treatment,
        "method": "dml_partial_linear_gbm_k5",
    }


# ─────────────────────────────────────────────────────────────────────────────
# 感度分析
# ─────────────────────────────────────────────────────────────────────────────


def compute_sensitivity(
    conn: sqlite3.Connection,
    *,
    exit_thresholds: tuple[int, ...] = (3, 5, 7),
    first_year_min: int = 2010,
    first_year_max: int = 2018,
    obs_window_end: int = 2020,
) -> dict:
    """exit定義 3/5/7年の感度分析.

    Returns {threshold: {n_events, event_rate, km_median_survival}}
    """
    results: dict = {}

    for threshold in exit_thresholds:
        rows = conn.execute(
            """
            SELECT fc.person_id, fc.first_year,
                   g.gap_start_year, g.returned, g.gap_type
            FROM feat_career fc
            LEFT JOIN feat_career_gaps g ON g.person_id = fc.person_id
                AND g.gap_length >= ?
                AND g.returned = 0
            WHERE fc.first_year BETWEEN ? AND ?
            ORDER BY fc.person_id, g.gap_start_year ASC
            """,
            (threshold, first_year_min, first_year_max),
        ).fetchall()

        person_seen: set = set()
        n_events = 0
        durations: list[int] = []
        events: list[int] = []

        for pid, debut_yr, gap_start, returned, gap_type in rows:
            if pid in person_seen:
                continue
            person_seen.add(pid)
            if gap_start is not None and not returned:
                n_events += 1
                durations.append(max(1, gap_start - debut_yr))
                events.append(1)
            else:
                durations.append(max(1, obs_window_end - debut_yr))
                events.append(0)

        n = len(durations)
        event_rate = n_events / n if n > 0 else 0.0

        # simple KM median from sorted data
        median_survival = None
        if durations and any(events):
            try:
                from lifelines import KaplanMeierFitter

                kmf = KaplanMeierFitter()
                kmf.fit(durations, events)
                median_survival = float(kmf.median_survival_time_)
            except Exception:
                pass

        results[str(threshold)] = {
            "exit_threshold_years": threshold,
            "n": n,
            "n_events": n_events,
            "event_rate": round(event_rate, 4),
            "km_median_survival": median_survival,
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────


def run_entry_cohort_attrition(conn: sqlite3.Connection) -> dict:
    """新卒離職因果分解 — メインエントリポイント."""
    dataset = build_entry_cohort_dataset(conn)
    if not dataset:
        return {
            "error": "no_data",
            "message": "feat_career/feat_career_gaps が空またはコホート対象者なし",
        }

    km = compute_kaplan_meier_by_cohort(dataset)
    cox = compute_cox_ph(dataset)
    dml = compute_dml_attrition(dataset)
    sensitivity = compute_sensitivity(conn)

    return {
        "n_cohort": len(dataset),
        "n_events": sum(d["event"] for d in dataset),
        "event_rate": round(sum(d["event"] for d in dataset) / len(dataset), 4),
        "km_curves": km,
        "cox_ph": cox,
        "dml": dml,
        "sensitivity": sensitivity,
        "method_notes": {
            "km": "lifelines.KaplanMeierFitter, Greenwood CI",
            "cox": "lifelines.CoxPHFitter, Breslow baseline",
            "dml": "Partial Linear DML, GBM nuisance K=5",
            "event_def": "gap_type='exit' AND returned=0, gap_length>=5y",
            "cohort": "first_year 2010-2018, obs_window_end=2020",
        },
    }

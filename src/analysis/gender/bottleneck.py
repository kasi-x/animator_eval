"""Gender bottleneck analysis — promotion KM / Oaxaca-Blinder / studio FE.

注意: gender データは欠損率が高い。
データ品質の制約を必ず結果に含める。
"""

from __future__ import annotations

import sqlite3
from typing import Any

import structlog

logger = structlog.get_logger()


# ─────────────────────────────────────────────────────────────────────────────
# KM by stage transition
# ─────────────────────────────────────────────────────────────────────────────


def compute_gender_survival_by_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """KM survival by stage transition (gender comparison) + log-rank test.

    Returns {transition: {F: {timeline, survival, ci}, M: {}, logrank_p, n_F, n_M}}
    """
    try:
        from lifelines import KaplanMeierFitter
        from lifelines.statistics import logrank_test
    except ImportError:
        logger.warning("lifelines_not_available")
        return {}

    import pandas as pd

    rows = conn.execute(
        """
        SELECT p.id AS person_id, p.gender,
               fc.first_year, fc.latest_year, fc.highest_stage, fc.active_years
        FROM persons p
        JOIN feat_career fc ON fc.person_id = p.id
        WHERE p.gender IN ('F', 'M')
        """
    ).fetchall()

    if not rows:
        return {
            "error": "no_gender_data",
            "data_quality_note": "persons.gender の記録率が低い — 結果の代表性を確認してください",
        }

    df = pd.DataFrame(
        rows,
        columns=[
            "person_id",
            "gender",
            "first_year",
            "latest_year",
            "highest_stage",
            "active_years",
        ],
    )

    total_persons = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    n_with_gender = len(df)

    results: dict = {
        "data_quality": {
            "total_persons": total_persons,
            "persons_with_gender": n_with_gender,
            "coverage_pct": round(100 * n_with_gender / total_persons, 1)
            if total_persons
            else 0,
            "note": "gender 欠損率が高い場合、サンプリングバイアスに注意",
        }
    }

    # Survival to reach highest_stage >= threshold
    for stage_threshold in [2, 3, 4]:
        grp_f = df[df["gender"] == "F"].copy()
        grp_m = df[df["gender"] == "M"].copy()

        # event = reached stage >= threshold, duration = years until event or censoring
        def _build(grp: "pd.DataFrame") -> tuple[list, list]:
            durs, evts = [], []
            for _, row in grp.iterrows():
                reached = int(row["highest_stage"] >= stage_threshold)
                if reached:
                    dur = max(
                        1, (row["latest_year"] or row["first_year"]) - row["first_year"]
                    )
                else:
                    dur = max(1, row["active_years"] or 1)
                durs.append(dur)
                evts.append(reached)
            return durs, evts

        if len(grp_f) < 5 or len(grp_m) < 5:
            continue

        dur_f, evt_f = _build(grp_f)
        dur_m, evt_m = _build(grp_m)

        kmf_f = KaplanMeierFitter()
        kmf_m = KaplanMeierFitter()
        kmf_f.fit(dur_f, evt_f)
        kmf_m.fit(dur_m, evt_m)

        try:
            lr = logrank_test(
                dur_f, dur_m, event_observed_A=evt_f, event_observed_B=evt_m
            )
            p_value = float(lr.p_value)
        except Exception:
            p_value = None

        def _km_dict(kmf: KaplanMeierFitter) -> dict:
            sf = kmf.survival_function_
            ci = kmf.confidence_interval_survival_function_
            return {
                "timeline": sf.index.tolist(),
                "survival": sf.iloc[:, 0].tolist(),
                "ci_lower": ci.iloc[:, 0].tolist(),
                "ci_upper": ci.iloc[:, 1].tolist(),
            }

        key = f"stage_0_to_{stage_threshold}"
        results[key] = {
            "F": _km_dict(kmf_f),
            "M": _km_dict(kmf_m),
            "logrank_p": p_value,
            "n_F": len(grp_f),
            "n_M": len(grp_m),
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Oaxaca-Blinder 分解
# ─────────────────────────────────────────────────────────────────────────────


def compute_promotion_gap_oaxaca(conn: sqlite3.Connection) -> dict[str, Any]:
    """Oaxaca-Blinder 分解: 昇進率ギャップの説明・未説明成分.

    outcome: highest_stage >= 3 (binary)
    covariates: active_years, total_credits, first_year (cohort)
    """
    from sklearn.linear_model import LogisticRegression
    import pandas as pd

    rows = conn.execute(
        """
        SELECT p.gender, fc.highest_stage, fc.active_years,
               fc.total_credits, fc.first_year
        FROM persons p
        JOIN feat_career fc ON fc.person_id = p.id
        WHERE p.gender IN ('F', 'M')
        """
    ).fetchall()

    if not rows:
        return {"error": "no_gender_data"}

    df = pd.DataFrame(
        rows,
        columns=[
            "gender",
            "highest_stage",
            "active_years",
            "total_credits",
            "first_year",
        ],
    )
    df = df.dropna()

    outcome_threshold = 3
    df["promoted"] = (df["highest_stage"] >= outcome_threshold).astype(int)

    feats = ["active_years", "total_credits", "first_year"]
    X = df[feats].values.astype(float)
    Y = df["promoted"].values.astype(int)
    is_F = (df["gender"] == "F").values

    X_f, Y_f = X[is_F], Y[is_F]
    X_m, Y_m = X[~is_F], Y[~is_F]

    if len(X_f) < 10 or len(X_m) < 10:
        return {
            "error": "insufficient_gendered_data",
            "n_F": int(is_F.sum()),
            "n_M": int((~is_F).sum()),
        }

    # Pooled model
    model_pool = LogisticRegression(max_iter=500)
    model_pool.fit(X, Y)

    # Group-specific means
    mean_f = X_f.mean(axis=0)
    mean_m = X_m.mean(axis=0)

    raw_gap = float(Y_f.mean() - Y_m.mean())

    # Explained: difference in characteristics evaluated at pooled coefs
    # Using linear approximation: coef · (mean_F - mean_M)
    coefs = model_pool.coef_[0]
    explained_components = {
        feat: round(float(coefs[i] * (mean_f[i] - mean_m[i])), 4)
        for i, feat in enumerate(feats)
    }
    explained = sum(explained_components.values())
    unexplained = raw_gap - explained

    return {
        "raw_gap": round(raw_gap, 4),
        "explained": round(explained, 4),
        "unexplained": round(unexplained, 4),
        "explained_fraction": round(explained / raw_gap, 4)
        if abs(raw_gap) > 0.001
        else None,
        "components": explained_components,
        "n_F": int(is_F.sum()),
        "n_M": int((~is_F).sum()),
        "outcome": f"highest_stage >= {outcome_threshold}",
        "method_note": "Oaxaca-Blinder using pooled logistic, linear approximation",
    }


# ─────────────────────────────────────────────────────────────────────────────
# studio gender fixed effects
# ─────────────────────────────────────────────────────────────────────────────


def compute_studio_gender_fe(conn: sqlite3.Connection) -> dict[str, Any]:
    """Studio-level gender interaction — Cox model (simplified studio FE).

    For each studio: compare promotion rate gap F vs M after controlling for
    cohort and credits. Returns top/bottom studios by gamma_j.
    """
    rows = conn.execute(
        """
        SELECT p.gender, fc.person_id, fc.highest_stage, fc.active_years,
               fc.first_year, sa.studio_id
        FROM persons p
        JOIN feat_career fc ON fc.person_id = p.id
        JOIN feat_studio_affiliation sa ON sa.person_id = p.id
        WHERE p.gender IN ('F', 'M')
        GROUP BY p.id, sa.studio_id
        HAVING COUNT(*) >= 1
        """
    ).fetchall()

    if not rows:
        return {"error": "no_data"}

    import pandas as pd

    df = pd.DataFrame(
        rows,
        columns=[
            "gender",
            "person_id",
            "highest_stage",
            "active_years",
            "first_year",
            "studio_id",
        ],
    )
    df = df.dropna()

    # per-studio: promotion gap (simple rate difference F - M, min 5 per gender)
    studio_results: dict = {}
    for studio_id, grp in df.groupby("studio_id"):
        f_grp = grp[grp["gender"] == "F"]
        m_grp = grp[grp["gender"] == "M"]
        if len(f_grp) < 3 or len(m_grp) < 3:
            continue

        promoted_f = (f_grp["highest_stage"] >= 3).mean()
        promoted_m = (m_grp["highest_stage"] >= 3).mean()
        gamma_j = float(promoted_f - promoted_m)

        studio_results[str(studio_id)] = {
            "gamma_j": round(gamma_j, 4),
            "n_F": int(len(f_grp)),
            "n_M": int(len(m_grp)),
        }

    # sort by gamma_j
    sorted_studios = sorted(studio_results.items(), key=lambda x: x[1]["gamma_j"])

    return {
        "n_studios": len(studio_results),
        "top_10_F_favored": dict(sorted_studios[-10:]),
        "bottom_10_M_favored": dict(sorted_studios[:10]),
        "method_note": "Simple promotion rate gap per studio, min n=3 per gender",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────


def run_gender_bottleneck(conn: sqlite3.Connection) -> dict[str, Any]:
    """Gender bottleneck analysis — main entry point."""
    survival = compute_gender_survival_by_stage(conn)
    oaxaca = compute_promotion_gap_oaxaca(conn)
    studio_fe = compute_studio_gender_fe(conn)

    return {
        "survival_by_stage": survival,
        "oaxaca_decomposition": oaxaca,
        "studio_gender_fe": studio_fe,
        "method_notes": {
            "data_quality_warning": (
                "persons.gender の欠損率が高い場合、分析結果の代表性は限定的。"
                "gender 記録のある人材にサンプリングバイアスが存在する可能性がある。"
            ),
        },
    }

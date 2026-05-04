"""Gender progression analysis — role advancement hazard rate disparity.

Computes:
- Cox proportional hazards model for gender differences in role advancement timing
- Mann-Whitney U test for within-cohort advancement timing differences
- Ego-network gender composition vs. null model (permutation test)

All metrics are structural (credit records only).  Viewer ratings are not used.
Viewer ratings are excluded from all computations.

Framing note (H2 compliance):
  Results are described solely in terms of "role advancement hazard rate difference"
  and "network position difference".  Structural descriptors only.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Ordered pipeline roles (low → high stage)
PIPELINE_ROLES: list[str] = [
    "in_between",          # 動画 — Stage 1
    "key_animator",        # 原画 — Stage 2
    "animation_director",  # 作監 — Stage 3
    "director",            # 監督 — Stage 4
]

#: Pipeline stage pairs for Cox / Mann-Whitney analysis
PIPELINE_PAIRS: list[tuple[str, str, str]] = [
    ("in_between", "key_animator", "動画→原画"),
    ("key_animator", "animation_director", "原画→作監"),
    ("animation_director", "director", "作監→監督"),
]

#: Cohort bin size in years
COHORT_BIN: int = 5

#: Gender normalisation mapping
_GENDER_NORM: dict[str, str] = {
    "male": "M",
    "Male": "M",
    "female": "F",
    "Female": "F",
    "Non-binary": "NB",
    "non-binary": "NB",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class GenderProgressionRecord:
    """Person-level progression record enriched with gender.

    duration_years is None for right-censored observations (person never reached
    role_to within the observation window).
    """

    person_id: str
    gender: str          # 'M', 'F', 'NB' — unknown persons excluded upstream
    role_from: str
    role_to: str
    first_year_from: int
    first_year_to: int | None
    duration_years: float | None
    cohort_5y: int


@dataclass
class CoxResult:
    """Cox proportional hazard model result for gender covariate.

    hr is the hazard ratio of gender='F' relative to gender='M' (reference).
    Positive log(HR) > 0 means higher hazard (faster advancement) for F.
    Positive log(HR) < 0 means slower advancement for F.

    Results are described as "advancement hazard rate difference" only.
    """

    role_from: str
    role_to: str
    pair_label: str
    hr_female_vs_male: float           # hazard ratio F vs M
    ci_lower: float                    # 95% CI lower
    ci_upper: float                    # 95% CI upper
    p_value: float | None
    n_female: int
    n_male: int
    n_events_female: int
    n_events_male: int
    method: str = "CoxPHFitter (lifelines, age+cohort covariate)"
    logrank_p: float | None = None     # log-rank test F vs M


@dataclass
class MannWhitneyResult:
    """Mann-Whitney U result for within-cohort advancement timing difference.

    Effect size r = Z / sqrt(n_total), following the standard formula.
    """

    role_from: str
    role_to: str
    pair_label: str
    cohort_5y: int
    n_female: int
    n_male: int
    u_statistic: float
    p_value: float
    effect_r: float     # |Z| / sqrt(n_female + n_male)
    median_years_female: float | None
    median_years_male: float | None
    method: str = "Mann-Whitney U (scipy.stats.mannwhitneyu, two-sided)"


@dataclass
class EgoNetworkResult:
    """Ego-network gender composition vs. permutation null model.

    same_gender_share: observed fraction of ego's collaborators sharing gender.
    null_mean / null_sd: from 1000-iteration random pairing.
    null_percentile: where observed share sits in null distribution (0–100).
    """

    person_id: str
    gender: str
    same_gender_share: float
    null_mean: float
    null_sd: float
    null_percentile: float
    n_collaborators: int
    role: str


@dataclass
class EgoNetworkSummary:
    """Aggregate summary of ego-network gender composition analysis."""

    n_persons: int
    mean_same_gender_share_female: float
    mean_same_gender_share_male: float
    median_null_percentile_female: float
    median_null_percentile_male: float
    n_above_95th_female: int   # persons where observed > 95th null pct
    n_above_95th_male: int
    n_null_iterations: int = 1000


# ---------------------------------------------------------------------------
# Step 1: Load gender-annotated progression records
# ---------------------------------------------------------------------------


def _normalise_gender(raw: str | None) -> str | None:
    """Normalise raw gender string to 'M', 'F', 'NB', or None."""
    if raw is None:
        return None
    return _GENDER_NORM.get(raw)


def load_gender_progression_records(
    conn: Any,
    role_from: str,
    role_to: str,
    *,
    min_year: int = 1970,
    max_year: int = 2025,
) -> list[GenderProgressionRecord]:
    """Load per-person advancement records with gender annotation.

    Persons with unknown gender (NULL or unrecognised value) are excluded.
    Only 'M' and 'F' are included in the primary analysis; 'NB' records are
    loaded but callers may filter them depending on sample size.

    Args:
        conn: SILVER-layer DB connection.
        role_from: Source role (e.g. 'in_between').
        role_to: Target role (e.g. 'key_animator').
        min_year: Earliest plausible credit year.
        max_year: Latest year to treat as fully observed.

    Returns:
        List of GenderProgressionRecord.
    """
    sql_from = """
        SELECT cr.person_id, MIN(cr.credit_year) AS first_year
        FROM credits cr
        WHERE cr.role = ?
          AND cr.credit_year IS NOT NULL
          AND cr.credit_year >= ?
          AND cr.credit_year <= ?
        GROUP BY cr.person_id
    """
    sql_to = """
        SELECT cr.person_id, MIN(cr.credit_year) AS first_year
        FROM credits cr
        WHERE cr.role = ?
          AND cr.credit_year IS NOT NULL
          AND cr.credit_year >= ?
          AND cr.credit_year <= ?
        GROUP BY cr.person_id
    """
    sql_gender = """
        SELECT p.id, p.gender
        FROM persons p
        WHERE p.gender IS NOT NULL
    """

    try:
        from_map = {
            r[0]: r[1]
            for r in conn.execute(sql_from, (role_from, min_year, max_year)).fetchall()
        }
        to_map = {
            r[0]: r[1]
            for r in conn.execute(sql_to, (role_to, min_year, max_year)).fetchall()
        }
        gender_map: dict[str, str | None] = {
            r[0]: _normalise_gender(r[1])
            for r in conn.execute(sql_gender).fetchall()
        }
    except Exception as exc:
        log.warning("gender_progression_query_failed", error=str(exc))
        return []

    records: list[GenderProgressionRecord] = []
    for person_id, first_from in from_map.items():
        gender = gender_map.get(person_id)
        if gender not in ("M", "F", "NB"):
            continue  # exclude unknown

        cohort_5y = (first_from // COHORT_BIN) * COHORT_BIN
        first_to = to_map.get(person_id)

        if first_to is not None and first_to >= first_from:
            duration = float(first_to - first_from)
        else:
            duration = None
            first_to = None

        records.append(
            GenderProgressionRecord(
                person_id=person_id,
                gender=gender,
                role_from=role_from,
                role_to=role_to,
                first_year_from=first_from,
                first_year_to=first_to,
                duration_years=duration,
                cohort_5y=cohort_5y,
            )
        )

    log.debug(
        "gender_progression_records_loaded",
        role_from=role_from,
        role_to=role_to,
        n=len(records),
        n_f=sum(1 for r in records if r.gender == "F"),
        n_m=sum(1 for r in records if r.gender == "M"),
    )
    return records


# ---------------------------------------------------------------------------
# Step 2: Cox proportional hazards — role advancement hazard rate
# ---------------------------------------------------------------------------


def cox_progression_hazard(
    records: list[GenderProgressionRecord],
    pair_label: str = "",
    *,
    censor_years: float = 25.0,
) -> CoxResult | None:
    """Fit Cox PH model with gender covariate on advancement timing.

    Model: h(t | gender, cohort_5y) = h0(t) * exp(β_gender * gender + β_cohort * cohort)

    gender is coded as binary (F=1, M=0).  Result HR represents the advancement
    hazard rate ratio of F relative to M. HR > 1 means higher hazard (faster
    advancement) for F; HR < 1 means slower advancement for F.

    Description is purely structural: "advancement hazard rate difference".

    Args:
        records: Output of load_gender_progression_records() filtered to M and F.
        pair_label: Human-readable label for logging.
        censor_years: Duration assigned to censored observations.

    Returns:
        CoxResult or None if lifelines unavailable or insufficient data.
    """
    try:
        import pandas as pd
        from lifelines import CoxPHFitter
        from lifelines.statistics import logrank_test
    except ImportError as exc:
        log.warning("cox_import_failed", error=str(exc))
        return None

    mf_records = [r for r in records if r.gender in ("M", "F")]
    if len(mf_records) < 20:
        log.info("cox_insufficient_data", pair_label=pair_label, n=len(mf_records))
        return None

    role_from = mf_records[0].role_from if mf_records else ""
    role_to = mf_records[0].role_to if mf_records else ""

    rows = []
    for rec in mf_records:
        dur = rec.duration_years if rec.duration_years is not None else censor_years
        event = int(rec.duration_years is not None)
        rows.append({
            "duration": dur,
            "event": event,
            "gender_f": 1 if rec.gender == "F" else 0,
            "cohort_5y": rec.cohort_5y,
        })

    df = pd.DataFrame(rows)

    # Normalise cohort to zero mean for numerical stability
    cohort_mean = df["cohort_5y"].mean()
    df["cohort_centered"] = df["cohort_5y"] - cohort_mean

    try:
        cph = CoxPHFitter()
        cph.fit(
            df[["duration", "event", "gender_f", "cohort_centered"]],
            duration_col="duration",
            event_col="event",
        )

        summary = cph.summary
        gender_row = summary.loc["gender_f"] if "gender_f" in summary.index else None

        if gender_row is None:
            log.warning("cox_gender_coef_missing", pair_label=pair_label)
            return None

        hr = float(math.exp(gender_row["coef"]))
        ci_lower = float(math.exp(gender_row["coef lower 95%"]))
        ci_upper = float(math.exp(gender_row["coef upper 95%"]))
        p_value = float(gender_row["p"])
    except Exception as exc:
        log.warning("cox_fit_failed", pair_label=pair_label, error=str(exc))
        return None

    # Log-rank test F vs M
    logrank_p: float | None = None
    try:
        f_dur = [r.duration_years if r.duration_years is not None else censor_years
                 for r in mf_records if r.gender == "F"]
        f_evt = [r.duration_years is not None for r in mf_records if r.gender == "F"]
        m_dur = [r.duration_years if r.duration_years is not None else censor_years
                 for r in mf_records if r.gender == "M"]
        m_evt = [r.duration_years is not None for r in mf_records if r.gender == "M"]
        lr = logrank_test(f_dur, m_dur, event_observed_A=f_evt, event_observed_B=m_evt)
        logrank_p = float(lr.p_value)
    except Exception:
        pass

    n_f = sum(1 for r in mf_records if r.gender == "F")
    n_m = sum(1 for r in mf_records if r.gender == "M")
    n_evt_f = sum(1 for r in mf_records if r.gender == "F" and r.duration_years is not None)
    n_evt_m = sum(1 for r in mf_records if r.gender == "M" and r.duration_years is not None)

    log.info(
        "cox_progression_hazard_computed",
        pair_label=pair_label,
        hr=round(hr, 3),
        ci=(round(ci_lower, 3), round(ci_upper, 3)),
        p_value=round(p_value, 4),
        n_f=n_f,
        n_m=n_m,
    )

    return CoxResult(
        role_from=role_from,
        role_to=role_to,
        pair_label=pair_label or f"{role_from}→{role_to}",
        hr_female_vs_male=hr,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        p_value=p_value,
        n_female=n_f,
        n_male=n_m,
        n_events_female=n_evt_f,
        n_events_male=n_evt_m,
        logrank_p=logrank_p,
    )


# ---------------------------------------------------------------------------
# Step 3: Mann-Whitney U — within-cohort advancement timing
# ---------------------------------------------------------------------------


def mannwhitney_advancement_timing(
    records: list[GenderProgressionRecord],
    pair_label: str = "",
    *,
    min_cohort_size: int = 5,
) -> list[MannWhitneyResult]:
    """Run Mann-Whitney U within each cohort to compare advancement timing by gender.

    Only observed (non-censored) durations are used. Cohorts with fewer than
    min_cohort_size observations per gender are excluded.

    Effect size r = |Z| / sqrt(n_female + n_male) (r ≈ 0.1 small, 0.3 medium, 0.5 large).

    Args:
        records: Progression records (M and F only recommended).
        pair_label: Human-readable label.
        min_cohort_size: Minimum observations per gender per cohort.

    Returns:
        List of MannWhitneyResult, one per cohort.
    """
    try:
        from scipy.stats import mannwhitneyu
    except ImportError:
        log.warning("scipy_not_available")
        return []

    mf_observed = [
        r for r in records
        if r.gender in ("M", "F") and r.duration_years is not None
    ]

    cohort_groups: dict[int, list[GenderProgressionRecord]] = {}
    for rec in mf_observed:
        cohort_groups.setdefault(rec.cohort_5y, []).append(rec)

    results: list[MannWhitneyResult] = []
    role_from = records[0].role_from if records else ""
    role_to = records[0].role_to if records else ""

    for cohort_5y, cohort_recs in sorted(cohort_groups.items()):
        f_dur = [r.duration_years for r in cohort_recs if r.gender == "F"]
        m_dur = [r.duration_years for r in cohort_recs if r.gender == "M"]

        if len(f_dur) < min_cohort_size or len(m_dur) < min_cohort_size:
            continue

        try:
            stat, pval = mannwhitneyu(f_dur, m_dur, alternative="two-sided")  # type: ignore[arg-type]
        except Exception as exc:
            log.warning("mannwhitney_failed", cohort=cohort_5y, error=str(exc))
            continue

        n_total = len(f_dur) + len(m_dur)
        # Normal approximation: Z ≈ (U - n1*n2/2) / sqrt(n1*n2*(n1+n2+1)/12)
        n1, n2 = len(f_dur), len(m_dur)
        mean_u = n1 * n2 / 2
        std_u = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)
        z = abs(stat - mean_u) / std_u if std_u > 0 else 0.0
        effect_r = z / math.sqrt(n_total) if n_total > 0 else 0.0

        sorted_f = sorted(v for v in f_dur if v is not None)
        sorted_m = sorted(v for v in m_dur if v is not None)
        median_f = sorted_f[len(sorted_f) // 2] if sorted_f else None
        median_m = sorted_m[len(sorted_m) // 2] if sorted_m else None

        results.append(
            MannWhitneyResult(
                role_from=role_from,
                role_to=role_to,
                pair_label=pair_label or f"{role_from}→{role_to}",
                cohort_5y=cohort_5y,
                n_female=n1,
                n_male=n2,
                u_statistic=float(stat),
                p_value=float(pval),
                effect_r=effect_r,
                median_years_female=median_f,
                median_years_male=median_m,
            )
        )

    log.debug(
        "mannwhitney_computed",
        pair_label=pair_label,
        n_cohorts=len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Step 4: Ego-network gender composition vs. null model
# ---------------------------------------------------------------------------

_EGO_NET_SQL = """
    SELECT
        c1.person_id AS ego_id,
        p1.gender AS ego_gender,
        c2.person_id AS alter_id,
        p2.gender AS alter_gender,
        c1.role AS ego_role
    FROM credits c1
    JOIN credits c2 ON c1.anime_id = c2.anime_id AND c1.person_id != c2.person_id
    JOIN persons p1 ON c1.person_id = p1.id
    JOIN persons p2 ON c2.person_id = p2.id
    WHERE p1.gender IS NOT NULL
      AND p2.gender IS NOT NULL
      AND c1.role IN ('in_between', 'key_animator', 'animation_director', 'director')
"""


def compute_ego_network_gender_composition(
    conn: Any,
    *,
    n_null_iterations: int = 1000,
    rng_seed: int = 42,
    sample_cap: int = 5000,
) -> tuple[list[EgoNetworkResult], EgoNetworkSummary]:
    """Compute ego-network same-gender share vs. permutation null model.

    For each person with known gender, computes the fraction of 1-hop collaborators
    who share the same gender (same_gender_share). Compares to a null distribution
    generated by randomly permuting gender labels across the collaborator pool
    (preserving the gender ratio) 1000 times.

    null_percentile = where observed same_gender_share falls in the null distribution.
    High null_percentile (>95) suggests homophily; low (<5) suggests heterophily.

    Capped at sample_cap persons for performance.

    Args:
        conn: SILVER-layer DB connection.
        n_null_iterations: Permutation iterations for null model.
        rng_seed: Random seed for reproducibility.
        sample_cap: Maximum number of persons to analyse.

    Returns:
        (list of EgoNetworkResult, EgoNetworkSummary)
    """
    rng = random.Random(rng_seed)

    try:
        raw_rows = conn.execute(_EGO_NET_SQL).fetchall()
    except Exception as exc:
        log.warning("ego_net_query_failed", error=str(exc))
        return [], EgoNetworkSummary(
            n_persons=0,
            mean_same_gender_share_female=0.0,
            mean_same_gender_share_male=0.0,
            median_null_percentile_female=50.0,
            median_null_percentile_male=50.0,
            n_above_95th_female=0,
            n_above_95th_male=0,
            n_null_iterations=n_null_iterations,
        )

    # Group: ego_id → list of (alter_gender_norm, ego_gender_norm, ego_role)
    ego_alters: dict[str, list[str]] = {}
    ego_gender: dict[str, str] = {}
    ego_role: dict[str, str] = {}

    for row in raw_rows:
        ego_id, ego_g_raw, _alter_id, alter_g_raw, role = row
        ego_g = _normalise_gender(ego_g_raw)
        alter_g = _normalise_gender(alter_g_raw)
        if ego_g not in ("M", "F") or alter_g not in ("M", "F"):
            continue
        ego_alters.setdefault(ego_id, []).append(alter_g)
        ego_gender[ego_id] = ego_g
        ego_role[ego_id] = role

    if not ego_alters:
        return [], EgoNetworkSummary(
            n_persons=0,
            mean_same_gender_share_female=0.0,
            mean_same_gender_share_male=0.0,
            median_null_percentile_female=50.0,
            median_null_percentile_male=50.0,
            n_above_95th_female=0,
            n_above_95th_male=0,
            n_null_iterations=n_null_iterations,
        )

    # Sample cap — prioritise persons with more collaborators
    sampled_ids = sorted(
        ego_alters.keys(), key=lambda x: len(ego_alters[x]), reverse=True
    )[:sample_cap]

    results: list[EgoNetworkResult] = []

    for ego_id in sampled_ids:
        alters = ego_alters[ego_id]
        g = ego_gender[ego_id]
        role = ego_role.get(ego_id, "")

        n_alters = len(alters)
        if n_alters == 0:
            continue

        observed_share = sum(1 for a in alters if a == g) / n_alters

        # Null model: permute alter genders preserving gender ratio
        n_f_alters = sum(1 for a in alters if a == "F")
        n_m_alters = n_alters - n_f_alters
        pool = ["F"] * n_f_alters + ["M"] * n_m_alters

        null_shares: list[float] = []
        for _ in range(n_null_iterations):
            shuffled = pool[:]
            rng.shuffle(shuffled)
            null_shares.append(sum(1 for a in shuffled if a == g) / n_alters)

        null_shares.sort()
        null_mean = sum(null_shares) / len(null_shares)
        null_sd = (
            math.sqrt(sum((x - null_mean) ** 2 for x in null_shares) / len(null_shares))
            if len(null_shares) > 1 else 0.0
        )
        null_pct = (
            100.0 * sum(1 for v in null_shares if v <= observed_share) / len(null_shares)
        )

        results.append(
            EgoNetworkResult(
                person_id=ego_id,
                gender=g,
                same_gender_share=observed_share,
                null_mean=null_mean,
                null_sd=null_sd,
                null_percentile=null_pct,
                n_collaborators=n_alters,
                role=role,
            )
        )

    # Aggregate summary
    f_results = [r for r in results if r.gender == "F"]
    m_results = [r for r in results if r.gender == "M"]

    def _median(vals: list[float]) -> float:
        if not vals:
            return 50.0
        s = sorted(vals)
        return s[len(s) // 2]

    mean_share_f = sum(r.same_gender_share for r in f_results) / len(f_results) if f_results else 0.0
    mean_share_m = sum(r.same_gender_share for r in m_results) / len(m_results) if m_results else 0.0
    med_pct_f = _median([r.null_percentile for r in f_results])
    med_pct_m = _median([r.null_percentile for r in m_results])
    n95_f = sum(1 for r in f_results if r.null_percentile >= 95)
    n95_m = sum(1 for r in m_results if r.null_percentile >= 95)

    summary = EgoNetworkSummary(
        n_persons=len(results),
        mean_same_gender_share_female=mean_share_f,
        mean_same_gender_share_male=mean_share_m,
        median_null_percentile_female=med_pct_f,
        median_null_percentile_male=med_pct_m,
        n_above_95th_female=n95_f,
        n_above_95th_male=n95_m,
        n_null_iterations=n_null_iterations,
    )

    log.info(
        "ego_net_gender_computed",
        n_persons=len(results),
        n_f=len(f_results),
        n_m=len(m_results),
    )
    return results, summary


# ---------------------------------------------------------------------------
# Convenience: run all three analyses for a given pipeline pair
# ---------------------------------------------------------------------------


def run_gender_progression_analysis(
    conn: Any,
    role_from: str,
    role_to: str,
    pair_label: str = "",
) -> tuple[
    CoxResult | None,
    list[MannWhitneyResult],
]:
    """Run Cox + Mann-Whitney for one pipeline pair.

    This is a convenience wrapper that calls load_gender_progression_records,
    cox_progression_hazard, and mannwhitney_advancement_timing.

    The ego-network analysis is separate (compute_ego_network_gender_composition)
    because it operates on the full collaborator graph rather than a single pair.

    Args:
        conn: SILVER-layer DB connection.
        role_from: Source role.
        role_to: Target role.
        pair_label: Human-readable label (used in logs and results).

    Returns:
        (CoxResult or None, list of MannWhitneyResult)
    """
    records = load_gender_progression_records(conn, role_from, role_to)
    cox = cox_progression_hazard(records, pair_label)
    mw = mannwhitney_advancement_timing(records, pair_label)
    return cox, mw

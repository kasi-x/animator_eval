"""Mentor effect estimation — mentor との初協業前後の mentee の構造的位置変化。

既存 `mentorship.py` の `infer_mentorships()` が mentor–mentee pair 推定を担当。
本モジュールは推定済 pair に対し:

    Δθ_mentee = θ_mentee(post mentor 初協業 +1..+5 年) - θ_mentee(pre mentor 初協業 -3..-1 年)

を計算し、event-study スタイルで mentor effect を推定。

問題: confounding (selection on observables) — 「経験豊富な mentor は構造的に
有利な mentee を選ぶ」逆因果。本モジュールは比較群 (similar non-mentored peer) を bootstrap し
**matched difference-in-differences** で control 群との差分の差分を取る。

H1: anime.score 非依存。
H2: 主観的評価 frame NG → 「協業経験あり/なし person の構造的位置の差」のみ。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Per-pair event-study
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MentorEventStudyRow:
    """1 mentor–mentee pair の event-time × delta_theta row。"""

    mentor_id: str
    mentee_id: str
    event_year: int          # mentor との初協業年
    pre_window: tuple[int, int]   # (start, end) inclusive
    post_window: tuple[int, int]
    pre_theta_mean: float
    post_theta_mean: float
    delta: float                  # post - pre
    n_pre_obs: int
    n_post_obs: int


def compute_pair_event_study(
    pair: tuple[str, str],
    event_year: int,
    mentee_year_theta: list[tuple[int, float]],
    *,
    pre_window: tuple[int, int] = (-3, -1),
    post_window: tuple[int, int] = (1, 5),
) -> MentorEventStudyRow | None:
    """1 pair に対し pre/post 窓内の平均 theta_i を計算。

    Args:
        pair: (mentor_id, mentee_id)
        event_year: mentor–mentee 初協業の年
        mentee_year_theta: [(year, theta_i)] mentee の年次 theta 系列
        pre_window: event_year + offset の範囲 (default -3..-1)
        post_window: event_year + offset の範囲 (default +1..+5)

    Returns:
        delta が計算できる場合 MentorEventStudyRow、データ不足は None。
    """
    pre_start, pre_end = event_year + pre_window[0], event_year + pre_window[1]
    post_start, post_end = event_year + post_window[0], event_year + post_window[1]

    pre_vals = [t for y, t in mentee_year_theta if pre_start <= y <= pre_end and t is not None]
    post_vals = [t for y, t in mentee_year_theta if post_start <= y <= post_end and t is not None]

    if not pre_vals or not post_vals:
        return None

    pre_mean = float(np.mean(pre_vals))
    post_mean = float(np.mean(post_vals))

    return MentorEventStudyRow(
        mentor_id=pair[0],
        mentee_id=pair[1],
        event_year=int(event_year),
        pre_window=pre_window,
        post_window=post_window,
        pre_theta_mean=pre_mean,
        post_theta_mean=post_mean,
        delta=post_mean - pre_mean,
        n_pre_obs=len(pre_vals),
        n_post_obs=len(post_vals),
    )


# ---------------------------------------------------------------------------
# Aggregate effect across pairs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MentorAggregateEffect:
    """全 pair で集計した mentor effect estimate。"""

    n_pairs: int
    mean_delta: float          # 平均 Δθ
    sd_delta: float
    median_delta: float
    ci_low: float              # bootstrap percentile
    ci_high: float
    bootstrap_n: int


def aggregate_mentor_effects(
    pair_rows: Sequence[MentorEventStudyRow],
    *,
    bootstrap_n: int = 1000,
    rng_seed: int = 42,
    ci_level: float = 0.95,
) -> MentorAggregateEffect:
    """Pair 単位の delta を平均し bootstrap CI。

    Note: confounding を完全除去はできない。control 群との DiD は
    estimate_matched_did() を別途使用すべき。
    """
    if not pair_rows:
        return MentorAggregateEffect(
            n_pairs=0, mean_delta=0.0, sd_delta=0.0, median_delta=0.0,
            ci_low=0.0, ci_high=0.0, bootstrap_n=bootstrap_n,
        )
    deltas = np.array([r.delta for r in pair_rows], dtype=float)
    rng = np.random.default_rng(rng_seed)
    bs = np.empty(bootstrap_n)
    for b in range(bootstrap_n):
        idx = rng.integers(0, deltas.size, deltas.size)
        bs[b] = deltas[idx].mean()
    alpha = (1.0 - ci_level) / 2.0
    return MentorAggregateEffect(
        n_pairs=int(deltas.size),
        mean_delta=float(deltas.mean()),
        sd_delta=float(deltas.std(ddof=1)) if deltas.size > 1 else 0.0,
        median_delta=float(np.median(deltas)),
        ci_low=float(np.percentile(bs, alpha * 100)),
        ci_high=float(np.percentile(bs, (1.0 - alpha) * 100)),
        bootstrap_n=bootstrap_n,
    )


# ---------------------------------------------------------------------------
# Matched DiD with control mentees
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchedDiDResult:
    """Mentored mentee vs matched non-mentored peer の DiD。"""

    n_treated: int
    n_control: int
    treated_delta_mean: float        # Δθ for mentees (post-pre)
    control_delta_mean: float        # Δθ for matched controls (post-pre)
    did_estimate: float              # treated_delta - control_delta
    did_ci_low: float
    did_ci_high: float
    bootstrap_n: int


def estimate_matched_did(
    treated_pairs: Sequence[MentorEventStudyRow],
    candidate_controls: dict[str, list[tuple[int, float]]],
    *,
    pre_window: tuple[int, int] = (-3, -1),
    post_window: tuple[int, int] = (1, 5),
    bootstrap_n: int = 500,
    rng_seed: int = 42,
    ci_level: float = 0.95,
) -> MatchedDiDResult:
    """Mentored mentee の delta vs unmentored peer の delta (DiD)。

    Matching: 各 treated mentee の event_year を control の "仮想 event_year" として
    流用し、同窓の pre/post theta を計算。1:1 matching ではなく
    candidate pool 内の全 candidate (cohort-similar) の平均を control とする。

    Args:
        treated_pairs: 推定済 mentor effect rows (treatment 群)。
        candidate_controls: {control_person_id: [(year, theta_i)]}.
            mentored じゃない person のみを含めること。

    Returns:
        DiD estimate + bootstrap CI.
    """
    if not treated_pairs:
        return MatchedDiDResult(
            n_treated=0, n_control=0, treated_delta_mean=0.0,
            control_delta_mean=0.0, did_estimate=0.0,
            did_ci_low=0.0, did_ci_high=0.0, bootstrap_n=bootstrap_n,
        )

    treated_deltas = np.array([r.delta for r in treated_pairs], dtype=float)

    # For each treated event year, compute the average control delta
    # using the same window.
    control_deltas_pool: list[float] = []
    treated_years = [r.event_year for r in treated_pairs]
    for ey in treated_years:
        for cid, series in candidate_controls.items():
            pre_start, pre_end = ey + pre_window[0], ey + pre_window[1]
            post_start, post_end = ey + post_window[0], ey + post_window[1]
            pre_vals = [t for y, t in series if pre_start <= y <= pre_end and t is not None]
            post_vals = [t for y, t in series if post_start <= y <= post_end and t is not None]
            if pre_vals and post_vals:
                control_deltas_pool.append(float(np.mean(post_vals)) - float(np.mean(pre_vals)))

    control_arr = np.array(control_deltas_pool, dtype=float) if control_deltas_pool else np.array([])
    treated_mean = float(treated_deltas.mean())
    control_mean = float(control_arr.mean()) if control_arr.size else 0.0
    did = treated_mean - control_mean

    # Bootstrap CI: resample both arms independently
    rng = np.random.default_rng(rng_seed)
    bs = np.empty(bootstrap_n)
    for b in range(bootstrap_n):
        t_idx = rng.integers(0, treated_deltas.size, treated_deltas.size)
        c_idx = (
            rng.integers(0, control_arr.size, control_arr.size)
            if control_arr.size > 0
            else None
        )
        t_mean = float(treated_deltas[t_idx].mean())
        c_mean = float(control_arr[c_idx].mean()) if c_idx is not None else 0.0
        bs[b] = t_mean - c_mean
    alpha = (1.0 - ci_level) / 2.0

    return MatchedDiDResult(
        n_treated=int(treated_deltas.size),
        n_control=int(control_arr.size),
        treated_delta_mean=treated_mean,
        control_delta_mean=control_mean,
        did_estimate=float(did),
        did_ci_low=float(np.percentile(bs, alpha * 100)),
        did_ci_high=float(np.percentile(bs, (1.0 - alpha) * 100)),
        bootstrap_n=bootstrap_n,
    )

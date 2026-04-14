"""Double/Debiased Machine Learning — 二重機械学習による因果推論.

Chernozhukov, Chetverikov, Demirer, Duflo, Hansen, Newey, Robins (2018):
"Double/debiased machine learning for treatment and structural parameters"

各パラメータについて「OLS推定 vs DML推定」の2パターンを出力し、
交絡バイアスの方向と大きさを可視化する。

Partial Linear Model:
  Y = θ₀·D + g₀(X) + ε
  D = m₀(X) + V

  OLS: g₀(X) を線形仮定 → バイアスあり
  DML: g₀(X) を GBM で柔軟推定 + cross-fitting → バイアス除去

対象パラメータ:
  1. person_fe → production_scale (AKM person FE の因果的妥当性)
  2. studio_switch → performance (スタジオ移籍の因果効果)
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field

import numpy as np
import structlog
from scipy import stats as scipy_stats
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import KFold

from src.models import Anime, Credit, Role

logger = structlog.get_logger()

# =============================================================================
# Result dataclasses
# =============================================================================


@dataclass
class EstimationResult:
    """単一推定手法の結果."""

    theta: float  # point estimate
    se: float  # standard error
    ci_lower: float = 0.0
    ci_upper: float = 0.0
    t_stat: float = 0.0
    p_value: float = 1.0
    r2_y: float = 0.0  # nuisance model R² for Y (DML only)
    r2_d: float = 0.0  # nuisance model R² for D (DML only)

    def __post_init__(self):
        if self.se > 0:
            self.t_stat = self.theta / self.se
            self.p_value = float(2 * (1 - scipy_stats.norm.cdf(abs(self.t_stat))))
        self.ci_lower = self.theta - 1.96 * self.se
        self.ci_upper = self.theta + 1.96 * self.se


@dataclass
class DualEstimate:
    """OLS vs DML の2パターン比較."""

    parameter: str  # 推定対象の名前
    description: str  # 日本語の説明
    ols: EstimationResult  # 従来推定（OLS/naive）
    dml: EstimationResult  # DML推定
    n_obs: int = 0
    n_folds: int = 5
    bias: float = 0.0  # OLS.theta - DML.theta (正=OLS過大)
    bias_pct: float = 0.0  # bias / |DML.theta| × 100
    interpretation: str = ""

    def __post_init__(self):
        self.bias = self.ols.theta - self.dml.theta
        if abs(self.dml.theta) > 1e-8:
            self.bias_pct = self.bias / abs(self.dml.theta) * 100

    def to_dict(self) -> dict:
        return {
            "parameter": self.parameter,
            "description": self.description,
            "ols": asdict(self.ols),
            "dml": asdict(self.dml),
            "n_obs": self.n_obs,
            "bias": round(self.bias, 6),
            "bias_pct": round(self.bias_pct, 2),
            "interpretation": self.interpretation,
        }


@dataclass
class StudioCATEResult:
    """スタジオ移籍 CATE（異質処置効果）."""

    stage: str
    theta: float
    se: float
    n_obs: int


@dataclass
class DMLReport:
    """全パラメータの OLS vs DML 比較レポート."""

    estimates: list[DualEstimate] = field(default_factory=list)
    studio_cate: list[StudioCATEResult] = field(default_factory=list)
    diagnostics: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "estimates": [e.to_dict() for e in self.estimates],
            "studio_cate": [asdict(c) for c in self.studio_cate],
            "diagnostics": self.diagnostics,
        }


# =============================================================================
# Core DML algorithm
# =============================================================================

_DML_N_FOLDS = 5
_DML_SEED = 42
_DML_MIN_OBS = 100


def _fit_dml(
    Y: np.ndarray,
    D: np.ndarray,
    X: np.ndarray,
    n_folds: int = _DML_N_FOLDS,
    seed: int = _DML_SEED,
) -> EstimationResult:
    """Partial Linear DML: Y = θ₀·D + g₀(X) + ε.

    Cross-fitted Neyman orthogonal estimation.
    """
    n = len(Y)
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=seed)

    Y_resid = np.zeros(n)
    D_resid = np.zeros(n)
    r2_y_list = []
    r2_d_list = []

    for train_idx, test_idx in kf.split(X):
        # E[Y|X]
        my = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=seed,
        )
        my.fit(X[train_idx], Y[train_idx])
        yh = my.predict(X[test_idx])
        Y_resid[test_idx] = Y[test_idx] - yh
        ss_r = np.sum((Y[test_idx] - yh) ** 2)
        ss_t = np.sum((Y[test_idx] - np.mean(Y[test_idx])) ** 2)
        r2_y_list.append(1 - ss_r / ss_t if ss_t > 0 else 0.0)

        # E[D|X]
        md = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=seed,
        )
        md.fit(X[train_idx], D[train_idx])
        dh = md.predict(X[test_idx])
        D_resid[test_idx] = D[test_idx] - dh
        ss_r = np.sum((D[test_idx] - dh) ** 2)
        ss_t = np.sum((D[test_idx] - np.mean(D[test_idx])) ** 2)
        r2_d_list.append(1 - ss_r / ss_t if ss_t > 0 else 0.0)

    # θ̂ = (D̃'Ỹ) / (D̃'D̃)
    denom = np.sum(D_resid**2)
    if denom < 1e-10:
        return EstimationResult(theta=0.0, se=0.0)

    theta = float(np.sum(D_resid * Y_resid) / denom)

    # Neyman orthogonal score SE
    psi = D_resid * (Y_resid - theta * D_resid)
    J = np.mean(D_resid**2)
    se = float(np.sqrt(np.mean(psi**2) / (J**2) / n))

    return EstimationResult(
        theta=theta,
        se=se,
        r2_y=float(np.mean(r2_y_list)),
        r2_d=float(np.mean(r2_d_list)),
    )


def _fit_ols(Y: np.ndarray, D: np.ndarray, X: np.ndarray) -> EstimationResult:
    """Naive OLS: Y = θ·D + X·β + ε (線形、cross-fittingなし)."""
    Z = np.column_stack([D.reshape(-1, 1), X])
    try:
        beta = np.linalg.lstsq(Z, Y, rcond=None)[0]
        theta = float(beta[0])
        resid = Y - Z @ beta
        sigma2 = np.sum(resid**2) / max(len(Y) - Z.shape[1], 1)
        ZtZ_inv = np.linalg.inv(Z.T @ Z + 1e-6 * np.eye(Z.shape[1]))
        se = float(np.sqrt(sigma2 * ZtZ_inv[0, 0]))

        # R²
        ss_res = np.sum(resid**2)
        ss_tot = np.sum((Y - np.mean(Y)) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    except np.linalg.LinAlgError:
        return EstimationResult(theta=0.0, se=0.0)

    return EstimationResult(theta=theta, se=se, r2_y=r2)


# =============================================================================
# Feature engineering helpers
# =============================================================================


def _build_anime_features(
    anime_map: dict[str, Anime],
    studio_fe: dict[str, float],
) -> tuple[list[str], list[str], dict[str, int], dict[str, int]]:
    """Build genre/format vocabularies."""
    all_genres: set[str] = set()
    all_formats: set[str] = set()
    for a in anime_map.values():
        all_genres.update(a.genres[:5])
        if a.format:
            all_formats.add(a.format)
    genre_list = sorted(all_genres)
    format_list = sorted(all_formats)
    return (
        genre_list,
        format_list,
        {g: i for i, g in enumerate(genre_list)},
        {f: i for i, f in enumerate(format_list)},
    )


def _anime_feature_vector(
    anime: Anime,
    studio_fe: dict[str, float],
    genre_idx: dict[str, int],
    format_idx: dict[str, int],
    n_staff: int,
    n_genres: int,
    n_formats: int,
) -> np.ndarray:
    """Build feature vector for an anime."""
    s_fe = studio_fe.get(anime.studio or "", 0.0)
    year = anime.year or 2020
    episodes = anime.episodes or 1
    duration = anime.duration or 24

    genre_vec = np.zeros(n_genres)
    for g in anime.genres[:5]:
        if g in genre_idx:
            genre_vec[genre_idx[g]] = 1.0

    format_vec = np.zeros(n_formats)
    if anime.format and anime.format in format_idx:
        format_vec[format_idx[anime.format]] = 1.0

    return np.concatenate(
        [
            [s_fe, year, episodes, duration, n_staff],
            genre_vec,
            format_vec,
        ]
    )


# =============================================================================
# Pattern 1: Person FE → production_scale
# =============================================================================


def _estimate_person_fe_effect(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    studio_fe: dict[str, float],
) -> DualEstimate | None:
    """person_fe が production_scale を因果的に説明するか検証.

    Y = log(production_scale), D = person_fe, X = studio/genre/format/year/role
    """
    genre_list, format_list, genre_idx, format_idx = _build_anime_features(
        anime_map, studio_fe
    )
    n_genres, n_formats = len(genre_list), len(format_list)

    role_list = sorted({r.value for r in Role})
    role_idx = {r: i for i, r in enumerate(role_list)}
    n_roles = len(role_list)

    # Compute staff counts
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)
    anime_staff = {aid: len(pids) for aid, pids in anime_persons.items()}

    # Build observation dataset: unique (person, anime) pairs
    seen = set()
    rows_Y, rows_D, rows_X = [], [], []

    for c in credits:
        key = (c.person_id, c.anime_id)
        if key in seen or c.person_id not in person_fe:
            continue
        seen.add(key)

        anime = anime_map.get(c.anime_id)
        if anime is None:
            continue
        ns = anime_staff.get(c.anime_id, 1)
        if ns < 2:
            continue
        eps = anime.episodes or 1
        dur = anime.duration or 24
        ps = ns * eps * (dur / 30.0)
        if ps <= 0:
            continue

        base_feat = _anime_feature_vector(
            anime,
            studio_fe,
            genre_idx,
            format_idx,
            ns,
            n_genres,
            n_formats,
        )
        role_vec = np.zeros(n_roles)
        role_vec[role_idx.get(c.role.value, 0)] = 1.0

        rows_Y.append(np.log(ps))
        rows_D.append(person_fe[c.person_id])
        rows_X.append(np.concatenate([base_feat, role_vec]))

    if len(rows_Y) < _DML_MIN_OBS:
        return None

    Y, D, X = np.array(rows_Y), np.array(rows_D), np.array(rows_X)

    logger.info("dml_person_fe_start", n_obs=len(Y), n_feat=X.shape[1])

    ols = _fit_ols(Y, D, X)
    dml = _fit_dml(Y, D, X)

    # Interpretation
    if dml.p_value < 0.05 and abs(dml.theta - 1.0) < 0.3:
        interp = f"person_fe は因果的に有効 (DML θ={dml.theta:.3f}≈1.0)。AKM推定のバイアスは小さい。"
    elif dml.p_value < 0.05 and dml.theta < 0.7:
        interp = f"person_fe は有意だが OLS 過大推定 (DML θ={dml.theta:.3f} < OLS θ={ols.theta:.3f})。交絡バイアス大。"
    elif dml.p_value >= 0.05:
        interp = (
            f"person_fe は交絡除去後に非有意 (p={dml.p_value:.3f})。因果的解釈に注意。"
        )
    else:
        interp = f"DML θ={dml.theta:.3f}, OLS θ={ols.theta:.3f}。バイアス={ols.theta - dml.theta:+.3f}。"

    est = DualEstimate(
        parameter="person_fe",
        description="AKM個人固定効果 → log(production_scale)",
        ols=ols,
        dml=dml,
        n_obs=len(Y),
        interpretation=interp,
    )
    logger.info(
        "dml_person_fe_done",
        ols_theta=round(ols.theta, 4),
        dml_theta=round(dml.theta, 4),
        bias=round(est.bias, 4),
        p_dml=round(dml.p_value, 4),
    )
    return est


# =============================================================================
# Pattern 2: Studio switch → performance
# =============================================================================


def _estimate_studio_switch_effect(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    studio_fe: dict[str, float],
) -> tuple[DualEstimate | None, list[StudioCATEResult]]:
    """スタジオ移籍の因果効果を OLS vs DML で比較推定.

    D = post_switch (binary), Y = log(production_scale), X = career controls
    """
    role_list = sorted({r.value for r in Role})
    role_idx = {r: i for i, r in enumerate(role_list)}
    n_roles = len(role_list)

    # Person role counts
    person_roles: dict[str, Counter] = defaultdict(Counter)
    for c in credits:
        person_roles[c.person_id][c.role.value] += 1

    # Build person-year-studio timeline
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)
    anime_staff = {aid: len(pids) for aid, pids in anime_persons.items()}

    person_tl: dict[str, list[tuple[int, str, float]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime is None or anime.year is None or not anime.studio:
            continue
        ns = anime_staff.get(c.anime_id, 1)
        eps = anime.episodes or 1
        dur = anime.duration or 24
        ps = ns * eps * (dur / 30.0)
        if ps <= 0:
            continue
        person_tl[c.person_id].append((anime.year, anime.studio, np.log(ps)))

    # Build mover panel
    rows_Y, rows_D, rows_X = [], [], []
    career_years_all = []
    pre_perfs, post_perfs = [], []

    n_movers = 0
    for pid, tl in person_tl.items():
        if pid not in person_fe:
            continue
        tl.sort()

        # Group by year → primary studio, avg perf
        year_studio: dict[int, str] = {}
        year_perf: dict[int, list[float]] = defaultdict(list)
        for yr, st, lps in tl:
            year_studio[yr] = st
            year_perf[yr].append(lps)

        years = sorted(year_studio.keys())
        if len(years) < 3:
            continue

        # Find first studio change
        switch_year = None
        for i in range(1, len(years)):
            if year_studio[years[i]] != year_studio[years[i - 1]]:
                switch_year = years[i]
                break
        if switch_year is None:
            continue

        n_movers += 1
        first_year = years[0]
        primary_role = (
            person_roles[pid].most_common(1)[0][0] if person_roles[pid] else "other"
        )
        role_vec = np.zeros(n_roles)
        if primary_role in role_idx:
            role_vec[role_idx[primary_role]] = 1.0

        for yr in years:
            avg_p = float(np.mean(year_perf[yr]))
            post = 1.0 if yr >= switch_year else 0.0
            cy = yr - first_year
            ya = len([y for y in years if y <= yr])

            (post_perfs if post > 0 else pre_perfs).append(avg_p)

            rows_Y.append(avg_p)
            rows_D.append(post)
            rows_X.append(
                np.concatenate(
                    [
                        [cy, ya, yr, person_fe[pid]],
                        role_vec,
                    ]
                )
            )
            career_years_all.append(cy)

    if len(rows_Y) < _DML_MIN_OBS or n_movers < 20:
        logger.warning("dml_studio_insufficient", n_obs=len(rows_Y), n_movers=n_movers)
        return None, []

    Y, D, X = np.array(rows_Y), np.array(rows_D), np.array(rows_X)
    career_arr = np.array(career_years_all)

    logger.info("dml_studio_start", n_obs=len(Y), n_movers=n_movers)

    ols = _fit_ols(Y, D, X)
    dml = _fit_dml(Y, D, X)

    naive = (
        (np.mean(post_perfs) - np.mean(pre_perfs)) if pre_perfs and post_perfs else 0.0
    )

    # CATE by career stage
    cate_results: list[StudioCATEResult] = []
    for stage, lo, hi in [
        ("新人(0-5年)", 0, 5),
        ("中堅(5-15年)", 5, 15),
        ("ベテラン(15年+)", 15, 100),
    ]:
        mask = (career_arr >= lo) & (career_arr < hi)
        if mask.sum() >= 50:
            sub_dml = _fit_dml(Y[mask], D[mask], X[mask])
            cate_results.append(
                StudioCATEResult(
                    stage=stage,
                    theta=round(sub_dml.theta, 4),
                    se=round(sub_dml.se, 4),
                    n_obs=int(mask.sum()),
                )
            )

    # Interpretation
    if dml.p_value < 0.05 and dml.theta > 0:
        interp = (
            f"スタジオ移籍は有意な正の因果効果 (DML θ={dml.theta:+.3f})。"
            f"ナイーブ差分 {naive:+.3f} との差 = 選抜バイアス {naive - dml.theta:+.3f}。"
        )
    elif dml.p_value < 0.05:
        interp = (
            f"移籍の因果効果は有意に負 (θ={dml.theta:+.3f})。移籍は平均的にマイナス。"
        )
    else:
        interp = f"移籍の因果効果は非有意 (p={dml.p_value:.3f})。観測差 ({naive:+.3f}) は交絡で説明可能。"

    est = DualEstimate(
        parameter="studio_switch",
        description="スタジオ移籍 (post_switch) → log(production_scale)",
        ols=ols,
        dml=dml,
        n_obs=len(Y),
        interpretation=interp,
    )

    logger.info(
        "dml_studio_done",
        ols_theta=round(ols.theta, 4),
        dml_theta=round(dml.theta, 4),
        bias=round(est.bias, 4),
        naive_diff=round(naive, 4),
        n_movers=n_movers,
    )
    return est, cate_results


# =============================================================================
# Public API
# =============================================================================


def run_dml_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    studio_fe: dict[str, float],
) -> DMLReport:
    """全パラメータについて OLS vs DML の2パターン推定を実行.

    Args:
        credits: 全クレジットデータ
        anime_map: anime_id → Anime
        person_fe: person_id → AKM person fixed effect
        studio_fe: studio_name → AKM studio fixed effect

    Returns:
        DMLReport with OLS vs DML comparisons for each parameter
    """
    report = DMLReport()
    report.diagnostics = {
        "n_credits": len(credits),
        "n_anime": len(anime_map),
        "n_persons_with_fe": len(person_fe),
        "n_studios_with_fe": len(studio_fe),
        "method": "Double/Debiased ML (Chernozhukov et al. 2018)",
        "nuisance_model": "GradientBoostingRegressor(n=200, depth=4)",
        "n_folds": _DML_N_FOLDS,
    }

    # Pattern 1: Person FE validation
    est1 = _estimate_person_fe_effect(credits, anime_map, person_fe, studio_fe)
    if est1:
        report.estimates.append(est1)

    # Pattern 2: Studio switch effect
    est2, cate = _estimate_studio_switch_effect(
        credits, anime_map, person_fe, studio_fe
    )
    if est2:
        report.estimates.append(est2)
    report.studio_cate = cate

    return report

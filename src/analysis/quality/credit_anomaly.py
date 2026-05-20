"""Credit attribution anomaly detection.

統計的に異常な credit 出現パターンを検出し flag する。
entity resolution audit (`28/01` drift snapshot) と相補:
- ER audit = "同一人物が複数 source で違う ID で merge 失敗" を検出
- 本 module = "クレジット数 / 役職分布が統計的に外れている" person/anime を検出

3 つの detector:

1. **Poisson outlier**: ある期間の credit 数が cohort 期待値から外れている (高 / 低)。
2. **Role distribution divergence**: role × person 分布が cohort norm から KL 大。
3. **Multi-source agreement check**: 同 person を複数 source が示す時、credit 数の
   source 間 z-score 外れ値で「片方の source が誤マッチしている可能性」を示唆。

Flag は確証ではなく **review priority**。entity resolution の自動修復は行わない。

H1: anime.score 非依存。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Poisson outlier
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PoissonOutlier:
    """ある期間の credit 数が Poisson 期待値から大きく外れる person。"""

    person_id: str
    observed: int
    expected: float
    z_score: float
    direction: str   # "high" or "low"


def detect_poisson_outliers(
    person_credits: dict[str, int],
    *,
    z_threshold: float = 3.0,
    min_expected: float = 5.0,
) -> list[PoissonOutlier]:
    """Cohort 内で credit 数の Poisson outlier を検出。

    Poisson の場合 SD ≈ sqrt(mean)。z_score = (obs - mean) / sqrt(mean)。
    expected < min_expected (= mean count) では z が大きく振れすぎるため skip。

    Args:
        person_credits: {person_id: n_credits} (single cohort/period のみ)。
        z_threshold: |z| 閾値 (デフォルト 3 σ)。
        min_expected: cohort mean がこれ未満なら検出を見送る。

    Returns:
        |z| >= threshold な outlier list、|z| 降順。
    """
    if not person_credits:
        return []
    values = np.array(list(person_credits.values()), dtype=float)
    mu = float(values.mean())
    if mu < min_expected:
        log.debug("poisson_outliers_skipped_low_mean", mean=mu, threshold=min_expected)
        return []
    sd = float(np.sqrt(mu))
    out: list[PoissonOutlier] = []
    for pid, n in person_credits.items():
        z = (n - mu) / sd if sd > 0 else 0.0
        if abs(z) >= z_threshold:
            out.append(
                PoissonOutlier(
                    person_id=pid,
                    observed=int(n),
                    expected=mu,
                    z_score=float(z),
                    direction="high" if z > 0 else "low",
                )
            )
    out.sort(key=lambda o: -abs(o.z_score))
    return out


# ---------------------------------------------------------------------------
# Role distribution divergence
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RoleDivergence:
    """Role 分布が cohort norm から逸脱する person。"""

    person_id: str
    kl_divergence: float
    n_credits: int
    dominant_role: str
    dominant_role_share: float


def _kl_divergence(p: np.ndarray, q: np.ndarray) -> float:
    """KL(p || q)。p, q は probability vectors (sum=1)。"""
    eps = 1e-9
    p_safe = np.clip(p, eps, 1.0)
    q_safe = np.clip(q, eps, 1.0)
    return float(np.sum(p_safe * np.log(p_safe / q_safe)))


def detect_role_divergence(
    person_role_counts: dict[str, dict[str, int]],
    *,
    kl_threshold: float = 1.5,
    min_credits: int = 10,
) -> list[RoleDivergence]:
    """各 person の役職分布が cohort marginal から KL 大きい outlier を検出。

    Args:
        person_role_counts: {person_id: {role: n_credits}}.
        kl_threshold: KL >= 閾値で flag。1.5 ≒ 強い乖離 (一般的目安)。
        min_credits: 総 credits 数下限 (これ未満は noise)。

    Returns:
        KL 降順 list。
    """
    if not person_role_counts:
        return []
    # marginal cohort role distribution
    all_roles: set[str] = set()
    for roles in person_role_counts.values():
        all_roles.update(roles.keys())
    roles_sorted = sorted(all_roles)
    if not roles_sorted:
        return []
    marg = np.zeros(len(roles_sorted), dtype=float)
    for roles in person_role_counts.values():
        for i, r in enumerate(roles_sorted):
            marg[i] += roles.get(r, 0)
    if marg.sum() == 0:
        return []
    marg /= marg.sum()

    results: list[RoleDivergence] = []
    for pid, roles in person_role_counts.items():
        n_total = sum(roles.values())
        if n_total < min_credits:
            continue
        p = np.array([roles.get(r, 0) for r in roles_sorted], dtype=float)
        p = p / p.sum()
        kl = _kl_divergence(p, marg)
        if kl < kl_threshold:
            continue
        max_idx = int(np.argmax(p))
        results.append(
            RoleDivergence(
                person_id=pid,
                kl_divergence=float(kl),
                n_credits=int(n_total),
                dominant_role=roles_sorted[max_idx],
                dominant_role_share=float(p[max_idx]),
            )
        )
    results.sort(key=lambda r: -r.kl_divergence)
    return results


# ---------------------------------------------------------------------------
# Multi-source agreement check
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceDisagreement:
    """同一 canonical person を複数 source が credit 数で大きく食い違わせる。"""

    canonical_id: str
    source_counts: dict[str, int]
    max_count: int
    min_count: int
    spread_ratio: float       # max / max(min, 1)
    z_max: float              # most-deviating source z-score within this person


def detect_source_disagreement(
    canonical_source_counts: dict[str, dict[str, int]],
    *,
    spread_threshold: float = 4.0,
    z_threshold: float = 2.5,
    min_total: int = 10,
) -> list[SourceDisagreement]:
    """同 canonical id に対し source 間で credit 数が大きく違う person を検出。

    Args:
        canonical_source_counts: {canonical_id: {source: n_credits}}.
        spread_threshold: max_count / max(min_count, 1) 閾値。
        z_threshold: 個別 source の z (cohort mean / sd) 閾値。
        min_total: 全 source 合計 credit 下限。

    Returns:
        spread 降順 list。
    """
    if not canonical_source_counts:
        return []
    # Per-source cohort statistics (across canonical ids that have that source)
    by_source: dict[str, list[int]] = {}
    for src_counts in canonical_source_counts.values():
        for src, n in src_counts.items():
            by_source.setdefault(src, []).append(n)
    src_stats = {
        s: (float(np.mean(vals)), float(np.std(vals)) if len(vals) > 1 else 1.0)
        for s, vals in by_source.items()
    }

    out: list[SourceDisagreement] = []
    for cid, src_counts in canonical_source_counts.items():
        if not src_counts:
            continue
        total = sum(src_counts.values())
        if total < min_total:
            continue
        counts = list(src_counts.values())
        max_c = int(max(counts))
        min_c = int(min(counts))
        spread = max_c / max(min_c, 1)
        if spread < spread_threshold:
            continue
        z_vals = []
        for src, n in src_counts.items():
            mu, sd = src_stats.get(src, (0.0, 1.0))
            z = (n - mu) / sd if sd > 0 else 0.0
            z_vals.append(z)
        z_max = float(max(z_vals, key=abs)) if z_vals else 0.0
        if abs(z_max) < z_threshold:
            continue
        out.append(
            SourceDisagreement(
                canonical_id=cid,
                source_counts=dict(src_counts),
                max_count=max_c,
                min_count=min_c,
                spread_ratio=float(spread),
                z_max=z_max,
            )
        )
    out.sort(key=lambda s: -s.spread_ratio)
    return out

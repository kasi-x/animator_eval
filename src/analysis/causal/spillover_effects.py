"""Spillover (peer effect) estimation — Manski (1993) reflection 問題回避 spec。

同 anime 共演 peer の構造的位置 (theta_i 平均) が、個人の theta 推移に与える影響を
推定。naive な「peer mean が effect」は reflection 問題 (互いを反映するため
同時推定不可) を含む → instrumental variable (peer of peer = exclusion restriction)
で identification する。

本実装は **2 stage least squares (2SLS) proxy**:

  Stage 1: peer_mean_theta_self ~ peer_of_peer_mean_theta (excluding self/peer)
  Stage 2: theta_self_year_t ~ predicted_peer_mean + own_lagged + controls

H1: anime.score 非依存。
H2: 「影響力ランキング」frame NG → "peer mean に対する coefficient" のみ。

References:
    - Manski (1993) "Identification of endogenous social effects"
    - Bramoullé, Djebbari, Fortin (2009) "Identification of peer effects via networks"
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from collections.abc import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class PeerObservation:
    """1 (person, year, anime) tuple の peer 環境."""

    person_id: str
    year: int
    anime_id: str
    own_theta: float
    peer_mean_theta: float           # 同 anime 共演者の平均 theta
    peer_of_peer_mean_theta: float    # 2-hop peer の平均 (IV)
    n_peers: int


@dataclass(frozen=True)
class SpilloverEstimate:
    """2SLS 推定結果."""

    beta_peer: float                  # peer effect coefficient
    se_peer: float
    ci_low: float
    ci_high: float
    first_stage_f: float              # weak IV diagnostic (≥ 10 推奨)
    weak_iv_flag: bool                # F < 10
    n_obs: int
    n_unique_persons: int
    notes: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Peer mean computation
# ---------------------------------------------------------------------------


def compute_peer_means(
    credits: Sequence[tuple[str, str, int]],  # (person_id, anime_id, year)
    theta_map: dict[str, float],
) -> list[PeerObservation]:
    """各 (person, anime, year) tuple に対し peer_mean と peer_of_peer_mean を集計。

    Args:
        credits: 共起クレジット。
        theta_map: {person_id: theta_i}.

    Returns:
        PeerObservation list. peer 不在 (n_peers=0) は除外。
    """
    anime_to_persons: dict[str, set[str]] = defaultdict(set)
    for pid, aid, _yr in credits:
        if pid and aid and pid in theta_map:
            anime_to_persons[aid].add(pid)

    obs: list[PeerObservation] = []
    for pid, aid, yr in credits:
        if pid not in theta_map or aid not in anime_to_persons:
            continue
        peers = anime_to_persons[aid] - {pid}
        peers_with_theta = [p for p in peers if p in theta_map]
        if not peers_with_theta:
            continue
        peer_thetas = [theta_map[p] for p in peers_with_theta]
        peer_mean = float(np.mean(peer_thetas))

        # 2-hop: peer の同共演者 (peer-of-peer, excluding pid & immediate peers)
        peer_of_peer_thetas: list[float] = []
        for peer in peers_with_theta:
            # peer が出てる他 anime での共演者
            for a2 in (a for a, ps in anime_to_persons.items() if peer in ps and a != aid):
                for q in anime_to_persons[a2]:
                    if q != pid and q not in peers and q in theta_map:
                        peer_of_peer_thetas.append(theta_map[q])
        if not peer_of_peer_thetas:
            continue
        pop_mean = float(np.mean(peer_of_peer_thetas))

        obs.append(
            PeerObservation(
                person_id=str(pid),
                year=int(yr),
                anime_id=str(aid),
                own_theta=float(theta_map[pid]),
                peer_mean_theta=peer_mean,
                peer_of_peer_mean_theta=pop_mean,
                n_peers=len(peers_with_theta),
            )
        )
    return obs


# ---------------------------------------------------------------------------
# 2SLS estimation
# ---------------------------------------------------------------------------


def estimate_spillover_2sls(
    observations: Sequence[PeerObservation],
    *,
    weak_iv_threshold: float = 10.0,
) -> SpilloverEstimate:
    """2SLS で peer_mean → own_theta の effect を推定。

    Stage 1: peer_mean = α + γ × peer_of_peer_mean + e1
    Stage 2: own_theta = β0 + β_peer × hat(peer_mean) + e2

    Returns SpilloverEstimate with β_peer (point), SE, CI, first-stage F-stat.
    """
    if not observations:
        return SpilloverEstimate(
            beta_peer=0.0, se_peer=0.0, ci_low=0.0, ci_high=0.0,
            first_stage_f=0.0, weak_iv_flag=True,
            n_obs=0, n_unique_persons=0,
            notes=("empty input",),
        )

    y = np.array([o.own_theta for o in observations], dtype=float)
    x_peer = np.array([o.peer_mean_theta for o in observations], dtype=float)
    z_iv = np.array([o.peer_of_peer_mean_theta for o in observations], dtype=float)
    n = y.size
    n_unique = len({o.person_id for o in observations})

    # Stage 1: peer ~ z
    z_full = np.column_stack([np.ones(n), z_iv])
    try:
        beta1, _, _, _ = np.linalg.lstsq(z_full, x_peer, rcond=None)
    except np.linalg.LinAlgError:
        return SpilloverEstimate(
            beta_peer=0.0, se_peer=0.0, ci_low=0.0, ci_high=0.0,
            first_stage_f=0.0, weak_iv_flag=True,
            n_obs=n, n_unique_persons=n_unique,
            notes=("stage 1 LinAlgError",),
        )
    x_hat = z_full @ beta1
    resid1 = x_peer - x_hat
    ss_resid1 = float(np.sum(resid1 ** 2))
    ss_total1 = float(np.sum((x_peer - x_peer.mean()) ** 2))
    if ss_total1 <= 0 or n - 2 <= 0:
        first_stage_f = 0.0
    else:
        r2 = 1.0 - ss_resid1 / ss_total1
        # F for 1 instrument: F = R² (n - 2) / (1 - R²)
        first_stage_f = float(r2 * (n - 2) / max(1.0 - r2, 1e-12))

    # Stage 2: y ~ x_hat
    x2 = np.column_stack([np.ones(n), x_hat])
    beta2, _, _, _ = np.linalg.lstsq(x2, y, rcond=None)
    resid2 = y - x2 @ beta2
    # SE for β_peer via standard OLS formula
    sigma2 = float(np.sum(resid2 ** 2) / max(n - 2, 1))
    try:
        cov = sigma2 * np.linalg.pinv(x2.T @ x2)
        se_peer = float(np.sqrt(max(cov[1, 1], 0.0)))
    except np.linalg.LinAlgError:
        se_peer = 0.0

    beta_peer = float(beta2[1])
    ci_low = beta_peer - 1.96 * se_peer
    ci_high = beta_peer + 1.96 * se_peer

    notes: list[str] = []
    if first_stage_f < weak_iv_threshold:
        notes.append(
            f"weak IV: first-stage F = {first_stage_f:.2f} < {weak_iv_threshold}. "
            "β_peer estimate likely biased toward OLS; use cautiously."
        )

    return SpilloverEstimate(
        beta_peer=beta_peer,
        se_peer=se_peer,
        ci_low=ci_low,
        ci_high=ci_high,
        first_stage_f=first_stage_f,
        weak_iv_flag=first_stage_f < weak_iv_threshold,
        n_obs=n,
        n_unique_persons=n_unique,
        notes=tuple(notes),
    )

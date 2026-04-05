"""クラスタ命名ヘルパー."""

from __future__ import annotations

import numpy as np


def name_clusters_by_rank(
    centers: np.ndarray,
    feat_specs: list[tuple[int, list[str]]],
) -> list[str]:
    """セントロイドの相対ランクに基づくクラスタ命名.

    Args:
        centers: (K, D) のクラスタ中心（逆変換済み実値）
        feat_specs: [(feature_index, [label_highest, ..., label_lowest]), ...]
            ラベル数 <= K。ランク1位→label_highest, 最下位→label_lowest。
    """
    k = centers.shape[0]
    names: list[list[str]] = [[] for _ in range(k)]

    for feat_idx, labels in feat_specs:
        vals = centers[:, feat_idx]
        order = np.argsort(-vals)  # 降順
        n_labels = len(labels)
        # ランクに応じてラベル割当（上位→labels[0], 下位→labels[-1]）
        for rank, cluster_idx in enumerate(order):
            if n_labels == 1:
                label_idx = 0 if rank == 0 else -1
            else:
                label_idx = min(rank, n_labels - 1)
            if rank < n_labels:
                names[cluster_idx].append(labels[label_idx])

    return [" / ".join(parts) if parts else f"Cluster {i}" for i, parts in enumerate(names)]


def name_clusters_distinctive(
    centers: np.ndarray,
    feature_names: list[str],
    top_n: int = 3,
) -> list[str]:
    """z-scoreが大きい上位特徴量でクラスタ命名.

    各クラスタのセントロイドを全クラスタ平均からのz-scoreで評価し、
    |z|が大きい上位top_n個の特徴量で命名。
    """
    k = centers.shape[0]
    mean = centers.mean(axis=0)
    std = centers.std(axis=0)
    std[std < 1e-10] = 1.0  # ゼロ除算防止

    names = []
    for i in range(k):
        z = (centers[i] - mean) / std
        top_idx = np.argsort(-np.abs(z))[:top_n]
        parts = []
        for idx in top_idx:
            direction = "高" if z[idx] > 0 else "低"
            parts.append(f"{direction}{feature_names[idx]}")
        names.append("・".join(parts))

    # 重複回避
    seen: dict[str, int] = {}
    for i, name in enumerate(names):
        if name in seen:
            seen[name] += 1
            names[i] = f"{name} ({seen[name]})"
        else:
            seen[name] = 1

    return names

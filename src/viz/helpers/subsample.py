"""サブサンプリング・アダプティブ描画ヘルパー."""

from __future__ import annotations

import random


def subsample_items(
    items: list[dict],
    max_n: int = 5000,
    seed: int = 42,
) -> list[dict]:
    """dictリストをmax_n以下にランダムサブサンプル."""
    if len(items) <= max_n:
        return items
    rng = random.Random(seed)
    return rng.sample(items, max_n)


def subsample_arrays(
    *arrays: tuple[float, ...],
    max_n: int = 5000,
    seed: int = 42,
) -> tuple[tuple[float, ...], ...]:
    """複数の同長配列を同じインデックスでサブサンプル."""
    n = len(arrays[0])
    if n <= max_n:
        return arrays
    rng = random.Random(seed)
    indices = sorted(rng.sample(range(n), max_n))
    return tuple(
        tuple(arr[i] for i in indices)
        for arr in arrays
    )


def raincloud_mode(n: int) -> str:
    """サンプルサイズに応じたraincloud描画モードを返す.

    Returns: "box" (n<5), "box_jitter" (n<40), "violin" (n>=40)
    """
    if n < 5:
        return "box"
    if n < 40:
        return "box_jitter"
    return "violin"

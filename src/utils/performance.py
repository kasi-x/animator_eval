"""パフォーマンス計測・モニタリング.

パイプライン実行時の各ステージの実行時間、メモリ使用量、
キャッシュヒット率を追跡する。
"""

import functools
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Callable

import psutil
import structlog

logger = structlog.get_logger()


class PerformanceMonitor:
    """パフォーマンスメトリクス収集."""

    def __init__(self) -> None:
        self.timings: dict[str, list[float]] = defaultdict(list)
        self.memory_snapshots: dict[str, float] = {}
        self.counters: dict[str, int] = defaultdict(int)
        self.cache_hits = 0
        self.cache_misses = 0
        self._process = psutil.Process()

    def record_timing(self, operation: str, duration: float) -> None:
        """実行時間を記録."""
        self.timings[operation].append(duration)
        logger.debug("timing_recorded", operation=operation, duration=round(duration, 3))

    def record_memory(self, checkpoint: str) -> None:
        """メモリ使用量を記録."""
        mem_mb = self._process.memory_info().rss / 1024 / 1024
        self.memory_snapshots[checkpoint] = mem_mb
        logger.debug("memory_snapshot", checkpoint=checkpoint, memory_mb=round(mem_mb, 1))

    def increment_counter(self, name: str, amount: int = 1) -> None:
        """カウンタをインクリメント."""
        self.counters[name] += amount

    def record_cache_hit(self) -> None:
        """キャッシュヒットを記録."""
        self.cache_hits += 1

    def record_cache_miss(self) -> None:
        """キャッシュミスを記録."""
        self.cache_misses += 1

    @contextmanager
    def measure(self, operation: str):
        """コンテキストマネージャで実行時間を計測."""
        start = time.monotonic()
        try:
            yield
        finally:
            duration = time.monotonic() - start
            self.record_timing(operation, duration)

    def get_summary(self) -> dict[str, Any]:
        """メトリクスサマリーを取得."""
        timing_summary = {}
        for op, durations in self.timings.items():
            if durations:
                timing_summary[op] = {
                    "count": len(durations),
                    "total": round(sum(durations), 3),
                    "avg": round(sum(durations) / len(durations), 3),
                    "min": round(min(durations), 3),
                    "max": round(max(durations), 3),
                }

        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / cache_total if cache_total > 0 else 0

        return {
            "timings": timing_summary,
            "memory_snapshots": {k: round(v, 1) for k, v in self.memory_snapshots.items()},
            "counters": dict(self.counters),
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate": round(cache_hit_rate, 3),
            },
        }

    def log_summary(self) -> None:
        """サマリーをログ出力."""
        summary = self.get_summary()
        logger.info("performance_summary", **summary)


# Global monitor instance
_monitor = PerformanceMonitor()


def get_monitor() -> PerformanceMonitor:
    """グローバルモニターインスタンスを取得."""
    return _monitor


def reset_monitor() -> None:
    """モニターをリセット（テスト用）."""
    global _monitor
    _monitor = PerformanceMonitor()


def timed(operation: str | None = None):
    """関数の実行時間を自動計測するデコレータ."""

    def decorator(func: Callable) -> Callable:
        op_name = operation or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with get_monitor().measure(op_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator

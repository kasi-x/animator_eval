"""パフォーマンス計測・モニタリング.

パイプライン実行時の各ステージの実行時間、メモリ使用量、
キャッシュヒット率を追跡する。

Enhanced version with:
- Percentile tracking (p50, p95, p99)
- Memory delta analysis
- Phase tagging
- JSON export for analysis
- Rich visualization support
"""

import functools
import json
import statistics
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import psutil
import structlog

logger = structlog.get_logger()


@dataclass
class TimingStats:
    """Timing statistics for an operation."""

    operation: str
    count: int
    total: float
    avg: float
    min: float
    max: float
    median: float
    p95: float
    p99: float
    stddev: float


@dataclass
class MemorySnapshot:
    """Memory usage snapshot at a checkpoint."""

    checkpoint: str
    timestamp: float
    rss_mb: float
    vms_mb: float
    percent: float
    delta_mb: float | None = None  # Delta from previous snapshot


@dataclass
class PerformanceReport:
    """Complete performance report for a pipeline run."""

    timestamp: str
    total_duration: float
    timings: list[TimingStats] = field(default_factory=list)
    memory_snapshots: list[MemorySnapshot] = field(default_factory=list)
    cache_stats: dict[str, Any] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    peak_memory_mb: float = 0.0
    total_memory_delta_mb: float = 0.0


class PerformanceMonitor:
    """パフォーマンスメトリクス収集."""

    def __init__(self) -> None:
        self.timings: dict[str, list[float]] = defaultdict(list)
        self.memory_snapshots_list: list[MemorySnapshot] = []
        self.counters: dict[str, int] = defaultdict(int)
        self.cache_hits = 0
        self.cache_misses = 0
        self._process = psutil.Process()
        self._start_time = time.monotonic()
        self._tags: dict[str, str] = {}  # Operation tags for grouping

    def record_timing(self, operation: str, duration: float) -> None:
        """実行時間を記録."""
        self.timings[operation].append(duration)
        logger.debug(
            "timing_recorded", operation=operation, duration=round(duration, 3)
        )

    def record_memory(self, checkpoint: str) -> None:
        """メモリ使用量を記録（RSS, VMS, percent）."""
        mem_info = self._process.memory_info()
        mem_percent = self._process.memory_percent()

        rss_mb = mem_info.rss / 1024 / 1024
        vms_mb = mem_info.vms / 1024 / 1024

        # Calculate delta from previous snapshot
        delta_mb = None
        if self.memory_snapshots_list:
            prev_rss = self.memory_snapshots_list[-1].rss_mb
            delta_mb = rss_mb - prev_rss

        snapshot = MemorySnapshot(
            checkpoint=checkpoint,
            timestamp=time.monotonic() - self._start_time,
            rss_mb=rss_mb,
            vms_mb=vms_mb,
            percent=mem_percent,
            delta_mb=delta_mb,
        )
        self.memory_snapshots_list.append(snapshot)

        logger.debug(
            "memory_snapshot",
            checkpoint=checkpoint,
            rss_mb=round(rss_mb, 1),
            delta_mb=round(delta_mb, 1) if delta_mb else None,
        )

    def increment_counter(self, name: str, amount: int = 1) -> None:
        """カウンタをインクリメント."""
        self.counters[name] += amount

    def record_cache_hit(self) -> None:
        """キャッシュヒットを記録."""
        self.cache_hits += 1

    def record_cache_miss(self) -> None:
        """キャッシュミスを記録."""
        self.cache_misses += 1

    def tag_operation(self, operation: str, tag: str) -> None:
        """オペレーションにタグを付ける（グルーピング用）."""
        self._tags[operation] = tag

    def get_timing_stats(self, operation: str) -> TimingStats | None:
        """特定のオペレーションの詳細統計を取得."""
        durations = self.timings.get(operation, [])
        if not durations:
            return None

        sorted_durations = sorted(durations)
        n = len(sorted_durations)

        return TimingStats(
            operation=operation,
            count=n,
            total=round(sum(durations), 3),
            avg=round(statistics.mean(durations), 3),
            min=round(min(durations), 3),
            max=round(max(durations), 3),
            median=round(statistics.median(durations), 3),
            p95=round(
                sorted_durations[int(n * 0.95)] if n > 1 else sorted_durations[0], 3
            ),
            p99=round(
                sorted_durations[int(n * 0.99)] if n > 1 else sorted_durations[0], 3
            ),
            stddev=round(statistics.stdev(durations) if n > 1 else 0.0, 3),
        )

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
        """メトリクスサマリーを取得（後方互換性のため保持）."""
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

        # Convert memory snapshots for backward compatibility
        memory_dict = {}
        for snapshot in self.memory_snapshots_list:
            memory_dict[snapshot.checkpoint] = round(snapshot.rss_mb, 1)

        return {
            "timings": timing_summary,
            "memory_snapshots": memory_dict,
            "counters": dict(self.counters),
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate": round(cache_hit_rate, 3),
            },
        }

    def generate_report(self) -> PerformanceReport:
        """詳細なパフォーマンスレポートを生成."""
        total_duration = time.monotonic() - self._start_time

        # Generate timing stats for all operations
        timing_stats = []
        for operation in sorted(self.timings.keys()):
            stats = self.get_timing_stats(operation)
            if stats:
                timing_stats.append(stats)

        # Calculate peak memory and total delta
        peak_memory = max((s.rss_mb for s in self.memory_snapshots_list), default=0.0)
        total_delta = 0.0
        if len(self.memory_snapshots_list) >= 2:
            total_delta = (
                self.memory_snapshots_list[-1].rss_mb
                - self.memory_snapshots_list[0].rss_mb
            )

        # Cache stats
        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / cache_total if cache_total > 0 else 0
        cache_stats = {
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "total": cache_total,
            "hit_rate": round(cache_hit_rate, 3),
        }

        return PerformanceReport(
            timestamp=datetime.now().isoformat(),
            total_duration=round(total_duration, 3),
            timings=timing_stats,
            memory_snapshots=self.memory_snapshots_list,
            cache_stats=cache_stats,
            counters=dict(self.counters),
            peak_memory_mb=round(peak_memory, 1),
            total_memory_delta_mb=round(total_delta, 1),
        )

    def export_report(self, output_path: Path | str) -> None:
        """レポートをJSON形式で出力."""
        report = self.generate_report()
        output_path = Path(output_path)

        # Convert dataclasses to dict
        report_dict = asdict(report)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)

        logger.info("performance_report_exported", path=str(output_path))

    def log_summary(self) -> None:
        """サマリーをログ出力."""
        summary = self.get_summary()
        logger.info("performance_summary", **summary)

    def print_report(self, show_percentiles: bool = True) -> None:
        """Rich tableでレポートを表示（CLI用）."""
        try:
            from rich.console import Console
            from rich.table import Table
        except ImportError:
            logger.warning("rich not installed, skipping visualization")
            return

        console = Console()
        report = self.generate_report()

        # Summary table
        console.print("\n[bold blue]Performance Report[/bold blue]\n")
        summary = Table(title="Summary")
        summary.add_column("Metric", style="cyan")
        summary.add_column("Value", style="green")
        summary.add_row("Total Duration", f"{report.total_duration:.2f}s")
        summary.add_row("Peak Memory", f"{report.peak_memory_mb:.1f} MB")
        summary.add_row("Memory Delta", f"{report.total_memory_delta_mb:+.1f} MB")
        summary.add_row("Cache Hit Rate", f"{report.cache_stats['hit_rate']:.1%}")
        console.print(summary)

        # Timing table
        if report.timings:
            timing_table = Table(title="Operation Timings")
            timing_table.add_column("Operation", style="cyan")
            timing_table.add_column("Count", justify="right")
            timing_table.add_column("Total", justify="right", style="yellow")
            timing_table.add_column("Avg", justify="right", style="green")
            if show_percentiles:
                timing_table.add_column("P50", justify="right", style="dim")
                timing_table.add_column("P95", justify="right", style="dim")
                timing_table.add_column("P99", justify="right", style="dim")
            timing_table.add_column("Min", justify="right", style="dim")
            timing_table.add_column("Max", justify="right", style="red")

            for stats in sorted(report.timings, key=lambda x: x.total, reverse=True):
                row = [
                    stats.operation,
                    str(stats.count),
                    f"{stats.total:.3f}s",
                    f"{stats.avg:.3f}s",
                ]
                if show_percentiles:
                    row.extend(
                        [
                            f"{stats.median:.3f}s",
                            f"{stats.p95:.3f}s",
                            f"{stats.p99:.3f}s",
                        ]
                    )
                row.extend(
                    [
                        f"{stats.min:.3f}s",
                        f"{stats.max:.3f}s",
                    ]
                )
                timing_table.add_row(*row)

            console.print(timing_table)

        # Memory snapshots table
        if report.memory_snapshots:
            mem_table = Table(title="Memory Snapshots")
            mem_table.add_column("Checkpoint", style="cyan")
            mem_table.add_column("Time", justify="right", style="dim")
            mem_table.add_column("RSS (MB)", justify="right", style="green")
            mem_table.add_column("Delta", justify="right", style="yellow")
            mem_table.add_column("% Used", justify="right", style="magenta")

            for snapshot in report.memory_snapshots:
                delta_str = (
                    f"{snapshot.delta_mb:+.1f}"
                    if snapshot.delta_mb is not None
                    else "-"
                )
                mem_table.add_row(
                    snapshot.checkpoint,
                    f"{snapshot.timestamp:.2f}s",
                    f"{snapshot.rss_mb:.1f}",
                    delta_str,
                    f"{snapshot.percent:.1f}%",
                )

            console.print(mem_table)


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

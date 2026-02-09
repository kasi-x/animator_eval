"""performance モジュールのテスト."""

import time

import pytest

from src.utils.performance import PerformanceMonitor, get_monitor, reset_monitor, timed


class TestPerformanceMonitor:
    def test_record_timing(self):
        monitor = PerformanceMonitor()
        monitor.record_timing("test_op", 1.5)
        monitor.record_timing("test_op", 2.0)

        summary = monitor.get_summary()
        assert "test_op" in summary["timings"]
        assert summary["timings"]["test_op"]["count"] == 2
        assert summary["timings"]["test_op"]["total"] == 3.5
        assert summary["timings"]["test_op"]["avg"] == 1.75
        assert summary["timings"]["test_op"]["min"] == 1.5
        assert summary["timings"]["test_op"]["max"] == 2.0

    def test_record_memory(self):
        monitor = PerformanceMonitor()
        monitor.record_memory("checkpoint1")

        summary = monitor.get_summary()
        assert "checkpoint1" in summary["memory_snapshots"]
        assert summary["memory_snapshots"]["checkpoint1"] > 0

    def test_increment_counter(self):
        monitor = PerformanceMonitor()
        monitor.increment_counter("items_processed")
        monitor.increment_counter("items_processed", 5)

        summary = monitor.get_summary()
        assert summary["counters"]["items_processed"] == 6

    def test_cache_hit_miss(self):
        monitor = PerformanceMonitor()
        monitor.record_cache_hit()
        monitor.record_cache_hit()
        monitor.record_cache_miss()

        summary = monitor.get_summary()
        assert summary["cache"]["hits"] == 2
        assert summary["cache"]["misses"] == 1
        assert summary["cache"]["hit_rate"] == 0.667

    def test_cache_hit_rate_zero_total(self):
        monitor = PerformanceMonitor()
        summary = monitor.get_summary()
        assert summary["cache"]["hit_rate"] == 0

    def test_measure_context_manager(self):
        monitor = PerformanceMonitor()

        with monitor.measure("sleep_op"):
            time.sleep(0.01)

        summary = monitor.get_summary()
        assert "sleep_op" in summary["timings"]
        assert summary["timings"]["sleep_op"]["count"] == 1
        assert summary["timings"]["sleep_op"]["total"] >= 0.01

    def test_measure_with_exception(self):
        """例外が発生してもタイミングは記録される."""
        monitor = PerformanceMonitor()

        with pytest.raises(ValueError):
            with monitor.measure("error_op"):
                time.sleep(0.01)
                raise ValueError("test error")

        summary = monitor.get_summary()
        assert "error_op" in summary["timings"]
        assert summary["timings"]["error_op"]["count"] == 1

    def test_get_summary_empty(self):
        monitor = PerformanceMonitor()
        summary = monitor.get_summary()

        assert summary["timings"] == {}
        assert summary["memory_snapshots"] == {}
        assert summary["counters"] == {}
        assert summary["cache"]["hits"] == 0
        assert summary["cache"]["misses"] == 0

    def test_summary_rounds_values(self):
        monitor = PerformanceMonitor()
        monitor.record_timing("op", 1.23456789)

        summary = monitor.get_summary()
        assert summary["timings"]["op"]["total"] == 1.235
        assert summary["timings"]["op"]["avg"] == 1.235


class TestGlobalMonitor:
    def test_get_monitor_returns_singleton(self):
        m1 = get_monitor()
        m2 = get_monitor()
        assert m1 is m2

    def test_reset_monitor_creates_new_instance(self):
        m1 = get_monitor()
        m1.record_timing("op", 1.0)

        reset_monitor()
        m2 = get_monitor()

        assert m1 is not m2
        summary = m2.get_summary()
        assert summary["timings"] == {}

    def test_global_monitor_shared_state(self):
        reset_monitor()
        monitor = get_monitor()
        monitor.record_timing("shared_op", 1.0)

        monitor2 = get_monitor()
        summary = monitor2.get_summary()
        assert "shared_op" in summary["timings"]


class TestTimedDecorator:
    def test_timed_decorator_default_name(self):
        reset_monitor()

        @timed()
        def sample_function():
            time.sleep(0.01)
            return "result"

        result = sample_function()
        assert result == "result"

        summary = get_monitor().get_summary()
        assert "tests.test_performance.sample_function" in summary["timings"]

    def test_timed_decorator_custom_name(self):
        reset_monitor()

        @timed(operation="custom_op")
        def sample_function():
            time.sleep(0.01)
            return 42

        result = sample_function()
        assert result == 42

        summary = get_monitor().get_summary()
        assert "custom_op" in summary["timings"]

    def test_timed_decorator_preserves_function_metadata(self):
        @timed()
        def documented_function():
            """This is a docstring."""
            pass

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is a docstring."

    def test_timed_decorator_with_args_and_kwargs(self):
        reset_monitor()

        @timed(operation="add_op")
        def add(a, b, multiplier=1):
            return (a + b) * multiplier

        result = add(2, 3, multiplier=10)
        assert result == 50

        summary = get_monitor().get_summary()
        assert "add_op" in summary["timings"]

    def test_timed_decorator_with_exception(self):
        reset_monitor()

        @timed(operation="error_func")
        def failing_function():
            raise RuntimeError("deliberate error")

        with pytest.raises(RuntimeError):
            failing_function()

        # タイミングは記録されている
        summary = get_monitor().get_summary()
        assert "error_func" in summary["timings"]


class TestPerformanceIntegration:
    def test_multiple_operations(self):
        """複数の操作を計測してサマリーを生成."""
        reset_monitor()
        monitor = get_monitor()

        with monitor.measure("load_data"):
            time.sleep(0.01)

        monitor.increment_counter("records_loaded", 100)
        monitor.record_memory("after_load")

        with monitor.measure("process_data"):
            time.sleep(0.01)
            monitor.record_cache_hit()
            monitor.record_cache_miss()

        monitor.increment_counter("records_processed", 80)
        monitor.record_memory("after_process")

        summary = monitor.get_summary()

        assert len(summary["timings"]) == 2
        assert "load_data" in summary["timings"]
        assert "process_data" in summary["timings"]
        assert summary["counters"]["records_loaded"] == 100
        assert summary["counters"]["records_processed"] == 80
        assert len(summary["memory_snapshots"]) == 2
        assert summary["cache"]["hits"] == 1
        assert summary["cache"]["misses"] == 1

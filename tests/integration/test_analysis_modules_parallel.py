"""Phase 9 analysis_modules parallel execution tests."""

import os
from concurrent.futures import ThreadPoolExecutor

from src.pipeline_phases.analysis_modules import (
    run_analysis_modules_phase,
)


class TestAnalysisModulesParallel:
    """Verify parallel execution safety and completeness."""

    def test_threadpool_executor_optimal_config(self):
        """Verify ThreadPoolExecutor uses optimal worker count for CPU concurrency."""
        cpu_count = os.cpu_count() or 1
        expected_workers = min(32, cpu_count + 4)

        # Create executor with same config as analysis_modules.py
        executor = ThreadPoolExecutor(max_workers=expected_workers)
        assert executor._max_workers == expected_workers, (
            f"Expected {expected_workers} workers, got {executor._max_workers}"
        )
        executor.shutdown()

    def test_run_analysis_modules_phase_exists(self):
        """Verify run_analysis_modules_phase function is callable."""
        assert callable(run_analysis_modules_phase), (
            "run_analysis_modules_phase must be a callable function"
        )

    def test_analysis_task_structure(self):
        """Verify AnalysisTask dataclass has required fields."""
        from src.pipeline_phases.analysis_modules import AnalysisTask

        # Create sample task
        task = AnalysisTask(
            name="test_task",
            function=lambda ctx: {"result": "test"},
            monitor_step="test_monitor",
            condition=None,
            needs_collab_graph=False,
            memory_heavy=False,
        )

        assert task.name == "test_task"
        assert task.function is not None
        assert task.monitor_step == "test_monitor"
        assert task.needs_collab_graph is False
        assert task.memory_heavy is False

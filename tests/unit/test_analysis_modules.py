"""Tests for Phase 9: analysis_modules parallel execution."""
import threading

import pytest

from src.pipeline_phases.analysis_modules import (
    ANALYSIS_TASKS,
    AnalysisTask,
    _execute_analysis_task,
    _run_task_batch,
)
from src.pipeline_phases.context import PipelineContext


@pytest.fixture
def ctx():
    """Minimal PipelineContext with no real data — sufficient for mock task tests."""
    return PipelineContext(visualize=False, dry_run=True)


class TestAnalysisTask:
    def test_required_fields(self):
        task = AnalysisTask("t", lambda ctx: None)
        assert task.name == "t"
        assert callable(task.function)

    def test_defaults(self):
        task = AnalysisTask("t", lambda ctx: None)
        assert task.monitor_step is None
        assert task.condition is None
        assert not task.needs_collab_graph
        assert not task.memory_heavy

    def test_optional_fields(self):
        task = AnalysisTask(
            "t",
            lambda ctx: None,
            monitor_step="step",
            needs_collab_graph=True,
            memory_heavy=True,
        )
        assert task.monitor_step == "step"
        assert task.needs_collab_graph
        assert task.memory_heavy


class TestExecuteAnalysisTask:
    def test_returns_result(self, ctx):
        task = AnalysisTask("ok", lambda c: {"val": 42})
        name, result, elapsed = _execute_analysis_task(task, ctx, threading.Lock())
        assert name == "ok"
        assert result == {"val": 42}
        assert elapsed >= 0.0

    def test_condition_false_returns_none(self, ctx):
        task = AnalysisTask("skip", lambda c: "never", condition=lambda c: False)
        name, result, elapsed = _execute_analysis_task(task, ctx, threading.Lock())
        assert name == "skip"
        assert result is None
        assert elapsed == 0.0

    def test_condition_true_runs(self, ctx):
        task = AnalysisTask("run", lambda c: "yes", condition=lambda c: True)
        _, result, _ = _execute_analysis_task(task, ctx, threading.Lock())
        assert result == "yes"

    def test_exception_returns_none(self, ctx):
        def _fail(c):
            raise ValueError("boom")

        task = AnalysisTask("fail", _fail)
        name, result, _ = _execute_analysis_task(task, ctx, threading.Lock())
        assert name == "fail"
        assert result is None


class TestRunTaskBatch:
    def test_results_stored(self, ctx):
        tasks = [AnalysisTask(f"t{i}", lambda c, i=i: {"n": i}) for i in range(5)]
        lock = threading.Lock()
        completed, failed, names = _run_task_batch(tasks, ctx, lock, 4, "batch")
        assert completed == 5
        assert failed == 0
        assert set(names) == {f"t{i}" for i in range(5)}
        for i in range(5):
            assert ctx.analysis_results[f"t{i}"] == {"n": i}

    def test_failure_isolation(self, ctx):
        def _fail(c):
            raise RuntimeError("intentional")

        tasks = [
            AnalysisTask("good1", lambda c: "a"),
            AnalysisTask("bad", _fail),
            AnalysisTask("good2", lambda c: "b"),
        ]
        lock = threading.Lock()
        completed, failed, _ = _run_task_batch(tasks, ctx, lock, 3, "batch")
        assert completed == 2
        assert failed == 1
        assert ctx.analysis_results.get("good1") == "a"
        assert ctx.analysis_results.get("good2") == "b"

    def test_thread_safety(self, ctx):
        """20 concurrent tasks must all land in analysis_results without races."""
        N = 20
        tasks = [AnalysisTask(f"p{i}", lambda c, i=i: i * 10) for i in range(N)]
        lock = threading.Lock()
        _run_task_batch(tasks, ctx, lock, 8, "concurrent")
        stored = {k: v for k, v in ctx.analysis_results.items() if k.startswith("p")}
        assert len(stored) == N

    def test_none_result_not_stored(self, ctx):
        tasks = [AnalysisTask("none", lambda c: None)]
        lock = threading.Lock()
        completed, failed, _ = _run_task_batch(tasks, ctx, lock, 1, "batch")
        assert completed == 0
        assert failed == 1
        assert "none" not in ctx.analysis_results

    def test_empty_task_list(self, ctx):
        lock = threading.Lock()
        completed, failed, names = _run_task_batch([], ctx, lock, 1, "batch")
        assert completed == 0
        assert failed == 0
        assert names == []


class TestAnalysisTasksList:
    def test_batch_split_covers_all(self):
        batch1 = {t.name for t in ANALYSIS_TASKS if t.needs_collab_graph}
        batch2 = {t.name for t in ANALYSIS_TASKS if not t.needs_collab_graph}
        assert batch1.isdisjoint(batch2)
        assert len(batch1) + len(batch2) == len(ANALYSIS_TASKS)

    def test_unique_names(self):
        names = [t.name for t in ANALYSIS_TASKS]
        assert len(names) == len(set(names))

    def test_all_callable(self):
        for task in ANALYSIS_TASKS:
            assert callable(task.function), f"task {task.name}: non-callable function"

    def test_has_tasks(self):
        assert len(ANALYSIS_TASKS) > 10

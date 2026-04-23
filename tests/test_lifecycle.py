"""Tests for Hamilton lifecycle hooks — TimingHook and CheckpointHook (§5.6)."""

from __future__ import annotations

import gzip
import json
from unittest.mock import patch



# ---------------------------------------------------------------------------
# TimingHook
# ---------------------------------------------------------------------------


class TestTimingHook:
    def test_logs_on_node_execution(self, tmp_path):
        from src.pipeline_phases.lifecycle import TimingHook

        import time

        hook = TimingHook()
        hook.run_before_node_execution(node_name="test_node", node_tags={})
        time.sleep(0.01)
        with patch("src.pipeline_phases.lifecycle.log") as mock_log:
            hook.run_after_node_execution(
                node_name="test_node",
                node_tags={"stage": "phase5", "cost": "moderate", "domain": "scoring"},
            )
            mock_log.info.assert_called_once()
            call_kwargs = mock_log.info.call_args
            assert call_kwargs[0][0] == "node_executed"

    def test_elapsed_is_positive(self):
        from src.pipeline_phases.lifecycle import TimingHook
        import time

        hook = TimingHook()
        hook.run_before_node_execution(node_name="n", node_tags={})
        time.sleep(0.005)

        logged = {}
        with patch("src.pipeline_phases.lifecycle.log") as mock_log:
            hook.run_after_node_execution(node_name="n", node_tags={})
            _, kwargs = mock_log.info.call_args
            logged = kwargs
        assert logged.get("elapsed_s", 0) > 0


# ---------------------------------------------------------------------------
# CheckpointHook
# ---------------------------------------------------------------------------


class _FakeCtx:
    def __init__(self):
        self.credits = [1, 2, 3]
        self.persons = ["p1", "p2"]
        self.person_fe = {"p1": 0.5}
        self.studio_fe = {}
        self.birank_person_scores = {}
        self.birank_anime_scores = {}
        self.iv_scores = {"p1": 1.0}
        self.iv_scores_historical = {}
        self.iv_lambda_weights = {}
        self.patronage_scores = {}
        self.dormancy_scores = {}
        self.results = [{"person_id": "p1", "iv_score": 1.0}]
        self.analysis_results = {"anime_stats": {"a1": {"credit_count": 10}}}


class TestCheckpointHook:
    def test_saves_after_results_post_processed(self, tmp_path):
        from src.pipeline_phases.lifecycle import CheckpointHook

        hook = CheckpointHook(checkpoint_dir=tmp_path)
        ctx = _FakeCtx()

        hook.run_before_node_execution(node_name="results_post_processed", node_tags={})
        hook.run_after_node_execution(
            node_name="results_post_processed",
            node_kwargs={"ctx": ctx, "results_assembled": []},
            success=True,
        )

        ckpt_path = tmp_path / "pipeline_checkpoint.json.gz"
        assert ckpt_path.exists()
        with gzip.open(ckpt_path, "rt") as f:
            data = json.load(f)
        assert data["last_completed_phase"] == 8

    def test_no_save_on_failure(self, tmp_path):
        from src.pipeline_phases.lifecycle import CheckpointHook

        hook = CheckpointHook(checkpoint_dir=tmp_path)
        ctx = _FakeCtx()

        hook.run_before_node_execution(node_name="results_post_processed", node_tags={})
        hook.run_after_node_execution(
            node_name="results_post_processed",
            node_kwargs={"ctx": ctx},
            success=False,
        )

        assert not (tmp_path / "pipeline_checkpoint.json.gz").exists()

    def test_no_save_for_other_nodes(self, tmp_path):
        from src.pipeline_phases.lifecycle import CheckpointHook

        hook = CheckpointHook(checkpoint_dir=tmp_path)
        ctx = _FakeCtx()

        hook.run_before_node_execution(node_name="akm_estimation", node_tags={})
        hook.run_after_node_execution(
            node_name="akm_estimation",
            node_kwargs={"ctx": ctx},
            success=True,
        )

        assert not (tmp_path / "pipeline_checkpoint.json.gz").exists()

    def test_no_save_when_ctx_missing(self, tmp_path):
        from src.pipeline_phases.lifecycle import CheckpointHook

        hook = CheckpointHook(checkpoint_dir=tmp_path)

        hook.run_before_node_execution(node_name="results_post_processed", node_tags={})
        hook.run_after_node_execution(
            node_name="results_post_processed",
            node_kwargs={},  # no ctx
            success=True,
        )

        assert not (tmp_path / "pipeline_checkpoint.json.gz").exists()

    def test_checkpoint_contains_scores(self, tmp_path):
        from src.pipeline_phases.lifecycle import CheckpointHook

        hook = CheckpointHook(checkpoint_dir=tmp_path)
        ctx = _FakeCtx()

        hook.run_before_node_execution(node_name="results_post_processed", node_tags={})
        hook.run_after_node_execution(
            node_name="results_post_processed",
            node_kwargs={"ctx": ctx},
            success=True,
        )

        with gzip.open(tmp_path / "pipeline_checkpoint.json.gz", "rt") as f:
            data = json.load(f)
        assert "iv_scores" in data
        assert data["iv_scores"] == {"p1": 1.0}
        assert len(data["results"]) == 1


# ---------------------------------------------------------------------------
# Resume path — pipeline.py integration
# ---------------------------------------------------------------------------


class TestResumePathUnit:
    def test_build_phase14_driver_builds(self):
        from src.pipeline import _build_phase14_driver
        dr = _build_phase14_driver()
        assert dr is not None

    def test_build_driver_with_checkpoint(self, tmp_path, monkeypatch):
        import src.pipeline as _pl

        monkeypatch.setattr(_pl, "JSON_DIR", tmp_path)
        dr = _pl._build_driver(with_checkpoint=True)
        assert dr is not None

    def test_build_driver_without_checkpoint(self, tmp_path, monkeypatch):
        import src.pipeline as _pl

        monkeypatch.setattr(_pl, "JSON_DIR", tmp_path)
        dr = _pl._build_driver(with_checkpoint=False)
        assert dr is not None

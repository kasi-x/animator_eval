"""stability モジュールのテスト."""

import json

from src.analysis.stability import compare_scores


class TestCompareScores:
    def test_no_previous(self, tmp_path):
        current = [
            {"person_id": "p1", "iv_score": 80.0, "name": "A"},
            {"person_id": "p2", "iv_score": 60.0, "name": "B"},
        ]
        prev_path = tmp_path / "scores.json"
        result = compare_scores(current, prev_path)
        assert len(result["new_persons"]) == 2
        assert result["summary"]["total_compared"] == 0

    def test_no_changes(self, tmp_path):
        data = [
            {"person_id": "p1", "iv_score": 80.0, "name": "A"},
            {"person_id": "p2", "iv_score": 60.0, "name": "B"},
        ]
        prev_path = tmp_path / "scores.json"
        prev_path.write_text(json.dumps(data))
        result = compare_scores(data, prev_path)
        assert result["significant_changes"] == []
        assert result["summary"]["avg_delta"] == 0.0

    def test_detects_significant_change(self, tmp_path):
        previous = [
            {"person_id": "p1", "iv_score": 80.0, "name": "A"},
            {"person_id": "p2", "iv_score": 60.0, "name": "B"},
        ]
        current = [
            {"person_id": "p1", "iv_score": 95.0, "name": "A"},
            {"person_id": "p2", "iv_score": 60.0, "name": "B"},
        ]
        prev_path = tmp_path / "scores.json"
        prev_path.write_text(json.dumps(previous))
        result = compare_scores(current, prev_path, threshold=10.0)
        assert len(result["significant_changes"]) == 1
        assert result["significant_changes"][0]["person_id"] == "p1"
        assert result["significant_changes"][0]["delta"] == 15.0

    def test_detects_new_and_removed(self, tmp_path):
        previous = [
            {"person_id": "p1", "iv_score": 80.0, "name": "A"},
            {"person_id": "p2", "iv_score": 60.0, "name": "B"},
        ]
        current = [
            {"person_id": "p1", "iv_score": 80.0, "name": "A"},
            {"person_id": "p3", "iv_score": 70.0, "name": "C"},
        ]
        prev_path = tmp_path / "scores.json"
        prev_path.write_text(json.dumps(previous))
        result = compare_scores(current, prev_path)
        assert "p3" in result["new_persons"]
        assert "p2" in result["removed_persons"]

    def test_rank_changes(self, tmp_path):
        previous = [
            {"person_id": "p1", "iv_score": 90.0, "name": "A"},
            {"person_id": "p2", "iv_score": 80.0, "name": "B"},
            {"person_id": "p3", "iv_score": 70.0, "name": "C"},
            {"person_id": "p4", "iv_score": 60.0, "name": "D"},
            {"person_id": "p5", "iv_score": 50.0, "name": "E"},
            {"person_id": "p6", "iv_score": 40.0, "name": "F"},
        ]
        # p6 jumps to top
        current = [
            {"person_id": "p6", "iv_score": 95.0, "name": "F"},
            {"person_id": "p1", "iv_score": 90.0, "name": "A"},
            {"person_id": "p2", "iv_score": 80.0, "name": "B"},
            {"person_id": "p3", "iv_score": 70.0, "name": "C"},
            {"person_id": "p4", "iv_score": 60.0, "name": "D"},
            {"person_id": "p5", "iv_score": 50.0, "name": "E"},
        ]
        prev_path = tmp_path / "scores.json"
        prev_path.write_text(json.dumps(previous))
        result = compare_scores(current, prev_path, threshold=50.0)
        assert len(result["rank_changes"]) >= 1
        p6_change = next(r for r in result["rank_changes"] if r["person_id"] == "p6")
        assert p6_change["delta"] == 5  # moved up 5 ranks (6→1)

    def test_custom_threshold(self, tmp_path):
        previous = [{"person_id": "p1", "iv_score": 50.0, "name": "A"}]
        current = [{"person_id": "p1", "iv_score": 55.0, "name": "A"}]
        prev_path = tmp_path / "scores.json"
        prev_path.write_text(json.dumps(previous))

        # With threshold 10, no significant changes
        result = compare_scores(current, prev_path, threshold=10.0)
        assert result["significant_changes"] == []

        # With threshold 3, one significant change
        result = compare_scores(current, prev_path, threshold=3.0)
        assert len(result["significant_changes"]) == 1

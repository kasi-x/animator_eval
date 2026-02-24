"""outliers モジュールのテスト."""

from src.analysis.outliers import detect_outliers


def _make_results():
    """正常分布に外れ値を混ぜたデータ."""
    results = []
    # Normal cluster: scores around 50
    for i in range(20):
        results.append(
            {
                "person_id": f"p{i}",
                "name": f"Person {i}",
                "authority": 45 + i * 0.5,
                "trust": 40 + i * 0.5,
                "skill": 50 + i * 0.3,
                "composite": 45 + i * 0.4,
            }
        )
    # High outlier
    results.append(
        {
            "person_id": "p_high",
            "name": "High Outlier",
            "authority": 99.0,
            "trust": 95.0,
            "skill": 98.0,
            "composite": 97.0,
        }
    )
    # Low outlier
    results.append(
        {
            "person_id": "p_low",
            "name": "Low Outlier",
            "authority": 1.0,
            "trust": 2.0,
            "skill": 1.0,
            "composite": 1.5,
        }
    )
    return results


class TestDetectOutliers:
    def test_detects_high_outlier(self):
        results = _make_results()
        out = detect_outliers(results)
        assert out["total_outliers"] >= 1
        high_ids = [
            o["person_id"]
            for axis_data in out["axis_outliers"].values()
            for o in axis_data["high"]
        ]
        assert "p_high" in high_ids

    def test_detects_low_outlier(self):
        results = _make_results()
        out = detect_outliers(results)
        low_ids = [
            o["person_id"]
            for axis_data in out["axis_outliers"].values()
            for o in axis_data["low"]
        ]
        assert "p_low" in low_ids

    def test_all_axes_checked(self):
        results = _make_results()
        out = detect_outliers(results)
        for axis in ("authority", "trust", "skill", "composite"):
            assert axis in out["axis_outliers"]

    def test_bounds_present(self):
        results = _make_results()
        out = detect_outliers(results)
        for axis_data in out["axis_outliers"].values():
            bounds = axis_data["bounds"]
            assert "iqr_lower" in bounds
            assert "iqr_upper" in bounds
            assert "mean" in bounds
            assert "std" in bounds

    def test_outlier_person_ids_list(self):
        results = _make_results()
        out = detect_outliers(results)
        assert isinstance(out["outlier_person_ids"], list)
        assert out["total_outliers"] == len(out["outlier_person_ids"])

    def test_too_few_results(self):
        results = [{"person_id": "p1", "authority": 50, "composite": 50}]
        out = detect_outliers(results)
        assert out["total_outliers"] == 0

    def test_empty(self):
        out = detect_outliers([])
        assert out["total_outliers"] == 0

    def test_custom_axes(self):
        results = _make_results()
        out = detect_outliers(results, axes=("composite",))
        assert "composite" in out["axis_outliers"]
        assert "authority" not in out["axis_outliers"]

    def test_zscore_in_entry(self):
        results = _make_results()
        out = detect_outliers(results)
        for axis_data in out["axis_outliers"].values():
            for entry in axis_data["high"] + axis_data["low"]:
                assert "zscore" in entry
                assert "iqr_outlier" in entry
                assert "zscore_outlier" in entry

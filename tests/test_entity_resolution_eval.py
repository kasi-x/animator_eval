"""entity_resolution_eval モジュールのテスト."""

import pytest

from src.analysis.entity_resolution_eval import (
    MatchDecision,
    ResolutionReport,
    export_matches_for_review,
    format_resolution_report,
    generate_resolution_report,
)
from src.models import Person


class TestGenerateResolutionReport:
    def test_empty_persons(self):
        """空のpersonsリストで動作する."""
        report = generate_resolution_report([], {}, {})

        assert report.total_persons == 0
        assert report.total_matches == 0
        assert report.match_rate == 0.0
        assert report.decisions == []

    def test_basic_report(self):
        """基本的なレポート生成."""
        persons = [
            Person(id="mal:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="mal:2", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="anilist:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
        ]

        canonical_map = {
            "mal:2": "mal:1",
            "anilist:1": "mal:1",
        }

        strategy_breakdown = {
            "exact": {"mal:2": "mal:1"},
            "cross_source": {"anilist:1": "mal:1"},
        }

        report = generate_resolution_report(persons, canonical_map, strategy_breakdown)

        assert report.total_persons == 3
        assert report.total_matches == 2
        assert report.match_rate == pytest.approx(2 / 3)
        assert report.matches_by_strategy["exact"] == 1
        assert report.matches_by_strategy["cross_source"] == 1
        assert len(report.decisions) == 2

    def test_decision_generation(self):
        """MatchDecision が正しく生成される."""
        persons = [
            Person(id="mal:1", name_ja="渡辺信一郎"),
            Person(id="mal:2", name_ja="渡邊信一郎"),
        ]

        canonical_map = {"mal:2": "mal:1"}
        strategy_breakdown = {"similarity": {"mal:2": "mal:1"}}

        report = generate_resolution_report(persons, canonical_map, strategy_breakdown)

        assert len(report.decisions) == 1
        decision = report.decisions[0]
        assert decision.source_id == "mal:2"
        assert decision.canonical_id == "mal:1"
        assert decision.strategy == "similarity"
        assert decision.confidence == 0.85
        assert "similarity" in decision.reason.lower()


class TestFormatResolutionReport:
    def test_format_empty_report(self):
        """空のレポートをフォーマット."""
        report = ResolutionReport(
            total_persons=0,
            total_matches=0,
            match_rate=0.0,
            matches_by_strategy={},
            decisions=[],
            ambiguous_cases=[],
        )

        text = format_resolution_report(report)

        assert "ENTITY RESOLUTION REPORT" in text
        assert "Total persons: 0" in text
        assert "Total matches: 0" in text

    def test_format_with_data(self):
        """データ付きレポートをフォーマット."""
        persons = [
            Person(id="mal:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="mal:2", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
        ]

        decision = MatchDecision(
            source_id="mal:2",
            canonical_id="mal:1",
            strategy="exact",
            confidence=1.0,
            reason="Exact name match: 宮崎駿",
            source_person=persons[1],
            canonical_person=persons[0],
        )

        report = ResolutionReport(
            total_persons=2,
            total_matches=1,
            match_rate=0.5,
            matches_by_strategy={"exact": 1},
            decisions=[decision],
            ambiguous_cases=[],
        )

        text = format_resolution_report(report)

        assert "Total persons: 2" in text
        assert "Total matches: 1" in text
        assert "Match rate: 50.0%" in text
        assert "exact" in text
        assert "宮崎駿" in text


class TestExportMatchesForReview:
    def test_export_csv(self, tmp_path):
        """CSV エクスポートが動作する."""
        persons = [
            Person(id="mal:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="mal:2", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
        ]

        decision = MatchDecision(
            source_id="mal:2",
            canonical_id="mal:1",
            strategy="exact",
            confidence=1.0,
            reason="Exact match",
            source_person=persons[1],
            canonical_person=persons[0],
        )

        report = ResolutionReport(
            total_persons=2,
            total_matches=1,
            match_rate=0.5,
            matches_by_strategy={"exact": 1},
            decisions=[decision],
            ambiguous_cases=[],
        )

        output_file = tmp_path / "review.csv"
        export_matches_for_review(report, str(output_file))

        assert output_file.exists()

        # Read and verify content
        content = output_file.read_text(encoding="utf-8")
        assert "Source ID" in content
        assert "Canonical ID" in content
        assert "mal:2" in content
        assert "mal:1" in content
        assert "宮崎駿" in content

    def test_export_with_confidence_filter(self, tmp_path):
        """信頼度フィルタが動作する."""
        persons = [
            Person(id="mal:1", name_ja="A"),
            Person(id="mal:2", name_ja="A"),
            Person(id="mal:3", name_ja="B"),
        ]

        decisions = [
            MatchDecision(
                source_id="mal:2",
                canonical_id="mal:1",
                strategy="exact",
                confidence=1.0,
                reason="Exact",
                source_person=persons[1],
                canonical_person=persons[0],
            ),
            MatchDecision(
                source_id="mal:3",
                canonical_id="mal:1",
                strategy="similarity",
                confidence=0.85,
                reason="Similar",
                source_person=persons[2],
                canonical_person=persons[0],
            ),
        ]

        report = ResolutionReport(
            total_persons=3,
            total_matches=2,
            match_rate=2 / 3,
            matches_by_strategy={"exact": 1, "similarity": 1},
            decisions=decisions,
            ambiguous_cases=[],
        )

        output_file = tmp_path / "uncertain_only.csv"
        # Only export matches with confidence 0.8-0.9
        export_matches_for_review(report, str(output_file), min_confidence=0.8, max_confidence=0.9)

        content = output_file.read_text(encoding="utf-8")
        # Should include the similarity match (0.85) but not exact (1.0)
        assert "similarity" in content
        assert "0.85" in content
        # Should have 2 lines: header + 1 data row
        assert len(content.strip().split("\n")) == 2


class TestCalculatePrecisionFromReview:
    def test_calculate_precision(self, tmp_path):
        """手動レビューから精度を計算."""
        from src.analysis.entity_resolution_eval import calculate_precision_from_review

        csv_content = """Source ID,Canonical ID,Strategy,Confidence,Source Name JA,Source Name EN,Canonical Name JA,Canonical Name EN,Reason,Correct? (Y/N)
mal:2,mal:1,exact,1.00,宮崎駿,Hayao Miyazaki,宮崎駿,Hayao Miyazaki,Exact match,Y
mal:3,mal:1,similarity,0.85,宮﨑駿,Hayao Miyazaki,宮崎駿,Hayao Miyazaki,Similar,Y
mal:4,mal:1,similarity,0.85,宮崎勤,,,Miyazaki Tsutomu,Similar,N
"""

        review_file = tmp_path / "review.csv"
        review_file.write_text(csv_content, encoding="utf-8")

        precision = calculate_precision_from_review(str(review_file))

        assert precision["exact"] == 1.0  # 1/1
        assert precision["similarity"] == 0.5  # 1/2

    def test_ignore_empty_reviews(self, tmp_path):
        """空の Correct? 列を無視する."""
        from src.analysis.entity_resolution_eval import calculate_precision_from_review

        csv_content = """Source ID,Canonical ID,Strategy,Confidence,Source Name JA,Source Name EN,Canonical Name JA,Canonical Name EN,Reason,Correct? (Y/N)
mal:2,mal:1,exact,1.00,A,,A,,Match,Y
mal:3,mal:1,exact,1.00,B,,B,,Match,
"""

        review_file = tmp_path / "review.csv"
        review_file.write_text(csv_content, encoding="utf-8")

        precision = calculate_precision_from_review(str(review_file))

        # Only count the reviewed row
        assert precision["exact"] == 1.0  # 1/1 (2nd row not counted)

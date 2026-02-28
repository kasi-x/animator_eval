"""graphml_export モジュールのテスト."""

from src.analysis.graphml_export import export_graphml
from src.models import Credit, Person, Role


def _make_data():
    persons = [
        Person(id="p1", name_ja="田中太郎", name_en="Taro Tanaka"),
        Person(id="p2", name_ja="山田花子", name_en="Hanako Yamada"),
        Person(id="p3", name_ja="鈴木次郎", name_en="Jiro Suzuki"),
    ]
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(
            person_id="p2", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
    ]
    return persons, credits


class TestExportGraphml:
    def test_creates_file(self, tmp_path):
        persons, credits = _make_data()
        out = tmp_path / "test.graphml"
        result = export_graphml(persons, credits, output_path=out)
        assert result == out
        assert out.exists()

    def test_file_content_is_xml(self, tmp_path):
        persons, credits = _make_data()
        out = tmp_path / "test.graphml"
        export_graphml(persons, credits, output_path=out)
        content = out.read_text()
        assert "<graphml" in content
        assert "<node" in content
        assert "<edge" in content

    def test_node_count(self, tmp_path):
        persons, credits = _make_data()
        out = tmp_path / "test.graphml"
        export_graphml(persons, credits, output_path=out)
        content = out.read_text()
        # 3 person nodes
        assert content.count("<node ") == 3

    def test_with_scores(self, tmp_path):
        persons, credits = _make_data()
        scores = {
            "p1": {
                "birank": 80.0,
                "patronage": 70.0,
                "person_fe": 60.0,
                "iv_score": 72.0,
                "primary_role": "director",
            },
            "p2": {
                "birank": 60.0,
                "patronage": 50.0,
                "person_fe": 40.0,
                "iv_score": 52.0,
                "primary_role": "animator",
            },
        }
        out = tmp_path / "test.graphml"
        export_graphml(persons, credits, person_scores=scores, output_path=out)
        content = out.read_text()
        assert out.exists()
        # Should contain score attributes
        assert "birank" in content

    def test_empty_credits(self, tmp_path):
        persons, _ = _make_data()
        out = tmp_path / "test.graphml"
        export_graphml(persons, [], output_path=out)
        assert out.exists()
        content = out.read_text()
        # Nodes exist but no edges
        assert "<node " in content

    def test_edge_weights(self, tmp_path):
        persons, credits = _make_data()
        out = tmp_path / "test.graphml"
        export_graphml(persons, credits, output_path=out)
        content = out.read_text()
        # p1-p2 share 2 works so weight should be 2
        assert "shared_works" in content

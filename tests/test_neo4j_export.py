"""neo4j_export モジュールのテスト."""

import csv

from src.analysis.neo4j_export import export_neo4j_csv
from src.models import Anime, Credit, Person, Role, ScoreResult


def _sample_data():
    persons = [
        Person(id="p1", name_en="Director A", name_ja="監督A"),
        Person(id="p2", name_en="Animator B", name_ja="作画B"),
        Person(id="p3", name_en="Newbie C"),
    ]
    anime = [
        Anime(id="a1", title_en="Show One", year=2022, score=8.0),
        Anime(id="a2", title_en="Show Two", year=2023, score=7.5),
    ]
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(
            person_id="p2", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p3", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
    ]
    scores = [
        ScoreResult(person_id="p1", birank=80.0, patronage=70.0, person_fe=60.0),
        ScoreResult(person_id="p2", birank=50.0, patronage=40.0, person_fe=55.0),
    ]
    return persons, anime, credits, scores


class TestExportNeo4jCsv:
    def test_creates_files(self, tmp_path):
        persons, anime, credits, scores = _sample_data()
        out = export_neo4j_csv(persons, anime, credits, scores, output_dir=tmp_path)
        assert (out / "persons.csv").exists()
        assert (out / "anime.csv").exists()
        assert (out / "credits.csv").exists()
        assert (out / "collaborations.csv").exists()

    def test_persons_csv_content(self, tmp_path):
        persons, anime, credits, scores = _sample_data()
        export_neo4j_csv(persons, anime, credits, scores, output_dir=tmp_path)

        with open(tmp_path / "persons.csv") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0][0] == "personId:ID(Person)"
        assert rows[0][-1] == ":LABEL"
        assert len(rows) == 4  # header + 3 persons
        # Check scores are included for p1
        p1_row = [r for r in rows[1:] if r[0] == "p1"][0]
        assert p1_row[6] == "80.0"  # birank

    def test_anime_csv_content(self, tmp_path):
        persons, anime, credits, scores = _sample_data()
        export_neo4j_csv(persons, anime, credits, scores, output_dir=tmp_path)

        with open(tmp_path / "anime.csv") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 3  # header + 2 anime

    def test_credits_csv_content(self, tmp_path):
        persons, anime, credits, scores = _sample_data()
        export_neo4j_csv(persons, anime, credits, scores, output_dir=tmp_path)

        with open(tmp_path / "credits.csv") as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert len(rows) == 6  # header + 5 credits
        assert rows[0][-1] == ":TYPE"

    def test_collaborations_csv(self, tmp_path):
        persons, anime, credits, scores = _sample_data()
        export_neo4j_csv(persons, anime, credits, scores, output_dir=tmp_path)

        with open(tmp_path / "collaborations.csv") as f:
            reader = csv.reader(f)
            rows = list(reader)
        # p1 and p2 share 2 works, so they should have a collaboration edge
        collab_rows = [r for r in rows[1:] if "p1" in r and "p2" in r]
        assert len(collab_rows) == 1
        assert collab_rows[0][2] == "2"  # shared_works

    def test_without_scores(self, tmp_path):
        persons, anime, credits, _ = _sample_data()
        export_neo4j_csv(persons, anime, credits, output_dir=tmp_path)
        assert (tmp_path / "persons.csv").exists()

    def test_empty_data(self, tmp_path):
        export_neo4j_csv([], [], [], output_dir=tmp_path)
        assert (tmp_path / "persons.csv").exists()
        with open(tmp_path / "persons.csv") as f:
            rows = list(csv.reader(f))
        assert len(rows) == 1  # header only

"""Unit tests for jvmg_fetcher.parse_wikidata_results."""

from __future__ import annotations

from src.models import Role
from src.scrapers.jvmg_fetcher import parse_wikidata_results
from src.scrapers.wikidata_role_map import WIKIDATA_ROLE_MAP


def _binding(anime_qid: str, person_qid: str, role: str = "director",
             anime_label: str = "Test Anime", person_label: str = "Test Person",
             person_label_ja: str = "", year: str = "2020") -> dict:
    return {
        "anime": {"value": f"http://www.wikidata.org/entity/{anime_qid}"},
        "animeLabel": {"value": anime_label},
        "year": {"value": year},
        "person": {"value": f"http://www.wikidata.org/entity/{person_qid}"},
        "personLabel": {"value": person_label},
        "personLabelJa": {"value": person_label_ja},
        "role": {"value": role},
    }


class TestParseWikidataResults:
    def test_empty_bindings(self):
        anime, persons, credits = parse_wikidata_results([])
        assert anime == []
        assert persons == []
        assert credits == []

    def test_single_director_credit(self):
        bindings = [_binding("Q1", "Q100", role="director")]
        anime, persons, credits = parse_wikidata_results(bindings)
        assert len(anime) == 1
        assert len(persons) == 1
        assert len(credits) == 1
        assert credits[0].role == Role.DIRECTOR

    def test_anime_id_format(self):
        bindings = [_binding("Q12345", "Q99")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].anime_id == "wd:Q12345"

    def test_person_id_format(self):
        bindings = [_binding("Q1", "Q99")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].person_id == "wd:pQ99"

    def test_year_parsed(self):
        bindings = [_binding("Q1", "Q2", year="2015")]
        anime, _, _ = parse_wikidata_results(bindings)
        assert anime[0].year == 2015

    def test_year_missing(self):
        b = _binding("Q1", "Q2")
        b.pop("year", None)
        b["year"] = {"value": ""}
        anime, _, _ = parse_wikidata_results([b])
        assert anime[0].year is None

    def test_screenwriter_maps_to_screenplay(self):
        bindings = [_binding("Q1", "Q2", role="screenplay")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].role == Role.SCREENPLAY

    def test_film_editor_maps_to_editing(self):
        bindings = [_binding("Q1", "Q2", role="film editor")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].role == Role.EDITING

    def test_art_director_maps_to_background_art(self):
        bindings = [_binding("Q1", "Q2", role="art director")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].role == Role.BACKGROUND_ART

    def test_dedup_anime(self):
        bindings = [
            _binding("Q1", "Q10", role="director"),
            _binding("Q1", "Q11", role="screenplay"),
        ]
        anime, _, _ = parse_wikidata_results(bindings)
        assert len(anime) == 1

    def test_dedup_person(self):
        bindings = [
            _binding("Q1", "Q10", role="director"),
            _binding("Q2", "Q10", role="director"),
        ]
        _, persons, _ = parse_wikidata_results(bindings)
        assert len(persons) == 1

    def test_multiple_credits_for_one_anime(self):
        bindings = [
            _binding("Q1", "Q10", role="director"),
            _binding("Q1", "Q11", role="screenplay"),
            _binding("Q1", "Q12", role="film editor"),
        ]
        _, _, credits = parse_wikidata_results(bindings)
        assert len(credits) == 3

    def test_source_is_wikidata(self):
        bindings = [_binding("Q1", "Q2")]
        _, _, credits = parse_wikidata_results(bindings)
        assert credits[0].source == "wikidata"

    def test_person_name_en(self):
        bindings = [_binding("Q1", "Q2", person_label="Hayao Miyazaki")]
        _, persons, _ = parse_wikidata_results(bindings)
        assert persons[0].name_en == "Hayao Miyazaki"

    def test_person_name_ja(self):
        bindings = [_binding("Q1", "Q2", person_label_ja="宮崎駿")]
        _, persons, _ = parse_wikidata_results(bindings)
        assert persons[0].name_ja == "宮崎駿"

    def test_missing_anime_uri_skipped(self):
        b = _binding("Q1", "Q2")
        b["anime"]["value"] = ""
        _, _, credits = parse_wikidata_results([b])
        assert credits == []

    def test_missing_person_uri_skipped(self):
        b = _binding("Q1", "Q2")
        b["person"]["value"] = ""
        _, _, credits = parse_wikidata_results([b])
        assert credits == []

    def test_empty_role_defaults_to_other(self):
        b = _binding("Q1", "Q2")
        b["role"] = {"value": ""}
        _, _, credits = parse_wikidata_results([b])
        assert credits[0].role == Role.SPECIAL  # parse_role("other") → SPECIAL


class TestWikidataRoleMap:
    def test_all_values_parseable(self):
        from src.models import parse_role, Role, ROLE_MAP
        for prop, token in WIKIDATA_ROLE_MAP.items():
            role = parse_role(token)
            assert role != Role.SPECIAL, (
                f"WIKIDATA_ROLE_MAP[{prop!r}]={token!r} falls back to Role.SPECIAL — "
                "add it to ROLE_MAP in src/models.py"
            )

    def test_known_mappings(self):
        assert WIKIDATA_ROLE_MAP["P57"] == "director"
        assert WIKIDATA_ROLE_MAP["P58"] == "screenplay"
        assert WIKIDATA_ROLE_MAP["P1040"] == "film editor"
        assert WIKIDATA_ROLE_MAP["P3174"] == "art director"

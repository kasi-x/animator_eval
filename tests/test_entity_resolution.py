"""entity_resolution モジュールのテスト."""

from src.analysis.entity_resolution import (
    _normalize_romaji,
    cross_source_match,
    exact_match_cluster,
    normalize_name,
    resolve_all,
    romaji_match,
)
from src.models import Person


class TestNormalizeName:
    def test_empty(self):
        assert normalize_name("") == ""

    def test_whitespace(self):
        assert normalize_name("  Hayao   Miyazaki  ") == "hayao miyazaki"

    def test_nfkc(self):
        # 全角英字 → 半角
        assert normalize_name("Ｈａｙａｏ") == "hayao"

    def test_honorific_removal(self):
        assert normalize_name("宮崎先生") == "宮崎"
        assert normalize_name("宮崎さん") == "宮崎"

    def test_japanese_preserved(self):
        assert normalize_name("宮崎駿") == "宮崎駿"

    def test_english_lowered(self):
        assert normalize_name("Hayao MIYAZAKI") == "hayao miyazaki"


class TestExactMatchCluster:
    def test_no_matches(self):
        persons = [
            Person(id="a", name_en="Alice"),
            Person(id="b", name_en="Bob"),
        ]
        result = exact_match_cluster(persons)
        assert result == {}

    def test_exact_match(self):
        persons = [
            Person(id="mal:p1", name_ja="宮崎駿"),
            Person(id="anilist:p1", name_ja="宮崎駿"),
        ]
        result = exact_match_cluster(persons)
        assert len(result) == 1
        assert "anilist:p1" in result
        assert result["anilist:p1"] == "mal:p1"


class TestCrossSourceMatch:
    def test_match_by_name(self):
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="anilist:p1", name_en="Hayao Miyazaki"),
        ]
        result = cross_source_match(persons)
        assert result.get("anilist:p1") == "mal:p1"

    def test_ambiguous_skipped(self):
        persons = [
            Person(id="mal:p1", name_en="Takashi"),
            Person(id="mal:p2", name_en="Takashi"),
            Person(id="anilist:p1", name_en="Takashi"),
        ]
        result = cross_source_match(persons)
        # 曖昧なマッチはスキップ
        assert "anilist:p1" not in result


class TestNormalizeRomaji:
    def test_empty(self):
        assert _normalize_romaji("") == ""

    def test_lowercase_and_sort(self):
        assert _normalize_romaji("Miyazaki Hayao") == "hayao miyazaki"

    def test_name_order_invariant(self):
        assert _normalize_romaji("Hayao Miyazaki") == _normalize_romaji("Miyazaki Hayao")

    def test_macron_removal(self):
        assert _normalize_romaji("Ōtomo Katsuhirō") == _normalize_romaji("Otomo Katsuhiro")

    def test_hyphen_removal(self):
        assert _normalize_romaji("Shin-ichirō") == _normalize_romaji("Shinichiro")


class TestRomajiMatch:
    def test_name_order_difference(self):
        persons = [
            Person(id="mal:p1", name_en="Miyazaki Hayao"),
            Person(id="anilist:p1", name_en="Hayao Miyazaki"),
        ]
        result = romaji_match(persons)
        assert result.get("anilist:p1") == "mal:p1"

    def test_macron_difference(self):
        persons = [
            Person(id="mal:p1", name_en="Katsuhiro Otomo"),
            Person(id="anilist:p1", name_en="Katsuhirō Ōtomo"),
        ]
        result = romaji_match(persons)
        assert result.get("anilist:p1") == "mal:p1"

    def test_short_names_excluded(self):
        """短い名前は曖昧性が高いためスキップ."""
        persons = [
            Person(id="mal:p1", name_en="Ai"),
            Person(id="anilist:p1", name_en="Ai"),
        ]
        result = romaji_match(persons)
        assert "anilist:p1" not in result

    def test_ambiguous_skipped(self):
        persons = [
            Person(id="mal:p1", name_en="Takashi Yamada"),
            Person(id="mal:p2", name_en="Yamada Takashi"),
            Person(id="anilist:p1", name_en="Takashi Yamada"),
        ]
        result = romaji_match(persons)
        # 2つのMAL IDが同じ正規化名にマッチ → 曖昧
        assert "anilist:p1" not in result

    def test_no_match(self):
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="anilist:p1", name_en="Satoshi Kon"),
        ]
        result = romaji_match(persons)
        assert result == {}


class TestResolveAll:
    def test_combines_exact_and_cross(self):
        persons = [
            Person(id="mal:p1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="anilist:p1", name_en="Hayao Miyazaki"),
        ]
        result = resolve_all(persons)
        assert "anilist:p1" in result

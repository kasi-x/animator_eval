"""Tests for the entity_resolution module."""

from src.analysis.entity_resolution import (
    _normalize_romaji,
    _transitive_closure,
    cross_source_match,
    exact_match_cluster,
    normalize_name,
    resolve_all,
    romaji_match,
    similarity_based_cluster,
)
from src.models import Person


class TestNormalizeName:
    def test_empty(self):
        assert normalize_name("") == ""

    def test_whitespace(self):
        assert normalize_name("  Hayao   Miyazaki  ") == "hayao miyazaki"

    def test_nfkc(self):
        # Fullwidth ASCII → halfwidth
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

    def test_different_kanji_same_romaji_no_match(self):
        """異なる漢字で同じローマ字表記の場合はマッチしない（false positive 防止）.

        例: 岡遼子 vs 岡亮子 はどちらも "Ryouko Oka" だが別人である可能性が高い。
        日本語名がある場合、日本語名が一致しない限りマッチさせない。
        """
        persons = [
            Person(id="anilist:p1", name_ja="岡遼子", name_en="Ryouko Oka"),
            Person(id="anilist:p2", name_ja="岡亮子", name_en="Ryouko Oka"),
        ]
        result = exact_match_cluster(persons)
        # 日本語名が異なるため、英語名が同じでもマッチしない
        assert result == {}

    def test_english_name_match_without_japanese(self):
        """日本語名がない場合は英語名でマッチング可能."""
        persons = [
            Person(id="mal:p1", name_en="John Smith"),
            Person(id="anilist:p1", name_en="John Smith"),
        ]
        result = exact_match_cluster(persons)
        assert len(result) == 1
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
        assert _normalize_romaji("Hayao Miyazaki") == _normalize_romaji(
            "Miyazaki Hayao"
        )

    def test_macron_removal(self):
        assert _normalize_romaji("Ōtomo Katsuhirō") == _normalize_romaji(
            "Otomo Katsuhiro"
        )

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


class TestSimilarityBasedCluster:
    def test_high_similarity_match(self):
        """非常に似た名前（タイポ）をマッチングする."""
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="mal:p2", name_en="Hayao Miyazakii"),  # 1文字余分
        ]
        result = similarity_based_cluster(persons, threshold=0.95)
        # 97%以上の類似度でマッチするはず
        assert "mal:p2" in result
        assert result["mal:p2"] == "mal:p1"

    def test_low_similarity_no_match(self):
        """類似度が低い名前はマッチしない."""
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="mal:p2", name_en="Satoshi Kon"),
        ]
        result = similarity_based_cluster(persons, threshold=0.95)
        assert result == {}

    def test_short_names_excluded(self):
        """5文字未満の短い名前は除外される."""
        persons = [
            Person(id="mal:p1", name_en="Ai Li"),
            Person(id="mal:p2", name_en="Ai Lee"),
        ]
        result = similarity_based_cluster(persons, threshold=0.90)
        # 短すぎるので除外
        assert result == {}

    def test_same_source_only(self):
        """同一ソース内でのみマッチングする（クロスソースは避ける）."""
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="anilist:p2", name_en="Hayao Miyazakii"),
        ]
        result = similarity_based_cluster(persons, threshold=0.95)
        # 異なるソースなのでマッチしない
        assert result == {}

    def test_multiple_candidates_skipped(self):
        """複数候補がある場合は曖昧性を避けてスキップ."""
        persons = [
            Person(id="mal:p1", name_en="Takashi Yamada"),
            Person(id="mal:p1b", name_en="Takashi Yamada"),  # 完全一致
            Person(id="mal:p2", name_en="Takashi Yamadaa"),
        ]
        # p1とp1bが同じ名前 → p2とのマッチが曖昧 → スキップ
        result = similarity_based_cluster(persons, threshold=0.95)
        # 1対1マッチでないためスキップされる
        assert "mal:p2" not in result

    def test_threshold_sensitivity(self):
        """閾値による動作の違い."""
        persons = [
            Person(id="mal:p1", name_en="Miyazaki Hayao"),
            Person(id="mal:p2", name_en="Miyazaki Haya"),  # 1文字削除
        ]
        # 高い閾値ではマッチしない
        result_high = similarity_based_cluster(persons, threshold=0.98)
        assert result_high == {}

        # 低い閾値ではマッチする
        result_low = similarity_based_cluster(persons, threshold=0.90)
        assert "mal:p2" in result_low

    def test_japanese_names(self):
        """日本語名の類似度マッチング."""
        persons = [
            Person(id="mal:p1", name_ja="渡辺信一郎太郎"),  # より長い名前
            Person(id="mal:p2", name_ja="渡辺信一郎太朗"),  # 最後の文字が異なる
        ]
        # 類似度は高い（1文字違い／8文字）
        result = similarity_based_cluster(persons, threshold=0.85)
        # 85%閾値なら十分マッチする
        assert "mal:p2" in result

    def test_empty_list(self):
        """空のリストでエラーが起きない."""
        result = similarity_based_cluster([], threshold=0.95)
        assert result == {}

    def test_single_person(self):
        """1人だけの場合はマッチングなし."""
        persons = [Person(id="mal:p1", name_en="Hayao Miyazaki")]
        result = similarity_based_cluster(persons, threshold=0.95)
        assert result == {}

    def test_typo_variations(self):
        """よくあるタイポパターン."""
        persons = [
            Person(id="mal:p1", name_en="Shinichiro Watanabe"),
            Person(id="mal:p2", name_en="Shinichiro Watanabee"),  # 末尾に1文字余分
        ]
        # 高い類似度でマッチ
        result = similarity_based_cluster(persons, threshold=0.95)
        assert "mal:p2" in result


class TestResolveAll:
    def test_combines_exact_and_cross(self):
        persons = [
            Person(id="mal:p1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="anilist:p1", name_en="Hayao Miyazaki"),
        ]
        result = resolve_all(persons)
        assert "anilist:p1" in result

    def test_includes_similarity_matches(self):
        """類似度ベースマッチングが統合結果に含まれる."""
        persons = [
            Person(id="mal:p1", name_en="Hayao Miyazaki"),
            Person(id="mal:p2", name_en="Hayao Miyazakii"),  # タイポ
            Person(id="anilist:p1", name_en="Satoshi Kon"),
        ]
        result = resolve_all(persons)
        # mal:p2 は mal:p1 にマッチするはず
        assert "mal:p2" in result
        assert result["mal:p2"] == "mal:p1"


class TestTransitiveClosure:
    def test_simple_chain(self):
        """A→B, B→C → A→C, B→C."""
        mapping = {"A": "B", "B": "C"}
        result = _transitive_closure(mapping)
        assert result["A"] == "C"
        assert result["B"] == "C"

    def test_longer_chain(self):
        """A→B→C→D → all point to D."""
        mapping = {"A": "B", "B": "C", "C": "D"}
        result = _transitive_closure(mapping)
        assert result["A"] == "D"
        assert result["B"] == "D"
        assert result["C"] == "D"

    def test_no_chain(self):
        """独立したマッピングはそのまま."""
        mapping = {"A": "X", "B": "Y"}
        result = _transitive_closure(mapping)
        assert result == {"A": "X", "B": "Y"}

    def test_empty(self):
        assert _transitive_closure({}) == {}

    def test_cycle_protection(self):
        """循環があっても無限ループしない."""
        mapping = {"A": "B", "B": "A"}
        result = _transitive_closure(mapping)
        # 循環: A→B→A... visited で停止。A→B, B→A のまま
        assert "A" in result and "B" in result

    def test_diamond(self):
        """A→B, C→B, B→D → A→D, C→D, B→D."""
        mapping = {"A": "B", "C": "B", "B": "D"}
        result = _transitive_closure(mapping)
        assert result["A"] == "D"
        assert result["C"] == "D"
        assert result["B"] == "D"

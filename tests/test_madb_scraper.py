"""メディア芸術データベース (MADB) スクレイパーのテスト."""

from __future__ import annotations

import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from src.models import Role, parse_role
from src.scrapers.mediaarts_scraper import (
    make_madb_person_id,
    normalize_title,
    parse_anime_list_results,
    parse_contributor_text,
)


# ============================================================
# TestParseContributorText — テキストパーサーの単体テスト
# ============================================================


class TestParseContributorText:
    """contributorテキストパーサーのテスト."""

    def test_standard_format(self):
        """標準的な [role]name / [role]name 形式."""
        text = "[脚本]仲倉重郎 / [演出]須永 司 / [作画監督]数井浩子"
        result = parse_contributor_text(text)
        assert result == [
            ("脚本", "仲倉重郎"),
            ("演出", "須永 司"),
            ("作画監督", "数井浩子"),
        ]

    def test_single_entry(self):
        """単一エントリ."""
        text = "[監督]宮崎駿"
        result = parse_contributor_text(text)
        assert result == [("監督", "宮崎駿")]

    def test_fullwidth_brackets(self):
        """全角ブラケット ［ ］ の対応."""
        text = "［脚本］山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎")]

    def test_no_brackets(self):
        """ブラケットなし → role="other"."""
        text = "山田太郎"
        result = parse_contributor_text(text)
        assert result == [("other", "山田太郎")]

    def test_multiple_roles_dot_separator(self):
        """複数ロール（中点区切り）."""
        text = "[脚本・演出]山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎"), ("演出", "山田太郎")]

    def test_empty_string(self):
        """空文字列."""
        assert parse_contributor_text("") == []
        assert parse_contributor_text("   ") == []

    def test_none_input(self):
        """None入力."""
        assert parse_contributor_text(None) == []

    def test_mixed_format(self):
        """ブラケット有り/無し混在."""
        text = "[監督]田中一 / 鈴木二郎 / [脚本]佐藤三"
        result = parse_contributor_text(text)
        assert result == [
            ("監督", "田中一"),
            ("other", "鈴木二郎"),
            ("脚本", "佐藤三"),
        ]

    def test_whitespace_handling(self):
        """余分な空白の処理."""
        text = "  [監督]  田中 太郎  /  [脚本]  鈴木 次郎  "
        result = parse_contributor_text(text)
        assert result == [
            ("監督", "田中 太郎"),
            ("脚本", "鈴木 次郎"),
        ]

    def test_slash_in_role(self):
        """ロール内のスラッシュ（脚本/演出）."""
        text = "[脚本/演出]山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎"), ("演出", "山田太郎")]


# ============================================================
# TestMakeMADBPersonId — ID生成テスト
# ============================================================


class TestMakeMADBPersonId:
    """MADB人物ID生成のテスト."""

    def test_deterministic(self):
        """同じ名前 → 常に同じID."""
        id1 = make_madb_person_id("宮崎駿")
        id2 = make_madb_person_id("宮崎駿")
        assert id1 == id2

    def test_format(self):
        """ID形式: madb:p_{hash12}."""
        pid = make_madb_person_id("宮崎駿")
        assert pid.startswith("madb:p_")
        assert len(pid) == len("madb:p_") + 12

    def test_different_names_different_ids(self):
        """異なる名前 → 異なるID."""
        id1 = make_madb_person_id("宮崎駿")
        id2 = make_madb_person_id("高畑勲")
        assert id1 != id2

    def test_whitespace_normalization(self):
        """空白の有無でIDが変わらない."""
        id1 = make_madb_person_id("須永 司")
        id2 = make_madb_person_id("須永司")
        assert id1 == id2

    def test_nfkc_normalization(self):
        """全角/半角の違いでIDが変わらない."""
        id1 = make_madb_person_id("ＡＢＣ")
        id2 = make_madb_person_id("ABC")
        assert id1 == id2


# ============================================================
# TestMADBRoleMapping — ロールマッピングテスト
# ============================================================


class TestMADBRoleMapping:
    """MADB固有ロールのマッピングテスト."""

    def test_madb_specific_roles(self):
        """MADB固有ロール → 正しいRole enum."""
        assert parse_role("作画") == Role.KEY_ANIMATOR
        assert parse_role("文芸") == Role.SCREENPLAY
        assert parse_role("総監督") == Role.DIRECTOR
        assert parse_role("撮影") == Role.PHOTOGRAPHY_DIRECTOR
        assert parse_role("制作進行") == Role.PRODUCER
        assert parse_role("動画チェック") == Role.IN_BETWEEN
        assert parse_role("原案") == Role.ORIGINAL_CREATOR

    def test_shared_roles_still_work(self):
        """既存の共通ロールも引き続き動作."""
        assert parse_role("監督") == Role.DIRECTOR
        assert parse_role("脚本") == Role.SCREENPLAY
        assert parse_role("原画") == Role.KEY_ANIMATOR
        assert parse_role("演出") == Role.EPISODE_DIRECTOR
        assert parse_role("作画監督") == Role.ANIMATION_DIRECTOR

    def test_unknown_role(self):
        """不明ロール → Role.OTHER."""
        assert parse_role("ナレーション") == Role.OTHER

    def test_new_madb_roles(self):
        """追加されたMADB固有ロール."""
        assert parse_role("音楽監督") == Role.SOUND_DIRECTOR
        assert parse_role("メカニックデザイン") == Role.MECHANICAL_DESIGNER
        assert parse_role("美術") == Role.BACKGROUND_ART
        assert parse_role("色彩設定") == Role.COLOR_DESIGNER
        assert parse_role("色指定") == Role.COLOR_DESIGNER
        assert parse_role("特殊効果") == Role.EFFECTS
        assert parse_role("エフェクト") == Role.EFFECTS
        assert parse_role("3DCG") == Role.CGI_DIRECTOR  # lowered to 3dcg
        assert parse_role("CG") == Role.CGI_DIRECTOR  # lowered to cg
        assert parse_role("構成") == Role.SERIES_COMPOSITION


# ============================================================
# TestNormalizeTitle — タイトル正規化テスト
# ============================================================


class TestNormalizeTitle:
    """タイトル正規化のテスト."""

    def test_basic(self):
        assert normalize_title("機動戦士ガンダム") == "機動戦士ガンダム"

    def test_whitespace(self):
        assert normalize_title("  機動戦士  ガンダム  ") == "機動戦士 ガンダム"

    def test_nfkc(self):
        """全角英数字の正規化."""
        assert normalize_title("ＧＵＮＤＡＭ") == "GUNDAM"

    def test_empty(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""


# ============================================================
# TestParseAnimeListResults — SPARQL結果パーステスト
# ============================================================


class TestParseAnimeListResults:
    """アニメ一覧のSPARQL結果パースのテスト."""

    def test_basic_parse(self):
        bindings = [
            {
                "anime": {"value": "https://mediaarts-db.bunka.go.jp/data/anime/123"},
                "title": {"value": "テストアニメ"},
                "year": {"value": "1990-04-01"},
            }
        ]
        result = parse_anime_list_results(bindings)
        assert len(result) == 1
        assert result[0]["uri"] == "https://mediaarts-db.bunka.go.jp/data/anime/123"
        assert result[0]["title"] == "テストアニメ"
        assert result[0]["year"] == 1990
        assert result[0]["identifiers"] == []

    def test_multiple_identifiers_merged(self):
        """同一URIの identifier が複数行 → 統合."""
        bindings = [
            {
                "anime": {"value": "https://example.com/anime/1"},
                "title": {"value": "テスト"},
                "identifier": {"value": "https://anilist.co/anime/100"},
            },
            {
                "anime": {"value": "https://example.com/anime/1"},
                "title": {"value": "テスト"},
                "identifier": {"value": "https://myanimelist.net/anime/200"},
            },
        ]
        result = parse_anime_list_results(bindings)
        assert len(result) == 1
        assert len(result[0]["identifiers"]) == 2

    def test_no_year(self):
        """年なし."""
        bindings = [
            {
                "anime": {"value": "https://example.com/anime/1"},
                "title": {"value": "テスト"},
            }
        ]
        result = parse_anime_list_results(bindings)
        assert result[0]["year"] is None

    def test_empty_bindings(self):
        assert parse_anime_list_results([]) == []


# ============================================================
# TestMADBIntegration — モックSPARQLでのE2Eテスト
# ============================================================


class TestMADBIntegration:
    """MADB統合のE2Eテスト（モックSPARQL）."""

    @pytest.fixture
    def db_conn(self, tmp_path):
        """テスト用DB接続."""
        from src.database import init_db

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn

    def test_scrape_with_mock(self, db_conn):
        """モックSPARQLでの基本的なスクレイプフロー（マッチングなし）."""
        import asyncio

        from src.scrapers.mediaarts_scraper import scrape_madb

        anime_list_response = [
            {
                "anime": {"value": "https://mediaarts-db.bunka.go.jp/data/anime/A001"},
                "title": {"value": "テストアニメ"},
                "year": {"value": "2000"},
            }
        ]
        contributor_response = [
            {
                "contributor": {"value": "[監督]テスト太郎 / [脚本]テスト花子"},
            }
        ]

        mock_client = AsyncMock()
        mock_client.fetch_anime_list = AsyncMock(
            side_effect=[anime_list_response, []]
        )
        mock_client.fetch_contributor = AsyncMock(return_value=contributor_response)
        mock_client.close = AsyncMock()

        with patch(
            "src.scrapers.mediaarts_scraper.MediaArtsClient",
            return_value=mock_client,
        ):
            stats = asyncio.run(
                scrape_madb(
                    db_conn,
                    anime_types=["AnimationTVRegularSeries"],
                    max_anime=10,
                    checkpoint_interval=5,
                )
            )

        assert stats["anime_fetched"] == 1
        assert stats["anime_with_contributors"] == 1
        assert stats["credits_created"] == 2
        assert stats["persons_created"] == 2

        # 全てmadb:IDで保存される
        credits = db_conn.execute("SELECT * FROM credits WHERE source='mediaarts'").fetchall()
        assert len(credits) == 2

        persons = db_conn.execute("SELECT * FROM persons WHERE id LIKE 'madb:%'").fetchall()
        assert len(persons) == 2

        anime = db_conn.execute("SELECT * FROM anime WHERE id LIKE 'madb:%'").fetchall()
        assert len(anime) == 1
        assert anime[0]["madb_id"] == "A001"

    def test_scrape_saves_external_ids(self, db_conn):
        """外部IDがexternal_links_jsonに保存される."""
        import asyncio
        import json

        from src.scrapers.mediaarts_scraper import scrape_madb

        anime_list_response = [
            {
                "anime": {"value": "https://mediaarts-db.bunka.go.jp/data/anime/B002"},
                "title": {"value": "テスト作品"},
                "year": {"value": "2005"},
                "identifier": {"value": "https://anilist.co/anime/999"},
            }
        ]
        contributor_response = [
            {"contributor": {"value": "[監督]山田太郎"}},
        ]

        mock_client = AsyncMock()
        mock_client.fetch_anime_list = AsyncMock(
            side_effect=[anime_list_response, []]
        )
        mock_client.fetch_contributor = AsyncMock(return_value=contributor_response)
        mock_client.close = AsyncMock()

        with patch(
            "src.scrapers.mediaarts_scraper.MediaArtsClient",
            return_value=mock_client,
        ):
            asyncio.run(
                scrape_madb(
                    db_conn,
                    anime_types=["AnimationTVRegularSeries"],
                    max_anime=10,
                )
            )

        anime = db_conn.execute("SELECT * FROM anime WHERE id = 'madb:B002'").fetchone()
        assert anime is not None
        ext_links = json.loads(anime["external_links_json"])
        assert "https://anilist.co/anime/999" in ext_links

    def test_short_name_skipped(self):
        """2文字未満の名前はスキップ."""
        text = "[監督]A"
        result = parse_contributor_text(text)
        # パーサー自体は返す（フィルタはscrape_madb内）
        assert len(result) == 1
        assert result[0] == ("監督", "A")


# ============================================================
# TestEntityResolutionMADB — MADB名寄せテスト
# ============================================================


class TestEntityResolutionMADB:
    """MADB人物の名寄せテスト."""

    def test_madb_to_anilist_match(self):
        """MADB人物がAniList人物にマッチする."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="madb:p_abc123def456", name_ja="宮崎駿"),
        ]
        result = cross_source_match(persons)
        assert result.get("madb:p_abc123def456") == "anilist:1"

    def test_madb_short_name_skipped(self):
        """短い名前(< 3文字)のMADB人物はスキップ."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="太郎", name_en="Taro"),
            Person(id="madb:p_abc123def456", name_ja="太郎"),
        ]
        result = cross_source_match(persons)
        # 2文字 < 3 → スキップ
        assert "madb:p_abc123def456" not in result

    def test_madb_ambiguous_skipped(self):
        """曖昧マッチ（同名が複数）はスキップ."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="田中太郎"),
            Person(id="anilist:2", name_ja="田中太郎"),
            Person(id="madb:p_abc123def456", name_ja="田中太郎"),
        ]
        result = cross_source_match(persons)
        assert "madb:p_abc123def456" not in result

    def test_madb_no_match(self):
        """マッチなし."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="宮崎駿"),
            Person(id="madb:p_abc123def456", name_ja="高畑勲"),
        ]
        result = cross_source_match(persons)
        assert "madb:p_abc123def456" not in result

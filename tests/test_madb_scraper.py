"""Tests for the Media Arts Database (MADB) scraper."""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.models import Role, parse_role
from src.scrapers.mediaarts_scraper import (
    _extract_name_from_schema,
    _extract_studios,
    _extract_year,
    make_madb_person_id,
    normalize_title,
    parse_contributor_text,
    parse_jsonld_dump,
)


# ============================================================
# TestParseContributorText — Text parser unit tests
# ============================================================


class TestParseContributorText:
    """Tests for the contributor text parser."""

    def test_standard_format(self):
        """Standard [role]name / [role]name format."""
        text = "[脚本]仲倉重郎 / [演出]須永 司 / [作画監督]数井浩子"
        result = parse_contributor_text(text)
        assert result == [
            ("脚本", "仲倉重郎"),
            ("演出", "須永 司"),
            ("作画監督", "数井浩子"),
        ]

    def test_fullwidth_slash_separator(self):
        """Fullwidth slash ／ separator (JSON-LD dump format)."""
        text = "[脚本]仲倉重郎 ／ [演出]須永 司 ／ [作画監督]数井浩子"
        result = parse_contributor_text(text)
        assert result == [
            ("脚本", "仲倉重郎"),
            ("演出", "須永 司"),
            ("作画監督", "数井浩子"),
        ]

    def test_single_entry(self):
        """Single entry."""
        text = "[監督]宮崎駿"
        result = parse_contributor_text(text)
        assert result == [("監督", "宮崎駿")]

    def test_fullwidth_brackets(self):
        """Fullwidth brackets ［ ］ support."""
        text = "［脚本］山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎")]

    def test_no_brackets(self):
        """No brackets -> role="other"."""
        text = "山田太郎"
        result = parse_contributor_text(text)
        assert result == [("other", "山田太郎")]

    def test_multiple_roles_dot_separator(self):
        """Multiple roles (interpunct separator)."""
        text = "[脚本・演出]山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎"), ("演出", "山田太郎")]

    def test_empty_string(self):
        """Empty string."""
        assert parse_contributor_text("") == []
        assert parse_contributor_text("   ") == []

    def test_none_input(self):
        """None input."""
        assert parse_contributor_text(None) == []

    def test_mixed_format(self):
        """Mixed bracketed/unbracketed entries."""
        text = "[監督]田中一 / 鈴木二郎 / [脚本]佐藤三"
        result = parse_contributor_text(text)
        assert result == [
            ("監督", "田中一"),
            ("other", "鈴木二郎"),
            ("脚本", "佐藤三"),
        ]

    def test_whitespace_handling(self):
        """Extra whitespace handling."""
        text = "  [監督]  田中 太郎  /  [脚本]  鈴木 次郎  "
        result = parse_contributor_text(text)
        assert result == [
            ("監督", "田中 太郎"),
            ("脚本", "鈴木 次郎"),
        ]

    def test_slash_in_role(self):
        """Slash inside role brackets (脚本/演出)."""
        text = "[脚本/演出]山田太郎"
        result = parse_contributor_text(text)
        assert result == [("脚本", "山田太郎"), ("演出", "山田太郎")]

    def test_mixed_slash_types(self):
        """Mixed halfwidth and fullwidth slashes."""
        text = "[監督]A太郎 / [脚本]B花子 ／ [演出]C次郎"
        result = parse_contributor_text(text)
        assert len(result) == 3
        assert result[0] == ("監督", "A太郎")
        assert result[1] == ("脚本", "B花子")
        assert result[2] == ("演出", "C次郎")


# ============================================================
# TestMakeMADBPersonId — ID generation tests
# ============================================================


class TestMakeMADBPersonId:
    """Tests for MADB person ID generation."""

    def test_deterministic(self):
        """Same name -> always same ID."""
        id1 = make_madb_person_id("宮崎駿")
        id2 = make_madb_person_id("宮崎駿")
        assert id1 == id2

    def test_format(self):
        """ID format: madb:p_{hash12}."""
        pid = make_madb_person_id("宮崎駿")
        assert pid.startswith("madb:p_")
        assert len(pid) == len("madb:p_") + 12

    def test_different_names_different_ids(self):
        """Different names -> different IDs."""
        id1 = make_madb_person_id("宮崎駿")
        id2 = make_madb_person_id("高畑勲")
        assert id1 != id2

    def test_whitespace_normalization(self):
        """Whitespace presence does not change ID."""
        id1 = make_madb_person_id("須永 司")
        id2 = make_madb_person_id("須永司")
        assert id1 == id2

    def test_nfkc_normalization(self):
        """Fullwidth/halfwidth differences do not change ID."""
        id1 = make_madb_person_id("ＡＢＣ")
        id2 = make_madb_person_id("ABC")
        assert id1 == id2


# ============================================================
# TestMADBRoleMapping — Role mapping tests
# ============================================================


class TestMADBRoleMapping:
    """Tests for MADB-specific role mapping."""

    def test_madb_specific_roles(self):
        """MADB-specific roles -> correct Role enum."""
        assert parse_role("作画") == Role.KEY_ANIMATOR
        assert parse_role("文芸") == Role.SCREENPLAY
        assert parse_role("総監督") == Role.DIRECTOR
        assert parse_role("撮影") == Role.PHOTOGRAPHY_DIRECTOR
        assert parse_role("制作進行") == Role.PRODUCER
        assert parse_role("動画チェック") == Role.IN_BETWEEN
        assert parse_role("原案") == Role.ORIGINAL_CREATOR

    def test_shared_roles_still_work(self):
        """Existing common roles still work."""
        assert parse_role("監督") == Role.DIRECTOR
        assert parse_role("脚本") == Role.SCREENPLAY
        assert parse_role("原画") == Role.KEY_ANIMATOR
        assert parse_role("演出") == Role.EPISODE_DIRECTOR
        assert parse_role("作画監督") == Role.ANIMATION_DIRECTOR

    def test_unknown_role(self):
        """Unknown role -> Role.OTHER."""
        assert parse_role("ナレーション") == Role.OTHER

    def test_new_madb_roles(self):
        """Additional MADB-specific roles."""
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
# TestNormalizeTitle — Title normalization tests
# ============================================================


class TestNormalizeTitle:
    """Tests for title normalization."""

    def test_basic(self):
        assert normalize_title("機動戦士ガンダム") == "機動戦士ガンダム"

    def test_whitespace(self):
        assert normalize_title("  機動戦士  ガンダム  ") == "機動戦士 ガンダム"

    def test_nfkc(self):
        """Fullwidth alphanumeric normalization."""
        assert normalize_title("ＧＵＮＤＡＭ") == "GUNDAM"

    def test_empty(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""


# ============================================================
# TestExtractNameFromSchema — JSON-LD name field extraction
# ============================================================


class TestExtractNameFromSchema:
    """Tests for schema:name field format variants."""

    def test_string(self):
        assert _extract_name_from_schema("タイトル") == "タイトル"

    def test_list_with_string_first(self):
        result = _extract_name_from_schema(
            ["タイトル", {"@value": "タイトル", "@language": "ja-hrkt"}]
        )
        assert result == "タイトル"

    def test_list_kana_only(self):
        """Falls back to katakana reading if nothing else available."""
        result = _extract_name_from_schema(
            [{"@value": "タイトル", "@language": "ja-hrkt"}]
        )
        assert result == "タイトル"

    def test_dict(self):
        result = _extract_name_from_schema({"@value": "タイトル", "@language": "ja"})
        assert result == "タイトル"

    def test_empty_list(self):
        assert _extract_name_from_schema([]) == ""

    def test_none(self):
        assert _extract_name_from_schema("") == ""


# ============================================================
# TestExtractYear — Year extraction tests
# ============================================================


class TestExtractYear:
    """Tests for year extraction from datePublished/startDate."""

    def test_date_published(self):
        assert _extract_year({"schema:datePublished": "2020-04-01"}) == 2020

    def test_start_date_fallback(self):
        assert _extract_year({"schema:startDate": "2021-10"}) == 2021

    def test_date_published_priority(self):
        item = {"schema:datePublished": "2020", "schema:startDate": "2021"}
        assert _extract_year(item) == 2020

    def test_no_date(self):
        assert _extract_year({}) is None

    def test_dict_value(self):
        assert _extract_year({"schema:datePublished": {"@value": "2019-01-01"}}) == 2019


# ============================================================
# TestExtractStudios — Studio extraction tests
# ============================================================


class TestExtractStudios:
    """Tests for studio name extraction from productionCompany."""

    def test_single_studio(self):
        item = {"schema:productionCompany": "[アニメーション制作]マッドハウス"}
        assert _extract_studios(item) == ["マッドハウス"]

    def test_multiple_studios(self):
        item = {
            "schema:productionCompany": "[アニメーション制作]マッドハウス ／ [制作]東映アニメーション"
        }
        result = _extract_studios(item)
        assert "マッドハウス" in result
        assert "東映アニメーション" in result

    def test_no_brackets(self):
        item = {"schema:productionCompany": "サンライズ"}
        assert _extract_studios(item) == ["サンライズ"]

    def test_empty(self):
        assert _extract_studios({}) == []


# ============================================================
# TestParseJsonLdDump — JSON-LD parsing tests
# ============================================================


class TestParseJsonLdDump:
    """Tests for JSON-LD dump parsing."""

    def _make_json(self, tmp_path: Path, graph: list[dict]) -> Path:
        json_path = tmp_path / "test.json"
        json_path.write_text(
            json.dumps({"@graph": graph}, ensure_ascii=False), encoding="utf-8"
        )
        return json_path

    def test_basic(self, tmp_path):
        graph = [
            {
                "schema:identifier": "C10001",
                "schema:name": "ギャラクシー エンジェル",
                "schema:datePublished": "2001-04-08",
                "schema:contributor": "[シリーズ構成]井上敏樹 ／ [監督]浅香守生",
                "schema:productionCompany": "[アニメーション制作]マッドハウス",
            }
        ]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        assert len(result) == 1
        assert result[0]["id"] == "C10001"
        assert result[0]["title"] == "ギャラクシー エンジェル"
        assert result[0]["year"] == 2001
        assert len(result[0]["contributors"]) == 2
        assert result[0]["studios"] == ["マッドハウス"]

    def test_creator_and_contributor_merged(self, tmp_path):
        """schema:creator + schema:contributor + ma:originalWorkCreator are merged."""
        graph = [
            {
                "schema:identifier": "C20001",
                "schema:name": "テスト作品",
                "schema:datePublished": "2022",
                "schema:creator": "[総監督]湯山邦彦",
                "schema:contributor": "[脚本]井上敏樹",
                "ma:originalWorkCreator": "[原作]村上真紀",
            }
        ]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        roles = [r for r, _n in result[0]["contributors"]]
        assert "総監督" in roles
        assert "脚本" in roles
        assert "原作" in roles

    def test_no_contributor_empty_list(self, tmp_path):
        """Empty contributors when no contributor fields."""
        graph = [
            {
                "schema:identifier": "C30001",
                "schema:name": "テスト",
                "schema:datePublished": "2023",
            }
        ]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        assert len(result) == 1
        assert result[0]["contributors"] == []

    def test_no_identifier_skipped(self, tmp_path):
        """Items without identifier are skipped."""
        graph = [{"schema:name": "NoID"}]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        assert len(result) == 0

    def test_no_title_skipped(self, tmp_path):
        """Items without title are skipped."""
        graph = [{"schema:identifier": "C40001"}]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        assert len(result) == 0

    def test_empty_graph(self, tmp_path):
        result = parse_jsonld_dump(self._make_json(tmp_path, []))
        assert result == []

    def test_name_as_list(self, tmp_path):
        """schema:name in list format."""
        graph = [
            {
                "schema:identifier": "C50001",
                "schema:name": [
                    "タイトル名",
                    {"@value": "タイトルメイ", "@language": "ja-hrkt"},
                ],
                "schema:datePublished": "2020",
                "schema:contributor": "[監督]テスト太郎",
            }
        ]
        result = parse_jsonld_dump(self._make_json(tmp_path, graph))
        assert result[0]["title"] == "タイトル名"


# ============================================================
# TestDownloadMADBDataset — Download tests
# ============================================================


class TestDownloadMADBDataset:
    """Tests for downloading from GitHub Releases."""

    def test_cache_hit(self, tmp_path):
        """Cached version skips download."""
        import asyncio

        from src.scrapers.mediaarts_scraper import (
            ANIME_COLLECTION_FILES,
            download_madb_dataset,
        )

        # Pre-create version file and JSON files
        (tmp_path / ".version").write_text("v1.2.12")
        for zip_name in ANIME_COLLECTION_FILES:
            json_name = zip_name.replace("_json.zip", ".json")
            (tmp_path / json_name).write_text("{}")

        mock_release = {"tag_name": "v1.2.12", "assets": []}
        mock_resp = httpx.Response(
            200,
            json=mock_release,
            request=httpx.Request(
                "GET", "https://api.github.com/repos/x/releases/latest"
            ),
        )

        async def run():
            with patch("httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(return_value=mock_resp)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                return await download_madb_dataset(tmp_path, version="latest")

        result = asyncio.run(run())
        assert len(result) == len(ANIME_COLLECTION_FILES)

    def test_download_and_extract(self, tmp_path):
        """ZIP download and extraction."""
        import asyncio

        from src.scrapers.mediaarts_scraper import download_madb_dataset

        # Create test ZIP data
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr("metadata207.json", json.dumps({"@graph": []}))
        zip_bytes = zip_buf.getvalue()

        mock_release = {
            "tag_name": "v1.2.13",
            "assets": [
                {
                    "name": "metadata207_json.zip",
                    "browser_download_url": "https://github.com/x/releases/download/v1.2.13/metadata207_json.zip",
                },
            ],
        }

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "api.github.com" in url:
                return httpx.Response(
                    200, json=mock_release, request=httpx.Request("GET", url)
                )
            else:
                return httpx.Response(
                    200, content=zip_bytes, request=httpx.Request("GET", url)
                )

        async def run():
            with patch("httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(side_effect=mock_get)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                return await download_madb_dataset(tmp_path, version="latest")

        result = asyncio.run(run())
        assert "AnimationTVRegularSeries" in result
        assert (tmp_path / "metadata207.json").exists()
        assert (tmp_path / ".version").read_text() == "v1.2.13"


# ============================================================
# TestMADBIntegration — E2E tests with JSON-LD dump
# ============================================================


class TestMADBIntegration:
    """MADB integration E2E tests (mock JSON-LD files)."""

    @pytest.fixture
    def db_conn(self, tmp_path):
        """Test DB connection."""
        from src.database import init_db

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn

    def test_scrape_with_mock_dump(self, db_conn, tmp_path):
        """Basic scrape flow from mock JSON-LD file."""
        import asyncio

        from src.scrapers.mediaarts_scraper import scrape_madb

        # Create test JSON-LD file
        data_dir = tmp_path / "madb"
        data_dir.mkdir()
        json_data = {
            "@graph": [
                {
                    "schema:identifier": "A001",
                    "schema:name": "テストアニメ",
                    "schema:datePublished": "2000-01-01",
                    "schema:contributor": "[監督]テスト太郎 ／ [脚本]テスト花子",
                    "schema:productionCompany": "[アニメーション制作]テストスタジオ",
                }
            ]
        }
        json_path = data_dir / "metadata207.json"
        json_path.write_text(
            json.dumps(json_data, ensure_ascii=False), encoding="utf-8"
        )

        # Mock download_madb_dataset to return local file
        async def mock_download(data_dir, version="latest"):
            return {"AnimationTVRegularSeries": json_path}

        with patch(
            "src.scrapers.mediaarts_scraper.download_madb_dataset",
            side_effect=mock_download,
        ):
            stats = asyncio.run(
                scrape_madb(
                    db_conn,
                    data_dir=data_dir,
                    max_anime=10,
                    checkpoint_interval=5,
                )
            )

        assert stats["anime_fetched"] == 1
        assert stats["anime_with_contributors"] == 1
        assert stats["credits_created"] == 2
        assert stats["persons_created"] == 2

        # All saved with madb: IDs
        credits = db_conn.execute(
            "SELECT * FROM credits WHERE source='mediaarts'"
        ).fetchall()
        assert len(credits) == 2

        persons = db_conn.execute(
            "SELECT * FROM persons WHERE id LIKE 'madb:%'"
        ).fetchall()
        assert len(persons) == 2

        anime = db_conn.execute("SELECT * FROM anime WHERE id LIKE 'madb:%'").fetchall()
        assert len(anime) == 1
        assert anime[0]["madb_id"] == "A001"

    def test_scrape_multiple_contributor_fields(self, db_conn, tmp_path):
        """creator + contributor + originalWorkCreator are merged."""
        import asyncio

        from src.scrapers.mediaarts_scraper import scrape_madb

        data_dir = tmp_path / "madb"
        data_dir.mkdir()
        json_data = {
            "@graph": [
                {
                    "schema:identifier": "B001",
                    "schema:name": "テスト作品2",
                    "schema:datePublished": "2010",
                    "schema:creator": "[総監督]クリエイター太郎",
                    "schema:contributor": "[脚本]脚本家花子",
                    "ma:originalWorkCreator": "[原作]原作者次郎",
                }
            ]
        }
        json_path = data_dir / "metadata207.json"
        json_path.write_text(
            json.dumps(json_data, ensure_ascii=False), encoding="utf-8"
        )

        async def mock_download(data_dir, version="latest"):
            return {"AnimationTVRegularSeries": json_path}

        with patch(
            "src.scrapers.mediaarts_scraper.download_madb_dataset",
            side_effect=mock_download,
        ):
            stats = asyncio.run(scrape_madb(db_conn, data_dir=data_dir, max_anime=10))

        assert stats["credits_created"] == 3
        assert stats["persons_created"] == 3

    def test_short_name_skipped(self):
        """Names shorter than 2 chars are skipped."""
        text = "[監督]A"
        result = parse_contributor_text(text)
        # Parser returns it (filtering is in scrape_madb)
        assert len(result) == 1
        assert result[0] == ("監督", "A")

    def test_no_files_returns_empty_stats(self, db_conn, tmp_path):
        """No downloaded files returns empty stats."""
        import asyncio

        from src.scrapers.mediaarts_scraper import scrape_madb

        async def mock_download(data_dir, version="latest"):
            return {}

        with patch(
            "src.scrapers.mediaarts_scraper.download_madb_dataset",
            side_effect=mock_download,
        ):
            stats = asyncio.run(scrape_madb(db_conn, data_dir=tmp_path, max_anime=10))

        assert stats["anime_fetched"] == 0
        assert stats["credits_created"] == 0


# ============================================================
# TestEntityResolutionMADB — MADB entity resolution tests
# ============================================================


class TestEntityResolutionMADB:
    """Tests for MADB person entity resolution."""

    def test_madb_to_anilist_match(self):
        """MADB person matches AniList person."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="madb:p_abc123def456", name_ja="宮崎駿"),
        ]
        result = cross_source_match(persons)
        assert result.get("madb:p_abc123def456") == "anilist:1"

    def test_madb_short_name_skipped(self):
        """Short names (< 3 chars) are skipped."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="太郎", name_en="Taro"),
            Person(id="madb:p_abc123def456", name_ja="太郎"),
        ]
        result = cross_source_match(persons)
        # 2 chars < 3 -> skipped
        assert "madb:p_abc123def456" not in result

    def test_madb_ambiguous_skipped(self):
        """Ambiguous matches (multiple same names) are skipped."""
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
        """No match."""
        from src.analysis.entity_resolution import cross_source_match
        from src.models import Person

        persons = [
            Person(id="anilist:1", name_ja="宮崎駿"),
            Person(id="madb:p_abc123def456", name_ja="高畑勲"),
        ]
        result = cross_source_match(persons)
        assert "madb:p_abc123def456" not in result

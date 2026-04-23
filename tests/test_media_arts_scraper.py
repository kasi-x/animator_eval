"""Comprehensive scraper tests for retry, error handling, checkpoint, and edge cases.

Targets 70%+ coverage across:
- anilist_scraper.py (AniListClient, parsers, edge cases)
- mal_scraper.py (JikanClient, parsers, checkpoint)
- mediaarts_scraper.py (JSON-LD dump parser, GitHub download)
- jvmg_fetcher.py (WikidataClient, parsers, checkpoint)
- image_downloader.py (download_image, content validation, retry)
- retry.py (retry_async utility)
- exceptions.py (exception hierarchy)

All async tests use asyncio.run() wrappers since pytest-asyncio is not available.
"""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.models import Role
from src.scrapers.exceptions import (
    AuthenticationError,
    ContentValidationError,
    DataParseError,
    EndpointUnreachableError,
    RateLimitError,
    ScraperError,
)


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)

# ---------------------------------------------------------------------------
# MediaArts scraper tests
# ---------------------------------------------------------------------------


class TestMediaArtsJsonLdParser:
    def test_parse_jsonld_dump_basic(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10001",
                    "schema:name": "テスト作品",
                    "schema:datePublished": "2020-04-01",
                    "schema:contributor": "[監督]山田太郎 ／ [脚本]鈴木次郎",
                    "schema:productionCompany": "[アニメーション制作]マッドハウス",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert len(result) == 1
        assert result[0]["id"] == "C10001"
        assert result[0]["title"] == "テスト作品"
        assert result[0]["year"] == 2020
        assert ("監督", "山田太郎") in result[0]["contributors"]
        assert ("脚本", "鈴木次郎") in result[0]["contributors"]
        assert "マッドハウス" in result[0]["studios"]

    def test_parse_jsonld_name_list(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10002",
                    "schema:name": [
                        "タイトル",
                        {"@value": "タイトル", "@language": "ja-hrkt"},
                    ],
                    "schema:datePublished": "2021",
                    "schema:contributor": "[監督]テスト太郎",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert result[0]["title"] == "タイトル"

    def test_parse_jsonld_creator_and_contributor(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {
            "@graph": [
                {
                    "schema:identifier": "C10003",
                    "schema:name": "テスト",
                    "schema:datePublished": "2022",
                    "schema:creator": "[総監督]湯山邦彦",
                    "schema:contributor": "[脚本]井上敏樹",
                    "ma:originalWorkCreator": "[原作]村上真紀",
                }
            ]
        }
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        roles = [r for r, _n in result[0]["contributors"]]
        assert "総監督" in roles
        assert "脚本" in roles
        assert "原作" in roles

    def test_parse_jsonld_no_identifier_skipped(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {"@graph": [{"schema:name": "NoID"}]}
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert len(result) == 0

    def test_parse_jsonld_empty_graph(self, tmp_path):
        from src.scrapers.mediaarts_scraper import parse_jsonld_dump

        data = {"@graph": []}
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

        result = parse_jsonld_dump(json_path)
        assert result == []


class TestMediaArtsParsers:
    def test_parse_contributor_text(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        result = parse_contributor_text("[監督]山田太郎 / [脚本]鈴木次郎")
        assert len(result) == 2
        assert result[0] == ("監督", "山田太郎")
        assert result[1] == ("脚本", "鈴木次郎")

    def test_parse_contributor_text_fullwidth_slash(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        result = parse_contributor_text("[監督]山田太郎 ／ [脚本]鈴木次郎")
        assert len(result) == 2
        assert result[0] == ("監督", "山田太郎")
        assert result[1] == ("脚本", "鈴木次郎")

    def test_parse_contributor_text_empty(self):
        from src.scrapers.mediaarts_scraper import parse_contributor_text

        assert parse_contributor_text("") == []
        assert parse_contributor_text(None) == []


# ---------------------------------------------------------------------------
# JVMG / Wikidata fetcher tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Integration-style tests for async fetch functions
# ---------------------------------------------------------------------------


class TestMediaArtsDownload:
    def test_download_cached(self, tmp_path):
        """Cached version skips download."""
        from src.scrapers.mediaarts_scraper import (
            ANIME_COLLECTION_FILES_PRIMARY,
            download_madb_dataset,
        )

        # Pre-create version file and JSON files
        (tmp_path / ".version").write_text("v1.2.12")
        for zip_name in ANIME_COLLECTION_FILES_PRIMARY:
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

        result = _run(run())
        assert len(result) == len(ANIME_COLLECTION_FILES_PRIMARY)



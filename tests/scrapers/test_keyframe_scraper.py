"""Tests for keyframe_scraper.py Phase 0-4 orchestration.

All network calls are mocked. Uses a tmp_path BRONZE root.
pytest-asyncio is not installed; async tests use asyncio.run().
"""
from __future__ import annotations

import asyncio
import gzip
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.scrapers.keyframe_scraper import (
    _collect_person_ids_from_preload,
    _parse_sitemap_xml,
    run_scraper,
)


def _run(coro):
    """Run a coroutine synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "scrapers" / "keyframe"

_SITEMAP_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://keyframe-staff-list.com/staff/test-anime-1</loc></url>
  <url><loc>https://keyframe-staff-list.com/staff/test-anime-2</loc></url>
  <url><loc>https://keyframe-staff-list.com/</loc></url>
</urlset>
"""

_PRELOAD_HTML = (
    '<html><head></head><body><script>preloadData = '
    '{"title":"Test","slug":"test-anime-1","uuid":"abc-123","anilistId":21,'
    '"settings":{"categories":[{"name":"Main Staff"}]},'
    '"anilist":{"id":21,"studios":{"edges":[{"node":{"name":"Toei"},"isMain":true}]}},'
    '"menus":[{"name":"Overview","credits":[{"name":"Core","roles":['
    '{"name":"Director","original":"監督","staff":['
    '{"id":100,"ja":"テスト太郎","en":"Taro Test","isStudio":false},'
    '{"id":200,"ja":"スタジオA","en":"Studio A","isStudio":true}'
    ']}]}]}]}'
    ';</script></body></html>'
)

_SHOW_DATA = json.loads((FIXTURE_DIR / "sample_show.json").read_text())
_ROLES_DATA = json.loads((FIXTURE_DIR / "sample_roles.json").read_text())
_PREVIEW_DATA = json.loads((FIXTURE_DIR / "sample_preview.json").read_text())


# ---------------------------------------------------------------------------
# _parse_sitemap_xml
# ---------------------------------------------------------------------------


class TestParseSitemapXml:
    def test_extracts_staff_slugs(self):
        slugs = _parse_sitemap_xml(_SITEMAP_XML)
        assert "test-anime-1" in slugs
        assert "test-anime-2" in slugs
        assert "" not in slugs

    def test_returns_empty_on_bad_xml(self):
        slugs = _parse_sitemap_xml("<not-xml>")
        assert slugs == []


# ---------------------------------------------------------------------------
# _collect_person_ids_from_preload
# ---------------------------------------------------------------------------


class TestCollectPersonIds:
    def test_extracts_person_id(self):
        from src.scrapers.parsers.keyframe import extract_preload_data

        data = extract_preload_data(_PRELOAD_HTML)
        assert data is not None
        ids = _collect_person_ids_from_preload(data)
        assert 100 in ids

    def test_skips_studio_entries(self):
        data = {
            "menus": [
                {
                    "credits": [
                        {
                            "roles": [
                                {"staff": [{"id": 999, "isStudio": True}]}
                            ]
                        }
                    ]
                }
            ]
        }
        ids = _collect_person_ids_from_preload(data)
        assert 999 not in ids

    def test_empty_menus(self):
        ids = _collect_person_ids_from_preload({"menus": []})
        assert ids == set()


# ---------------------------------------------------------------------------
# Full Phase 0-4 integration (mocked network)
# ---------------------------------------------------------------------------


@pytest.fixture()
def bronze_root(tmp_path, monkeypatch):
    """Redirect BronzeWriter output to tmp dir."""
    root = tmp_path / "bronze"
    root.mkdir()
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", root)
    return root


@pytest.fixture()
def data_dir(tmp_path):
    d = tmp_path / "keyframe"
    d.mkdir()
    return d


@pytest.fixture()
def mock_client():
    """Return a mock KeyframeApiClient that returns fixture data."""
    client = MagicMock()
    client.close = AsyncMock()

    async def fake_roles():
        return _ROLES_DATA

    async def fake_sitemap():
        return _SITEMAP_XML

    async def fake_anime_page(slug):
        if slug.startswith("test-anime"):
            return _PRELOAD_HTML
        return None

    async def fake_person_show(pid):
        # Return fixture data (which has credits) for any person ID
        # The fixture uses person_id=133359 internally but we return it for all IDs
        return {
            "staff": {"id": pid, "isStudio": False, "ja": "名前", "en": "Name",
                      "aliases": [], "avatar": None, "bio": None},
            "jobs": ["Key Animation"],
            "studios": {"スタジオA": ["Studio A"]},
            "credits": _SHOW_DATA["credits"][:1],  # at least one credit row
        }

    async def fake_preview():
        return _PREVIEW_DATA

    client.get_roles_master = fake_roles
    client.get_sitemap = fake_sitemap
    client.get_anime_page = fake_anime_page
    client.get_person_show = fake_person_show
    client.get_preview = fake_preview
    return client


def count_parquet_rows(bronze_root: Path, table: str) -> int:
    """Count rows across all parquet files for a given table."""
    import duckdb

    files = list(bronze_root.glob(f"source=keyframe/table={table}/**/*.parquet"))
    if not files:
        return 0
    con = duckdb.connect(":memory:")
    file_list = [str(f) for f in files]
    return con.execute(f"SELECT count(*) FROM read_parquet({file_list!r})").fetchone()[0]


def _scrape(mock_client, data_dir, skip_persons=True, max_anime=1, fresh=True):
    async def run():
        with patch(
            "src.scrapers.keyframe_scraper.KeyframeApiClient", return_value=mock_client
        ):
            return await run_scraper(
                data_dir=data_dir,
                delay=0,
                skip_persons=skip_persons,
                max_anime=max_anime,
                fresh=fresh,
            )

    return _run(run())


class TestRunScraper:
    def test_phase0_writes_roles_master(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        n = count_parquet_rows(bronze_root, "roles_master")
        assert n == len(_ROLES_DATA), f"Expected {len(_ROLES_DATA)} roles, got {n}"

    def test_phase2_writes_anime_table(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        assert count_parquet_rows(bronze_root, "anime") >= 1

    def test_phase2_writes_anime_studios(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        assert count_parquet_rows(bronze_root, "anime_studios") >= 1

    def test_phase2_writes_settings_categories(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        assert count_parquet_rows(bronze_root, "settings_categories") >= 1

    def test_phase2_writes_credits(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        assert count_parquet_rows(bronze_root, "credits") >= 1

    def test_phase3_writes_person_profile(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir, skip_persons=False)
        assert count_parquet_rows(bronze_root, "person_profile") >= 1

    def test_phase3_writes_person_credits(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir, skip_persons=False)
        assert count_parquet_rows(bronze_root, "person_credits") >= 1

    def test_phase4_writes_preview(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        # 2 recent + 2 airing + 2 data = 6
        assert count_parquet_rows(bronze_root, "preview") == 6

    def test_all_11_tables_populated(self, bronze_root, data_dir, mock_client):
        """All 11 BRONZE tables must have at least 1 row."""
        _scrape(mock_client, data_dir, skip_persons=False)
        expected_tables = [
            "roles_master",
            "anime",
            "anime_studios",
            "settings_categories",
            "credits",
            "studios_master",
            "person_profile",
            "person_jobs",
            "person_studios",
            "person_credits",
            "preview",
        ]
        empty = [t for t in expected_tables if count_parquet_rows(bronze_root, t) == 0]
        assert not empty, f"Tables with 0 rows: {empty}"

    def test_checkpoint_written(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        cp_path = data_dir / "checkpoint_anime.json"
        assert cp_path.exists()
        cp = json.loads(cp_path.read_text())
        assert cp["roles_master_fetched_at"] is not None
        assert cp["completed_ids"]  # contains completed slugs

    def test_checkpoint_resume_skips_completed(self, bronze_root, data_dir, mock_client):
        """Running twice with fresh=False does not re-fetch roles_master."""
        _scrape(mock_client, data_dir)

        call_count = 0
        original = mock_client.get_roles_master

        async def counting_roles():
            nonlocal call_count
            call_count += 1
            return await original()

        mock_client.get_roles_master = counting_roles
        _scrape(mock_client, data_dir, fresh=False)
        assert call_count == 0, "Roles master should not be re-fetched on resume"

    def test_fresh_flag_resets_checkpoint(self, bronze_root, data_dir, mock_client):
        """fresh=True ignores existing checkpoint."""
        _scrape(mock_client, data_dir)

        call_count = 0
        original = mock_client.get_roles_master

        async def counting_roles():
            nonlocal call_count
            call_count += 1
            return await original()

        mock_client.get_roles_master = counting_roles
        _scrape(mock_client, data_dir, fresh=True)
        assert call_count == 1, "With fresh=True, roles master should be re-fetched"

    def test_raw_html_gzip_saved(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir)
        gz_files = list((data_dir / "raw").glob("*.html.gz"))
        assert len(gz_files) >= 1
        with gzip.open(gz_files[0], "rt", encoding="utf-8") as fh:
            content = fh.read()
        assert "preloadData" in content

    def test_raw_json_gzip_saved(self, bronze_root, data_dir, mock_client):
        _scrape(mock_client, data_dir, skip_persons=False)
        gz_files = list((data_dir / "person_raw").glob("*.json.gz"))
        assert len(gz_files) >= 1
        with gzip.open(gz_files[0], "rt", encoding="utf-8") as fh:
            raw = json.load(fh)
        assert "staff" in raw

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
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx



def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)

# ---------------------------------------------------------------------------
# Image downloader tests
# ---------------------------------------------------------------------------


class TestImageDownloader:
    def test_download_image_empty_url(self):
        from src.scrapers.image_downloader import download_image

        client = AsyncMock()
        result = _run(download_image(client, "", Path("/tmp/test")))
        assert result is None

    def test_download_image_none_url(self):
        from src.scrapers.image_downloader import download_image

        client = AsyncMock()
        result = _run(download_image(client, None, Path("/tmp/test")))
        assert result is None

    def test_download_image_already_exists(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        # IMAGES_DIR must be patched so that save_path.relative_to(IMAGES_DIR.parent)
        # works correctly when using tmp_path
        images_dir = tmp_path / "images"
        images_dir.mkdir()
        save_dir = images_dir / "subdir"
        save_dir.mkdir()
        existing = save_dir / "test.jpg"
        existing.write_bytes(b"existing image")

        client = AsyncMock()

        async def run():
            with patch("src.scrapers.image_downloader.IMAGES_DIR", images_dir):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is not None
        # Client should not have been called (file already exists)
        client.get.assert_not_called()

    def test_download_image_success(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048  # JPEG-like content

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None
        assert (save_dir / "test.jpg").exists()

    def test_download_image_invalid_content_type(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.content = b"<html>not an image</html>"

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_too_small(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = b"\xff\xd8"  # Too small (< 1024 bytes)

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_http_error_retry(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}
        mock_response.content = image_content

        client = AsyncMock()
        # First attempt fails, second succeeds
        client.get = AsyncMock(
            side_effect=[httpx.ConnectError("timeout"), mock_response]
        )

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None

    def test_download_image_all_retries_fail(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_429_rate_limit(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        success = MagicMock()
        success.status_code = 200
        success.headers = {"content-type": "image/jpeg"}
        success.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(side_effect=[rate_limited, success])

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/img.jpg", save_dir, "test.jpg"
                    )

        result = _run(run())
        assert result is not None

    def test_download_image_429_all_retries(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        client = AsyncMock()
        client.get = AsyncMock(return_value=rate_limited)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_non_200_status(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        not_found = MagicMock()
        not_found.status_code = 404
        not_found.headers = {}

        client = AsyncMock()
        client.get = AsyncMock(return_value=not_found)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_general_exception(self, tmp_path):
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        client = AsyncMock()
        client.get = AsyncMock(side_effect=RuntimeError("unexpected"))

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                return await download_image(
                    client, "http://example.com/img.jpg", save_dir, "test.jpg"
                )

        result = _run(run())
        assert result is None

    def test_download_image_auto_filename(self, tmp_path):
        """Test that filename is auto-generated from URL hash when not provided."""
        from src.scrapers.image_downloader import download_image

        save_dir = tmp_path / "test_images"

        image_content = b"\xff\xd8\xff\xe0" + b"\x00" * 2048

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/png"}
        mock_response.content = image_content

        client = AsyncMock()
        client.get = AsyncMock(return_value=mock_response)

        async def run():
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.scrapers.image_downloader.IMAGES_DIR", tmp_path):
                    return await download_image(
                        client, "http://example.com/photo.png", save_dir
                    )

        result = _run(run())
        assert result is not None
        # Check a file was created in save_dir
        files = list(save_dir.iterdir())
        assert len(files) == 1
        assert files[0].suffix == ".png"


class TestDownloadPersonImages:
    def test_download_person_images_no_progress(self):
        from src.scrapers.image_downloader import download_person_images

        persons = [("person:1", "http://img/large.jpg", "http://img/med.jpg")]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/persons/person_1/large.png"
                return await download_person_images(persons, show_progress=False)

        results = _run(run())
        assert "person:1" in results
        assert results["person:1"]["large"] is not None

    def test_download_person_images_no_urls(self):
        from src.scrapers.image_downloader import download_person_images

        persons = [("person:1", None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                result = await download_person_images(persons, show_progress=False)
                return result, mock_dl

        results, mock_dl = _run(run())
        assert results["person:1"]["large"] is None
        assert results["person:1"]["medium"] is None
        mock_dl.assert_not_called()


class TestDownloadAnimeImages:
    def test_download_anime_images_no_progress(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [
            (
                "anime:1",
                "http://cover/l.jpg",
                "http://cover/xl.jpg",
                "http://banner.jpg",
            )
        ]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                return await download_anime_images(anime, show_progress=False)

        results = _run(run())
        assert "anime:1" in results
        assert results["anime:1"]["cover_large"] is not None

    def test_download_anime_images_prefers_xl_cover(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", "http://cover/l.jpg", "http://cover/xl.jpg", None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                await download_anime_images(anime, show_progress=False)
                return mock_dl

        mock_dl = _run(run())
        # Only one call for cover (xl preferred), no banner call
        assert mock_dl.call_count == 1
        call_url = mock_dl.call_args_list[0][0][1]
        assert call_url == "http://cover/xl.jpg"

    def test_download_anime_images_fallback_to_large(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", "http://cover/l.jpg", None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                mock_dl.return_value = "images/anime/anime_1/cover.jpg"
                await download_anime_images(anime, show_progress=False)
                return mock_dl

        mock_dl = _run(run())
        call_url = mock_dl.call_args_list[0][0][1]
        assert call_url == "http://cover/l.jpg"

    def test_download_anime_images_no_covers(self):
        from src.scrapers.image_downloader import download_anime_images

        anime = [("anime:1", None, None, None)]

        async def run():
            with patch(
                "src.scrapers.image_downloader.download_image", new_callable=AsyncMock
            ) as mock_dl:
                result = await download_anime_images(anime, show_progress=False)
                return result, mock_dl

        results, mock_dl = _run(run())
        assert results["anime:1"]["cover_large"] is None
        assert results["anime:1"]["banner"] is None
        mock_dl.assert_not_called()



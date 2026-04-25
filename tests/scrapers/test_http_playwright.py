"""Unit tests for PlaywrightFetcher — no real network calls."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.scrapers.http_playwright import PlaywrightFetcher, _UA, _CF_TITLES


class TestPlaywrightFetcherInit:
    def test_headless_default(self):
        f = PlaywrightFetcher()
        assert f._headless is True

    def test_headless_false_explicit(self):
        f = PlaywrightFetcher(headless=False)
        assert f._headless is False

    def test_headful_env_overrides(self, monkeypatch):
        monkeypatch.setenv("HEADFUL", "1")
        f = PlaywrightFetcher(headless=True)
        assert f._headless is False

    def test_headful_env_absent_preserves_arg(self, monkeypatch):
        monkeypatch.delenv("HEADFUL", raising=False)
        f = PlaywrightFetcher(headless=True)
        assert f._headless is True

    def test_profile_dir_default(self):
        f = PlaywrightFetcher()
        assert f._profile_dir == Path("data/playwright_profile")

    def test_profile_dir_custom(self, tmp_path):
        f = PlaywrightFetcher(profile_dir=tmp_path)
        assert f._profile_dir == tmp_path

    def test_initial_state_none(self):
        f = PlaywrightFetcher()
        assert f._pw is None
        assert f._context is None


class TestUserAgent:
    def test_no_forbidden_tokens(self):
        forbidden = ["Claudebot", "GPTBot", "ChatGPT-User", "Bytespider"]
        for token in forbidden:
            assert token.lower() not in _UA.lower(), f"Forbidden token in UA: {token}"

    def test_looks_like_chrome(self):
        assert "Chrome/" in _UA
        assert "Mozilla/5.0" in _UA


class TestCFTitles:
    def test_contains_just_a_moment(self):
        assert "just a moment" in _CF_TITLES

    def test_contains_attention_required(self):
        assert "attention required" in _CF_TITLES


@pytest.mark.skipif(os.getenv("RUN_E2E") != "1", reason="E2E: set RUN_E2E=1")
class TestPlaywrightFetcherE2E:
    async def test_fetch_atwiki_page(self):
        async with PlaywrightFetcher() as f:
            html = await f.fetch("https://www18.atwiki.jp/sakuga/pages/1.html")
        assert len(html) > 1000
        assert "作画" in html

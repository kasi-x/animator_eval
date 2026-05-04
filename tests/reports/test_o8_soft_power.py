"""Tests for O8 ソフトパワー指標 report.

Covers:
- smoke test: generate() returns a Path with valid HTML
- platform extraction from external_links_json
- soft_power_index: formula invariants (anime.score not used)
- Mann-Whitney U + bootstrap CI: result structure
- lint_vocab: no forbidden terms in report source
- method gate: CI + null model annotations present
- H1 invariant: anime.score not referenced in source or HTML output
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from scripts.report_generators.reports.o8_soft_power import (
    AnimeDistributionProfile,
    MannWhitneyResult,
    PersonNetworkRow,
    PlatformCount,
    _compute_u_stat,
    _extract_platform,
    _normal_sf,
    compute_mann_whitney,
    compute_soft_power_index,
    extract_anime_distribution_profiles,
    fetch_person_network_rows,
    O8SoftPowerReport,
)
import random


# ---------------------------------------------------------------------------
# Synthetic in-memory SQLite DB
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS anime (
    id TEXT PRIMARY KEY,
    title_romaji TEXT,
    episodes INTEGER DEFAULT 12,
    duration INTEGER DEFAULT 24,
    external_links_json TEXT
);

CREATE TABLE IF NOT EXISTS persons (
    id TEXT PRIMARY KEY,
    name_romaji TEXT
);

CREATE TABLE IF NOT EXISTS credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roles (
    name TEXT PRIMARY KEY,
    weight REAL
);
"""

# Anime with various external links
_ANIME_ROWS = [
    # id, title, eps, dur, external_links_json
    ("a1", "Intl Anime A", 12, 24,
     '[{"url": "https://www.netflix.com/title/12345"}, '
     '{"url": "https://www.crunchyroll.com/intl-anime-a"}]'),
    ("a2", "Intl Anime B", 24, 24,
     '[{"url": "https://crunchyroll.com/intl-anime-b"}]'),
    ("a3", "Intl Anime C", 12, 30,
     '[{"url": "https://www.funimation.com/shows/intl-anime-c/"}]'),
    ("a4", "Domestic Only", 13, 24, None),
    ("a5", "Domestic With Link", 1, 90,
     '[{"url": "https://www.example.com/domestic"}]'),  # no matching platform
    ("a6", "Hidive Anime", 12, 24,
     '[{"url": "https://www.hidive.com/stream/hidive-anime"}]'),
]

_PERSON_ROWS = [
    ("p1", "Director One"),
    ("p2", "Animator Two"),
    ("p3", "Animator Three"),
    ("p4", "Designer Four"),
    ("p5", "Director Five"),
]

_CREDIT_ROWS = [
    # Persons on international anime
    ("p1", "a1", "director"),
    ("p1", "a2", "director"),
    ("p2", "a1", "key_animator"),
    ("p3", "a3", "animation_director"),
    ("p4", "a6", "character_designer"),
    # Persons on domestic anime only
    ("p5", "a4", "director"),
    ("p2", "a5", "key_animator"),  # p2 also on domestic (but still counts as intl due to a1)
]

_ROLE_ROWS = [
    ("director", 3.0),
    ("animation_director", 2.8),
    ("key_animator", 2.0),
    ("character_designer", 2.3),
]


def _build_test_db() -> sqlite3.Connection:
    """Create a minimal in-memory DB for O8 tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)
    conn.executemany("INSERT INTO anime VALUES (?, ?, ?, ?, ?)", _ANIME_ROWS)
    conn.executemany("INSERT INTO persons VALUES (?, ?)", _PERSON_ROWS)
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role) VALUES (?, ?, ?)",
        _CREDIT_ROWS,
    )
    conn.executemany("INSERT INTO roles VALUES (?, ?)", _ROLE_ROWS)
    conn.commit()
    return conn


@pytest.fixture
def test_conn() -> sqlite3.Connection:
    return _build_test_db()


@pytest.fixture
def rng() -> random.Random:
    return random.Random(0)


# ---------------------------------------------------------------------------
# Step 1: Platform extraction
# ---------------------------------------------------------------------------


class TestExtractPlatform:
    def test_netflix_detected(self):
        assert _extract_platform("https://www.netflix.com/title/12345") == "netflix"

    def test_crunchyroll_detected(self):
        assert _extract_platform("https://crunchyroll.com/show") == "crunchyroll"

    def test_funimation_detected(self):
        assert _extract_platform("https://www.funimation.com/shows/x/") == "funimation"

    def test_hidive_detected(self):
        assert _extract_platform("https://www.hidive.com/stream/show") == "hidive"

    def test_unknown_url_returns_none(self):
        assert _extract_platform("https://www.example.com/anime") is None

    def test_empty_url_returns_none(self):
        assert _extract_platform("") is None

    def test_case_insensitive(self):
        assert _extract_platform("https://NETFLIX.COM/title/123") == "netflix"


class TestExtractAnimeDistributionProfiles:
    def test_returns_profiles(self, test_conn):
        profiles, platform_counts = extract_anime_distribution_profiles(test_conn)
        assert isinstance(profiles, list)
        assert len(profiles) > 0

    def test_intl_anime_detected(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        anime_ids = {p.anime_id for p in profiles}
        # a1, a2, a3, a6 should be detected (have known platform links)
        assert "a1" in anime_ids
        assert "a2" in anime_ids
        assert "a3" in anime_ids
        assert "a6" in anime_ids

    def test_domestic_anime_not_in_profiles(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        anime_ids = {p.anime_id for p in profiles}
        # a4 (no links), a5 (unknown platform) should not appear
        assert "a4" not in anime_ids
        assert "a5" not in anime_ids

    def test_multi_platform_anime(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        # a1 has netflix + crunchyroll
        a1 = next((p for p in profiles if p.anime_id == "a1"), None)
        assert a1 is not None
        assert a1.platform_count == 2
        assert "netflix" in a1.platforms
        assert "crunchyroll" in a1.platforms

    def test_platform_counts_correct(self, test_conn):
        _, platform_counts = extract_anime_distribution_profiles(test_conn)
        # crunchyroll in a1 + a2 = 2
        assert platform_counts.get("crunchyroll", PlatformCount("", "", 0)).anime_count == 2

    def test_null_external_links_excluded(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        anime_ids = {p.anime_id for p in profiles}
        assert "a4" not in anime_ids  # NULL external_links_json

    def test_empty_db_returns_empty(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            "CREATE TABLE anime (id TEXT, title_romaji TEXT, external_links_json TEXT)"
        )
        profiles, counts = extract_anime_distribution_profiles(conn)
        assert profiles == []
        assert counts == {}


# ---------------------------------------------------------------------------
# Step 2: Person network rows
# ---------------------------------------------------------------------------


class TestFetchPersonNetworkRows:
    def test_returns_rows(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        intl_ids = {p.anime_id for p in profiles}
        rows = fetch_person_network_rows(test_conn, intl_ids)
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_intl_flag_set_correctly(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        intl_ids = {p.anime_id for p in profiles}
        rows = fetch_person_network_rows(test_conn, intl_ids)

        # p1 contributed to a1 (intl) — should be True
        p1_row = next((r for r in rows if r.person_id == "p1"), None)
        assert p1_row is not None
        assert p1_row.is_international is True

    def test_domestic_only_person_flag_false(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        intl_ids = {p.anime_id for p in profiles}
        rows = fetch_person_network_rows(test_conn, intl_ids)

        # p5 contributed only to a4 (domestic) — should be False
        p5_row = next((r for r in rows if r.person_id == "p5"), None)
        assert p5_row is not None
        assert p5_row.is_international is False

    def test_theta_proxy_positive(self, test_conn):
        profiles, _ = extract_anime_distribution_profiles(test_conn)
        intl_ids = {p.anime_id for p in profiles}
        rows = fetch_person_network_rows(test_conn, intl_ids)
        for r in rows:
            assert r.theta_proxy >= 0.0

    def test_empty_intl_ids(self, test_conn):
        rows = fetch_person_network_rows(test_conn, set())
        # All persons should have is_international=False
        for r in rows:
            assert r.is_international is False


# ---------------------------------------------------------------------------
# Step 3: Mann-Whitney U + CI
# ---------------------------------------------------------------------------


class TestComputeUStatistic:
    def test_identical_groups_returns_half_product(self):
        # If both groups are equal, U = n1*n2/2
        a = [1.0, 2.0, 3.0]
        b = [1.0, 2.0, 3.0]
        u = _compute_u_stat(a, b)
        # With ties everywhere: 0.5 * 9 = 4.5
        assert abs(u - 4.5) < 1e-9

    def test_a_dominates_b(self):
        a = [10.0, 11.0, 12.0]
        b = [1.0, 2.0, 3.0]
        u = _compute_u_stat(a, b)
        # All pairs (a > b): U = n1 * n2 = 9
        assert u == 9.0

    def test_b_dominates_a(self):
        a = [1.0, 2.0, 3.0]
        b = [10.0, 11.0, 12.0]
        u = _compute_u_stat(a, b)
        # All pairs (a < b): U = 0
        assert u == 0.0


class TestNormalSf:
    def test_z_0_returns_05(self):
        assert abs(_normal_sf(0.0) - 0.5) < 1e-6

    def test_z_1_96_approx_025(self):
        # p ≈ 0.025 for z=1.96
        p = _normal_sf(1.96)
        assert abs(p - 0.025) < 0.005

    def test_large_z_near_zero(self):
        assert _normal_sf(10.0) < 1e-5


class TestComputeMannWhitney:
    def _make_rows(self, intl_vals, dom_vals) -> list[PersonNetworkRow]:
        rows = []
        for i, v in enumerate(intl_vals):
            rows.append(PersonNetworkRow(
                person_id=f"pi{i}", name=f"Intl {i}", theta_proxy=v, is_international=True
            ))
        for i, v in enumerate(dom_vals):
            rows.append(PersonNetworkRow(
                person_id=f"pd{i}", name=f"Dom {i}", theta_proxy=v, is_international=False
            ))
        return rows

    def test_returns_result_with_sufficient_data(self, rng):
        rows = self._make_rows(
            [3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
            [1.0, 2.0, 3.0],
        )
        result = compute_mann_whitney(rows, rng, n_bootstrap=200)
        assert result is not None
        assert isinstance(result, MannWhitneyResult)

    def test_ci_lower_lte_upper(self, rng):
        rows = self._make_rows(
            [3.0, 4.0, 5.0, 6.0, 7.0],
            [1.0, 2.0, 3.0],
        )
        result = compute_mann_whitney(rows, rng, n_bootstrap=200)
        assert result is not None
        assert result.ci_lower <= result.ci_upper

    def test_p_value_in_range(self, rng):
        rows = self._make_rows(
            [3.0, 4.0, 5.0, 6.0, 7.0],
            [1.0, 2.0, 3.0],
        )
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        assert 0.0 <= result.p_value_approx <= 1.0

    def test_effect_r_in_range(self, rng):
        rows = self._make_rows(
            [3.0, 4.0, 5.0, 6.0, 7.0],
            [1.0, 2.0, 3.0],
        )
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        assert -1.0 <= result.effect_r <= 1.0

    def test_insufficient_intl_returns_none(self, rng):
        # Only 2 intl, below _MIN_INTL_ANIME=5
        rows = self._make_rows([3.0, 4.0], [1.0, 2.0, 3.0])
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is None

    def test_no_domestic_returns_none(self, rng):
        rows = self._make_rows([3.0, 4.0, 5.0, 6.0, 7.0], [])
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is None

    def test_n_counts_correct(self, rng):
        intl = [3.0, 4.0, 5.0, 6.0, 7.0]
        dom = [1.0, 2.0, 3.0]
        rows = self._make_rows(intl, dom)
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        assert result.n_intl == 5
        assert result.n_domestic == 3


# ---------------------------------------------------------------------------
# Step 4: soft_power_index
# ---------------------------------------------------------------------------


class TestComputeSoftPowerIndex:
    def _make_inputs(self):
        platform_counts = {
            "netflix": PlatformCount("netflix", "Netflix", 10),
            "crunchyroll": PlatformCount("crunchyroll", "Crunchyroll", 20),
        }
        profiles = [
            AnimeDistributionProfile("a1", "Anime A", 2, ["netflix", "crunchyroll"]),
            AnimeDistributionProfile("a2", "Anime B", 1, ["crunchyroll"]),
        ]
        person_rows = [
            PersonNetworkRow("p1", "P1", theta_proxy=2.0, is_international=True),
            PersonNetworkRow("p2", "P2", theta_proxy=3.0, is_international=True),
            PersonNetworkRow("p3", "P3", theta_proxy=1.0, is_international=False),
        ]
        return platform_counts, profiles, person_rows

    def test_returns_rows(self):
        pc, pr, per = self._make_inputs()
        rows = compute_soft_power_index(pc, pr, per)
        assert isinstance(rows, list)
        assert len(rows) == 2

    def test_sorted_desc_by_spi(self):
        pc, pr, per = self._make_inputs()
        rows = compute_soft_power_index(pc, pr, per)
        spis = [r.soft_power_index for r in rows]
        assert spis == sorted(spis, reverse=True)

    def test_spi_formula(self):
        pc, pr, per = self._make_inputs()
        rows = compute_soft_power_index(pc, pr, per)
        for r in rows:
            expected_spi = r.anime_count * r.mean_theta_proxy
            assert abs(r.soft_power_index - expected_spi) < 1e-9

    def test_spi_non_negative(self):
        pc, pr, per = self._make_inputs()
        rows = compute_soft_power_index(pc, pr, per)
        for r in rows:
            assert r.soft_power_index >= 0.0

    def test_no_anime_score_in_formula(self):
        """Confirm soft_power_index does not use anime.score (H1)."""
        # The formula uses anime_count × mean_theta_proxy only
        pc = {"x": PlatformCount("x", "X", 5)}
        pr = [AnimeDistributionProfile("a1", "A", 1, ["x"])]
        per = [PersonNetworkRow("p1", "P1", theta_proxy=1.5, is_international=True)]
        rows = compute_soft_power_index(pc, pr, per)
        assert len(rows) == 1
        # SPI = 5 * 1.5 = 7.5
        assert abs(rows[0].soft_power_index - 7.5) < 1e-9

    def test_empty_inputs(self):
        rows = compute_soft_power_index({}, [], [])
        assert rows == []


# ---------------------------------------------------------------------------
# Smoke: full report generation
# ---------------------------------------------------------------------------


class TestO8SoftPowerReportSmoke:
    def test_generate_returns_path(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert isinstance(out, Path)
        assert out.exists()

    def test_output_is_html(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in text or "<html" in text.lower()

    def test_disclaimer_present(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # REPORT_PHILOSOPHY §9: both JA + EN disclaimer required
        assert "ネットワーク" in text
        assert "network structure" in text.lower()

    def test_method_note_present(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # method note must mention CI or bootstrap
        assert "bootstrap" in text.lower() or "ci" in text.lower()

    def test_no_anime_score_in_output(self, test_conn, tmp_path):
        """H1 invariant: anime.score must not appear in output HTML."""
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "anime.score" not in text

    def test_generate_with_empty_db(self, tmp_path):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA_SQL)
        report = O8SoftPowerReport(conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert out.exists()

    def test_soft_power_index_mentioned(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "soft_power_index" in text

    def test_platform_names_in_output(self, test_conn, tmp_path):
        report = O8SoftPowerReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # At least one known platform should appear
        platform_labels = {"Netflix", "Crunchyroll", "Funimation", "HIDIVE"}
        found = any(label in text for label in platform_labels)
        assert found


# ---------------------------------------------------------------------------
# Lint vocab: forbidden terms must not appear in report source
# ---------------------------------------------------------------------------


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestLintVocabCompliance:
    """Verify that o8_soft_power.py contains no forbidden vocabulary."""

    REPORT_FILE = _PROJECT_ROOT / "scripts/report_generators/reports/o8_soft_power.py"
    FORBIDDEN_PATTERN = re.compile(
        r"\b(ability|skill|talent|competence|capability)\b", re.IGNORECASE
    )

    def test_no_forbidden_vocab_in_source(self):
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        matches = self.FORBIDDEN_PATTERN.findall(source)
        assert matches == [], (
            f"Forbidden vocabulary found in o8_soft_power.py: {matches}"
        )

    def test_no_anime_score_in_source(self):
        """H1 invariant: anime.score must not appear in any SQL or formula."""
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        assert "anime.score" not in source, (
            "anime.score found in o8_soft_power.py — violates H1"
        )

    def test_no_display_score_in_source(self):
        """H1 invariant: display_score must not appear in scoring path."""
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        # display_score is only allowed in display_lookup, not in analysis
        assert "display_score" not in source, (
            "display_score found in o8_soft_power.py — violates H1"
        )


# ---------------------------------------------------------------------------
# Method gate: CI + null model both present
# ---------------------------------------------------------------------------


class TestMethodGate:
    """REPORT_PHILOSOPHY §3 — CI and null model must be present."""

    def _make_large_rows(self, n_intl=20, n_dom=30) -> list[PersonNetworkRow]:
        rng = random.Random(99)
        rows = []
        for i in range(n_intl):
            rows.append(PersonNetworkRow(
                f"pi{i}", f"Intl {i}",
                theta_proxy=rng.uniform(1.0, 5.0),
                is_international=True,
            ))
        for i in range(n_dom):
            rows.append(PersonNetworkRow(
                f"pd{i}", f"Dom {i}",
                theta_proxy=rng.uniform(0.5, 3.0),
                is_international=False,
            ))
        return rows

    def test_bootstrap_ci_computed(self):
        rng = random.Random(42)
        rows = self._make_large_rows()
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        # CI must be present (not degenerate: lower == upper is possible but both must exist)
        assert result.ci_lower is not None
        assert result.ci_upper is not None
        assert result.ci_lower <= result.ci_upper

    def test_null_model_is_normal_approximation(self):
        """Null model for MWU is the normal approximation."""
        rng = random.Random(42)
        rows = self._make_large_rows()
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        # p_value_approx must be a valid probability
        assert 0.0 <= result.p_value_approx <= 1.0

    def test_effect_size_r_provided(self):
        rng = random.Random(42)
        rows = self._make_large_rows()
        result = compute_mann_whitney(rows, rng, n_bootstrap=100)
        assert result is not None
        assert result.effect_r is not None
        assert -1.0 <= result.effect_r <= 1.0

    def test_spi_formula_uses_fixed_weights(self):
        """soft_power_index uses fixed platform_weight=1.0 (method note compliance)."""
        platform_counts = {
            "netflix": PlatformCount("netflix", "Netflix", 10),
        }
        profiles = [
            AnimeDistributionProfile("a1", "A", 1, ["netflix"]),
        ]
        person_rows = [
            PersonNetworkRow("p1", "P", theta_proxy=2.0, is_international=True),
        ]
        rows = compute_soft_power_index(platform_counts, profiles, person_rows)
        assert len(rows) == 1
        # With fixed weight=1: SPI = 10 * 2.0 = 20.0
        assert abs(rows[0].soft_power_index - 20.0) < 1e-9

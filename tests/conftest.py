"""pytest 共通設定."""

from pathlib import Path

import duckdb
import pytest
import structlog

from src.runtime.models import (
    BronzeAnime as Anime,
    Character,
    CharacterVoiceActor,
    Credit,
    Role,
)


def build_silver_duckdb(silver_path: Path, persons: list, anime_list: list, credits: list) -> None:
    """Create a minimal silver.duckdb for testing.

    Writes persons, anime, and credits into a fresh DuckDB file at silver_path.
    Column names match integrate_duckdb.py DDL.
    """
    conn = duckdb.connect(str(silver_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id VARCHAR PRIMARY KEY,
            name_ja VARCHAR NOT NULL DEFAULT '',
            name_en VARCHAR NOT NULL DEFAULT '',
            birth_date VARCHAR,
            death_date VARCHAR,
            website_url VARCHAR,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime (
            id VARCHAR PRIMARY KEY,
            title_ja VARCHAR NOT NULL DEFAULT '',
            title_en VARCHAR NOT NULL DEFAULT '',
            year INTEGER,
            season VARCHAR,
            quarter INTEGER,
            episodes INTEGER,
            format VARCHAR,
            duration INTEGER,
            start_date VARCHAR,
            end_date VARCHAR,
            status VARCHAR,
            source_mat VARCHAR,
            work_type VARCHAR,
            scale_class VARCHAR,
            studios VARCHAR[],
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credits (
            person_id VARCHAR NOT NULL,
            anime_id VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            raw_role VARCHAR,
            episode INTEGER,
            evidence_source VARCHAR NOT NULL DEFAULT '',
            affiliation VARCHAR,
            position INTEGER,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)

    for p in persons:
        conn.execute(
            "INSERT OR IGNORE INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
            [p.id, p.name_ja or "", p.name_en or ""],
        )
    for a in anime_list:
        studios_val = getattr(a, "studios", None) or []
        conn.execute(
            "INSERT OR IGNORE INTO anime (id, title_ja, title_en, year, episodes, format, studios) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                a.id,
                getattr(a, "title_ja", "") or "",
                getattr(a, "title_en", "") or "",
                getattr(a, "year", None),
                getattr(a, "episodes", None),
                getattr(a, "format", None),
                studios_val,
            ],
        )
    for c in credits:
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, episode, evidence_source) VALUES (?, ?, ?, ?, ?)",
            [c.person_id, c.anime_id, c.role.value, c.episode, getattr(c, "evidence_source", None) or getattr(c, "source", "") or ""],
        )
    conn.close()


# ============================================================
# Shared fixtures — used by test_va, test_genre_analysis, test_studio_analysis
# ============================================================


@pytest.fixture
def anime_map():
    """5 anime across 3 studios with genres and seasons."""
    return {
        "a1": Anime(
            id="a1",
            title_ja="作品1",
            year=2020,
            episodes=12,
            duration=24,
            genres=["Action", "Adventure"],
            studios=["StudioA"],
            season="winter",
        ),
        "a2": Anime(
            id="a2",
            title_ja="作品2",
            year=2021,
            episodes=24,
            duration=24,
            genres=["Action", "Drama"],
            studios=["StudioA", "StudioB"],
            season="spring",
        ),
        "a3": Anime(
            id="a3",
            title_ja="作品3",
            year=2022,
            episodes=12,
            duration=24,
            genres=["Drama", "Romance"],
            studios=["StudioB"],
            season="summer",
        ),
        "a4": Anime(
            id="a4",
            title_ja="作品4",
            year=2023,
            episodes=13,
            duration=24,
            genres=["Action", "Sci-Fi"],
            studios=["StudioC"],
            season="fall",
        ),
        "a5": Anime(
            id="a5",
            title_ja="作品5",
            year=2024,
            episodes=12,
            duration=24,
            genres=["Comedy", "Romance"],
            studios=["StudioA"],
            season="winter",
        ),
    }


@pytest.fixture
def anime_list(anime_map):
    return list(anime_map.values())


@pytest.fixture
def va_credits():
    """VA credits: VA1=main star, VA2=supporting, VA3=background, VA4=co-star."""
    return [
        CharacterVoiceActor(
            character_id="c1", person_id="va1", anime_id="a1", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c1", person_id="va1", anime_id="a2", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c2", person_id="va1", anime_id="a3", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c3",
            person_id="va1",
            anime_id="a4",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c4",
            person_id="va2",
            anime_id="a1",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c5",
            person_id="va2",
            anime_id="a2",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c6",
            person_id="va2",
            anime_id="a3",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c7",
            person_id="va2",
            anime_id="a4",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c8", person_id="va2", anime_id="a5", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c9",
            person_id="va3",
            anime_id="a1",
            character_role="BACKGROUND",
        ),
        CharacterVoiceActor(
            character_id="c10",
            person_id="va3",
            anime_id="a2",
            character_role="BACKGROUND",
        ),
        CharacterVoiceActor(
            character_id="c11", person_id="va4", anime_id="a1", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c11", person_id="va4", anime_id="a2", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c12",
            person_id="va4",
            anime_id="a3",
            character_role="SUPPORTING",
        ),
    ]


@pytest.fixture
def production_credits():
    """Production credits: sound directors + directors + animators across 5 anime."""
    return [
        Credit(person_id="sd1", anime_id="a1", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd1", anime_id="a2", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd2", anime_id="a3", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd2", anime_id="a4", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd1", anime_id="a5", role=Role.SOUND_DIRECTOR),
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a5", role=Role.DIRECTOR),
        Credit(person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a4", role=Role.KEY_ANIMATOR),
        Credit(person_id="p6", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p6", anime_id="a5", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p7", anime_id="a3", role=Role.IN_BETWEEN),
        Credit(person_id="p7", anime_id="a4", role=Role.KEY_ANIMATOR),
    ]


@pytest.fixture
def characters():
    """12 characters alternating gender."""
    return {
        f"c{i}": Character(
            id=f"c{i}",
            name_ja=f"キャラ{i}",
            gender="Male" if i % 2 == 0 else "Female",
        )
        for i in range(1, 13)
    }


@pytest.fixture
def person_fe():
    """Person fixed-effect scores for 7 production staff."""
    return {
        "p1": 1.5,
        "p2": 0.8,
        "p3": 1.2,
        "p4": 0.5,
        "p5": 0.3,
        "p6": 0.9,
        "p7": -0.2,
    }


# ============================================================
# DuckDB & file path fixtures — used by integration tests
# ============================================================


@pytest.fixture
def silver_path(tmp_path: Path) -> Path:
    """Path to temporary silver.duckdb file."""
    return tmp_path / "silver.duckdb"


@pytest.fixture
def gold_path(tmp_path: Path) -> Path:
    """Path to temporary gold.duckdb file."""
    return tmp_path / "gold.duckdb"


@pytest.fixture
def silver_gold_dbs(tmp_path: Path, monkeypatch):
    """Create both silver and gold DuckDB files with schema."""
    silver = tmp_path / "silver.duckdb"
    gold = tmp_path / "gold.duckdb"

    # Monkeypatch paths for pipeline
    monkeypatch.setattr("src.analysis.io.silver_reader.DEFAULT_SILVER_PATH", silver)
    monkeypatch.setattr("src.analysis.io.gold_writer.DEFAULT_GOLD_DB_PATH", gold)

    return {"silver": silver, "gold": gold}


def pytest_configure(config):
    """structlog をテスト用に設定する.

    テスト時はログ出力を抑制し、pytest の出力キャプチャとの衝突を回避する。
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    config.addinivalue_line(
        "markers",
        "requires_meta_tables: skip unless the meta_* tables are populated",
    )

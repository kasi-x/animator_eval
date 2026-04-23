"""pytest 共通設定."""

from pathlib import Path

import duckdb
import structlog


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

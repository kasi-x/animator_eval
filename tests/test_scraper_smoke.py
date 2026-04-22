"""Scraper writes must succeed against the new schema (synthetic input)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from src.database import init_db, insert_credit, upsert_anime, upsert_person
from src.models import BronzeAnime as Anime, Credit, Person, Role


def test_scraper_writes_all_layers(tmp_path: Path):
    db = tmp_path / "smoke.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        init_db(conn)

        anime = Anime(id="anilist:1", title_en="Cowboy Bebop", title_ja="カウボーイビバップ", year=1998)
        person = Person(id="anilist:p1", name_en="Shinichiro Watanabe", name_ja="渡辺信一郎")
        credit = Credit(person_id="anilist:p1", anime_id="anilist:1", role=Role.DIRECTOR, source="anilist")

        upsert_anime(conn, anime)
        upsert_person(conn, person)
        insert_credit(conn, credit)
        conn.commit()

        anime_count = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        persons_count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        credits_count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]

        assert anime_count == 1
        assert persons_count == 1
        assert credits_count == 1

        row = conn.execute("SELECT id, title_en FROM anime WHERE id = 'anilist:1'").fetchone()
        assert row["id"] == "anilist:1"
        assert row["title_en"] == "Cowboy Bebop"

    finally:
        conn.close()

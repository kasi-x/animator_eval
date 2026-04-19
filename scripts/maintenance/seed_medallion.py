"""Bronze テーブル (src_*) にシードデータを投入して ETL を実行するスクリプト.

実際のスクレイピングなしに新メダリオンアーキテクチャの動作を検証する。
複数ソースで同じ人物・作品が重複して取得される状況を再現する。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import structlog

log = structlog.get_logger()

# ─── シードデータ ─────────────────────────────────────────────────────────────

# AniList データ (整数ID, Pydantic モデル経由でない)
ANILIST_ANIME = [
    {
        "anilist_id": 1,
        "title_ja": "カウボーイビバップ",
        "title_en": "Cowboy Bebop",
        "year": 1998,
        "season": "SPRING",
        "episodes": 26,
        "format": "TV",
        "status": "FINISHED",
        "start_date": "1998-04-03",
        "end_date": "1999-04-24",
        "duration": 24,
        "source": "ORIGINAL",
        "description": "Space bounty hunters.",
        "score": 86,
        "genres": ["Action", "Adventure", "Drama"],
        "tags": [],
        "studios": ["Sunrise"],
        "synonyms": [],
        "cover_large": None,
        "cover_medium": None,
        "banner": None,
        "popularity": 1,
        "favourites": 50000,
        "site_url": "https://anilist.co/anime/1",
        "mal_id": 1,
    },
    {
        "anilist_id": 20,
        "title_ja": "NARUTO",
        "title_en": "Naruto",
        "year": 2002,
        "season": "FALL",
        "episodes": 220,
        "format": "TV",
        "status": "FINISHED",
        "start_date": "2002-10-03",
        "end_date": "2007-02-08",
        "duration": 23,
        "source": "MANGA",
        "description": "Ninja story.",
        "score": 78,
        "genres": ["Action", "Adventure"],
        "tags": [],
        "studios": ["Pierrot"],
        "synonyms": [],
        "cover_large": None,
        "cover_medium": None,
        "banner": None,
        "popularity": 2,
        "favourites": 40000,
        "site_url": "https://anilist.co/anime/20",
        "mal_id": 20,
    },
]

ANILIST_PERSONS = [
    {
        "anilist_id": 101,
        "name_ja": "渡辺信一郎",
        "name_en": "Shinichiro Watanabe",
        "aliases": [],
        "date_of_birth": "1965-05-24",
        "age": None,
        "gender": "Male",
        "years_active": [],
        "hometown": "Kyoto",
        "blood_type": "A",
        "description": "Director of Cowboy Bebop.",
        "image_large": None,
        "image_medium": None,
        "favourites": 5000,
        "site_url": None,
    },
    {
        "anilist_id": 102,
        "name_ja": "菅野よう子",
        "name_en": "Yoko Kanno",
        "aliases": ["Gabriela Robin"],
        "date_of_birth": "1963-03-18",
        "age": None,
        "gender": "Female",
        "years_active": [],
        "hometown": "Miyagi",
        "blood_type": None,
        "description": "Composer.",
        "image_large": None,
        "image_medium": None,
        "favourites": 8000,
        "site_url": None,
    },
    {
        "anilist_id": 201,
        "name_ja": "岸本斉史",
        "name_en": "Masashi Kishimoto",
        "aliases": [],
        "date_of_birth": "1974-11-08",
        "age": None,
        "gender": "Male",
        "years_active": [],
        "hometown": "Okayama",
        "blood_type": None,
        "description": "Naruto creator.",
        "image_large": None,
        "image_medium": None,
        "favourites": 3000,
        "site_url": None,
    },
]

ANILIST_CREDITS = [
    {"anilist_anime_id": 1,  "anilist_person_id": 101, "role": "director",         "role_raw": "Director"},
    {"anilist_anime_id": 1,  "anilist_person_id": 102, "role": "music",            "role_raw": "Music"},
    {"anilist_anime_id": 20, "anilist_person_id": 201, "role": "original_creator", "role_raw": "Original Story"},
]

# ─── ANN データ ───────────────────────────────────────────────────────────────

ANN_ANIME = [
    {
        "ann_id": 44,
        "title_en": "Cowboy Bebop",
        "title_ja": "カウボーイビバップ",
        "year": 1998,
        "episodes": 26,
        "format": "TV",
        "genres": ["Action", "Science Fiction"],
        "start_date": "1998-04-03",
        "end_date": "1999-04-24",
    },
]

ANN_PERSONS = [
    {
        "ann_id": 501,
        "name_en": "Shinichiro Watanabe",
        "name_ja": "渡辺信一郎",
        "date_of_birth": "1965-05-24",
        "hometown": "Kyoto, Japan",
        "blood_type": "A",
        "website": None,
        "description": "Director known for Cowboy Bebop and Samurai Champloo.",
    },
    {
        "ann_id": 502,
        "name_en": "Yoko Kanno",
        "name_ja": "菅野よう子",
        "date_of_birth": "1963-03-18",
        "hometown": "Miyagi, Japan",
        "blood_type": None,
        "website": None,
        "description": "Prolific anime composer.",
    },
]

ANN_CREDITS = [
    {"ann_anime_id": 44, "ann_person_id": 501, "name_en": "Shinichiro Watanabe", "role": "director",   "role_raw": "Director"},
    {"ann_anime_id": 44, "ann_person_id": 502, "name_en": "Yoko Kanno",          "role": "music",      "role_raw": "Music"},
]

# ─── allcinema データ ─────────────────────────────────────────────────────────

ALLCINEMA_ANIME = [
    {
        "allcinema_id": 90001,
        "title_ja": "カウボーイビバップ",
        "year": 1998,
        "start_date": "1998-04-03",
        "synopsis": "賞金稼ぎたちの宇宙の旅。",
    },
]

ALLCINEMA_PERSONS = [
    {
        "allcinema_id": 80001,
        "name_ja": "渡辺信一郎",
        "yomigana": "わたなべしんいちろう",
        "name_en": "Shinichiro Watanabe",
    },
    {
        "allcinema_id": 80002,
        "name_ja": "菅野よう子",
        "yomigana": "かんのようこ",
        "name_en": "Yoko Kanno",
    },
]

ALLCINEMA_CREDITS = [
    {"allcinema_anime_id": 90001, "allcinema_person_id": 80001, "name_ja": "渡辺信一郎", "name_en": "Shinichiro Watanabe", "job_name": "監督",   "job_id": 1},
    {"allcinema_anime_id": 90001, "allcinema_person_id": 80002, "name_ja": "菅野よう子", "name_en": "Yoko Kanno",          "job_name": "音楽",   "job_id": 7},
]


# ─── 投入処理 ─────────────────────────────────────────────────────────────────

def seed(conn: sqlite3.Connection) -> None:
    """全 src_* テーブルにシードデータを投入する."""

    # AniList anime
    for a in ANILIST_ANIME:
        conn.execute(
            """INSERT OR REPLACE INTO src_anilist_anime
               (anilist_id, title_ja, title_en, year, season, episodes, format, status,
                start_date, end_date, duration, source, description, score,
                genres, tags, studios, synonyms, cover_large, cover_medium, banner,
                popularity, favourites, site_url, mal_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                a["anilist_id"], a["title_ja"], a["title_en"], a["year"], a["season"],
                a["episodes"], a["format"], a["status"], a["start_date"], a["end_date"],
                a["duration"], a["source"], a["description"], a["score"],
                json.dumps(a["genres"]), json.dumps(a["tags"]),
                json.dumps(a["studios"]), json.dumps(a["synonyms"]),
                a["cover_large"], a["cover_medium"], a["banner"],
                a["popularity"], a["favourites"], a["site_url"], a["mal_id"],
            ),
        )

    # AniList persons
    for p in ANILIST_PERSONS:
        conn.execute(
            """INSERT OR REPLACE INTO src_anilist_persons
               (anilist_id, name_ja, name_en, aliases, date_of_birth, age, gender,
                years_active, hometown, blood_type, description,
                image_large, image_medium, favourites, site_url)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p["anilist_id"], p["name_ja"], p["name_en"],
                json.dumps(p["aliases"]), p["date_of_birth"], p["age"], p["gender"],
                json.dumps(p["years_active"]), p["hometown"], p["blood_type"],
                p["description"], p["image_large"], p["image_medium"],
                p["favourites"], p["site_url"],
            ),
        )

    # AniList credits
    for c in ANILIST_CREDITS:
        conn.execute(
            "INSERT OR IGNORE INTO src_anilist_credits (anilist_anime_id, anilist_person_id, role, role_raw) VALUES (?,?,?,?)",
            (c["anilist_anime_id"], c["anilist_person_id"], c["role"], c["role_raw"]),
        )

    # ANN anime
    for a in ANN_ANIME:
        conn.execute(
            """INSERT OR REPLACE INTO src_ann_anime
               (ann_id, title_en, title_ja, year, episodes, format, genres, start_date, end_date)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (a["ann_id"], a["title_en"], a["title_ja"], a["year"], a["episodes"],
             a["format"], json.dumps(a["genres"]), a["start_date"], a["end_date"]),
        )

    # ANN persons
    for p in ANN_PERSONS:
        conn.execute(
            """INSERT OR REPLACE INTO src_ann_persons
               (ann_id, name_en, name_ja, date_of_birth, hometown, blood_type, website, description)
               VALUES (?,?,?,?,?,?,?,?)""",
            (p["ann_id"], p["name_en"], p["name_ja"], p["date_of_birth"],
             p["hometown"], p["blood_type"], p["website"], p["description"]),
        )

    # ANN credits
    for c in ANN_CREDITS:
        conn.execute(
            "INSERT OR IGNORE INTO src_ann_credits (ann_anime_id, ann_person_id, name_en, role, role_raw) VALUES (?,?,?,?,?)",
            (c["ann_anime_id"], c["ann_person_id"], c["name_en"], c["role"], c["role_raw"]),
        )

    # allcinema anime
    for a in ALLCINEMA_ANIME:
        conn.execute(
            """INSERT OR REPLACE INTO src_allcinema_anime
               (allcinema_id, title_ja, year, start_date, synopsis)
               VALUES (?,?,?,?,?)""",
            (a["allcinema_id"], a["title_ja"], a["year"], a["start_date"], a["synopsis"]),
        )

    # allcinema persons
    for p in ALLCINEMA_PERSONS:
        conn.execute(
            """INSERT OR REPLACE INTO src_allcinema_persons
               (allcinema_id, name_ja, yomigana, name_en)
               VALUES (?,?,?,?)""",
            (p["allcinema_id"], p["name_ja"], p["yomigana"], p["name_en"]),
        )

    # allcinema credits
    for c in ALLCINEMA_CREDITS:
        conn.execute(
            """INSERT OR IGNORE INTO src_allcinema_credits
               (allcinema_anime_id, allcinema_person_id, name_ja, name_en, job_name, job_id)
               VALUES (?,?,?,?,?,?)""",
            (c["allcinema_anime_id"], c["allcinema_person_id"],
             c["name_ja"], c["name_en"], c["job_name"], c["job_id"]),
        )

    conn.commit()
    print("✅ シードデータ投入完了")


def report(conn: sqlite3.Connection) -> None:
    """src_* テーブルと正規テーブルの状態を表示する."""
    src_tables = [
        "src_anilist_anime", "src_anilist_persons", "src_anilist_credits",
        "src_ann_anime", "src_ann_persons", "src_ann_credits",
        "src_allcinema_anime", "src_allcinema_persons", "src_allcinema_credits",
    ]
    print("\n── Bronze (src_*) ──────────────────────────────")
    for t in src_tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:40s} {n:>4} 件")

    print("\n── Silver (正規テーブル) 追加分 ─────────────────")
    # 今回のシードで追加されたもの
    rows = conn.execute(
        "SELECT id, title_ja, title_en, year FROM anime WHERE id LIKE 'anilist:%' OR id LIKE 'ann-%' OR id LIKE 'allcinema:%' ORDER BY id"
    ).fetchall()
    for r in rows:
        print(f"  anime: {r[0]:30s}  {r[1] or r[2]}")

    rows = conn.execute(
        "SELECT id, name_ja, name_en FROM persons WHERE id LIKE 'anilist:p%' OR id LIKE 'ann-%' OR id LIKE 'allcinema:%' ORDER BY id"
    ).fetchall()
    for r in rows:
        print(f"  person: {r[0]:28s}  {r[1] or r[2]}")

    rows = conn.execute(
        "SELECT p.name_ja, a.title_ja, c.role, c.source FROM credits c JOIN persons p ON p.id=c.person_id JOIN anime a ON a.id=c.anime_id WHERE c.source IN ('anilist','ann','allcinema') ORDER BY c.source, p.name_ja"
    ).fetchall()
    print(f"\n── クレジット ({len(rows)} 件) ──────────────────────")
    for r in rows:
        print(f"  [{r[3]:10s}] {r[0]:20s} → {r[1]:20s} ({r[2]})")


if __name__ == "__main__":
    from src.database import get_connection, init_db, _run_migrations
    from src.etl.integrate import run_integration

    conn = get_connection()
    init_db(conn)
    _run_migrations(conn)

    print("=== Step 1: Bronze テーブルにシードデータ投入 ===")
    seed(conn)

    print("\n=== Step 2: ETL (Bronze → Silver) ===")
    results = run_integration(conn)
    for source, stats in results.items():
        print(f"  {source}: {stats}")

    print("\n=== Step 3: 結果確認 ===")
    report(conn)

    conn.close()

"""正規テーブル → src_* バックフィルスクリプト.

既存データ（旧スクレイパーが正規テーブルに直書きしたもの）を
Bronze (src_*) テーブルにコピーする。

INSERT OR IGNORE で実行するため、既存の src_* データは上書きしない。
"""

from __future__ import annotations

import sqlite3
import time

import structlog

log = structlog.get_logger()


def backfill_anilist(conn: sqlite3.Connection) -> dict[str, int]:
    """anime/persons/credits → src_anilist_*"""
    stats: dict[str, int] = {}

    # anime
    conn.execute("""
        INSERT OR IGNORE INTO src_anilist_anime
            (anilist_id, title_ja, title_en, year, season, episodes, format, status,
             start_date, end_date, duration, source, description, score,
             genres, tags, studios, synonyms, cover_large, cover_medium, banner,
             popularity, favourites, site_url, mal_id)
        SELECT
            anilist_id, title_ja, title_en, year, season, episodes, format, status,
            start_date, end_date, duration, source, description, score,
            COALESCE(genres, '[]'), COALESCE(tags, '[]'),
            COALESCE(studios, '[]'), COALESCE(synonyms, '[]'),
            cover_large, cover_medium, banner,
            popularity_rank, favourites, site_url, mal_id
        FROM anime
        WHERE anilist_id IS NOT NULL
    """)
    stats["anime"] = conn.execute(
        "SELECT COUNT(*) FROM src_anilist_anime"
    ).fetchone()[0]

    # persons
    conn.execute("""
        INSERT OR IGNORE INTO src_anilist_persons
            (anilist_id, name_ja, name_en, aliases, date_of_birth, age, gender,
             years_active, hometown, blood_type, description,
             image_large, image_medium, favourites, site_url)
        SELECT
            anilist_id, name_ja, name_en,
            COALESCE(aliases, '[]'), date_of_birth, age, gender,
            COALESCE(years_active, '[]'), hometown, blood_type, description,
            image_large, image_medium, favourites, site_url
        FROM persons
        WHERE anilist_id IS NOT NULL
    """)
    stats["persons"] = conn.execute(
        "SELECT COUNT(*) FROM src_anilist_persons"
    ).fetchone()[0]

    # credits — anime_id "anilist:N" / person_id "anilist:pN" から整数IDを取得
    conn.execute("""
        INSERT OR IGNORE INTO src_anilist_credits
            (anilist_anime_id, anilist_person_id, role, role_raw)
        SELECT
            CAST(REPLACE(c.anime_id, 'anilist:', '') AS INTEGER),
            CAST(REPLACE(c.person_id, 'anilist:p', '') AS INTEGER),
            c.role,
            c.raw_role
        FROM credits c
        WHERE c.evidence_source = 'anilist'
          AND c.anime_id LIKE 'anilist:%'
          AND c.person_id LIKE 'anilist:p%'
    """)
    stats["credits"] = conn.execute(
        "SELECT COUNT(*) FROM src_anilist_credits"
    ).fetchone()[0]

    return stats


def backfill_ann(conn: sqlite3.Connection) -> dict[str, int]:
    """anime/persons/credits → src_ann_*"""
    stats: dict[str, int] = {}

    conn.execute("""
        INSERT OR IGNORE INTO src_ann_anime
            (ann_id, title_en, title_ja, year, episodes, format, genres, start_date, end_date)
        SELECT
            ann_id, COALESCE(title_en,''), COALESCE(title_ja,''),
            year, episodes, format,
            COALESCE(genres, '[]'), start_date, end_date
        FROM anime
        WHERE ann_id IS NOT NULL
    """)
    stats["anime"] = conn.execute(
        "SELECT COUNT(*) FROM src_ann_anime"
    ).fetchone()[0]

    conn.execute("""
        INSERT OR IGNORE INTO src_ann_persons
            (ann_id, name_en, name_ja, date_of_birth, hometown, blood_type, website, description)
        SELECT
            ann_id, COALESCE(name_en,''), COALESCE(name_ja,''),
            date_of_birth, hometown, blood_type, site_url, description
        FROM persons
        WHERE ann_id IS NOT NULL
    """)
    stats["persons"] = conn.execute(
        "SELECT COUNT(*) FROM src_ann_persons"
    ).fetchone()[0]

    # ANN credits: source='ann', 両方の ann_id が取得できるものだけ
    conn.execute("""
        INSERT OR IGNORE INTO src_ann_credits
            (ann_anime_id, ann_person_id, name_en, role, role_raw)
        SELECT
            a.ann_id, p.ann_id,
            COALESCE(p.name_en, ''),
            c.role, c.raw_role
        FROM credits c
        JOIN anime   a ON a.id = c.anime_id
        JOIN persons p ON p.id = c.person_id
        WHERE c.evidence_source = 'ann'
          AND a.ann_id IS NOT NULL
          AND p.ann_id IS NOT NULL
    """)
    stats["credits"] = conn.execute(
        "SELECT COUNT(*) FROM src_ann_credits"
    ).fetchone()[0]

    return stats


def backfill_allcinema(conn: sqlite3.Connection) -> dict[str, int]:
    """anime/persons/credits → src_allcinema_*"""
    stats: dict[str, int] = {}

    conn.execute("""
        INSERT OR IGNORE INTO src_allcinema_anime
            (allcinema_id, title_ja, year, start_date, synopsis)
        SELECT allcinema_id, COALESCE(title_ja,''), year, start_date, description
        FROM anime
        WHERE allcinema_id IS NOT NULL
    """)
    stats["anime"] = conn.execute(
        "SELECT COUNT(*) FROM src_allcinema_anime"
    ).fetchone()[0]

    conn.execute("""
        INSERT OR IGNORE INTO src_allcinema_persons
            (allcinema_id, name_ja, yomigana, name_en)
        SELECT allcinema_id, COALESCE(name_ja,''), '', COALESCE(name_en,'')
        FROM persons
        WHERE allcinema_id IS NOT NULL
    """)
    stats["persons"] = conn.execute(
        "SELECT COUNT(*) FROM src_allcinema_persons"
    ).fetchone()[0]

    conn.execute("""
        INSERT OR IGNORE INTO src_allcinema_credits
            (allcinema_anime_id, allcinema_person_id, name_ja, name_en, job_name, job_id)
        SELECT
            a.allcinema_id, p.allcinema_id,
            COALESCE(p.name_ja,''), COALESCE(p.name_en,''),
            COALESCE(c.raw_role, ''), 0
        FROM credits c
        JOIN anime   a ON a.id = c.anime_id
        JOIN persons p ON p.id = c.person_id
        WHERE a.allcinema_id IS NOT NULL
          AND p.allcinema_id IS NOT NULL
    """)
    stats["credits"] = conn.execute(
        "SELECT COUNT(*) FROM src_allcinema_credits"
    ).fetchone()[0]

    return stats


def backfill_seesaawiki(conn: sqlite3.Connection) -> dict[str, int]:
    """anime/credits → src_seesaawiki_*"""
    stats: dict[str, int] = {}

    conn.execute("""
        INSERT OR IGNORE INTO src_seesaawiki_anime
            (id, title_ja, year, episodes)
        SELECT id, COALESCE(title_ja,''), year, episodes
        FROM anime
        WHERE id LIKE 'seesaa:%'
    """)
    stats["anime"] = conn.execute(
        "SELECT COUNT(*) FROM src_seesaawiki_anime"
    ).fetchone()[0]

    # credits: person名は persons.name_ja から取得
    conn.execute("""
        INSERT OR IGNORE INTO src_seesaawiki_credits
            (anime_src_id, person_name, role, role_raw, episode, affiliation, is_company)
        SELECT
            c.anime_id,
            COALESCE(p.name_ja, ''),
            c.role,
            COALESCE(c.raw_role, ''),
            CASE WHEN c.episode = -1 THEN -1 ELSE COALESCE(c.episode, -1) END,
            NULL,
            0
        FROM credits c
        JOIN persons p ON p.id = c.person_id
        WHERE c.evidence_source = 'seesaawiki'
          AND c.anime_id LIKE 'seesaa:%'
    """)
    stats["credits"] = conn.execute(
        "SELECT COUNT(*) FROM src_seesaawiki_credits"
    ).fetchone()[0]

    return stats


def backfill_keyframe(conn: sqlite3.Connection) -> dict[str, int]:
    """anime/credits → src_keyframe_*

    keyframe:slug → src_keyframe_anime
    keyframe:slug credits のみ (anilist: に解決済みの分は slug が失われているため除外)
    """
    stats: dict[str, int] = {}

    conn.execute("""
        INSERT OR IGNORE INTO src_keyframe_anime
            (slug, title_ja, title_en, anilist_id)
        SELECT
            REPLACE(id, 'keyframe:', ''),
            COALESCE(title_ja, ''),
            COALESCE(title_en, ''),
            anilist_id
        FROM anime
        WHERE id LIKE 'keyframe:%'
    """)
    stats["anime"] = conn.execute(
        "SELECT COUNT(*) FROM src_keyframe_anime"
    ).fetchone()[0]

    # person_id は "keyframe:p_{n}" → n は整数 (負値もある)
    # anime_id LIKE 'keyframe:%' のクレジットのみバックフィル
    conn.execute("""
        INSERT OR IGNORE INTO src_keyframe_credits
            (keyframe_slug, kf_person_id, name_ja, name_en, role_ja, role_en, episode)
        SELECT
            REPLACE(c.anime_id, 'keyframe:', ''),
            CAST(REPLACE(c.person_id, 'keyframe:p_', '') AS INTEGER),
            COALESCE(p.name_ja, ''),
            COALESCE(p.name_en, ''),
            COALESCE(c.raw_role, ''),
            '',
            CASE WHEN c.episode = -1 THEN -1 ELSE COALESCE(c.episode, -1) END
        FROM credits c
        JOIN persons p ON p.id = c.person_id
        WHERE c.evidence_source = 'keyframe'
          AND c.anime_id LIKE 'keyframe:%'
          AND c.person_id LIKE 'keyframe:p_%'
    """)
    stats["credits"] = conn.execute(
        "SELECT COUNT(*) FROM src_keyframe_credits"
    ).fetchone()[0]

    return stats


def run_backfill(conn: sqlite3.Connection) -> None:
    sources = [
        ("anilist",    backfill_anilist),
        ("ann",        backfill_ann),
        ("allcinema",  backfill_allcinema),
        ("seesaawiki", backfill_seesaawiki),
        ("keyframe",   backfill_keyframe),
    ]

    total_start = time.monotonic()
    for source, fn in sources:
        t0 = time.monotonic()
        stats = fn(conn)
        conn.commit()
        elapsed = time.monotonic() - t0
        log.info(
            "backfill_done",
            source=source,
            elapsed_s=round(elapsed, 1),
            **stats,
        )

    # サマリー
    print()
    print("=== バックフィル結果 ===")
    src_tables = [
        "src_anilist_anime", "src_anilist_persons", "src_anilist_credits",
        "src_ann_anime", "src_ann_persons", "src_ann_credits",
        "src_allcinema_anime", "src_allcinema_persons", "src_allcinema_credits",
        "src_seesaawiki_anime", "src_seesaawiki_credits",
        "src_keyframe_anime", "src_keyframe_credits",
    ]
    for t in src_tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:40s} {n:>8,} 件")

    print(f"\n  合計所要時間: {time.monotonic() - total_start:.1f}s")

    # keyframe の注記
    kf_total = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE evidence_source='keyframe'"
    ).fetchone()[0]
    kf_backfilled = conn.execute(
        "SELECT COUNT(*) FROM src_keyframe_credits"
    ).fetchone()[0]
    if kf_total > kf_backfilled:
        print(f"\n  ※ keyframe クレジット {kf_total:,} 件中 {kf_backfilled:,} 件のみバックフィル済み")
        print(f"     残り {kf_total - kf_backfilled:,} 件はAniListとマッチ済みでslug情報が失われているため除外")


if __name__ == "__main__":
    from src.db import get_connection, init_db

    conn = get_connection()
    init_db(conn)
    run_backfill(conn)
    conn.close()

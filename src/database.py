"""SQLite database management."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import structlog

from src.models import (
    BronzeAnime as Anime,
    AnimeRelation,
    AnimeStudio,
    Character,
    CharacterVoiceActor,
    Credit,
    Person,
    ScoreResult,
    Studio,
)
from src.utils.config import DB_PATH

logger = structlog.get_logger()

DEFAULT_DB_PATH = DB_PATH

SCHEMA_VERSION = 58

# Fuzzy match rules for unmatched anime titles (90%+ confidence)
# Entries where SeesaaWiki title slightly differs from AniList title
_FUZZY_MATCH_RULES = {
    "AKIBA'S TRIP -THE ANIMATION-": ("Akiba's Trip -The Animation-", 2017),
    "Vivy -Fluorite Eye's Song-": ("Vivy -Fluorite Eye's Song-", 2021),
    "妖怪ウォッチJam 妖怪学園Y 〜Nとの遭遇〜": (
        "妖怪ウォッチJam: 妖怪学園Y 〜Nとの遭遇〜",
        2020,
    ),
    "ウルトラヴァイオレット：コード044": ("ウルトラヴァイオレットコード044", 2008),
    "魔法遊戯 飛び出す!!ハナマル大冒険": ("魔法遊戯: 飛び出す!!ハナマル大冒険", 2001),
    "お前が魔王に勝てると思うなと勇者パーティを追放されたので王都で気ままに暮らしたい": (
        "「お前ごときが魔王に勝てると思うな」と勇者パーティを追放されたので、王都で気ままに暮らしたい",
        2026,
    ),
    "神霊狩/GHOST HOUND": ("神霊狩 Ghost Hound", 2007),
    "うたの☆プリンスさまっ♪ マジLOVEレボリューションズ": (
        "うたの☆プリンスさまっ♪マジLOVEレボリューションス",
        2015,
    ),
    "フォーチュンクエストL": ("フォーチュン·クエストL", 1997),
    "マンガで分かる！Fate/Grand Order": ("マンガでわかる！Fate/Grand Order", 2018),
    "シュヴァリエ 〜Le Chevalier D'Eon〜": ("シェヴァリエ ~Le Chevalier D'Eon~", 2006),
    "乙女はお姉さまに恋してる 2人のエルダー": (
        "処女はお姉さまに恋してる ～2人のエルダー～",
        2012,
    ),
    "無職転生\ufffd\ufffd 〜異世界行ったら本気だす〜": (
        "無職転生 ～異世界行ったら本気だす～",
        2021,
    ),
    "ドラゴノーツ -ザ・レゾナンス-": ("ドラゴノーツ-ザ・レソナンス-", 2007),
    "アクティヴレイド2nd 機動強襲室第八係": ("アクティヴレイド 機動強襲室第八係", 2016),
    "W'z《ウィズ》": ("W'ｚ《ウィズ》", 2019),
    "みだらな青ちゃんは勉強ができない": ("淫らな青ちゃんは勉強ができない", 2019),
    "天地無用! 魎皇鬼 第三期": ("天地無用! 魎皇鬼 第1期", 1992),
    '"文学少女"メモワール': ("文学少女 メモワール", 2014),
    # added v24 (85%+ confidence, manually verified)
    "ゾイドフューザーズ": ("ゾイド・フューザース", 2004),
    "ゆるゆり さん☆ハイ!": ("ゆるゆり さん☆はい！", 2015),
    "スラップアップパーティー -アラド戦記-": (
        "アラド戦記 ～スラップアップパーティー～",
        2009,
    ),
    "TOKKO 特公": ("TOKKÔ[特公]", 2006),
    "I・R・I・A ZEIRAM THE ANIMATION": ("I・Я・I・A ZЁIЯAM THE ANIMATION", 1994),
    "ビーストウォーズネオ 超生命体トランスフォーマー": (
        "ビーストウォーズⅡ（セカンド） 超生命体トランスフォーマー",
        1998,
    ),
    "おちゃめなふたご クレア学院物語": ("おちゃめな双子　－クレア学院物語－", 1991),
    "サンリオ世界名作劇場": ("サンリオ・アニメ世界名作劇場", 2001),
    "まほろまてぃっく特別編 ただいま◆おかえり": (
        "まほろまてぃっく ただいま◇おかえり",
        2009,
    ),
    "空の境界 第五章　矛盾螺旋": ("空の境界 矛盾螺旋", 2008),
    "空の境界 第七章　殺人考察（後）": ("空の境界 殺人考察(後)", 2009),
    "東京魔人學園剣風帖 龍龍 第弐幕": ("東京魔人學園剣風帖　龖（トウ） 第弐幕", 2007),
    "メジャーセカンド（第2シリーズ）": ("メジャー2nd 第２シリーズ", 2020),
    "テイルズ オブ シンフォニア THE ANIMATION（第3期）": (
        "テイルズ オブ シンフォニア THE ANIMATION テセアラ編",
        2010,
    ),
    "テイルズ オブ シンフォニア THE ANIMATION（第2期）": (
        "テイルズ オブ シンフォニア THE ANIMATION テセアラ編",
        2010,
    ),
    "マジでオタクなイングリッシュ!りぼんちゃん the TV": (
        "マジでオタクなイングリッシュ! りぼんちゃん ~英語で戦う魔法少女~ the TV",
        2013,
    ),
    "Bビーダマン爆外伝V": ("B[ボンバーマン]ビーダマン爆外伝Ｖ", 1999),
    "Bビーダマン爆外伝": ("B[ボンバーマン]ビーダマン爆外伝", 1998),
    "頭文字D Second Stage": ("頭文字〈イニシャル〉D SECOND STAGE", 1999),
    "頭文字D Fourth Stage": ("頭文字〈イニシャル〉D FOURTH STAGE", 2004),
    "頭文字D Fifth Stage": ("頭文字〈イニシャル〉D Fifth Stage", 2012),
    "攻殻機動隊 ARISE　border:1 Ghost Pain": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:1 Ghost Pain",
        2013,
    ),
    "攻殻機動隊 ARISE　border:2 Ghost Whispers": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:2 Ghost Whispers",
        2013,
    ),
    "攻殻機動隊 ARISE　border:3 Ghost Tears": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:3 Ghost Tears",
        2014,
    ),
    "攻殻機動隊 ARISE　border:4 Ghost Stands Alone": (
        "攻殻機動隊ARISE -GHOST IN THE SHELL- border:4 Ghost Stands Alone",
        2014,
    ),
}


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Return a SQLite connection."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Check if new BEFORE connecting
    is_new_db = not db_path.exists()
    # Regular isolation mode for normal operations (init_db will set autocommit temporarily)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Only set WAL if DB already existed
    if not is_new_db:
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_connection(
    db_path: Path | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for a SQLite connection.

    Auto-commits on success, rolls back on exception, always closes.

    Usage::

        with db_connection() as conn:
            conn.execute("INSERT ...")
            # auto-commit on success, rollback on exception
    """
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables (delegates to init_db_v2 target schema)."""
    from src.database_v2 import init_db_v2
    init_db_v2(conn)
def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the current schema version."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except sqlite3.OperationalError:
        return 0


def upsert_feat_person_scores(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_person_scores.

    Args:
        conn: SQLite connection
        rows: list of dicts in the same format as scores.json entries.
              Required key: person_id. Missing fields default to None.
        run_id: pipeline_runs.id (optional)
    """

    def _pct(d: dict, key: str) -> float | None:
        return d.get(key)

    def _conf(d: dict) -> tuple:
        sr = d.get("score_range") or {}
        return (
            d.get("confidence"),
            sr.get("low") if isinstance(sr, dict) else None,
            sr.get("high") if isinstance(sr, dict) else None,
        )

    batch = []
    for d in rows:
        conf, sr_low, sr_high = _conf(d)
        batch.append(
            (
                d["person_id"],
                run_id,
                d.get("person_fe"),
                d.get("person_fe_se"),
                d.get("person_fe_n_obs"),
                d.get("studio_fe_exposure"),
                d.get("birank"),
                d.get("patronage"),
                d.get("awcc"),
                d.get("dormancy"),
                d.get("ndi"),
                d.get("career_friction"),
                d.get("peer_boost"),
                d.get("iv_score"),
                _pct(d, "iv_score_pct"),
                _pct(d, "person_fe_pct"),
                _pct(d, "birank_pct"),
                _pct(d, "patronage_pct"),
                _pct(d, "awcc_pct"),
                _pct(d, "dormancy_pct"),
                conf,
                sr_low,
                sr_high,
            )
        )
    conn.executemany(
        """
        INSERT INTO feat_person_scores (
            person_id, run_id,
            person_fe, person_fe_se, person_fe_n_obs, studio_fe_exposure,
            birank, patronage, awcc,
            dormancy, ndi, career_friction, peer_boost,
            iv_score,
            iv_score_pct, person_fe_pct, birank_pct, patronage_pct,
            awcc_pct, dormancy_pct,
            confidence, score_range_low, score_range_high,
            updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            run_id=excluded.run_id,
            person_fe=excluded.person_fe, person_fe_se=excluded.person_fe_se,
            person_fe_n_obs=excluded.person_fe_n_obs,
            studio_fe_exposure=excluded.studio_fe_exposure,
            birank=excluded.birank, patronage=excluded.patronage, awcc=excluded.awcc,
            dormancy=excluded.dormancy, ndi=excluded.ndi,
            career_friction=excluded.career_friction, peer_boost=excluded.peer_boost,
            iv_score=excluded.iv_score,
            iv_score_pct=excluded.iv_score_pct, person_fe_pct=excluded.person_fe_pct,
            birank_pct=excluded.birank_pct, patronage_pct=excluded.patronage_pct,
            awcc_pct=excluded.awcc_pct, dormancy_pct=excluded.dormancy_pct,
            confidence=excluded.confidence,
            score_range_low=excluded.score_range_low,
            score_range_high=excluded.score_range_high,
            updated_at=CURRENT_TIMESTAMP
    """,
        batch,
    )
    logger.info("feat_person_scores_upserted", count=len(batch))


def upsert_feat_network(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_network.

    Each dict must contain person_id plus centrality/hub_score/bridge info.
    Built from the centrality sub-dict in scores.json and from bridges.json.
    """
    batch = []
    for d in rows:
        c = d.get("centrality") or {}
        batch.append(
            (
                d["person_id"],
                run_id,
                c.get("degree"),
                c.get("betweenness"),
                c.get("closeness"),
                c.get("eigenvector"),
                (d.get("network") or {}).get("hub_score"),
                (d.get("network") or {}).get("collaborators"),
                (d.get("network") or {}).get("unique_anime"),
                d.get("bridge_score"),
                d.get("n_bridge_communities"),
            )
        )
    conn.executemany(
        """
        INSERT INTO feat_network (
            person_id, run_id,
            degree_centrality, betweenness_centrality,
            closeness_centrality, eigenvector_centrality,
            hub_score, n_collaborators, n_unique_anime,
            bridge_score, n_bridge_communities,
            updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            run_id=excluded.run_id,
            degree_centrality=excluded.degree_centrality,
            betweenness_centrality=excluded.betweenness_centrality,
            closeness_centrality=excluded.closeness_centrality,
            eigenvector_centrality=excluded.eigenvector_centrality,
            hub_score=excluded.hub_score,
            n_collaborators=excluded.n_collaborators,
            n_unique_anime=excluded.n_unique_anime,
            bridge_score=excluded.bridge_score,
            n_bridge_communities=excluded.n_bridge_communities,
            updated_at=CURRENT_TIMESTAMP
    """,
        batch,
    )
    logger.info("feat_network_upserted", count=len(batch))


def upsert_feat_career(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_career.

    Built from the career/growth sub-dicts in scores.json and from growth.json.
    """
    batch = []
    for d in rows:
        car = d.get("career") or {}
        grw = d.get("growth") or {}
        batch.append(
            (
                d["person_id"],
                run_id,
                car.get("first_year"),
                car.get("latest_year"),
                car.get("active_years"),
                d.get("total_credits"),
                car.get("highest_stage"),
                d.get("primary_role"),
                d.get("career_track"),
                car.get("peak_year"),
                car.get("peak_credits"),
                grw.get("trend"),
                d.get("growth_score"),
                grw.get("activity_ratio"),
                grw.get("recent_credits"),
            )
        )
    conn.executemany(
        """
        INSERT INTO feat_career (
            person_id, run_id,
            first_year, latest_year, active_years, total_credits,
            highest_stage, primary_role, career_track,
            peak_year, peak_credits,
            growth_trend, growth_score, activity_ratio, recent_credits,
            updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            run_id=excluded.run_id,
            first_year=excluded.first_year, latest_year=excluded.latest_year,
            active_years=excluded.active_years, total_credits=excluded.total_credits,
            highest_stage=excluded.highest_stage, primary_role=excluded.primary_role,
            career_track=excluded.career_track,
            peak_year=excluded.peak_year, peak_credits=excluded.peak_credits,
            growth_trend=excluded.growth_trend, growth_score=excluded.growth_score,
            activity_ratio=excluded.activity_ratio, recent_credits=excluded.recent_credits,
            updated_at=CURRENT_TIMESTAMP
    """,
        batch,
    )
    logger.info("feat_career_upserted", count=len(batch))


def upsert_feat_genre_affinity(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_genre_affinity.

    Args:
        rows: list of dicts with keys {"person_id", "genre", "affinity_score", "work_count"}
    """
    batch = [
        (
            d["person_id"],
            d["genre"],
            run_id,
            d.get("affinity_score"),
            d.get("work_count"),
        )
        for d in rows
    ]
    conn.executemany(
        """
        INSERT INTO feat_genre_affinity (person_id, genre, run_id, affinity_score, work_count)
        VALUES (?,?,?,?,?)
        ON CONFLICT(person_id, genre) DO UPDATE SET
            run_id=excluded.run_id,
            affinity_score=excluded.affinity_score,
            work_count=excluded.work_count
    """,
        batch,
    )
    logger.info("feat_genre_affinity_upserted", count=len(batch))


def upsert_feat_contribution(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_contribution.

    Built from entries in individual_profiles.json.
    """
    batch = [
        (
            d["person_id"],
            run_id,
            d.get("peer_percentile"),
            d.get("opportunity_residual"),
            d.get("consistency"),
            d.get("independent_value"),
        )
        for d in rows
    ]
    conn.executemany(
        """
        INSERT INTO feat_contribution (
            person_id, run_id,
            peer_percentile, opportunity_residual, consistency, independent_value,
            updated_at
        ) VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            run_id=excluded.run_id,
            peer_percentile=excluded.peer_percentile,
            opportunity_residual=excluded.opportunity_residual,
            consistency=excluded.consistency,
            independent_value=excluded.independent_value,
            updated_at=CURRENT_TIMESTAMP
    """,
        batch,
    )
    logger.info("feat_contribution_upserted", count=len(batch))


def upsert_agg_milestones(
    conn: sqlite3.Connection,
    milestones_dict: dict,
    run_id: int | None = None,
) -> None:
    """Bulk upsert into agg_milestones (L2: career events).

    Args:
        milestones_dict: {person_id: [{type, year, anime_id, anime_title, description}]}
    """
    batch = []
    for person_id, events in milestones_dict.items():
        if not isinstance(events, list):
            continue
        for ev in events:
            if not isinstance(ev, dict):
                continue
            batch.append(
                (
                    person_id,
                    ev.get("type") or "",
                    int(ev.get("year") or 0),
                    ev.get("anime_id") or "",
                    ev.get("anime_title"),
                    ev.get("description"),
                )
            )
    if not batch:
        return
    conn.executemany(
        """
        INSERT INTO agg_milestones (person_id, event_type, year, anime_id, anime_title, description)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(person_id, event_type, year, anime_id) DO UPDATE SET
            anime_title=excluded.anime_title,
            description=excluded.description
    """,
        batch,
    )
    logger.info("agg_milestones_upserted", count=len(batch))


def upsert_agg_director_circles(
    conn: sqlite3.Connection,
    circles_dict: dict,
    run_id: int | None = None,
) -> None:
    """Bulk upsert into agg_director_circles (L2: co-credit aggregation).

    Args:
        circles_dict: {director_id: obj} where obj is a DirectorCircle dataclass
                      or a dict with {members: [{person_id, shared_works, hit_rate, roles, latest_year}]}.
    """
    import dataclasses
    import json as _json

    batch = []
    for director_id, circle in circles_dict.items():
        # convert dataclass to dict if needed
        if dataclasses.is_dataclass(circle) and not isinstance(circle, type):
            circle = dataclasses.asdict(circle)
        if not isinstance(circle, dict):
            continue
        for member in circle.get("members", []):
            if dataclasses.is_dataclass(member) and not isinstance(member, type):
                member = dataclasses.asdict(member)
            if not isinstance(member, dict):
                continue
            pid = member.get("person_id")
            if not pid:
                continue
            roles = member.get("roles") or []
            batch.append(
                (
                    pid,
                    director_id,
                    member.get("shared_works") or 0,
                    member.get("hit_rate"),
                    _json.dumps(roles, ensure_ascii=False)
                    if isinstance(roles, list)
                    else roles,
                    member.get("latest_year"),
                )
            )
    if not batch:
        return
    conn.executemany(
        """
        INSERT INTO agg_director_circles
            (person_id, director_id, shared_works, hit_rate, roles, latest_year)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(person_id, director_id) DO UPDATE SET
            shared_works=excluded.shared_works,
            hit_rate=excluded.hit_rate,
            roles=excluded.roles,
            latest_year=excluded.latest_year
    """,
        batch,
    )
    logger.info("agg_director_circles_upserted", count=len(batch))


def upsert_feat_mentorships(
    conn: sqlite3.Connection,
    mentorships_list: list,
    run_id: int | None = None,
) -> None:
    """Bulk upsert into feat_mentorships (L3: algorithmic mentor estimation).

    Args:
        mentorships_list: [{mentor_id, mentee_id, n_shared_works, hit_rate,
                            mentor_stage, mentee_stage, first_year, latest_year}]
    """
    batch = [
        (
            m["mentor_id"],
            m["mentee_id"],
            m.get("n_shared_works") or 0,
            m.get("hit_rate"),
            m.get("mentor_stage"),
            m.get("mentee_stage"),
            m.get("first_year"),
            m.get("latest_year"),
        )
        for m in mentorships_list
        if isinstance(m, dict) and m.get("mentor_id") and m.get("mentee_id")
    ]
    if not batch:
        return
    conn.executemany(
        """
        INSERT INTO feat_mentorships
            (mentor_id, mentee_id, n_shared_works, hit_rate,
             mentor_stage, mentee_stage, first_year, latest_year)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(mentor_id, mentee_id) DO UPDATE SET
            n_shared_works=excluded.n_shared_works,
            hit_rate=excluded.hit_rate,
            mentor_stage=excluded.mentor_stage,
            mentee_stage=excluded.mentee_stage,
            first_year=excluded.first_year,
            latest_year=excluded.latest_year
    """,
        batch,
    )
    logger.info("feat_mentorships_upserted", count=len(batch))


def load_feat_person_scores(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return feat_person_scores as person_id → dict mapping."""
    rows = conn.execute("SELECT * FROM feat_person_scores").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def load_feat_network(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return feat_network as person_id → dict mapping."""
    rows = conn.execute("SELECT * FROM feat_network").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def load_feat_career(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return feat_career as person_id → dict mapping."""
    rows = conn.execute("SELECT * FROM feat_career").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def _insert_career_annual_batch(conn: sqlite3.Connection, batch: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO feat_career_annual (
            person_id, career_year, credit_year,
            n_works, n_credits, n_roles,
            works_direction, works_animation_supervision, works_animation,
            works_design, works_technical, works_art, works_sound, works_writing,
            works_production, works_production_management, works_finishing,
            works_editing, works_settings, works_other
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(person_id, career_year) DO UPDATE SET
            credit_year=excluded.credit_year,
            n_works=excluded.n_works, n_credits=excluded.n_credits,
            n_roles=excluded.n_roles,
            works_direction=excluded.works_direction,
            works_animation_supervision=excluded.works_animation_supervision,
            works_animation=excluded.works_animation,
            works_design=excluded.works_design,
            works_technical=excluded.works_technical,
            works_art=excluded.works_art,
            works_sound=excluded.works_sound,
            works_writing=excluded.works_writing,
            works_production=excluded.works_production,
            works_production_management=excluded.works_production_management,
            works_finishing=excluded.works_finishing,
            works_editing=excluded.works_editing,
            works_settings=excluded.works_settings,
            works_other=excluded.works_other
        """,
        batch,
    )


def compute_feat_career_annual(
    conn: sqlite3.Connection,
    batch_size: int = 2000,
) -> int:
    """Aggregate work/credit counts by person × career year × role category into feat_career_annual.

    career_year = credit_year - first_credit_year  (0 = debut year)

    Role categories follow ROLE_CATEGORY in src/utils/role_groups.py (14 categories).
    Unknown roles are counted under works_other.

    Args:
        conn: SQLite connection
        batch_size: batch size for bulk INSERT (per person)

    Returns:
        number of rows written
    """
    from src.utils.role_groups import ROLE_CATEGORY

    # role → column name mapping
    CAT_COL = {
        "direction": "works_direction",
        "animation_supervision": "works_animation_supervision",
        "animation": "works_animation",
        "design": "works_design",
        "technical": "works_technical",
        "art": "works_art",
        "sound": "works_sound",
        "writing": "works_writing",
        "production": "works_production",
        "production_management": "works_production_management",
        "finishing": "works_finishing",
        "editing": "works_editing",
        "settings": "works_settings",
    }
    logger.info("feat_career_annual_compute_start")

    # fetch debut year per person (minimum credit_year)
    debut_sql = """
        SELECT person_id, MIN(credit_year) AS debut_year
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id
    """
    debut_year = {
        r["person_id"]: r["debut_year"] for r in conn.execute(debut_sql).fetchall()
    }

    # aggregate person × year × role (unique work count and credit count)
    agg_sql = """
        SELECT
            person_id,
            credit_year,
            role,
            COUNT(DISTINCT anime_id) AS n_works,
            COUNT(*) AS n_credits
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id, credit_year, role
        ORDER BY person_id, credit_year
    """

    # process by person_id
    current_pid: str | None = None
    current_years: dict[int, dict] = {}  # credit_year → row_accumulator

    def _make_row_acc() -> dict:
        return {
            "n_works": 0,
            "n_credits": 0,
            "roles": set(),
            **{col: 0 for col in CAT_COL.values()},
            "works_other": 0,
        }

    rows_to_insert: list[tuple] = []
    total_written = 0

    def _flush_person(pid: str, years: dict[int, dict]) -> None:
        nonlocal rows_to_insert, total_written
        debut = debut_year.get(pid)
        if debut is None:
            return
        for credit_year, acc in sorted(years.items()):
            career_y = credit_year - debut
            rows_to_insert.append(
                (
                    pid,
                    career_y,
                    credit_year,
                    acc["n_works"],
                    acc["n_credits"],
                    len(acc["roles"]),
                    acc["works_direction"],
                    acc["works_animation_supervision"],
                    acc["works_animation"],
                    acc["works_design"],
                    acc["works_technical"],
                    acc["works_art"],
                    acc["works_sound"],
                    acc["works_writing"],
                    acc["works_production"],
                    acc["works_production_management"],
                    acc["works_finishing"],
                    acc["works_editing"],
                    acc["works_settings"],
                    acc["works_other"],
                )
            )
        if len(rows_to_insert) >= batch_size * 10:
            _insert_career_annual_batch(conn, rows_to_insert)
            total_written += len(rows_to_insert)
            rows_to_insert = []

    for row in conn.execute(agg_sql):
        pid = row["person_id"]
        if pid != current_pid:
            if current_pid is not None:
                _flush_person(current_pid, current_years)
            current_pid = pid
            current_years = {}

        yr = row["credit_year"]
        if yr not in current_years:
            current_years[yr] = _make_row_acc()
        acc = current_years[yr]

        role = row["role"]
        cat = ROLE_CATEGORY.get(role, "other")
        col = CAT_COL.get(cat, "works_other")
        acc[col] += row["n_works"]
        acc["n_works"] += row["n_works"]
        acc["n_credits"] += row["n_credits"]
        acc["roles"].add(role)

    if current_pid is not None:
        _flush_person(current_pid, current_years)

    if rows_to_insert:
        _insert_career_annual_batch(conn, rows_to_insert)
        total_written += len(rows_to_insert)

    conn.commit()
    logger.info("feat_career_annual_computed", rows=total_written)
    return total_written
def load_feat_career_annual(
    conn: sqlite3.Connection,
    person_id: str | None = None,
) -> list[dict]:
    """Return feat_career_annual rows.

    Args:
        person_id: filter to a specific person (returns all if omitted)

    Returns:
        list of dicts with keys {person_id, career_year, credit_year, n_works, ...}
    """
    if person_id is not None:
        rows = conn.execute(
            "SELECT * FROM feat_career_annual WHERE person_id=? ORDER BY career_year",
            (person_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM feat_career_annual ORDER BY person_id, career_year"
        ).fetchall()
    return [dict(r) for r in rows]
def compute_feat_studio_affiliation(
    conn: sqlite3.Connection,
    batch_size: int = 10000,
) -> int:
    """Aggregate per-person × year × studio work participation into feat_studio_affiliation.

    Joins credits → anime_studios → studios to determine which studio's works
    each person participated in each year. Restricting to main studios (is_main=1)
    excludes production committees.

    Args:
        conn: SQLite connection
        batch_size: batch size for bulk INSERT

    Returns:
        number of rows written
    """
    logger.info("feat_studio_affiliation_compute_start")

    sql = """
        SELECT
            c.person_id,
            c.credit_year,
            ast.studio_id,
            COALESCE(s.name, '') AS studio_name,
            ast.is_main,
            COUNT(DISTINCT c.anime_id) AS n_works,
            COUNT(*) AS n_credits
        FROM credits c
        INNER JOIN anime_studios ast ON ast.anime_id = c.anime_id
        LEFT JOIN studios s ON s.id = ast.studio_id
        WHERE c.credit_year IS NOT NULL
        GROUP BY c.person_id, c.credit_year, ast.studio_id, ast.is_main
        ORDER BY c.person_id, c.credit_year
    """

    batch: list[tuple] = []
    total_written = 0

    for row in conn.execute(sql):
        batch.append(
            (
                row["person_id"],
                row["credit_year"],
                row["studio_id"],
                row["studio_name"],
                row["n_works"],
                row["n_credits"],
                row["is_main"],
            )
        )
        if len(batch) >= batch_size:
            conn.executemany(
                """
                INSERT INTO feat_studio_affiliation
                    (person_id, credit_year, studio_id, studio_name,
                     n_works, n_credits, is_main_studio)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(person_id, credit_year, studio_id) DO UPDATE SET
                    studio_name=excluded.studio_name,
                    n_works=excluded.n_works,
                    n_credits=excluded.n_credits,
                    is_main_studio=excluded.is_main_studio
            """,
                batch,
            )
            total_written += len(batch)
            batch = []

    if batch:
        conn.executemany(
            """
            INSERT INTO feat_studio_affiliation
                (person_id, credit_year, studio_id, studio_name,
                 n_works, n_credits, is_main_studio)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(person_id, credit_year, studio_id) DO UPDATE SET
                studio_name=excluded.studio_name,
                n_works=excluded.n_works,
                n_credits=excluded.n_credits,
                is_main_studio=excluded.is_main_studio
        """,
            batch,
        )
        total_written += len(batch)

    conn.commit()
    logger.info("feat_studio_affiliation_computed", rows=total_written)
    return total_written


def load_feat_studio_affiliation(
    conn: sqlite3.Connection,
    person_id: str | None = None,
    studio_id: str | None = None,
    main_only: bool = False,
) -> list[dict]:
    """Return feat_studio_affiliation rows.

    Args:
        person_id: filter to a specific person (returns all if omitted)
        studio_id: filter to a specific studio (returns all if omitted)
        main_only: if True, return only rows with is_main_studio=1
    """
    where_clauses = []
    params: list = []
    if person_id is not None:
        where_clauses.append("person_id = ?")
        params.append(person_id)
    if studio_id is not None:
        where_clauses.append("studio_id = ?")
        params.append(studio_id)
    if main_only:
        where_clauses.append("is_main_studio = 1")
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    rows = conn.execute(
        f"SELECT * FROM feat_studio_affiliation {where} ORDER BY credit_year",
        params,
    ).fetchall()
    return [dict(r) for r in rows]
def load_feat_credit_contribution(
    conn: sqlite3.Connection,
    person_id: str | None = None,
    anime_id: str | None = None,
    min_edge_weight: float | None = None,
) -> list[dict]:
    """Return feat_credit_contribution rows.

    Args:
        person_id: filter to a specific person
        anime_id: filter to a specific anime
        min_edge_weight: return only rows with edge_weight >= this value
    """
    where: list[str] = []
    params: list = []
    if person_id is not None:
        where.append("person_id = ?")
        params.append(person_id)
    if anime_id is not None:
        where.append("anime_id = ?")
        params.append(anime_id)
    if min_edge_weight is not None:
        where.append("edge_weight >= ?")
        params.append(min_edge_weight)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT * FROM feat_credit_contribution {w} ORDER BY iv_contrib_est DESC NULLS LAST",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def load_feat_person_work_summary(
    conn: sqlite3.Connection,
    person_id: str | None = None,
) -> dict | list[dict]:
    """Return feat_person_work_summary.

    Args:
        person_id: specific person → returns dict. If omitted → returns list[dict].
    """
    if person_id is not None:
        row = conn.execute(
            "SELECT * FROM feat_person_work_summary WHERE person_id = ?", (person_id,)
        ).fetchone()
        return dict(row) if row else {}
    rows = conn.execute("SELECT * FROM feat_person_work_summary").fetchall()
    return [dict(r) for r in rows]


def upsert_career_tracks(
    conn: sqlite3.Connection,
    career_tracks: dict[str, str],
) -> None:
    """Bulk-write person_id → career_track mapping to the scores table.

    Only updates rows that already exist in scores (no INSERT).
    Persons not yet scored (e.g. immediately after a fresh scrape) are skipped.

    Args:
        conn: SQLite connection
        career_tracks: dict of person_id → career_track
    """
    rows = [(track, pid) for pid, track in career_tracks.items()]
    conn.executemany(
        "UPDATE person_scores SET career_track = ?, updated_at = CURRENT_TIMESTAMP WHERE person_id = ?",
        rows,
    )
    logger.info("career_tracks_upserted", count=len(rows))


def insert_person_affiliation(
    conn: sqlite3.Connection,
    person_id: str,
    anime_id: str,
    studio_name: str,
    source: str = "",
) -> None:
    """Record a person's affiliated studio for a work (ignore duplicates)."""
    conn.execute(
        """INSERT OR IGNORE INTO person_affiliations
           (person_id, anime_id, studio_name, source)
           VALUES (?, ?, ?, ?)""",
        (person_id, anime_id, studio_name, source),
    )


def mark_person_unfetchable(
    conn: sqlite3.Connection,
    anilist_id: int,
    status: str = "not_found",
    source: str = "anilist",
) -> None:
    """Record an AniList person ID that could not be fetched via API."""
    conn.execute(
        """INSERT INTO person_fetch_status (anilist_id, status, source, updated_at)
           VALUES (?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(anilist_id) DO UPDATE SET
               status = excluded.status,
               updated_at = CURRENT_TIMESTAMP""",
        (anilist_id, status, source),
    )


def get_unfetchable_person_ids(
    conn: sqlite3.Connection, source: str = "anilist"
) -> set[int]:
    """Return the set of anilist_ids recorded as unfetchable."""
    rows = conn.execute(
        "SELECT anilist_id FROM person_fetch_status WHERE source = ?",
        (source,),
    ).fetchall()
    return {row[0] for row in rows}


# Source priority for canonical name selection.
# Higher value = more authoritative for primary name fields.
_SOURCE_PRIORITY: dict[str, int] = {
    "anilist": 3,
    "mal": 2,
    "seesaawiki": 2,
    "mediaarts": 2,
    "ann": 1,
    "jvmg": 1,
    "keyframe": 1,
    "allcinema": 1,
}

# CJK name fields where historical variants should be preserved in aliases.
_CJK_NAME_FIELDS: tuple[str, ...] = ("name_ja", "name_ko", "name_zh")


def upsert_person(
    conn: sqlite3.Connection,
    person: Person,
    source: str = "",
) -> None:
    """Insert or update a person with source-aware primary name selection.

    Primary name determination:
    1. Higher-priority source wins (anilist=3 > mal/seesaawiki=2 > ann/others=1).
    2. When a CJK name field changes, the displaced value is added to aliases
       so no name history is lost.
    3. Non-name fields (bio, dates, social) always use COALESCE (first non-null wins).

    Call normalize_primary_names_by_credits() after full ETL + entity resolution
    to re-rank primary names by credit count (most-used name = primary).
    """
    import json

    incoming_priority = _SOURCE_PRIORITY.get(source, 0)

    existing = conn.execute(
        "SELECT name_ja, name_ko, name_zh, aliases, name_priority FROM persons WHERE id = ?",
        (person.id,),
    ).fetchone()

    if existing is None:
        # New record — straightforward insert
        conn.execute(
            """INSERT OR IGNORE INTO persons (
                   id, name_ja, name_en, name_ko, name_zh, aliases, nationality,
                   mal_id, anilist_id,
                   date_of_birth, hometown, blood_type, description, years_active, favourites, site_url,
                   name_priority
               )
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                person.id,
                person.name_ja,
                person.name_en,
                person.name_ko,
                person.name_zh,
                json.dumps(person.aliases, ensure_ascii=False),
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id,
                person.anilist_id,
                person.date_of_birth,
                person.hometown,
                person.blood_type,
                person.description,
                json.dumps(getattr(person, "years_active", []) or [], ensure_ascii=False),
                person.favourites,
                person.site_url,
                incoming_priority,
            ),
        )
        return

    # Existing record — check name field conflicts and preserve history in aliases.
    existing_priority: int = existing["name_priority"] or 0
    old_aliases: list[str] = json.loads(existing["aliases"] or "[]")

    # Decide whether incoming source can update primary name fields.
    update_primary = incoming_priority >= existing_priority

    # Collect the loser's CJK names into aliases so no name history is lost.
    # When incoming wins: existing names are displaced. When existing wins: incoming names are rejected.
    for field in _CJK_NAME_FIELDS:
        old_val: str = existing[field] or ""
        new_val: str = getattr(person, field, "") or ""
        if old_val and new_val and old_val != new_val:
            loser = old_val if update_primary else new_val
            if loser not in old_aliases:
                old_aliases.append(loser)

    # Also absorb any aliases the incoming record carries.
    for alias in person.aliases:
        if alias and alias not in old_aliases:
            old_aliases.append(alias)

    final_aliases = json.dumps(old_aliases, ensure_ascii=False)

    incoming_years_active = json.dumps(getattr(person, "years_active", []) or [], ensure_ascii=False)

    if update_primary:
        conn.execute(
            """UPDATE persons SET
                   name_ja   = COALESCE(NULLIF(?, ''), name_ja),
                   name_en   = COALESCE(NULLIF(?, ''), name_en),
                   name_ko   = COALESCE(NULLIF(?, ''), name_ko),
                   name_zh   = COALESCE(NULLIF(?, ''), name_zh),
                   aliases   = ?,
                   nationality = COALESCE(NULLIF(?, '[]'), nationality),
                   mal_id    = COALESCE(?, mal_id),
                   anilist_id = COALESCE(?, anilist_id),
                   date_of_birth = COALESCE(?, date_of_birth),
                   hometown  = COALESCE(?, hometown),
                   blood_type = COALESCE(?, blood_type),
                   description = COALESCE(?, description),
                   years_active = CASE WHEN ? != '[]' THEN ? ELSE years_active END,
                   favourites = COALESCE(?, favourites),
                   site_url  = COALESCE(?, site_url),
                   name_priority = ?
               WHERE id = ?""",
            (
                person.name_ja, person.name_en, person.name_ko, person.name_zh,
                final_aliases,
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id, person.anilist_id,
                person.date_of_birth, person.hometown, person.blood_type,
                person.description,
                incoming_years_active, incoming_years_active,
                person.favourites, person.site_url,
                max(incoming_priority, existing_priority),
                person.id,
            ),
        )
    else:
        # Lower-priority source: skip primary name fields, update metadata + aliases only.
        conn.execute(
            """UPDATE persons SET
                   aliases   = ?,
                   nationality = COALESCE(NULLIF(?, '[]'), nationality),
                   mal_id    = COALESCE(?, mal_id),
                   anilist_id = COALESCE(?, anilist_id),
                   date_of_birth = COALESCE(?, date_of_birth),
                   hometown  = COALESCE(?, hometown),
                   blood_type = COALESCE(?, blood_type),
                   description = COALESCE(?, description),
                   years_active = CASE WHEN ? != '[]' THEN ? ELSE years_active END,
                   favourites = COALESCE(?, favourites),
                   site_url  = COALESCE(?, site_url)
               WHERE id = ?""",
            (
                final_aliases,
                json.dumps(person.nationality, ensure_ascii=False),
                person.mal_id, person.anilist_id,
                person.date_of_birth, person.hometown, person.blood_type,
                person.description,
                incoming_years_active, incoming_years_active,
                person.favourites, person.site_url,
                person.id,
            ),
        )


def normalize_primary_names_by_credits(conn: sqlite3.Connection) -> int:
    """Post-ETL: re-rank primary names by credit count (most-used name = primary).

    Run AFTER integrate_* functions and entity resolution are complete.

    For each person, finds which source contributed the most credits.
    If that source's name differs from the current primary, swaps:
      old primary → aliases, source name → primary.

    Returns the number of persons whose primary name was updated.
    """
    import json

    # Count credits per (person_id, evidence_source)
    credit_counts: dict[tuple[str, str], int] = {}
    for row in conn.execute(
        "SELECT person_id, evidence_source, COUNT(*) AS n FROM credits "
        "WHERE evidence_source != '' GROUP BY person_id, evidence_source"
    ):
        credit_counts[(row["person_id"], row["evidence_source"])] = row["n"]

    # Source → bronze name table mapping
    _BRONZE_NAME_QUERY: dict[str, str] = {
        "anilist": "SELECT name_ja, name_en FROM src_anilist_persons "
                   "WHERE anilist_id = CAST(? AS INT)",
        "ann":     "SELECT name_ja, name_en FROM src_ann_persons "
                   "WHERE ann_id = CAST(? AS INT)",
        "mal":     "SELECT name_ja, name_en FROM src_mal_persons "
                   "WHERE mal_id = CAST(? AS INT)",
    }

    # Build (person_id → best source) mapping by credit count
    best: dict[str, tuple[str, int]] = {}  # person_id → (source, n_credits)
    for (pid, src), n in credit_counts.items():
        cur_src, cur_n = best.get(pid, ("", 0))
        if n > cur_n or (n == cur_n and _SOURCE_PRIORITY.get(src, 0) > _SOURCE_PRIORITY.get(cur_src, 0)):
            best[pid] = (src, n)

    updated = 0
    for pid, (top_src, _) in best.items():
        query = _BRONZE_NAME_QUERY.get(top_src)
        if not query:
            continue  # source not in our name-lookup table

        # Look up external ID for this person/source
        ext_row = conn.execute(
            "SELECT external_id FROM person_external_ids WHERE person_id = ? AND source = ?",
            (pid, top_src),
        ).fetchone()
        if not ext_row:
            continue

        bronze_row = conn.execute(query, (ext_row["external_id"],)).fetchone()
        if not bronze_row:
            continue

        top_name_ja = bronze_row["name_ja"] or ""
        top_name_en = bronze_row["name_en"] or ""

        current = conn.execute(
            "SELECT name_ja, name_en, aliases FROM persons WHERE id = ?", (pid,)
        ).fetchone()
        if not current:
            continue

        cur_ja = current["name_ja"] or ""
        cur_en = current["name_en"] or ""
        if top_name_ja == cur_ja and top_name_en == cur_en:
            continue  # already correct

        old_aliases: list[str] = json.loads(current["aliases"] or "[]")
        for old_name in (cur_ja, cur_en):
            if old_name and old_name not in old_aliases:
                old_aliases.append(old_name)

        conn.execute(
            """UPDATE persons SET
                   name_ja = COALESCE(NULLIF(?, ''), name_ja),
                   name_en = COALESCE(NULLIF(?, ''), name_en),
                   aliases = ?
               WHERE id = ?""",
            (
                top_name_ja, top_name_en,
                json.dumps(old_aliases, ensure_ascii=False),
                pid,
            ),
        )
        updated += 1

    logger.info("normalize_primary_names_done", updated=updated)
    return updated


def upsert_anime(conn: sqlite3.Connection, anime: Anime) -> None:
    """Insert or update an anime (canonical silver: structural columns only)."""
    import json as _json

    conn.execute(
        """INSERT INTO anime (
               id, title_ja, title_en, year, season, episodes, format, status,
               start_date, end_date, duration, original_work_type, quarter, work_type, scale_class,
               country_of_origin, synonyms, is_adult
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
                title_ja = COALESCE(NULLIF(excluded.title_ja, ''), anime.title_ja),
                title_en = COALESCE(NULLIF(excluded.title_en, ''), anime.title_en),
                year = COALESCE(excluded.year, anime.year),
                season = COALESCE(excluded.season, anime.season),
                episodes = COALESCE(excluded.episodes, anime.episodes),
                format = COALESCE(excluded.format, anime.format),
                status = COALESCE(excluded.status, anime.status),
                start_date = COALESCE(excluded.start_date, anime.start_date),
                end_date = COALESCE(excluded.end_date, anime.end_date),
                duration = COALESCE(excluded.duration, anime.duration),
                original_work_type = COALESCE(excluded.original_work_type, anime.original_work_type),
                quarter = COALESCE(excluded.quarter, anime.quarter),
                work_type = COALESCE(excluded.work_type, anime.work_type),
                scale_class = COALESCE(excluded.scale_class, anime.scale_class),
                country_of_origin = COALESCE(excluded.country_of_origin, anime.country_of_origin),
                synonyms = CASE WHEN excluded.synonyms != '[]' THEN excluded.synonyms ELSE anime.synonyms END,
                is_adult = COALESCE(excluded.is_adult, anime.is_adult),
                updated_at = CURRENT_TIMESTAMP
        """,
        (
            anime.id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            getattr(anime, "original_work_type", None) or getattr(anime, "source", None),
            anime.quarter,
            anime.work_type,
            anime.scale_class,
            getattr(anime, "country_of_origin", None),
            _json.dumps(getattr(anime, "synonyms", []) or [], ensure_ascii=False),
            1 if getattr(anime, "is_adult", None) else (0 if getattr(anime, "is_adult", None) is not None else None),
        ),
    )

    upsert_anime_analysis(
        conn,
        {
            "id": anime.id,
            "title_ja": anime.title_ja,
            "title_en": anime.title_en,
            "year": anime.year,
            "season": anime.season,
            "quarter": anime.quarter,
            "episodes": anime.episodes,
            "format": anime.format,
            "duration": anime.duration,
            "start_date": anime.start_date,
            "end_date": anime.end_date,
            "status": anime.status,
            "source": anime.source,
            "work_type": anime.work_type,
            "scale_class": anime.scale_class,
            "mal_id": anime.mal_id,
            "anilist_id": anime.anilist_id,
            "ann_id": getattr(anime, "ann_id", None),
            "allcinema_id": getattr(anime, "allcinema_id", None),
            "madb_id": getattr(anime, "madb_id", None),
        },
    )

    # Write studios to normalized tables (replaces anime_display.studios JSON denorm).
    for i, studio_name in enumerate(anime.studios or []):
        studio_id = studio_name.lower().replace(" ", "_")
        conn.execute(
            "INSERT OR IGNORE INTO studios (id, name) VALUES (?, ?)",
            (studio_id, studio_name),
        )
        conn.execute(
            "INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main) VALUES (?, ?, ?)",
            (anime.id, studio_id, 1 if i == 0 else 0),
        )


    # Keep external identifiers in normalized table (anime_external_ids).
    for source, external_id in (
        ("mal", anime.mal_id),
        ("anilist", anime.anilist_id),
        ("ann", getattr(anime, "ann_id", None)),
        ("allcinema", getattr(anime, "allcinema_id", None)),
        ("madb", getattr(anime, "madb_id", None)),
    ):
        if external_id is None:
            continue
        ext = str(external_id).strip()
        if not ext:
            continue
        conn.execute(
            """
            INSERT INTO anime_external_ids (anime_id, source, external_id)
            VALUES (?, ?, ?)
            ON CONFLICT(anime_id, source) DO UPDATE SET
                external_id = excluded.external_id
            """,
            (anime.id, source, ext),
        )


_ANIME_ANALYSIS_COLUMNS = (
    "id",
    "title_ja",
    "title_en",
    "year",
    "season",
    "quarter",
    "episodes",
    "format",
    "duration",
    "start_date",
    "end_date",
    "status",
    "source",
    "work_type",
    "scale_class",
    "mal_id",
    "anilist_id",
    "ann_id",
    "allcinema_id",
    "madb_id",
)


def upsert_anime_analysis(conn: sqlite3.Connection, row: dict) -> None:
    """No-op: anime_analysis table removed in target schema (v2)."""
    return


def ensure_meta_quality_snapshot(conn: sqlite3.Connection) -> None:
    """Create meta_quality_snapshot table/index if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta_quality_snapshot (
            computed_at TEXT NOT NULL,
            table_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (computed_at, table_name, metric)
        );
        CREATE INDEX IF NOT EXISTS idx_quality_snapshot_metric
            ON meta_quality_snapshot(table_name, metric, computed_at);
        """
    )


def ensure_calc_execution_records(conn: sqlite3.Connection) -> None:
    """Create calc_execution_records table/index if missing."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS calc_execution_records (
            scope TEXT NOT NULL,
            calc_name TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'success',
            output_path TEXT NOT NULL DEFAULT '',
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (scope, calc_name)
        );
        CREATE INDEX IF NOT EXISTS idx_calc_exec_scope_hash
            ON calc_execution_records(scope, input_hash);
        """
    )


def get_calc_execution_hashes(
    conn: sqlite3.Connection,
    scope: str,
) -> dict[str, str]:
    """Get latest input_hash by calc_name for a scope."""
    ensure_calc_execution_records(conn)
    rows = conn.execute(
        "SELECT calc_name, input_hash FROM calc_execution_records WHERE scope = ?",
        (scope,),
    ).fetchall()
    return {row["calc_name"]: row["input_hash"] for row in rows}


def record_calc_execution(
    conn: sqlite3.Connection,
    scope: str,
    calc_name: str,
    input_hash: str,
    *,
    status: str = "success",
    output_path: str = "",
) -> None:
    """Upsert a calc execution record."""
    ensure_calc_execution_records(conn)
    conn.execute(
        """
        INSERT INTO calc_execution_records
            (scope, calc_name, input_hash, status, output_path, computed_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(scope, calc_name) DO UPDATE SET
            input_hash = excluded.input_hash,
            status = excluded.status,
            output_path = excluded.output_path,
            computed_at = CURRENT_TIMESTAMP
        """,
        (scope, calc_name, input_hash, status, output_path),
    )


def register_meta_lineage(
    conn: sqlite3.Connection,
    table_name: str,
    audience: str,
    source_silver_tables: list[str],
    formula_version: str,
    *,
    source_bronze_forbidden: int = 1,
    source_display_allowed: int = 0,
    ci_method: str | None = None,
    null_model: str | None = None,
    holdout_method: str | None = None,
    description: str = "",
    row_count: int | None = None,
    rng_seed: int | None = None,
    git_sha: str | None = None,
    inputs_hash: str | None = None,
    notes: str | None = None,
) -> None:
    """Register Gold table lineage information into the meta_lineage table."""
    import json as _json
    import hashlib as _hashlib
    import subprocess as _subprocess

    lineage_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(ops_lineage)").fetchall()
    }
    if row_count is None:
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        except sqlite3.OperationalError:
            row_count = None

    if git_sha is None:
        try:
            git_sha = _subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip()
        except Exception:
            git_sha = ""
    if inputs_hash is None:
        payload = {
            "table_name": table_name,
            "source_silver_tables": sorted(source_silver_tables),
            "formula_version": formula_version,
        }
        inputs_hash = _hashlib.sha256(
            _json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

    all_values = {
        "table_name": table_name,
        "audience": audience,
        "source_silver_tables": _json.dumps(source_silver_tables, ensure_ascii=False),
        "source_bronze_forbidden": source_bronze_forbidden,
        "source_display_allowed": source_display_allowed,
        "description": description,
        "formula_version": formula_version,
        "ci_method": ci_method,
        "null_model": null_model,
        "holdout_method": holdout_method,
        "row_count": row_count,
        "notes": notes,
        "rng_seed": rng_seed,
        "git_sha": git_sha or "",
        "inputs_hash": inputs_hash or "",
    }
    ordered_cols = [
        "table_name",
        "audience",
        "source_silver_tables",
        "source_bronze_forbidden",
        "source_display_allowed",
        "description",
        "formula_version",
        "computed_at",
        "ci_method",
        "null_model",
        "holdout_method",
        "row_count",
        "notes",
        "rng_seed",
        "git_sha",
        "inputs_hash",
    ]
    insert_cols: list[str] = []
    insert_values: list[Any] = []
    for col in ordered_cols:
        if col == "computed_at":
            continue
        if col in lineage_cols:
            insert_cols.append(col)
            insert_values.append(all_values[col])
    update_cols = [c for c in insert_cols if c != "table_name"]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    insert_cols_sql = ", ".join(insert_cols + ["computed_at"])
    placeholders = ", ".join(["?"] * len(insert_cols) + ["CURRENT_TIMESTAMP"])

    conn.execute(
        f"""INSERT INTO ops_lineage ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(table_name) DO UPDATE SET
                {update_clause},
                computed_at = CURRENT_TIMESTAMP""",
        insert_values,
    )


def upsert_meta_entity_resolution_audit(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert the entity-resolution audit table and update lineage."""
    if not rows:
        return 0

    cols = [
        "person_id",
        "canonical_name",
        "merge_method",
        "merge_confidence",
        "merged_from_keys",
        "merge_evidence",
        "reviewed_by",
        "reviewed_at",
    ]
    placeholders = ", ".join("?" for _ in cols)
    update_clause = ", ".join(
        f"{c}=excluded.{c}" for c in cols if c not in {"person_id", "reviewed_at"}
    )
    conn.executemany(
        f"""INSERT INTO ops_entity_resolution_audit ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT(person_id) DO UPDATE SET
                {update_clause},
                merged_at = CURRENT_TIMESTAMP""",
        [[r.get(c) for c in cols] for r in rows],
    )
    register_meta_lineage(
        conn,
        table_name="ops_entity_resolution_audit",
        audience="technical_appendix",
        source_silver_tables=["persons", "credits", "person_aliases"],
        formula_version="v2.0",
        description="Entity-resolution merge audit trail for legal verification.",
        ci_method="n/a",
        null_model="n/a",
        holdout_method="n/a",
        notes="Merge decisions logged without changing matching logic.",
    )
    return len(rows)


def insert_credit(conn: sqlite3.Connection, credit: Credit) -> None:
    """Insert a credit record (ignore duplicates)."""
    source = credit.evidence_source or credit.source
    raw_role = credit.raw_role or ""
    # SQLite UNIQUE treats NULL != NULL, so whole-series credits (episode=None)
    # need an explicit existence check to avoid duplicates.
    if credit.episode is None:
        exists = conn.execute(
            "SELECT 1 FROM credits WHERE person_id=? AND anime_id=? AND raw_role=? AND episode IS NULL",
            (credit.person_id, credit.anime_id, raw_role),
        ).fetchone()
        if exists:
            return
    conn.execute(
        """INSERT OR IGNORE INTO credits
           (person_id, anime_id, role, raw_role, episode, evidence_source, affiliation, position)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            credit.person_id,
            credit.anime_id,
            credit.role.value,
            raw_role,
            credit.episode,
            source,
            credit.affiliation,
            credit.position,
        ),
    )


def upsert_character(conn: sqlite3.Connection, character: Character) -> None:
    """Insert or update a character."""
    import json

    conn.execute(
        """INSERT INTO characters (
               id, name_ja, name_en, aliases, anilist_id,
               image_large, image_medium, description, gender,
               date_of_birth, age, blood_type, favourites, site_url
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), characters.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), characters.name_en),
               aliases = excluded.aliases,
               anilist_id = COALESCE(excluded.anilist_id, characters.anilist_id),
               image_large = COALESCE(excluded.image_large, characters.image_large),
               image_medium = COALESCE(excluded.image_medium, characters.image_medium),
               description = COALESCE(excluded.description, characters.description),
               gender = COALESCE(excluded.gender, characters.gender),
               date_of_birth = COALESCE(excluded.date_of_birth, characters.date_of_birth),
               age = COALESCE(excluded.age, characters.age),
               blood_type = COALESCE(excluded.blood_type, characters.blood_type),
               favourites = COALESCE(excluded.favourites, characters.favourites),
               site_url = COALESCE(excluded.site_url, characters.site_url)
        """,
        (
            character.id,
            character.name_ja,
            character.name_en,
            json.dumps(character.aliases, ensure_ascii=False),
            character.anilist_id,
            character.image_large,
            character.image_medium,
            character.description,
            character.gender,
            character.date_of_birth,
            character.age,
            character.blood_type,
            character.favourites,
            character.site_url,
        ),
    )


def insert_character_voice_actor(
    conn: sqlite3.Connection, cva: CharacterVoiceActor
) -> None:
    """Insert a character × voice actor × work relationship (ignore duplicates)."""
    conn.execute(
        """INSERT OR IGNORE INTO character_voice_actors
           (character_id, person_id, anime_id, character_role, source)
           VALUES (?, ?, ?, ?, ?)""",
        (cva.character_id, cva.person_id, cva.anime_id, cva.character_role, cva.source),
    )


def upsert_studio(conn: sqlite3.Connection, studio: Studio) -> None:
    """Insert or update a studio."""
    conn.execute(
        """INSERT INTO studios (id, name, anilist_id, is_animation_studio, country_of_origin, favourites, site_url)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name = COALESCE(NULLIF(excluded.name, ''), studios.name),
               anilist_id = COALESCE(excluded.anilist_id, studios.anilist_id),
               is_animation_studio = COALESCE(excluded.is_animation_studio, studios.is_animation_studio),
               country_of_origin = COALESCE(excluded.country_of_origin, studios.country_of_origin),
               favourites = COALESCE(excluded.favourites, studios.favourites),
               site_url = COALESCE(excluded.site_url, studios.site_url)
        """,
        (
            studio.id,
            studio.name,
            studio.anilist_id,
            1
            if studio.is_animation_studio
            else (0 if studio.is_animation_studio is not None else None),
            getattr(studio, "country_of_origin", None),
            studio.favourites,
            studio.site_url,
        ),
    )


def insert_anime_studio(conn: sqlite3.Connection, anime_studio: AnimeStudio) -> None:
    """Insert an anime × studio relationship (ignore duplicates)."""
    conn.execute(
        """INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main)
           VALUES (?, ?, ?)""",
        (
            anime_studio.anime_id,
            anime_studio.studio_id,
            1 if anime_studio.is_main else 0,
        ),
    )


def insert_anime_relation(conn: sqlite3.Connection, relation: AnimeRelation) -> None:
    """Insert an anime-to-anime relation (ignore duplicates)."""
    conn.execute(
        """INSERT OR IGNORE INTO anime_relations
           (anime_id, related_anime_id, relation_type, related_title, related_format)
           VALUES (?, ?, ?, ?, ?)""",
        (
            relation.anime_id,
            relation.related_anime_id,
            relation.relation_type,
            relation.related_title,
            relation.related_format,
        ),
    )


def upsert_score(conn: sqlite3.Connection, score: ScoreResult) -> None:
    """Insert or update a score record."""
    conn.execute(
        """INSERT INTO person_scores
               (person_id, person_fe, studio_fe_exposure, birank,
                patronage, dormancy, awcc, iv_score, career_track)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(person_id) DO UPDATE SET
               person_fe = excluded.person_fe,
               studio_fe_exposure = excluded.studio_fe_exposure,
               birank = excluded.birank,
               patronage = excluded.patronage,
               dormancy = excluded.dormancy,
               awcc = excluded.awcc,
               iv_score = excluded.iv_score,
               career_track = excluded.career_track,
               updated_at = CURRENT_TIMESTAMP
        """,
        (
            score.person_id,
            score.person_fe,
            score.studio_fe_exposure,
            score.birank,
            score.patronage,
            score.dormancy,
            score.awcc,
            score.iv_score,
            score.career_track,
        ),
    )


def load_all_persons(conn: sqlite3.Connection) -> list[Person]:
    """Load all persons from the database."""
    from src.db_rows import PersonRow

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [Person.from_db_row(PersonRow.from_row(row)) for row in rows]


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """Load all anime from the database."""
    from src.db_rows import AnimeRow

    rows = conn.execute("SELECT * FROM anime").fetchall()
    anime_list = [Anime.from_db_row(AnimeRow.from_row(row)) for row in rows]
    by_id = {a.id: a for a in anime_list}

    # Hydrate normalized external IDs into in-memory anime objects.
    for row in conn.execute(
        "SELECT anime_id, source, external_id FROM anime_external_ids"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is None:
            continue
        source = row["source"]
        external_id = row["external_id"]
        if source in {"mal", "anilist", "ann", "allcinema"}:
            try:
                setattr(anime, f"{source}_id", int(external_id))
            except (TypeError, ValueError):
                continue
        elif source == "madb":
            anime.madb_id = external_id

    # Hydrate genres/tags from normalized tables.
    for row in conn.execute(
        "SELECT anime_id, genre_name FROM anime_genres ORDER BY genre_name"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None:
            anime.genres.append(row["genre_name"])
    for row in conn.execute(
        "SELECT anime_id, tag_name, rank FROM anime_tags ORDER BY rank DESC, tag_name"
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None:
            anime.tags.append({"name": row["tag_name"], "rank": row["rank"]})

    # Hydrate studios from normalized relation table.
    for row in conn.execute(
        """
        SELECT ast.anime_id, s.name
        FROM anime_studios ast
        JOIN studios s ON s.id = ast.studio_id
        ORDER BY ast.is_main DESC, s.name
        """
    ).fetchall():
        anime = by_id.get(row["anime_id"])
        if anime is not None and row["name"]:
            anime.studios.append(row["name"])

    return anime_list


def load_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """Load all credits from the database."""
    from src.db_rows import CreditRow

    rows = conn.execute("SELECT * FROM credits").fetchall()
    credits: list[Credit] = []
    skipped = 0
    for row in rows:
        try:
            credits.append(Credit.from_db_row(CreditRow.from_row(row)))
        except ValueError:
            skipped += 1
    if skipped:
        logger.warning("credits_skipped_unknown_role", count=skipped)
    return credits


def get_db_stats(conn: sqlite3.Connection) -> dict[str, int | float]:
    """Return database statistics."""
    stats: dict[str, int | float] = {}

    for table in ("persons", "anime", "credits", "person_scores"):
        stats[f"{table}_count"] = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]  # noqa: S608

    # role distribution
    role_counts = conn.execute("""
        SELECT role, COUNT(*) as cnt FROM credits
        GROUP BY role ORDER BY cnt DESC
    """).fetchall()
    stats["distinct_roles"] = len(role_counts)

    # year coverage
    year_range = conn.execute("""
        SELECT MIN(year), MAX(year) FROM anime WHERE year IS NOT NULL
    """).fetchone()
    if year_range[0]:
        stats["year_min"] = year_range[0]
        stats["year_max"] = year_range[1]

    # credit count by source
    source_counts = conn.execute("""
        SELECT evidence_source AS source_code, COUNT(*) as cnt
        FROM credits
        WHERE evidence_source != ''
        GROUP BY source_code
        ORDER BY cnt DESC
    """).fetchall()
    for source, cnt in source_counts:
        stats[f"credits_source_{source or 'unknown'}"] = cnt

    # average credits per person
    avg = conn.execute("""
        SELECT AVG(cnt) FROM (
            SELECT COUNT(*) as cnt FROM credits GROUP BY person_id
        )
    """).fetchone()[0]
    if avg:
        stats["avg_credits_per_person"] = round(avg, 1)

    return stats


def search_persons(
    conn: sqlite3.Connection,
    query: str,
    limit: int = 20,
) -> list[dict]:
    """Search persons by name or ID.

    Args:
        conn: DB connection
        query: search string (partial match)
        limit: maximum number of results

    Returns:
        [{id, name_ja, name_en, name_ko, name_zh, iv_score, credit_count}]
    """
    pattern = f"%{query}%"
    rows = conn.execute(
        """SELECT p.id, p.name_ja, p.name_en, p.name_ko, p.name_zh,
                  s.iv_score, s.person_fe, s.birank, s.patronage,
                  (SELECT COUNT(*) FROM credits c WHERE c.person_id = p.id) as credit_count
           FROM persons p
           LEFT JOIN person_scores s ON p.id = s.person_id
           WHERE p.name_ja LIKE ?
              OR p.name_en LIKE ? COLLATE NOCASE
              OR p.name_ko LIKE ?
              OR p.name_zh LIKE ?
              OR p.aliases LIKE ? COLLATE NOCASE
              OR p.id LIKE ?
           ORDER BY s.iv_score DESC NULLS LAST
           LIMIT ?""",
        (pattern, pattern, pattern, pattern, pattern, pattern, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def update_data_source(
    conn: sqlite3.Connection,
    source: str,
    item_count: int,
    status: str = "ok",
) -> None:
    """Update the last-scraped timestamp for a data source."""
    conn.execute(
        """INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
               last_scraped_at = CURRENT_TIMESTAMP,
               item_count = excluded.item_count,
               status = excluded.status
        """,
        (source, item_count, status),
    )


def get_source_scrape_status(conn: sqlite3.Connection) -> list[dict]:
    """Return scrape sync state per source (last_scraped_at, item_count, status)."""
    rows = conn.execute(
        "SELECT source, last_scraped_at, item_count, status FROM ops_source_scrape_status ORDER BY source"
    ).fetchall()
    return [dict(r) for r in rows]


def get_data_sources(conn: sqlite3.Connection) -> list[dict]:
    """Alias for get_source_scrape_status."""
    return get_source_scrape_status(conn)


def load_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """Load all scores from the database."""
    rows = conn.execute("SELECT * FROM person_scores").fetchall()
    return [
        ScoreResult(
            person_id=row["person_id"],
            person_fe=row["person_fe"],
            studio_fe_exposure=row["studio_fe_exposure"],
            birank=row["birank"],
            patronage=row["patronage"],
            dormancy=row["dormancy"],
            awcc=row["awcc"],
            iv_score=row["iv_score"],
            career_track=row["career_track"]
            if "career_track" in row.keys()
            else "multi_track",
        )
        for row in rows
    ]


def load_all_characters(conn: sqlite3.Connection) -> list:
    """Load all characters from the database."""
    import json as _json
    from src.models import Character

    rows = conn.execute("SELECT * FROM characters").fetchall()
    result = []
    for row in rows:
        aliases: list = []
        if row["aliases"]:
            try:
                aliases = _json.loads(row["aliases"])
            except (ValueError, TypeError):
                pass
        result.append(
            Character(
                id=row["id"],
                name_ja=row["name_ja"] or "",
                name_en=row["name_en"] or "",
                anilist_id=row["anilist_id"],
                image_large=row["image_large"],
                image_medium=row["image_medium"],
                description=row["description"],
                gender=row["gender"],
                date_of_birth=row["date_of_birth"],
                age=row["age"],
                blood_type=row["blood_type"],
                favourites=row["favourites"],
                site_url=row["site_url"],
                aliases=aliases,
            )
        )
    return result


def load_all_voice_actor_credits(conn: sqlite3.Connection) -> list:
    """Load all character_voice_actors records."""
    from src.models import CharacterVoiceActor

    rows = conn.execute("SELECT * FROM character_voice_actors").fetchall()
    return [
        CharacterVoiceActor(
            character_id=row["character_id"],
            person_id=row["person_id"],
            anime_id=row["anime_id"],
            character_role=row["character_role"] or "",
            source=row["source"] or "",
        )
        for row in rows
    ]


def record_pipeline_run(
    conn: sqlite3.Connection,
    credit_count: int,
    person_count: int,
    elapsed: float,
    mode: str = "full",
) -> int:
    """Record a pipeline run."""
    cursor = conn.execute(
        """INSERT INTO pipeline_runs (credit_count, person_count, elapsed_seconds, mode)
           VALUES (?, ?, ?, ?)""",
        (credit_count, person_count, elapsed, mode),
    )
    return cursor.lastrowid or 0


def get_last_pipeline_run(conn: sqlite3.Connection) -> dict | None:
    """Return information about the most recent pipeline run."""
    row = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def has_credits_changed_since_last_run(conn: sqlite3.Connection) -> bool:
    """Determine whether credit data has changed since the last pipeline run.

    Detects changes by comparing credit_count and person_count.
    Returns True if no previous pipeline run exists.

    Returns:
        True: credit count or person count has changed, or no prior run exists
        False: no change detected
    """
    last_run = get_last_pipeline_run(conn)
    if last_run is None:
        return True

    current_credit_count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
    if current_credit_count != last_run["credit_count"]:
        return True

    current_person_count = conn.execute(
        "SELECT COUNT(DISTINCT person_id) FROM credits"
    ).fetchone()[0]
    if current_person_count != last_run.get("person_count", -1):
        return True

    return False


def get_persons_with_new_credits(
    conn: sqlite3.Connection,
    since_run_id: int,
) -> set[str]:
    """Return IDs of persons who have new credits since the specified pipeline run.

    Approximated as credits with id > (max credit id at the reference run).
    """
    # Get max credit ID at the time of the reference run
    max_credit = conn.execute(
        """SELECT MAX(c.id) FROM credits c
           JOIN pipeline_runs pr ON pr.id = ?
           WHERE c.id <= (SELECT COALESCE(
               (SELECT MAX(id) FROM credits WHERE rowid <=
                   (SELECT credit_count FROM pipeline_runs WHERE id = ?)),
               (SELECT MAX(id) FROM credits)
           ))""",
        (since_run_id, since_run_id),
    ).fetchone()[0]

    if max_credit is None:
        # No reference point, all persons are "new"
        rows = conn.execute("SELECT DISTINCT person_id FROM credits").fetchall()
        return {r["person_id"] for r in rows}

    rows = conn.execute(
        "SELECT DISTINCT person_id FROM credits WHERE id > ?", (max_credit,)
    ).fetchall()
    return {r["person_id"] for r in rows}


def save_score_history(
    conn: sqlite3.Connection,
    score: ScoreResult,
    year: int | None = None,
    quarter: int | None = None,
) -> None:
    """Save score history with year and quarter."""
    conn.execute(
        """INSERT INTO score_history
               (person_id, person_fe, studio_fe_exposure, birank,
                patronage, dormancy, awcc, iv_score, year, quarter)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            score.person_id,
            score.person_fe,
            score.studio_fe_exposure,
            score.birank,
            score.patronage,
            score.dormancy,
            score.awcc,
            score.iv_score,
            year,
            quarter,
        ),
    )


def get_score_history(
    conn: sqlite3.Connection,
    person_id: str,
    limit: int = 50,
) -> list[dict]:
    """Return score history for a person (most recent first)."""
    rows = conn.execute(
        """SELECT person_fe, studio_fe_exposure, birank,
                  patronage, dormancy, awcc, iv_score,
                  year, quarter, run_at
           FROM score_history
           WHERE person_id = ?
           ORDER BY year DESC, quarter DESC, id DESC
           LIMIT ?""",
        (person_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_score_history_by_quarter(
    conn: sqlite3.Connection,
    year: int | None = None,
    quarter: int | None = None,
) -> list[dict]:
    """Return score history grouped by quarter."""
    conditions = []
    params: list = []
    if year is not None:
        conditions.append("year = ?")
        params.append(year)
    if quarter is not None:
        conditions.append("quarter = ?")
        params.append(quarter)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"""SELECT person_id, person_fe, studio_fe_exposure, birank,
                  patronage, dormancy, awcc, iv_score,
                  year, quarter, run_at
           FROM score_history
           {where}
           ORDER BY year, quarter, person_id""",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_person_ids(conn: sqlite3.Connection) -> set[str]:
    """Quickly fetch all existing person IDs (for skip-check purposes)."""
    rows = conn.execute("SELECT id FROM persons").fetchall()
    return {row["id"] for row in rows}


# ---------------------------------------------------------------------------
# LLM decision cache — DB-backed persistence
# ---------------------------------------------------------------------------


def get_llm_decision(conn: sqlite3.Connection, name: str, task: str) -> dict | None:
    """Retrieve a cached LLM decision.

    Args:
        name: target name (person name or name pair)
        task: task type ("org_classification" | "name_normalization" | "entity_match")

    Returns:
        result_json parsed as dict, or None if not found
    """
    import json

    row = conn.execute(
        "SELECT result_json FROM llm_decisions WHERE name = ? AND task = ?",
        (name, task),
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row["result_json"])
    except (json.JSONDecodeError, TypeError):
        return None


def upsert_llm_decision(
    conn: sqlite3.Connection,
    name: str,
    task: str,
    result: dict,
    model: str = "",
) -> None:
    """Save or update an LLM decision result."""
    import json

    conn.execute(
        """INSERT INTO llm_decisions (name, task, result_json, model, updated_at)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(name, task) DO UPDATE SET
               result_json = excluded.result_json,
               model = excluded.model,
               updated_at = CURRENT_TIMESTAMP""",
        (name, task, json.dumps(result, ensure_ascii=False), model),
    )


def get_all_llm_decisions(conn: sqlite3.Connection, task: str) -> dict[str, dict]:
    """Fetch all cached LLM decisions for a given task in bulk.

    Returns:
        {name: result_dict} mapping
    """
    import json

    rows = conn.execute(
        "SELECT name, result_json FROM llm_decisions WHERE task = ?",
        (task,),
    ).fetchall()
    result: dict[str, dict] = {}
    for row in rows:
        try:
            result[row["name"]] = json.loads(row["result_json"])
        except (json.JSONDecodeError, TypeError):
            continue
    return result


# =============================================================================
# v37–v40 migration functions
def upsert_feat_causal_estimates(
    conn: sqlite3.Connection,
    peer_boosts: dict[str, float],
    friction_index: dict[str, float],
    era_fe_by_person: dict[str, float],
    iv_scores: dict[str, float],
    opportunity_residuals: dict[str, float] | None = None,
) -> int:
    """Save causal inference results to feat_causal_estimates.

    Args:
        conn: SQLite connection
        peer_boosts: person_id → peer effect boost (PeerEffectResult.person_peer_boost)
        friction_index: person_id → career friction index (0=no friction, 1=max)
        era_fe_by_person: person_id → era fixed effect corresponding to debut year
        iv_scores: person_id → IV score (used to compute era_deflated_iv)
        opportunity_residuals: person_id → opportunity-adjusted residual (optional)

    Returns:
        number of rows written
    """
    all_pids = set(peer_boosts) | set(friction_index) | set(era_fe_by_person)
    rows: list[tuple] = []
    for pid in all_pids:
        era = era_fe_by_person.get(pid)
        iv = iv_scores.get(pid)
        # era_deflated_iv: iv_score adjusted for era fixed effect (era_fe > 0 = favourable era)
        era_deflated = (
            round(iv - era, 6) if (iv is not None and era is not None) else None
        )
        rows.append(
            (
                pid,
                round(peer_boosts.get(pid, 0.0), 6),
                round(friction_index.get(pid, 0.0), 6),
                round(era, 6) if era is not None else None,
                era_deflated,
                round(opportunity_residuals[pid], 6)
                if opportunity_residuals and pid in opportunity_residuals
                else None,
            )
        )

    conn.executemany(
        """INSERT INTO feat_causal_estimates
               (person_id, peer_effect_boost, career_friction, era_fe,
                era_deflated_iv, opportunity_residual, updated_at)
           VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)
           ON CONFLICT(person_id) DO UPDATE SET
               peer_effect_boost=excluded.peer_effect_boost,
               career_friction=excluded.career_friction,
               era_fe=excluded.era_fe,
               era_deflated_iv=excluded.era_deflated_iv,
               opportunity_residual=excluded.opportunity_residual,
               updated_at=CURRENT_TIMESTAMP""",
        rows,
    )
    conn.commit()
    logger.info("feat_causal_estimates_upserted", count=len(rows))
    return len(rows)


def load_feat_causal_estimates(
    conn: sqlite3.Connection,
    person_id: str | None = None,
) -> dict[str, dict] | dict:
    """Return feat_causal_estimates rows.

    Args:
        person_id: specific person → returns dict. If omitted → returns {person_id: dict}.
    """
    if person_id is not None:
        row = conn.execute(
            "SELECT * FROM feat_causal_estimates WHERE person_id=?", (person_id,)
        ).fetchone()
        return dict(row) if row else {}
    rows = conn.execute("SELECT * FROM feat_causal_estimates").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def load_feat_work_context(
    conn: sqlite3.Connection,
    anime_id: str | None = None,
) -> dict[str, dict] | dict:
    """Return feat_work_context rows.

    Args:
        anime_id: specific anime → returns dict. If omitted → returns {anime_id: dict}.
    """
    if anime_id is not None:
        row = conn.execute(
            "SELECT * FROM feat_work_context WHERE anime_id=?", (anime_id,)
        ).fetchone()
        return dict(row) if row else {}
    rows = conn.execute("SELECT * FROM feat_work_context").fetchall()
    return {r["anime_id"]: dict(r) for r in rows}


def load_feat_person_role_progression(
    conn: sqlite3.Connection,
    person_id: str | None = None,
    role_category: str | None = None,
) -> list[dict]:
    """Return feat_person_role_progression rows.

    Args:
        person_id: filter to a specific person
        role_category: filter to a specific role category
    """
    where: list[str] = []
    params: list = []
    if person_id is not None:
        where.append("person_id = ?")
        params.append(person_id)
    if role_category is not None:
        where.append("role_category = ?")
        params.append(role_category)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT * FROM feat_person_role_progression {w} ORDER BY role_category",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


# =============================================================================
# v41: feat_cluster_membership migration + upsert + load
def upsert_feat_cluster_membership(
    conn: sqlite3.Connection,
    community_map: dict[str, int],
    career_tracks: dict[str, str],
    growth_data: dict,
    studio_clustering: dict,
    cooccurrence_groups: dict,
    studio_affiliation: dict[str, str] | None = None,
) -> int:
    """Save clustering membership to feat_cluster_membership.

    Args:
        conn: SQLite connection
        community_map: person_id → community_id (Phase 4 graph community)
        career_tracks: person_id → career_track string (Phase 6)
        growth_data: equivalent to growth.json {"persons": {pid: {"trend": ...}}} or list
        studio_clustering: equivalent to studio_clustering.json {"assignments": {studio: {...}}}
        cooccurrence_groups: equivalent to cooccurrence_groups.json {"groups": [{members: [...]}]}
        studio_affiliation: person_id → main_studio_id (if omitted, fetched from feat_studio_affiliation)

    Returns:
        number of rows written
    """
    # --- growth_trend per person ---
    growth_trend_map: dict[str, str] = {}
    persons_data = (
        growth_data.get("persons", {}) if isinstance(growth_data, dict) else {}
    )
    if isinstance(persons_data, dict):
        for pid, info in persons_data.items():
            if isinstance(info, dict) and "trend" in info:
                growth_trend_map[pid] = info["trend"]
    elif isinstance(persons_data, list):
        for info in persons_data:
            if isinstance(info, dict) and "person_id" in info and "trend" in info:
                growth_trend_map[info["person_id"]] = info["trend"]

    # --- studio cluster per person (via main studio) ---
    # studio_clustering.json: {"assignments": {studio_name: {cluster_id, cluster_name, ...}}}
    studio_cluster_map: dict[
        str, tuple[int | None, str | None]
    ] = {}  # person_id → (id, name)
    assignments = {}
    if isinstance(studio_clustering, dict):
        assignments = studio_clustering.get("assignments", {})

    # person → main studio from feat_studio_affiliation (most frequent studio overall)
    if studio_affiliation is None:
        rows = conn.execute("""
            SELECT person_id, studio_name
            FROM feat_studio_affiliation
            WHERE n_works = (
                SELECT MAX(n_works2.n_works)
                FROM feat_studio_affiliation n_works2
                WHERE n_works2.person_id = feat_studio_affiliation.person_id
            )
            GROUP BY person_id
        """).fetchall()
        studio_affiliation = {r["person_id"]: r["studio_name"] for r in rows}

    for pid, studio_name in (studio_affiliation or {}).items():
        info = assignments.get(studio_name)
        if info and isinstance(info, dict):
            studio_cluster_map[pid] = (info.get("cluster_id"), info.get("cluster_name"))

    # --- cooccurrence_group_id per person (inverted index) ---
    cooccurrence_map: dict[str, int] = {}
    groups = []
    if isinstance(cooccurrence_groups, dict):
        groups = cooccurrence_groups.get("groups", [])
    for idx, group in enumerate(groups):
        if isinstance(group, dict):
            for pid in group.get("members", []):
                cooccurrence_map[pid] = idx

    # --- full set of all person_ids ---
    all_pids: set[str] = (
        set(community_map)
        | set(career_tracks)
        | set(growth_trend_map)
        | set(studio_cluster_map)
        | set(cooccurrence_map)
    )

    rows_out: list[tuple] = []
    for pid in all_pids:
        sc = studio_cluster_map.get(pid, (None, None))
        rows_out.append(
            (
                pid,
                community_map.get(pid),
                career_tracks.get(pid),
                growth_trend_map.get(pid),
                sc[0],
                sc[1],
                cooccurrence_map.get(pid),
            )
        )

    conn.executemany(
        """INSERT INTO feat_cluster_membership
               (person_id, community_id, career_track, growth_trend,
                studio_cluster_id, studio_cluster_name, cooccurrence_group_id,
                updated_at)
           VALUES (?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
           ON CONFLICT(person_id) DO UPDATE SET
               community_id=excluded.community_id,
               career_track=excluded.career_track,
               growth_trend=excluded.growth_trend,
               studio_cluster_id=excluded.studio_cluster_id,
               studio_cluster_name=excluded.studio_cluster_name,
               cooccurrence_group_id=excluded.cooccurrence_group_id,
               updated_at=CURRENT_TIMESTAMP""",
        rows_out,
    )
    conn.commit()
    logger.info("feat_cluster_membership_upserted", count=len(rows_out))
    return len(rows_out)


def load_feat_cluster_membership(
    conn: sqlite3.Connection,
    person_id: str | None = None,
    community_id: int | None = None,
    career_track: str | None = None,
    growth_trend: str | None = None,
) -> dict[str, dict] | dict:
    """Return feat_cluster_membership rows.

    Args:
        person_id: specific person → returns dict. If omitted → returns {person_id: dict}.
        community_id: filter by community ID
        career_track: filter by career track
        growth_trend: filter by growth trend
    """
    if person_id is not None:
        row = conn.execute(
            "SELECT * FROM feat_cluster_membership WHERE person_id=?", (person_id,)
        ).fetchone()
        return dict(row) if row else {}

    where: list[str] = []
    params: list = []
    if community_id is not None:
        where.append("community_id = ?")
        params.append(community_id)
    if career_track is not None:
        where.append("career_track = ?")
        params.append(career_track)
    if growth_trend is not None:
        where.append("growth_trend = ?")
        params.append(growth_trend)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(f"SELECT * FROM feat_cluster_membership {w}", params).fetchall()
    return {r["person_id"]: dict(r) for r in rows}


# =============================================================================
# v42: feat_birank_annual — annual BiRank snapshot
# =============================================================================

_BIRANK_ANNUAL_MIN_YEAR = 1980  # graphs before this year are too sparse for meaningful scores
def upsert_feat_birank_annual(
    conn: sqlite3.Connection,
    birank_timelines: dict[str, dict],
    min_year: int = _BIRANK_ANNUAL_MIN_YEAR,
) -> int:
    """Save annual BiRank snapshots to feat_birank_annual.

    Args:
        conn: SQLite connection
        birank_timelines: {person_id: {"snapshots": [{year, birank, raw_pagerank,
            graph_size, n_credits_cumulative}, ...], ...}}
            i.e. the return value of compute_temporal_pagerank converted with asdict().
        min_year: only save snapshots from this year onward (default 1980)

    Returns:
        number of rows written
    """
    rows: list[tuple] = []
    for pid, tl in birank_timelines.items():
        for snap in tl.get("snapshots", []):
            year = snap.get("year")
            birank = snap.get("birank")
            if year is None or birank is None or year < min_year:
                continue
            rows.append(
                (
                    pid,
                    year,
                    float(birank),
                    snap.get("raw_pagerank"),
                    snap.get("graph_size"),
                    snap.get("n_credits_cumulative"),
                )
            )

    conn.executemany(
        """INSERT INTO feat_birank_annual
               (person_id, year, birank, raw_pagerank, graph_size, n_credits_cumulative)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(person_id, year) DO UPDATE SET
               birank = excluded.birank,
               raw_pagerank = excluded.raw_pagerank,
               graph_size = excluded.graph_size,
               n_credits_cumulative = excluded.n_credits_cumulative""",
        rows,
    )
    conn.commit()
    logger.info("feat_birank_annual_upserted", count=len(rows), min_year=min_year)
    return len(rows)


def load_feat_birank_annual(
    conn: sqlite3.Connection,
    person_id: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> list[dict] | dict[str, list[dict]]:
    """Return feat_birank_annual rows.

    Args:
        person_id: specific person → list[dict] (ascending year). If omitted → {person_id: list[dict]}.
        year_from: lower year bound (inclusive)
        year_to: upper year bound (inclusive)

    Returns:
        with person_id: [{"year": ..., "birank": ..., ...}, ...]
        all records: {"person_id": [snapshots...], ...}
    """
    where: list[str] = []
    params: list = []
    if person_id is not None:
        where.append("person_id = ?")
        params.append(person_id)
    if year_from is not None:
        where.append("year >= ?")
        params.append(year_from)
    if year_to is not None:
        where.append("year <= ?")
        params.append(year_to)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT person_id, year, birank, raw_pagerank, graph_size, n_credits_cumulative "
        f"FROM feat_birank_annual {w} ORDER BY person_id, year",
        params,
    ).fetchall()

    if person_id is not None:
        return [dict(r) for r in rows]

    result: dict[str, list[dict]] = {}
    for r in rows:
        result.setdefault(r["person_id"], []).append(dict(r))
    return result


# =============================================================================
# v43: birank_compute_state — BiRank computation input fingerprint (for change detection)
def upsert_birank_compute_state(
    conn: sqlite3.Connection,
    states: dict[int, dict],
) -> None:
    """Save per-year BiRank computation fingerprints.

    Args:
        conn: SQLite connection
        states: {year: {"credit_count": int, "anime_count": int, "person_count": int}}
                records the credit/anime/person counts used in the computation for each year.
    """
    import time as _time

    now = _time.time()
    rows = [
        (yr, d["credit_count"], d["anime_count"], d["person_count"], now)
        for yr, d in states.items()
    ]
    conn.executemany(
        """INSERT INTO birank_compute_state
               (year, credit_count, anime_count, person_count, computed_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(year) DO UPDATE SET
               credit_count = excluded.credit_count,
               anime_count  = excluded.anime_count,
               person_count = excluded.person_count,
               computed_at  = excluded.computed_at""",
        rows,
    )
    conn.commit()
    logger.debug("birank_compute_state_upserted", count=len(rows))


def load_birank_compute_state(conn: sqlite3.Connection) -> dict[int, dict]:
    """Return saved BiRank computation fingerprints.

    Returns:
        {year: {"credit_count": int, "anime_count": int, "person_count": int}}
    """
    rows = conn.execute(
        "SELECT year, credit_count, anime_count, person_count "
        "FROM birank_compute_state ORDER BY year"
    ).fetchall()
    return {
        r["year"]: {
            "credit_count": r["credit_count"],
            "anime_count": r["anime_count"],
            "person_count": r["person_count"],
        }
        for r in rows
    }


# =============================================================================
# v44: work scale tier — computed from format + episodes + duration only
# staff count is not used because data quality is low (median 2-11)
# =============================================================================

# format_group: group similar formats
_FORMAT_GROUP: dict[str | None, str] = {
    "MOVIE": "movie",
    "TV": "tv",
    "TV_SHORT": "tv_short",
    "OVA": "ova",
    "ONA": "ona",
    "SPECIAL": "special",
    "MUSIC": "music",
    None: "other",
}
def compute_feat_career_gaps(
    conn: sqlite3.Connection,
    *,
    semi_exit_years: int = 3,
    exit_years: int = 5,
    reliable_max_year: int = 2025,
) -> int:
    """Compute career gaps and save to feat_career_gaps.

    Detects consecutive gaps in each person's credit year list:
    - semi_exit: semi_exit_years <= gap < exit_years
    - exit: gap >= exit_years
    - returned: True if credits exist after the gap

    Returns:
        Total rows written.
    """
    logger = structlog.get_logger()
    logger.info(
        "feat_career_gaps_compute_start",
        semi_exit_years=semi_exit_years,
        exit_years=exit_years,
    )

    # Get all person credit years (distinct)
    rows = conn.execute("""
        SELECT c.person_id, GROUP_CONCAT(DISTINCT a.year) AS years
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.year IS NOT NULL
        GROUP BY c.person_id
    """).fetchall()

    inserts: list[tuple] = []
    for row in rows:
        pid = row[0]
        year_strs = row[1]
        if not year_strs:
            continue
        years = sorted(set(int(y) for y in year_strs.split(",")))
        if len(years) < 2:
            continue

        for i in range(len(years) - 1):
            gap = years[i + 1] - years[i]
            if gap >= semi_exit_years:
                gap_start = years[i]
                gap_end = years[i + 1]
                gap_type = "exit" if gap >= exit_years else "semi_exit"
                inserts.append(
                    (
                        pid,
                        gap_start,
                        gap_end,
                        gap,
                        1,  # returned = True (there is a subsequent credit)
                        gap_type,
                    )
                )

        # Check if the person's last credit year indicates an ongoing gap
        last_year = years[-1]
        ongoing_gap = reliable_max_year - last_year
        if ongoing_gap >= semi_exit_years:
            gap_type = "exit" if ongoing_gap >= exit_years else "semi_exit"
            inserts.append(
                (
                    pid,
                    last_year,
                    None,  # gap_end = NULL (not yet returned)
                    ongoing_gap,
                    0,  # returned = False
                    gap_type,
                )
            )

    conn.execute("DELETE FROM feat_career_gaps")
    if inserts:
        conn.executemany(
            """INSERT OR REPLACE INTO feat_career_gaps
               (person_id, gap_start_year, gap_end_year, gap_length, returned, gap_type)
               VALUES (?,?,?,?,?,?)""",
            inserts,
        )
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM feat_career_gaps").fetchone()[0]
    n_returned = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE returned = 1"
    ).fetchone()[0]
    n_exit = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE gap_type = 'exit'"
    ).fetchone()[0]
    n_semi = conn.execute(
        "SELECT COUNT(*) FROM feat_career_gaps WHERE gap_type = 'semi_exit'"
    ).fetchone()[0]
    logger.info(
        "feat_career_gaps_computed",
        total=total,
        returned=n_returned,
        exits=n_exit,
        semi_exits=n_semi,
    )
    return total
def upsert_src_anilist_anime(conn: sqlite3.Connection, anime: "Anime") -> None:
    """Save raw AniList anime data to src_anilist_anime."""
    import json as _json

    if anime.anilist_id is None:
        return
    conn.execute(
        """INSERT INTO src_anilist_anime (
               anilist_id, title_ja, title_en, year, season, episodes, format,
               status, start_date, end_date, duration, source, description,
               score, genres, tags, studios, synonyms, cover_large, cover_medium,
               banner, popularity, favourites, site_url, mal_id,
               country_of_origin, is_licensed, is_adult, mean_score, relations_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(anilist_id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_anilist_anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), src_anilist_anime.title_en),
               year = COALESCE(excluded.year, src_anilist_anime.year),
               season = COALESCE(excluded.season, src_anilist_anime.season),
               episodes = COALESCE(excluded.episodes, src_anilist_anime.episodes),
               format = COALESCE(excluded.format, src_anilist_anime.format),
               status = COALESCE(excluded.status, src_anilist_anime.status),
               start_date = COALESCE(excluded.start_date, src_anilist_anime.start_date),
               end_date = COALESCE(excluded.end_date, src_anilist_anime.end_date),
               description = COALESCE(excluded.description, src_anilist_anime.description),
               score = COALESCE(excluded.score, src_anilist_anime.score),
               genres = excluded.genres,
               tags = excluded.tags,
               studios = excluded.studios,
               synonyms = excluded.synonyms,
               cover_large = COALESCE(excluded.cover_large, src_anilist_anime.cover_large),
               country_of_origin = COALESCE(excluded.country_of_origin, src_anilist_anime.country_of_origin),
               is_licensed = COALESCE(excluded.is_licensed, src_anilist_anime.is_licensed),
               is_adult = COALESCE(excluded.is_adult, src_anilist_anime.is_adult),
               mean_score = COALESCE(excluded.mean_score, src_anilist_anime.mean_score),
               relations_json = COALESCE(excluded.relations_json, src_anilist_anime.relations_json),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            anime.anilist_id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            anime.source,
            anime.description,
            anime.score,
            _json.dumps(anime.genres, ensure_ascii=False),
            _json.dumps(anime.tags, ensure_ascii=False),
            _json.dumps(anime.studios, ensure_ascii=False),
            _json.dumps(anime.synonyms, ensure_ascii=False),
            anime.cover_large,
            anime.cover_medium,
            anime.banner,
            anime.popularity_rank,
            anime.favourites,
            anime.site_url,
            anime.mal_id,
            anime.country_of_origin,
            1 if anime.is_licensed else (0 if anime.is_licensed is not None else None),
            1 if anime.is_adult else (0 if anime.is_adult is not None else None),
            anime.mean_score,
            anime.relations_json,
        ),
    )


def upsert_src_anilist_person(conn: sqlite3.Connection, person: "Person") -> None:
    """Save raw AniList person data to src_anilist_persons."""
    import json as _json

    if person.anilist_id is None:
        return
    conn.execute(
        """INSERT INTO src_anilist_persons (
               anilist_id, name_ja, name_en, name_ko, name_zh, aliases, nationality,
               date_of_birth, age, gender,
               years_active, hometown, blood_type, description,
               image_large, image_medium, favourites, site_url
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(anilist_id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), src_anilist_persons.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), src_anilist_persons.name_en),
               name_ko = COALESCE(NULLIF(excluded.name_ko, ''), src_anilist_persons.name_ko),
               name_zh = COALESCE(NULLIF(excluded.name_zh, ''), src_anilist_persons.name_zh),
               aliases = excluded.aliases,
               nationality = COALESCE(NULLIF(excluded.nationality, '[]'), src_anilist_persons.nationality),
               date_of_birth = COALESCE(excluded.date_of_birth, src_anilist_persons.date_of_birth),
               hometown = COALESCE(excluded.hometown, src_anilist_persons.hometown),
               description = COALESCE(excluded.description, src_anilist_persons.description),
               image_large = COALESCE(excluded.image_large, src_anilist_persons.image_large),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            person.anilist_id,
            # BRONZE: use name_native_raw so ambiguous CJK (zh_or_ja, nationality=[])
            # is preserved even when SILVER name_ja is empty.
            person.name_ja or getattr(person, "name_native_raw", ""),
            person.name_en,
            person.name_ko,
            person.name_zh,
            _json.dumps(person.aliases, ensure_ascii=False),
            _json.dumps(person.nationality, ensure_ascii=False),
            person.date_of_birth,
            person.age,
            person.gender,
            _json.dumps(person.years_active, ensure_ascii=False),
            person.hometown,
            person.blood_type,
            person.description,
            person.image_large,
            person.image_medium,
            person.favourites,
            person.site_url,
        ),
    )


def insert_src_anilist_credit(
    conn: sqlite3.Connection,
    anilist_anime_id: int,
    anilist_person_id: int,
    role: str,
    role_raw: str,
) -> None:
    """Save raw AniList credit data to src_anilist_credits."""
    conn.execute(
        """INSERT OR IGNORE INTO src_anilist_credits
               (anilist_anime_id, anilist_person_id, role, role_raw)
           VALUES (?, ?, ?, ?)""",
        (anilist_anime_id, anilist_person_id, role, role_raw),
    )


def upsert_src_ann_anime(conn: sqlite3.Connection, rec: object) -> None:
    """Save raw ANN anime data to src_ann_anime (accepts AnnAnimeRecord)."""
    import json as _json

    conn.execute(
        """INSERT INTO src_ann_anime
               (ann_id, title_en, title_ja, year, episodes, format, genres, start_date, end_date)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(ann_id) DO UPDATE SET
               title_en = COALESCE(NULLIF(excluded.title_en, ''), src_ann_anime.title_en),
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_ann_anime.title_ja),
               year = COALESCE(excluded.year, src_ann_anime.year),
               episodes = COALESCE(excluded.episodes, src_ann_anime.episodes),
               format = COALESCE(excluded.format, src_ann_anime.format),
               genres = excluded.genres,
               start_date = COALESCE(excluded.start_date, src_ann_anime.start_date),
               end_date = COALESCE(excluded.end_date, src_ann_anime.end_date),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            rec.ann_id,
            rec.title_en,
            rec.title_ja,
            rec.year,
            rec.episodes,
            rec.format,
            _json.dumps(rec.genres, ensure_ascii=False),
            rec.start_date,
            rec.end_date,
        ),
    )


def upsert_src_ann_person(conn: sqlite3.Connection, detail: object) -> None:
    """Save raw ANN person data to src_ann_persons (accepts AnnPersonDetail)."""
    conn.execute(
        """INSERT INTO src_ann_persons
               (ann_id, name_en, name_ja, date_of_birth, hometown, blood_type, website, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(ann_id) DO UPDATE SET
               name_en = COALESCE(NULLIF(excluded.name_en, ''), src_ann_persons.name_en),
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), src_ann_persons.name_ja),
               date_of_birth = COALESCE(excluded.date_of_birth, src_ann_persons.date_of_birth),
               hometown = COALESCE(excluded.hometown, src_ann_persons.hometown),
               blood_type = COALESCE(excluded.blood_type, src_ann_persons.blood_type),
               website = COALESCE(excluded.website, src_ann_persons.website),
               description = COALESCE(excluded.description, src_ann_persons.description),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            detail.ann_id,
            detail.name_en,
            detail.name_ja,
            detail.date_of_birth,
            detail.hometown,
            detail.blood_type,
            detail.website,
            detail.description,
        ),
    )


def insert_src_ann_credit(
    conn: sqlite3.Connection,
    ann_anime_id: int,
    ann_person_id: int,
    name_en: str,
    role: str,
    role_raw: str,
) -> None:
    """Save raw ANN credit data to src_ann_credits."""
    conn.execute(
        """INSERT OR IGNORE INTO src_ann_credits
               (ann_anime_id, ann_person_id, name_en, role, role_raw)
           VALUES (?, ?, ?, ?, ?)""",
        (ann_anime_id, ann_person_id, name_en, role, role_raw),
    )


def upsert_src_allcinema_anime(conn: sqlite3.Connection, rec: object) -> None:
    """Save raw allcinema anime data to src_allcinema_anime."""
    conn.execute(
        """INSERT INTO src_allcinema_anime
               (allcinema_id, title_ja, year, start_date, synopsis)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(allcinema_id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_allcinema_anime.title_ja),
               year = COALESCE(excluded.year, src_allcinema_anime.year),
               synopsis = COALESCE(excluded.synopsis, src_allcinema_anime.synopsis),
               scraped_at = CURRENT_TIMESTAMP""",
        (rec.cinema_id, rec.title_ja, rec.year, rec.start_date, rec.synopsis or None),
    )


def upsert_src_allcinema_person(conn: sqlite3.Connection, rec: object) -> None:
    """Save raw allcinema person data to src_allcinema_persons."""
    conn.execute(
        """INSERT INTO src_allcinema_persons
               (allcinema_id, name_ja, yomigana, name_en)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(allcinema_id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), src_allcinema_persons.name_ja),
               yomigana = COALESCE(NULLIF(excluded.yomigana, ''), src_allcinema_persons.yomigana),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), src_allcinema_persons.name_en),
               scraped_at = CURRENT_TIMESTAMP""",
        (rec.allcinema_id, rec.name_ja, rec.yomigana, rec.name_en),
    )


def insert_src_allcinema_credit(
    conn: sqlite3.Connection, allcinema_anime_id: int, credit: object
) -> None:
    """Save raw allcinema credit data to src_allcinema_credits."""
    conn.execute(
        """INSERT OR IGNORE INTO src_allcinema_credits
               (allcinema_anime_id, allcinema_person_id, name_ja, name_en, job_name, job_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            allcinema_anime_id,
            credit.allcinema_person_id,
            credit.name_ja,
            credit.name_en,
            credit.job_name,
            credit.job_id,
        ),
    )


def upsert_src_seesaawiki_anime(
    conn: sqlite3.Connection,
    anime_id: str,
    title_ja: str,
    year: int | None,
    episodes: int | None,
) -> None:
    """Save raw SeesaaWiki anime data to src_seesaawiki_anime."""
    conn.execute(
        """INSERT INTO src_seesaawiki_anime (id, title_ja, year, episodes)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_seesaawiki_anime.title_ja),
               year = COALESCE(excluded.year, src_seesaawiki_anime.year),
               episodes = COALESCE(excluded.episodes, src_seesaawiki_anime.episodes),
               scraped_at = CURRENT_TIMESTAMP""",
        (anime_id, title_ja, year, episodes),
    )


def insert_src_seesaawiki_credit(
    conn: sqlite3.Connection,
    anime_src_id: str,
    person_name: str,
    role: str,
    role_raw: str,
    episode: int | None = None,
    affiliation: str | None = None,
    is_company: bool = False,
) -> None:
    """Save raw SeesaaWiki credit data to src_seesaawiki_credits."""
    conn.execute(
        """INSERT OR IGNORE INTO src_seesaawiki_credits
               (anime_src_id, person_name, role, role_raw, episode, affiliation, is_company)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            anime_src_id,
            person_name,
            role,
            role_raw,
            episode,
            affiliation,
            int(is_company),
        ),
    )


def upsert_src_keyframe_anime(
    conn: sqlite3.Connection,
    slug: str,
    title_ja: str,
    title_en: str,
    anilist_id: int | None,
) -> None:
    """Save raw KeyFrame anime data to src_keyframe_anime."""
    conn.execute(
        """INSERT INTO src_keyframe_anime (slug, title_ja, title_en, anilist_id)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(slug) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), src_keyframe_anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), src_keyframe_anime.title_en),
               anilist_id = COALESCE(excluded.anilist_id, src_keyframe_anime.anilist_id),
               scraped_at = CURRENT_TIMESTAMP""",
        (slug, title_ja, title_en, anilist_id),
    )


def insert_src_keyframe_credit(
    conn: sqlite3.Connection,
    keyframe_slug: str,
    kf_person_id: int,
    name_ja: str,
    name_en: str,
    role_ja: str,
    role_en: str,
    episode: int | None = None,
) -> None:
    """Save raw KeyFrame credit data to src_keyframe_credits."""
    conn.execute(
        """INSERT OR IGNORE INTO src_keyframe_credits
               (keyframe_slug, kf_person_id, name_ja, name_en, role_ja, role_en, episode)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (keyframe_slug, kf_person_id, name_ja, name_en, role_ja, role_en, episode),
    )


# ============================================================================
# v50 migration: canonical silver normalization
# ============================================================================
#
# This migration introduces the normalization tables described in
# detailed_todo.md §1.4 (N-1 sources, N-2 roles, N-3 person_aliases,
# N-4 anime_external_ids/person_external_ids) and renames the credits
# evidence column (C-1).
#
# IMPORTANT: this migration is intentionally *additive* for the large
# destructive changes (dropping legacy ``anime``, ``anime_display``, and
# external-ID columns). Those drops require ~29 analysis modules and the
# ``Anime`` Pydantic shim to stop reading the removed columns first — work
# that is still in progress at the time this migration was written. The
# additive pieces unblock downstream consumers (reports, ETL) without
# breaking the 1947-test suite. Destructive drops are scheduled for v51
# once consumers have migrated.
#
# What v50 *does* change physically:
#   - CREATE tables: sources, roles, anime_external_ids, person_external_ids,
#     person_aliases, anime_genres, anime_tags
#   - Seed sources (5 existing + mal) and roles (from role_groups.py)
#   - ADD credits.evidence_source as a mirror of legacy source column; keep both
#     in sync so consumers can migrate at their own pace
#   - Convert credits.episode sentinel -1 → NULL
#   - Create person_scores as a VIEW over scores (alias for the eventual
#     rename)
#   - Backfill anime_external_ids / person_external_ids / person_aliases /
#     anime_genres / anime_tags from existing silver + bronze state
#   - Snapshot legacy tables into _archive_v49_* (E-3) for reversibility
# ============================================================================


_V50_SOURCE_SEEDS: tuple[tuple[str, str, str, str, str], ...] = (
    # code, name_ja, base_url, license, description
    (
        "anilist",
        "AniList",
        "https://anilist.co",
        "proprietary",
        "GraphQL で structured staff 情報が最も豊富",
    ),
    (
        "ann",
        "Anime News Network",
        "https://www.animenewsnetwork.com",
        "proprietary",
        "historical depth と職種粒度",
    ),
    (
        "allcinema",
        "allcinema",
        "https://www.allcinema.net",
        "proprietary",
        "邦画・OVA の網羅性",
    ),
    (
        "seesaawiki",
        "SeesaaWiki",
        "https://seesaawiki.jp",
        "CC-BY-SA",
        "fan-curated 詳細エピソード情報",
    ),
    (
        "keyframe",
        "Sakugabooru/Keyframe",
        "https://www.sakugabooru.com",
        "CC",
        "sakuga コミュニティ別名情報",
    ),
    (
        "mal",
        "MyAnimeList",
        "https://myanimelist.net",
        "proprietary",
        "MAL external IDs — anime_external_ids に移行される legacy ID",
    ),
)


# ROLE_GROUP classification per detailed_todo.md §1.4 N-2 CHECK constraint:
#   'director' | 'animator' | 'sound' | 'production' | 'writer' |
#   'voice_actor' | 'other'
_V50_ROLE_SEEDS: tuple[tuple[str, str, str, str, float, str], ...] = (
    # code, name_ja, name_en, role_group, weight_default, description_ja
    (
        "director",
        "監督",
        "Director",
        "director",
        1.0,
        "作品全体の演出責任者。最も高い責任を持つ",
    ),
    (
        "animation_director",
        "作画監督",
        "Animation Director",
        "director",
        0.9,
        "作画部門の監督。原画の品質を統括",
    ),
    (
        "episode_director",
        "演出",
        "Episode Director",
        "director",
        0.7,
        "話数ごとの演出・絵コンテ",
    ),
    (
        "key_animator",
        "原画",
        "Key Animator",
        "animator",
        0.6,
        "原画担当。動きの起点を描く",
    ),
    (
        "second_key_animator",
        "第二原画",
        "Second Key Animator",
        "animator",
        0.4,
        "原画の清書段階",
    ),
    (
        "in_between",
        "動画",
        "In-between Animator",
        "animator",
        0.3,
        "原画の間のフレームを描く動画工程",
    ),
    (
        "character_designer",
        "キャラクターデザイン",
        "Character Designer",
        "animator",
        0.8,
        "キャラクター設計。メカデザインを含む",
    ),
    (
        "photography_director",
        "撮影監督",
        "Photography Director",
        "animator",
        0.6,
        "撮影・エフェクト監督。コンポジット+特効",
    ),
    (
        "producer",
        "プロデューサー",
        "Producer",
        "production",
        0.5,
        "制作全体の統括プロデューサー",
    ),
    (
        "production_manager",
        "制作進行",
        "Production Manager",
        "production",
        0.3,
        "制作進行・制作デスク",
    ),
    (
        "sound_director",
        "音響監督",
        "Sound Director",
        "sound",
        0.5,
        "音響・音響効果の監督",
    ),
    ("music", "音楽", "Music", "sound", 0.3, "音楽・主題歌・挿入歌。非制作職"),
    ("screenplay", "脚本", "Screenplay", "writer", 0.6, "脚本・シリーズ構成"),
    ("original_creator", "原作", "Original Creator", "writer", 0.4, "原作者。非制作職"),
    (
        "background_art",
        "美術",
        "Background Art",
        "animator",
        0.5,
        "美術・背景。美術監督を含む",
    ),
    ("cgi_director", "CGI監督", "CGI Director", "animator", 0.6, "CG 部門監督"),
    (
        "layout",
        "レイアウト",
        "Layout Artist",
        "animator",
        0.4,
        "レイアウト作業 (原画工程の一部)",
    ),
    (
        "finishing",
        "仕上げ",
        "Finishing",
        "animator",
        0.3,
        "仕上げ・色彩設計・色指定・検査",
    ),
    ("editing", "編集", "Editing", "production", 0.3, "編集・ポスプロ"),
    ("settings", "設定", "Settings", "other", 0.2, "設定系"),
    ("voice_actor", "声優", "Voice Actor", "voice_actor", 0.1, "声優。非制作職"),
    (
        "localization",
        "ローカライズ",
        "Localization",
        "other",
        0.1,
        "各国語版スタッフ。非制作職",
    ),
    ("other", "その他", "Other", "other", 0.1, "ロール特定不可・分類不能なクレジット"),
    (
        "special",
        "スペシャル",
        "Special",
        "other",
        0.0,
        "スペシャルサンクス・ゲスト・制作外特別枠",
    ),
)

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


def _execute_sql_script(conn: sqlite3.Connection, script: str) -> None:
    """Execute SQL script by parsing statements individually.
    
    Avoids executescript() which has locking issues with WAL mode.
    """
    statements = []
    current = []
    in_string = False
    quote_char = None
    
    i = 0
    while i < len(script):
        char = script[i]
        
        if char == '-' and i + 1 < len(script) and script[i+1] == '-' and not in_string:
            while i < len(script) and script[i] != '\n':
                i += 1
            i += 1
            continue
        
        if char in ('"', "'", '`') and (i == 0 or script[i-1] != '\\'):
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                in_string = False
                quote_char = None
        
        if char == ';' and not in_string:
            current.append(char)
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)
        
        i += 1
    
    for stmt in statements:
        if stmt and not stmt.startswith('--'):
            conn.execute(stmt)
            conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables (delegates to init_db_v2 target schema)."""
    from src.database_v2 import init_db_v2
    init_db_v2(conn)


def _init_db_legacy(conn: sqlite3.Connection) -> None:
    """Legacy DDL — kept for reference only, not called in production."""
    # Use autocommit mode (isolation_level=None) and custom SQL parser to avoid locking
    old_isolation = conn.isolation_level
    conn.isolation_level = None
    conn.commit()  # Ensure clean state before autocommit
    try:
        _execute_sql_script(conn, """
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            name_ko TEXT NOT NULL DEFAULT '',
            name_zh TEXT NOT NULL DEFAULT '',
            aliases TEXT NOT NULL DEFAULT '[]',
            nationality TEXT NOT NULL DEFAULT '[]',
            mal_id INTEGER,
            anilist_id INTEGER,
            canonical_id TEXT,
            name_priority INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            start_date TEXT CHECK (
                start_date IS NULL
                OR start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            ),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_anime_year ON anime(year);

        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, role, episode)
        );

        -- v50 canonical silver normalization (see _migrate_v50_canonical_silver)
        CREATE TABLE IF NOT EXISTS sources (
            code         TEXT PRIMARY KEY,
            name_ja      TEXT NOT NULL,
            base_url     TEXT NOT NULL,
            license      TEXT NOT NULL,
            added_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            retired_at   TIMESTAMP,
            description  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS roles (
            code            TEXT PRIMARY KEY,
            name_ja         TEXT NOT NULL,
            name_en         TEXT NOT NULL,
            role_group      TEXT NOT NULL CHECK (role_group IN
                ('director','animator','sound','production','writer',
                 'voice_actor','other')),
            weight_default  REAL NOT NULL CHECK (weight_default >= 0),
            description_ja  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS anime_external_ids (
            anime_id     TEXT NOT NULL,
            source       TEXT NOT NULL REFERENCES sources(code),
            external_id  TEXT NOT NULL,
            PRIMARY KEY (anime_id, source),
            UNIQUE (source, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_ext_ids_source
            ON anime_external_ids(source, external_id);

        CREATE TABLE IF NOT EXISTS person_external_ids (
            person_id    TEXT NOT NULL,
            source       TEXT NOT NULL REFERENCES sources(code),
            external_id  TEXT NOT NULL,
            PRIMARY KEY (person_id, source),
            UNIQUE (source, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_person_ext_ids_source
            ON person_external_ids(source, external_id);

        CREATE TABLE IF NOT EXISTS person_aliases (
            person_id   TEXT NOT NULL,
            alias       TEXT NOT NULL,
            source      TEXT NOT NULL REFERENCES sources(code),
            lang        TEXT,
            confidence  REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
            added_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (person_id, alias, source)
        );
        CREATE INDEX IF NOT EXISTS idx_person_aliases_alias
            ON person_aliases(alias, person_id);

        CREATE TABLE IF NOT EXISTS anime_genres (
            anime_id   TEXT NOT NULL,
            genre_name TEXT NOT NULL,
            PRIMARY KEY (anime_id, genre_name)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_genres_genre
            ON anime_genres(genre_name, anime_id);

        CREATE TABLE IF NOT EXISTS anime_tags (
            anime_id TEXT NOT NULL,
            tag_name TEXT NOT NULL,
            rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
            PRIMARY KEY (anime_id, tag_name)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_tags_tag
            ON anime_tags(tag_name, rank, anime_id);

        CREATE TABLE IF NOT EXISTS person_scores (
            person_id TEXT PRIMARY KEY,
            person_fe REAL NOT NULL DEFAULT 0.0,
            studio_fe_exposure REAL NOT NULL DEFAULT 0.0,
            birank REAL NOT NULL DEFAULT 0.0,
            patronage REAL NOT NULL DEFAULT 0.0,
            dormancy REAL NOT NULL DEFAULT 1.0,
            awcc REAL NOT NULL DEFAULT 0.0,
            iv_score REAL NOT NULL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS source_scrape_status (
            source TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ok'
        );

        CREATE TABLE IF NOT EXISTS characters (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            aliases TEXT NOT NULL DEFAULT '[]',
            anilist_id INTEGER,
            image_large TEXT,
            image_medium TEXT,
            description TEXT,
            gender TEXT,
            date_of_birth TEXT,
            age TEXT,
            blood_type TEXT,
            favourites INTEGER,
            site_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS character_voice_actors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            character_id TEXT NOT NULL,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            character_role TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(character_id, person_id, anime_id)
        );

        CREATE TABLE IF NOT EXISTS studios (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            is_animation_studio INTEGER,
            favourites INTEGER,
            site_url TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime_studios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            studio_id TEXT NOT NULL,
            is_main INTEGER NOT NULL DEFAULT 0,
            UNIQUE(anime_id, studio_id)
        );

        CREATE TABLE IF NOT EXISTS anime_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            related_anime_id TEXT NOT NULL,
            relation_type TEXT NOT NULL DEFAULT '',
            related_title TEXT NOT NULL DEFAULT '',
            related_format TEXT,
            UNIQUE(anime_id, related_anime_id, relation_type)
        );

        -- anime_analysis removed: v50 migration renamed it to `anime` (canonical silver).
        -- Fresh installs create `anime` directly; legacy upgrade path preserved in _migrate_v50_*.
        -- anime_display deprecated: dropped in v55 migration via display_lookup bronze path.

        -- anime_display deprecated: display metadata comes from bronze via display_lookup.
        -- Dropped in v55 migration. See TASK_CARDS/01_schema_fix/04_anime_display_removal.md
        -- CREATE TABLE IF NOT EXISTS anime_display ( ... );
        -- CREATE INDEX IF NOT EXISTS idx_anime_display_score ON anime_display(score);

        -- ============================================================
        -- Gold layer: pre-aggregated tables for direct report reads (meta_*)
        -- Derived from feat_* tables. Reports SELECT only from here.
        -- ============================================================

        CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            description TEXT NOT NULL DEFAULT '',
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            row_count INTEGER,
            notes TEXT,
            rng_seed INTEGER,
            git_sha TEXT NOT NULL DEFAULT '',
            inputs_hash TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS meta_quality_snapshot (
            computed_at TEXT NOT NULL,
            table_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (computed_at, table_name, metric)
        );
        CREATE INDEX IF NOT EXISTS idx_quality_snapshot_metric
            ON meta_quality_snapshot(table_name, metric, computed_at);

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

        -- common: Person Parameter Card (10 axes × 3 columns + archetype)
        CREATE TABLE IF NOT EXISTS meta_common_person_parameters (
            person_id TEXT PRIMARY KEY,
            scale_reach_pct REAL,
            scale_reach_ci_low REAL,
            scale_reach_ci_high REAL,
            collab_width_pct REAL,
            collab_width_ci_low REAL,
            collab_width_ci_high REAL,
            continuity_pct REAL,
            continuity_ci_low REAL,
            continuity_ci_high REAL,
            mentor_contribution_pct REAL,
            mentor_contribution_ci_low REAL,
            mentor_contribution_ci_high REAL,
            centrality_pct REAL,
            centrality_ci_low REAL,
            centrality_ci_high REAL,
            trust_accum_pct REAL,
            trust_accum_ci_low REAL,
            trust_accum_ci_high REAL,
            role_evolution_pct REAL,
            role_evolution_ci_low REAL,
            role_evolution_ci_high REAL,
            genre_specialization_pct REAL,
            genre_specialization_ci_low REAL,
            genre_specialization_ci_high REAL,
            recent_activity_pct REAL,
            recent_activity_ci_low REAL,
            recent_activity_ci_high REAL,
            compatibility_pct REAL,
            compatibility_ci_low REAL,
            compatibility_ci_high REAL,
            archetype TEXT,
            archetype_confidence REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- policy: attrition analysis (cohort × treatment)
        CREATE TABLE IF NOT EXISTS meta_policy_attrition (
            cohort_year INTEGER NOT NULL,
            treatment TEXT NOT NULL,
            ate REAL,
            ate_ci_low REAL,
            ate_ci_high REAL,
            hazard_ratio REAL,
            hr_ci_low REAL,
            hr_ci_high REAL,
            n_treated INTEGER,
            n_control INTEGER,
            p_value REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort_year, treatment)
        );

        -- policy: labour market concentration (year × studio)
        CREATE TABLE IF NOT EXISTS meta_policy_monopsony (
            year INTEGER NOT NULL,
            studio TEXT NOT NULL,
            hhi REAL,
            hhi_star REAL,
            hhi_ci_low REAL,
            hhi_ci_high REAL,
            logit_stay_beta REAL,
            logit_stay_se REAL,
            n_persons INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, studio)
        );

        -- policy: gender survival analysis (transition_stage × cohort)
        CREATE TABLE IF NOT EXISTS meta_policy_gender (
            transition_stage TEXT NOT NULL,
            cohort TEXT NOT NULL,
            survival_prob REAL,
            survival_ci_low REAL,
            survival_ci_high REAL,
            log_rank_chi2 REAL,
            log_rank_p REAL,
            n_female INTEGER,
            n_male INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transition_stage, cohort)
        );

        -- policy: generational career survival curves (cohort × career_year_bin)
        CREATE TABLE IF NOT EXISTS meta_policy_generation (
            cohort TEXT NOT NULL,
            career_year_bin INTEGER NOT NULL,
            survival_rate REAL,
            survival_ci_low REAL,
            survival_ci_high REAL,
            n_at_risk INTEGER,
            n_events INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort, career_year_bin)
        );

        -- hr: studio benchmark (studio × year)
        CREATE TABLE IF NOT EXISTS meta_hr_studio_benchmark (
            studio TEXT NOT NULL,
            year INTEGER NOT NULL,
            r5_retention REAL,
            r5_ci_low REAL,
            r5_ci_high REAL,
            value_added REAL,
            va_ci_low REAL,
            va_ci_high REAL,
            h_score REAL,
            attraction_rate REAL,
            n_persons INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (studio, year)
        );

        -- hr: director mentoring contribution card (director_id)
        CREATE TABLE IF NOT EXISTS meta_hr_mentor_card (
            director_id TEXT PRIMARY KEY,
            mentor_score REAL,
            mentor_ci_low REAL,
            mentor_ci_high REAL,
            null_permutation_p REAL,
            n_mentees INTEGER,
            n_works INTEGER,
            archetype TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- hr: attrition risk profile (person_id) — authentication required
        CREATE TABLE IF NOT EXISTS meta_hr_attrition_risk (
            person_id TEXT PRIMARY KEY,
            predicted_risk REAL,
            risk_ci_low REAL,
            risk_ci_high REAL,
            c_index REAL,
            shap_feature1 TEXT, shap_value1 REAL,
            shap_feature2 TEXT, shap_value2 REAL,
            shap_feature3 TEXT, shap_value3 REAL,
            shap_feature4 TEXT, shap_value4 REAL,
            shap_feature5 TEXT, shap_value5 REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- hr: successor candidates (veteran × candidate, aggregate published)
        CREATE TABLE IF NOT EXISTS meta_hr_succession (
            veteran_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            successor_score REAL,
            role TEXT,
            overlap_works INTEGER,
            career_gap_years REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (veteran_id, candidate_id)
        );

        -- biz: genre whitespace map (genre × year)
        CREATE TABLE IF NOT EXISTS meta_biz_whitespace (
            genre TEXT NOT NULL,
            year INTEGER NOT NULL,
            cagr REAL,
            cagr_ci_low REAL,
            cagr_ci_high REAL,
            penetration REAL,
            whitespace_score REAL,
            n_anime INTEGER,
            n_staff INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (genre, year)
        );

        -- biz: underexposed talent (person_id)
        CREATE TABLE IF NOT EXISTS meta_biz_undervalued (
            person_id TEXT PRIMARY KEY,
            undervaluation_score REAL,
            archetype TEXT,
            network_reach REAL,
            opportunity_residual REAL,
            career_band TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- biz: trust network entry gatekeepers (gatekeeper_id)
        CREATE TABLE IF NOT EXISTS meta_biz_trust_entry (
            gatekeeper_id TEXT PRIMARY KEY,
            gatekeeper_score REAL,
            reach_score REAL,
            n_new_entrants INTEGER,
            avg_entry_speed REAL,
            community_diversity REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- biz: team templates (cluster × tier)
        CREATE TABLE IF NOT EXISTS meta_biz_team_template (
            cluster_id TEXT NOT NULL,
            tier TEXT NOT NULL,
            role_distribution TEXT,
            avg_career_years REAL,
            silhouette_score REAL,
            n_teams INTEGER,
            representative_works TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cluster_id, tier)
        );

        -- biz: independent production units (community_id)
        CREATE TABLE IF NOT EXISTS meta_biz_independent_unit (
            community_id TEXT PRIMARY KEY,
            coverage REAL,
            density REAL,
            value_generated REAL,
            n_members INTEGER,
            n_works INTEGER,
            core_studio TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS meta_entity_resolution_audit (
            person_id TEXT PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            merge_method TEXT NOT NULL CHECK (merge_method IN
                ('exact_match','cross_source','romaji','similarity','ai_assisted','manual')),
            merge_confidence REAL NOT NULL CHECK (merge_confidence BETWEEN 0 AND 1),
            merged_from_keys TEXT NOT NULL,
            merge_evidence TEXT NOT NULL,
            merged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_by TEXT,
            reviewed_at TIMESTAMP,
            FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_era_method
            ON meta_entity_resolution_audit(merge_method, merge_confidence);

        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
        CREATE INDEX IF NOT EXISTS idx_anime_year ON anime(year);
        CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id);
        CREATE INDEX IF NOT EXISTS idx_cva_character ON character_voice_actors(character_id);
        CREATE INDEX IF NOT EXISTS idx_cva_person ON character_voice_actors(person_id);
        CREATE INDEX IF NOT EXISTS idx_cva_anime ON character_voice_actors(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_anime ON anime_studios(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
        CREATE TABLE IF NOT EXISTS person_affiliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, studio_name)
        );

        CREATE INDEX IF NOT EXISTS idx_anime_relations_anime ON anime_relations(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_relations_related ON anime_relations(related_anime_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_person ON person_affiliations(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_anime ON person_affiliations(anime_id);

        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        -- ============================================================
        -- src_* table group: raw scraped data by source (Medallion Bronze layer)
        -- Fully separated from canonical tables (anime/persons/credits)
        -- ============================================================

        CREATE TABLE IF NOT EXISTS src_anilist_anime (
            anilist_id INTEGER PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            format TEXT,
            status TEXT,
            start_date TEXT,
            end_date TEXT,
            duration INTEGER,
            source TEXT,
            description TEXT,
            score REAL,
            genres TEXT DEFAULT '[]',
            tags TEXT DEFAULT '[]',
            studios TEXT DEFAULT '[]',
            synonyms TEXT DEFAULT '[]',
            cover_large TEXT,
            cover_medium TEXT,
            banner TEXT,
            popularity INTEGER,
            favourites INTEGER,
            site_url TEXT,
            mal_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_anilist_persons (
            anilist_id INTEGER PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            name_ko TEXT NOT NULL DEFAULT '',
            name_zh TEXT NOT NULL DEFAULT '',
            aliases TEXT DEFAULT '[]',
            nationality TEXT NOT NULL DEFAULT '[]',
            date_of_birth TEXT,
            age INTEGER,
            gender TEXT,
            years_active TEXT DEFAULT '[]',
            hometown TEXT,
            blood_type TEXT,
            description TEXT,
            image_large TEXT,
            image_medium TEXT,
            favourites INTEGER,
            site_url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_anilist_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anilist_anime_id INTEGER NOT NULL,
            anilist_person_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            role_raw TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_anime_id, anilist_person_id, role)
        );

        CREATE TABLE IF NOT EXISTS src_ann_anime (
            ann_id INTEGER PRIMARY KEY,
            title_en TEXT NOT NULL DEFAULT '',
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER,
            episodes INTEGER,
            format TEXT,
            genres TEXT DEFAULT '[]',
            start_date TEXT,
            end_date TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_ann_persons (
            ann_id INTEGER PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            name_ja TEXT NOT NULL DEFAULT '',
            date_of_birth TEXT,
            hometown TEXT,
            blood_type TEXT,
            website TEXT,
            description TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_ann_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ann_anime_id INTEGER NOT NULL,
            ann_person_id INTEGER NOT NULL,
            name_en TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL,
            role_raw TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ann_anime_id, ann_person_id, role)
        );

        CREATE TABLE IF NOT EXISTS src_allcinema_anime (
            allcinema_id INTEGER PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER,
            start_date TEXT,
            synopsis TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_allcinema_persons (
            allcinema_id INTEGER PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            yomigana TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_allcinema_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allcinema_anime_id INTEGER NOT NULL,
            allcinema_person_id INTEGER NOT NULL,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            job_name TEXT NOT NULL,
            job_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(allcinema_anime_id, allcinema_person_id, job_name)
        );

        CREATE TABLE IF NOT EXISTS src_seesaawiki_anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER,
            episodes INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_seesaawiki_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_src_id TEXT NOT NULL,
            person_name TEXT NOT NULL,
            role TEXT NOT NULL,
            role_raw TEXT,
            episode INTEGER DEFAULT -1,
            affiliation TEXT,
            is_company INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anime_src_id, person_name, role, episode)
        );

        CREATE TABLE IF NOT EXISTS src_keyframe_anime (
            slug TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS src_keyframe_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyframe_slug TEXT NOT NULL,
            kf_person_id INTEGER NOT NULL,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            role_ja TEXT NOT NULL,
            role_en TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(keyframe_slug, kf_person_id, role_ja, episode)
        );

        CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_anime ON src_anilist_credits(anilist_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_person ON src_anilist_credits(anilist_person_id);
        CREATE INDEX IF NOT EXISTS idx_src_ann_credits_anime ON src_ann_credits(ann_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_ann_credits_person ON src_ann_credits(ann_person_id);
        CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_anime ON src_allcinema_credits(allcinema_anime_id);
        CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_person ON src_allcinema_credits(allcinema_person_id);
        CREATE INDEX IF NOT EXISTS idx_src_seesaawiki_credits_anime ON src_seesaawiki_credits(anime_src_id);
        CREATE INDEX IF NOT EXISTS idx_src_keyframe_credits_slug ON src_keyframe_credits(keyframe_slug);

        -- ============================================================
        -- feat_* table group: derived features computed by the pipeline
        -- Distinguished from raw data tables (persons/anime/credits/studios) by naming convention
        -- ============================================================

        -- All score metrics (supersedes the scores table; scores is kept for backwards compatibility)
        CREATE TABLE IF NOT EXISTS feat_person_scores (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            -- AKM fixed effects
            person_fe REAL,
            person_fe_se REAL,
            person_fe_n_obs INTEGER,
            studio_fe_exposure REAL,
            -- BiRank / network
            birank REAL,
            patronage REAL,
            awcc REAL,
            -- IV correction factors
            dormancy REAL,
            ndi REAL,
            career_friction REAL,
            peer_boost REAL,
            -- integrated score
            iv_score REAL,
            -- percentile ranks (0–100)
            iv_score_pct REAL,
            person_fe_pct REAL,
            birank_pct REAL,
            patronage_pct REAL,
            awcc_pct REAL,
            dormancy_pct REAL,
            -- confidence intervals
            confidence REAL,
            score_range_low REAL,
            score_range_high REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Network metrics derived from graph structure
        CREATE TABLE IF NOT EXISTS feat_network (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            degree_centrality REAL,
            betweenness_centrality REAL,
            closeness_centrality REAL,
            eigenvector_centrality REAL,
            hub_score REAL,
            n_collaborators INTEGER,
            n_unique_anime INTEGER,
            bridge_score REAL,
            n_bridge_communities INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Metrics derived from career trajectory
        CREATE TABLE IF NOT EXISTS feat_career (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            first_year INTEGER,
            latest_year INTEGER,
            active_years INTEGER,
            total_credits INTEGER,
            highest_stage INTEGER,
            primary_role TEXT,
            career_track TEXT,
            peak_year INTEGER,
            peak_credits INTEGER,
            growth_trend TEXT,
            growth_score REAL,
            activity_ratio REAL,
            recent_credits INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Genre × person affinity scores (multiple rows per person)
        CREATE TABLE IF NOT EXISTS feat_genre_affinity (
            person_id TEXT NOT NULL,
            genre TEXT NOT NULL,
            run_id INTEGER REFERENCES pipeline_runs(id),
            affinity_score REAL,
            work_count INTEGER,
            PRIMARY KEY (person_id, genre)
        );

        -- Individual contribution profile (Layer 2)
        CREATE TABLE IF NOT EXISTS feat_contribution (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            peer_percentile REAL,
            opportunity_residual REAL,
            consistency REAL,
            independent_value REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_feat_person_scores_iv ON feat_person_scores(iv_score);
        CREATE INDEX IF NOT EXISTS idx_feat_career_first_year ON feat_career(first_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_track ON feat_career(career_track);
        CREATE INDEX IF NOT EXISTS idx_feat_genre_genre ON feat_genre_affinity(genre);

        -- Credit activity pattern (gaps, activity density, hiatus history)
        -- abs_quarter = year * 4 + (quarter - 1)  e.g. 2020Q1 → 8080
        CREATE TABLE IF NOT EXISTS feat_credit_activity (
            person_id TEXT PRIMARY KEY,
            first_abs_quarter INTEGER,
            last_abs_quarter INTEGER,
            activity_span_quarters INTEGER,
            active_quarters INTEGER,
            density REAL,
            n_gaps INTEGER,
            mean_gap_quarters REAL,
            median_gap_quarters REAL,
            min_gap_quarters INTEGER,
            max_gap_quarters INTEGER,
            std_gap_quarters REAL,
            consecutive_quarters INTEGER,
            consecutive_rate REAL,
            n_hiatuses INTEGER,
            longest_hiatus_quarters INTEGER,
            quarters_since_last_credit INTEGER,
            active_years INTEGER,
            n_year_gaps INTEGER,
            mean_year_gap REAL,
            max_year_gap INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_span
            ON feat_credit_activity(activity_span_quarters);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_last
            ON feat_credit_activity(last_abs_quarter);

        -- Annual aggregation by career year × role category
        -- career_year = credit_year - first_credit_year (0 = debut year)
        CREATE TABLE IF NOT EXISTS feat_career_annual (
            person_id TEXT NOT NULL,
            career_year INTEGER NOT NULL,
            credit_year INTEGER NOT NULL,
            n_works INTEGER NOT NULL DEFAULT 0,
            n_credits INTEGER NOT NULL DEFAULT 0,
            n_roles INTEGER NOT NULL DEFAULT 0,
            works_direction INTEGER NOT NULL DEFAULT 0,
            works_animation_supervision INTEGER NOT NULL DEFAULT 0,
            works_animation INTEGER NOT NULL DEFAULT 0,
            works_design INTEGER NOT NULL DEFAULT 0,
            works_technical INTEGER NOT NULL DEFAULT 0,
            works_art INTEGER NOT NULL DEFAULT 0,
            works_sound INTEGER NOT NULL DEFAULT 0,
            works_writing INTEGER NOT NULL DEFAULT 0,
            works_production INTEGER NOT NULL DEFAULT 0,
            works_production_management INTEGER NOT NULL DEFAULT 0,
            works_finishing INTEGER NOT NULL DEFAULT 0,
            works_editing INTEGER NOT NULL DEFAULT 0,
            works_settings INTEGER NOT NULL DEFAULT 0,
            works_other INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (person_id, career_year)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_year
            ON feat_career_annual(career_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_credit_year
            ON feat_career_annual(credit_year);

        -- Annual BiRank snapshot per person (v42)
        -- Stored from 1980 onwards only (earlier graphs are too small for meaningful scores)
        -- birank: 0-100 normalised score among all persons in that year
        -- raw_pagerank: raw PageRank value (before normalisation)
        -- graph_size: number of person nodes in the cumulative graph for that year
        -- n_credits_cumulative: cumulative industry credit count up to that year (population-size proxy)
        CREATE TABLE IF NOT EXISTS feat_birank_annual (
            person_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            birank REAL NOT NULL,
            raw_pagerank REAL,
            graph_size INTEGER,
            n_credits_cumulative INTEGER,
            PRIMARY KEY (person_id, year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_birank_annual_year
            ON feat_birank_annual(year);

        -- birank_compute_state: fingerprint of input data used for BiRank computation (per year)
        -- Used for change detection: recompute if credit/anime/person count differs from previous run
        CREATE TABLE IF NOT EXISTS birank_compute_state (
            year INTEGER PRIMARY KEY,
            credit_count INTEGER NOT NULL,
            anime_count INTEGER NOT NULL,
            person_count INTEGER NOT NULL,
            computed_at REAL NOT NULL    -- unix timestamp
        );

        -- Annual studio affiliation aggregation per person
        -- Joins anime_studios + credits to count works per studio per year
        CREATE TABLE IF NOT EXISTS feat_studio_affiliation (
            person_id TEXT NOT NULL,
            credit_year INTEGER NOT NULL,
            studio_id TEXT NOT NULL,
            studio_name TEXT NOT NULL DEFAULT '',
            n_works INTEGER NOT NULL DEFAULT 0,    -- number of works at this studio
            n_credits INTEGER NOT NULL DEFAULT 0,  -- credit row count
            is_main_studio INTEGER NOT NULL DEFAULT 0, -- main studio flag (anime_studios.is_main)
            PRIMARY KEY (person_id, credit_year, studio_id)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_person
            ON feat_studio_affiliation(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_studio
            ON feat_studio_affiliation(studio_id, credit_year);

        -- Per-person × work × role score contribution estimates
        -- production_scale: AKM outcome variable (fully computed)
        -- edge_weight: graph edge contribution (fully computed)
        -- iv_contrib_est: IV score share estimate (edge_weight_share × iv_score)
        CREATE TABLE IF NOT EXISTS feat_credit_contribution (
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER,
            -- AKM outcome variable (log production scale)
            production_scale REAL,
            -- graph edge contribution
            role_weight REAL,
            episode_coverage REAL,
            dur_mult REAL,
            edge_weight REAL,
            -- IV score share (edge_weight / person's total_edge_weight × iv_score)
            edge_weight_share REAL,
            iv_contrib_est REAL,
            PRIMARY KEY (person_id, anime_id, role)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_anime
            ON feat_credit_contribution(anime_id);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_year
            ON feat_credit_contribution(credit_year);

        -- Per-person work contribution summary
        CREATE TABLE IF NOT EXISTS feat_person_work_summary (
            person_id TEXT PRIMARY KEY,
            n_distinct_works INTEGER,
            total_production_scale REAL,
            mean_production_scale REAL,
            max_production_scale REAL,
            best_work_anime_id TEXT,
            total_edge_weight REAL,
            mean_edge_weight_per_work REAL,
            max_edge_weight REAL,
            top_contrib_anime_id TEXT,
            total_iv_contrib_est REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Per-work team statistics (v38) + scale tier (v44)
        CREATE TABLE IF NOT EXISTS feat_work_context (
            anime_id TEXT PRIMARY KEY,
            credit_year INTEGER,
            n_staff INTEGER,
            n_distinct_roles INTEGER,
            n_direction INTEGER,
            n_animation_supervision INTEGER,
            n_animation INTEGER,
            n_design INTEGER,
            n_technical INTEGER,
            n_art INTEGER,
            n_sound INTEGER,
            n_writing INTEGER,
            n_production INTEGER,
            n_other INTEGER,
            mean_career_year REAL,
            median_career_year REAL,
            max_career_year INTEGER,
            production_scale REAL,
            difficulty_score REAL,
            -- Work scale tier (v44): computed from format + episodes + duration only
            scale_tier INTEGER,
            scale_label TEXT,
            scale_raw REAL,
            format_group TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_work_context_year
            ON feat_work_context(credit_year);
        -- Note: idx_feat_work_context_tier added by v44 migration

        -- Per-person role-category time-series progression (v39)
        CREATE TABLE IF NOT EXISTS feat_person_role_progression (
            person_id TEXT NOT NULL,
            role_category TEXT NOT NULL,
            first_year INTEGER,
            last_year INTEGER,
            peak_year INTEGER,
            n_works INTEGER,
            n_credits INTEGER,
            career_year_first INTEGER,
            still_active INTEGER,
            PRIMARY KEY (person_id, role_category)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_person
            ON feat_person_role_progression(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_category
            ON feat_person_role_progression(role_category);

        -- Causal inference results (v40)
        CREATE TABLE IF NOT EXISTS feat_causal_estimates (
            person_id TEXT PRIMARY KEY,
            peer_effect_boost REAL,
            career_friction REAL,
            era_fe REAL,
            era_deflated_iv REAL,
            opportunity_residual REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Cluster memberships — multiple dimensions collapsed to one row (v41)
        CREATE TABLE IF NOT EXISTS feat_cluster_membership (
            person_id TEXT PRIMARY KEY,
            -- Graph community detection (Phase 4: Louvain / Leiden)
            community_id INTEGER,
            -- Career track (Phase 6: rule-based classification)
            career_track TEXT,
            -- Growth trend (Phase 9: growth analysis)
            growth_trend TEXT,
            -- Primary studio cluster (K-Means on studio features)
            studio_cluster_id INTEGER,
            studio_cluster_name TEXT,
            -- Co-credit group (Phase 9: cooccurrence_groups)
            cooccurrence_group_id INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_community
            ON feat_cluster_membership(community_id);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_career_track
            ON feat_cluster_membership(career_track);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_growth
            ON feat_cluster_membership(growth_trend);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_studio
            ON feat_cluster_membership(studio_cluster_id);
    """)
    finally:
        # Ensure all transactions are committed before restoring isolation level
        conn.commit()
        # Restore isolation level and enable WAL
        conn.isolation_level = old_isolation
        conn.execute("PRAGMA journal_mode=WAL")


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the current schema version."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except sqlite3.OperationalError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    """Set the schema version."""
    conn.execute(
        """INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
        (str(version),),
    )


def _run_migrations(conn: sqlite3.Connection) -> None:
    """No-op: target schema is created by init_db_v2 directly; no incremental migrations."""
    return

    # Legacy migration table — kept for reference, never executed
    current = get_schema_version(conn)
    if current >= SCHEMA_VERSION:
        return

    migrations = {
        1: _migrate_v1_add_score_history,
        2: _migrate_v2_add_score_history_index,
        3: _migrate_v3_add_pipeline_meta,
        4: _migrate_v4_add_studio_column,
        5: _migrate_v5_add_person_metadata,
        6: _migrate_v6_add_anime_metadata,
        7: _migrate_v7_drop_credits_fk,
        8: _migrate_v8_raw_role_unique_and_anime_extra,
        9: _migrate_v9_add_studios_tables,
        10: _migrate_v10_schema_cleanup,
        11: _migrate_v11_add_madb_ids,
        12: _migrate_v12_add_person_fetch_status,
        13: _migrate_v13_add_structural_score_columns,
        14: _migrate_v14_drop_legacy_score_columns,
        15: _migrate_v15_add_va_scores,
        16: _migrate_v16_add_person_affiliations,
        17: _migrate_v17_add_llm_decisions,
        18: _migrate_v18_add_score_history_quarter,
        19: _migrate_v19_add_anime_quarter,
        20: _migrate_v20_add_credit_temporal,
        21: _migrate_v21_enhanced_title_matching,
        22: _migrate_v22_deep_title_matching,
        23: _migrate_v23_fuzzy_match_rules,
        24: _migrate_v24_improved_matching,
        25: _migrate_v25_kanji_hira_matching,
        26: _migrate_v26_anime_scale_classification,
        27: _migrate_v27_normalize_legacy_roles,
        28: _migrate_v28_add_career_track,
        29: _migrate_v29_add_feat_tables,
        30: _migrate_v30_add_feat_credit_activity,
        31: _migrate_v31_add_feat_career_annual,
        32: _migrate_v32_add_feat_studio_affiliation,
        33: _migrate_v33_add_feat_credit_contribution,
        34: _migrate_v34_add_agg_milestones,
        35: _migrate_v35_add_agg_director_circles,
        36: _migrate_v36_add_feat_mentorships,
        37: _migrate_v37_credit_contribution_career_year,
        38: _migrate_v38_add_feat_work_context,
        39: _migrate_v39_add_feat_person_role_progression,
        40: _migrate_v40_add_feat_causal_estimates,
        41: _migrate_v41_add_feat_cluster_membership,
        42: _migrate_v42_add_feat_birank_annual,
        43: _migrate_v43_add_birank_compute_state,
        44: _migrate_v44_add_work_scale_tier,
        45: _migrate_v45_add_feat_career_gaps,
        46: _migrate_v46_add_ann_ids,
        47: _migrate_v47_add_allcinema_ids,
        48: _migrate_v48_add_source_tables,
        49: _migrate_v49_add_silver_layer,
        50: _migrate_v50_canonical_silver,
        51: _migrate_v51_meta_lineage_and_audit,
        52: _migrate_v52_add_calc_execution_records,
        53: _migrate_v53_slim_anime_table,
        54: _migrate_v54_drop_legacy_credit_source,
        55: _migrate_v54_to_v55,
        56: _migrate_v56_multilang_names,
    }

    for version in range(current + 1, SCHEMA_VERSION + 1):
        migration_fn = migrations.get(version)
        if migration_fn:
            logger.info("running_migration", version=version)
            migration_fn(conn)
        _set_schema_version(conn, version)
        conn.commit()

    logger.info("schema_up_to_date", version=SCHEMA_VERSION)


def _migrate_v1_add_score_history(conn: sqlite3.Connection) -> None:
    """v1: add score_history table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS score_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            person_fe REAL DEFAULT 0.0,
            studio_fe_exposure REAL DEFAULT 0.0,
            birank REAL DEFAULT 0.0,
            patronage REAL DEFAULT 0.0,
            dormancy REAL DEFAULT 1.0,
            awcc REAL DEFAULT 0.0,
            iv_score REAL DEFAULT 0.0,
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v2_add_score_history_index(conn: sqlite3.Connection) -> None:
    """v2: add indexes to score_history."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_score_history_person ON score_history(person_id);
        CREATE INDEX IF NOT EXISTS idx_score_history_run ON score_history(run_at);
    """)


def _migrate_v3_add_pipeline_meta(conn: sqlite3.Connection) -> None:
    """v3: add pipeline_runs table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            credit_count INTEGER DEFAULT 0,
            person_count INTEGER DEFAULT 0,
            elapsed_seconds REAL DEFAULT 0.0,
            mode TEXT DEFAULT 'full'
        );
    """)


def _migrate_v4_add_studio_column(conn: sqlite3.Connection) -> None:
    """v4: add studio column to anime table."""
    try:
        conn.execute("ALTER TABLE anime ADD COLUMN studio TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists


def _migrate_v5_add_person_metadata(conn: sqlite3.Connection) -> None:
    """v5: add metadata columns to persons table."""
    new_columns = [
        "image_large TEXT",
        "image_medium TEXT",
        "image_large_path TEXT",
        "image_medium_path TEXT",
        "date_of_birth TEXT",
        "age INTEGER",
        "gender TEXT",
        "years_active TEXT DEFAULT '[]'",
        "hometown TEXT",
        "blood_type TEXT",
        "description TEXT",
        "favourites INTEGER",
        "site_url TEXT",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE persons ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v6_add_anime_metadata(conn: sqlite3.Connection) -> None:
    """v6: add metadata columns to anime table."""
    new_columns = [
        "cover_large TEXT",
        "cover_extra_large TEXT",
        "cover_medium TEXT",
        "banner TEXT",
        "cover_large_path TEXT",
        "banner_path TEXT",
        "description TEXT",
        "format TEXT",
        "status TEXT",
        "start_date TEXT",
        "end_date TEXT",
        "duration INTEGER",
        "source TEXT",
        "genres TEXT DEFAULT '[]'",
        "tags TEXT DEFAULT '[]'",
        "popularity_rank INTEGER",
        "favourites INTEGER",
        "studios TEXT DEFAULT '[]'",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v7_drop_credits_fk(conn: sqlite3.Connection) -> None:
    """v7: drop foreign key constraints from credits table.

    In the two-phase pipeline credits are written before persons, so FK constraints
    would cause IntegrityError. SQLite cannot drop FKs via ALTER TABLE, so the table
    must be recreated.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );
        INSERT OR IGNORE INTO credits_new
            SELECT id, person_id, anime_id, role, NULL as raw_role, episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
    """)


def _migrate_v8_raw_role_unique_and_anime_extra(conn: sqlite3.Connection) -> None:
    """v8: change credits UNIQUE to raw_role basis + add extra anime columns.

    - UNIQUE(person_id, anime_id, role, episode) → UNIQUE(person_id, anime_id, raw_role, episode)
    - raw_role changed to NOT NULL DEFAULT ''
    - additional metadata columns added to anime table
    """
    # 1. recreate credits table (raw_role-based UNIQUE)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            UNIQUE(person_id, anime_id, raw_role, episode)
        );
        INSERT OR IGNORE INTO credits_new (person_id, anime_id, role, raw_role, episode, source)
            SELECT person_id, anime_id, role, COALESCE(raw_role, ''), episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
    """)

    # 2. add extra columns to anime table
    new_columns = [
        "synonyms TEXT DEFAULT '[]'",
        "mean_score INTEGER",
        "country_of_origin TEXT",
        "is_licensed INTEGER",
        "is_adult INTEGER",
        "hashtag TEXT",
        "site_url TEXT",
        "trailer_url TEXT",
        "trailer_site TEXT",
        "relations_json TEXT",
        "external_links_json TEXT",
        "rankings_json TEXT",
    ]
    for column in new_columns:
        try:
            conn.execute(f"ALTER TABLE anime ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass  # Column already exists


def _migrate_v9_add_studios_tables(conn: sqlite3.Connection) -> None:
    """v9: add studios + anime_studios tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS studios (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            anilist_id INTEGER,
            is_animation_studio INTEGER,
            favourites INTEGER,
            site_url TEXT,
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS anime_studios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            studio_id TEXT NOT NULL,
            is_main INTEGER NOT NULL DEFAULT 0,
            UNIQUE(anime_id, studio_id)
        );

        CREATE INDEX IF NOT EXISTS idx_anime_studios_anime ON anime_studios(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
    """)


def _migrate_v10_schema_cleanup(conn: sqlite3.Connection) -> None:
    """v10: schema cleanup.

    - drop anime.studio (singular) column
    - change credits.episode default from -1 → NULL
    - add updated_at column (persons, anime, credits, characters, character_voice_actors, studios)
    - add persons.canonical_id index
    - add anime_relations table
    """
    # 1. recreate credits table (add updated_at)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, raw_role, episode)
        );
        INSERT OR IGNORE INTO credits_new (person_id, anime_id, role, raw_role, episode, source)
            SELECT person_id, anime_id, role, COALESCE(raw_role, ''),
                   episode, source FROM credits;
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
    """)

    # 2. drop anime.studio (singular) column
    try:
        conn.execute("ALTER TABLE anime DROP COLUMN studio")
    except sqlite3.OperationalError:
        pass  # Column doesn't exist or SQLite too old

    # 3. add updated_at column
    for table in [
        "persons",
        "anime",
        "characters",
        "character_voice_actors",
        "studios",
    ]:
        try:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )
        except sqlite3.OperationalError:
            pass

    # 4. persons.canonical_id index
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id)"
    )

    # 5. anime_relations table
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anime_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id TEXT NOT NULL,
            related_anime_id TEXT NOT NULL,
            relation_type TEXT NOT NULL DEFAULT '',
            related_title TEXT NOT NULL DEFAULT '',
            related_format TEXT,
            UNIQUE(anime_id, related_anime_id, relation_type)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_relations_anime ON anime_relations(anime_id);
        CREATE INDEX IF NOT EXISTS idx_anime_relations_related ON anime_relations(related_anime_id);
    """)


def _migrate_v11_add_madb_ids(conn: sqlite3.Connection) -> None:
    """v11: add madb_id column to anime/persons tables."""
    for table in ["anime", "persons"]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN madb_id TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_madb_id ON anime(madb_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_madb_id ON persons(madb_id)")


def _migrate_v12_add_person_fetch_status(conn: sqlite3.Connection) -> None:
    """v12: add table for recording API fetch failures (404, etc.)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS person_fetch_status (
            anilist_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'anilist',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v13_add_structural_score_columns(conn: sqlite3.Connection) -> None:
    """v13: 8-component structural estimation columns to scores and score_history."""
    # Add new columns to scores table
    for col in [
        "person_fe REAL DEFAULT 0.0",
        "studio_fe_exposure REAL DEFAULT 0.0",
        "birank REAL DEFAULT 0.0",
        "patronage REAL DEFAULT 0.0",
        "dormancy REAL DEFAULT 1.0",
        "awcc REAL DEFAULT 0.0",
        "iv_score REAL DEFAULT 0.0",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE scores ADD COLUMN {col}")
        except sqlite3.OperationalError:
            # Column already exists
            logger.debug("column_already_exists", table="scores", column=col_name)

    # Add new columns to score_history table
    for col in [
        "person_fe REAL DEFAULT 0.0",
        "studio_fe_exposure REAL DEFAULT 0.0",
        "birank REAL DEFAULT 0.0",
        "patronage REAL DEFAULT 0.0",
        "dormancy REAL DEFAULT 1.0",
        "awcc REAL DEFAULT 0.0",
        "iv_score REAL DEFAULT 0.0",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE score_history ADD COLUMN {col}")
        except sqlite3.OperationalError:
            logger.debug(
                "column_already_exists", table="score_history", column=col_name
            )


def _migrate_v14_drop_legacy_score_columns(conn: sqlite3.Connection) -> None:
    """v14: Drop legacy authority/trust/skill/composite columns from scores/score_history.

    These are replaced by the 8-component structural fields (person_fe, birank, etc.).
    Uses ALTER TABLE DROP COLUMN (SQLite 3.35.0+, 2021-03-12).
    """
    for table in ["scores", "score_history"]:
        for col in ["authority", "trust", "skill", "composite"]:
            try:
                conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
            except sqlite3.OperationalError:
                # Column doesn't exist (fresh DB) or SQLite too old
                logger.debug("drop_column_skipped", table=table, column=col)


def _migrate_v15_add_va_scores(conn: sqlite3.Connection) -> None:
    """v15: Add va_scores table for voice actor evaluation system."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS va_scores (
            person_id TEXT PRIMARY KEY,
            person_fe REAL DEFAULT 0.0,
            sd_fe_exposure REAL DEFAULT 0.0,
            birank REAL DEFAULT 0.0,
            patronage REAL DEFAULT 0.0,
            trust REAL DEFAULT 0.0,
            dormancy REAL DEFAULT 1.0,
            awcc REAL DEFAULT 0.0,
            va_iv_score REAL DEFAULT 0.0,
            character_diversity_index REAL DEFAULT 0.0,
            main_role_count INTEGER DEFAULT 0,
            supporting_role_count INTEGER DEFAULT 0,
            total_characters INTEGER DEFAULT 0,
            casting_tier TEXT DEFAULT 'newcomer',
            replacement_difficulty REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v16_add_person_affiliations(conn: sqlite3.Connection) -> None:
    """v16: add person_affiliations table (person × work × affiliated studio)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS person_affiliations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            studio_name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, studio_name)
        );
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_person
            ON person_affiliations(person_id);
        CREATE INDEX IF NOT EXISTS idx_person_affiliations_anime
            ON person_affiliations(anime_id);
    """)


def _migrate_v17_add_llm_decisions(conn: sqlite3.Connection) -> None:
    """v17: add LLM decision result table (org classification, name normalization, identity matching)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS llm_decisions (
            name TEXT NOT NULL,
            task TEXT NOT NULL,
            result_json TEXT NOT NULL,
            model TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (name, task)
        );
        CREATE INDEX IF NOT EXISTS idx_llm_decisions_task
            ON llm_decisions(task);
    """)


def _migrate_v18_add_score_history_quarter(conn: sqlite3.Connection) -> None:
    """v18: add year/quarter columns to score_history (for quarterly aggregation)."""
    for col in [
        "year INTEGER",
        "quarter INTEGER",
    ]:
        col_name = col.split()[0]
        try:
            conn.execute(f"ALTER TABLE score_history ADD COLUMN {col}")
        except sqlite3.OperationalError:
            logger.debug(
                "column_already_exists", table="score_history", column=col_name
            )
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_score_history_yq
            ON score_history(year, quarter);
    """)


def _migrate_v19_add_anime_quarter(conn: sqlite3.Connection) -> None:
    """v19: add quarter column to anime and compute it from season / start_date.

    Also backfills year=NULL entries from SeesaaWiki/Keyframes via AniList title matching.

    Priority: season → month of start_date → NULL (unknown).
    season_to_quarter: winter=1, spring=2, summer=3, fall=4.
    start_date (YYYY-MM-DD) month: 1-3=Q1, 4-6=Q2, 7-9=Q3, 10-12=Q4.
    """

    try:
        conn.execute("ALTER TABLE anime ADD COLUMN quarter INTEGER")
    except sqlite3.OperationalError:
        logger.debug("column_already_exists", table="anime", column="quarter")

    # Phase 1: compute quarter directly from season / start_date
    conn.execute("""
        UPDATE anime SET quarter = CASE
            WHEN LOWER(season) = 'winter' THEN 1
            WHEN LOWER(season) = 'spring' THEN 2
            WHEN LOWER(season) = 'summer' THEN 3
            WHEN LOWER(season) = 'fall'   THEN 4
            WHEN start_date IS NOT NULL AND LENGTH(start_date) >= 7
                THEN (CAST(SUBSTR(start_date, 6, 2) AS INTEGER) - 1) / 3 + 1
            ELSE NULL
        END
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_quarter ON anime(year, quarter)")

    phase1 = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE quarter IS NOT NULL"
    ).fetchone()[0]
    logger.info("anime_quarter_phase1", count=phase1)

    # Phase 2: backfill year/quarter/format via AniList title matching for SeesaaWiki/Keyframes entries
    _backfill_from_anilist_titles(conn)


def _backfill_from_anilist_titles(conn: sqlite3.Connection) -> None:
    """Backfill year=NULL non-AniList entries via AniList title matching.

    Match strategy (in priority order):
      1. title_ja exact match
      2. NFKC-normalized + symbol/whitespace-stripped match
      3. Bracket annotation stripped (e.g. "（TV第2作）", "(第3期)") match
         → Use year hint from annotation when multiple series share the same name

    Columns backfilled: year, quarter, format, season, start_date
    """
    import re
    import unicodedata

    # --- targets: non-AniList entries with year=NULL ---
    targets = conn.execute(
        "SELECT id, title_ja, title_en FROM anime "
        "WHERE year IS NULL AND id NOT LIKE 'anilist:%' AND title_ja != ''"
    ).fetchall()
    if not targets:
        return

    # --- AniList reference data ---
    refs = conn.execute(
        "SELECT id, title_ja, title_en, year, quarter, season, start_date, format "
        "FROM anime WHERE id LIKE 'anilist:%' AND year IS NOT NULL AND title_ja != ''"
    ).fetchall()

    def _normalize(s: str) -> str:
        """NFKC normalization + symbol/whitespace removal + lowercase."""
        import html

        # decode HTML entities: &#9825; → ♡
        s = html.unescape(s)
        s = unicodedata.normalize("NFKC", s).lower().strip()
        # normalize 〈〉 to <> so they are stripped by the regex below
        s = s.replace("\u3008", "<").replace("\u3009", ">")
        s = re.sub(
            r"[\s\u3000・\-–—―~〜!！?？、。,.'\"()\（\）\[\]【】「」『』《》☆★♪♡♥♡−＝<>〈〉◆◇#\$%@&*]+",
            "",
            s,
        )
        return s

    def _strip_movie_prefix(s: str) -> str:
        """Remove media-format prefixes like 「映画」 and 「劇場版」."""
        s = re.sub(r"^(?:映画|劇場版|劇場版映画|特別版|OVA|OAD)\s*", "", s)
        return s.strip()

    def _sorted_word_key(s: str) -> str:
        """Generate a key by sorting words (for word-order-agnostic matching).
        Example: 'SHADOW SKILL 影技' → '影技shadowskill'"""
        # split on spaces/symbols and sort
        words = re.split(r"[\s・\-_/]+", s)
        return "".join(sorted(w for w in words if w))

    def _normalize_roman_greek(s: str) -> str:
        """Convert Greek letters and Roman numerals to ASCII."""
        for old, new in [
            ("ΖΖ", "ZZ"),
            ("Ζ", "Z"),
            ("Ⅲ", "III"),
            ("Ⅱ", "II"),
            ("Ⅰ", "I"),
            ("Ⅳ", "IV"),
            ("Ⅴ", "V"),
            ("Ⅵ", "VI"),
            ("Ⅶ", "VII"),
            ("Ⅷ", "VIII"),
            ("Ⅸ", "IX"),
            ("Ⅹ", "X"),
        ]:
            s = s.replace(old, new)
        return s

    def _kanji_hira_key(s: str) -> str:
        """Extract only kanji and hiragana (removing katakana, ASCII, symbols).

        Strips both the English form (WEED) and katakana transliteration (ウィード),
        so titles that share only their kanji+hiragana portion are treated as identical.
        """
        s = unicodedata.normalize("NFKC", s)
        return "".join(
            c
            for c in s
            if "\u4e00" <= c <= "\u9fff"  # CJK unified ideographs
            or "\u3400" <= c <= "\u4dbf"  # CJK extension A
            or "\u3040" <= c <= "\u309f"  # hiragana
        )

    def _strip_reading_parens(s: str) -> str:
        """Remove katakana/ASCII reading parentheses: GATE（ゲート）→ GATE."""
        return re.sub(r"[（(][ァ-ヾA-Za-zＡ-Ｚ　\s]+[）)]", "", s).strip()

    def _strip_furigana(s: str) -> str:
        """Remove furigana parentheses on the AniList side: 魔法騎士（マジックナイト）→ 魔法騎士."""
        return re.sub(r"（[ぁ-ヾァ-ヾA-Za-zＡ-Ｚ\w]+）", "", s).strip()

    def _strip_english_prefix(s: str) -> str:
        """Remove English prefix before Japanese characters: MAJOR メジャー → メジャー."""
        return re.sub(r"^[A-Za-z0-9\s.\-/!?&:'+]+(?=[ぁ-ヾァ-ヾ一-龥])", "", s).strip()

    def _strip_annotations(s: str) -> str:
        """Remove bracket annotations: （TV第2作）, (第3期), （2020年）, etc."""
        s = re.sub(r"[（(][^）)]*(?:シリーズ|期|作|版|年)[^）)]*[）)]", "", s)
        # also strip trailing year suffixes: "ぼのぼの (2016)" → "ぼのぼの"
        s = re.sub(r"\s*[（(]\d{4}[）)]$", "", s)
        return s.strip()

    def _extract_year_hint(title: str) -> int | None:
        """Extract year hint from title: （2020年）→ 2020."""
        m = re.search(r"[（(](\d{4})年?[）)]", title)
        return int(m.group(1)) if m else None

    def _strip_tv_prefix(s: str) -> str:
        """Remove TV prefix: 'TVそれいけ!アンパンマン' → 'それいけ!アンパンマン'."""
        return re.sub(r"^TV\s*", "", s)

    # --- build AniList lookup indexes ---
    # exact title_ja → list of refs
    idx_exact: dict[str, list] = {}
    # normalized → list of refs
    idx_norm: dict[str, list] = {}
    # stripped + normalized → list of refs
    idx_stripped: dict[str, list] = {}
    # normalized title_en → list of refs (for English title cross-matching)
    idx_en: dict[str, list] = {}
    # (normalized_ja, entry) for containment lookups
    ref_entries_for_containment: list[tuple[str, tuple]] = []
    # furigana-stripped AniList title → list of refs
    idx_furigana: dict[str, list] = {}
    # movie-prefix-stripped → list of refs (strips 「映画」/「劇場版」)
    idx_movie: dict[str, list] = {}
    # sorted-word key → list of refs (word-order-agnostic matching)
    idx_sorted: dict[str, list] = {}
    # kanji+hiragana key → list of refs (matches when only kanji part agrees, ignoring English/katakana)
    idx_khk: dict[str, list] = {}

    for row in refs:
        aid, ja, en, yr, q, sea, sd, fmt = row
        entry = (aid, yr, q, sea, sd, fmt, ja)

        idx_exact.setdefault(ja, []).append(entry)

        nk = _normalize(ja)
        if nk:
            idx_norm.setdefault(nk, []).append(entry)
            # record entry for containment matching
            ref_entries_for_containment.append((nk, entry))

        sk = _normalize(_strip_annotations(ja))
        if sk and sk != nk:
            idx_stripped.setdefault(sk, []).append(entry)
        # if stripped key equals norm key it is already in idx_norm; no need to duplicate
        if sk and sk not in idx_stripped:
            idx_stripped.setdefault(sk, []).append(entry)

        # title_en index (English title cross-matching)
        if en:
            enk = _normalize(en)
            if enk:
                idx_en.setdefault(enk, []).append(entry)

        # furigana-stripped index: 魔法騎士（マジックナイト）レイアース → 魔法騎士レイアース
        fk = _normalize(_strip_furigana(ja))
        if fk and fk != nk:
            idx_furigana.setdefault(fk, []).append(entry)

        # movie/film-prefix-stripped index
        mk = _normalize(_strip_movie_prefix(ja))
        if mk and mk != nk:
            idx_movie.setdefault(mk, []).append(entry)

        # word-sorted index (SHADOW SKILL 影技 ↔ 影技 SHADOW SKILL)
        # only when 5+ normalized chars and 2+ words
        words = [w for w in re.split(r"[\s\u3000・\-_/]+", ja) if w]
        if len(words) >= 2 and len(nk) >= 5:
            swk = _normalize(_sorted_word_key(ja))
            if swk and swk != nk:
                idx_sorted.setdefault(swk, []).append(entry)

        # kanji+hiragana key index (e.g. English WEED ↔ katakana ウィード)
        khk = _kanji_hira_key(ja)
        if len(khk) >= 4:
            idx_khk.setdefault(khk, []).append(entry)

    def _pick_best(candidates: list, year_hint: int | None = None) -> tuple | None:
        """Select the best match from multiple candidates."""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        # if a year hint is available, pick the closest match
        if year_hint:
            return min(candidates, key=lambda c: abs((c[1] or 0) - year_hint))
        # otherwise pick the most recent (usually the correct entry for long-running series)
        return max(candidates, key=lambda c: c[1] or 0)

    # --- run matching ---
    updates: list[tuple] = []  # (year, quarter, season, start_date, format, target_id)

    for sid, sja, sen in targets:
        match = None
        year_hint = _extract_year_hint(sja)

        # 1. exact match
        if sja in idx_exact:
            match = _pick_best(idx_exact[sja], year_hint)

        # 2. normalized match
        if not match:
            nk = _normalize(sja)
            if nk in idx_norm:
                match = _pick_best(idx_norm[nk], year_hint)

        # 3. annotation-stripped + normalized match
        if not match:
            sk = _normalize(_strip_annotations(sja))
            if sk in idx_norm:
                match = _pick_best(idx_norm[sk], year_hint)
            elif sk in idx_stripped:
                match = _pick_best(idx_stripped[sk], year_hint)

        # 4. base-title match (main title before first space only)
        #    quality gate: base title 5+ chars, few candidates
        if not match:
            _base = re.sub(r"[（(][^）)]*[）)]", "", sja).strip()
            _parts = re.split(r"[\s　]+", _base)
            _base_title = _parts[0] if len(_parts) > 1 else _base
            _bk = _normalize(_base_title)
            # skip titles too short (false-match risk) and category pages
            _SKIP_PATTERNS = {"年代", "シリーズリスト", "アニバーサリー", "music"}
            if (
                len(_bk) >= 5
                and not any(p in sja for p in _SKIP_PATTERNS)
                and _bk in idx_norm
            ):
                candidates = idx_norm[_bk]
                # skip if too many candidates (generic title)
                if len(candidates) <= 10:
                    match = _pick_best(candidates, year_hint)

        # 5. title_en cross-match (SeesaaWiki title_ja ↔ AniList title_en)
        #    e.g. NANA, D.Gray-man, CLANNAD, MAJOR, BLACK CAT
        if not match:
            nk = _normalize(sja)
            if nk in idx_en:
                match = _pick_best(idx_en[nk], year_hint)

        # 6. TV-prefix-stripped match
        #    e.g. TVそれいけ!アンパンマン → それいけ!アンパンマン
        if not match and (sja.startswith("TV") or sja.startswith("ＴＶ")):
            tv_stripped = _normalize(_strip_tv_prefix(sja))
            if tv_stripped and tv_stripped in idx_norm:
                match = _pick_best(idx_norm[tv_stripped], year_hint)

        # 7. reverse-containment match (AniList title_ja contains SeesaaWiki title_ja)
        #    e.g. "NANA" → "NANA-ナナ-", "D.Gray-man" → "D.Gray-man ディー・グレイマン"
        #    quality gate: 5+ normalized chars, ≤10 candidates
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 5:
                rev_candidates = [
                    entry
                    for ref_nk, entry in ref_entries_for_containment
                    if ref_nk != nk and nk in ref_nk
                ]
                if 0 < len(rev_candidates) <= 10:
                    match = _pick_best(rev_candidates, year_hint)

        # 8. target title_en → AniList idx_norm (target English title ↔ normalized AniList title_ja)
        if not match and sen:
            senk = _normalize(sen)
            if senk and senk in idx_norm:
                match = _pick_best(idx_norm[senk], year_hint)

        # 9. Roman/Greek numeral normalization (Ζ→Z, Ⅲ→III)
        #    e.g. 機動戦士ガンダムΖΖ → 機動戦士ガンダムZZ
        if not match:
            rk = _normalize(_normalize_roman_greek(sja))
            if rk != _normalize(sja) and rk in idx_norm:
                match = _pick_best(idx_norm[rk], year_hint)

        # 10. reading-paren removal (SeesaaWiki side)
        #     e.g. GATE（ゲート）自衛隊 → GATE自衛隊
        if not match:
            rp = _normalize(_strip_reading_parens(sja))
            if rp != _normalize(sja) and rp in idx_norm:
                match = _pick_best(idx_norm[rp], year_hint)

        # 11. furigana removal (AniList side) — SeesaaWiki title ↔ furigana-stripped AniList
        #     e.g. 魔法騎士レイアース → 魔法騎士（マジックナイト）レイアース
        if not match:
            nk = _normalize(sja)
            if nk in idx_furigana:
                match = _pick_best(idx_furigana[nk], year_hint)

        # 12. English prefix removal
        #     e.g. MAJOR メジャー 第3シリーズ → メジャー第3シリーズ
        if not match:
            ep = _strip_english_prefix(sja)
            if ep and ep != sja:
                epk = _normalize(ep)
                if epk and epk in idx_norm:
                    match = _pick_best(idx_norm[epk], year_hint)

        # 13. reverse-containment with 4-char threshold (SeesaaWiki ⊂ AniList, 4+ chars)
        #     e.g. 監獄学園 → 監獄学園〈プリズンスクール〉
        if not match:
            nk = _normalize(sja)
            if 4 <= len(nk) < 5:
                rev_candidates = [
                    entry
                    for ref_nk, entry in ref_entries_for_containment
                    if ref_nk != nk and nk in ref_nk
                ]
                if 0 < len(rev_candidates) <= 10:
                    match = _pick_best(rev_candidates, year_hint)

        # 14. forward containment (AniList ⊂ SeesaaWiki, ratio>50%)
        #     e.g. それいけ!アンパンマン ⊂ TVそれいけ!アンパンマン（2009年）
        #     quality gate: AniList normalized title covers ≥50% of SeesaaWiki title
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 8:
                fwd_candidates = []
                for ref_nk, entry in ref_entries_for_containment:
                    if (
                        ref_nk != nk
                        and len(ref_nk) >= 5
                        and ref_nk in nk
                        and len(ref_nk) / len(nk) >= 0.5
                    ):
                        fwd_candidates.append(entry)
                if 0 < len(fwd_candidates) <= 10:
                    # prefer longest match
                    fwd_candidates.sort(
                        key=lambda e: len(_normalize(e[6])),
                        reverse=True,
                    )
                    match = _pick_best(fwd_candidates[:3], year_hint)

        # 15. kanji+hiragana key match (English/katakana differs but kanji agrees)
        #     e.g. 銀牙伝説WEED ↔ 銀牙伝説ウィード (shared key: 銀牙伝説)
        #     quality gate: key 4+ chars, only 1 AniList candidate
        if not match:
            khk = _kanji_hira_key(sja)
            if len(khk) >= 4 and khk in idx_khk:
                cands = idx_khk[khk]
                if len(cands) == 1:
                    match = cands[0]

        # 16. fuzzy match rule dictionary (90%+ confidence, manually verified)
        if not match and sja in _FUZZY_MATCH_RULES:
            anilist_ja, rule_year = _FUZZY_MATCH_RULES[sja]
            for aid, ja, en, yr, q, sea, sd, fmt in refs:
                if ja == anilist_ja and yr == rule_year:
                    match = (aid, yr, q, sea, sd, fmt, ja)
                    break

        # 16. movie/film-prefix-stripped match
        #     e.g. 映画 聲の形 → 聲の形, 劇場版シティーハンター → シティーハンター
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja) and mk in idx_norm:
                match = _pick_best(idx_norm[mk], year_hint)
            elif mk and mk in idx_movie:
                match = _pick_best(idx_movie[mk], year_hint)

        # 17. word-order-reversed match (SHADOW SKILL -影技- ↔ 影技 SHADOW SKILL)
        #     quality gate: 5+ chars, ≤10 candidates
        if not match:
            nk = _normalize(sja)
            if len(nk) >= 5:
                words = [w for w in re.split(r"[\s\u3000・\-_/]+", sja) if w]
                if len(words) >= 2:
                    swk = _normalize(_sorted_word_key(sja))
                    if swk and swk in idx_sorted:
                        cands = idx_sorted[swk]
                        if 0 < len(cands) <= 10:
                            match = _pick_best(cands, year_hint)
                    # also check if the sorted key matches AniList idx_norm
                    if not match and swk in idx_norm:
                        match = _pick_best(idx_norm[swk], year_hint)

        # 18. movie-prefix removal + AniList movie index
        #     e.g. 映画 プリキュアオールスターズF → プリキュアオールスターズF
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja):
                # also attempt forward containment (ratio>60%)
                if len(mk) >= 6:
                    mv_cands = [
                        entry
                        for ref_nk, entry in ref_entries_for_containment
                        if ref_nk != mk
                        and len(ref_nk) >= 5
                        and ref_nk in mk
                        and len(ref_nk) / len(mk) >= 0.6
                    ]
                    if 0 < len(mv_cands) <= 5:
                        mv_cands.sort(key=lambda e: len(_normalize(e[6])), reverse=True)
                        match = _pick_best(mv_cands[:3], year_hint)

        if match:
            _, yr, q, sea, sd, fmt, _ = match
            updates.append((yr, q, sea, sd, fmt, sid))

    # --- bulk DB update ---
    if updates:
        conn.executemany(
            "UPDATE anime SET year=?, quarter=?, season=?, start_date=?, format=? "
            "WHERE id=?",
            updates,
        )

    # recompute quarter for entries that still have NULL after backfill
    # (season/start_date was backfilled but quarter was not set)
    conn.execute("""
        UPDATE anime SET quarter = CASE
            WHEN LOWER(season) = 'winter' THEN 1
            WHEN LOWER(season) = 'spring' THEN 2
            WHEN LOWER(season) = 'summer' THEN 3
            WHEN LOWER(season) = 'fall'   THEN 4
            WHEN start_date IS NOT NULL AND LENGTH(start_date) >= 7
                THEN (CAST(SUBSTR(start_date, 6, 2) AS INTEGER) - 1) / 3 + 1
            ELSE quarter
        END
        WHERE id IN (SELECT id FROM anime WHERE year IS NOT NULL AND quarter IS NULL)
    """)

    total_backfilled = len(updates)
    final_count = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE quarter IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "anime_backfill_from_anilist",
        matched=total_backfilled,
        total_quarter_populated=final_count,
    )


def _migrate_v20_add_credit_temporal(conn: sqlite3.Connection) -> None:
    """v20: add credit_year / credit_quarter to credits table.

    Short series (≤12 episodes or episode=-1): copy year/quarter from anime directly.
    Long-running series (>12 episodes, with episode info): estimate airing date per episode
        using start_date + one-episode-per-week assumption.

    Formula: air_date = start_date + (episode - 1) * 7 days
    """
    from datetime import date, timedelta

    for col in ["credit_year INTEGER", "credit_quarter INTEGER"]:
        try:
            conn.execute(f"ALTER TABLE credits ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # Phase 1: default-set credit_year/quarter from anime for all credits
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a WHERE credits.anime_id = a.id AND a.year IS NOT NULL
    """)

    phase1 = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE credit_year IS NOT NULL"
    ).fetchone()[0]
    logger.info("credit_temporal_phase1", count=phase1)

    # Phase 2: per-episode airing date estimation for long-running series
    #   target: series with start_date, episode-numbered credits, and 13+ episodes
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        # estimate broadcast interval
        # if end_date exists: (end - start) / max_ep
        # otherwise assume weekly (7 days)
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = (ed - sd).days / (max_ep - 1)
                    # sanity check: clamp to 3-14 days (weekly ± tolerance)
                    interval_days = max(3, min(14, interval_days))
            except (ValueError, TypeError):
                pass

        # update credits for each episode
        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            updated += len(batch)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_credits_yq ON credits(credit_year, credit_quarter)"
    )

    final = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE credit_year IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "credit_temporal_phase2",
        long_anime=len(long_anime),
        episodes_updated=updated,
        total_with_temporal=final,
    )


def _migrate_v21_enhanced_title_matching(conn: sqlite3.Connection) -> None:
    """v21: re-run enhanced title matching (title_en, TV prefix, reverse containment).

    New match strategies have been added to _backfill_from_anilist_titles since v19.
    This migration re-runs backfill to cover remaining unmatched entries and also
    populates credit_year/credit_quarter for newly matched titles.
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # set credit_year/credit_quarter for credits of newly matched titles
    # (credit_year IS NULL means the title was unmatched at the time of v20)
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # also apply per-episode estimation for newly matched long-running series
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year  -- v20で一括設定されたままのもの
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v21_enhanced_title_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v22_deep_title_matching(conn: sqlite3.Connection) -> None:
    """v22: deep title matching (Roman numerals, furigana, English prefix, forward containment).

    Covers remaining unmatched entries using strategies 9-14 added to backfill since v21.
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # Set credit_year/credit_quarter on credits for newly matched works
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # also apply per-episode estimation for newly matched long-running series
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v22_deep_title_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v23_fuzzy_match_rules(conn: sqlite3.Connection) -> None:
    """v23: apply fuzzy match rules (90%+ confidence) to cover remaining entries.

    Uses phase 15 (fuzzy match rules) added to backfill since v22
    to match high-confidence title substitution pairs.
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    # Set credit_year/credit_quarter on credits for newly matched works
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # also apply per-episode estimation for newly matched long-running series
    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes,
               MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0
          AND c.credit_year = a.year
        GROUP BY a.id
        HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue

        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass

        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()

        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            yr = air_date.year
            q = (air_date.month - 1) // 3 + 1
            batch.append((yr, q, rowid))

        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v23_fuzzy_match_rules",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v24_improved_matching(conn: sqlite3.Connection) -> None:
    """v24: improved matching (movie prefix, word-order reversal, normalize improvements, extra fuzzy rules).

    Changes:
    - normalize(): unify 〈〉→<>, HTML entity decode, add ◆◇ etc.
    - Phase 16: movie/film-prefix-stripped match
    - Phase 17: word-order-reversed match (SHADOW SKILL-影技- ↔ 影技 SHADOW SKILL)
    - Phase 18: movie prefix removal + forward containment
    - _FUZZY_MATCH_RULES: 25 new entries in v24 (攻殻機動隊ARISE, 頭文字D, etc.)
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]
    newly_matched = before - after

    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes, MAX(c.episode) as max_ep
        FROM anime a
        INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0 AND c.credit_year = a.year
        GROUP BY a.id HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, episodes_col, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass
        ep_credits = conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id = ? AND episode > 0",
            (anime_id,),
        ).fetchall()
        batch: list[tuple] = []
        for rowid, ep in ep_credits:
            air_date = sd + timedelta(days=(ep - 1) * interval_days)
            batch.append((air_date.year, (air_date.month - 1) // 3 + 1, rowid))
        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v24_improved_matching",
        newly_matched=newly_matched,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def _migrate_v25_kanji_hira_matching(conn: sqlite3.Connection) -> None:
    """v25: kanji+hiragana key match — identify titles that agree on kanji even when English/katakana differs.

    Example: 銀牙伝説WEED ↔ 銀牙伝説ウィード (key: 銀牙伝説)
    Quality gate: key 4+ chars, only one AniList candidate accepted.
    """
    from datetime import date, timedelta

    before = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    _backfill_from_anilist_titles(conn)

    after = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE year IS NULL AND id NOT LIKE 'anilist:%'"
    ).fetchone()[0]

    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL AND credits.credit_year IS NULL
    """)

    long_anime = conn.execute("""
        SELECT a.id, a.start_date, a.end_date, a.episodes, MAX(c.episode) as max_ep
        FROM anime a INNER JOIN credits c ON c.anime_id = a.id
        WHERE a.start_date IS NOT NULL AND LENGTH(a.start_date) >= 10
          AND c.episode > 0 AND c.credit_year = a.year
        GROUP BY a.id HAVING max_ep > 12
    """).fetchall()

    ep_updated = 0
    for anime_id, start_str, end_str, _, max_ep in long_anime:
        try:
            sd = date.fromisoformat(start_str[:10])
        except (ValueError, TypeError):
            continue
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = max(3, min(14, (ed - sd).days / (max_ep - 1)))
            except (ValueError, TypeError):
                pass
        batch = []
        for rowid, ep in conn.execute(
            "SELECT rowid, episode FROM credits WHERE anime_id=? AND episode>0",
            (anime_id,),
        ).fetchall():
            air = sd + timedelta(days=(ep - 1) * interval_days)
            batch.append((air.year, (air.month - 1) // 3 + 1, rowid))
        if batch:
            conn.executemany(
                "UPDATE credits SET credit_year=?, credit_quarter=? WHERE rowid=?",
                batch,
            )
            ep_updated += len(batch)

    logger.info(
        "v25_kanji_hira_matching",
        newly_matched=before - after,
        remaining_null=after,
        episodes_updated=ep_updated,
    )


def compute_anime_scale_classes(conn: sqlite3.Connection) -> dict[str, int]:
    """Classify anime by work_type (tv/tanpatsu) × scale_class (large/medium/small).

    Features: log(total_animator_credits), log(unique_animators)
    Minimum data threshold: total_animator_credits >= 10
    K-means K=3 applied independently to TV and tanpatsu subsets.
    Centroids sorted by median total_animator_credits → mapped to small/medium/large.

    Returns:
        {'tv_classified': n, 'tanpatsu_classified': n, 'null_assigned': n}
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    ANIMATOR_ROLES = (
        "key_animator",
        "second_key_animator",
        "animation_director",
        "in_between",
        "finishing",
        "layout",
    )
    TV_FORMATS = {"TV", "TV_SHORT"}
    TANPATSU_FORMATS = {"OVA", "MOVIE", "ONA", "SPECIAL"}
    MIN_CREDITS = 10
    K = 3
    SCALE_LABELS = ["small", "medium", "large"]

    # Collect per-anime animator stats
    rows = conn.execute(
        """
        SELECT a.id, a.format,
               COUNT(c.id)              AS total,
               COUNT(DISTINCT c.person_id) AS unique_anim
        FROM anime a
        JOIN credits c ON c.anime_id = a.id
        WHERE c.role IN ({})
          AND a.format IS NOT NULL
        GROUP BY a.id
        """.format(",".join(f"'{r}'" for r in ANIMATOR_ROLES))
    ).fetchall()

    tv_rows = [r for r in rows if r[1] in TV_FORMATS and r[2] >= MIN_CREDITS]
    tan_rows = [r for r in rows if r[1] in TANPATSU_FORMATS and r[2] >= MIN_CREDITS]

    def _classify(data: list) -> dict[str, str]:
        """anime_id → scale_class ('small'/'medium'/'large')."""
        if len(data) < K:
            return {}
        X_raw = np.array([[r[2], r[3]] for r in data], dtype=float)
        X = StandardScaler().fit_transform(np.log1p(X_raw))
        km = KMeans(n_clusters=K, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        # Sort clusters by median total_credits → small/medium/large
        medians = [
            (
                int(
                    np.median([data[i][2] for i in range(len(data)) if labels[i] == cl])
                ),
                cl,
            )
            for cl in range(K)
        ]
        medians.sort()
        cl_to_label = {cl: SCALE_LABELS[rank] for rank, (_, cl) in enumerate(medians)}
        return {data[i][0]: cl_to_label[labels[i]] for i in range(len(data))}

    tv_map = _classify(tv_rows)
    tan_map = _classify(tan_rows)

    # Reset columns first
    conn.execute("UPDATE anime SET work_type = NULL, scale_class = NULL")

    # Assign work_type for all anime with known format
    conn.execute("UPDATE anime SET work_type = 'tv' WHERE format IN ('TV', 'TV_SHORT')")
    conn.execute(
        "UPDATE anime SET work_type = 'tanpatsu'"
        " WHERE format IN ('OVA', 'MOVIE', 'ONA', 'SPECIAL')"
    )

    # Assign scale_class in batches
    for anime_id, scale in tv_map.items():
        conn.execute("UPDATE anime SET scale_class = ? WHERE id = ?", (scale, anime_id))
    for anime_id, scale in tan_map.items():
        conn.execute("UPDATE anime SET scale_class = ? WHERE id = ?", (scale, anime_id))

    null_count = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE work_type IS NOT NULL AND scale_class IS NULL"
    ).fetchone()[0]

    logger.info(
        "anime_scale_classified",
        tv_classified=len(tv_map),
        tanpatsu_classified=len(tan_map),
        null_scale=null_count,
    )
    return {
        "tv_classified": len(tv_map),
        "tanpatsu_classified": len(tan_map),
        "null_assigned": null_count,
    }


def _migrate_v26_anime_scale_classification(conn: sqlite3.Connection) -> None:
    """v26: add work_type / scale_class columns to anime table and classify via K-means."""
    conn.executescript("""
        ALTER TABLE anime ADD COLUMN work_type  TEXT;
        ALTER TABLE anime ADD COLUMN scale_class TEXT;
        CREATE INDEX IF NOT EXISTS idx_anime_work_type  ON anime(work_type);
        CREATE INDEX IF NOT EXISTS idx_anime_scale_class ON anime(scale_class);
    """)
    compute_anime_scale_classes(conn)


def _migrate_v27_normalize_legacy_roles(conn: sqlite3.Connection) -> None:
    """v27: normalize legacy role values in credits table to current Role enum values.

    Applies the runtime conversion previously handled by _LEGACY_ROLE_MAP as a
    one-time data migration and persists it.

    Note: "other" stays as Role.OTHER (catch-all for unclassifiable credits).
          "special" is a different concept (special thanks, etc.) and must not be conflated.
    """
    legacy_map = {
        "chief_animation_director": "animation_director",
        "storyboard": "episode_director",
        "mechanical_designer": "character_designer",
        "art_director": "background_art",
        "color_designer": "finishing",
        "effects": "photography_director",
        "theme_song": "music",
        "series_composition": "screenplay",
        "adr": "voice_actor",
    }
    for old_role, new_role in legacy_map.items():
        conn.execute(
            "UPDATE credits SET role = ? WHERE role = ?",
            (new_role, old_role),
        )


def _migrate_v28_add_career_track(conn: sqlite3.Connection) -> None:
    """v28: add career_track column to scores table.

    career_track is derived/processed data estimated by the pipeline, not raw data.
    Placed in the scores table to maintain separation from raw credits.

    Values: 'animator' / 'animator_director' / 'director' /
            'production' / 'technical' / 'multi_track'
    """
    conn.executescript("""
        ALTER TABLE scores ADD COLUMN career_track TEXT NOT NULL DEFAULT 'multi_track';
        CREATE INDEX IF NOT EXISTS idx_scores_career_track ON scores(career_track);
    """)


def _migrate_v29_add_feat_tables(conn: sqlite3.Connection) -> None:
    """v29: add feat_* derived-feature table group.

    Naming clearly separates raw data (persons/anime/credits) from pipeline-computed results.
    Adds 5 tables — feat_person_scores, feat_network, feat_career, feat_genre_affinity,
    feat_contribution — to progressively reduce dependence on JSON files.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_person_scores (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            person_fe REAL,
            person_fe_se REAL,
            person_fe_n_obs INTEGER,
            studio_fe_exposure REAL,
            birank REAL,
            patronage REAL,
            awcc REAL,
            dormancy REAL,
            ndi REAL,
            career_friction REAL,
            peer_boost REAL,
            iv_score REAL,
            iv_score_pct REAL,
            person_fe_pct REAL,
            birank_pct REAL,
            patronage_pct REAL,
            awcc_pct REAL,
            dormancy_pct REAL,
            confidence REAL,
            score_range_low REAL,
            score_range_high REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_network (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            degree_centrality REAL,
            betweenness_centrality REAL,
            closeness_centrality REAL,
            eigenvector_centrality REAL,
            hub_score REAL,
            n_collaborators INTEGER,
            n_unique_anime INTEGER,
            bridge_score REAL,
            n_bridge_communities INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_career (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            first_year INTEGER,
            latest_year INTEGER,
            active_years INTEGER,
            total_credits INTEGER,
            highest_stage INTEGER,
            primary_role TEXT,
            career_track TEXT,
            peak_year INTEGER,
            peak_credits INTEGER,
            growth_trend TEXT,
            growth_score REAL,
            activity_ratio REAL,
            recent_credits INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS feat_genre_affinity (
            person_id TEXT NOT NULL,
            genre TEXT NOT NULL,
            run_id INTEGER REFERENCES pipeline_runs(id),
            affinity_score REAL,
            work_count INTEGER,
            PRIMARY KEY (person_id, genre)
        );

        CREATE TABLE IF NOT EXISTS feat_contribution (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            peer_percentile REAL,
            opportunity_residual REAL,
            consistency REAL,
            independent_value REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_feat_person_scores_iv ON feat_person_scores(iv_score);
        CREATE INDEX IF NOT EXISTS idx_feat_career_first_year ON feat_career(first_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_track ON feat_career(career_track);
        CREATE INDEX IF NOT EXISTS idx_feat_genre_genre ON feat_genre_affinity(genre);
    """)


# ================================================================
# feat_* DAO: bulk write / read for derived feature tables
# ================================================================


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


def _migrate_v30_add_feat_credit_activity(conn: sqlite3.Connection) -> None:
    """v30: add feat_credit_activity table.

    Pre-aggregates per-person gap periods, activity density, and hiatus history.
    compute_feat_credit_activity() re-computes all rows on the next pipeline run.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_credit_activity (
            person_id TEXT PRIMARY KEY,
            first_abs_quarter INTEGER,
            last_abs_quarter INTEGER,
            activity_span_quarters INTEGER,
            active_quarters INTEGER,
            density REAL,
            n_gaps INTEGER,
            mean_gap_quarters REAL,
            median_gap_quarters REAL,
            min_gap_quarters INTEGER,
            max_gap_quarters INTEGER,
            std_gap_quarters REAL,
            consecutive_quarters INTEGER,
            consecutive_rate REAL,
            n_hiatuses INTEGER,
            longest_hiatus_quarters INTEGER,
            quarters_since_last_credit INTEGER,
            active_years INTEGER,
            n_year_gaps INTEGER,
            mean_year_gap REAL,
            max_year_gap INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_span
            ON feat_credit_activity(activity_span_quarters);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_activity_last
            ON feat_credit_activity(last_abs_quarter);
    """)


def compute_feat_credit_activity(
    conn: sqlite3.Connection,
    current_year: int | None = None,
    current_quarter: int | None = None,
    batch_size: int = 5000,
) -> int:
    """Compute per-person credit gap periods and activity patterns and save to feat_credit_activity.

    Computed metrics:
    - Quarter-level gaps: mean/median/min/max/std interval (in quarters)
    - Consecutive quarter rate: fraction of gaps equal to 1 quarter
    - Hiatus (n ≥ 4Q): count and longest duration
    - Year-level gaps (includes credits with quarter=NULL)
    - Quarters elapsed since last credit

    abs_quarter representation: year * 4 + (quarter - 1)
    Example: 2020Q1 → 8080, 2020Q4 → 8083

    Args:
        conn: SQLite connection
        current_year: reference year (defaults to current year)
        current_quarter: reference quarter (defaults to current quarter)
        batch_size: batch size for bulk INSERT

    Returns:
        number of rows written
    """
    import datetime
    import math
    import statistics

    if current_year is None:
        current_year = datetime.datetime.now().year
    if current_quarter is None:
        current_quarter = (datetime.datetime.now().month - 1) // 3 + 1
    current_abs_q = current_year * 4 + current_quarter - 1

    # --- Step 1: quarter-level data (consecutive diffs via LAG) ---
    # use CTE to get per-person gap list (SQLite window functions)
    quarter_gaps_sql = """
        WITH distinct_quarters AS (
            SELECT
                person_id,
                credit_year * 4 + credit_quarter - 1 AS abs_quarter
            FROM credits
            WHERE credit_year IS NOT NULL AND credit_quarter IS NOT NULL
            GROUP BY person_id, credit_year, credit_quarter
        ),
        with_lag AS (
            SELECT
                person_id,
                abs_quarter,
                LAG(abs_quarter) OVER (
                    PARTITION BY person_id ORDER BY abs_quarter
                ) AS prev_quarter
            FROM distinct_quarters
        )
        SELECT person_id, abs_quarter - prev_quarter AS gap
        FROM with_lag
        WHERE prev_quarter IS NOT NULL
        ORDER BY person_id, gap
    """

    # --- Step 2: activity range (quarters) ---
    activity_sql = """
        SELECT
            person_id,
            MIN(credit_year * 4 + credit_quarter - 1) AS first_abs_quarter,
            MAX(credit_year * 4 + credit_quarter - 1) AS last_abs_quarter,
            COUNT(DISTINCT credit_year * 4 + credit_quarter - 1) AS active_quarters
        FROM credits
        WHERE credit_year IS NOT NULL AND credit_quarter IS NOT NULL
        GROUP BY person_id
    """

    # --- Step 3: year-level gaps (all credits) ---
    year_gaps_sql = """
        WITH distinct_years AS (
            SELECT person_id, credit_year
            FROM credits
            WHERE credit_year IS NOT NULL
            GROUP BY person_id, credit_year
        ),
        with_lag AS (
            SELECT
                person_id,
                credit_year - LAG(credit_year) OVER (
                    PARTITION BY person_id ORDER BY credit_year
                ) AS gap
            FROM distinct_years
        )
        SELECT
            person_id,
            COUNT(*) AS n_year_gaps,
            AVG(gap) AS mean_year_gap,
            MAX(gap) AS max_year_gap
        FROM with_lag
        WHERE gap IS NOT NULL
        GROUP BY person_id
    """

    year_activity_sql = """
        SELECT person_id, COUNT(DISTINCT credit_year) AS active_years
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id
    """

    logger.info("feat_credit_activity_compute_start")

    # load activity range
    activity_rows = conn.execute(activity_sql).fetchall()
    activity = {
        r["person_id"]: {
            "first_abs_quarter": r["first_abs_quarter"],
            "last_abs_quarter": r["last_abs_quarter"],
            "active_quarters": r["active_quarters"],
        }
        for r in activity_rows
    }

    # year-level gaps
    year_gaps = {
        r["person_id"]: {
            "n_year_gaps": r["n_year_gaps"],
            "mean_year_gap": r["mean_year_gap"],
            "max_year_gap": r["max_year_gap"],
        }
        for r in conn.execute(year_gaps_sql).fetchall()
    }
    year_activity = {
        r["person_id"]: r["active_years"]
        for r in conn.execute(year_activity_sql).fetchall()
    }

    # aggregate gaps by person_id (streaming read)
    gaps_by_person: dict[str, list[int]] = {}
    cur = conn.execute(quarter_gaps_sql)
    for row in cur:
        pid = row["person_id"]
        if pid not in gaps_by_person:
            gaps_by_person[pid] = []
        gaps_by_person[pid].append(row["gap"])

    logger.info(
        "feat_credit_activity_data_loaded",
        persons_with_quarters=len(activity),
        persons_with_gaps=len(gaps_by_person),
    )

    # compute stats and batch insert
    batch: list[tuple] = []
    total_written = 0

    for pid, act in activity.items():
        first_q = act["first_abs_quarter"]
        last_q = act["last_abs_quarter"]
        active_q = act["active_quarters"]
        span = last_q - first_q  # 0 means active in only one quarter
        density = active_q / (span + 1) if span >= 0 else 1.0

        gaps = gaps_by_person.get(pid, [])
        n_gaps = len(gaps)

        if n_gaps > 0:
            mean_gap = sum(gaps) / n_gaps
            median_gap = statistics.median(gaps)
            min_gap = min(gaps)
            max_gap = max(gaps)
            variance = sum((g - mean_gap) ** 2 for g in gaps) / n_gaps
            std_gap = math.sqrt(variance)
            consec = sum(1 for g in gaps if g == 1)
            consec_rate = consec / n_gaps
            hiatuses = [g for g in gaps if g >= 4]
            n_hiatuses = len(hiatuses)
            longest_hiatus = max(hiatuses) if hiatuses else 0
        else:
            mean_gap = median_gap = min_gap = max_gap = std_gap = None
            consec = consec_rate = 0
            n_hiatuses = longest_hiatus = 0

        yg = year_gaps.get(pid, {})
        batch.append(
            (
                pid,
                first_q,
                last_q,
                span,
                active_q,
                round(density, 4),
                n_gaps,
                round(mean_gap, 4) if mean_gap is not None else None,
                round(median_gap, 4) if median_gap is not None else None,
                min_gap,
                max_gap,
                round(std_gap, 4) if std_gap is not None else None,
                consec,
                round(consec_rate, 4) if n_gaps > 0 else None,
                n_hiatuses,
                longest_hiatus or None,
                current_abs_q - last_q,
                year_activity.get(pid),
                yg.get("n_year_gaps"),
                round(yg["mean_year_gap"], 4) if yg.get("mean_year_gap") else None,
                yg.get("max_year_gap"),
            )
        )

        if len(batch) >= batch_size:
            _insert_feat_credit_activity_batch(conn, batch)
            total_written += len(batch)
            batch = []

    if batch:
        _insert_feat_credit_activity_batch(conn, batch)
        total_written += len(batch)

    conn.commit()
    logger.info("feat_credit_activity_computed", rows=total_written)
    return total_written


def _insert_feat_credit_activity_batch(
    conn: sqlite3.Connection, batch: list[tuple]
) -> None:
    conn.executemany(
        """
        INSERT INTO feat_credit_activity (
            person_id,
            first_abs_quarter, last_abs_quarter, activity_span_quarters,
            active_quarters, density,
            n_gaps, mean_gap_quarters, median_gap_quarters,
            min_gap_quarters, max_gap_quarters, std_gap_quarters,
            consecutive_quarters, consecutive_rate,
            n_hiatuses, longest_hiatus_quarters,
            quarters_since_last_credit,
            active_years, n_year_gaps, mean_year_gap, max_year_gap,
            updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            first_abs_quarter=excluded.first_abs_quarter,
            last_abs_quarter=excluded.last_abs_quarter,
            activity_span_quarters=excluded.activity_span_quarters,
            active_quarters=excluded.active_quarters,
            density=excluded.density,
            n_gaps=excluded.n_gaps,
            mean_gap_quarters=excluded.mean_gap_quarters,
            median_gap_quarters=excluded.median_gap_quarters,
            min_gap_quarters=excluded.min_gap_quarters,
            max_gap_quarters=excluded.max_gap_quarters,
            std_gap_quarters=excluded.std_gap_quarters,
            consecutive_quarters=excluded.consecutive_quarters,
            consecutive_rate=excluded.consecutive_rate,
            n_hiatuses=excluded.n_hiatuses,
            longest_hiatus_quarters=excluded.longest_hiatus_quarters,
            quarters_since_last_credit=excluded.quarters_since_last_credit,
            active_years=excluded.active_years,
            n_year_gaps=excluded.n_year_gaps,
            mean_year_gap=excluded.mean_year_gap,
            max_year_gap=excluded.max_year_gap,
            updated_at=CURRENT_TIMESTAMP
    """,
        batch,
    )


def load_feat_credit_activity(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return feat_credit_activity as person_id → dict mapping."""
    rows = conn.execute("SELECT * FROM feat_credit_activity").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def _migrate_v31_add_feat_career_annual(conn: sqlite3.Connection) -> None:
    """v31: add feat_career_annual table.

    Pre-aggregates per-person work/credit counts by career year
    (years since debut) and role category.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_career_annual (
            person_id TEXT NOT NULL,
            career_year INTEGER NOT NULL,
            credit_year INTEGER NOT NULL,
            n_works INTEGER NOT NULL DEFAULT 0,
            n_credits INTEGER NOT NULL DEFAULT 0,
            n_roles INTEGER NOT NULL DEFAULT 0,
            works_direction INTEGER NOT NULL DEFAULT 0,
            works_animation_supervision INTEGER NOT NULL DEFAULT 0,
            works_animation INTEGER NOT NULL DEFAULT 0,
            works_design INTEGER NOT NULL DEFAULT 0,
            works_technical INTEGER NOT NULL DEFAULT 0,
            works_art INTEGER NOT NULL DEFAULT 0,
            works_sound INTEGER NOT NULL DEFAULT 0,
            works_writing INTEGER NOT NULL DEFAULT 0,
            works_production INTEGER NOT NULL DEFAULT 0,
            works_production_management INTEGER NOT NULL DEFAULT 0,
            works_finishing INTEGER NOT NULL DEFAULT 0,
            works_editing INTEGER NOT NULL DEFAULT 0,
            works_settings INTEGER NOT NULL DEFAULT 0,
            works_other INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (person_id, career_year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_year
            ON feat_career_annual(career_year);
        CREATE INDEX IF NOT EXISTS idx_feat_career_annual_credit_year
            ON feat_career_annual(credit_year);
    """)


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


def _migrate_v33_add_feat_credit_contribution(conn: sqlite3.Connection) -> None:
    """v33: add feat_credit_contribution / feat_person_work_summary tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_credit_contribution (
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER,
            production_scale REAL,
            role_weight REAL,
            episode_coverage REAL,
            dur_mult REAL,
            edge_weight REAL,
            edge_weight_share REAL,
            iv_contrib_est REAL,
            PRIMARY KEY (person_id, anime_id, role)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_anime
            ON feat_credit_contribution(anime_id);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_year
            ON feat_credit_contribution(credit_year);

        CREATE TABLE IF NOT EXISTS feat_person_work_summary (
            person_id TEXT PRIMARY KEY,
            n_distinct_works INTEGER,
            total_production_scale REAL,
            mean_production_scale REAL,
            max_production_scale REAL,
            best_work_anime_id TEXT,
            total_edge_weight REAL,
            mean_edge_weight_per_work REAL,
            max_edge_weight REAL,
            top_contrib_anime_id TEXT,
            total_iv_contrib_est REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _migrate_v34_add_agg_milestones(conn: sqlite3.Connection) -> None:
    """v34: add agg_milestones table (L2: career events extracted from raw data)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agg_milestones (
            person_id   TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            year        INTEGER NOT NULL DEFAULT 0,
            anime_id    TEXT NOT NULL DEFAULT '',
            anime_title TEXT,
            description TEXT,
            PRIMARY KEY (person_id, event_type, year, anime_id)
        );
        CREATE INDEX IF NOT EXISTS idx_agg_milestones_person ON agg_milestones(person_id);
        CREATE INDEX IF NOT EXISTS idx_agg_milestones_year   ON agg_milestones(year);
    """)


def _migrate_v35_add_agg_director_circles(conn: sqlite3.Connection) -> None:
    """v35: add agg_director_circles table (L2: co-credit count aggregation)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agg_director_circles (
            person_id    TEXT NOT NULL,
            director_id  TEXT NOT NULL,
            shared_works INTEGER DEFAULT 0,
            hit_rate     REAL,
            roles        TEXT DEFAULT '[]',
            latest_year  INTEGER,
            PRIMARY KEY (person_id, director_id)
        );
        CREATE INDEX IF NOT EXISTS idx_agg_dir_circles_person
            ON agg_director_circles(person_id);
        CREATE INDEX IF NOT EXISTS idx_agg_dir_circles_director
            ON agg_director_circles(director_id);
    """)


def _migrate_v36_add_feat_mentorships(conn: sqlite3.Connection) -> None:
    """v36: add feat_mentorships table (L3: algorithmic mentor estimation)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_mentorships (
            mentor_id      TEXT NOT NULL,
            mentee_id      TEXT NOT NULL,
            n_shared_works INTEGER DEFAULT 0,
            hit_rate       REAL,
            mentor_stage   INTEGER,
            mentee_stage   INTEGER,
            first_year     INTEGER,
            latest_year    INTEGER,
            PRIMARY KEY (mentor_id, mentee_id)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_mentorships_mentor
            ON feat_mentorships(mentor_id);
        CREATE INDEX IF NOT EXISTS idx_feat_mentorships_mentee
            ON feat_mentorships(mentee_id);
    """)


def _migrate_v32_add_feat_studio_affiliation(conn: sqlite3.Connection) -> None:
    """v32: add feat_studio_affiliation table.

    Pre-aggregates which studios each person participated in, by year.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_studio_affiliation (
            person_id TEXT NOT NULL,
            credit_year INTEGER NOT NULL,
            studio_id TEXT NOT NULL,
            studio_name TEXT NOT NULL DEFAULT '',
            n_works INTEGER NOT NULL DEFAULT 0,
            n_credits INTEGER NOT NULL DEFAULT 0,
            is_main_studio INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (person_id, credit_year, studio_id)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_person
            ON feat_studio_affiliation(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_studio
            ON feat_studio_affiliation(studio_id, credit_year);
    """)


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


def compute_feat_credit_contribution(
    conn: sqlite3.Connection,
    batch_size: int = 5000,
) -> tuple[int, int]:
    """Compute and save score contribution amounts per person × work × role.

    Computed metrics:
    - production_scale: AKM outcome variable log1p(staff_count) × log1p(episodes) × dur_mult
      → measures how large-scale the production is; the regression dependent variable itself.
    - edge_weight: graph edge contribution role_weight × episode_coverage × dur_mult
      → measures how much weight this role/participation scale carries in the network.
    - iv_contrib_est: edge_weight_share × iv_score
      → proportional attribution of IV score. Approximation of the true LOO marginal.

    Note: iv_contrib_est is an approximation. Exact marginals require a full pipeline re-run.

    Args:
        conn: SQLite connection
        batch_size: INSERT batch size

    Returns:
        (feat_credit_contribution row count, feat_person_work_summary row count)
    """
    import math

    from src.utils.config import (
        DURATION_BASELINE_MINUTES,
        DURATION_MAX_MULTIPLIER,
        ROLE_WEIGHTS,
    )

    logger.info("feat_credit_contribution_compute_start")

    # --- pre-compute anime staff_count ---
    staff_sql = """
        SELECT anime_id, COUNT(DISTINCT person_id) AS staff_count
        FROM credits GROUP BY anime_id
    """
    anime_staff: dict[str, int] = {
        r["anime_id"]: r["staff_count"] for r in conn.execute(staff_sql).fetchall()
    }

    # --- anime metadata ---
    anime_sql = "SELECT id, episodes, duration, format FROM anime"
    anime_meta: dict[str, dict] = {
        r["id"]: {"eps": r["episodes"] or 1, "dur": r["duration"], "fmt": r["format"]}
        for r in conn.execute(anime_sql).fetchall()
    }

    # format → dur_mult fallback (used when duration=NULL)
    FORMAT_DUR: dict[str | None, float] = {
        "MOVIE": DURATION_MAX_MULTIPLIER,
        "TV_SHORT": 0.25,
        "SPECIAL": 0.8,
        "ONA": 0.8,
        "TV": 0.8,
        "OVA": 1.0,
        "MUSIC": 0.25,
        None: 1.0,
    }

    def _dur_mult(dur: float | None, fmt: str | None) -> float:
        if dur is not None:
            return min(dur / DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER)
        return FORMAT_DUR.get(fmt, 1.0)

    # --- (person, anime, role) aggregation query ---
    # episode_coverage: distinct episode count where episode>0 / anime.episodes
    agg_sql = """
        SELECT
            c.person_id, c.anime_id, c.role,
            MIN(c.credit_year) AS credit_year,
            COUNT(DISTINCT CASE WHEN c.episode > 0 THEN c.episode END) AS ep_count
        FROM credits c
        GROUP BY c.person_id, c.anime_id, c.role
        ORDER BY c.person_id
    """

    # --- load per-person iv_score ---
    iv_by_pid: dict[str, float] = {}
    for r in conn.execute(
        "SELECT person_id, iv_score FROM feat_person_scores"
    ).fetchall():
        if r["iv_score"] is not None:
            iv_by_pid[r["person_id"]] = r["iv_score"]

    # --- compute and batch INSERT ---
    current_pid: str | None = None
    # track (anime_id → max edge_weight, max production_scale) within each person
    person_anime_ew: dict[str, float] = {}  # anime_id → max edge_weight in this person
    person_anime_ps: dict[str, float] = {}  # anime_id → production_scale
    person_total_ew: float = 0.0

    rows_contrib: list[tuple] = []
    rows_summary: list[tuple] = []
    total_contrib = 0
    total_summary = 0

    def _flush_person(pid: str) -> None:
        nonlocal rows_summary, total_summary
        if not person_anime_ew:
            return
        iv = iv_by_pid.get(pid, 0.0)
        total_ew = person_total_ew
        n_works = len(person_anime_ew)
        total_ps = sum(person_anime_ps.values())
        max_ps = max(person_anime_ps.values())
        best_work = max(person_anime_ps, key=person_anime_ps.__getitem__)
        max_ew = max(person_anime_ew.values())
        top_work = max(person_anime_ew, key=person_anime_ew.__getitem__)
        mean_ew = total_ew / n_works if n_works > 0 else 0.0
        mean_ps = total_ps / n_works if n_works > 0 else 0.0
        # iv_contrib_est ≈ iv_score (sum of shares = 1.0 when ew > 0)
        rows_summary.append(
            (
                pid,
                n_works,
                round(total_ps, 6),
                round(mean_ps, 6),
                round(max_ps, 6),
                best_work,
                round(total_ew, 6),
                round(mean_ew, 6),
                round(max_ew, 6),
                top_work,
                round(iv, 6),
            )
        )

    def _finalize_person_ew(pid: str) -> None:
        """Finalise edge_weight_share and iv_contrib_est after all credits for a person are processed."""
        # A 2nd-pass UPDATE is safer than back-patching rows_contrib.
        # Leave person total_ew in context here; actual UPDATE happens in the 2nd pass.
        pass

    for row in conn.execute(agg_sql):
        pid = row["person_id"]
        if pid != current_pid:
            if current_pid is not None:
                _flush_person(current_pid)
            current_pid = pid
            person_anime_ew = {}
            person_anime_ps = {}
            person_total_ew = 0.0

        aid = row["anime_id"]
        role_str = row["role"]
        meta = anime_meta.get(aid, {"eps": 1, "dur": None, "fmt": None})
        staff_cnt = anime_staff.get(aid, 1)

        # dur_mult
        dm = _dur_mult(meta["dur"], meta["fmt"])

        # production_scale
        ps = math.log1p(staff_cnt) * math.log1p(meta["eps"]) * dm

        # episode_coverage
        ep_cnt = row["ep_count"] or 0
        total_eps = meta["eps"]
        if ep_cnt > 0 and total_eps > 1:
            ep_cov = min(ep_cnt / total_eps, 1.0)
        else:
            ep_cov = 1.0

        # role_weight
        rw = ROLE_WEIGHTS.get(role_str, 1.0)

        # edge_weight
        ew = rw * ep_cov * dm

        # accumulate
        person_anime_ew[aid] = max(person_anime_ew.get(aid, 0.0), ew)
        person_anime_ps[aid] = max(person_anime_ps.get(aid, 0.0), ps)
        person_total_ew += ew

        rows_contrib.append(
            (
                pid,
                aid,
                role_str,
                row["credit_year"],
                round(ps, 6),
                round(rw, 4),
                round(ep_cov, 4),
                round(dm, 4),
                round(ew, 6),
                None,
                None,  # edge_weight_share / iv_contrib_est: updated in 2nd pass
            )
        )

        if len(rows_contrib) >= batch_size:
            conn.executemany(
                """
                INSERT INTO feat_credit_contribution
                    (person_id, anime_id, role, credit_year,
                     production_scale, role_weight, episode_coverage, dur_mult, edge_weight,
                     edge_weight_share, iv_contrib_est)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(person_id, anime_id, role) DO UPDATE SET
                    credit_year=excluded.credit_year,
                    production_scale=excluded.production_scale,
                    role_weight=excluded.role_weight,
                    episode_coverage=excluded.episode_coverage,
                    dur_mult=excluded.dur_mult,
                    edge_weight=excluded.edge_weight,
                    edge_weight_share=excluded.edge_weight_share,
                    iv_contrib_est=excluded.iv_contrib_est
            """,
                rows_contrib,
            )
            total_contrib += len(rows_contrib)
            rows_contrib = []

        if len(rows_summary) >= batch_size:
            _write_summary_batch(conn, rows_summary)
            total_summary += len(rows_summary)
            rows_summary = []

    # last person
    if current_pid is not None:
        _flush_person(current_pid)

    # flush remainder
    if rows_contrib:
        conn.executemany(
            """
            INSERT INTO feat_credit_contribution
                (person_id, anime_id, role, credit_year,
                 production_scale, role_weight, episode_coverage, dur_mult, edge_weight,
                 edge_weight_share, iv_contrib_est)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(person_id, anime_id, role) DO UPDATE SET
                credit_year=excluded.credit_year,
                production_scale=excluded.production_scale,
                role_weight=excluded.role_weight,
                episode_coverage=excluded.episode_coverage,
                dur_mult=excluded.dur_mult,
                edge_weight=excluded.edge_weight,
                edge_weight_share=excluded.edge_weight_share,
                iv_contrib_est=excluded.iv_contrib_est
        """,
            rows_contrib,
        )
        total_contrib += len(rows_contrib)

    if rows_summary:
        _write_summary_batch(conn, rows_summary)
        total_summary += len(rows_summary)

    conn.commit()
    logger.info("feat_credit_contribution_phase1_done", rows=total_contrib)

    # --- 2nd pass: UPDATE edge_weight_share and iv_contrib_est ---
    # prorate using total_edge_weight from feat_person_work_summary
    logger.info("feat_credit_contribution_phase2_start")
    update_sql = """
        UPDATE feat_credit_contribution
        SET
            edge_weight_share = CASE
                WHEN s.total_edge_weight > 0
                THEN feat_credit_contribution.edge_weight / s.total_edge_weight
                ELSE NULL END,
            iv_contrib_est = CASE
                WHEN s.total_edge_weight > 0
                THEN feat_credit_contribution.edge_weight / s.total_edge_weight * COALESCE(p.iv_score, 0)
                ELSE NULL END
        FROM feat_person_work_summary s
        LEFT JOIN feat_person_scores p ON p.person_id = s.person_id
        WHERE feat_credit_contribution.person_id = s.person_id
    """
    conn.execute(update_sql)
    conn.commit()
    logger.info(
        "feat_credit_contribution_computed",
        contrib_rows=total_contrib,
        summary_rows=total_summary,
    )
    return total_contrib, total_summary


def _write_summary_batch(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO feat_person_work_summary
            (person_id, n_distinct_works,
             total_production_scale, mean_production_scale, max_production_scale, best_work_anime_id,
             total_edge_weight, mean_edge_weight_per_work, max_edge_weight, top_contrib_anime_id,
             total_iv_contrib_est, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(person_id) DO UPDATE SET
            n_distinct_works=excluded.n_distinct_works,
            total_production_scale=excluded.total_production_scale,
            mean_production_scale=excluded.mean_production_scale,
            max_production_scale=excluded.max_production_scale,
            best_work_anime_id=excluded.best_work_anime_id,
            total_edge_weight=excluded.total_edge_weight,
            mean_edge_weight_per_work=excluded.mean_edge_weight_per_work,
            max_edge_weight=excluded.max_edge_weight,
            top_contrib_anime_id=excluded.top_contrib_anime_id,
            total_iv_contrib_est=excluded.total_iv_contrib_est,
            updated_at=CURRENT_TIMESTAMP
    """,
        rows,
    )


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
# =============================================================================


def _migrate_v37_credit_contribution_career_year(conn: sqlite3.Connection) -> None:
    """v37: add career year columns to feat_credit_contribution.

    Adds debut_year, career_year_at_credit, and is_debut_work; computes each
    person's debut year from the credits table and backfills the new columns.
    """
    for col_def in [
        "debut_year INTEGER",
        "career_year_at_credit INTEGER",
        "is_debut_work INTEGER",
    ]:
        try:
            conn.execute(f"ALTER TABLE feat_credit_contribution ADD COLUMN {col_def}")
        except Exception:
            pass  # already exists

    # load each person's debut year into memory
    debut_map: dict[str, int] = {
        r["person_id"]: r["debut_year"]
        for r in conn.execute("""
            SELECT person_id, MIN(credit_year) AS debut_year
            FROM credits
            WHERE credit_year IS NOT NULL AND credit_year > 1900
            GROUP BY person_id
        """).fetchall()
    }

    batch: list[tuple] = []
    for row in conn.execute(
        "SELECT person_id, anime_id, role, credit_year FROM feat_credit_contribution"
    ):
        debut = debut_map.get(row["person_id"])
        if debut is None or row["credit_year"] is None:
            continue
        cy = row["credit_year"] - debut
        is_debut = 1 if row["credit_year"] == debut else 0
        batch.append(
            (debut, cy, is_debut, row["person_id"], row["anime_id"], row["role"])
        )
        if len(batch) >= 10000:
            conn.executemany(
                """UPDATE feat_credit_contribution
                   SET debut_year=?, career_year_at_credit=?, is_debut_work=?
                   WHERE person_id=? AND anime_id=? AND role=?""",
                batch,
            )
            batch.clear()
    if batch:
        conn.executemany(
            """UPDATE feat_credit_contribution
               SET debut_year=?, career_year_at_credit=?, is_debut_work=?
               WHERE person_id=? AND anime_id=? AND role=?""",
            batch,
        )
    logger.info(
        "feat_credit_contribution_career_year_backfilled", persons=len(debut_map)
    )


def _migrate_v38_add_feat_work_context(conn: sqlite3.Connection) -> None:
    """v38: add feat_work_context table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_work_context (
            anime_id TEXT PRIMARY KEY,
            credit_year INTEGER,
            n_staff INTEGER,
            n_distinct_roles INTEGER,
            n_direction INTEGER,
            n_animation_supervision INTEGER,
            n_animation INTEGER,
            n_design INTEGER,
            n_technical INTEGER,
            n_art INTEGER,
            n_sound INTEGER,
            n_writing INTEGER,
            n_production INTEGER,
            n_other INTEGER,
            mean_career_year REAL,
            median_career_year REAL,
            max_career_year INTEGER,
            production_scale REAL,
            difficulty_score REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_work_context_year
            ON feat_work_context(credit_year);
    """)


def _migrate_v39_add_feat_person_role_progression(conn: sqlite3.Connection) -> None:
    """v39: add feat_person_role_progression table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_person_role_progression (
            person_id TEXT NOT NULL,
            role_category TEXT NOT NULL,
            first_year INTEGER,
            last_year INTEGER,
            peak_year INTEGER,
            n_works INTEGER,
            n_credits INTEGER,
            career_year_first INTEGER,
            still_active INTEGER,
            PRIMARY KEY (person_id, role_category)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_person
            ON feat_person_role_progression(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_role_prog_category
            ON feat_person_role_progression(role_category);
    """)


def _migrate_v40_add_feat_causal_estimates(conn: sqlite3.Connection) -> None:
    """v40: add feat_causal_estimates table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_causal_estimates (
            person_id TEXT PRIMARY KEY,
            peer_effect_boost REAL,
            career_friction REAL,
            era_fe REAL,
            era_deflated_iv REAL,
            opportunity_residual REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


# =============================================================================
# v37: compute career_year columns for feat_credit_contribution
# =============================================================================


def _backfill_credit_contribution_career_year(conn: sqlite3.Connection) -> None:
    """Compute and update career year columns in feat_credit_contribution.

    Call after compute_feat_credit_contribution INSERT.
    Only updates rows where debut_year IS NULL.
    """
    debut_map: dict[str, int] = {
        r["person_id"]: r["debut_year"]
        for r in conn.execute("""
            SELECT person_id, MIN(credit_year) AS debut_year
            FROM credits
            WHERE credit_year IS NOT NULL AND credit_year > 1900
            GROUP BY person_id
        """).fetchall()
    }

    batch: list[tuple] = []
    for row in conn.execute(
        """SELECT person_id, anime_id, role, credit_year
           FROM feat_credit_contribution
           WHERE debut_year IS NULL"""
    ):
        debut = debut_map.get(row["person_id"])
        if debut is None or row["credit_year"] is None:
            continue
        cy = row["credit_year"] - debut
        is_debut = 1 if row["credit_year"] == debut else 0
        batch.append(
            (debut, cy, is_debut, row["person_id"], row["anime_id"], row["role"])
        )
        if len(batch) >= 10000:
            conn.executemany(
                """UPDATE feat_credit_contribution
                   SET debut_year=?, career_year_at_credit=?, is_debut_work=?
                   WHERE person_id=? AND anime_id=? AND role=?""",
                batch,
            )
            batch.clear()
    if batch:
        conn.executemany(
            """UPDATE feat_credit_contribution
               SET debut_year=?, career_year_at_credit=?, is_debut_work=?
               WHERE person_id=? AND anime_id=? AND role=?""",
            batch,
        )
    conn.commit()


# =============================================================================
# v38: compute function for feat_work_context
# =============================================================================


def compute_feat_work_context(
    conn: sqlite3.Connection,
    current_year: int | None = None,
) -> int:
    """Aggregate per-work team statistics and save to feat_work_context.

    Requires career_year_at_credit to be populated first; call
    _backfill_credit_contribution_career_year() before this function.

    Args:
        conn: SQLite connection
        current_year: reference year (defaults to max credit_year in credits)

    Returns:
        number of rows written
    """
    import math
    import statistics

    from src.utils.config import DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER
    from src.utils.role_groups import ROLE_CATEGORY

    logger.info("feat_work_context_compute_start")

    if current_year is None:
        row = conn.execute(
            "SELECT MAX(credit_year) FROM credits WHERE credit_year IS NOT NULL"
        ).fetchone()
        current_year = row[0] if row and row[0] else 2024

    # anime metadata
    FORMAT_DUR: dict[str | None, float] = {
        "MOVIE": DURATION_MAX_MULTIPLIER,
        "TV_SHORT": 0.25,
        "SPECIAL": 0.8,
        "ONA": 0.8,
        "TV": 0.8,
        "OVA": 1.0,
        "MUSIC": 0.25,
        None: 1.0,
    }
    anime_meta: dict[str, dict] = {}
    for r in conn.execute("SELECT id, episodes, duration, format FROM anime"):
        dur = r["duration"]
        dm = (
            min(dur / DURATION_BASELINE_MINUTES, DURATION_MAX_MULTIPLIER)
            if dur is not None
            else FORMAT_DUR.get(r["format"], 1.0)
        )
        anime_meta[r["id"]] = {
            "eps": r["episodes"] or 1,
            "dm": dm,
            "year": None,  # populated below
        }

    # representative year per anime (min credit_year from credits)
    for r in conn.execute(
        "SELECT anime_id, MIN(credit_year) AS yr FROM credits "
        "WHERE credit_year IS NOT NULL GROUP BY anime_id"
    ):
        if r["anime_id"] in anime_meta:
            anime_meta[r["anime_id"]]["year"] = r["yr"]

    # staff_count per anime
    staff_count: dict[str, int] = {
        r["anime_id"]: r["cnt"]
        for r in conn.execute(
            "SELECT anime_id, COUNT(DISTINCT person_id) AS cnt FROM credits GROUP BY anime_id"
        )
    }

    # fetch role and career year from feat_credit_contribution
    # aggregate (person_id, anime_id, role, career_year_at_credit)
    CATEGORY_COLS = {
        "direction": "n_direction",
        "animation_supervision": "n_animation_supervision",
        "animation": "n_animation",
        "design": "n_design",
        "technical": "n_technical",
        "art": "n_art",
        "sound": "n_sound",
        "writing": "n_writing",
        "production": "n_production",
        "production_management": "n_production",  # merge into production
        "finishing": "n_other",
        "editing": "n_other",
        "settings": "n_other",
        "other": "n_other",
    }
    COL_ZERO = {v: 0 for v in CATEGORY_COLS.values()}

    # anime_id → {person_id: (max_career_year, role_categories)}
    anime_data: dict[str, dict] = {}

    for row in conn.execute(
        """SELECT fcc.anime_id, fcc.person_id, fcc.role, fcc.career_year_at_credit
           FROM feat_credit_contribution fcc
           ORDER BY fcc.anime_id"""
    ):
        aid = row["anime_id"]
        pid = row["person_id"]
        if aid not in anime_data:
            anime_data[aid] = {}
        if pid not in anime_data[aid]:
            anime_data[aid][pid] = {"max_cy": None, "cats": set()}
        cy = row["career_year_at_credit"]
        if cy is not None:
            prev = anime_data[aid][pid]["max_cy"]
            anime_data[aid][pid]["max_cy"] = cy if prev is None else max(prev, cy)
        cat = ROLE_CATEGORY.get(row["role"], "other")
        anime_data[aid][pid]["cats"].add(cat)

    rows_out: list[tuple] = []
    for aid, persons in anime_data.items():
        meta = anime_meta.get(aid, {"eps": 1, "dm": 1.0, "year": None})
        sc = staff_count.get(aid, len(persons))

        # production_scale
        ps = math.log1p(sc) * math.log1p(meta["eps"]) * meta["dm"]

        # career years (one value per person — use max role career year)
        career_years = [
            v["max_cy"] for v in persons.values() if v["max_cy"] is not None
        ]
        mean_cy = (
            round(sum(career_years) / len(career_years), 2) if career_years else None
        )
        median_cy = round(statistics.median(career_years), 2) if career_years else None
        max_cy = max(career_years) if career_years else None

        # role category counts (unique persons per category)
        col_counts = dict(COL_ZERO)
        role_set: set[str] = set()
        for pdata in persons.values():
            for cat in pdata["cats"]:
                col = CATEGORY_COLS.get(cat, "n_other")
                col_counts[col] += 1
                role_set.add(cat)

        rows_out.append(
            (
                aid,
                meta["year"],
                sc,
                len(role_set),
                col_counts["n_direction"],
                col_counts["n_animation_supervision"],
                col_counts["n_animation"],
                col_counts["n_design"],
                col_counts["n_technical"],
                col_counts["n_art"],
                col_counts["n_sound"],
                col_counts["n_writing"],
                col_counts["n_production"],
                col_counts["n_other"],
                mean_cy,
                median_cy,
                max_cy,
                round(ps, 6),
                None,  # difficulty_score: updated separately from era_effects
            )
        )

    conn.executemany(
        """INSERT INTO feat_work_context
               (anime_id, credit_year, n_staff, n_distinct_roles,
                n_direction, n_animation_supervision, n_animation, n_design,
                n_technical, n_art, n_sound, n_writing, n_production, n_other,
                mean_career_year, median_career_year, max_career_year,
                production_scale, difficulty_score, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
           ON CONFLICT(anime_id) DO UPDATE SET
               credit_year=excluded.credit_year,
               n_staff=excluded.n_staff,
               n_distinct_roles=excluded.n_distinct_roles,
               n_direction=excluded.n_direction,
               n_animation_supervision=excluded.n_animation_supervision,
               n_animation=excluded.n_animation,
               n_design=excluded.n_design,
               n_technical=excluded.n_technical,
               n_art=excluded.n_art,
               n_sound=excluded.n_sound,
               n_writing=excluded.n_writing,
               n_production=excluded.n_production,
               n_other=excluded.n_other,
               mean_career_year=excluded.mean_career_year,
               median_career_year=excluded.median_career_year,
               max_career_year=excluded.max_career_year,
               production_scale=excluded.production_scale,
               updated_at=CURRENT_TIMESTAMP""",
        rows_out,
    )
    conn.commit()
    logger.info("feat_work_context_computed", rows=len(rows_out))
    return len(rows_out)


# =============================================================================
# v39: compute function for feat_person_role_progression
# =============================================================================


def compute_feat_person_role_progression(
    conn: sqlite3.Connection,
    current_year: int | None = None,
    active_threshold_years: int = 3,
    batch_size: int = 10000,
) -> int:
    """Aggregate per-person × role-category time-series progression into feat_person_role_progression.

    Args:
        conn: SQLite connection
        current_year: reference year (defaults to max credit_year in credits)
        active_threshold_years: set still_active=1 if last credit is within this many years
        batch_size: INSERT batch size

    Returns:
        number of rows written
    """
    from collections import defaultdict

    from src.utils.role_groups import ROLE_CATEGORY

    logger.info("feat_person_role_progression_compute_start")

    if current_year is None:
        row = conn.execute(
            "SELECT MAX(credit_year) FROM credits WHERE credit_year IS NOT NULL"
        ).fetchone()
        current_year = row[0] if row and row[0] else 2024

    # debut year per person
    debut_map: dict[str, int] = {
        r["person_id"]: r["debut_year"]
        for r in conn.execute("""
            SELECT person_id, MIN(credit_year) AS debut_year
            FROM credits
            WHERE credit_year IS NOT NULL AND credit_year > 1900
            GROUP BY person_id
        """).fetchall()
    }

    # (person_id, role_category) → {year: n_works}
    # aggregate directly from credits
    pid_cat_years: dict[tuple[str, str], dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    pid_cat_credits: dict[tuple[str, str], int] = defaultdict(int)

    for row in conn.execute(
        """SELECT c.person_id, c.role, c.credit_year,
                  COUNT(DISTINCT c.anime_id) AS n_works
           FROM credits c
           WHERE c.credit_year IS NOT NULL AND c.credit_year > 1900
           GROUP BY c.person_id, c.role, c.credit_year"""
    ):
        cat = ROLE_CATEGORY.get(row["role"], "other")
        key = (row["person_id"], cat)
        pid_cat_years[key][row["credit_year"]] += row["n_works"]
        pid_cat_credits[key] += 1

    rows_out: list[tuple] = []
    for (pid, cat), year_works in pid_cat_years.items():
        if not year_works:
            continue
        first_year = min(year_works)
        last_year = max(year_works)
        peak_year = max(year_works, key=year_works.__getitem__)
        n_works = sum(year_works.values())
        n_credits = pid_cat_credits[(pid, cat)]
        debut = debut_map.get(pid)
        career_year_first = first_year - debut if debut is not None else None
        still_active = 1 if current_year - last_year <= active_threshold_years else 0

        rows_out.append(
            (
                pid,
                cat,
                first_year,
                last_year,
                peak_year,
                n_works,
                n_credits,
                career_year_first,
                still_active,
            )
        )

        if len(rows_out) >= batch_size:
            conn.executemany(
                """INSERT INTO feat_person_role_progression
                       (person_id, role_category, first_year, last_year, peak_year,
                        n_works, n_credits, career_year_first, still_active)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(person_id, role_category) DO UPDATE SET
                       first_year=excluded.first_year,
                       last_year=excluded.last_year,
                       peak_year=excluded.peak_year,
                       n_works=excluded.n_works,
                       n_credits=excluded.n_credits,
                       career_year_first=excluded.career_year_first,
                       still_active=excluded.still_active""",
                rows_out,
            )
            rows_out.clear()

    if rows_out:
        conn.executemany(
            """INSERT INTO feat_person_role_progression
                   (person_id, role_category, first_year, last_year, peak_year,
                    n_works, n_credits, career_year_first, still_active)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT(person_id, role_category) DO UPDATE SET
                   first_year=excluded.first_year,
                   last_year=excluded.last_year,
                   peak_year=excluded.peak_year,
                   n_works=excluded.n_works,
                   n_credits=excluded.n_credits,
                   career_year_first=excluded.career_year_first,
                   still_active=excluded.still_active""",
            rows_out,
        )

    conn.commit()
    total = conn.execute(
        "SELECT COUNT(*) FROM feat_person_role_progression"
    ).fetchone()[0]
    logger.info("feat_person_role_progression_computed", rows=total)
    return total


# =============================================================================
# v40: upsert function for feat_causal_estimates (called from the pipeline)
# =============================================================================


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
# =============================================================================


def _migrate_v41_add_feat_cluster_membership(conn: sqlite3.Connection) -> None:
    """v41: add feat_cluster_membership table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_cluster_membership (
            person_id TEXT PRIMARY KEY,
            community_id INTEGER,
            career_track TEXT,
            growth_trend TEXT,
            studio_cluster_id INTEGER,
            studio_cluster_name TEXT,
            cooccurrence_group_id INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_community
            ON feat_cluster_membership(community_id);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_career_track
            ON feat_cluster_membership(career_track);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_growth
            ON feat_cluster_membership(growth_trend);
        CREATE INDEX IF NOT EXISTS idx_feat_cluster_studio
            ON feat_cluster_membership(studio_cluster_id);
    """)


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


def _migrate_v42_add_feat_birank_annual(conn: sqlite3.Connection) -> None:
    """v42: add feat_birank_annual table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_birank_annual (
            person_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            birank REAL NOT NULL,
            raw_pagerank REAL,
            graph_size INTEGER,
            n_credits_cumulative INTEGER,
            PRIMARY KEY (person_id, year)
        );
        CREATE INDEX IF NOT EXISTS idx_feat_birank_annual_year
            ON feat_birank_annual(year);
    """)
    conn.commit()


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
# =============================================================================


def _migrate_v43_add_birank_compute_state(conn: sqlite3.Connection) -> None:
    """v43: add birank_compute_state table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS birank_compute_state (
            year INTEGER PRIMARY KEY,
            credit_count INTEGER NOT NULL,
            anime_count INTEGER NOT NULL,
            person_count INTEGER NOT NULL,
            computed_at REAL NOT NULL
        );
    """)
    conn.commit()


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


# (format_group, episode_bracket) → base_score
# episode_bracket: rough bucketing by episode count
#   TV:      4-cour+(48+) → major, 2-cour(24-47) → large, 1-cour(12-23) → standard,
#            half-cour(6-11) → small, <6 → micro
#   ONA:     24+ → large, 12-23 → standard, 6-11 → small, <6 → micro
#   OVA:     6+ → standard, 3-5 → small, 1-2 → micro
#   MOVIE:   feature(90min+) → major, standard(45-89min) → large, short(<45min) → standard
#   SPECIAL: small
#   TV_SHORT:follows TV by episode count but one step lower
#   MUSIC:   micro
def _compute_scale_raw(
    fmt: str | None,
    episodes: int | None,
    duration: int | None,  # minutes per episode
) -> float:
    """Compute a continuous scale score from format + episodes + duration.

    Does not use staff count, so works fairly even for incompletely scraped titles.

    Returns:
        scale_raw: continuous value in the range 0.0–5.0
    """
    eps = episodes or 1
    dur = duration or 0  # minutes per episode
    total_min = eps * dur  # total duration (minutes)

    group = _FORMAT_GROUP.get(fmt, "other")

    if group == "movie":
        # movie: evaluated by total duration (episode count is usually 1, so meaningless)
        if total_min >= 120:
            return 5.0
        elif total_min >= 90:
            return 4.5
        elif total_min >= 60:
            return 4.0
        elif total_min >= 30:
            return 3.0
        else:
            return 2.5  # short film

    elif group == "tv":
        if eps >= 48:
            return 5.0  # 4-cour+ (4+ cours)
        elif eps >= 36:
            return 4.5  # 3-cour
        elif eps >= 24:
            return 4.0  # 2-cour
        elif eps >= 13:
            return 3.0  # 1-cour (13 episodes)
        elif eps >= 10:
            return 2.5  # ~12 episodes
        elif eps >= 6:
            return 2.0  # half-cour
        else:
            return 1.5  # 1-5 episodes

    elif group == "ona":
        if eps >= 24:
            return 4.0
        elif eps >= 12:
            return 3.0
        elif eps >= 6:
            return 2.0
        elif eps >= 3:
            return 1.5
        else:
            return 1.0

    elif group == "ova":
        if eps >= 6:
            return 3.0
        elif eps >= 3:
            return 2.0
        else:
            return 1.5

    elif group == "tv_short":
        # TV_SHORT: short runtime per episode even with many episodes → one step lower
        if eps >= 48:
            return 4.0
        elif eps >= 24:
            return 3.0
        elif eps >= 12:
            return 2.0
        else:
            return 1.5

    elif group == "special":
        # SPECIAL: essentially standalone; fine-tuned by total duration
        if total_min >= 60:
            return 2.5
        else:
            return 1.5

    elif group == "music":
        return 1.0

    else:
        return 1.5  # OTHER/unknown


# scale_raw → (tier, label)
_TIER_THRESHOLDS: list[tuple[float, int, str]] = [
    (4.5, 5, "major"),  # scale_raw >= 4.5 → tier 5
    (3.5, 4, "large"),  # scale_raw >= 3.5 → tier 4
    (2.5, 3, "standard"),  # scale_raw >= 2.5 → tier 3
    (1.5, 2, "small"),  # scale_raw >= 1.5 → tier 2
    (0.0, 1, "micro"),  # below all thresholds → tier 1
]


def _scale_raw_to_tier(raw: float) -> tuple[int, str]:
    for threshold, tier, label in _TIER_THRESHOLDS:
        if raw >= threshold:
            return tier, label
    return 1, "micro"


def _migrate_v44_add_work_scale_tier(conn: sqlite3.Connection) -> None:
    """v44: add work scale tier columns to feat_work_context and backfill.

    Computed from format + episodes + duration only, without staff count.
    This allows accurate scale assessment even for incompletely scraped titles.
    """
    for col_def in [
        "scale_tier INTEGER",
        "scale_label TEXT",
        "scale_raw REAL",
        "format_group TEXT",
    ]:
        try:
            conn.execute(f"ALTER TABLE feat_work_context ADD COLUMN {col_def}")
        except Exception:
            pass

    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_feat_work_context_tier ON feat_work_context(scale_tier)"
        )
    except Exception:
        pass

    compute_feat_work_scale_tier(conn)


def compute_feat_work_scale_tier(
    conn: sqlite3.Connection,
    batch_size: int = 5000,
) -> int:
    """Compute scale_tier for all works and write to feat_work_context.

    Also processes works that appear in credits but are not yet in feat_work_context,
    inserting them from the anime table directly.

    Returns:
        number of rows updated
    """
    logger.info("work_scale_tier_compute_start")

    rows = conn.execute("SELECT id, format, episodes, duration FROM anime").fetchall()

    updates: list[tuple] = []
    inserts: list[tuple] = []  # works not yet in feat_work_context
    existing = {
        r["anime_id"]
        for r in conn.execute("SELECT anime_id FROM feat_work_context").fetchall()
    }

    for row in rows:
        aid = row["id"]
        raw = _compute_scale_raw(row["format"], row["episodes"], row["duration"])
        tier, label = _scale_raw_to_tier(raw)
        grp = _FORMAT_GROUP.get(row["format"], "other")

        if aid in existing:
            updates.append((tier, label, round(raw, 2), grp, aid))
        else:
            # in anime table but not in feat_work_context → INSERT minimal row
            inserts.append((aid, tier, label, round(raw, 2), grp))

        if len(updates) >= batch_size:
            conn.executemany(
                """UPDATE feat_work_context
                   SET scale_tier=?, scale_label=?, scale_raw=?, format_group=?
                   WHERE anime_id=?""",
                updates,
            )
            updates.clear()
        if len(inserts) >= batch_size:
            conn.executemany(
                """INSERT OR IGNORE INTO feat_work_context
                       (anime_id, scale_tier, scale_label, scale_raw, format_group)
                   VALUES (?,?,?,?,?)""",
                inserts,
            )
            inserts.clear()

    if updates:
        conn.executemany(
            """UPDATE feat_work_context
               SET scale_tier=?, scale_label=?, scale_raw=?, format_group=?
               WHERE anime_id=?""",
            updates,
        )
    if inserts:
        conn.executemany(
            """INSERT OR IGNORE INTO feat_work_context
                   (anime_id, scale_tier, scale_label, scale_raw, format_group)
               VALUES (?,?,?,?,?)""",
            inserts,
        )

    conn.commit()
    total = conn.execute(
        "SELECT COUNT(*) FROM feat_work_context WHERE scale_tier IS NOT NULL"
    ).fetchone()[0]
    logger.info("work_scale_tier_computed", rows=total)
    return total


# ── v45: feat_career_gaps ───────────────────────────────────────────


def _migrate_v45_add_feat_career_gaps(conn: sqlite3.Connection) -> None:
    """v45: feat_career_gaps — career gap (exit / semi-exit / return) statistics table."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_career_gaps (
            person_id TEXT NOT NULL,
            gap_start_year INTEGER NOT NULL,  -- last credit year
            gap_end_year INTEGER,             -- return year (NULL = not yet returned)
            gap_length INTEGER NOT NULL,      -- gap length in years
            returned INTEGER NOT NULL DEFAULT 0,  -- whether the person returned (0/1)
            gap_type TEXT NOT NULL,           -- 'semi_exit' (3-4yr) / 'exit' (5+yr)
            PRIMARY KEY (person_id, gap_start_year)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_career_gaps_type
            ON feat_career_gaps(gap_type);
        CREATE INDEX IF NOT EXISTS idx_feat_career_gaps_returned
            ON feat_career_gaps(returned);
    """)
    conn.commit()


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


# ── v46: ann_id columns ─────────────────────────────────────────────


def _migrate_v46_add_ann_ids(conn: sqlite3.Connection) -> None:
    """v46: add ann_id column + unique index to anime and persons."""
    for stmt in [
        "ALTER TABLE anime ADD COLUMN ann_id INTEGER",
        "ALTER TABLE persons ADD COLUMN ann_id INTEGER",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_anime_ann_id ON anime(ann_id) WHERE ann_id IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_persons_ann_id ON persons(ann_id) WHERE ann_id IS NOT NULL",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # already exists


# ── v47: allcinema_id columns ─────────────────────────────────────────────────


def _migrate_v47_add_allcinema_ids(conn: sqlite3.Connection) -> None:
    """v47: add allcinema_id column + unique index to anime and persons."""
    for stmt in [
        "ALTER TABLE anime ADD COLUMN allcinema_id INTEGER",
        "ALTER TABLE persons ADD COLUMN allcinema_id INTEGER",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_anime_allcinema_id ON anime(allcinema_id) WHERE allcinema_id IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_persons_allcinema_id ON persons(allcinema_id) WHERE allcinema_id IS NOT NULL",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # already exists


# ── v48: src_* source tables ──────────────────────────────────────────────────


def _migrate_v48_add_source_tables(conn: sqlite3.Connection) -> None:
    """v48: add Bronze-layer source tables (applied to existing DBs)."""
    stmts = [
        # src_anilist_*
        """CREATE TABLE IF NOT EXISTS src_anilist_anime (
            anilist_id INTEGER PRIMARY KEY, title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '', year INTEGER, season TEXT,
            episodes INTEGER, format TEXT, status TEXT, start_date TEXT,
            end_date TEXT, duration INTEGER, source TEXT, description TEXT,
            score REAL, genres TEXT DEFAULT '[]', tags TEXT DEFAULT '[]',
            studios TEXT DEFAULT '[]', synonyms TEXT DEFAULT '[]',
            cover_large TEXT, cover_medium TEXT, banner TEXT,
            popularity INTEGER, favourites INTEGER, site_url TEXT, mal_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_anilist_persons (
            anilist_id INTEGER PRIMARY KEY, name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '', aliases TEXT DEFAULT '[]',
            date_of_birth TEXT, age INTEGER, gender TEXT,
            years_active TEXT DEFAULT '[]', hometown TEXT, blood_type TEXT,
            description TEXT, image_large TEXT, image_medium TEXT,
            favourites INTEGER, site_url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_anilist_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anilist_anime_id INTEGER NOT NULL, anilist_person_id INTEGER NOT NULL,
            role TEXT NOT NULL, role_raw TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anilist_anime_id, anilist_person_id, role))""",
        # src_ann_*
        """CREATE TABLE IF NOT EXISTS src_ann_anime (
            ann_id INTEGER PRIMARY KEY, title_en TEXT NOT NULL DEFAULT '',
            title_ja TEXT NOT NULL DEFAULT '', year INTEGER, episodes INTEGER,
            format TEXT, genres TEXT DEFAULT '[]', start_date TEXT, end_date TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_ann_persons (
            ann_id INTEGER PRIMARY KEY, name_en TEXT NOT NULL DEFAULT '',
            name_ja TEXT NOT NULL DEFAULT '', date_of_birth TEXT, hometown TEXT,
            blood_type TEXT, website TEXT, description TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_ann_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ann_anime_id INTEGER NOT NULL, ann_person_id INTEGER NOT NULL,
            name_en TEXT NOT NULL DEFAULT '', role TEXT NOT NULL, role_raw TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ann_anime_id, ann_person_id, role))""",
        # src_allcinema_*
        """CREATE TABLE IF NOT EXISTS src_allcinema_anime (
            allcinema_id INTEGER PRIMARY KEY, title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER, start_date TEXT, synopsis TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_allcinema_persons (
            allcinema_id INTEGER PRIMARY KEY, name_ja TEXT NOT NULL DEFAULT '',
            yomigana TEXT NOT NULL DEFAULT '', name_en TEXT NOT NULL DEFAULT '',
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_allcinema_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            allcinema_anime_id INTEGER NOT NULL, allcinema_person_id INTEGER NOT NULL,
            name_ja TEXT NOT NULL DEFAULT '', name_en TEXT NOT NULL DEFAULT '',
            job_name TEXT NOT NULL, job_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(allcinema_anime_id, allcinema_person_id, job_name))""",
        # src_seesaawiki_*
        """CREATE TABLE IF NOT EXISTS src_seesaawiki_anime (
            id TEXT PRIMARY KEY, title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER, episodes INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_seesaawiki_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_src_id TEXT NOT NULL, person_name TEXT NOT NULL,
            role TEXT NOT NULL, role_raw TEXT, episode INTEGER DEFAULT -1,
            affiliation TEXT, is_company INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(anime_src_id, person_name, role, episode))""",
        # src_keyframe_*
        """CREATE TABLE IF NOT EXISTS src_keyframe_anime (
            slug TEXT PRIMARY KEY, title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '', anilist_id INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS src_keyframe_credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyframe_slug TEXT NOT NULL, kf_person_id INTEGER NOT NULL,
            name_ja TEXT NOT NULL DEFAULT '', name_en TEXT NOT NULL DEFAULT '',
            role_ja TEXT NOT NULL, role_en TEXT NOT NULL DEFAULT '',
            episode INTEGER DEFAULT -1,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(keyframe_slug, kf_person_id, role_ja, episode))""",
        # indexes
        "CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_anime ON src_anilist_credits(anilist_anime_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_anilist_credits_person ON src_anilist_credits(anilist_person_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_ann_credits_anime ON src_ann_credits(ann_anime_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_ann_credits_person ON src_ann_credits(ann_person_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_anime ON src_allcinema_credits(allcinema_anime_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_allcinema_credits_person ON src_allcinema_credits(allcinema_person_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_seesaawiki_credits_anime ON src_seesaawiki_credits(anime_src_id)",
        "CREATE INDEX IF NOT EXISTS idx_src_keyframe_credits_slug ON src_keyframe_credits(keyframe_slug)",
    ]
    for stmt in stmts:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass


def _migrate_v49_add_silver_layer(conn: sqlite3.Connection) -> None:
    """v49: add Silver-layer and Gold-layer table groups.

    anime_analysis:  analysis-only table with no score columns (silver.analysis)
    anime_display:   display-only table for score/popularity/description (silver.display)
    meta_lineage:    data lineage tracking for Gold tables
    meta_common_*:   shared Gold tables
    meta_policy_*:   Gold tables for the Policy Brief audience
    meta_hr_*:       Gold tables for the HR Brief audience
    meta_biz_*:      Gold tables for the Business Brief audience
    """
    stmts = [
        """CREATE TABLE IF NOT EXISTS anime_analysis (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER, season TEXT, quarter INTEGER,
            episodes INTEGER, format TEXT, duration INTEGER,
            start_date TEXT, end_date TEXT, status TEXT, source TEXT,
            work_type TEXT, scale_class TEXT,
            mal_id INTEGER, anilist_id INTEGER, ann_id INTEGER,
            allcinema_id INTEGER, madb_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id), UNIQUE(anilist_id))""",
        "CREATE INDEX IF NOT EXISTS idx_anime_analysis_year ON anime_analysis(year)",
        "CREATE INDEX IF NOT EXISTS idx_anime_analysis_format ON anime_analysis(format)",
        "CREATE INDEX IF NOT EXISTS idx_anime_analysis_anilist ON anime_analysis(anilist_id)",
        """CREATE TABLE IF NOT EXISTS anime_display (
            id TEXT PRIMARY KEY,
            score REAL, popularity INTEGER, popularity_rank INTEGER,
            favourites INTEGER, mean_score INTEGER,
            description TEXT, cover_large TEXT, cover_extra_large TEXT,
            cover_medium TEXT, cover_large_path TEXT, banner TEXT,
            banner_path TEXT, site_url TEXT,
            genres TEXT DEFAULT '[]', tags TEXT DEFAULT '[]',
            studios TEXT DEFAULT '[]', synonyms TEXT DEFAULT '[]',
            country_of_origin TEXT, is_adult INTEGER,
            relations_json TEXT, external_links_json TEXT, rankings_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id) REFERENCES anime_analysis(id) ON DELETE CASCADE)""",
        "CREATE INDEX IF NOT EXISTS idx_anime_display_score ON anime_display(score)",
        """CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL,
            ci_method TEXT, null_model TEXT, holdout_method TEXT,
            row_count INTEGER, notes TEXT)""",
        # Gold tables — common
        """CREATE TABLE IF NOT EXISTS meta_common_person_parameters (
            person_id TEXT PRIMARY KEY,
            scale_reach_pct REAL, scale_reach_ci_low REAL, scale_reach_ci_high REAL,
            collab_width_pct REAL, collab_width_ci_low REAL, collab_width_ci_high REAL,
            continuity_pct REAL, continuity_ci_low REAL, continuity_ci_high REAL,
            mentor_contribution_pct REAL, mentor_contribution_ci_low REAL, mentor_contribution_ci_high REAL,
            centrality_pct REAL, centrality_ci_low REAL, centrality_ci_high REAL,
            trust_accum_pct REAL, trust_accum_ci_low REAL, trust_accum_ci_high REAL,
            role_evolution_pct REAL, role_evolution_ci_low REAL, role_evolution_ci_high REAL,
            genre_specialization_pct REAL, genre_specialization_ci_low REAL, genre_specialization_ci_high REAL,
            recent_activity_pct REAL, recent_activity_ci_low REAL, recent_activity_ci_high REAL,
            compatibility_pct REAL, compatibility_ci_low REAL, compatibility_ci_high REAL,
            archetype TEXT, archetype_confidence REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        # Gold tables — policy
        """CREATE TABLE IF NOT EXISTS meta_policy_attrition (
            cohort_year INTEGER NOT NULL, treatment TEXT NOT NULL,
            ate REAL, ate_ci_low REAL, ate_ci_high REAL,
            hazard_ratio REAL, hr_ci_low REAL, hr_ci_high REAL,
            n_treated INTEGER, n_control INTEGER, p_value REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort_year, treatment))""",
        """CREATE TABLE IF NOT EXISTS meta_policy_monopsony (
            year INTEGER NOT NULL, studio TEXT NOT NULL,
            hhi REAL, hhi_star REAL, hhi_ci_low REAL, hhi_ci_high REAL,
            logit_stay_beta REAL, logit_stay_se REAL, n_persons INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, studio))""",
        """CREATE TABLE IF NOT EXISTS meta_policy_gender (
            transition_stage TEXT NOT NULL, cohort TEXT NOT NULL,
            survival_prob REAL, survival_ci_low REAL, survival_ci_high REAL,
            log_rank_chi2 REAL, log_rank_p REAL, n_female INTEGER, n_male INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transition_stage, cohort))""",
        """CREATE TABLE IF NOT EXISTS meta_policy_generation (
            cohort TEXT NOT NULL, career_year_bin INTEGER NOT NULL,
            survival_rate REAL, survival_ci_low REAL, survival_ci_high REAL,
            n_at_risk INTEGER, n_events INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cohort, career_year_bin))""",
        # Gold tables — hr
        """CREATE TABLE IF NOT EXISTS meta_hr_studio_benchmark (
            studio TEXT NOT NULL, year INTEGER NOT NULL,
            r5_retention REAL, r5_ci_low REAL, r5_ci_high REAL,
            value_added REAL, va_ci_low REAL, va_ci_high REAL,
            h_score REAL, attraction_rate REAL, n_persons INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (studio, year))""",
        """CREATE TABLE IF NOT EXISTS meta_hr_mentor_card (
            director_id TEXT PRIMARY KEY,
            mentor_score REAL, mentor_ci_low REAL, mentor_ci_high REAL,
            null_permutation_p REAL, n_mentees INTEGER, n_works INTEGER, archetype TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS meta_hr_attrition_risk (
            person_id TEXT PRIMARY KEY,
            predicted_risk REAL, risk_ci_low REAL, risk_ci_high REAL, c_index REAL,
            shap_feature1 TEXT, shap_value1 REAL,
            shap_feature2 TEXT, shap_value2 REAL,
            shap_feature3 TEXT, shap_value3 REAL,
            shap_feature4 TEXT, shap_value4 REAL,
            shap_feature5 TEXT, shap_value5 REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS meta_hr_succession (
            veteran_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
            successor_score REAL, role TEXT, overlap_works INTEGER, career_gap_years REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (veteran_id, candidate_id))""",
        # Gold tables — biz
        """CREATE TABLE IF NOT EXISTS meta_biz_whitespace (
            genre TEXT NOT NULL, year INTEGER NOT NULL,
            cagr REAL, cagr_ci_low REAL, cagr_ci_high REAL,
            penetration REAL, whitespace_score REAL, n_anime INTEGER, n_staff INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (genre, year))""",
        """CREATE TABLE IF NOT EXISTS meta_biz_undervalued (
            person_id TEXT PRIMARY KEY,
            undervaluation_score REAL, archetype TEXT,
            network_reach REAL, opportunity_residual REAL, career_band TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS meta_biz_trust_entry (
            gatekeeper_id TEXT PRIMARY KEY,
            gatekeeper_score REAL, reach_score REAL,
            n_new_entrants INTEGER, avg_entry_speed REAL, community_diversity REAL,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        """CREATE TABLE IF NOT EXISTS meta_biz_team_template (
            cluster_id TEXT NOT NULL, tier TEXT NOT NULL,
            role_distribution TEXT, avg_career_years REAL, silhouette_score REAL,
            n_teams INTEGER, representative_works TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cluster_id, tier))""",
        """CREATE TABLE IF NOT EXISTS meta_biz_independent_unit (
            community_id TEXT PRIMARY KEY,
            coverage REAL, density REAL, value_generated REAL,
            n_members INTEGER, n_works INTEGER, core_studio TEXT,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    ]
    for stmt in stmts:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass

    # Dynamically detect which columns exist in the anime table
    # (test fixtures may have a minimal schema without all columns)
    anime_cols = {row[1] for row in conn.execute("PRAGMA table_info(anime)").fetchall()}

    # Columns to copy into anime_analysis (only those that exist in anime)
    _analysis_candidates = [
        "id",
        "title_ja",
        "title_en",
        "year",
        "season",
        "quarter",
        "episodes",
        "work_type",
        "scale_class",
        "mal_id",
        "anilist_id",
        "ann_id",
        "allcinema_id",
        "madb_id",
    ]
    analysis_cols = [c for c in _analysis_candidates if c in anime_cols]
    if analysis_cols:
        col_list = ", ".join(analysis_cols)
        conn.execute(
            f"INSERT OR IGNORE INTO anime_analysis ({col_list}) SELECT {col_list} FROM anime"
        )

    # Backfill format/duration/start_date/end_date/status/source from src_anilist_anime
    # (only if that bronze table exists)
    src_anilist_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='src_anilist_anime'"
    ).fetchone()
    if src_anilist_exists:
        conn.execute("""
            UPDATE anime_analysis
            SET
                format    = COALESCE(anime_analysis.format,    (SELECT format     FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id)),
                duration  = COALESCE(anime_analysis.duration,  (SELECT duration   FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id)),
                start_date= COALESCE(anime_analysis.start_date,(SELECT start_date FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id)),
                end_date  = COALESCE(anime_analysis.end_date,  (SELECT end_date   FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id)),
                status    = COALESCE(anime_analysis.status,    (SELECT status     FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id)),
                source    = COALESCE(anime_analysis.source,    (SELECT source     FROM src_anilist_anime s WHERE s.anilist_id = anime_analysis.anilist_id))
            WHERE anime_analysis.anilist_id IS NOT NULL
        """)

    # Backfill work_type/scale_class if those columns exist in anime
    if "work_type" in anime_cols:
        conn.execute("""
            UPDATE anime_analysis
            SET
                work_type   = COALESCE(anime_analysis.work_type,   (SELECT work_type   FROM anime a WHERE a.id = anime_analysis.id)),
                scale_class = COALESCE(anime_analysis.scale_class, (SELECT scale_class FROM anime a WHERE a.id = anime_analysis.id))
            WHERE EXISTS (SELECT 1 FROM anime a WHERE a.id = anime_analysis.id)
        """)

    # Columns to copy into anime_display (score + display; only those that exist in anime)
    _display_candidates = [
        "id",
        "score",
        "popularity_rank",
        "favourites",
        "mean_score",
        "description",
        "cover_large",
        "cover_extra_large",
        "cover_medium",
        "cover_large_path",
        "banner",
        "banner_path",
        "site_url",
        "genres",
        "tags",
        "studios",
        "synonyms",
        "country_of_origin",
        "is_adult",
        "relations_json",
        "external_links_json",
        "rankings_json",
    ]
    display_cols = [c for c in _display_candidates if c in anime_cols]
    if display_cols and "id" in display_cols:
        col_list = ", ".join(display_cols)
        conn.execute(
            f"INSERT OR IGNORE INTO anime_display ({col_list})"
            f" SELECT {col_list} FROM anime WHERE id IN (SELECT id FROM anime_analysis)"
        )

    n_analysis = conn.execute("SELECT COUNT(*) FROM anime_analysis").fetchone()[0]
    n_display = conn.execute("SELECT COUNT(*) FROM anime_display").fetchone()[0]
    logger.info(
        "v49_silver_layer_populated", anime_analysis=n_analysis, anime_display=n_display
    )


# ── src_* table write functions ────────────────────────────────────────────
# Write-only path to the Bronze layer (raw scrape data).
# These functions never touch canonical tables (anime/persons/credits).


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


def _migrate_v50_canonical_silver(conn: sqlite3.Connection) -> None:
    """v50: canonical silver normalization — sources/roles lookups, external_ids,
    person_aliases, anime_genres, anime_tags, credits.evidence_source rename.

    See detailed_todo.md §2.3 (Task 1-3) and §1.4 (N-1 through N-4, C-1, E-3).

    This migration is intentionally additive where dropping legacy tables
    would break ~29 analysis modules still using ``anime.score`` and the
    ``Anime`` Pydantic shim. The destructive drops (``anime_display``,
    legacy ``anime``, external-ID columns on silver ``anime``) are deferred
    to a v51 migration once those consumers are updated.
    """
    import json as _json

    # --- (E-3) Reversible: snapshot legacy tables before any change --------
    _archive_targets = (
        "anime",
        "anime_display",
        "anime_analysis",
        "credits",
        "persons",
        "scores",
    )
    for tbl in _archive_targets:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (tbl,),
        ).fetchone()
        if exists:
            archive = f"_archive_v49_{tbl}"
            try:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {archive} AS SELECT * FROM {tbl}"
                )
            except sqlite3.OperationalError as exc:
                logger.warning("v50_archive_failed", table=tbl, error=str(exc))

    # --- (N-1) sources lookup ------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            code         TEXT PRIMARY KEY,
            name_ja      TEXT NOT NULL,
            base_url     TEXT NOT NULL,
            license      TEXT NOT NULL,
            added_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            retired_at   TIMESTAMP,
            description  TEXT NOT NULL
        )
    """)
    for code, name_ja, base_url, lic, desc in _V50_SOURCE_SEEDS:
        conn.execute(
            """INSERT OR IGNORE INTO sources
                   (code, name_ja, base_url, license, description)
               VALUES (?, ?, ?, ?, ?)""",
            (code, name_ja, base_url, lic, desc),
        )

    # --- (N-2) roles lookup --------------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            code            TEXT PRIMARY KEY,
            name_ja         TEXT NOT NULL,
            name_en         TEXT NOT NULL,
            role_group      TEXT NOT NULL CHECK (role_group IN
                ('director','animator','sound','production','writer',
                 'voice_actor','other')),
            weight_default  REAL NOT NULL CHECK (weight_default >= 0),
            description_ja  TEXT NOT NULL
        )
    """)
    for code, name_ja, name_en, role_group, weight, desc in _V50_ROLE_SEEDS:
        conn.execute(
            """INSERT OR IGNORE INTO roles
                   (code, name_ja, name_en, role_group,
                    weight_default, description_ja)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (code, name_ja, name_en, role_group, weight, desc),
        )

    # --- (N-4) anime_external_ids -------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime_external_ids (
            anime_id     TEXT NOT NULL,
            source       TEXT NOT NULL REFERENCES sources(code),
            external_id  TEXT NOT NULL,
            PRIMARY KEY (anime_id, source),
            UNIQUE (source, external_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_anime_ext_ids_source "
        "ON anime_external_ids(source, external_id)"
    )

    # Backfill anime_external_ids from both legacy `anime` and `anime_analysis`.
    def _columns(table: str) -> set[str]:
        try:
            return {
                row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
        except sqlite3.OperationalError:
            return set()

    for legacy_table in ("anime", "anime_analysis"):
        cols = _columns(legacy_table)
        if not cols:
            continue
        for col_name, src_code in (
            ("anilist_id", "anilist"),
            ("mal_id", "mal"),
            ("ann_id", "ann"),
            ("allcinema_id", "allcinema"),
            ("madb_id", "madb"),  # madb not in seeded sources by default
        ):
            if col_name not in cols:
                continue
            # Make sure the source code exists (idempotent).
            if src_code == "madb":
                conn.execute(
                    "INSERT OR IGNORE INTO sources "
                    "(code, name_ja, base_url, license, description) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        "madb",
                        "メディア芸術データベース",
                        "https://mediaarts-db.artmuseums.go.jp",
                        "Agency for Cultural Affairs",
                        "メディア芸術DB の作品 URI",
                    ),
                )
            conn.execute(
                f"""INSERT OR IGNORE INTO anime_external_ids (anime_id, source, external_id)
                    SELECT id, ?, CAST({col_name} AS TEXT)
                    FROM {legacy_table}
                    WHERE {col_name} IS NOT NULL""",
                (src_code,),
            )

    # --- (N-4) person_external_ids ------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS person_external_ids (
            person_id    TEXT NOT NULL,
            source       TEXT NOT NULL REFERENCES sources(code),
            external_id  TEXT NOT NULL,
            PRIMARY KEY (person_id, source),
            UNIQUE (source, external_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_person_ext_ids_source "
        "ON person_external_ids(source, external_id)"
    )
    persons_cols = _columns("persons")
    for col_name, src_code in (
        ("anilist_id", "anilist"),
        ("mal_id", "mal"),
        ("ann_id", "ann"),
        ("allcinema_id", "allcinema"),
    ):
        if col_name not in persons_cols:
            continue
        conn.execute(
            f"""INSERT OR IGNORE INTO person_external_ids (person_id, source, external_id)
                SELECT id, ?, CAST({col_name} AS TEXT)
                FROM persons
                WHERE {col_name} IS NOT NULL""",
            (src_code,),
        )

    # --- (N-3) person_aliases -----------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS person_aliases (
            person_id   TEXT NOT NULL,
            alias       TEXT NOT NULL,
            source      TEXT NOT NULL REFERENCES sources(code),
            confidence  REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
            added_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (person_id, alias, source)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_person_aliases_alias "
        "ON person_aliases(alias, person_id)"
    )
    # Backfill from persons.aliases (JSON list) — classify as 'anilist' by
    # default (existing rows came through the AniList pipeline).
    if "aliases" in persons_cols:
        cursor = conn.execute(
            "SELECT id, aliases FROM persons "
            "WHERE aliases IS NOT NULL AND aliases != '' AND aliases != '[]'"
        )
        for pid, raw in cursor.fetchall():
            try:
                alias_list = _json.loads(raw) if raw else []
            except (TypeError, _json.JSONDecodeError):
                continue
            if not isinstance(alias_list, list):
                continue
            for alias in alias_list:
                if not isinstance(alias, str) or not alias:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO person_aliases "
                    "(person_id, alias, source) VALUES (?, ?, ?)",
                    (pid, alias, "anilist"),
                )

    # --- anime_genres / anime_tags normalization -----------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime_genres (
            anime_id   TEXT NOT NULL,
            genre_name TEXT NOT NULL,
            PRIMARY KEY (anime_id, genre_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_anime_genres_genre "
        "ON anime_genres(genre_name, anime_id)"
    )
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime_tags (
            anime_id TEXT NOT NULL,
            tag_name TEXT NOT NULL,
            rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
            PRIMARY KEY (anime_id, tag_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_anime_tags_tag "
        "ON anime_tags(tag_name, rank, anime_id)"
    )

    # Backfill anime_genres / anime_tags from the best available source:
    #   1. bronze src_anilist_anime (authoritative JSON) joined via
    #      anime_external_ids — primary path.
    #   2. anime_display.genres / anime_display.tags JSON — secondary.
    #   3. legacy anime.genres / anime.tags JSON — tertiary.
    def _backfill_genres_tags_from_json(
        conn_: sqlite3.Connection,
        select_sql: str,
    ) -> None:
        for row in conn_.execute(select_sql).fetchall():
            anime_id = row[0]
            genres_raw = row[1] if len(row) > 1 else None
            tags_raw = row[2] if len(row) > 2 else None
            if genres_raw:
                try:
                    for g in _json.loads(genres_raw):
                        if isinstance(g, str) and g:
                            conn_.execute(
                                "INSERT OR IGNORE INTO anime_genres "
                                "(anime_id, genre_name) VALUES (?, ?)",
                                (anime_id, g),
                            )
                        elif isinstance(g, dict):
                            name = g.get("name")
                            if isinstance(name, str) and name:
                                conn_.execute(
                                    "INSERT OR IGNORE INTO anime_genres "
                                    "(anime_id, genre_name) VALUES (?, ?)",
                                    (anime_id, name),
                                )
                except (TypeError, _json.JSONDecodeError):
                    pass
            if tags_raw:
                try:
                    for t in _json.loads(tags_raw):
                        if isinstance(t, str) and t:
                            conn_.execute(
                                "INSERT OR IGNORE INTO anime_tags "
                                "(anime_id, tag_name, rank) VALUES (?, ?, NULL)",
                                (anime_id, t),
                            )
                        elif isinstance(t, dict):
                            name = t.get("name")
                            rank = t.get("rank")
                            if isinstance(name, str) and name:
                                if rank is not None and not (
                                    isinstance(rank, int) and 0 <= rank <= 100
                                ):
                                    rank = None
                                conn_.execute(
                                    "INSERT OR IGNORE INTO anime_tags "
                                    "(anime_id, tag_name, rank) VALUES (?, ?, ?)",
                                    (anime_id, name, rank),
                                )
                except (TypeError, _json.JSONDecodeError):
                    pass

    # Bronze path (preferred) — only if src_anilist_anime present.
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='src_anilist_anime'"
    ).fetchone():
        _backfill_genres_tags_from_json(
            conn,
            """SELECT x.anime_id,
                      s.genres,
                      s.tags
                 FROM anime_external_ids x
                 JOIN src_anilist_anime s
                   ON x.source = 'anilist'
                  AND CAST(s.anilist_id AS TEXT) = x.external_id
            """,
        )

    # anime_display (silver.display) path.
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='anime_display'"
    ).fetchone():
        _backfill_genres_tags_from_json(
            conn,
            "SELECT id, genres, tags FROM anime_display",
        )

    # Legacy anime path (covers fixtures / tests without bronze).
    anime_cols = _columns("anime")
    if "genres" in anime_cols or "tags" in anime_cols:
        has_g = "genres" in anime_cols
        has_t = "tags" in anime_cols
        if has_g and has_t:
            _backfill_genres_tags_from_json(conn, "SELECT id, genres, tags FROM anime")
        elif has_g:
            _backfill_genres_tags_from_json(conn, "SELECT id, genres, NULL FROM anime")
        elif has_t:
            _backfill_genres_tags_from_json(conn, "SELECT id, NULL, tags FROM anime")

    # --- (C-1) credits.evidence_source (additive rename) --------------------
    # We add an ``evidence_source`` column that mirrors ``source``. Writers
    # that already set ``source`` continue to work; new writers may use
    # either. A trigger keeps the two columns in sync until a later
    # migration drops ``source``.
    credit_cols = _columns("credits")
    if "evidence_source" not in credit_cols:
        try:
            conn.execute(
                "ALTER TABLE credits ADD COLUMN evidence_source TEXT NOT NULL DEFAULT ''"
            )
        except sqlite3.OperationalError:
            pass
    if "source" in credit_cols:
        # Backfill evidence_source from source, then keep them in sync.
        conn.execute(
            "UPDATE credits SET evidence_source = source "
            "WHERE evidence_source = '' OR evidence_source IS NULL"
        )
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_credits_source_to_evidence
            AFTER INSERT ON credits
            WHEN NEW.source IS NOT NULL AND NEW.source != ''
                 AND (NEW.evidence_source IS NULL OR NEW.evidence_source = '')
            BEGIN
                UPDATE credits SET evidence_source = NEW.source WHERE id = NEW.id;
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_credits_evidence_to_source
            AFTER INSERT ON credits
            WHEN NEW.evidence_source IS NOT NULL AND NEW.evidence_source != ''
                 AND (NEW.source IS NULL OR NEW.source = '')
            BEGIN
                UPDATE credits SET source = NEW.evidence_source WHERE id = NEW.id;
            END
        """)

    # --- Normalize credits.episode sentinel (-1 → NULL) ---------------------
    # Our code uses episode=-1 to mean "whole series". v50 normalizes this to
    # NULL (matching detailed_todo.md §1.3.2). Writers that insert -1 will be
    # rewritten by a trigger so this migration is stable on new data.
    if "episode" in credit_cols:
        try:
            conn.execute("UPDATE credits SET episode = NULL WHERE episode = -1")
        except sqlite3.OperationalError:
            pass

    # --- person_scores VIEW removed ------------------------------------------
    # v50: VIEW person_scores was a compatibility alias for the scores table.
    # v55 migration now performs a physical ALTER TABLE scores RENAME TO person_scores,
    # so the VIEW is no longer created here.

    # --- Log summary --------------------------------------------------------
    n_sources = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    n_roles = conn.execute("SELECT COUNT(*) FROM roles").fetchone()[0]
    n_ext_anime = conn.execute("SELECT COUNT(*) FROM anime_external_ids").fetchone()[0]
    n_ext_person = conn.execute("SELECT COUNT(*) FROM person_external_ids").fetchone()[
        0
    ]
    n_aliases = conn.execute("SELECT COUNT(*) FROM person_aliases").fetchone()[0]
    n_genres = conn.execute("SELECT COUNT(*) FROM anime_genres").fetchone()[0]
    n_tags = conn.execute("SELECT COUNT(*) FROM anime_tags").fetchone()[0]
    logger.info(
        "v50_canonical_silver_ready",
        sources=n_sources,
        roles=n_roles,
        anime_ext_ids=n_ext_anime,
        person_ext_ids=n_ext_person,
        person_aliases=n_aliases,
        anime_genres=n_genres,
        anime_tags=n_tags,
    )


def _migrate_v51_meta_lineage_and_audit(conn: sqlite3.Connection) -> None:
    """v51: expand meta_lineage reproducibility fields + add ER audit table."""
    lineage_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(ops_lineage)").fetchall()
    }
    if "description" not in lineage_cols:
        conn.execute(
            "ALTER TABLE meta_lineage ADD COLUMN description TEXT NOT NULL DEFAULT ''"
        )
    if "rng_seed" not in lineage_cols:
        conn.execute("ALTER TABLE meta_lineage ADD COLUMN rng_seed INTEGER")
    if "git_sha" not in lineage_cols:
        conn.execute(
            "ALTER TABLE meta_lineage ADD COLUMN git_sha TEXT NOT NULL DEFAULT ''"
        )
    if "inputs_hash" not in lineage_cols:
        conn.execute(
            "ALTER TABLE meta_lineage ADD COLUMN inputs_hash TEXT NOT NULL DEFAULT ''"
        )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta_entity_resolution_audit (
            person_id TEXT PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            merge_method TEXT NOT NULL CHECK (merge_method IN
                ('exact_match','cross_source','romaji','similarity','ai_assisted','manual')),
            merge_confidence REAL NOT NULL CHECK (merge_confidence BETWEEN 0 AND 1),
            merged_from_keys TEXT NOT NULL,
            merge_evidence TEXT NOT NULL,
            merged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            reviewed_by TEXT,
            reviewed_at TIMESTAMP,
            FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_era_method "
        "ON meta_entity_resolution_audit(merge_method, merge_confidence)"
    )


def _migrate_v52_add_calc_execution_records(conn: sqlite3.Connection) -> None:
    """v52: add calc_execution_records for hash-based recomputation skip."""
    ensure_calc_execution_records(conn)


def _migrate_v53_slim_anime_table(conn: sqlite3.Connection) -> None:
    """v53: slim canonical anime table to structural columns only.

    - Drop display-only columns (score/popularity/images/description/etc.)
    - Drop embedded external-id columns (mal_id/anilist_id/ann_id/allcinema_id/madb_id)
    - Preserve external ids in anime_external_ids
    - Keep normalized genre/tag/studio tables as the source of those dimensions
    """
    anime_cols = {row[1] for row in conn.execute("PRAGMA table_info(anime)").fetchall()}
    if not anime_cols:
        return

    # Ensure external-id table exists and is backfilled before slimming anime.
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS anime_external_ids (
            anime_id     TEXT NOT NULL,
            source       TEXT NOT NULL REFERENCES sources(code),
            external_id  TEXT NOT NULL,
            PRIMARY KEY (anime_id, source),
            UNIQUE (source, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_anime_ext_ids_source
            ON anime_external_ids(source, external_id);
    """)
    for source, col in (
        ("mal", "mal_id"),
        ("anilist", "anilist_id"),
        ("ann", "ann_id"),
        ("allcinema", "allcinema_id"),
        ("madb", "madb_id"),
    ):
        if col in anime_cols:
            conn.execute(
                f"""INSERT OR IGNORE INTO anime_external_ids (anime_id, source, external_id)
                    SELECT id, ?, CAST({col} AS TEXT) FROM anime
                    WHERE {col} IS NOT NULL AND CAST({col} AS TEXT) != ''""",
                (source,),
            )

    # Backfill normalized genre/tag tables from legacy JSON columns if still present.
    if "genres" in anime_cols or "tags" in anime_cols:
        import json as _json

        genre_expr = "genres" if "genres" in anime_cols else "NULL AS genres"
        tag_expr = "tags" if "tags" in anime_cols else "NULL AS tags"
        rows = conn.execute(
            f"SELECT id, {genre_expr}, {tag_expr} FROM anime"  # noqa: S608
        ).fetchall()
        for row in rows:
            anime_id = row["id"]
            raw_genres = row["genres"]
            raw_tags = row["tags"]

            if raw_genres:
                try:
                    genres = _json.loads(raw_genres)
                except (TypeError, ValueError):
                    genres = []
                for g in genres if isinstance(genres, list) else []:
                    if isinstance(g, str) and g:
                        conn.execute(
                            "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
                            (anime_id, g),
                        )
                    elif isinstance(g, dict):
                        name = g.get("name")
                        if isinstance(name, str) and name:
                            conn.execute(
                                "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
                                (anime_id, name),
                            )

            if raw_tags:
                try:
                    tags = _json.loads(raw_tags)
                except (TypeError, ValueError):
                    tags = []
                for t in tags if isinstance(tags, list) else []:
                    if isinstance(t, str) and t:
                        conn.execute(
                            "INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank) VALUES (?, ?, NULL)",
                            (anime_id, t),
                        )
                    elif isinstance(t, dict):
                        name = t.get("name")
                        if not isinstance(name, str) or not name:
                            continue
                        rank = t.get("rank")
                        if rank is not None and not (
                            isinstance(rank, int) and 0 <= rank <= 100
                        ):
                            rank = None
                        conn.execute(
                            "INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank) VALUES (?, ?, ?)",
                            (anime_id, name, rank),
                        )

    conn.executescript("""
        CREATE TABLE anime_new (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            format TEXT,
            status TEXT,
            start_date TEXT CHECK (
                start_date IS NULL
                OR start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            ),
            end_date TEXT CHECK (
                end_date IS NULL
                OR end_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            ),
            duration INTEGER,
            source TEXT,
            quarter INTEGER,
            work_type TEXT,
            scale_class TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO anime_new (
            id, title_ja, title_en, year, season, episodes, format, status,
            start_date, end_date, duration, source, quarter, work_type, scale_class,
            updated_at
        )
        SELECT
            id, title_ja, title_en, year, season, episodes, format, status,
            start_date, end_date, duration, source, quarter, work_type, scale_class,
            COALESCE(updated_at, CURRENT_TIMESTAMP)
        FROM anime;
        DROP TABLE anime;
        ALTER TABLE anime_new RENAME TO anime;
        CREATE INDEX IF NOT EXISTS idx_anime_year ON anime(year);
        CREATE INDEX IF NOT EXISTS idx_anime_year_fmt ON anime(year, format);
    """)


def _migrate_v54_drop_legacy_credit_source(conn: sqlite3.Connection) -> None:
    """v54: remove legacy source column from credits (keep evidence_source only)."""
    credit_cols = {row[1] for row in conn.execute("PRAGMA table_info(credits)").fetchall()}
    if "source" not in credit_cols:
        return

    conn.executescript("""
        DROP TRIGGER IF EXISTS trg_credits_source_to_evidence;
        DROP TRIGGER IF EXISTS trg_credits_evidence_to_source;

        CREATE TABLE credits_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER DEFAULT -1,
            evidence_source TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            credit_year INTEGER,
            credit_quarter INTEGER,
            UNIQUE(person_id, anime_id, role, episode)
        );
    """)
    conn.execute(
        """INSERT INTO credits_new (
            id,
            person_id,
            anime_id,
            role,
            raw_role,
            episode,
            evidence_source,
            updated_at,
            credit_year,
            credit_quarter
        )
        SELECT
            id,
            person_id,
            anime_id,
            role,
            raw_role,
            episode,
            CASE
                WHEN evidence_source IS NULL OR evidence_source = '' THEN source
                ELSE evidence_source
            END,
            updated_at,
            credit_year,
            credit_quarter
        FROM credits"""
    )
    conn.executescript("""
        DROP TABLE credits;
        ALTER TABLE credits_new RENAME TO credits;
        CREATE INDEX IF NOT EXISTS idx_credits_person ON credits(person_id);
        CREATE INDEX IF NOT EXISTS idx_credits_anime ON credits(anime_id);
        CREATE INDEX IF NOT EXISTS idx_credits_role ON credits(role);
        CREATE INDEX IF NOT EXISTS idx_credits_yq ON credits(credit_year, credit_quarter);
    """)


# ============================================================================
# PHASE 1 SCHEMA MIGRATION: v54 → v55
# ============================================================================
# Migration adds lookup tables (sources, roles, person_aliases) to enforce
# normalization and enable FK constraints.
# ============================================================================


def _migrate_v54_to_v55(conn: sqlite3.Connection) -> None:
    """Migrate schema from v54 to v55.

    Adds:
    - person_aliases table (entity resolution audit trail)

    Seeds:
    - sources lookup (DDL defined in init_db / v50 migration)

    No breaking changes to existing tables.
    """
    cursor = conn.cursor()

    # 1. Seed sources lookup table (DDL is in init_db() / _migrate_v50_canonical_silver)
    # PK=code, with name_ja/base_url/license/description columns
    SOURCE_SEEDS = [
        ("anilist",    "AniList",               "https://anilist.co",                "proprietary", "GraphQL で structured staff 情報が最も豊富"),
        ("mal",        "MyAnimeList",            "https://myanimelist.net",           "proprietary", "viewer ratings の参照源 (表示のみ、分析不使用)"),
        ("ann",        "Anime News Network",     "https://www.animenewsnetwork.com",  "proprietary", "historical depth と職種粒度"),
        ("allcinema",  "allcinema",              "https://www.allcinema.net",         "proprietary", "邦画・OVA の網羅性"),
        ("seesaawiki", "SeesaaWiki",             "https://seesaawiki.jp",             "CC-BY-SA",    "fan-curated 詳細エピソード情報"),
        ("keyframe",   "Sakugabooru/Keyframe",   "https://www.sakugabooru.com",       "CC",          "sakuga コミュニティ別名情報"),
        ("madb",       "メディア芸術データベース",   "https://mediaarts-db.bunka.go.jp",  "CC-BY",       "文化庁運営の公的日本語作品データベース"),
    ]
    for code, name_ja, base_url, license_, desc in SOURCE_SEEDS:
        cursor.execute(
            "INSERT OR IGNORE INTO sources (code, name_ja, base_url, license, description) VALUES (?, ?, ?, ?, ?)",
            (code, name_ja, base_url, license_, desc),
        )
    logger.info("sources_seeded", count=len(SOURCE_SEEDS))
    
    # 2. Create person_aliases table (entity resolution audit)
    # (roles table DDL and seeds are handled by init_db / _migrate_v50_canonical_silver)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS person_aliases (
            person_id TEXT NOT NULL,
            alias_name TEXT NOT NULL,
            source TEXT NOT NULL,
            confidence REAL DEFAULT 0.5,
            matched_to TEXT,
            notes TEXT,
            PRIMARY KEY (person_id, alias_name, source),
            FOREIGN KEY (person_id) REFERENCES persons(id)
        )
    """)
    logger.info("person_aliases_table_created")
    
    # 4. Drop deprecated anime_display table
    # (display metadata now comes from bronze via display_lookup helper)
    cursor.execute("DROP TABLE IF EXISTS anime_display")
    logger.info("anime_display_dropped")

    # 5. Physical rename: scores → person_scores
    # Drop the compat VIEW from v50 first (if it still exists), then rename.
    if cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scores'"
    ).fetchone():
        # Drop any pre-existing person_scores (TABLE or VIEW) before rename.
        # DROP VIEW IF EXISTS fails if name belongs to a TABLE, so check type first.
        ps_type = cursor.execute(
            "SELECT type FROM sqlite_master WHERE name='person_scores'"
        ).fetchone()
        if ps_type:
            stmt = "DROP TABLE" if ps_type[0] == "table" else "DROP VIEW"
            cursor.execute(f"{stmt} person_scores")
        cursor.execute("ALTER TABLE scores RENAME TO person_scores")
        logger.info("scores_renamed_to_person_scores")

    # 6. Physical rename: va_scores → voice_actor_scores (naming symmetry with person_scores)
    if cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='va_scores'"
    ).fetchone():
        cursor.execute("ALTER TABLE va_scores RENAME TO voice_actor_scores")
        logger.info("va_scores_renamed_to_voice_actor_scores")

    # 7. Physical rename: data_sources → source_scrape_status
    # (disambiguate from the `sources` canonical lookup table)
    if cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='data_sources'"
    ).fetchone():
        cursor.execute("ALTER TABLE data_sources RENAME TO source_scrape_status")
        logger.info("data_sources_renamed_to_source_scrape_status")

    conn.commit()
    _set_schema_version(conn, 55)
    logger.info("schema_migration_complete", to_version=55)


# ============================================================================
# SCHEMA MIGRATION: v55 → v56 — Multi-language names + nationality
# ============================================================================


def _migrate_v56_multilang_names(conn: sqlite3.Connection) -> None:
    """Migrate schema from v55 to v56.

    Adds separate name columns for Korean and Chinese names (previously
    both collapsed into name_ja), plus nationality (ISO 3166-1 alpha-2 list)
    and lang tag on person_aliases.

    No breaking changes; all new columns have safe defaults.
    """
    cursor = conn.cursor()

    new_person_cols = [
        "name_ko TEXT NOT NULL DEFAULT ''",
        "name_zh TEXT NOT NULL DEFAULT ''",
        "nationality TEXT NOT NULL DEFAULT '[]'",
    ]
    for col in new_person_cols:
        col_name = col.split()[0]
        try:
            cursor.execute(f"ALTER TABLE persons ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass  # already exists
        try:
            cursor.execute(f"ALTER TABLE src_anilist_persons ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
        logger.debug("multilang_col_added", column=col_name)

    try:
        cursor.execute("ALTER TABLE person_aliases ADD COLUMN lang TEXT")
    except sqlite3.OperationalError:
        pass  # already exists

    conn.commit()
    _set_schema_version(conn, 56)
    logger.info("schema_migration_complete", to_version=56)


# ============================================================================
# PHASE 1 SCHEMA MIGRATION: v56 → v57 (OPTIONAL — Genre Normalization)
# ============================================================================


def _migrate_v56_to_v57_genre_normalization(conn: sqlite3.Connection) -> None:
    """Migrate schema from v56 to v57.

    Normalizes anime.genres JSON into anime_genres N-M table.
    Optional but recommended for query efficiency.
    """
    # STATUS: deferred — intentionally NOT registered in migrations dict.
    # Execution cost is high (full-table JSON expansion) and current
    # production DBs are already post-v53 (no genres JSON column).
    # Schedule explicitly via a separate task when data shape changes.
    # See TASK_CARDS/01_schema_fix/06_v56_defer_comment.md for context.
    cursor = conn.cursor()
    
    # 1. Create genres lookup table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    """)
    
    # 2. Create anime_genres N-M table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS anime_genres (
            anime_id TEXT NOT NULL,
            genre_id INTEGER NOT NULL,
            PRIMARY KEY (anime_id, genre_id),
            FOREIGN KEY (anime_id) REFERENCES anime(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id)
        )
    """)
    logger.info("anime_genres_table_created")
    
    # 3. Migrate existing genres JSON → N-M rows
    import json
    
    cursor.execute("SELECT id, genres FROM anime WHERE genres IS NOT NULL")
    anime_genres_list = cursor.fetchall()
    
    genre_seen = {}
    rows_inserted = 0
    
    for anime_id, genres_json in anime_genres_list:
        try:
            genres = json.loads(genres_json) if isinstance(genres_json, str) else []
            for genre_name in genres:
                # Insert genre if not seen
                if genre_name not in genre_seen:
                    cursor.execute(
                        "INSERT INTO genres (name) VALUES (?)",
                        (genre_name,),
                    )
                    genre_id = cursor.lastrowid
                    genre_seen[genre_name] = genre_id
                else:
                    genre_id = genre_seen[genre_name]
                
                # Insert anime_genre link
                cursor.execute(
                    "INSERT OR IGNORE INTO anime_genres (anime_id, genre_id) VALUES (?, ?)",
                    (anime_id, genre_id),
                )
                rows_inserted += 1
        except json.JSONDecodeError:
            logger.warning("genres_json_decode_error", anime_id=anime_id)
    
    logger.info(
        "genres_migrated",
        anime_count=len(anime_genres_list),
        genre_rows=rows_inserted,
    )
    
    # 4. Drop genres column from anime (if schema allows)
    # Note: SQLite doesn't support DROP COLUMN easily, so we leave it for safety
    logger.warning("genres_column_not_dropped", reason="SQLite limitation; column remains for rollback safety")
    
    conn.commit()
    _set_schema_version(conn, 56)
    logger.info("schema_migration_complete", to_version=56)


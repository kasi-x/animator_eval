"""SQLite データベース管理."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog

from src.models import (
    Anime,
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

SCHEMA_VERSION = 49

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
    # v24 追加 (85%+ confidence, 手動確認済)
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
    """SQLite 接続を取得する."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_connection(
    db_path: Path | None = None,
) -> Generator[sqlite3.Connection, None, None]:
    """SQLite 接続のコンテキストマネージャ.

    正常終了時は自動コミット、例外時はロールバック、常にクローズする。

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
    """テーブルを作成する."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT '',
            aliases TEXT NOT NULL DEFAULT '[]',
            mal_id INTEGER,
            anilist_id INTEGER,
            canonical_id TEXT,
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
            mal_id INTEGER,
            anilist_id INTEGER,
            score REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT,
            episode INTEGER DEFAULT -1,
            source TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(person_id, anime_id, role, episode)
        );

        CREATE TABLE IF NOT EXISTS scores (
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

        CREATE TABLE IF NOT EXISTS data_sources (
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

        -- ============================================================
        -- Silver layer: analysis と display の分離
        -- anime_analysis: score/popularity/favourites を意図的に含まない分析専用層
        -- anime_display:  score を含む表示専用層。analysis 層からは参照禁止
        -- ============================================================

        CREATE TABLE IF NOT EXISTS anime_analysis (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            title_en TEXT NOT NULL DEFAULT '',
            year INTEGER,
            season TEXT,
            quarter INTEGER,
            episodes INTEGER,
            format TEXT,
            duration INTEGER,
            start_date TEXT,
            end_date TEXT,
            status TEXT,
            source TEXT,
            work_type TEXT,
            scale_class TEXT,
            mal_id INTEGER,
            anilist_id INTEGER,
            ann_id INTEGER,
            allcinema_id INTEGER,
            madb_id TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );

        CREATE INDEX IF NOT EXISTS idx_anime_analysis_year ON anime_analysis(year);
        CREATE INDEX IF NOT EXISTS idx_anime_analysis_format ON anime_analysis(format);
        CREATE INDEX IF NOT EXISTS idx_anime_analysis_anilist ON anime_analysis(anilist_id);

        CREATE TABLE IF NOT EXISTS anime_display (
            id TEXT PRIMARY KEY,
            score REAL,
            popularity INTEGER,
            popularity_rank INTEGER,
            favourites INTEGER,
            mean_score INTEGER,
            description TEXT,
            cover_large TEXT,
            cover_extra_large TEXT,
            cover_medium TEXT,
            cover_large_path TEXT,
            banner TEXT,
            banner_path TEXT,
            site_url TEXT,
            genres TEXT DEFAULT '[]',
            tags TEXT DEFAULT '[]',
            studios TEXT DEFAULT '[]',
            synonyms TEXT DEFAULT '[]',
            country_of_origin TEXT,
            is_adult INTEGER,
            relations_json TEXT,
            external_links_json TEXT,
            rankings_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id) REFERENCES anime_analysis(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_anime_display_score ON anime_display(score);

        -- ============================================================
        -- Gold layer: レポート直読用の事前集計テーブル (meta_*)
        -- feat_* を原料として派生。レポートはここだけを SELECT する
        -- ============================================================

        CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            row_count INTEGER,
            notes TEXT
        );

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
        -- src_* テーブル群: ソース別生スクレイプデータ (メダリオン Bronze 層)
        -- canonical テーブル (anime/persons/credits) とは完全に分離
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
            aliases TEXT DEFAULT '[]',
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
        -- feat_* テーブル群: パイプラインが計算した派生特徴量
        -- 生データテーブル (persons/anime/credits/studios) とは命名で区別
        -- ============================================================

        -- 全スコア指標 (scores テーブルの上位互換。scores は後方互換のため残す)
        CREATE TABLE IF NOT EXISTS feat_person_scores (
            person_id TEXT PRIMARY KEY,
            run_id INTEGER REFERENCES pipeline_runs(id),
            -- AKM 固定効果
            person_fe REAL,
            person_fe_se REAL,
            person_fe_n_obs INTEGER,
            studio_fe_exposure REAL,
            -- BiRank・ネットワーク
            birank REAL,
            patronage REAL,
            awcc REAL,
            -- IV 修正因子
            dormancy REAL,
            ndi REAL,
            career_friction REAL,
            peer_boost REAL,
            -- 統合スコア
            iv_score REAL,
            -- パーセンタイル順位 (0–100)
            iv_score_pct REAL,
            person_fe_pct REAL,
            birank_pct REAL,
            patronage_pct REAL,
            awcc_pct REAL,
            dormancy_pct REAL,
            -- 信頼区間
            confidence REAL,
            score_range_low REAL,
            score_range_high REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- グラフ構造から導出されたネットワーク指標
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

        -- キャリア軌跡から導出された指標
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

        -- ジャンル × 人物の親和性スコア (人物ごとに複数行)
        CREATE TABLE IF NOT EXISTS feat_genre_affinity (
            person_id TEXT NOT NULL,
            genre TEXT NOT NULL,
            run_id INTEGER REFERENCES pipeline_runs(id),
            affinity_score REAL,
            work_count INTEGER,
            PRIMARY KEY (person_id, genre)
        );

        -- 個人貢献プロファイル (Layer 2)
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

        -- クレジット活動パターン (空白期間・活動密度・休止履歴)
        -- abs_quarter = year * 4 + (quarter - 1)  例: 2020Q1 → 8080
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

        -- キャリア年 × 職種カテゴリ別集計
        -- career_year = credit_year - first_credit_year (0=デビュー年)
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

        -- 人物ごとの年次BiRankスナップショット (v42)
        -- 1980年以降のみ保存（それ以前はグラフが極小でスコアが無意味）
        -- birank: その年の全人物内での 0-100 正規化スコア
        -- raw_pagerank: PageRank生値（正規化前）
        -- graph_size: その年の累積グラフにおける人物ノード数
        -- n_credits_cumulative: その年までの業界累積クレジット数（正規化の母集団規模指標）
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

        -- birank_compute_state: BiRank 計算に使った入力データのフィンガープリント（年別）
        -- 変更検出に使用: 各年のクレジット数・アニメ数・人物数が前回計算時と異なれば再計算
        CREATE TABLE IF NOT EXISTS birank_compute_state (
            year INTEGER PRIMARY KEY,
            credit_count INTEGER NOT NULL,
            anime_count INTEGER NOT NULL,
            person_count INTEGER NOT NULL,
            computed_at REAL NOT NULL    -- unix timestamp
        );

        -- 個人のスタジオ所属年別集計
        -- anime_studios + credits を結合して「その年どのスタジオの作品に参加したか」を集計
        CREATE TABLE IF NOT EXISTS feat_studio_affiliation (
            person_id TEXT NOT NULL,
            credit_year INTEGER NOT NULL,
            studio_id TEXT NOT NULL,
            studio_name TEXT NOT NULL DEFAULT '',
            n_works INTEGER NOT NULL DEFAULT 0,    -- そのスタジオの作品に参加した数
            n_credits INTEGER NOT NULL DEFAULT 0,  -- クレジット行数
            is_main_studio INTEGER NOT NULL DEFAULT 0, -- 主要スタジオ (anime_studios.is_main)
            PRIMARY KEY (person_id, credit_year, studio_id)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_person
            ON feat_studio_affiliation(person_id);
        CREATE INDEX IF NOT EXISTS idx_feat_studio_aff_studio
            ON feat_studio_affiliation(studio_id, credit_year);

        -- 個人 × 作品 × 役職ごとのスコア貢献推定
        -- production_scale: AKM 目的変数 (完全計算)
        -- edge_weight: グラフ辺寄与 (完全計算)
        -- iv_contrib_est: IV スコアへの按分推定 (edge_weight_share × iv_score)
        CREATE TABLE IF NOT EXISTS feat_credit_contribution (
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER,
            -- AKM 目的変数 (生産規模の対数)
            production_scale REAL,
            -- グラフ辺寄与
            role_weight REAL,
            episode_coverage REAL,
            dur_mult REAL,
            edge_weight REAL,
            -- IV スコアへの按分 (edge_weight / 人物の total_edge_weight × iv_score)
            edge_weight_share REAL,
            iv_contrib_est REAL,
            PRIMARY KEY (person_id, anime_id, role)
        );

        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_anime
            ON feat_credit_contribution(anime_id);
        CREATE INDEX IF NOT EXISTS idx_feat_credit_contrib_year
            ON feat_credit_contribution(credit_year);

        -- 個人の作品コントリビュート集計
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

        -- 作品ごとのチーム統計 (v38) + 規模ティア (v44)
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
            -- 作品規模ティア (v44): format + episodes + duration のみで計算
            scale_tier INTEGER,
            scale_label TEXT,
            scale_raw REAL,
            format_group TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_feat_work_context_year
            ON feat_work_context(credit_year);
        -- Note: idx_feat_work_context_tier は v44 マイグレーションで追加

        -- 個人×職種カテゴリの時系列進行 (v39)
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

        -- 因果推論結果 (v40)
        CREATE TABLE IF NOT EXISTS feat_causal_estimates (
            person_id TEXT PRIMARY KEY,
            peer_effect_boost REAL,
            career_friction REAL,
            era_fe REAL,
            era_deflated_iv REAL,
            opportunity_residual REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- クラスタリング帰属 — 複数次元を1行に集約 (v41)
        CREATE TABLE IF NOT EXISTS feat_cluster_membership (
            person_id TEXT PRIMARY KEY,
            -- グラフコミュニティ検出 (Phase 4: Louvain / Leiden)
            community_id INTEGER,
            -- キャリアトラック (Phase 6: ルールベース分類)
            career_track TEXT,
            -- 成長トレンド (Phase 9: growth analysis)
            growth_trend TEXT,
            -- 主要所属スタジオのクラスタ (K-Means on studio features)
            studio_cluster_id INTEGER,
            studio_cluster_name TEXT,
            -- 共同クレジットグループ (Phase 9: cooccurrence_groups)
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
    conn.commit()
    _run_migrations(conn)


def get_schema_version(conn: sqlite3.Connection) -> int:
    """現在のスキーマバージョンを取得する."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except sqlite3.OperationalError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    """スキーマバージョンを設定する."""
    conn.execute(
        """INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
        (str(version),),
    )


def _run_migrations(conn: sqlite3.Connection) -> None:
    """未適用のマイグレーションを順番に実行する."""
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
    """v1: score_history テーブルを追加."""
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
    """v2: score_history にインデックスを追加."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_score_history_person ON score_history(person_id);
        CREATE INDEX IF NOT EXISTS idx_score_history_run ON score_history(run_at);
    """)


def _migrate_v3_add_pipeline_meta(conn: sqlite3.Connection) -> None:
    """v3: pipeline_runs テーブルを追加."""
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
    """v4: anime テーブルに studio カラムを追加."""
    try:
        conn.execute("ALTER TABLE anime ADD COLUMN studio TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists


def _migrate_v5_add_person_metadata(conn: sqlite3.Connection) -> None:
    """v5: persons テーブルにメタデータカラムを追加."""
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
    """v6: anime テーブルにメタデータカラムを追加."""
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
    """v7: credits テーブルの外部キー制約を削除.

    二段階パイプラインでは credits が persons より先に保存されるため、
    FK 制約があると IntegrityError になる。
    SQLite は ALTER TABLE で FK を削除できないため、テーブル再作成が必要。
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
    """v8: credits の UNIQUE を raw_role ベースに変更 + anime に追加カラム.

    - UNIQUE(person_id, anime_id, role, episode) → UNIQUE(person_id, anime_id, raw_role, episode)
    - raw_role を NOT NULL DEFAULT '' に変更
    - anime テーブルに追加メタデータカラム
    """
    # 1. credits テーブル再作成（raw_role ベース UNIQUE）
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

    # 2. anime テーブルに追加カラム
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
    """v9: studios + anime_studios テーブルを追加."""
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
    """v10: スキーマ整理.

    - anime.studio (単数) カラム削除
    - credits.episode デフォルト -1 → NULL
    - updated_at カラム追加 (persons, anime, credits, characters, character_voice_actors, studios)
    - persons.canonical_id インデックス追加
    - anime_relations テーブル追加
    """
    # 1. credits テーブル再作成 (updated_at 追加)
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

    # 2. anime.studio (単数) カラム削除
    try:
        conn.execute("ALTER TABLE anime DROP COLUMN studio")
    except sqlite3.OperationalError:
        pass  # Column doesn't exist or SQLite too old

    # 3. updated_at カラム追加
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

    # 4. persons.canonical_id インデックス
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_persons_canonical ON persons(canonical_id)"
    )

    # 5. anime_relations テーブル
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
    """v11: anime/persons テーブルに madb_id カラムを追加."""
    for table in ["anime", "persons"]:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN madb_id TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_anime_madb_id ON anime(madb_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_madb_id ON persons(madb_id)")


def _migrate_v12_add_person_fetch_status(conn: sqlite3.Connection) -> None:
    """v12: API取得失敗（404等）を記録するテーブルを追加."""
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
    """v16: person_affiliations テーブルを追加 (人物×作品×所属スタジオ)."""
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
    """v17: LLM判定結果テーブルを追加 (org分類・名前正規化・同一人物判定)."""
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
    """v18: score_history に year/quarter カラムを追加（四半期集計用）."""
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
    """v19: anime に quarter カラムを追加し、season / start_date から一括算出.

    さらに SeesaaWiki/Keyframes 由来の year=NULL 作品を AniList タイトルマッチで補完。

    優先順: season → start_date の月 → NULL（不明）。
    season_to_quarter: winter=1, spring=2, summer=3, fall=4.
    start_date (YYYY-MM-DD) の月: 1-3=Q1, 4-6=Q2, 7-9=Q3, 10-12=Q4.
    """

    try:
        conn.execute("ALTER TABLE anime ADD COLUMN quarter INTEGER")
    except sqlite3.OperationalError:
        logger.debug("column_already_exists", table="anime", column="quarter")

    # Phase 1: season / start_date から quarter を直接算出
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

    # Phase 2: SeesaaWiki/Keyframes → AniList タイトルマッチで year/quarter/format 補完
    _backfill_from_anilist_titles(conn)


def _backfill_from_anilist_titles(conn: sqlite3.Connection) -> None:
    """year=NULL の非 AniList 作品を AniList タイトルマッチで補完.

    マッチ戦略 (優先順):
      1. title_ja 完全一致
      2. NFKC正規化 + 記号/空白除去 後の一致
      3. 括弧注釈除去 (「（TV第2作）」「(第3期)」等) 後の一致
         → 同名シリーズが複数ある場合は年代注釈からヒント取得

    補完対象カラム: year, quarter, format, season, start_date
    """
    import re
    import unicodedata

    # --- ターゲット: year=NULL の非AniList作品 ---
    targets = conn.execute(
        "SELECT id, title_ja, title_en FROM anime "
        "WHERE year IS NULL AND id NOT LIKE 'anilist:%' AND title_ja != ''"
    ).fetchall()
    if not targets:
        return

    # --- AniList 参照データ ---
    refs = conn.execute(
        "SELECT id, title_ja, title_en, year, quarter, season, start_date, format "
        "FROM anime WHERE id LIKE 'anilist:%' AND year IS NOT NULL AND title_ja != ''"
    ).fetchall()

    def _normalize(s: str) -> str:
        """NFKC正規化 + 記号/空白除去 + 小文字."""
        import html

        # HTMLエンティティをデコード: &#9825; → ♡
        s = html.unescape(s)
        s = unicodedata.normalize("NFKC", s).lower().strip()
        # 〈〉を<>に統一してから除去対象に含める
        s = s.replace("\u3008", "<").replace("\u3009", ">")
        s = re.sub(
            r"[\s\u3000・\-–—―~〜!！?？、。,.'\"()\（\）\[\]【】「」『』《》☆★♪♡♥♡−＝<>〈〉◆◇#\$%@&*]+",
            "",
            s,
        )
        return s

    def _strip_movie_prefix(s: str) -> str:
        """「映画」「劇場版」等のメディア形式プレフィックスを除去."""
        s = re.sub(r"^(?:映画|劇場版|劇場版映画|特別版|OVA|OAD)\s*", "", s)
        return s.strip()

    def _sorted_word_key(s: str) -> str:
        """単語をソートしてキーを生成（順序不問マッチ用）.
        例: 'SHADOW SKILL 影技' → '影技shadowskill' (sorted bigrams)"""
        # スペース・記号で分割してソート
        words = re.split(r"[\s・\-_/]+", s)
        return "".join(sorted(w for w in words if w))

    def _normalize_roman_greek(s: str) -> str:
        """ギリシャ文字・ローマ数字を ASCII に変換."""
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
        """漢字+ひらがなのみ抽出（カタカナ・ASCII・記号を除去）.

        英語部分(WEED)とカタカナ翻字(ウィード)を同時に除去して、
        残る漢字+ひらがなが一致していれば同一作品と判定する。
        """
        s = unicodedata.normalize("NFKC", s)
        return "".join(
            c
            for c in s
            if "\u4e00" <= c <= "\u9fff"  # CJK統合漢字
            or "\u3400" <= c <= "\u4dbf"  # CJK拡張A
            or "\u3040" <= c <= "\u309f"  # ひらがな
        )

    def _strip_reading_parens(s: str) -> str:
        """カタカナ/英字の読み括弧を除去: GATE（ゲート）→ GATE."""
        return re.sub(r"[（(][ァ-ヾA-Za-zＡ-Ｚ　\s]+[）)]", "", s).strip()

    def _strip_furigana(s: str) -> str:
        """AniList側のふりがな括弧を除去: 魔法騎士（マジックナイト）→ 魔法騎士."""
        return re.sub(r"（[ぁ-ヾァ-ヾA-Za-zＡ-Ｚ\w]+）", "", s).strip()

    def _strip_english_prefix(s: str) -> str:
        """日本語文字の前の英語プレフィックスを除去: MAJOR メジャー → メジャー."""
        return re.sub(r"^[A-Za-z0-9\s.\-/!?&:'+]+(?=[ぁ-ヾァ-ヾ一-龥])", "", s).strip()

    def _strip_annotations(s: str) -> str:
        """括弧注釈を除去: （TV第2作）、(第3期)、（2020年）等."""
        s = re.sub(r"[（(][^）)]*(?:シリーズ|期|作|版|年)[^）)]*[）)]", "", s)
        # 末尾の数字年号も除去: "ぼのぼの (2016)" → "ぼのぼの"
        s = re.sub(r"\s*[（(]\d{4}[）)]$", "", s)
        return s.strip()

    def _extract_year_hint(title: str) -> int | None:
        """タイトルから年代ヒントを抽出: （2020年）→2020."""
        m = re.search(r"[（(](\d{4})年?[）)]", title)
        return int(m.group(1)) if m else None

    def _strip_tv_prefix(s: str) -> str:
        """TVプレフィックスを除去: 'TVそれいけ!アンパンマン' → 'それいけ!アンパンマン'."""
        return re.sub(r"^TV\s*", "", s)

    # --- AniList インデックス構築 ---
    # exact title_ja → list of refs
    idx_exact: dict[str, list] = {}
    # normalized → list of refs
    idx_norm: dict[str, list] = {}
    # stripped + normalized → list of refs
    idx_stripped: dict[str, list] = {}
    # normalized title_en → list of refs (英語タイトルクロスマッチ用)
    idx_en: dict[str, list] = {}
    # (normalized_ja, entry) for containment lookups
    ref_entries_for_containment: list[tuple[str, tuple]] = []
    # furigana-stripped AniList title → list of refs
    idx_furigana: dict[str, list] = {}
    # movie-prefix-stripped → list of refs (「映画」「劇場版」除去)
    idx_movie: dict[str, list] = {}
    # sorted-word key → list of refs (単語順序不問マッチ)
    idx_sorted: dict[str, list] = {}
    # 漢字+ひらがなキー → list of refs (英語/カタカナ部分が違っても漢字が一致する場合)
    idx_khk: dict[str, list] = {}

    for row in refs:
        aid, ja, en, yr, q, sea, sd, fmt = row
        entry = (aid, yr, q, sea, sd, fmt, ja)

        idx_exact.setdefault(ja, []).append(entry)

        nk = _normalize(ja)
        if nk:
            idx_norm.setdefault(nk, []).append(entry)
            # 包含マッチ用にエントリを記録
            ref_entries_for_containment.append((nk, entry))

        sk = _normalize(_strip_annotations(ja))
        if sk and sk != nk:
            idx_stripped.setdefault(sk, []).append(entry)
        # stripped も同じキーなら norm に含まれるので追加不要
        if sk and sk not in idx_stripped:
            idx_stripped.setdefault(sk, []).append(entry)

        # title_en インデックス (英語タイトルクロスマッチ)
        if en:
            enk = _normalize(en)
            if enk:
                idx_en.setdefault(enk, []).append(entry)

        # ふりがな除去インデックス: 魔法騎士（マジックナイト）レイアース → 魔法騎士レイアース
        fk = _normalize(_strip_furigana(ja))
        if fk and fk != nk:
            idx_furigana.setdefault(fk, []).append(entry)

        # 映画/劇場版 プレフィックス除去インデックス
        mk = _normalize(_strip_movie_prefix(ja))
        if mk and mk != nk:
            idx_movie.setdefault(mk, []).append(entry)

        # 単語ソートインデックス (SHADOW SKILL 影技 ↔ 影技 SHADOW SKILL)
        # 5文字以上かつ複数単語の場合のみ
        words = [w for w in re.split(r"[\s\u3000・\-_/]+", ja) if w]
        if len(words) >= 2 and len(nk) >= 5:
            swk = _normalize(_sorted_word_key(ja))
            if swk and swk != nk:
                idx_sorted.setdefault(swk, []).append(entry)

        # 漢字+ひらがなキーインデックス (英語WEED ↔ カタカナウィード)
        khk = _kanji_hira_key(ja)
        if len(khk) >= 4:
            idx_khk.setdefault(khk, []).append(entry)

    def _pick_best(candidates: list, year_hint: int | None = None) -> tuple | None:
        """複数候補から最適な1件を選択."""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        # 年代ヒントがあれば最も近いものを選択
        if year_hint:
            return min(candidates, key=lambda c: abs((c[1] or 0) - year_hint))
        # なければ最新のものを選択（長期シリーズの最新版が通常正しい）
        return max(candidates, key=lambda c: c[1] or 0)

    # --- マッチング実行 ---
    updates: list[tuple] = []  # (year, quarter, season, start_date, format, target_id)

    for sid, sja, sen in targets:
        match = None
        year_hint = _extract_year_hint(sja)

        # 1. 完全一致
        if sja in idx_exact:
            match = _pick_best(idx_exact[sja], year_hint)

        # 2. 正規化一致
        if not match:
            nk = _normalize(sja)
            if nk in idx_norm:
                match = _pick_best(idx_norm[nk], year_hint)

        # 3. 括弧注釈除去 + 正規化一致
        if not match:
            sk = _normalize(_strip_annotations(sja))
            if sk in idx_norm:
                match = _pick_best(idx_norm[sk], year_hint)
            elif sk in idx_stripped:
                match = _pick_best(idx_stripped[sk], year_hint)

        # 4. ベースタイトルマッチ（スペース前の主タイトルのみ）
        #    品質制御: ベースタイトル5文字以上、かつ候補が少数の場合のみ
        if not match:
            _base = re.sub(r"[（(][^）)]*[）)]", "", sja).strip()
            _parts = re.split(r"[\s　]+", _base)
            _base_title = _parts[0] if len(_parts) > 1 else _base
            _bk = _normalize(_base_title)
            # 短すぎるベースタイトルは誤マッチの原因なのでスキップ
            # カテゴリページ等もスキップ
            _SKIP_PATTERNS = {"年代", "シリーズリスト", "アニバーサリー", "music"}
            if (
                len(_bk) >= 5
                and not any(p in sja for p in _SKIP_PATTERNS)
                and _bk in idx_norm
            ):
                candidates = idx_norm[_bk]
                # 候補が多すぎる場合（汎用的なタイトル）はスキップ
                if len(candidates) <= 10:
                    match = _pick_best(candidates, year_hint)

        # 5. title_en クロスマッチ (SeesaaWiki title_ja ↔ AniList title_en)
        #    例: NANA, D.Gray-man, CLANNAD, MAJOR, BLACK CAT
        if not match:
            nk = _normalize(sja)
            if nk in idx_en:
                match = _pick_best(idx_en[nk], year_hint)

        # 6. TVプレフィックス除去マッチ
        #    例: TVそれいけ!アンパンマン → それいけ!アンパンマン
        if not match and (sja.startswith("TV") or sja.startswith("ＴＶ")):
            tv_stripped = _normalize(_strip_tv_prefix(sja))
            if tv_stripped and tv_stripped in idx_norm:
                match = _pick_best(idx_norm[tv_stripped], year_hint)

        # 7. 逆包含マッチ (AniList title_ja が SeesaaWiki title_ja を含む)
        #    例: "NANA" → "NANA-ナナ-", "D.Gray-man" → "D.Gray-man ディー・グレイマン"
        #    品質制御: 正規化後5文字以上、候補10件以下
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

        # 8. target title_en → AniList idx_norm (target英語タイトル ↔ AniList title_ja正規化)
        if not match and sen:
            senk = _normalize(sen)
            if senk and senk in idx_norm:
                match = _pick_best(idx_norm[senk], year_hint)

        # 9. ローマ/ギリシャ数字正規化 (Ζ→Z, Ⅲ→III)
        #    例: 機動戦士ガンダムΖΖ → 機動戦士ガンダムZZ
        if not match:
            rk = _normalize(_normalize_roman_greek(sja))
            if rk != _normalize(sja) and rk in idx_norm:
                match = _pick_best(idx_norm[rk], year_hint)

        # 10. 読み括弧除去 (SeesaaWiki側)
        #     例: GATE（ゲート）自衛隊 → GATE自衛隊
        if not match:
            rp = _normalize(_strip_reading_parens(sja))
            if rp != _normalize(sja) and rp in idx_norm:
                match = _pick_best(idx_norm[rp], year_hint)

        # 11. ふりがな除去 (AniList側) — SeesaaWiki title ↔ AniList stripped
        #     例: 魔法騎士レイアース → 魔法騎士（マジックナイト）レイアース
        if not match:
            nk = _normalize(sja)
            if nk in idx_furigana:
                match = _pick_best(idx_furigana[nk], year_hint)

        # 12. 英語プレフィックス除去
        #     例: MAJOR メジャー 第3シリーズ → メジャー第3シリーズ
        if not match:
            ep = _strip_english_prefix(sja)
            if ep and ep != sja:
                epk = _normalize(ep)
                if epk and epk in idx_norm:
                    match = _pick_best(idx_norm[epk], year_hint)

        # 13. 逆包含 4文字閾値 (SeesaaWiki ⊂ AniList, 4文字以上)
        #     例: 監獄学園 → 監獄学園〈プリズンスクール〉
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

        # 14. 前方包含 (AniList ⊂ SeesaaWiki, ratio>50%)
        #     例: それいけ!アンパンマン ⊂ TVそれいけ!アンパンマン（2009年）
        #     品質制御: AniList正規化タイトルが SeesaaWiki の50%以上
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
                    # 最長一致を優先
                    fwd_candidates.sort(
                        key=lambda e: len(_normalize(e[6])),
                        reverse=True,
                    )
                    match = _pick_best(fwd_candidates[:3], year_hint)

        # 15. 漢字+ひらがなキーマッチ (英語/カタカナ部分が異なっても漢字が一致)
        #     例: 銀牙伝説WEED ↔ 銀牙伝説ウィード (共通キー: 銀牙伝説)
        #     品質制御: キー4文字以上、AniList候補が1件のみ
        if not match:
            khk = _kanji_hira_key(sja)
            if len(khk) >= 4 and khk in idx_khk:
                cands = idx_khk[khk]
                if len(cands) == 1:
                    match = cands[0]

        # 16. Fuzzy match ルール辞書 (90%+ confidence, 手動確認済)
        if not match and sja in _FUZZY_MATCH_RULES:
            anilist_ja, rule_year = _FUZZY_MATCH_RULES[sja]
            for aid, ja, en, yr, q, sea, sd, fmt in refs:
                if ja == anilist_ja and yr == rule_year:
                    match = (aid, yr, q, sea, sd, fmt, ja)
                    break

        # 16. 「映画」「劇場版」プレフィックス除去マッチ
        #     例: 映画 聲の形 → 聲の形, 劇場版シティーハンター → シティーハンター
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja) and mk in idx_norm:
                match = _pick_best(idx_norm[mk], year_hint)
            elif mk and mk in idx_movie:
                match = _pick_best(idx_movie[mk], year_hint)

        # 17. 単語順逆転マッチ (SHADOW SKILL -影技- ↔ 影技 SHADOW SKILL)
        #     品質制御: 5文字以上、候補10件以下
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
                    # また AniList側の sorted key との一致も確認
                    if not match and swk in idx_norm:
                        match = _pick_best(idx_norm[swk], year_hint)

        # 18. 映画/劇場版 prefix 除去 + AniList側の movie index
        #     例: 映画 プリキュアオールスターズF → プリキュアオールスターズF
        if not match:
            mk = _normalize(_strip_movie_prefix(sja))
            if mk and mk != _normalize(sja):
                # 前方包含も試みる (ratio>60%)
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

    # --- DB 一括更新 ---
    if updates:
        conn.executemany(
            "UPDATE anime SET year=?, quarter=?, season=?, start_date=?, format=? "
            "WHERE id=?",
            updates,
        )

    # 補完後に quarter が NULL のまま残っている作品の quarter を再計算
    # (season/start_date が補完されたが quarter が未設定のケース)
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
    """v20: credits に credit_year / credit_quarter を追加.

    短期作品 (≤12話 or episode=-1): anime の year/quarter をそのまま使用。
    長期作品 (>12話, episode情報あり): start_date + 週1放送仮定で各話の放送時期を推定。

    推定式: air_date = start_date + (episode - 1) * 7 days
    """
    from datetime import date, timedelta

    for col in ["credit_year INTEGER", "credit_quarter INTEGER"]:
        try:
            conn.execute(f"ALTER TABLE credits ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass

    # Phase 1: 全クレジットに anime の year/quarter をデフォルト設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a WHERE credits.anime_id = a.id AND a.year IS NOT NULL
    """)

    phase1 = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE credit_year IS NOT NULL"
    ).fetchone()[0]
    logger.info("credit_temporal_phase1", count=phase1)

    # Phase 2: 長期作品のエピソード別時期推定
    # 対象: start_date あり & 話数付きクレジットがある & 13話以上の作品
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

        # 放送間隔の推定
        # end_date があれば (end - start) / max_ep で計算
        # なければ週1 (7日) を仮定
        interval_days = 7
        if end_str and len(end_str) >= 10:
            try:
                ed = date.fromisoformat(end_str[:10])
                if max_ep > 1 and ed > sd:
                    interval_days = (ed - sd).days / (max_ep - 1)
                    # 妥当性チェック: 3-14日の範囲（週1±α）
                    interval_days = max(3, min(14, interval_days))
            except (ValueError, TypeError):
                pass

        # 各エピソードのクレジットを更新
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
    """v21: 拡張タイトルマッチング (title_en, TVプレフィックス, 逆包含) で再マッチ.

    v19 で追加した _backfill_from_anilist_titles に新しいマッチ戦略を追加済み。
    ここでは再実行して残りの未マッチ作品を補完し、
    新規マッチした作品の credit_year/credit_quarter も設定する。
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

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    # (credit_year IS NULL = v20 migration 時にマッチしていなかった作品)
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
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
    """v22: 深層タイトルマッチング (ローマ数字, ふりがな, 英語prefix, 前方包含).

    v21 backfill に追加した戦略 9-14 で残りの未マッチ作品を補完。
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

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
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
    """v23: Fuzzy マッチングルール (90%+ confidence) を適用して残りを補完.

    v22 backfill に追加した phase 15 (fuzzy match rules) で
    高精度なタイトル置換ペアをマッチ。
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

    # 新規マッチした作品の credits にも credit_year/credit_quarter を設定
    conn.execute("""
        UPDATE credits SET credit_year = a.year, credit_quarter = a.quarter
        FROM anime a
        WHERE credits.anime_id = a.id
          AND a.year IS NOT NULL
          AND credits.credit_year IS NULL
    """)

    # 新規マッチの長期作品もエピソード別時期推定
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
    """v24: 改良マッチング (映画prefix, 単語順逆転, normalize改善, 追加fuzzy rules).

    改善内容:
    - normalize(): 〈〉→<>統一, HTMLエンティティデコード, ◆◇等追加
    - Phase 16: 「映画」「劇場版」プレフィックス除去マッチ
    - Phase 17: 単語順逆転マッチ (SHADOW SKILL-影技- ↔ 影技 SHADOW SKILL)
    - Phase 18: 映画prefix除去+前方包含
    - _FUZZY_MATCH_RULES: v24で25件追加 (攻殻機動隊ARISE, 頭文字D等)
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
    """v25: 漢字+ひらがなキーマッチ — 英語/カタカナ部分が異なっても漢字が一致すれば同定.

    例: 銀牙伝説WEED ↔ 銀牙伝説ウィード (キー: 銀牙伝説)
    品質制御: キー4文字以上、AniList候補が1件のみ受理。
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
    """アニメを work_type (tv/tanpatsu) × scale_class (large/medium/small) に分類する.

    特徴量: log(total_animator_credits), log(unique_animators)
    最小データ閾値: total_animator_credits >= 10
    K-means K=3 を TV / 単発 それぞれ独立に適用。
    centroids は total_animator_credits の中央値でソートし 小→中→大 に対応付ける。

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
    """v26: anime テーブルに work_type / scale_class カラムを追加し K-means で分類."""
    conn.executescript("""
        ALTER TABLE anime ADD COLUMN work_type  TEXT;
        ALTER TABLE anime ADD COLUMN scale_class TEXT;
        CREATE INDEX IF NOT EXISTS idx_anime_work_type  ON anime(work_type);
        CREATE INDEX IF NOT EXISTS idx_anime_scale_class ON anime(scale_class);
    """)
    compute_anime_scale_classes(conn)


def _migrate_v27_normalize_legacy_roles(conn: sqlite3.Connection) -> None:
    """v27: credits テーブルのレガシーロール値を現行 Role enum 値に正規化.

    かつてコード側の _LEGACY_ROLE_MAP で実行時に変換していた処理を
    データとして一度だけ適用し、永続化する。

    注意: "other" は Role.OTHER として残す (分類不能クレジット用)。
          "special" は別概念 (スペシャルサンクス等) なので混同しない。
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
    """v28: scores テーブルに career_track カラムを追加.

    career_track は生データではなくパイプラインが推定した加工データ（派生属性）。
    scores テーブルに置くことで生データ（credits）との分離を維持する。

    値: 'animator' / 'animator_director' / 'director' /
        'production' / 'technical' / 'multi_track'
    """
    conn.executescript("""
        ALTER TABLE scores ADD COLUMN career_track TEXT NOT NULL DEFAULT 'multi_track';
        CREATE INDEX IF NOT EXISTS idx_scores_career_track ON scores(career_track);
    """)


def _migrate_v29_add_feat_tables(conn: sqlite3.Connection) -> None:
    """v29: feat_* 派生特徴量テーブル群を追加.

    生データ (persons/anime/credits) とパイプライン計算結果を命名で明確に分離する。
    feat_person_scores, feat_network, feat_career, feat_genre_affinity, feat_contribution
    の 5 テーブルを追加し、JSON ファイルへの依存を段階的に削減する。
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
# feat_* DAO: 派生特徴量の一括書き込み / 読み込み
# ================================================================


def upsert_feat_person_scores(
    conn: sqlite3.Connection,
    rows: list[dict],
    run_id: int | None = None,
) -> None:
    """feat_person_scores を一括 upsert する.

    Args:
        conn: SQLite 接続
        rows: scores.json の各エントリと同形式の dict リスト。
              必須キー: person_id。残りは欠損時に None。
        run_id: pipeline_runs.id (省略可)
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
    """feat_network を一括 upsert する.

    各 dict に person_id と centrality/hub_score/bridge 情報を含める。
    scores.json の centrality サブ dict および bridges.json から構築する。
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
    """feat_career を一括 upsert する.

    scores.json の career/growth サブ dict および growth.json から構築する。
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
    """feat_genre_affinity を一括 upsert する.

    Args:
        rows: {"person_id", "genre", "affinity_score", "work_count"} の dict リスト
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
    """feat_contribution を一括 upsert する.

    individual_profiles.json のエントリから構築する。
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
    """agg_milestones を一括 upsert する (L2: キャリアイベント).

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
    """agg_director_circles を一括 upsert する (L2: 共同クレジット集計).

    Args:
        circles_dict: {director_id: obj} ここで obj は DirectorCircle dataclass
                      または {members: [{person_id, shared_works, hit_rate, roles, latest_year}]} dict。
    """
    import dataclasses
    import json as _json

    batch = []
    for director_id, circle in circles_dict.items():
        # dataclass の場合は dict に変換
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
    """feat_mentorships を一括 upsert する (L3: メンター推定).

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
    """feat_person_scores を person_id → dict で返す."""
    rows = conn.execute("SELECT * FROM feat_person_scores").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def load_feat_network(conn: sqlite3.Connection) -> dict[str, dict]:
    """feat_network を person_id → dict で返す."""
    rows = conn.execute("SELECT * FROM feat_network").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def load_feat_career(conn: sqlite3.Connection) -> dict[str, dict]:
    """feat_career を person_id → dict で返す."""
    rows = conn.execute("SELECT * FROM feat_career").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def _migrate_v30_add_feat_credit_activity(conn: sqlite3.Connection) -> None:
    """v30: feat_credit_activity テーブルを追加.

    個人ごとの空白期間・活動密度・休止履歴を事前集計して格納する。
    次回パイプライン起動時に compute_feat_credit_activity() で全件再計算する。
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
    """個人ごとのクレジット空白期間・活動パターンを計算して feat_credit_activity に保存する.

    計算内容:
    - 四半期精度ギャップ: mean/median/min/max/std の間隔 (quarters)
    - 連続参加率: 間隔が1四半期の割合
    - 休止 (n ≥ 4Q): 件数・最長期間
    - 年レベルギャップ (quarter=NULL のクレジットも含む)
    - 最終クレジットからの経過四半期

    abs_quarter 表現: year * 4 + (quarter - 1)
    例: 2020Q1 → 8080, 2020Q4 → 8083

    Args:
        conn: SQLite 接続
        current_year: 基準年 (省略時: 現在年)
        current_quarter: 基準四半期 (省略時: 現在四半期)
        batch_size: 一括 INSERT のバッチサイズ

    Returns:
        書き込んだ行数
    """
    import datetime
    import math
    import statistics

    if current_year is None:
        current_year = datetime.datetime.now().year
    if current_quarter is None:
        current_quarter = (datetime.datetime.now().month - 1) // 3 + 1
    current_abs_q = current_year * 4 + current_quarter - 1

    # --- Step 1: 四半期精度データ (LAG で連続差分) ---
    # CTE で per-person gap リストを取得 (SQLite window functions)
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

    # --- Step 2: 活動範囲 (四半期) ---
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

    # --- Step 3: 年レベルギャップ (全クレジット対象) ---
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

    # 活動範囲を読み込み
    activity_rows = conn.execute(activity_sql).fetchall()
    activity = {
        r["person_id"]: {
            "first_abs_quarter": r["first_abs_quarter"],
            "last_abs_quarter": r["last_abs_quarter"],
            "active_quarters": r["active_quarters"],
        }
        for r in activity_rows
    }

    # 年レベルギャップ
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

    # ギャップを person_id ごとに集計 (streaming で読み込み)
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

    # 統計計算してバッチ挿入
    batch: list[tuple] = []
    total_written = 0

    for pid, act in activity.items():
        first_q = act["first_abs_quarter"]
        last_q = act["last_abs_quarter"]
        active_q = act["active_quarters"]
        span = last_q - first_q  # 0 の場合は活動が1四半期のみ
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
    """feat_credit_activity を person_id → dict で返す."""
    rows = conn.execute("SELECT * FROM feat_credit_activity").fetchall()
    return {r["person_id"]: dict(r) for r in rows}


def _migrate_v31_add_feat_career_annual(conn: sqlite3.Connection) -> None:
    """v31: feat_career_annual テーブルを追加.

    個人のキャリア年（デビューからの経過年数）× 職種カテゴリ別に
    作品数・クレジット数を集計して格納する。
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
    """個人 × キャリア年 × 職種カテゴリ別の作品数・クレジット数を集計して feat_career_annual に保存.

    career_year = credit_year - first_credit_year  (0 = デビュー年)

    職種カテゴリは src/utils/role_groups.py の ROLE_CATEGORY に従い 14 種に分類。
    未知の役職は works_other にカウントする。

    Args:
        conn: SQLite 接続
        batch_size: 一括 INSERT のバッチサイズ (person 単位)

    Returns:
        書き込んだ行数
    """
    from src.utils.role_groups import ROLE_CATEGORY

    # role → column名 マッピング
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

    # デビュー年を取得 (credit_year の最小値)
    debut_sql = """
        SELECT person_id, MIN(credit_year) AS debut_year
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id
    """
    debut_year = {
        r["person_id"]: r["debut_year"] for r in conn.execute(debut_sql).fetchall()
    }

    # person × year × role の集計 (ユニーク作品数・クレジット数)
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

    # person_id ごとにまとめて処理
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
    """feat_career_annual を返す.

    Args:
        person_id: 特定の人物に絞る場合に指定 (省略時は全件)

    Returns:
        {person_id, career_year, credit_year, n_works, ...} の list
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
    """v33: feat_credit_contribution / feat_person_work_summary テーブルを追加."""
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
    """v34: agg_milestones テーブルを追加 (L2: 生データから抽出したキャリアイベント)."""
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
    """v35: agg_director_circles テーブルを追加 (L2: 共同クレジット数の集計)."""
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
    """v36: feat_mentorships テーブルを追加 (L3: アルゴリズムによるメンター推定)."""
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
    """v32: feat_studio_affiliation テーブルを追加.

    個人が年別にどのスタジオの作品に参加したかを事前集計する。
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
    """個人 × 年 × スタジオ別の参加作品数を集計して feat_studio_affiliation に保存.

    credits → anime_studios → studios を結合して、
    「その年どのスタジオの作品に参加したか」を個人ごとに集計する。
    主要スタジオ (is_main=1) のみを対象にすることで、制作委員会などを除外できる。

    Args:
        conn: SQLite 接続
        batch_size: 一括 INSERT のバッチサイズ

    Returns:
        書き込んだ行数
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
    """feat_studio_affiliation を返す.

    Args:
        person_id: 特定人物に絞る (省略時は全件)
        studio_id: 特定スタジオに絞る (省略時は全件)
        main_only: True の場合 is_main_studio=1 のみ返す
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
    """個人 × 作品 × 役職ごとのスコア貢献量を計算して保存する.

    計算内容:
    - production_scale: AKM 目的変数 log1p(staff_count) × log1p(episodes) × dur_mult
      → その作品がいかに「大規模な制作」かを表す。回帰の被説明変数そのもの。
    - edge_weight: グラフ辺寄与 role_weight × episode_coverage × dur_mult
      → その役職・参加規模がネットワーク上でどれだけの重みを持つか。
    - iv_contrib_est: edge_weight_share × iv_score
      → IV スコアへの比例按分。真の LOO marginal の近似。

    注意: iv_contrib_est は近似値。正確な marginal はパイプライン再実行が必要。

    Args:
        conn: SQLite 接続
        batch_size: INSERT バッチサイズ

    Returns:
        (feat_credit_contribution 行数, feat_person_work_summary 行数)
    """
    import math

    from src.utils.config import (
        DURATION_BASELINE_MINUTES,
        DURATION_MAX_MULTIPLIER,
        ROLE_WEIGHTS,
    )

    logger.info("feat_credit_contribution_compute_start")

    # --- アニメ staff_count を事前計算 ---
    staff_sql = """
        SELECT anime_id, COUNT(DISTINCT person_id) AS staff_count
        FROM credits GROUP BY anime_id
    """
    anime_staff: dict[str, int] = {
        r["anime_id"]: r["staff_count"] for r in conn.execute(staff_sql).fetchall()
    }

    # --- アニメメタデータ ---
    anime_sql = "SELECT id, episodes, duration, format FROM anime"
    anime_meta: dict[str, dict] = {
        r["id"]: {"eps": r["episodes"] or 1, "dur": r["duration"], "fmt": r["format"]}
        for r in conn.execute(anime_sql).fetchall()
    }

    # format → dur_mult のフォールバック (duration=NULL 時)
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

    # --- (person, anime, role) 集計クエリ ---
    # episode_coverage: episode>0 の distinct episode 数 / anime.episodes
    agg_sql = """
        SELECT
            c.person_id, c.anime_id, c.role,
            MIN(c.credit_year) AS credit_year,
            COUNT(DISTINCT CASE WHEN c.episode > 0 THEN c.episode END) AS ep_count
        FROM credits c
        GROUP BY c.person_id, c.anime_id, c.role
        ORDER BY c.person_id
    """

    # --- person の iv_score をロード ---
    iv_by_pid: dict[str, float] = {}
    for r in conn.execute(
        "SELECT person_id, iv_score FROM feat_person_scores"
    ).fetchall():
        if r["iv_score"] is not None:
            iv_by_pid[r["person_id"]] = r["iv_score"]

    # --- 計算・バッチ INSERT ---
    current_pid: str | None = None
    # person 内の (anime_id → edge_weight の max, production_scale の max) を追跡
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
        """person の全クレジット処理後に edge_weight_share と iv_contrib_est を確定."""
        # rows_contrib の末尾から当 person 分を逆引きして更新するより
        # 2パスが安全。ここでは person total_ew を context として残し、
        # 2nd pass (UPDATE) で行う。
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

        # 蓄積
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
                None,  # edge_weight_share / iv_contrib_est: 2nd pass で更新
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

    # 最後の person
    if current_pid is not None:
        _flush_person(current_pid)

    # 残りをフラッシュ
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

    # --- 2nd pass: edge_weight_share と iv_contrib_est を UPDATE ---
    # feat_person_work_summary の total_edge_weight を使って按分
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
    """feat_credit_contribution を返す.

    Args:
        person_id: 特定人物に絞る
        anime_id: 特定作品に絞る
        min_edge_weight: この値以上の edge_weight に絞る
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
    """feat_person_work_summary を返す.

    Args:
        person_id: 特定人物 → dict を返す。省略時 → list[dict] を返す。
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
    """person_id → career_track のマッピングを scores テーブルに一括書き込む.

    scores 行が既に存在する person_id のみ更新する（INSERT しない）。
    まだスコアが計算されていない人物（新規スクレイプ直後など）はスキップされる。

    Args:
        conn: SQLite 接続
        career_tracks: person_id → career_track の辞書
    """
    rows = [(track, pid) for pid, track in career_tracks.items()]
    conn.executemany(
        "UPDATE scores SET career_track = ?, updated_at = CURRENT_TIMESTAMP WHERE person_id = ?",
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
    """人物の所属スタジオ情報を記録する（重複は無視）."""
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
    """APIで取得不可だった人物IDを記録する."""
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
    """取得不可と記録された人物のanilist_idセットを返す."""
    rows = conn.execute(
        "SELECT anilist_id FROM person_fetch_status WHERE source = ?",
        (source,),
    ).fetchall()
    return {row[0] for row in rows}


def upsert_person(conn: sqlite3.Connection, person: Person) -> None:
    """人物を挿入または更新する（包括的データ対応）."""
    import json

    conn.execute(
        """INSERT INTO persons (
               id, name_ja, name_en, aliases, mal_id, anilist_id, madb_id, ann_id, allcinema_id,
               image_large, image_medium, image_large_path, image_medium_path,
               date_of_birth, age, gender, years_active, hometown, blood_type,
               description, favourites, site_url
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), persons.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), persons.name_en),
               aliases = excluded.aliases,
               mal_id = COALESCE(excluded.mal_id, persons.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, persons.anilist_id),
               madb_id = COALESCE(excluded.madb_id, persons.madb_id),
               ann_id = COALESCE(excluded.ann_id, persons.ann_id),
               allcinema_id = COALESCE(excluded.allcinema_id, persons.allcinema_id),
               image_large = COALESCE(excluded.image_large, persons.image_large),
               image_medium = COALESCE(excluded.image_medium, persons.image_medium),
               image_large_path = COALESCE(excluded.image_large_path, persons.image_large_path),
               image_medium_path = COALESCE(excluded.image_medium_path, persons.image_medium_path),
               date_of_birth = COALESCE(excluded.date_of_birth, persons.date_of_birth),
               age = COALESCE(excluded.age, persons.age),
               gender = COALESCE(excluded.gender, persons.gender),
               years_active = COALESCE(excluded.years_active, persons.years_active),
               hometown = COALESCE(excluded.hometown, persons.hometown),
               blood_type = COALESCE(excluded.blood_type, persons.blood_type),
               description = COALESCE(excluded.description, persons.description),
               favourites = COALESCE(excluded.favourites, persons.favourites),
               site_url = COALESCE(excluded.site_url, persons.site_url)
        """,
        (
            person.id,
            person.name_ja,
            person.name_en,
            json.dumps(person.aliases, ensure_ascii=False),
            person.mal_id,
            person.anilist_id,
            person.madb_id,
            person.ann_id,
            getattr(person, "allcinema_id", None),
            person.image_large,
            person.image_medium,
            person.image_large_path,
            person.image_medium_path,
            person.date_of_birth,
            person.age,
            person.gender,
            json.dumps(person.years_active, ensure_ascii=False),
            person.hometown,
            person.blood_type,
            person.description,
            person.favourites,
            person.site_url,
        ),
    )


def upsert_anime(conn: sqlite3.Connection, anime: Anime) -> None:
    """アニメを挿入または更新する（包括的データ対応）."""
    import json

    conn.execute(
        """INSERT INTO anime (
               id, title_ja, title_en, year, season, episodes, mal_id, anilist_id, madb_id, ann_id, allcinema_id, score,
               cover_large, cover_extra_large, cover_medium, banner, cover_large_path, banner_path,
               description, format, status, start_date, end_date, duration, source,
               genres, tags, popularity_rank, favourites, studios,
               synonyms, mean_score, country_of_origin, is_licensed, is_adult,
               hashtag, site_url, trailer_url, trailer_site,
               relations_json, external_links_json, rankings_json
           )
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?,
                   ?, ?, ?, ?,
                   ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               title_ja = COALESCE(NULLIF(excluded.title_ja, ''), anime.title_ja),
               title_en = COALESCE(NULLIF(excluded.title_en, ''), anime.title_en),
               year = COALESCE(excluded.year, anime.year),
               season = COALESCE(excluded.season, anime.season),
               episodes = COALESCE(excluded.episodes, anime.episodes),
               mal_id = COALESCE(excluded.mal_id, anime.mal_id),
               anilist_id = COALESCE(excluded.anilist_id, anime.anilist_id),
               madb_id = COALESCE(excluded.madb_id, anime.madb_id),
               ann_id = COALESCE(excluded.ann_id, anime.ann_id),
               allcinema_id = COALESCE(excluded.allcinema_id, anime.allcinema_id),
               score = COALESCE(excluded.score, anime.score),
               cover_large = COALESCE(excluded.cover_large, anime.cover_large),
               cover_extra_large = COALESCE(excluded.cover_extra_large, anime.cover_extra_large),
               cover_medium = COALESCE(excluded.cover_medium, anime.cover_medium),
               banner = COALESCE(excluded.banner, anime.banner),
               cover_large_path = COALESCE(excluded.cover_large_path, anime.cover_large_path),
               banner_path = COALESCE(excluded.banner_path, anime.banner_path),
               description = COALESCE(excluded.description, anime.description),
               format = COALESCE(excluded.format, anime.format),
               status = COALESCE(excluded.status, anime.status),
               start_date = COALESCE(excluded.start_date, anime.start_date),
               end_date = COALESCE(excluded.end_date, anime.end_date),
               duration = COALESCE(excluded.duration, anime.duration),
               source = COALESCE(excluded.source, anime.source),
               genres = COALESCE(excluded.genres, anime.genres),
               tags = COALESCE(excluded.tags, anime.tags),
               popularity_rank = COALESCE(excluded.popularity_rank, anime.popularity_rank),
               favourites = COALESCE(excluded.favourites, anime.favourites),
               studios = COALESCE(excluded.studios, anime.studios),
               synonyms = COALESCE(excluded.synonyms, anime.synonyms),
               mean_score = COALESCE(excluded.mean_score, anime.mean_score),
               country_of_origin = COALESCE(excluded.country_of_origin, anime.country_of_origin),
               is_licensed = COALESCE(excluded.is_licensed, anime.is_licensed),
               is_adult = COALESCE(excluded.is_adult, anime.is_adult),
               hashtag = COALESCE(excluded.hashtag, anime.hashtag),
               site_url = COALESCE(excluded.site_url, anime.site_url),
               trailer_url = COALESCE(excluded.trailer_url, anime.trailer_url),
               trailer_site = COALESCE(excluded.trailer_site, anime.trailer_site),
               relations_json = COALESCE(excluded.relations_json, anime.relations_json),
               external_links_json = COALESCE(excluded.external_links_json, anime.external_links_json),
               rankings_json = COALESCE(excluded.rankings_json, anime.rankings_json),
               updated_at = CURRENT_TIMESTAMP
        """,
        (
            anime.id,
            anime.title_ja,
            anime.title_en,
            anime.year,
            anime.season,
            anime.episodes,
            anime.mal_id,
            anime.anilist_id,
            anime.madb_id,
            anime.ann_id,
            getattr(anime, "allcinema_id", None),
            anime.score,
            anime.cover_large,
            anime.cover_extra_large,
            anime.cover_medium,
            anime.banner,
            anime.cover_large_path,
            anime.banner_path,
            anime.description,
            anime.format,
            anime.status,
            anime.start_date,
            anime.end_date,
            anime.duration,
            anime.source,
            json.dumps(anime.genres, ensure_ascii=False),
            json.dumps(anime.tags, ensure_ascii=False),
            anime.popularity_rank,
            anime.favourites,
            json.dumps(anime.studios, ensure_ascii=False),
            json.dumps(anime.synonyms, ensure_ascii=False),
            anime.mean_score,
            anime.country_of_origin,
            1 if anime.is_licensed else (0 if anime.is_licensed is not None else None),
            1 if anime.is_adult else (0 if anime.is_adult is not None else None),
            anime.hashtag,
            anime.site_url,
            anime.trailer_url,
            anime.trailer_site,
            anime.relations_json,
            anime.external_links_json,
            anime.rankings_json,
        ),
    )


_ANIME_ANALYSIS_COLUMNS = (
    "id", "title_ja", "title_en", "year", "season", "quarter", "episodes",
    "format", "duration", "start_date", "end_date", "status", "source",
    "work_type", "scale_class", "mal_id", "anilist_id", "ann_id",
    "allcinema_id", "madb_id",
)

_ANIME_DISPLAY_COLUMNS = (
    "id", "score", "popularity", "popularity_rank", "favourites", "mean_score",
    "description", "cover_large", "cover_extra_large", "cover_medium",
    "cover_large_path", "banner", "banner_path", "site_url",
    "genres", "tags", "studios", "synonyms",
    "country_of_origin", "is_adult",
    "relations_json", "external_links_json", "rankings_json",
)


def upsert_anime_analysis(conn: sqlite3.Connection, row: dict) -> None:
    """anime_analysis (silver.analysis) へ upsert。score カラムは受け付けない."""
    if "score" in row:
        raise ValueError("score must not enter silver.analysis layer (anime_analysis)")
    cols = [c for c in _ANIME_ANALYSIS_COLUMNS if c in row]
    if not cols:
        return
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join(
        f"{c} = COALESCE(excluded.{c}, anime_analysis.{c})"
        for c in cols
        if c != "id"
    )
    conn.execute(
        f"INSERT INTO anime_analysis ({col_list}) VALUES ({placeholders})"
        f" ON CONFLICT(id) DO UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP",
        [row[c] for c in cols],
    )


def upsert_anime_display(conn: sqlite3.Connection, row: dict) -> None:
    """anime_display (silver.display) へ upsert。id は anime_analysis に存在する必要あり."""
    cols = [c for c in _ANIME_DISPLAY_COLUMNS if c in row]
    if not cols:
        return
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    updates = ", ".join(
        f"{c} = COALESCE(excluded.{c}, anime_display.{c})"
        for c in cols
        if c != "id"
    )
    conn.execute(
        f"INSERT INTO anime_display ({col_list}) VALUES ({placeholders})"
        f" ON CONFLICT(id) DO UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP",
        [row[c] for c in cols],
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
    notes: str | None = None,
) -> None:
    """meta_lineage テーブルに Gold テーブルの系譜情報を登録."""
    import json as _json

    row_count = None
    try:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """INSERT INTO meta_lineage
               (table_name, audience, source_silver_tables, source_bronze_forbidden,
                source_display_allowed, formula_version, computed_at,
                ci_method, null_model, holdout_method, row_count, notes)
           VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
           ON CONFLICT(table_name) DO UPDATE SET
               source_silver_tables = excluded.source_silver_tables,
               source_bronze_forbidden = excluded.source_bronze_forbidden,
               source_display_allowed = excluded.source_display_allowed,
               formula_version = excluded.formula_version,
               computed_at = CURRENT_TIMESTAMP,
               ci_method = excluded.ci_method,
               null_model = excluded.null_model,
               holdout_method = excluded.holdout_method,
               row_count = excluded.row_count,
               notes = excluded.notes""",
        (
            table_name, audience,
            _json.dumps(source_silver_tables, ensure_ascii=False),
            source_bronze_forbidden, source_display_allowed,
            formula_version, ci_method, null_model, holdout_method,
            row_count, notes,
        ),
    )


def insert_credit(conn: sqlite3.Connection, credit: Credit) -> None:
    """クレジットを挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO credits (person_id, anime_id, role, raw_role, episode, source)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            credit.person_id,
            credit.anime_id,
            credit.role.value,
            credit.raw_role or "",  # 元のロール文字列を保存（NOT NULL）
            credit.episode if credit.episode is not None else -1,
            credit.source,
        ),
    )


def upsert_character(conn: sqlite3.Connection, character: Character) -> None:
    """キャラクターを挿入または更新する."""
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
    """キャラクター×声優×作品の関係を挿入する（重複は無視）."""
    conn.execute(
        """INSERT OR IGNORE INTO character_voice_actors
           (character_id, person_id, anime_id, character_role, source)
           VALUES (?, ?, ?, ?, ?)""",
        (cva.character_id, cva.person_id, cva.anime_id, cva.character_role, cva.source),
    )


def upsert_studio(conn: sqlite3.Connection, studio: Studio) -> None:
    """スタジオを挿入または更新する."""
    conn.execute(
        """INSERT INTO studios (id, name, anilist_id, is_animation_studio, favourites, site_url)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               name = COALESCE(NULLIF(excluded.name, ''), studios.name),
               anilist_id = COALESCE(excluded.anilist_id, studios.anilist_id),
               is_animation_studio = COALESCE(excluded.is_animation_studio, studios.is_animation_studio),
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
            studio.favourites,
            studio.site_url,
        ),
    )


def insert_anime_studio(conn: sqlite3.Connection, anime_studio: AnimeStudio) -> None:
    """アニメ×スタジオの関係を挿入する（重複は無視）."""
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
    """アニメ間の関連を挿入する（重複は無視）."""
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
    """スコアを挿入または更新する."""
    conn.execute(
        """INSERT INTO scores
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
    """全人物を読み込む."""
    from src.db_rows import PersonRow

    rows = conn.execute("SELECT * FROM persons").fetchall()
    return [Person.from_db_row(PersonRow.from_row(row)) for row in rows]


def load_all_anime(conn: sqlite3.Connection) -> list[Anime]:
    """全アニメを読み込む."""
    from src.db_rows import AnimeRow

    rows = conn.execute("SELECT * FROM anime").fetchall()
    return [Anime.from_db_row(AnimeRow.from_row(row)) for row in rows]


def load_all_credits(conn: sqlite3.Connection) -> list[Credit]:
    """全クレジットを読み込む."""
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
    """DB統計情報を取得する."""
    stats: dict[str, int | float] = {}

    for table in ("persons", "anime", "credits", "scores"):
        stats[f"{table}_count"] = conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]  # noqa: S608

    # 役職分布
    role_counts = conn.execute("""
        SELECT role, COUNT(*) as cnt FROM credits
        GROUP BY role ORDER BY cnt DESC
    """).fetchall()
    stats["distinct_roles"] = len(role_counts)

    # 年代カバレッジ
    year_range = conn.execute("""
        SELECT MIN(year), MAX(year) FROM anime WHERE year IS NOT NULL
    """).fetchone()
    if year_range[0]:
        stats["year_min"] = year_range[0]
        stats["year_max"] = year_range[1]

    # ソース別クレジット数
    source_counts = conn.execute("""
        SELECT source, COUNT(*) as cnt FROM credits
        GROUP BY source ORDER BY cnt DESC
    """).fetchall()
    for source, cnt in source_counts:
        stats[f"credits_source_{source or 'unknown'}"] = cnt

    # 平均クレジット数/人
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
    """名前またはIDで人物を検索する.

    Args:
        conn: DB接続
        query: 検索文字列（部分一致）
        limit: 最大件数

    Returns:
        [{id, name_ja, name_en, iv_score, credit_count}]
    """
    pattern = f"%{query}%"
    rows = conn.execute(
        """SELECT p.id, p.name_ja, p.name_en,
                  s.iv_score, s.person_fe, s.birank, s.patronage,
                  (SELECT COUNT(*) FROM credits c WHERE c.person_id = p.id) as credit_count
           FROM persons p
           LEFT JOIN scores s ON p.id = s.person_id
           WHERE p.name_ja LIKE ? OR p.name_en LIKE ? COLLATE NOCASE OR p.id LIKE ?
           ORDER BY s.iv_score DESC NULLS LAST
           LIMIT ?""",
        (pattern, pattern, pattern, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def update_data_source(
    conn: sqlite3.Connection,
    source: str,
    item_count: int,
    status: str = "ok",
) -> None:
    """データソースの最終取得日時を更新する."""
    conn.execute(
        """INSERT INTO data_sources (source, last_scraped_at, item_count, status)
           VALUES (?, CURRENT_TIMESTAMP, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
               last_scraped_at = CURRENT_TIMESTAMP,
               item_count = excluded.item_count,
               status = excluded.status
        """,
        (source, item_count, status),
    )


def get_data_sources(conn: sqlite3.Connection) -> list[dict]:
    """全データソースの情報を取得する."""
    rows = conn.execute(
        "SELECT source, last_scraped_at, item_count, status FROM data_sources ORDER BY source"
    ).fetchall()
    return [dict(r) for r in rows]


def load_all_scores(conn: sqlite3.Connection) -> list[ScoreResult]:
    """全スコアを読み込む."""
    rows = conn.execute("SELECT * FROM scores").fetchall()
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
    """パイプライン実行を記録する."""
    cursor = conn.execute(
        """INSERT INTO pipeline_runs (credit_count, person_count, elapsed_seconds, mode)
           VALUES (?, ?, ?, ?)""",
        (credit_count, person_count, elapsed, mode),
    )
    return cursor.lastrowid or 0


def get_last_pipeline_run(conn: sqlite3.Connection) -> dict | None:
    """最後のパイプライン実行情報を取得する."""
    row = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def has_credits_changed_since_last_run(conn: sqlite3.Connection) -> bool:
    """最後のパイプライン実行以降にクレジットデータが変化したか判定する.

    credit_count と person_count を比較して変化を検出する。
    前回のパイプライン実行が存在しない場合は True を返す。

    Returns:
        True: クレジットまたは人物数が変化した、またはパイプライン未実行
        False: データに変化なし
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
    """指定パイプライン実行以降に新しいクレジットが追加された人物IDを返す.

    新クレジットは id > (last run の最大 credit id) で近似する。
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
    """スコア履歴を保存する（年・四半期付き）."""
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
    """人物のスコア履歴を取得する（新しい順）."""
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
    """四半期ごとのスコア履歴を取得する."""
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
    """既存の全人物IDを高速取得する（スキップ判定用）."""
    rows = conn.execute("SELECT id FROM persons").fetchall()
    return {row["id"] for row in rows}


# ---------------------------------------------------------------------------
# LLM decision cache — DB-backed persistence
# ---------------------------------------------------------------------------


def get_llm_decision(conn: sqlite3.Connection, name: str, task: str) -> dict | None:
    """LLM判定キャッシュを取得する.

    Args:
        name: 対象の名前 (人物名・ペア名)
        task: タスク種別 ("org_classification" | "name_normalization" | "entity_match")

    Returns:
        result_json を dict にパースした結果、なければ None
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
    """LLM判定結果を保存/更新する."""
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
    """指定タスクの全LLM判定キャッシュを一括取得する.

    Returns:
        {name: result_dict} の辞書
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
# v37–v40 マイグレーション関数
# =============================================================================


def _migrate_v37_credit_contribution_career_year(conn: sqlite3.Connection) -> None:
    """v37: feat_credit_contribution にキャリア年次列を追加.

    debut_year, career_year_at_credit, is_debut_work の3列を追加し、
    credits テーブルから個人のデビュー年を計算してバックフィルする。
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

    # 各 person のデビュー年をメモリに読み込み
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
    """v38: feat_work_context テーブルを追加."""
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
    """v39: feat_person_role_progression テーブルを追加."""
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
    """v40: feat_causal_estimates テーブルを追加."""
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
# v37: feat_credit_contribution への career_year 付与を含む compute
# =============================================================================


def _backfill_credit_contribution_career_year(conn: sqlite3.Connection) -> None:
    """feat_credit_contribution の career_year 関連列を計算・更新する.

    compute_feat_credit_contribution の INSERT 後に呼び出す。
    既存行 (debut_year IS NULL) のみ更新する。
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
# v38: feat_work_context の compute 関数
# =============================================================================


def compute_feat_work_context(
    conn: sqlite3.Connection,
    current_year: int | None = None,
) -> int:
    """作品ごとのチーム統計を集計して feat_work_context に保存.

    feat_credit_contribution の career_year_at_credit を使うため、
    _backfill_credit_contribution_career_year() 実行後に呼ぶこと。

    Args:
        conn: SQLite 接続
        current_year: 現在の年 (省略時は credits の最大年を使用)

    Returns:
        書き込んだ行数
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

    # アニメメタデータ
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

    # アニメの代表年（credits の最小 credit_year）
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

    # feat_credit_contribution からロール・キャリア年次を取得
    # (person_id, anime_id, role, career_year_at_credit) を集計
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
                None,  # difficulty_score: era_effects から別途更新
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
# v39: feat_person_role_progression の compute 関数
# =============================================================================


def compute_feat_person_role_progression(
    conn: sqlite3.Connection,
    current_year: int | None = None,
    active_threshold_years: int = 3,
    batch_size: int = 10000,
) -> int:
    """個人×職種カテゴリの時系列進行を集計して feat_person_role_progression に保存.

    Args:
        conn: SQLite 接続
        current_year: 現在の年 (省略時は credits の最大年)
        active_threshold_years: 最終クレジットからこの年数以内なら still_active=1
        batch_size: INSERT バッチサイズ

    Returns:
        書き込んだ行数
    """
    from collections import defaultdict

    from src.utils.role_groups import ROLE_CATEGORY

    logger.info("feat_person_role_progression_compute_start")

    if current_year is None:
        row = conn.execute(
            "SELECT MAX(credit_year) FROM credits WHERE credit_year IS NOT NULL"
        ).fetchone()
        current_year = row[0] if row and row[0] else 2024

    # デビュー年 per person
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
    # credits から直接集計
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
# v40: feat_causal_estimates の upsert 関数 (パイプラインから呼び出し)
# =============================================================================


def upsert_feat_causal_estimates(
    conn: sqlite3.Connection,
    peer_boosts: dict[str, float],
    friction_index: dict[str, float],
    era_fe_by_person: dict[str, float],
    iv_scores: dict[str, float],
    opportunity_residuals: dict[str, float] | None = None,
) -> int:
    """因果推論結果を feat_causal_estimates に保存する.

    Args:
        conn: SQLite 接続
        peer_boosts: person_id → ピア効果ブースト (PeerEffectResult.person_peer_boost)
        friction_index: person_id → キャリア摩擦指数 (0=摩擦なし, 1=最大)
        era_fe_by_person: person_id → 当人のデビュー年に対応する時代固定効果
        iv_scores: person_id → IV スコア (era_deflated_iv 計算用)
        opportunity_residuals: person_id → 機会補正残差 (省略可)

    Returns:
        書き込んだ行数
    """
    all_pids = set(peer_boosts) | set(friction_index) | set(era_fe_by_person)
    rows: list[tuple] = []
    for pid in all_pids:
        era = era_fe_by_person.get(pid)
        iv = iv_scores.get(pid)
        # era_deflated_iv: iv_score を時代固定効果で補正 (era_fe > 0 の年は恵まれた時代)
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
    """feat_causal_estimates を返す.

    Args:
        person_id: 特定人物 → dict を返す。省略時 → {person_id: dict} を返す。
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
    """feat_work_context を返す.

    Args:
        anime_id: 特定作品 → dict を返す。省略時 → {anime_id: dict} を返す。
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
    """feat_person_role_progression を返す.

    Args:
        person_id: 特定人物に絞る
        role_category: 特定職種カテゴリに絞る
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
# v41: feat_cluster_membership マイグレーション + upsert + load
# =============================================================================


def _migrate_v41_add_feat_cluster_membership(conn: sqlite3.Connection) -> None:
    """v41: feat_cluster_membership テーブルを追加."""
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
    """クラスタリング帰属を feat_cluster_membership に保存する.

    Args:
        conn: SQLite 接続
        community_map: person_id → community_id (Phase 4 グラフコミュニティ)
        career_tracks: person_id → career_track 文字列 (Phase 6)
        growth_data: growth.json 相当 {"persons": {pid: {"trend": ...}}} または list
        studio_clustering: studio_clustering.json 相当 {"assignments": {studio: {...}}}
        cooccurrence_groups: cooccurrence_groups.json 相当 {"groups": [{members: [...]}]}
        studio_affiliation: person_id → main_studio_id (省略時は feat_studio_affiliation から取得)

    Returns:
        書き込んだ行数
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

    # --- 全 person_id の集合 ---
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
    """feat_cluster_membership を返す.

    Args:
        person_id: 特定人物 → dict を返す。省略時 → {person_id: dict} を返す。
        community_id: コミュニティIDで絞る
        career_track: キャリアトラックで絞る
        growth_trend: 成長トレンドで絞る
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
# v42: feat_birank_annual — 年次BiRankスナップショット
# =============================================================================

_BIRANK_ANNUAL_MIN_YEAR = 1980  # それ以前はグラフが極小でスコアが無意味


def _migrate_v42_add_feat_birank_annual(conn: sqlite3.Connection) -> None:
    """v42: feat_birank_annual テーブルを追加."""
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
    """年次BiRankスナップショットを feat_birank_annual に保存する.

    Args:
        conn: SQLite 接続
        birank_timelines: {person_id: {"snapshots": [{year, birank, raw_pagerank,
            graph_size, n_credits_cumulative}, ...], ...}}
            compute_temporal_pagerank の戻り値を asdict() したもの。
        min_year: この年以降のスナップショットのみ保存（デフォルト 1980）

    Returns:
        書き込んだ行数
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
    """feat_birank_annual を返す.

    Args:
        person_id: 特定人物 → list[dict] (年昇順)。省略時 → {person_id: list[dict]}。
        year_from: この年以降（inclusive）
        year_to: この年以前（inclusive）

    Returns:
        person_id 指定時: [{"year": ..., "birank": ..., ...}, ...]
        全件時: {"person_id": [snapshots...], ...}
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
# v43: birank_compute_state — BiRank 計算入力フィンガープリント（変更検出用）
# =============================================================================


def _migrate_v43_add_birank_compute_state(conn: sqlite3.Connection) -> None:
    """v43: birank_compute_state テーブルを追加."""
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
    """年別 BiRank 計算フィンガープリントを保存する.

    Args:
        conn: SQLite 接続
        states: {year: {"credit_count": int, "anime_count": int, "person_count": int}}
                各年について、計算に使用したクレジット数・アニメ数・人物数を記録。
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
    """保存済み BiRank 計算フィンガープリントを返す.

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
# v44: 作品規模ティア — format + episodes + duration のみで計算
# スタッフ数はデータ品質が低い（中央値 2〜11）ため使用しない
# =============================================================================

# format_group: 類似フォーマットをグループ化
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
# episode_bracket: 話数で大まかに区分
#   TV:      4-cour+(48+) → major, 2-cour(24-47) → large, 1-cour(12-23) → standard,
#            half-cour(6-11) → small, <6 → micro
#   ONA:     24+ → large, 12-23 → standard, 6-11 → small, <6 → micro
#   OVA:     6+ → standard, 3-5 → small, 1-2 → micro
#   MOVIE:   長尺(90min+) → major, 標準(45-89min) → large, 短編(<45min) → standard
#   SPECIAL: small
#   TV_SHORT:話数で TV に準ずるが一段下
#   MUSIC:   micro
def _compute_scale_raw(
    fmt: str | None,
    episodes: int | None,
    duration: int | None,  # minutes per episode
) -> float:
    """format + episodes + duration から連続スコアを計算.

    スタッフ数を使わないため、スクレイピング不完全な作品でも公平に評価できる。

    Returns:
        scale_raw: 0.0〜5.0 の連続値
    """
    eps = episodes or 1
    dur = duration or 0  # minutes per episode
    total_min = eps * dur  # 総時間 (分)

    group = _FORMAT_GROUP.get(fmt, "other")

    if group == "movie":
        # 映画: 総時間で評価（話数は通常1なので意味がない）
        if total_min >= 120:
            return 5.0
        elif total_min >= 90:
            return 4.5
        elif total_min >= 60:
            return 4.0
        elif total_min >= 30:
            return 3.0
        else:
            return 2.5  # 短編映画

    elif group == "tv":
        if eps >= 48:
            return 5.0  # 4-cour以上 (4クール+)
        elif eps >= 36:
            return 4.5  # 3-cour
        elif eps >= 24:
            return 4.0  # 2-cour
        elif eps >= 13:
            return 3.0  # 1-cour (13話)
        elif eps >= 10:
            return 2.5  # 12話近辺
        elif eps >= 6:
            return 2.0  # 半クール
        else:
            return 1.5  # 単発〜5話

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
        # TV_SHORTは話数が多くても尺が短い → 一段下げ
        if eps >= 48:
            return 4.0
        elif eps >= 24:
            return 3.0
        elif eps >= 12:
            return 2.0
        else:
            return 1.5

    elif group == "special":
        # SPECIAL: 基本的に単発、総時間で微調整
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
    (0.0, 1, "micro"),  # それ以下 → tier 1
]


def _scale_raw_to_tier(raw: float) -> tuple[int, str]:
    for threshold, tier, label in _TIER_THRESHOLDS:
        if raw >= threshold:
            return tier, label
    return 1, "micro"


def _migrate_v44_add_work_scale_tier(conn: sqlite3.Connection) -> None:
    """v44: feat_work_context に作品規模ティア列を追加してバックフィル.

    スタッフ数を使わず format + episodes + duration だけで計算する。
    これにより、スクレイピング不完全な作品でも正確に規模を把握できる。
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
    """全作品の scale_tier を計算して feat_work_context に書き込む.

    feat_work_context が存在しない作品 (ID が credits にあるが work_context に未登録) も
    anime テーブルから直接処理して INSERT する。

    Returns:
        更新した行数
    """
    logger.info("work_scale_tier_compute_start")

    rows = conn.execute("SELECT id, format, episodes, duration FROM anime").fetchall()

    updates: list[tuple] = []
    inserts: list[tuple] = []  # feat_work_context に未登録の作品
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
            # anime テーブルにあるが feat_work_context 未登録 → 最小限の行を INSERT
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
    """v45: feat_career_gaps — キャリアギャップ (退職/準退職/復職) 統計テーブル."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS feat_career_gaps (
            person_id TEXT NOT NULL,
            gap_start_year INTEGER NOT NULL,  -- 最後のクレジット年
            gap_end_year INTEGER,             -- 復帰年 (NULL = 未復帰)
            gap_length INTEGER NOT NULL,      -- ギャップ年数
            returned INTEGER NOT NULL DEFAULT 0,  -- 復帰したか (0/1)
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
    """キャリアギャップを計算して feat_career_gaps に保存.

    各人のクレジット年リストから連続するギャップを検出:
    - semi_exit: semi_exit_years <= gap < exit_years (準退職)
    - exit: gap >= exit_years (退職)
    - returned: ギャップ後にクレジットがあれば True

    Returns:
        Total rows written.
    """
    logger = structlog.get_logger()
    logger.info("feat_career_gaps_compute_start",
                semi_exit_years=semi_exit_years, exit_years=exit_years)

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
                inserts.append((
                    pid,
                    gap_start,
                    gap_end,
                    gap,
                    1,  # returned = True (there is a subsequent credit)
                    gap_type,
                ))

        # Check if the person's last credit year indicates an ongoing gap
        last_year = years[-1]
        ongoing_gap = reliable_max_year - last_year
        if ongoing_gap >= semi_exit_years:
            gap_type = "exit" if ongoing_gap >= exit_years else "semi_exit"
            inserts.append((
                pid,
                last_year,
                None,  # gap_end = NULL (not yet returned)
                ongoing_gap,
                0,  # returned = False
                gap_type,
            ))

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
    logger.info("feat_career_gaps_computed",
                total=total, returned=n_returned, exits=n_exit, semi_exits=n_semi)
    return total


# ── v46: ann_id columns ─────────────────────────────────────────────


def _migrate_v46_add_ann_ids(conn: sqlite3.Connection) -> None:
    """v46: anime と persons に ann_id カラム + ユニーク索引を追加."""
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
    """v47: anime と persons に allcinema_id カラム + ユニーク索引を追加."""
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
    """v48: Bronze 層ソーステーブルを追加する (既存 DB への適用)."""
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
    """v49: Silver 層 (anime_analysis / anime_display) と Gold 層 (meta_lineage) を追加.

    anime_analysis: スコアカラムを持たない分析専用テーブル
    anime_display:  score/popularity/description などの表示専用テーブル
    meta_lineage:   Gold テーブルのデータ系譜 (lineage) 管理
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
        "id", "title_ja", "title_en", "year", "season", "quarter", "episodes",
        "work_type", "scale_class", "mal_id", "anilist_id", "ann_id",
        "allcinema_id", "madb_id",
    ]
    analysis_cols = [c for c in _analysis_candidates if c in anime_cols]
    if analysis_cols:
        col_list = ", ".join(analysis_cols)
        conn.execute(f"INSERT OR IGNORE INTO anime_analysis ({col_list}) SELECT {col_list} FROM anime")

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
        "id", "score", "popularity_rank", "favourites", "mean_score",
        "description", "cover_large", "cover_extra_large", "cover_medium",
        "cover_large_path", "banner", "banner_path", "site_url",
        "genres", "tags", "studios", "synonyms",
        "country_of_origin", "is_adult",
        "relations_json", "external_links_json", "rankings_json",
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
    logger.info("v49_silver_layer_populated", anime_analysis=n_analysis, anime_display=n_display)


# ── src_* テーブルへの書き込み関数 ────────────────────────────────────────────
# これらは Bronze 層 (生スクレイプデータ) への書き込み専用。
# canonical テーブル (anime/persons/credits) には一切触れない。


def upsert_src_anilist_anime(conn: sqlite3.Connection, anime: "Anime") -> None:
    """AniList アニメ生データを src_anilist_anime に保存."""
    import json as _json
    if anime.anilist_id is None:
        return
    conn.execute(
        """INSERT INTO src_anilist_anime (
               anilist_id, title_ja, title_en, year, season, episodes, format,
               status, start_date, end_date, duration, source, description,
               score, genres, tags, studios, synonyms, cover_large, cover_medium,
               banner, popularity, favourites, site_url, mal_id
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
               cover_large = COALESCE(excluded.cover_large, src_anilist_anime.cover_large),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            anime.anilist_id,
            anime.title_ja, anime.title_en,
            anime.year, anime.season, anime.episodes, anime.format,
            anime.status, anime.start_date, anime.end_date,
            anime.duration, anime.source, anime.description,
            anime.score,
            _json.dumps(anime.genres, ensure_ascii=False),
            _json.dumps(anime.tags, ensure_ascii=False),
            _json.dumps(anime.studios, ensure_ascii=False),
            _json.dumps(anime.synonyms, ensure_ascii=False),
            anime.cover_large, anime.cover_medium, anime.banner,
            anime.popularity_rank, anime.favourites, anime.site_url,
            anime.mal_id,
        ),
    )


def upsert_src_anilist_person(conn: sqlite3.Connection, person: "Person") -> None:
    """AniList 人物生データを src_anilist_persons に保存."""
    import json as _json
    if person.anilist_id is None:
        return
    conn.execute(
        """INSERT INTO src_anilist_persons (
               anilist_id, name_ja, name_en, aliases, date_of_birth, age, gender,
               years_active, hometown, blood_type, description,
               image_large, image_medium, favourites, site_url
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(anilist_id) DO UPDATE SET
               name_ja = COALESCE(NULLIF(excluded.name_ja, ''), src_anilist_persons.name_ja),
               name_en = COALESCE(NULLIF(excluded.name_en, ''), src_anilist_persons.name_en),
               aliases = excluded.aliases,
               date_of_birth = COALESCE(excluded.date_of_birth, src_anilist_persons.date_of_birth),
               description = COALESCE(excluded.description, src_anilist_persons.description),
               image_large = COALESCE(excluded.image_large, src_anilist_persons.image_large),
               scraped_at = CURRENT_TIMESTAMP""",
        (
            person.anilist_id,
            person.name_ja, person.name_en,
            _json.dumps(person.aliases, ensure_ascii=False),
            person.date_of_birth, person.age, person.gender,
            _json.dumps(person.years_active, ensure_ascii=False),
            person.hometown, person.blood_type, person.description,
            person.image_large, person.image_medium,
            person.favourites, person.site_url,
        ),
    )


def insert_src_anilist_credit(
    conn: sqlite3.Connection,
    anilist_anime_id: int,
    anilist_person_id: int,
    role: str,
    role_raw: str,
) -> None:
    """AniList クレジット生データを src_anilist_credits に保存."""
    conn.execute(
        """INSERT OR IGNORE INTO src_anilist_credits
               (anilist_anime_id, anilist_person_id, role, role_raw)
           VALUES (?, ?, ?, ?)""",
        (anilist_anime_id, anilist_person_id, role, role_raw),
    )


def upsert_src_ann_anime(conn: sqlite3.Connection, rec: object) -> None:
    """ANN アニメ生データを src_ann_anime に保存 (AnnAnimeRecord 受け取り)."""
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
            rec.ann_id, rec.title_en, rec.title_ja,
            rec.year, rec.episodes, rec.format,
            _json.dumps(rec.genres, ensure_ascii=False),
            rec.start_date, rec.end_date,
        ),
    )


def upsert_src_ann_person(conn: sqlite3.Connection, detail: object) -> None:
    """ANN 人物生データを src_ann_persons に保存 (AnnPersonDetail 受け取り)."""
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
            detail.ann_id, detail.name_en, detail.name_ja,
            detail.date_of_birth, detail.hometown,
            detail.blood_type, detail.website, detail.description,
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
    """ANN クレジット生データを src_ann_credits に保存."""
    conn.execute(
        """INSERT OR IGNORE INTO src_ann_credits
               (ann_anime_id, ann_person_id, name_en, role, role_raw)
           VALUES (?, ?, ?, ?, ?)""",
        (ann_anime_id, ann_person_id, name_en, role, role_raw),
    )


def upsert_src_allcinema_anime(conn: sqlite3.Connection, rec: object) -> None:
    """allcinema アニメ生データを src_allcinema_anime に保存."""
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
    """allcinema 人物生データを src_allcinema_persons に保存."""
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


def insert_src_allcinema_credit(conn: sqlite3.Connection, allcinema_anime_id: int, credit: object) -> None:
    """allcinema クレジット生データを src_allcinema_credits に保存."""
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


def upsert_src_seesaawiki_anime(conn: sqlite3.Connection, anime_id: str, title_ja: str, year: int | None, episodes: int | None) -> None:
    """seesaawiki アニメ生データを src_seesaawiki_anime に保存."""
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
    episode: int = -1,
    affiliation: str | None = None,
    is_company: bool = False,
) -> None:
    """seesaawiki クレジット生データを src_seesaawiki_credits に保存."""
    conn.execute(
        """INSERT OR IGNORE INTO src_seesaawiki_credits
               (anime_src_id, person_name, role, role_raw, episode, affiliation, is_company)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (anime_src_id, person_name, role, role_raw, episode, affiliation, int(is_company)),
    )


def upsert_src_keyframe_anime(
    conn: sqlite3.Connection,
    slug: str,
    title_ja: str,
    title_en: str,
    anilist_id: int | None,
) -> None:
    """KeyFrame アニメ生データを src_keyframe_anime に保存."""
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
    episode: int = -1,
) -> None:
    """KeyFrame クレジット生データを src_keyframe_credits に保存."""
    conn.execute(
        """INSERT OR IGNORE INTO src_keyframe_credits
               (keyframe_slug, kf_person_id, name_ja, name_en, role_ja, role_en, episode)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (keyframe_slug, kf_person_id, name_ja, name_en, role_ja, role_en, episode),
    )

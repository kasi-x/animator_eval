"""プロジェクト共通のパス定義・定数."""

from pathlib import Path

# プロジェクトルート
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# データディレクトリ
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# 出力ディレクトリ
RESULT_DIR = ROOT_DIR / "result"
DB_DIR = RESULT_DIR / "db"
JSON_DIR = RESULT_DIR / "json"
NOTEBOOKS_DIR = RESULT_DIR / "notebooks"

# PageRank パラメータ
DAMPING_FACTOR = 0.85
MAX_ITERATIONS = 100
CONVERGENCE_THRESHOLD = 1e-6

# 役職の重み（エッジ重み係数）
ROLE_WEIGHTS: dict[str, float] = {
    "director": 3.0,
    "chief_animation_director": 2.8,
    "animation_director": 2.5,
    "character_designer": 2.3,
    "key_animator": 2.0,
    "storyboard": 2.0,
    "episode_director": 2.5,
    "second_key_animator": 1.5,
    "in_between": 1.0,
    "mechanical_designer": 1.8,
    "art_director": 2.0,
    "color_designer": 1.5,
    "photography_director": 1.8,
    "effects": 1.5,
    "producer": 1.5,
    "sound_director": 1.8,
    "music": 1.2,
    "series_composition": 2.0,
    "screenplay": 1.8,
    "original_creator": 1.0,
    "background_art": 1.3,
    "cgi_director": 2.0,
    "layout": 1.5,
    "other": 1.0,
}

# 統合スコアの重み
COMPOSITE_WEIGHTS = {
    "authority": 0.4,
    "trust": 0.35,
    "skill": 0.25,
}

# 正規化方式: "minmax" | "percentile" | "zscore"
NORMALIZATION_METHOD = "minmax"

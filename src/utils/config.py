"""プロジェクト共通のパス定義・定数."""

import os
from pathlib import Path

import structlog
from dotenv import load_dotenv

log = structlog.get_logger()

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
HTML_DIR = RESULT_DIR / "html"
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

# AI-assisted entity resolution
# Ollama (OpenAI-compatible API)
LLM_BASE_URL = "http://localhost:11434/v1"  # Ollama default
LLM_MODEL_NAME = "qwen3:8b"  # or qwen3:32b for better accuracy
LLM_TEMPERATURE = 0.1  # 低温度で決定論的な出力
LLM_MAX_TOKENS = 200  # Qwen3 needs more tokens for reasoning mode
LLM_TIMEOUT = 15.0  # seconds


def load_dotenv_if_exists(env_path: Path | None = None) -> bool:
    """Load .env file into os.environ via python-dotenv.

    Does NOT override existing environment variables.
    Returns True if a .env file was found and loaded.
    """
    if env_path is None:
        env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return False

    loaded = load_dotenv(env_path, override=False)
    log.debug("dotenv_loaded", path=str(env_path), loaded=loaded)
    return loaded


def validate_environment() -> list[str]:
    """Check for recommended environment variables and return warnings.

    Returns a list of warning messages for missing optional vars.
    """
    warnings: list[str] = []

    if not os.environ.get("API_SECRET_KEY"):
        warnings.append(
            "API_SECRET_KEY not set — write endpoints are unprotected (dev mode)"
        )

    if not os.environ.get("ANILIST_ACCESS_TOKEN"):
        warnings.append(
            "ANILIST_ACCESS_TOKEN not set — anonymous rate limits apply (90 req/min)"
        )

    for w in warnings:
        log.warning("env_warning", message=w)

    return warnings

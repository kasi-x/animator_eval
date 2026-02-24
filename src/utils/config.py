"""プロジェクト共通のパス定義・定数."""

import os
from pathlib import Path

import structlog
from dotenv import load_dotenv

from src.utils.role_groups import ROLE_CATEGORY

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

# =============================================================================
# Commitment Multipliers — カテゴリ別の責任/関与度 (tunable)
# =============================================================================
# Each category's base weight reflecting its level of responsibility.
# Example: direction=3.0 means a director-class role carries 3x base commitment.
# Adjust these to change how much each department contributes to edge weights.
COMMITMENT_MULTIPLIERS: dict[str, float] = {
    "direction": 3.0,           # 監督・演出系 — 作品全体の責任
    "animation_supervision": 2.8,  # 作画監督系 — 作画品質の責任
    "animation": 2.0,           # アニメーター系 — 作画実務
    "design": 2.3,              # デザイン系 — ビジュアル設計
    "technical": 2.0,           # 技術系 — 撮影・CG・エフェクト
    "art": 1.3,                 # 美術系 — 背景美術
    "sound": 1.8,               # 音響系 — 音響・音楽
    "writing": 1.8,             # 脚本系 — 脚本・原作
    "production": 1.5,          # 制作系 — プロデューサー
    "other": 1.0,               # その他
}

# =============================================================================
# Role Rank — カテゴリ内での相対的な重要度 (0.0–1.0)
# =============================================================================
# Within each category, how important is this specific role relative to the top role?
# 1.0 = category lead, lower values = supporting roles within the category.
ROLE_RANK: dict[str, float] = {
    # Direction
    "director": 1.0,
    "episode_director": 0.83,
    "storyboard": 0.67,
    "series_composition": 0.67,
    # Animation Supervision
    "chief_animation_director": 1.0,
    "animation_director": 0.89,
    # Animation
    "key_animator": 1.0,
    "second_key_animator": 0.75,
    "in_between": 0.5,
    "layout": 0.75,
    # Design
    "character_designer": 1.0,
    "mechanical_designer": 0.78,
    "art_director": 0.87,
    "color_designer": 0.65,
    # Technical
    "effects": 0.75,
    "cgi_director": 1.0,
    "photography_director": 0.9,
    # Art
    "background_art": 1.0,
    # Sound
    "sound_director": 1.0,
    "music": 0.67,
    # Writing
    "screenplay": 1.0,
    "original_creator": 0.56,
    # Production
    "producer": 1.0,
    # Other
    "other": 1.0,
}

# Role → category mapping — derived from role_groups.ROLE_CATEGORY (single source of truth)
_ROLE_TO_CATEGORY: dict[str, str] = {role.value: cat for role, cat in ROLE_CATEGORY.items()}


def _compute_role_weights() -> dict[str, float]:
    """Compute ROLE_WEIGHTS = COMMITMENT_MULTIPLIERS[category] × ROLE_RANK[role]."""
    weights: dict[str, float] = {}
    for role, category in _ROLE_TO_CATEGORY.items():
        multiplier = COMMITMENT_MULTIPLIERS.get(category, 1.0)
        rank = ROLE_RANK.get(role, 1.0)
        weights[role] = round(multiplier * rank, 2)
    return weights


# 役職の重み（エッジ重み係数） — COMMITMENT_MULTIPLIERS × ROLE_RANK から動的に計算
ROLE_WEIGHTS: dict[str, float] = _compute_role_weights()

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

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
# D01 rationale: These weights reflect role-level responsibility in anime production.
# The hierarchy (direction 3.0 > supervision 2.8 > animation 2.0 > ...) follows
# the anime industry credit ordering convention (監督 > 作画監督 > 原画 > 動画).
# Sensitivity analysis shows rank-order stability: top-100 persons change <5% when
# all multipliers are perturbed ±20%, because AKM person_fe dominates IV and
# multipliers only affect graph edge weights (one of many IV inputs).
# If better calibration data becomes available (e.g., salary surveys), update here.
COMMITMENT_MULTIPLIERS: dict[str, float] = {
    "direction": 3.0,  # 監督・演出系 — 作品全体の責任
    "animation_supervision": 2.8,  # 作画監督系 — 作画品質の責任
    "animation": 2.0,  # アニメーター系 — 作画実務
    "design": 2.3,  # デザイン系 — ビジュアル設計
    "technical": 2.0,  # 技術系 — 撮影・CG・エフェクト
    "art": 1.3,  # 美術系 — 背景美術
    "sound": 1.8,  # 音響系 — 音響・音楽
    "writing": 1.8,  # 脚本系 — 脚本・原作
    "production": 1.5,  # 制作系 — プロデューサー
    "production_management": 1.2,  # 制作進行・デスク系 — 現場管理
    "finishing": 1.2,  # 仕上げ系 — 仕上・検査
    "editing": 1.5,  # 編集系 — 編集・ポスプロ
    "settings": 1.5,  # 設定系 — 設定・プロップ
    "non_production": 0.5,  # 非制作部門
}

# =============================================================================
# Role Rank — カテゴリ内での相対的な重要度 (0.0–1.0)
# =============================================================================
# 1.0 = category lead, lower values = supporting roles within the category.
# 統合後は1カテゴリ=1ロールが多いため、大半が1.0。
ROLE_RANK: dict[str, float] = {
    "director": 1.0,
    "episode_director": 0.83,
    "animation_director": 1.0,
    "key_animator": 1.0,
    "second_key_animator": 0.7,
    "in_between": 0.5,
    "layout": 0.75,
    "character_designer": 1.0,
    "photography_director": 1.0,  # 撮影+エフェクト
    "cgi_director": 1.0,
    "background_art": 1.0,  # 美術+背景
    "sound_director": 1.0,
    "music": 0.67,
    "screenplay": 1.0,  # +シリーズ構成
    "original_creator": 0.56,
    "producer": 1.0,
    "production_manager": 1.0,
    "finishing": 1.0,  # +色彩設計
    "editing": 1.0,
    "settings": 1.0,
    "voice_actor": 1.0,
    "special": 1.0,
}

# Role → category mapping — derived from role_groups.ROLE_CATEGORY (single source of truth)
_ROLE_TO_CATEGORY: dict[str, str] = {
    role.value: cat for role, cat in ROLE_CATEGORY.items()
}


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

# =============================================================================
# BiRank parameters
# =============================================================================
BIRANK_ALPHA = 0.85
BIRANK_BETA = 0.85
BIRANK_MAX_ITER = 100
BIRANK_TOL = 1e-6

# =============================================================================
# AKM parameters
# =============================================================================
AKM_MAX_ITER = 50  # iterative demeaning convergence
AKM_TOL = 1e-8
AKM_MIN_MOVER_FRACTION = 0.10  # warn if fewer movers

# =============================================================================
# Dormancy parameters
# =============================================================================
# 0.5 gives 50% weight at 1 year past grace period — a suitable half-life
# for a seasonal industry where 1-2 year gaps between projects are common.
DORMANCY_DECAY_RATE = 0.5
DORMANCY_GRACE_PERIOD = 2.0  # years

# =============================================================================
# IV weight optimization
# =============================================================================
IV_CV_FOLDS = 5
IV_CV_SEED = 42

# Duration-based work importance weighting
# 30分アニメを基準 (1.0x), ミニアニメは減衰, 映画は増幅
DURATION_BASELINE_MINUTES = 30  # 30分 = 1.0x multiplier
DURATION_MAX_MULTIPLIER = 2.0  # 映画等の上限キャップ

# BiRank edge weight: role hierarchy compression
# Controls how much "being a director" matters vs "what you directed"
# 0.0 = role title ignored (pure content weight)
# 0.5 = sqrt compression (director 3.0 → 1.73, animator 1.0 → 1.0)
# 1.0 = full role hierarchy (original behavior)
BIRANK_ROLE_DAMPING = 0.5

# 正規化方式: "minmax" | "percentile" | "zscore"
NORMALIZATION_METHOD = "minmax"

# AI-assisted entity resolution
# Ollama (OpenAI-compatible API)
LLM_BASE_URL = "http://localhost:11434/v1"  # Ollama default
LLM_MODEL_NAME = "qwen3:32b"  # 32b for higher accuracy on name classification
LLM_TEMPERATURE = 0.1  # 低温度で決定論的な出力
LLM_MAX_TOKENS = 500  # Batch prompts need more tokens
LLM_TIMEOUT = 30.0  # seconds (batch prompts take longer)
LLM_BATCH_SIZE = 30  # items per batch prompt


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

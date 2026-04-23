"""プロジェクト共通のパス定義・定数.

すべての設定は環境変数でオーバーライド可能。
優先順位: 環境変数 > .env ファイル > コード内デフォルト値

.env ファイルはプロジェクトルートに置くか、ANIMETOR_ENV_FILE 環境変数で指定する。
モジュールインポート時に自動ロードされるため、明示的な呼び出しは不要。
"""

from __future__ import annotations

import os
from pathlib import Path

import structlog
from dotenv import load_dotenv

from src.utils.role_groups import ROLE_CATEGORY

log = structlog.get_logger()

# =============================================================================
# .env の自動ロード (import 時に実行)
# =============================================================================
# 環境変数はコード内デフォルト値より優先されるが、
# シェルで既にセットされた変数は上書きしない (override=False)。
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
_env_file = Path(os.environ.get("ANIMETOR_ENV_FILE", str(_ROOT_DIR / ".env")))
if _env_file.exists():
    load_dotenv(_env_file, override=False)
    log.debug("dotenv_loaded", path=str(_env_file))

# =============================================================================
# パス定義  (ANIMETOR_*_DIR / ANIMETOR_DB_PATH でオーバーライド可)
# =============================================================================
ROOT_DIR: Path = _ROOT_DIR

DATA_DIR: Path = Path(os.environ.get("ANIMETOR_DATA_DIR", str(ROOT_DIR / "data")))
RAW_DIR: Path = DATA_DIR / "raw"
INTERIM_DIR: Path = DATA_DIR / "interim"
PROCESSED_DIR: Path = DATA_DIR / "processed"

RESULT_DIR: Path = Path(os.environ.get("ANIMETOR_RESULT_DIR", str(ROOT_DIR / "result")))
DB_DIR: Path = RESULT_DIR / "db"
JSON_DIR: Path = Path(os.environ.get("ANIMETOR_JSON_DIR", str(RESULT_DIR / "json")))
REPORTS_DIR: Path = Path(
    os.environ.get("ANIMETOR_REPORTS_DIR", str(RESULT_DIR / "reports"))
)
HTML_DIR: Path = Path(os.environ.get("ANIMETOR_HTML_DIR", str(RESULT_DIR / "html")))
NOTEBOOKS_DIR: Path = RESULT_DIR / "notebooks"

# DB ファイルパス (database.py はここを参照する)
DB_PATH: Path = Path(
    os.environ.get("ANIMETOR_DB_PATH", str(DB_DIR / "animetor_eval.db"))
)

# DuckDB GOLD layer — Phase B: pipeline output (person_scores, score_history, etc.)
# Write: pipeline (once per run). Read: API + report generators.
GOLD_DB_PATH: Path = Path(
    os.environ.get("ANIMETOR_GOLD_DB_PATH", str(RESULT_DIR / "gold.duckdb"))
)

# =============================================================================
# PageRank パラメータ
# =============================================================================
DAMPING_FACTOR: float = float(os.environ.get("ANIMETOR_PAGERANK_DAMPING", "0.85"))
MAX_ITERATIONS: int = int(os.environ.get("ANIMETOR_PAGERANK_MAX_ITER", "100"))
CONVERGENCE_THRESHOLD: float = float(os.environ.get("ANIMETOR_PAGERANK_TOL", "1e-6"))

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
ROLE_RANK: dict[str, float] = {
    "director": 1.0,
    "episode_director": 0.83,
    "animation_director": 1.0,
    "key_animator": 1.0,
    "second_key_animator": 0.7,
    "in_between": 0.5,
    "layout": 0.75,
    "character_designer": 1.0,
    "photography_director": 1.0,
    "cgi_director": 1.0,
    "background_art": 1.0,
    "sound_director": 1.0,
    "music": 0.67,
    "screenplay": 1.0,
    "original_creator": 0.56,
    "producer": 1.0,
    "production_manager": 1.0,
    "finishing": 1.0,
    "editing": 1.0,
    "settings": 1.0,
    "voice_actor": 1.0,
    "other": 1.0,
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
# BiRank parameters  (ANIMETOR_BIRANK_* でオーバーライド可)
# =============================================================================
BIRANK_ALPHA: float = float(os.environ.get("ANIMETOR_BIRANK_ALPHA", "0.85"))
BIRANK_BETA: float = float(os.environ.get("ANIMETOR_BIRANK_BETA", "0.85"))
BIRANK_MAX_ITER: int = int(os.environ.get("ANIMETOR_BIRANK_MAX_ITER", "100"))
BIRANK_TOL: float = float(os.environ.get("ANIMETOR_BIRANK_TOL", "1e-6"))

# role hierarchy compression in edge weight
# 0.0 = role title ignored, 0.5 = sqrt compression, 1.0 = full hierarchy
BIRANK_ROLE_DAMPING: float = float(
    os.environ.get("ANIMETOR_BIRANK_ROLE_DAMPING", "0.5")
)

# =============================================================================
# AKM parameters  (ANIMETOR_AKM_* でオーバーライド可)
# =============================================================================
AKM_MAX_ITER: int = int(os.environ.get("ANIMETOR_AKM_MAX_ITER", "50"))
AKM_TOL: float = float(os.environ.get("ANIMETOR_AKM_TOL", "1e-8"))
AKM_MIN_MOVER_FRACTION: float = float(
    os.environ.get("ANIMETOR_AKM_MIN_MOVER_FRACTION", "0.10")
)

# =============================================================================
# Dormancy parameters  (ANIMETOR_DORMANCY_* でオーバーライド可)
# =============================================================================
# 0.5 gives 50% weight at 1 year past grace period — a suitable half-life
# for a seasonal industry where 1-2 year gaps between projects are common.
DORMANCY_DECAY_RATE: float = float(
    os.environ.get("ANIMETOR_DORMANCY_DECAY_RATE", "0.5")
)
DORMANCY_GRACE_PERIOD: float = float(
    os.environ.get("ANIMETOR_DORMANCY_GRACE_PERIOD", "2.0")
)

# =============================================================================
# IV (Integrated Value) parameters
# =============================================================================
IV_CV_FOLDS: int = int(os.environ.get("ANIMETOR_IV_CV_FOLDS", "5"))
IV_CV_SEED: int = int(os.environ.get("ANIMETOR_IV_CV_SEED", "42"))

# =============================================================================
# Duration-based work importance weighting
# =============================================================================
# 30分アニメを基準 (1.0x), ミニアニメは減衰, 映画は増幅
DURATION_BASELINE_MINUTES: int = int(
    os.environ.get("ANIMETOR_DURATION_BASELINE_MINUTES", "30")
)
DURATION_MAX_MULTIPLIER: float = float(
    os.environ.get("ANIMETOR_DURATION_MAX_MULTIPLIER", "2.0")
)

# =============================================================================
# Normalization
# =============================================================================
# "minmax" | "percentile" | "zscore"
NORMALIZATION_METHOD: str = os.environ.get("ANIMETOR_NORMALIZATION_METHOD", "minmax")

# =============================================================================
# Scraping  (ANIMETOR_SCRAPE_* でオーバーライド可)
# =============================================================================
SCRAPE_CHECKPOINT_INTERVAL: int = int(
    os.environ.get("ANIMETOR_SCRAPE_CHECKPOINT_INTERVAL", "3")
)
SCRAPE_DELAY_SECONDS: float = float(os.environ.get("ANIMETOR_SCRAPE_DELAY", "1.0"))
SCRAPE_MAX_RETRIES: int = int(os.environ.get("ANIMETOR_SCRAPE_MAX_RETRIES", "3"))

# =============================================================================
# LLM / AI-assisted entity resolution  (ANIMETOR_LLM_* でオーバーライド可)
# =============================================================================
LLM_BASE_URL: str = os.environ.get("ANIMETOR_LLM_BASE_URL", "http://localhost:11434/v1")
LLM_MODEL_NAME: str = os.environ.get("ANIMETOR_LLM_MODEL", "qwen3:32b")
LLM_TEMPERATURE: float = float(os.environ.get("ANIMETOR_LLM_TEMPERATURE", "0.1"))
LLM_MAX_TOKENS: int = int(os.environ.get("ANIMETOR_LLM_MAX_TOKENS", "500"))
LLM_TIMEOUT: float = float(os.environ.get("ANIMETOR_LLM_TIMEOUT", "30.0"))
LLM_BATCH_SIZE: int = int(os.environ.get("ANIMETOR_LLM_BATCH_SIZE", "30"))


# =============================================================================
# Helpers
# =============================================================================


def load_dotenv_if_exists(env_path: Path | None = None) -> bool:
    """Load an additional .env file (e.g. per-environment override).

    The default .env at ROOT_DIR is already loaded at module import time.
    Call this only when you need to load a second .env (e.g. .env.test).
    Does NOT override already-set environment variables.
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
    """Check for recommended environment variables and return warning messages."""
    warnings: list[str] = []

    if not os.environ.get("API_SECRET_KEY"):
        warnings.append(
            "API_SECRET_KEY not set — write endpoints are unprotected (dev mode)"
        )
    if not os.environ.get("ANILIST_ACCESS_TOKEN"):
        warnings.append(
            "ANILIST_ACCESS_TOKEN not set — anonymous rate limits apply (30 req/min)"
        )

    for w in warnings:
        log.warning("env_warning", message=w)

    return warnings

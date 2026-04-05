"""FastAPI サーバー — スコア照会 API.

エンドポイント:
  GET /api/persons          — 全人物スコア一覧 (ページネーション対応)
  GET /api/persons/search   — 人物検索
  GET /api/persons/{id}     — 人物プロフィール
  GET /api/persons/{id}/profile — 個人貢献プロファイル（二層モデル）
  GET /api/persons/{id}/similar — 類似人物
  GET /api/ranking          — ランキング (フィルタ対応)
  GET /api/anime            — アニメ統計一覧
  GET /api/anime/{id}       — アニメ詳細
  GET /api/summary          — パイプラインサマリー
  GET /api/health           — ヘルスチェック
"""

import os

import structlog
import uvicorn
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pathlib import Path
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.responses import JSONResponse

from src.analysis.explain import explain_individual_profile
from src.analysis.similarity import find_similar_persons
from src.api_validators import AnimeId, PersonId, validate_query_string
from src.database import (
    db_connection,
    get_data_sources,
    get_db_stats,
    get_score_history,
    search_persons,
)
from src.utils.config import JSON_DIR
from src.utils.json_io import (
    load_anime_statistics_from_json,
    load_bridge_analysis_from_json,
    load_career_milestones_from_json,
    load_collaboration_pairs_from_json,
    load_cross_validation_results_from_json,
    load_decade_analysis_from_json,
    load_genre_affinity_from_json,
    load_growth_trends_from_json,
    load_individual_profiles_from_json,
    load_influence_tree_from_json,
    load_mentorship_relationships_from_json,
    load_network_evolution_from_json,
    load_outlier_analysis_from_json,
    load_person_scores_from_json,
    load_person_tags_from_json,
    load_pipeline_summary_from_json,
    load_productivity_metrics_from_json,
    load_role_flow_from_json,
    load_role_transitions_from_json,
    load_seasonal_trends_from_json,
    load_studio_analysis_from_json,
    load_studio_bias_from_json,
    load_team_patterns_from_json,
    load_time_series_from_json,
)

logger = structlog.get_logger()

app = FastAPI(
    title="Animetor Eval API",
    description="アニメ業界人物評価 API — 個人の貢献を可視化し、適正な報酬と業界の健全化を支援する",
    version="0.1.0",
)

# --- CORS ---
_cors_env = os.environ.get(
    "CORS_ORIGINS", "http://localhost:3000,http://localhost:8000"
)
CORS_ORIGINS = [origin.strip() for origin in _cors_env.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Cache-Control Headers ---
_CACHE_MAX_AGE = int(os.environ.get("API_CACHE_MAX_AGE", "300"))  # 5 minutes default


@app.middleware("http")
async def add_cache_control_headers(request: Request, call_next):
    """Add Cache-Control headers to GET API responses."""
    response = await call_next(request)
    if (
        request.method == "GET"
        and request.url.path.startswith("/api/")
        and response.status_code == 200
    ):
        response.headers["Cache-Control"] = f"public, max-age={_CACHE_MAX_AGE}"
    return response


# --- Rate Limiting (slowapi) ---
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Try again later."},
    )


# --- API Key Auth (for write endpoints) ---


def verify_api_key(request: Request) -> None:
    """Verify API key for protected endpoints.

    If API_SECRET_KEY is not configured, allow all requests (dev mode).
    """
    secret = os.environ.get("API_SECRET_KEY")
    if not secret:
        # Dev mode — no key required
        return
    provided = request.headers.get("X-API-Key", "")
    if provided != secret:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")


# Mount static files for WebSocket demo
STATIC_DIR = Path(__file__).parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Mount report files
REPORTS_DIR = Path(__file__).parent.parent / "result" / "reports"
if REPORTS_DIR.exists():
    app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR), html=True), name="reports")


# --- Response Models ---


class HealthResponse(BaseModel):
    status: str
    db_exists: bool
    scores_exist: bool


class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    per_page: int
    pages: int


# --- Endpoints ---


@app.get("/api/health", response_model=HealthResponse)
def health():
    """ヘルスチェック."""
    from src.utils.config import DB_DIR

    db_path = DB_DIR / "animetor_eval.db"
    scores_path = JSON_DIR / "scores.json"
    return HealthResponse(
        status="ok",
        db_exists=db_path.exists(),
        scores_exist=scores_path.exists(),
    )


@app.get("/api/i18n/{language}")
def get_translations(language: str):
    """Get translations for specified language.

    Args:
        language: Language code ("en" or "ja")

    Returns:
        Translation dictionary for the specified language
    """
    from src.i18n import get_i18n

    i18n = get_i18n()

    if language not in i18n.supported_languages:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language: {language}. Supported: {', '.join(i18n.supported_languages)}",
        )

    translations = i18n.get_all_translations(language=language)
    return {
        "language": language,
        "translations": translations,
    }


@app.get("/api/summary")
def summary():
    """パイプラインサマリー."""
    data = load_pipeline_summary_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Summary not found. Run pipeline first."
        )
    return data


_PERSON_SELECT_SQL = """
    SELECT s.person_id,
           p.name_ja, p.name_en, p.image_medium,
           s.iv_score, s.birank, s.patronage, s.person_fe,
           s.awcc, s.dormancy, s.studio_fe_exposure,
           MIN(c.credit_year) AS first_year,
           MAX(c.credit_year) AS latest_year,
           COUNT(DISTINCT c.anime_id) AS total_works,
           (SELECT role FROM credits
            WHERE person_id = s.person_id
            GROUP BY role ORDER BY COUNT(*) DESC LIMIT 1) AS primary_role
    FROM scores s
    JOIN persons p ON s.person_id = p.id
    LEFT JOIN credits c ON s.person_id = c.person_id
    WHERE s.iv_score IS NOT NULL {extra_where}
    GROUP BY s.person_id
"""


def _row_to_person(r) -> dict:
    return {
        "person_id": r["person_id"],
        "name": r["name_ja"] or r["name_en"],
        "name_ja": r["name_ja"],
        "name_en": r["name_en"],
        "image_medium": r["image_medium"],
        "iv_score": r["iv_score"],
        "birank": r["birank"],
        "patronage": r["patronage"],
        "person_fe": r["person_fe"],
        "awcc": r["awcc"],
        "dormancy": r["dormancy"],
        "studio_fe_exposure": r["studio_fe_exposure"],
        "primary_role": r["primary_role"],
        "career": {
            "first_year": r["first_year"],
            "latest_year": r["latest_year"],
            "total_works": r["total_works"],
        },
    }


@app.get("/api/persons")
def list_persons(
    page: int = Query(1, ge=1, description="ページ番号"),
    per_page: int = Query(50, ge=1, le=200, description="1ページあたりの件数"),
    sort: str = Query(
        "iv_score", description="ソート軸 (iv_score, person_fe, birank, patronage)"
    ),
):
    """全人物スコア一覧（ページネーション対応）."""
    valid_sorts = {"iv_score", "person_fe", "birank", "patronage", "dormancy", "awcc"}
    if sort not in valid_sorts:
        raise HTTPException(
            status_code=400, detail=f"Invalid sort: {sort}. Use: {valid_sorts}"
        )

    import sqlite3 as _sqlite3
    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        total = conn.execute(
            "SELECT COUNT(DISTINCT s.person_id) FROM scores s WHERE s.iv_score IS NOT NULL"
        ).fetchone()[0]
        pages = (total + per_page - 1) // per_page
        offset = (page - 1) * per_page
        sql = _PERSON_SELECT_SQL.format(extra_where="")
        rows = conn.execute(
            f"{sql} ORDER BY s.{sort} DESC LIMIT ? OFFSET ?",
            [per_page, offset],
        ).fetchall()

    items = [_row_to_person(r) for r in rows]
    return PaginatedResponse(items=items, total=total, page=page, per_page=per_page, pages=pages)


@app.get("/api/persons/search")
def search(
    q: str = Query(..., min_length=1, max_length=500, description="検索クエリ"),
    limit: int = Query(20, ge=1, le=100, description="最大件数"),
):
    """人物検索（名前・IDの部分一致）."""
    # Validate query string for SQL injection patterns
    q = validate_query_string(q)
    with db_connection() as conn:
        results = search_persons(conn, q, limit=limit)
    return {"query": q, "count": len(results), "results": results}


@app.get("/api/persons/{person_id}")
def get_person(person_id: PersonId):
    """人物プロフィール（スコア + ブレークダウン）."""
    import sqlite3 as _sqlite3
    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        sql = _PERSON_SELECT_SQL.format(extra_where="AND s.person_id = ?")
        row = conn.execute(sql, [person_id]).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
    return _row_to_person(row)


@app.get("/api/persons/{person_id}/similar")
def get_similar(
    person_id: PersonId,
    top_n: int = Query(10, ge=1, le=50, description="類似人物の数"),
):
    """類似人物検索（コサイン類似度）."""
    import sqlite3 as _sqlite3
    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        # 対象人物と同じ主役職のスコア上位2000人のみで類似計算
        target = conn.execute(
            "SELECT person_fe, birank, patronage, awcc, dormancy,"
            " (SELECT role FROM credits WHERE person_id=s.person_id"
            "  GROUP BY role ORDER BY COUNT(*) DESC LIMIT 1) AS primary_role"
            " FROM scores s WHERE s.person_id = ?",
            [person_id],
        ).fetchone()
        if target is None:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        sql = _PERSON_SELECT_SQL.format(extra_where="")
        rows = conn.execute(
            f"{sql} ORDER BY s.iv_score DESC LIMIT 2000"
        ).fetchall()

    scores = [_row_to_person(r) for r in rows]
    similar = find_similar_persons(person_id, scores, top_n=top_n)
    return {"person_id": person_id, "similar": similar or []}


@app.get("/api/persons/{person_id}/history")
def get_person_history(
    person_id: PersonId,
    limit: int = Query(50, ge=1, le=200, description="履歴件数"),
):
    """人物のスコア履歴."""
    with db_connection() as conn:
        history = get_score_history(conn, person_id, limit=limit)
    if not history:
        raise HTTPException(status_code=404, detail=f"No history for {person_id}")
    return {"person_id": person_id, "history": history}


@app.get("/api/ranking")
def ranking(
    role: str | None = Query(
        None, description="役職フィルタ (director, animator, etc.)"
    ),
    year_from: int | None = Query(None, description="開始年"),
    year_to: int | None = Query(None, description="終了年"),
    sort: str = Query("iv_score", description="ソート軸"),
    limit: int = Query(50, ge=1, le=500, description="件数"),
):
    """ランキング（フィルタ対応）— SQLiteから直接クエリ."""
    valid_sorts = {"iv_score", "person_fe", "birank", "patronage", "dormancy", "awcc"}
    if sort not in valid_sorts:
        raise HTTPException(status_code=400, detail=f"Invalid sort: {sort}")

    with db_connection() as conn:
        conn.row_factory = __import__("sqlite3").Row
        conditions = ["s.iv_score IS NOT NULL"]
        params: list = []

        if role:
            conditions.append(
                "EXISTS (SELECT 1 FROM credits cr WHERE cr.person_id = s.person_id"
                " AND cr.role = ? GROUP BY cr.role"
                " HAVING COUNT(*) = (SELECT MAX(cnt) FROM"
                " (SELECT COUNT(*) AS cnt FROM credits WHERE person_id = s.person_id GROUP BY role)))"
            )
            params.append(role)

        if year_from is not None:
            conditions.append(
                "EXISTS (SELECT 1 FROM credits cr WHERE cr.person_id = s.person_id"
                " AND cr.credit_year >= ?)"
            )
            params.append(year_from)

        if year_to is not None:
            conditions.append(
                "EXISTS (SELECT 1 FROM credits cr WHERE cr.person_id = s.person_id"
                " AND cr.credit_year <= ?)"
            )
            params.append(year_to)

        where = " AND ".join(conditions)

        count_sql = f"SELECT COUNT(DISTINCT s.person_id) FROM scores s WHERE {where}"
        total = conn.execute(count_sql, params).fetchone()[0]

        sql = f"""
            SELECT s.person_id,
                   p.name_ja, p.name_en,
                   s.iv_score, s.birank, s.patronage, s.person_fe,
                   s.awcc, s.dormancy,
                   MIN(c.credit_year) AS first_year,
                   MAX(c.credit_year) AS latest_year,
                   (SELECT role FROM credits
                    WHERE person_id = s.person_id
                    GROUP BY role ORDER BY COUNT(*) DESC LIMIT 1) AS primary_role
            FROM scores s
            JOIN persons p ON s.person_id = p.id
            LEFT JOIN credits c ON s.person_id = c.person_id
            WHERE {where}
            GROUP BY s.person_id
            ORDER BY s.{sort} DESC
            LIMIT ?
        """
        rows = conn.execute(sql, params + [limit]).fetchall()

    items = [
        {
            "person_id": r["person_id"],
            "name": r["name_ja"] or r["name_en"],
            "name_ja": r["name_ja"],
            "name_en": r["name_en"],
            "iv_score": r["iv_score"],
            "birank": r["birank"],
            "patronage": r["patronage"],
            "person_fe": r["person_fe"],
            "awcc": r["awcc"],
            "dormancy": r["dormancy"],
            "primary_role": r["primary_role"],
            "career": {"first_year": r["first_year"], "latest_year": r["latest_year"]},
        }
        for r in rows
    ]

    return {
        "items": items,
        "total": total,
        "sort": sort,
        "filters": {"role": role, "year_from": year_from, "year_to": year_to},
    }


@app.get("/api/anime")
def list_anime(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort: str = Query(
        "credit_count", description="ソート (credit_count, avg_person_score, year)"
    ),
):
    """アニメ統計一覧."""
    stats = load_anime_statistics_from_json()
    if not stats:
        return PaginatedResponse(
            items=[], total=0, page=page, per_page=per_page, pages=0
        )

    items = [{"anime_id": k, **v} for k, v in stats.items()]

    sort_key_map = {
        "credit_count": lambda x: x.get("credit_count", 0),
        "avg_person_score": lambda x: x.get("avg_person_score", 0),
        "year": lambda x: x.get("year") or 0,
    }
    key_fn = sort_key_map.get(sort, sort_key_map["credit_count"])
    items.sort(key=key_fn, reverse=True)

    total = len(items)
    pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page

    return PaginatedResponse(
        items=items[start : start + per_page],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/api/anime/{anime_id}")
def get_anime(anime_id: AnimeId):
    """アニメ詳細統計."""
    stats = load_anime_statistics_from_json()
    if anime_id not in stats:
        raise HTTPException(status_code=404, detail=f"Anime {anime_id} not found")
    return {"anime_id": anime_id, **stats[anime_id]}


@app.get("/api/transitions")
def transitions():
    """役職遷移分析."""
    data = load_role_transitions_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Transitions not found. Run pipeline first."
        )
    return data


@app.get("/api/crossval")
def crossval():
    """スコアクロスバリデーション結果."""
    data = load_cross_validation_results_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Cross-validation not found. Run pipeline first."
        )
    return data


@app.get("/api/influence")
def influence():
    """影響ツリー（メンター・メンティー関係）."""
    data = load_influence_tree_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Influence data not found. Run pipeline first."
        )
    return data


@app.get("/api/studios")
def studios():
    """スタジオ分析."""
    data = load_studio_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Studio data not found. Run pipeline first."
        )
    return data


@app.get("/api/seasonal")
def seasonal():
    """シーズントレンド."""
    data = load_seasonal_trends_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Seasonal data not found. Run pipeline first."
        )
    return data


@app.get("/api/collaborations")
def collaborations(
    limit: int = Query(50, ge=1, le=500, description="件数"),
    person_id: str | None = Query(None, description="人物IDでフィルタ"),
):
    """コラボレーション強度ペア."""
    data = load_collaboration_pairs_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Collaboration data not found. Run pipeline first."
        )
    if person_id:
        data = [
            d
            for d in data
            if d.get("person_a") == person_id or d.get("person_b") == person_id
        ]
    return {"items": data[:limit], "total": len(data)}


@app.get("/api/outliers")
def outliers():
    """スコア外れ値検出結果."""
    data = load_outlier_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Outlier data not found. Run pipeline first."
        )
    return data


@app.get("/api/teams")
def teams():
    """チーム構成分析."""
    data = load_team_patterns_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Team data not found. Run pipeline first."
        )
    return data


@app.get("/api/growth")
def growth(
    trend: str | None = Query(
        None, description="トレンドフィルタ (rising/stable/declining/inactive)"
    ),
    limit: int = Query(50, ge=1, le=500),
):
    """成長トレンド."""
    data = load_growth_trends_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Growth data not found. Run pipeline first."
        )
    persons = data.get("persons", {})
    if trend:
        persons = {pid: d for pid, d in persons.items() if d.get("trend") == trend}
    items = [{"person_id": pid, **d} for pid, d in list(persons.items())[:limit]]
    return {
        "trend_summary": data.get("trend_summary", {}),
        "total": len(persons),
        "items": items,
    }


@app.get("/api/time-series")
def time_series():
    """年次時系列データ."""
    data = load_time_series_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Time series data not found. Run pipeline first."
        )
    return data


@app.get("/api/decades")
def decades():
    """年代別分析."""
    data = load_decade_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Decade data not found. Run pipeline first."
        )
    return data


@app.get("/api/tags")
def tags(
    tag: str | None = Query(None, description="タグでフィルタ"),
):
    """人物タグ."""
    data = load_person_tags_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Tag data not found. Run pipeline first."
        )

    if tag:
        filtered = {
            pid: t_list
            for pid, t_list in data.get("person_tags", {}).items()
            if tag in t_list
        }
        return {
            "tag_filter": tag,
            "count": len(filtered),
            "persons": filtered,
            "tag_summary": data.get("tag_summary", {}),
        }
    return data


@app.get("/api/role-flow")
def role_flow():
    """役職遷移フロー（Sankey diagram data）."""
    data = load_role_flow_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Role flow data not found. Run pipeline first."
        )
    return data


@app.get("/api/compare")
def compare_persons(
    ids: str = Query(..., description="比較対象の人物ID (カンマ区切り)"),
):
    """複数人物の比較マトリクス."""
    from src.analysis.comparison_matrix import build_comparison_matrix

    person_ids = [pid.strip() for pid in ids.split(",") if pid.strip()]
    if len(person_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 person IDs required")

    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    result = build_comparison_matrix(person_ids, scores)
    if not result["persons"]:
        raise HTTPException(
            status_code=404, detail="None of the specified persons found"
        )

    return result


@app.get("/api/data-quality")
def data_quality():
    """データ品質スコア."""
    from src.analysis.data_quality import compute_data_quality_score

    with db_connection() as conn:
        stats = get_db_stats(conn)

        total_credits = stats.get("credits_count", 0)
        total_persons = stats.get("persons_count", 0)
        total_anime = stats.get("anime_count", 0)

        credits_with_source = (
            conn.execute("SELECT COUNT(*) FROM credits WHERE source != ''").fetchone()[
                0
            ]
            if total_credits
            else 0
        )

        persons_with_score = (
            conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
            if total_persons
            else 0
        )

        anime_with_year = (
            conn.execute(
                "SELECT COUNT(*) FROM anime WHERE year IS NOT NULL"
            ).fetchone()[0]
            if total_anime
            else 0
        )

        anime_with_score = (
            conn.execute(
                "SELECT COUNT(*) FROM anime WHERE score IS NOT NULL"
            ).fetchone()[0]
            if total_anime
            else 0
        )

        source_count = conn.execute(
            "SELECT COUNT(DISTINCT source) FROM credits WHERE source != ''"
        ).fetchone()[0]

        latest_year_row = conn.execute(
            "SELECT MAX(year) FROM anime WHERE year IS NOT NULL"
        ).fetchone()
        latest_year = latest_year_row[0] if latest_year_row else None

    return compute_data_quality_score(
        stats={"latest_year": latest_year},
        credits_with_source=credits_with_source,
        total_credits=total_credits,
        persons_with_score=persons_with_score,
        total_persons=total_persons,
        anime_with_year=anime_with_year,
        total_anime=total_anime,
        anime_with_score=anime_with_score,
        source_count=source_count,
    )


@app.get("/api/persons/{person_id}/network")
def get_person_network(
    person_id: PersonId,
    hops: int = Query(1, ge=1, le=3, description="ネットワーク深度"),
):
    """人物のエゴグラフ（ローカルネットワーク）."""
    from src.analysis.ego_graph import extract_ego_graph
    from src.database import load_all_anime, load_all_credits

    with db_connection() as conn:
        credits = load_all_credits(conn)
        anime_list = load_all_anime(conn)

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = {r["person_id"]: r.get("iv_score", 0) for r in scores} if scores else None

    result = extract_ego_graph(
        person_id, credits, anime_map, hops=hops, person_scores=person_scores
    )
    if result["total_nodes"] == 0:
        raise HTTPException(
            status_code=404, detail=f"Person {person_id} not found in credits"
        )
    return result


@app.get("/api/recommend")
def recommend(
    team: str = Query(..., description="既存チームの人物ID (カンマ区切り)"),
    top_n: int = Query(10, ge=1, le=50),
):
    """チームへの人材推薦."""
    from src.analysis.recommendation import recommend_for_team
    from src.database import load_all_credits

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    with db_connection() as conn:
        credits = load_all_credits(conn)

    recs = recommend_for_team(team_ids, scores, credits, top_n=top_n)
    return {"team": team_ids, "recommendations": recs}


@app.get("/api/predict")
def predict(
    team: str = Query(..., description="チームの人物ID (カンマ区切り)"),
):
    """チーム構成からアニメスコアを予測."""
    from src.analysis.anime_prediction import predict_anime_score
    from src.database import load_all_anime, load_all_credits

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    with db_connection() as conn:
        credits = load_all_credits(conn)
        anime_list = load_all_anime(conn)

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = {r["person_id"]: r.get("iv_score", 0) for r in scores} if scores else None

    result = predict_anime_score(
        team_ids, credits, anime_map, person_scores=person_scores
    )
    return {"team": team_ids, **result}


@app.get("/api/bridges")
def bridges():
    """コミュニティ間ブリッジ人物."""
    data = load_bridge_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Bridge data not found. Run pipeline first."
        )
    return data


@app.get("/api/mentorships")
def mentorships():
    """推定メンターシップ関係."""
    data = load_mentorship_relationships_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Mentorship data not found. Run pipeline first."
        )
    return data


@app.get("/api/persons/{person_id}/milestones")
def get_person_milestones(person_id: PersonId):
    """人物のキャリアマイルストーン."""
    data = load_career_milestones_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Milestone data not found. Run pipeline first."
        )
    if person_id not in data:
        raise HTTPException(status_code=404, detail=f"No milestones for {person_id}")
    return {"person_id": person_id, "milestones": data[person_id]}


@app.get("/api/persons/{person_id}/profile")
def get_person_profile(person_id: PersonId):
    """個人貢献プロファイル（二層モデル: ネットワーク + 個人貢献）."""
    # Layer 1: Network Profile (scores.json)
    scores = load_person_scores_from_json()
    network_profile = None
    for entry in scores:
        if entry["person_id"] == person_id:
            network_profile = {
                "iv_score": entry.get("iv_score"),
                "person_fe": entry.get("person_fe"),
                "birank": entry.get("birank"),
                "patronage": entry.get("patronage"),
                "dormancy": entry.get("dormancy"),
                "awcc": entry.get("awcc"),
            }
            break
    if network_profile is None:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

    # Layer 2: Individual Contribution Profile (individual_profiles.json)
    individual_data = load_individual_profiles_from_json()
    profiles = individual_data.get("profiles", {})
    individual_profile = profiles.get(person_id)

    # Interpret individual profile metrics
    interpretation = None
    if individual_profile:
        interpretation = explain_individual_profile(individual_profile)

    return {
        "person_id": person_id,
        "network_profile": network_profile,
        "individual_profile": individual_profile,
        "interpretation": interpretation,
        "model_r_squared": individual_data.get("model_r_squared"),
    }


@app.get("/api/studio-disparity")
def studio_disparity():
    """スタジオ間待遇差分析 — 同Skill帯のスコア差を比較."""
    data = load_studio_bias_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Studio bias data not found. Run pipeline first."
        )
    disparity = data.get("studio_disparity", {})
    prestige = data.get("studio_prestige", {})
    return {
        "studio_disparity": disparity,
        "studio_prestige": prestige,
        "studios_analyzed": len(disparity),
    }


@app.get("/api/network-evolution")
def network_evolution():
    """ネットワーク進化の時系列データ."""
    data = load_network_evolution_from_json()
    if not data:
        raise HTTPException(
            status_code=404,
            detail="Network evolution data not found. Run pipeline first.",
        )
    return data


@app.get("/api/genre-affinity")
def genre_affinity(
    person_id: str | None = Query(None, description="人物IDでフィルタ"),
):
    """ジャンル親和性データ."""
    data = load_genre_affinity_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Genre affinity data not found. Run pipeline first."
        )
    if person_id:
        if person_id not in data:
            raise HTTPException(
                status_code=404, detail=f"No genre data for {person_id}"
            )
        return {"person_id": person_id, **data[person_id]}
    return data


@app.get("/api/productivity")
def productivity(
    limit: int = Query(50, ge=1, le=500),
):
    """生産性指標."""
    data = load_productivity_metrics_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Productivity data not found. Run pipeline first."
        )
    items = [{"person_id": pid, **d} for pid, d in list(data.items())[:limit]]
    return {"total": len(data), "items": items}


@app.get("/api/stats")
def db_stats():
    """DB統計情報."""
    with db_connection() as conn:
        stats = get_db_stats(conn)
        sources = get_data_sources(conn)
    return {"stats": stats, "data_sources": sources}


@app.get("/api/freshness")
def freshness():
    """Data source freshness check."""
    from src.monitoring import get_freshness_summary

    with db_connection() as conn:
        return get_freshness_summary(conn)


# --- Neo4j Query Endpoints ---
# These endpoints require a running Neo4j instance. If Neo4j is unavailable,
# they return 503 with a descriptive error message.


def _get_neo4j_reader():
    """Create a Neo4jReader instance, raising 503 if unavailable.

    Returns:
        Neo4jReader instance

    Raises:
        HTTPException: 503 if Neo4j is not available
    """
    try:
        from src.analysis.neo4j_direct import Neo4jReader

        return Neo4jReader()
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Neo4j driver not installed. Install with: pixi install",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Neo4j configuration error: {e}",
        )
    except Exception as e:
        logger.warning("neo4j_unavailable", error=str(e))
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available",
        )


@app.get("/api/neo4j/path")
def neo4j_shortest_path(
    from_id: str = Query(..., alias="from", description="Source person ID"),
    to_id: str = Query(..., alias="to", description="Target person ID"),
):
    """Find shortest collaboration path between two persons via Neo4j.

    Returns the shortest path through COLLABORATED_WITH relationships.
    """
    reader = _get_neo4j_reader()
    try:
        return reader.find_shortest_path(from_id, to_id)
    except Exception as e:
        logger.warning("neo4j_path_query_failed", error=str(e))
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available",
        )
    finally:
        reader.close()


@app.get("/api/neo4j/common")
def neo4j_common_collaborators(
    person_a: str = Query(..., description="First person ID"),
    person_b: str = Query(..., description="Second person ID"),
):
    """Find persons who collaborated with both person A and person B."""
    reader = _get_neo4j_reader()
    try:
        collaborators = reader.find_common_collaborators(person_a, person_b)
        return {
            "person_a": person_a,
            "person_b": person_b,
            "count": len(collaborators),
            "collaborators": collaborators,
        }
    except Exception as e:
        logger.warning("neo4j_common_query_failed", error=str(e))
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available",
        )
    finally:
        reader.close()


@app.get("/api/neo4j/neighborhood")
def neo4j_neighborhood(
    person_id: str = Query(..., description="Center person ID"),
    depth: int = Query(2, ge=1, le=5, description="Traversal depth"),
    limit: int = Query(50, ge=1, le=200, description="Max neighbor nodes"),
):
    """Get the collaboration neighborhood around a person."""
    reader = _get_neo4j_reader()
    try:
        return reader.get_neighborhood(person_id, depth=depth, limit=limit)
    except Exception as e:
        logger.warning("neo4j_neighborhood_query_failed", error=str(e))
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available",
        )
    finally:
        reader.close()


@app.get("/api/neo4j/stats")
def neo4j_stats():
    """Get high-level collaboration graph statistics from Neo4j."""
    reader = _get_neo4j_reader()
    try:
        return reader.get_collaboration_stats()
    except Exception as e:
        logger.warning("neo4j_stats_query_failed", error=str(e))
        raise HTTPException(
            status_code=503,
            detail="Neo4j not available",
        )
    finally:
        reader.close()


@app.websocket("/ws/pipeline")
async def websocket_pipeline_progress(websocket: WebSocket):
    """WebSocket endpoint for real-time pipeline progress updates.

    Connect to this endpoint to receive live updates during pipeline execution:
    - Pipeline start/complete events
    - Phase progress updates
    - Error notifications

    Example JavaScript client:
        const ws = new WebSocket('ws://localhost:8000/ws/pipeline');
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('Pipeline update:', data);
        };
    """
    from src.websocket_manager import get_websocket_manager

    manager = get_websocket_manager()
    await manager.connect(websocket)

    try:
        # Send initial connection confirmation
        await manager.send_personal_message(
            {
                "type": "connection_established",
                "message": "Connected to pipeline progress stream",
                "timestamp": __import__("datetime").datetime.now().isoformat(),
            },
            websocket,
        )

        # Keep connection alive and listen for client messages
        while True:
            # Wait for any client messages (ping/pong, etc.)
            data = await websocket.receive_text()

            # Echo back (simple ping/pong)
            if data == "ping":
                await manager.send_personal_message({"type": "pong"}, websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("websocket_client_disconnected")
    except Exception as e:
        logger.exception("websocket_error", error=str(e))
        manager.disconnect(websocket)


@app.post("/api/pipeline/run", dependencies=[Depends(verify_api_key)])
@limiter.limit("2/minute")
async def run_pipeline_async(
    request: Request,
    visualize: bool = Query(False, description="Generate visualizations"),
    dry_run: bool = Query(False, description="Dry run (validation only)"),
    incremental: bool = Query(
        False, description="Skip if no data changes since last run"
    ),
):
    """Run scoring pipeline asynchronously with WebSocket progress updates.

    This endpoint triggers the pipeline in a background task and returns immediately.
    Connect to /ws/pipeline WebSocket to receive real-time progress updates.

    Args:
        visualize: Generate matplotlib visualizations
        dry_run: Run validation only, don't compute scores
        incremental: Skip pipeline if no credit changes since last run

    Returns:
        Job started confirmation with job ID
    """
    import asyncio
    from datetime import datetime

    job_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Run pipeline in background task
    async def run_pipeline_task():
        """Background task to run pipeline with WebSocket updates."""
        from src.pipeline import run_scoring_pipeline

        try:
            logger.info("pipeline_task_started", job_id=job_id)
            # Note: run_scoring_pipeline is sync, so run in thread pool
            await asyncio.to_thread(
                run_scoring_pipeline,
                visualize,
                dry_run,
                incremental=incremental,
            )
            logger.info("pipeline_task_complete", job_id=job_id)

        except Exception as e:
            logger.exception("pipeline_task_failed", job_id=job_id, error=str(e))

    # Create background task
    asyncio.create_task(run_pipeline_task())

    return {
        "status": "started",
        "job_id": job_id,
        "message": "Pipeline started in background. Connect to /ws/pipeline for progress updates.",
        "websocket_url": "ws://localhost:8000/ws/pipeline",
    }


def main() -> None:
    """API サーバーを起動する."""
    from src.log import setup_logging

    setup_logging()
    logger.info("starting_api_server", host="0.0.0.0", port=8000)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()

"""FastAPI server — score query API.

Endpoints:
  GET /api/ranking          — ranking (with filters)
  GET /api/anime            — anime statistics list
  GET /api/anime/{id}       — anime detail
  GET /api/summary          — pipeline summary
  GET /api/health           — health check

Person endpoints (mounted from ``src.routers.persons``):
  GET /api/persons, /api/persons/search, /api/persons/{id}
  GET /api/persons/{id}/similar, /api/persons/{id}/history
  GET /api/persons/{id}/network, /api/persons/{id}/milestones
  GET /api/persons/{id}/profile

i18n endpoint (mounted from ``src.routers.i18n``):
  GET /api/i18n/{language}

Report endpoints (mounted from ``src.routers.reports``):
  POST /api/briefs/generate, GET /api/briefs/status, /api/briefs/{id}/...
  GET /api/appendix, POST /api/appendix/regenerate, WS /ws/regenerate
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

from src.analysis.io.mart_writer import GoldReader
from src.analysis.io.conformed_reader import (
    DEFAULT_SILVER_PATH,
    conformed_connect,
    silver_available,
    silver_db_stats,
    load_all_credits as silver_load_all_credits,
    load_all_anime as silver_load_all_anime,
)
from src.routers.i18n import router as i18n_router
from src.routers.persons import router as persons_router
from src.routers.reports import router as reports_router
from src.routers.validators import AnimeId
from src.utils.config import JSON_DIR, REPORTS_DIR
from src.utils.json_io import (
    load_anime_statistics_from_json,
    load_bridge_analysis_from_json,
    load_collaboration_pairs_from_json,
    load_cross_validation_results_from_json,
    load_decade_analysis_from_json,
    load_genre_affinity_from_json,
    load_growth_trends_from_json,
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
    description="Anime industry personnel evaluation API — making individual contributions visible to support fair compensation and a healthier industry",
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
_CACHE_MAX_AGE = int(os.environ.get("API_CACHE_MAX_AGE", "300"))


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


# --- Sub-routers ---
app.include_router(reports_router)
app.include_router(persons_router)
app.include_router(i18n_router)


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

# Mount report files (path from config, overridable via ANIMETOR_REPORTS_DIR)
if REPORTS_DIR.exists():
    app.mount(
        "/reports", StaticFiles(directory=str(REPORTS_DIR), html=True), name="reports"
    )


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
    """Health check."""
    from src.utils.config import DB_PATH

    scores_path = JSON_DIR / "scores.json"
    return HealthResponse(
        status="ok",
        db_exists=DB_PATH.exists(),
        scores_exist=scores_path.exists(),
    )


@app.get("/api/summary")
def summary():
    """Pipeline summary."""
    data = load_pipeline_summary_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Summary not found. Run pipeline first."
        )
    return data


@app.get("/api/ranking")
def ranking(
    role: str | None = Query(
        None, description="Role filter (director, animator, etc.)"
    ),
    year_from: int | None = Query(None, description="Start year"),
    year_to: int | None = Query(None, description="End year"),
    sort: str = Query("iv_score", description="Sort axis"),
    limit: int = Query(50, ge=1, le=500, description="Number of results"),
):
    """Ranking (with filters) — DuckDB GOLD preferred, SQLite fallback."""
    valid_sorts = {"iv_score", "person_fe", "birank", "patronage", "dormancy", "awcc"}
    if sort not in valid_sorts:
        raise HTTPException(status_code=400, detail=f"Invalid sort: {sort}")

    def _build_conditions(pfx: str) -> tuple[list[str], list]:
        conds: list[str] = []
        p: list = []
        if role:
            conds.append(
                f"EXISTS (SELECT 1 FROM {pfx}credits cr WHERE cr.person_id = s.person_id"
                f" AND cr.role = ?)"
            )
            p.append(role)
        if year_from is not None:
            conds.append(
                f"EXISTS (SELECT 1 FROM {pfx}credits cr WHERE cr.person_id = s.person_id"
                f" AND cr.credit_year >= ?)"
            )
            p.append(year_from)
        if year_to is not None:
            conds.append(
                f"EXISTS (SELECT 1 FROM {pfx}credits cr WHERE cr.person_id = s.person_id"
                f" AND cr.credit_year <= ?)"
            )
            p.append(year_to)
        return conds, p

    # --- Try DuckDB GOLD path ---
    gold = GoldReader()
    if gold.available():
        try:
            ddb_conds, ddb_params = _build_conditions("sl.")
            total, rows_raw = gold.ranking_query(
                DEFAULT_SILVER_PATH,
                conditions=ddb_conds,
                params=ddb_params,
                sort=sort,
                limit=limit,
            )
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
                    "career": {
                        "first_year": r["first_year"],
                        "latest_year": r["latest_year"],
                    },
                    "_source": "gold_duckdb",
                }
                for r in rows_raw
            ]
            return {
                "items": items,
                "total": total,
                "sort": sort,
                "filters": {"role": role, "year_from": year_from, "year_to": year_to},
            }
        except Exception as exc:
            logger.warning("ranking_gold_duckdb_failed", error=str(exc))
            raise HTTPException(status_code=503, detail="Ranking data not available. Run pipeline first.")
    raise HTTPException(status_code=503, detail="Ranking data not available. Run pipeline first.")


@app.get("/api/anime")
def list_anime(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort: str = Query(
        "credit_count", description="Sort key (credit_count, avg_person_score, year)"
    ),
):
    """Anime statistics list."""
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
    """Anime detail statistics."""
    stats = load_anime_statistics_from_json()
    if anime_id not in stats:
        raise HTTPException(status_code=404, detail=f"Anime {anime_id} not found")
    return {"anime_id": anime_id, **stats[anime_id]}


@app.get("/api/transitions")
def transitions():
    """Role transition analysis."""
    data = load_role_transitions_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Transitions not found. Run pipeline first."
        )
    return data


@app.get("/api/crossval")
def crossval():
    """Score cross-validation results."""
    data = load_cross_validation_results_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Cross-validation not found. Run pipeline first."
        )
    return data


@app.get("/api/influence")
def influence():
    """Influence tree (mentor-mentee relationships)."""
    data = load_influence_tree_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Influence data not found. Run pipeline first."
        )
    return data


@app.get("/api/studios")
def studios():
    """Studio analysis."""
    data = load_studio_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Studio data not found. Run pipeline first."
        )
    return data


@app.get("/api/seasonal")
def seasonal():
    """Seasonal trends."""
    data = load_seasonal_trends_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Seasonal data not found. Run pipeline first."
        )
    return data


@app.get("/api/collaborations")
def collaborations(
    limit: int = Query(50, ge=1, le=500, description="Number of results"),
    person_id: str | None = Query(None, description="人物IDでフィルタ"),
):
    """Collaboration strength pairs."""
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
    """Score outlier detection results."""
    data = load_outlier_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Outlier data not found. Run pipeline first."
        )
    return data


@app.get("/api/teams")
def teams():
    """Team composition analysis."""
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
    """Growth trends."""
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
    """Annual time-series data."""
    data = load_time_series_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Time series data not found. Run pipeline first."
        )
    return data


@app.get("/api/decades")
def decades():
    """Decade-level analysis."""
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
    """Person tags."""
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
    """Role transition flow (Sankey diagram data)."""
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
    """Multi-person comparison matrix."""
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
    """Data quality score."""
    from src.analysis.data_quality import compute_data_quality_score

    stats = silver_db_stats()

    total_credits = stats.get("credits_count", 0)
    total_persons = stats.get("persons_count", 0)
    total_anime = stats.get("anime_count", 0)

    credits_with_source = 0
    persons_with_score = 0
    anime_with_year = 0
    anime_with_score = 0
    source_count = 0
    latest_year = None

    if silver_available():
        with conformed_connect() as conn:
            if total_credits:
                credits_with_source = conn.execute(
                    "SELECT COUNT(*) FROM credits WHERE evidence_source != ''"
                ).fetchone()[0]
            if total_persons:
                try:
                    from src.analysis.io.mart_writer import gold_connect, DEFAULT_GOLD_DB_PATH
                    if DEFAULT_GOLD_DB_PATH.exists():
                        with gold_connect() as gc:
                            persons_with_score = gc.execute(
                                "SELECT COUNT(*) FROM person_scores"
                            ).fetchone()[0]
                except Exception:
                    pass
            if total_anime:
                anime_with_year = conn.execute(
                    "SELECT COUNT(*) FROM anime WHERE year IS NOT NULL"
                ).fetchone()[0]
                # src_anilist_anime is BRONZE — not in silver; default 0
                anime_with_score = 0
            source_count = conn.execute(
                "SELECT COUNT(DISTINCT evidence_source) FROM credits"
                " WHERE evidence_source != ''"
            ).fetchone()[0]
            row = conn.execute(
                "SELECT MAX(year) FROM anime WHERE year IS NOT NULL"
            ).fetchone()
            latest_year = row[0] if row else None

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


@app.get("/api/recommend")
def recommend(
    team: str = Query(..., description="既存チームの人物ID (カンマ区切り)"),
    top_n: int = Query(10, ge=1, le=50),
):
    """Personnel recommendation for a team."""
    from src.analysis.recommendation import recommend_for_team

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    credits = silver_load_all_credits()

    recs = recommend_for_team(team_ids, scores, credits, top_n=top_n)
    return {"team": team_ids, "recommendations": recs}


@app.get("/api/predict")
def predict(
    team: str = Query(..., description="チームの人物ID (カンマ区切り)"),
):
    """Predict production scale from team composition."""
    from src.analysis.anime_prediction import predict_anime_score

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    credits = silver_load_all_credits()
    anime_list = silver_load_all_anime()

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = (
        {r["person_id"]: r.get("iv_score", 0) for r in scores} if scores else None
    )

    result = predict_anime_score(
        team_ids, credits, anime_map, person_scores=person_scores
    )
    return {"team": team_ids, **result}


@app.get("/api/bridges")
def bridges():
    """Bridge persons between communities."""
    data = load_bridge_analysis_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Bridge data not found. Run pipeline first."
        )
    return data


@app.get("/api/mentorships")
def mentorships():
    """Inferred mentorship relationships."""
    data = load_mentorship_relationships_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Mentorship data not found. Run pipeline first."
        )
    return data


@app.get("/api/studio-disparity")
def studio_disparity():
    """Cross-studio compensation gap analysis — compare score gaps within the same skill band."""
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
    """Time-series data of network evolution."""
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
    """Genre affinity data."""
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
    """Productivity metrics."""
    data = load_productivity_metrics_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Productivity data not found. Run pipeline first."
        )
    items = [{"person_id": pid, **d} for pid, d in list(data.items())[:limit]]
    return {"total": len(data), "items": items}


@app.get("/api/stats")
def db_stats():
    """Database statistics."""
    stats = silver_db_stats()
    sources = []  # ops_source_scrape_status not in DuckDB yet
    return {"stats": stats, "data_sources": sources}


@app.get("/api/freshness")
def freshness():
    """Data source freshness check."""
    return {}  # freshness data not available until Bronze Parquet migration (Card 06)


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
    from src.infra.websocket import get_websocket_manager

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
        from src.runtime.pipeline import run_scoring_pipeline

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
    """Start the API server."""
    from src.infra.logging import setup_logging

    setup_logging()
    logger.info("starting_api_server", host="0.0.0.0", port=8000)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()

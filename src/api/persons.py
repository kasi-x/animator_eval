"""Person-related API endpoints.

Endpoints:
  GET /api/persons               — list all person scores (paginated)
  GET /api/persons/search        — person search
  GET /api/persons/{id}          — person profile
  GET /api/persons/{id}/similar  — similar persons
  GET /api/persons/{id}/history  — score history
  GET /api/persons/{id}/network  — ego graph
  GET /api/persons/{id}/milestones — career milestones
  GET /api/persons/{id}/profile  — two-layer contribution profile
"""

from __future__ import annotations

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.analysis.explain import explain_individual_profile
from src.analysis.similarity import find_similar_persons
from src.api_validators import PersonId, validate_query_string
from src.database import (
    db_connection,
    get_score_history,
    search_persons,
)
from src.utils.json_io import (
    load_career_milestones_from_json,
    load_individual_profiles_from_json,
    load_person_scores_from_json,
)

logger = structlog.get_logger()

router = APIRouter(tags=["persons"])


class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    per_page: int
    pages: int


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
    FROM person_scores s
    JOIN persons p ON s.person_id = p.id
    LEFT JOIN credits c ON s.person_id = c.person_id
    WHERE s.iv_score IS NOT NULL {extra_where}
    GROUP BY s.person_id
"""


def _row_to_person(r) -> dict:
    """Convert database row to person response dict with metadata disclaimer."""
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
        "metadata": {
            "disclaimer": (
                "スコア (iv_score, birank, patronage, person_fe, awcc, dormancy) は、"
                "公開クレジットデータに基づくネットワーク上の位置・協業密度を示す定量指標です。"
                "個人の能力・技量・芸術性を評価または測定するものではありません。"
                "本スコアを雇用・報酬・人事評価の根拠として使用する場合は、"
                "信頼区間（confidence_lower, confidence_upper）も併用してください。"
            ),
            "ci_required_for_compensation": True,
            "score_interpretation": (
                "高スコア = ネットワーク中心性・協業密度が高い "
                "（多くの同僚に信頼されている、規模の大きい作品に参加している、などを示唆）。 "
                "低スコア = データセット上の可視性が限定的 "
                "（実力の不足を意味しない）。"
            ),
        },
    }


@router.get("/api/persons")
def list_persons(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(50, ge=1, le=200, description="Results per page"),
    sort: str = Query(
        "iv_score", description="Sort axis (iv_score, person_fe, birank, patronage)"
    ),
):
    """All person scores (paginated)."""
    valid_sorts = {"iv_score", "person_fe", "birank", "patronage", "dormancy", "awcc"}
    if sort not in valid_sorts:
        raise HTTPException(
            status_code=400, detail=f"Invalid sort: {sort}. Use: {valid_sorts}"
        )

    import sqlite3 as _sqlite3

    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        total = conn.execute(
            "SELECT COUNT(DISTINCT s.person_id) FROM person_scores s WHERE s.iv_score IS NOT NULL"
        ).fetchone()[0]
        pages = (total + per_page - 1) // per_page
        offset = (page - 1) * per_page
        sql = _PERSON_SELECT_SQL.format(extra_where="")
        rows = conn.execute(
            f"{sql} ORDER BY s.{sort} DESC LIMIT ? OFFSET ?",
            [per_page, offset],
        ).fetchall()

    items = [_row_to_person(r) for r in rows]
    return PaginatedResponse(
        items=items, total=total, page=page, per_page=per_page, pages=pages
    )


@router.get("/api/persons/search")
def search(
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
):
    """Person search (partial name/ID match)."""
    q = validate_query_string(q)
    with db_connection() as conn:
        results = search_persons(conn, q, limit=limit)
    return {"query": q, "count": len(results), "results": results}


@router.get("/api/persons/{person_id}")
def get_person(person_id: PersonId):
    """Person profile (scores + breakdown)."""
    import sqlite3 as _sqlite3

    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        sql = _PERSON_SELECT_SQL.format(extra_where="AND s.person_id = ?")
        row = conn.execute(sql, [person_id]).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
    return _row_to_person(row)


@router.get("/api/persons/{person_id}/similar")
def get_similar(
    person_id: PersonId,
    top_n: int = Query(10, ge=1, le=50, description="Number of similar persons"),
):
    """Similar person search (cosine similarity)."""
    import sqlite3 as _sqlite3

    with db_connection() as conn:
        conn.row_factory = _sqlite3.Row
        target = conn.execute(
            "SELECT person_fe, birank, patronage, awcc, dormancy,"
            " (SELECT role FROM credits WHERE person_id=s.person_id"
            "  GROUP BY role ORDER BY COUNT(*) DESC LIMIT 1) AS primary_role"
            " FROM person_scores s WHERE s.person_id = ?",
            [person_id],
        ).fetchone()
        if target is None:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        sql = _PERSON_SELECT_SQL.format(extra_where="")
        rows = conn.execute(f"{sql} ORDER BY s.iv_score DESC LIMIT 2000").fetchall()

    scores = [_row_to_person(r) for r in rows]
    similar = find_similar_persons(person_id, scores, top_n=top_n)
    return {"person_id": person_id, "similar": similar or []}


@router.get("/api/persons/{person_id}/history")
def get_person_history(
    person_id: PersonId,
    limit: int = Query(50, ge=1, le=200, description="Number of history entries"),
):
    """Person score history."""
    with db_connection() as conn:
        history = get_score_history(conn, person_id, limit=limit)
    if not history:
        raise HTTPException(status_code=404, detail=f"No history for {person_id}")
    return {"person_id": person_id, "history": history}


@router.get("/api/persons/{person_id}/network")
def get_person_network(
    person_id: PersonId,
    hops: int = Query(1, ge=1, le=3, description="ネットワーク深度"),
):
    """Person ego graph (local network)."""
    from src.analysis.network.ego_graph import extract_ego_graph
    from src.database import load_all_anime, load_all_credits

    with db_connection() as conn:
        credits = load_all_credits(conn)
        anime_list = load_all_anime(conn)

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = (
        {r["person_id"]: r.get("iv_score", 0) for r in scores} if scores else None
    )

    result = extract_ego_graph(
        person_id, credits, anime_map, hops=hops, person_scores=person_scores
    )
    if result["total_nodes"] == 0:
        raise HTTPException(
            status_code=404, detail=f"Person {person_id} not found in credits"
        )
    return result


@router.get("/api/persons/{person_id}/milestones")
def get_person_milestones(person_id: PersonId):
    """Person career milestones."""
    data = load_career_milestones_from_json()
    if not data:
        raise HTTPException(
            status_code=404, detail="Milestone data not found. Run pipeline first."
        )
    if person_id not in data:
        raise HTTPException(status_code=404, detail=f"No milestones for {person_id}")
    return {"person_id": person_id, "milestones": data[person_id]}


@router.get("/api/persons/{person_id}/profile")
def get_person_profile(person_id: PersonId):
    """Individual contribution profile (two-layer model: network + individual contribution)."""
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

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
from src.analysis.io.mart_writer import GoldReader, gold_connect_with_silver
from src.analysis.similarity import find_similar_persons
from src.analysis.io.conformed_reader import (
    load_all_anime as silver_load_anime,
    load_all_credits as silver_load_credits,
)
from src.routers.validators import PersonId, validate_query_string
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


_PERSON_LIST_SQL = """
WITH primary_roles AS (
    SELECT person_id, arg_max(role, cnt) AS primary_role
    FROM (SELECT person_id, role, COUNT(*) AS cnt FROM credits GROUP BY person_id, role)
    GROUP BY person_id
)
SELECT
    s.person_id,
    p.name_ja, p.name_en, p.image_medium,
    s.iv_score, s.birank, s.patronage, s.person_fe,
    s.awcc, s.dormancy, s.studio_fe_exposure,
    MIN(c.credit_year) AS first_year,
    MAX(c.credit_year) AS latest_year,
    COUNT(DISTINCT c.anime_id) AS total_works,
    pr.primary_role
FROM person_scores s
JOIN persons p ON p.id = s.person_id
LEFT JOIN credits c ON c.person_id = s.person_id
LEFT JOIN primary_roles pr ON pr.person_id = s.person_id
WHERE s.iv_score IS NOT NULL {extra_where}
GROUP BY
    s.person_id, p.name_ja, p.name_en, p.image_medium,
    s.iv_score, s.birank, s.patronage, s.person_fe,
    s.awcc, s.dormancy, s.studio_fe_exposure, pr.primary_role
ORDER BY s.{sort} DESC NULLS LAST
{limit_offset}
"""


def _row_to_person(r: dict) -> dict:
    """Convert database row dict to person response dict with metadata disclaimer."""
    return {
        "person_id": r["person_id"],
        "name": r.get("name_ja") or r.get("name_en"),
        "name_ja": r.get("name_ja"),
        "name_en": r.get("name_en"),
        "image_medium": r.get("image_medium"),
        "iv_score": r.get("iv_score"),
        "birank": r.get("birank"),
        "patronage": r.get("patronage"),
        "person_fe": r.get("person_fe"),
        "awcc": r.get("awcc"),
        "dormancy": r.get("dormancy"),
        "studio_fe_exposure": r.get("studio_fe_exposure"),
        "primary_role": r.get("primary_role"),
        "career": {
            "first_year": r.get("first_year"),
            "latest_year": r.get("latest_year"),
            "total_works": r.get("total_works") or 0,
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

    offset = (page - 1) * per_page
    try:
        with gold_connect_with_silver() as conn:
            total = conn.execute(
                "SELECT COUNT(DISTINCT s.person_id) FROM person_scores s"
                " WHERE s.iv_score IS NOT NULL"
            ).fetchone()[0]
            pages = (total + per_page - 1) // per_page
            sql = _PERSON_LIST_SQL.format(
                extra_where="",
                sort=sort,
                limit_offset=f"LIMIT {per_page} OFFSET {offset}",
            )
            rel = conn.execute(sql)
            cols = [d[0] for d in rel.description]
            rows = [dict(zip(cols, row)) for row in rel.fetchall()]
    except Exception:
        logger.warning("list_persons_gold_unavailable")
        return PaginatedResponse(items=[], total=0, page=page, per_page=per_page, pages=0)

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
    pattern = f"%{q}%"
    sql = """
        SELECT p.id, p.name_ja, p.name_en, p.name_ko, p.name_zh,
               s.iv_score, s.person_fe, s.birank, s.patronage,
               COUNT(c.anime_id) AS credit_count
        FROM persons p
        LEFT JOIN person_scores s ON s.person_id = p.id
        LEFT JOIN credits c ON c.person_id = p.id
        WHERE p.name_ja ILIKE ?
           OR p.name_en ILIKE ?
           OR p.name_ko ILIKE ?
           OR p.name_zh ILIKE ?
           OR p.aliases ILIKE ?
           OR p.id ILIKE ?
        GROUP BY p.id, p.name_ja, p.name_en, p.name_ko, p.name_zh,
                 s.iv_score, s.person_fe, s.birank, s.patronage
        ORDER BY s.iv_score DESC NULLS LAST
        LIMIT ?
    """
    try:
        with gold_connect_with_silver() as conn:
            rel = conn.execute(sql, [pattern] * 6 + [limit])
            cols = [d[0] for d in rel.description]
            results = [dict(zip(cols, row)) for row in rel.fetchall()]
    except Exception:
        logger.warning("search_gold_unavailable")
        results = []
    return {"query": q, "count": len(results), "results": results}


@router.get("/api/persons/{person_id}")
def get_person(person_id: PersonId):
    """Person profile (scores + breakdown)."""
    try:
        with gold_connect_with_silver() as conn:
            sql = _PERSON_LIST_SQL.format(
                extra_where="AND s.person_id = ?",
                sort="iv_score",
                limit_offset="",
            )
            rel = conn.execute(sql, [person_id])
            cols = [d[0] for d in rel.description]
            row = rel.fetchone()
    except Exception:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
    if row is None:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
    return _row_to_person(dict(zip(cols, row)))


@router.get("/api/persons/{person_id}/similar")
def get_similar(
    person_id: PersonId,
    top_n: int = Query(10, ge=1, le=50, description="Number of similar persons"),
):
    """Similar person search (cosine similarity)."""
    try:
        with gold_connect_with_silver() as conn:
            rel = conn.execute(
                """
                WITH primary_roles AS (
                    SELECT person_id, arg_max(role, cnt) AS primary_role
                    FROM (SELECT person_id, role, COUNT(*) AS cnt FROM credits GROUP BY person_id, role)
                    GROUP BY person_id
                )
                SELECT s.person_fe, s.birank, s.patronage, s.awcc, s.dormancy,
                       pr.primary_role
                FROM person_scores s
                LEFT JOIN primary_roles pr ON pr.person_id = s.person_id
                WHERE s.person_id = ?
                """,
                [person_id],
            )
            target_row = rel.fetchone()
            if target_row is None:
                raise HTTPException(
                    status_code=404, detail=f"Person {person_id} not found"
                )

            sql = _PERSON_LIST_SQL.format(
                extra_where="",
                sort="iv_score",
                limit_offset="LIMIT 2000",
            )
            rel2 = conn.execute(sql)
            cols = [d[0] for d in rel2.description]
            rows = [dict(zip(cols, row)) for row in rel2.fetchall()]
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=503, detail="Score data unavailable")

    scores = [_row_to_person(r) for r in rows]
    similar = find_similar_persons(person_id, scores, top_n=top_n)
    return {"person_id": person_id, "similar": similar or []}


@router.get("/api/persons/{person_id}/history")
def get_person_history(
    person_id: PersonId,
    limit: int = Query(50, ge=1, le=200, description="Number of history entries"),
):
    """Person score history."""
    gold = GoldReader()
    history = gold.score_history_for(person_id)
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

    credits = silver_load_credits()
    anime_list = silver_load_anime()

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

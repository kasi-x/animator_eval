"""Report API router — brief generation, versioning, technical appendix.

Mounted into the main FastAPI app in ``src/api.py`` via
``app.include_router(router)``.

Migrated from ``scripts/report_api.py`` (2026-04-23, §8.3).
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import structlog
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from scripts.generate_briefs_v2 import generate_all_briefs
from scripts.generate_technical_appendix import generate_appendix
from scripts.report_generators.export import render_brief_html
from scripts.report_generators.versioning import (
    compare_versions,
    get_report_git_history,
    rollback_to_version,
)

logger = structlog.get_logger()

router = APIRouter(tags=["reports"])

_BRIEF_IDS = ("policy", "hr", "business")


@contextmanager
def _api_error_boundary(event_name: str, **log_ctx):
    """Error boundary for API endpoints — logs and converts to HTTPException."""
    try:
        yield
    except HTTPException:
        raise
    except Exception as e:
        logger.error(event_name, error=str(e), **log_ctx)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Response models ──────────────────────────────────────────────


class BriefResponse(BaseModel):
    brief_id: str
    status: str
    sections: int
    method_gates: int
    size_kb: float


class VersionInfo(BaseModel):
    brief_id: str
    version_sha: str
    timestamp: str
    author: str
    commit_message: str
    size_bytes: int


class ComparisonResult(BaseModel):
    brief_id: str
    version1_sha: str
    version2_sha: str
    sections_changed: list[str]
    size_delta_bytes: int


# ─── Brief endpoints ──────────────────────────────────────────────


@router.post("/api/briefs/generate")
async def generate_briefs(background_tasks: BackgroundTasks):
    """Generate all 3 briefs on demand."""
    with _api_error_boundary("brief_generation_error"):
        logger.info("brief_generation_requested")
        background_tasks.add_task(generate_all_briefs)
        return {
            "status": "generating",
            "message": "Brief generation started in background",
            "check_status": "/api/briefs/status",
        }


def _read_brief_status(brief_id: str) -> BriefResponse:
    """Read brief status from disk — ready if exists and valid, not_ready otherwise."""
    file_path = Path(f"result/json/{brief_id}_brief.json")
    if file_path.exists():
        try:
            data = json.loads(file_path.read_text())
            return BriefResponse(
                brief_id=brief_id,
                status="ready",
                sections=len(data.get("sections", [])),
                method_gates=len(data.get("method_gates", [])),
                size_kb=file_path.stat().st_size / 1024,
            )
        except Exception as e:
            logger.warning("brief_status_error", brief_id=brief_id, error=str(e))
    return BriefResponse(
        brief_id=brief_id,
        status="not_ready",
        sections=0,
        method_gates=0,
        size_kb=0,
    )


@router.get("/api/briefs/status")
async def briefs_status():
    """Get status of all briefs."""
    with _api_error_boundary("briefs_status_error"):
        results = [_read_brief_status(brief_id) for brief_id in _BRIEF_IDS]
        return {"briefs": results}


@router.get("/api/briefs/{brief_id}")
async def get_brief(brief_id: str):
    """Get a brief by ID."""
    with _api_error_boundary("brief_fetch_error", brief_id=brief_id):
        file_path = Path(f"result/json/{brief_id}_brief.json")
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Brief {brief_id} not found")
        return json.loads(file_path.read_text())


@router.get("/api/briefs/{brief_id}/html")
async def get_brief_html(brief_id: str):
    """Get brief as HTML."""
    with _api_error_boundary("brief_html_error", brief_id=brief_id):
        html_path = render_brief_html(brief_id, output_dir="result/html")
        return FileResponse(html_path, media_type="text/html")


@router.get("/api/briefs/{brief_id}/history")
async def get_brief_history(brief_id: str):
    """Get version history of a brief."""
    with _api_error_boundary("history_fetch_error", brief_id=brief_id):
        history = get_report_git_history(brief_id, max_versions=20)
        return history.to_dict()


@router.get("/api/briefs/{brief_id}/compare")
async def compare_brief_versions(brief_id: str, v1: str, v2: str):
    """Compare two versions of a brief."""
    with _api_error_boundary("comparison_error", brief_id=brief_id):
        changes = compare_versions(brief_id, v1, v2)
        return ComparisonResult(
            brief_id=brief_id,
            version1_sha=v1,
            version2_sha=v2,
            sections_changed=changes.get("sections_changed", []),
            size_delta_bytes=changes.get("size_delta_bytes", 0),
        )


@router.post("/api/briefs/{brief_id}/rollback")
async def restore_brief_version(brief_id: str, target_sha: str):
    """Restore a brief to a previous version."""
    with _api_error_boundary("rollback_error", brief_id=brief_id):
        success = rollback_to_version(brief_id, target_sha)
        if not success:
            raise HTTPException(status_code=400, detail="Rollback failed")
        return {"status": "restored", "brief_id": brief_id, "sha": target_sha}


# ─── Technical appendix endpoints ─────────────────────────────────


@router.get("/api/appendix")
async def get_technical_appendix():
    """Get technical appendix."""
    with _api_error_boundary("appendix_fetch_error"):
        file_path = Path("result/json/technical_appendix.json")
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Technical appendix not found")
        return json.loads(file_path.read_text())


@router.post("/api/appendix/regenerate")
async def regenerate_appendix(background_tasks: BackgroundTasks):
    """Regenerate technical appendix."""
    with _api_error_boundary("appendix_regeneration_error"):
        logger.info("appendix_regeneration_requested")
        background_tasks.add_task(generate_appendix)
        return {
            "status": "regenerating",
            "message": "Appendix regeneration started in background",
            "check_status": "/api/appendix",
        }


# ─── Discovery endpoint ───────────────────────────────────────────


@router.get("/api/reports/docs")
async def api_docs():
    """Report API endpoint catalog (human-readable)."""
    return {
        "title": "Animetor Report API",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/briefs/generate": "Generate all briefs (async)",
            "GET /api/briefs/status": "Get all briefs status",
            "GET /api/briefs/{brief_id}": "Get brief JSON",
            "GET /api/briefs/{brief_id}/html": "Get brief as HTML",
            "GET /api/briefs/{brief_id}/history": "Get version history",
            "GET /api/briefs/{brief_id}/compare?v1=sha1&v2=sha2": "Compare versions",
            "POST /api/briefs/{brief_id}/rollback?target_sha=sha": "Restore version",
            "GET /api/appendix": "Get technical appendix",
            "POST /api/appendix/regenerate": "Regenerate appendix (async)",
        },
    }

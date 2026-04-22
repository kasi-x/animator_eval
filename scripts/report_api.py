"""FastAPI REST endpoints for report generation and versioning."""

from fastapi import FastAPI, HTTPException, WebSocket, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from pathlib import Path
import asyncio
import structlog
from typing import Optional, List
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generate_briefs_v2 import generate_all_briefs, validate_briefs
from scripts.generate_technical_appendix import generate_appendix
from scripts.report_generators.versioning import (
    get_report_git_history, compare_versions, rollback_to_version
)
from scripts.report_generators.export import render_brief_html

logger = structlog.get_logger()

app = FastAPI(
    title="Animetor Report API",
    description="REST API for anime staff evaluation reports",
    version="1.0.0"
)

# Enable CORS for web frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
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
    sections_changed: List[str]
    size_delta_bytes: int

# REST Endpoints

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "report-api"}

@app.post("/api/briefs/generate")
async def generate_briefs(background_tasks: BackgroundTasks):
    """Generate all 3 briefs on demand."""
    try:
        logger.info("brief_generation_requested")
        background_tasks.add_task(generate_all_briefs)
        return {
            "status": "generating",
            "message": "Brief generation started in background",
            "check_status": "/api/briefs/status"
        }
    except Exception as e:
        logger.error("brief_generation_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/briefs/status")
async def briefs_status():
    """Get status of all briefs."""
    results = []
    for brief_id in ["policy", "hr", "business"]:
        file_path = Path(f"result/json/{brief_id}_brief.json")
        if file_path.exists():
            try:
                data = json.loads(file_path.read_text())
                results.append(BriefResponse(
                    brief_id=brief_id,
                    status="ready",
                    sections=len(data.get("sections", [])),
                    method_gates=len(data.get("method_gates", [])),
                    size_kb=file_path.stat().st_size / 1024,
                ))
            except Exception as e:
                logger.warning("brief_status_error", brief_id=brief_id, error=str(e))
        else:
            results.append(BriefResponse(
                brief_id=brief_id, status="not_ready", sections=0, method_gates=0, size_kb=0
            ))
    return {"briefs": results}

@app.get("/api/briefs/{brief_id}")
async def get_brief(brief_id: str):
    """Get a brief by ID."""
    file_path = Path(f"result/json/{brief_id}_brief.json")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Brief {brief_id} not found")
    
    try:
        return json.loads(file_path.read_text())
    except Exception as e:
        logger.error("brief_fetch_error", brief_id=brief_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/briefs/{brief_id}/html")
async def get_brief_html(brief_id: str):
    """Get brief as HTML."""
    try:
        html_path = render_brief_html(brief_id, output_dir="result/html")
        return FileResponse(html_path, media_type="text/html")
    except Exception as e:
        logger.error("brief_html_error", brief_id=brief_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/briefs/{brief_id}/history")
async def get_brief_history(brief_id: str):
    """Get version history of a brief."""
    try:
        history = get_report_git_history(brief_id, max_versions=20)
        return history.to_dict()
    except Exception as e:
        logger.error("history_fetch_error", brief_id=brief_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/briefs/{brief_id}/compare")
async def compare_brief_versions(brief_id: str, v1: str, v2: str):
    """Compare two versions of a brief."""
    try:
        changes = compare_versions(brief_id, v1, v2)
        return ComparisonResult(
            brief_id=brief_id,
            version1_sha=v1,
            version2_sha=v2,
            sections_changed=changes.get("sections_changed", []),
            size_delta_bytes=changes.get("size_delta_bytes", 0),
        )
    except Exception as e:
        logger.error("comparison_error", brief_id=brief_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/briefs/{brief_id}/rollback")
async def restore_brief_version(brief_id: str, target_sha: str):
    """Restore a brief to a previous version."""
    try:
        success = rollback_to_version(brief_id, target_sha)
        if not success:
            raise HTTPException(status_code=400, detail="Rollback failed")
        return {"status": "restored", "brief_id": brief_id, "sha": target_sha}
    except Exception as e:
        logger.error("rollback_error", brief_id=brief_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/appendix")
async def get_technical_appendix():
    """Get technical appendix."""
    file_path = Path("result/json/technical_appendix.json")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Technical appendix not found")
    
    try:
        return json.loads(file_path.read_text())
    except Exception as e:
        logger.error("appendix_fetch_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/appendix/regenerate")
async def regenerate_appendix(background_tasks: BackgroundTasks):
    """Regenerate technical appendix."""
    try:
        logger.info("appendix_regeneration_requested")
        background_tasks.add_task(generate_appendix)
        return {
            "status": "regenerating",
            "message": "Appendix regeneration started in background",
            "check_status": "/api/appendix"
        }
    except Exception as e:
        logger.error("appendix_regeneration_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint for live regeneration feedback
connected_clients: List[WebSocket] = []

@app.websocket("/ws/regenerate")
async def websocket_regenerate(websocket: WebSocket):
    """WebSocket for live regeneration feedback."""
    await websocket.accept()
    connected_clients.append(websocket)
    
    try:
        while True:
            data = await websocket.receive_text()
            if data == "start":
                # Start regeneration
                logger.info("websocket_regeneration_started")
                await websocket.send_json({
                    "status": "started",
                    "timestamp": str(datetime.now())
                })
                
                # Run generation (simplified for demo)
                await asyncio.sleep(0.5)
                
                await websocket.send_json({
                    "status": "completed",
                    "timestamp": str(datetime.now())
                })
                
    except Exception as e:
        logger.error("websocket_error", error=str(e))
    finally:
        connected_clients.remove(websocket)

@app.get("/api/docs")
async def api_docs():
    """API documentation."""
    return {
        "title": "Animetor Report API",
        "version": "1.0.0",
        "endpoints": {
            "GET /api/health": "Health check",
            "POST /api/briefs/generate": "Generate all briefs (async)",
            "GET /api/briefs/status": "Get all briefs status",
            "GET /api/briefs/{brief_id}": "Get brief JSON",
            "GET /api/briefs/{brief_id}/html": "Get brief as HTML",
            "GET /api/briefs/{brief_id}/history": "Get version history",
            "GET /api/briefs/{brief_id}/compare?v1=sha1&v2=sha2": "Compare versions",
            "POST /api/briefs/{brief_id}/rollback?target_sha=sha": "Restore version",
            "GET /api/appendix": "Get technical appendix",
            "POST /api/appendix/regenerate": "Regenerate appendix (async)",
            "WS /ws/regenerate": "WebSocket for live feedback",
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

"""Report versioning system with git history tracking."""

import json
import subprocess
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, List
import structlog

logger = structlog.get_logger()

@dataclass
class ReportVersion:
    """Single version of a report."""
    brief_id: str
    version_sha: str
    timestamp: str
    author: str
    commit_message: str
    file_path: str
    size_bytes: int
    sections_count: int
    method_gates: List[str] = field(default_factory=list)
    
    def to_dict(self):
        return asdict(self)

@dataclass
class ReportHistory:
    """Complete version history for a report."""
    brief_id: str
    versions: List[ReportVersion] = field(default_factory=list)
    total_versions: int = 0
    current_version: Optional[ReportVersion] = None
    
    def add_version(self, version: ReportVersion):
        """Add a version to history."""
        self.versions.append(version)
        self.total_versions = len(self.versions)
        self.current_version = version
        logger.info("version_added", brief_id=self.brief_id, sha=version.version_sha)
    
    def get_previous_version(self, offset: int = 1) -> Optional[ReportVersion]:
        """Get a previous version by offset."""
        idx = len(self.versions) - 1 - offset
        if idx >= 0:
            return self.versions[idx]
        return None
    
    def to_dict(self):
        return {
            "brief_id": self.brief_id,
            "total_versions": self.total_versions,
            "current_version": self.current_version.to_dict() if self.current_version else None,
            "versions": [v.to_dict() for v in self.versions[-5:]],  # Last 5 versions
        }

def get_report_git_history(brief_id: str, max_versions: int = 20) -> ReportHistory:
    """Extract git history for a report."""
    file_path = f"result/json/{brief_id}_brief.json"
    history = ReportHistory(brief_id=brief_id)
    
    try:
        # Get all commits touching this file
        cmd = [
            "git", "log",
            "--follow", "--pretty=format:%H|%aI|%an|%s",
            "--", file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
        
        if result.returncode != 0:
            logger.warning("git_history_failed", brief_id=brief_id, error=result.stderr)
            return history
        
        for line in result.stdout.strip().split('\n')[:max_versions]:
            if not line:
                continue
                
            sha, timestamp, author, message = line.split('|', 3)
            
            # Get file size at this commit
            cmd_size = ["git", "show", f"{sha}:{file_path}"]
            result_size = subprocess.run(cmd_size, capture_output=True, text=True, cwd=Path.cwd())
            
            if result_size.returncode == 0:
                try:
                    data = json.loads(result_size.stdout)
                    size_bytes = len(result_size.stdout.encode())
                    sections_count = len(data.get("sections", []))
                    method_gates = data.get("method_gates", [])
                    
                    version = ReportVersion(
                        brief_id=brief_id,
                        version_sha=sha[:8],
                        timestamp=timestamp,
                        author=author,
                        commit_message=message,
                        file_path=file_path,
                        size_bytes=size_bytes,
                        sections_count=sections_count,
                        method_gates=method_gates,
                    )
                    history.add_version(version)
                except json.JSONDecodeError:
                    logger.warning("json_parse_failed", brief_id=brief_id, sha=sha)
                    continue
    except Exception as e:
        logger.error("version_extraction_failed", brief_id=brief_id, error=str(e))
    
    return history

def compare_versions(brief_id: str, version1_sha: str, version2_sha: str) -> dict:
    """Compare two versions of a report."""
    file_path = f"result/json/{brief_id}_brief.json"
    
    try:
        # Get both versions
        cmd1 = ["git", "show", f"{version1_sha}:{file_path}"]
        cmd2 = ["git", "show", f"{version2_sha}:{file_path}"]
        
        result1 = subprocess.run(cmd1, capture_output=True, text=True, cwd=Path.cwd())
        result2 = subprocess.run(cmd2, capture_output=True, text=True, cwd=Path.cwd())
        
        if result1.returncode != 0 or result2.returncode != 0:
            logger.warning("version_comparison_failed", brief_id=brief_id)
            return {}
        
        data1 = json.loads(result1.stdout)
        data2 = json.loads(result2.stdout)
        
        # Find differences
        changes = {
            "sections_changed": [],
            "fields_changed": [],
            "size_delta_bytes": len(result2.stdout.encode()) - len(result1.stdout.encode()),
        }
        
        # Compare sections
        sections1 = {s["id"]: s for s in data1.get("sections", [])}
        sections2 = {s["id"]: s for s in data2.get("sections", [])}
        
        for sec_id in sections1:
            if sec_id not in sections2:
                changes["sections_changed"].append(f"removed: {sec_id}")
            elif sections1[sec_id] != sections2[sec_id]:
                changes["sections_changed"].append(f"modified: {sec_id}")
        
        for sec_id in sections2:
            if sec_id not in sections1:
                changes["sections_changed"].append(f"added: {sec_id}")
        
        logger.info("version_compared", brief_id=brief_id, changes_count=len(changes["sections_changed"]))
        return changes
        
    except Exception as e:
        logger.error("version_comparison_error", brief_id=brief_id, error=str(e))
        return {}

def rollback_to_version(brief_id: str, target_sha: str) -> bool:
    """Restore a previous version of a report."""
    file_path = f"result/json/{brief_id}_brief.json"
    
    try:
        cmd = ["git", "show", f"{target_sha}:{file_path}"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
        
        if result.returncode != 0:
            logger.error("rollback_failed_git", brief_id=brief_id, sha=target_sha)
            return False
        
        # Write to current location
        output_path = Path.cwd() / file_path
        output_path.write_text(result.stdout)
        
        logger.info("rollback_completed", brief_id=brief_id, target_sha=target_sha[:8])
        return True
        
    except Exception as e:
        logger.error("rollback_error", brief_id=brief_id, error=str(e))
        return False

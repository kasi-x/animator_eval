"""Reproducibility footer — git_sha + spec_hash + timestamp + data cutoff。

各 v2 report の HTML footer に再現可能性メタを自動挿入する。
誰でも report の数値を後追い再現できることを担保:

- git_sha: 当該 generate コマンド実行時の HEAD SHA
- spec_hash: SHA-256 of ReportSpec (claim + identifying_assumption + ...)
- timestamp: ISO-8601 UTC
- data_cutoff_date: 入力 data の最終更新日 (lineage 経由)
- pipeline_version: 既知の semver
- pixi_lock_hash: dependency closure hash

JSON 出力も別途 `result/reports/_repro.json` に保存し、報告書外でも参照可。
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ReproMetadata:
    """Single report の再現メタ。"""

    report_name: str
    git_sha: str
    spec_hash: str
    timestamp_utc: str
    pipeline_version: str
    pixi_lock_hash: str
    data_cutoff_date: str | None = None
    sources: tuple[str, ...] = ()
    meta_table: str | None = None


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def get_git_sha() -> str:
    """HEAD SHA (short)。subprocess 失敗時は 'unknown'。"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True, text=True, check=False, timeout=2,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def get_pixi_lock_hash() -> str:
    """pixi.lock の SHA-256 (short)。"""
    p = Path("pixi.lock")
    if not p.exists():
        return "no-lock"
    try:
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        return h[:12]
    except OSError:
        return "read-fail"


def get_pipeline_version() -> str:
    """VERSION ファイル or env から取得。"""
    p = Path("VERSION")
    if p.exists():
        try:
            return p.read_text(encoding="utf-8").strip()
        except OSError:
            pass
    return os.environ.get("ANIMETOR_VERSION", "dev")


def compute_spec_hash(spec_obj: object) -> str:
    """ReportSpec の決定論的 SHA-256 (short)。

    claim + identifying_assumption + null_model + sources + meta_table の
    JSON 表現を SHA-256。
    """
    if spec_obj is None:
        return "no-spec"
    fields = {}
    for attr in (
        "name", "claim", "identifying_assumption",
        "null_model", "sources", "meta_table", "estimator",
    ):
        val = getattr(spec_obj, attr, None)
        if val is not None:
            if isinstance(val, (list, tuple)):
                fields[attr] = list(val)
            else:
                fields[attr] = str(val)
    raw = json.dumps(fields, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Build & render
# ---------------------------------------------------------------------------


def build_metadata(
    report_name: str,
    *,
    spec_obj: object | None = None,
    data_cutoff_date: str | None = None,
) -> ReproMetadata:
    """Report 1 件分の メタを集計。"""
    sources_tuple: tuple[str, ...] = ()
    meta_table: str | None = None
    if spec_obj is not None:
        srcs = getattr(spec_obj, "sources", None)
        if srcs:
            sources_tuple = tuple(srcs)
        meta_table = getattr(spec_obj, "meta_table", None)

    return ReproMetadata(
        report_name=report_name,
        git_sha=get_git_sha(),
        spec_hash=compute_spec_hash(spec_obj),
        timestamp_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        pipeline_version=get_pipeline_version(),
        pixi_lock_hash=get_pixi_lock_hash(),
        data_cutoff_date=data_cutoff_date,
        sources=sources_tuple,
        meta_table=meta_table,
    )


def render_footer_html(meta: ReproMetadata) -> str:
    """Footer HTML を生成 (report 末尾に挿入)。"""
    sources_str = ", ".join(meta.sources) if meta.sources else "—"
    cutoff = meta.data_cutoff_date or "—"
    return (
        '<footer class="repro-footer" id="repro-footer" '
        'style="margin-top:1.5rem;padding:0.8rem 1rem;'
        'border-top:1px solid rgba(176,196,196,0.18);'
        'font-size:0.78rem;color:#909abd;font-family:monospace;">'
        f'<div><strong>Reproducibility:</strong> '
        f'git={meta.git_sha} · spec_hash={meta.spec_hash} · '
        f'pipeline_v={meta.pipeline_version} · '
        f'lock_hash={meta.pixi_lock_hash} · '
        f'generated_at={meta.timestamp_utc}'
        '</div>'
        f'<div>sources: {sources_str} · meta_table: {meta.meta_table or "—"} · '
        f'data_cutoff: {cutoff}</div>'
        '<div style="margin-top:0.3rem;font-size:0.7rem;color:#7a829e;">'
        'これらの値で本レポートの数値は再現可能。 / '
        'These values allow reproducing the numbers in this report.'
        '</div>'
        '</footer>'
    )


# ---------------------------------------------------------------------------
# Aggregate registry (writes to result/reports/_repro.json)
# ---------------------------------------------------------------------------


_REGISTRY: dict[str, ReproMetadata] = {}


def register(meta: ReproMetadata) -> None:
    _REGISTRY[meta.report_name] = meta


def flush_registry(path: Path | str = "result/reports/_repro.json") -> Path:
    """Registry を JSON 出力。pipeline 終了時に呼ぶ。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {name: asdict(meta) for name, meta in _REGISTRY.items()}
    p.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    log.info("repro_registry_flushed", path=str(p), n=len(_REGISTRY))
    return p

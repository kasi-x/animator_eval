"""Pipeline data → KeyFinding loader for executive_summary auto-inject.

Pipeline post_processing が `result/json/brief_keyfindings.json` を出力し、
各 brief generate 関数で `load_keyfindings(brief_id)` を呼び KeyFinding 一覧を
取得 → executive_summary に挿入する設計。

JSON 不在時は graceful: 空 list を返し、placeholder skeleton をレンダーする。

JSON schema:
{
  "policy": [
    {
      "metric_label": "structural opportunity gap (female vs male)",
      "value": -0.123,
      "unit": "log credits",
      "ci_low": -0.180, "ci_high": -0.066,
      "source_report": "equity_oaxaca",
      "method_gate": "bootstrap CI n=1000",
      "coverage_caveat": "",
      "direction": "-"
    },
    ...
  ],
  "hr": [...],
  "business": [...],
}
"""

from __future__ import annotations

import json
from pathlib import Path

import structlog

from scripts.report_generators.briefs.executive_summary import KeyFinding

log = structlog.get_logger(__name__)


_DEFAULT_PATH = Path("result/json/brief_keyfindings.json")


def load_keyfindings(
    brief_id: str,
    *,
    path: Path | str | None = None,
) -> list[KeyFinding]:
    """Load KeyFinding list for a brief; gracefully returns [] when unavailable."""
    p = Path(path) if path else _DEFAULT_PATH
    if not p.exists():
        log.debug("keyfindings_file_absent", path=str(p), brief=brief_id)
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("keyfindings_load_failed", path=str(p), error=str(exc))
        return []
    items = data.get(brief_id, []) if isinstance(data, dict) else []
    findings: list[KeyFinding] = []
    for entry in items:
        try:
            findings.append(
                KeyFinding(
                    metric_label=str(entry.get("metric_label", "")),
                    value=float(entry.get("value", 0.0)),
                    unit=str(entry.get("unit", "")),
                    ci_low=(
                        float(entry["ci_low"])
                        if entry.get("ci_low") is not None else None
                    ),
                    ci_high=(
                        float(entry["ci_high"])
                        if entry.get("ci_high") is not None else None
                    ),
                    source_report=str(entry.get("source_report", "")),
                    method_gate=str(entry.get("method_gate", "")),
                    coverage_caveat=str(entry.get("coverage_caveat", "")),
                    direction=str(entry.get("direction", "")),
                )
            )
        except (TypeError, ValueError, KeyError) as exc:
            log.warning("keyfinding_skip_invalid", entry=str(entry), error=str(exc))
    return findings


def write_keyfindings(
    payload: dict[str, list[dict]],
    *,
    path: Path | str | None = None,
) -> Path:
    """Persist KeyFinding payload (dict of brief_id → list of dict entries)。

    pipeline post_processing から呼ぶ前提。entry の schema は load_keyfindings の
    docstring に同じ。
    """
    p = Path(path) if path else _DEFAULT_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("keyfindings_written", path=str(p), n_briefs=len(payload))
    return p

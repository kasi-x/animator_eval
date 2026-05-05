"""LLM レビュー: merge_decisions.jsonl の各 case に verdict を付与。

Local LLM (ollama / lmstudio / vllm 等 OpenAI 互換 endpoint) を想定。
env LLM_BASE_URL / LLM_MODEL / LLM_API_KEY で切替。

Usage:
    pixi run python scripts/maintenance/review_merge_decisions.py \\
        --in result/merge_decisions.jsonl \\
        --out result/merge_decisions_reviewed.jsonl \\
        --max 100

各行 (LLM verdict 未済) を LLM に投げて `llm_review` を埋める。
verdict ∈ {ok, split, merge, wrong_value, low_confidence}
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROMPT_TEMPLATE = """You are auditing a merge decision in an anime credit database.

Each anime/person/studio has been clustered across multiple sources (anilist,
mal, ann, bgm, seesaa, madb, keyframe, sakuga, tmdb). For each field, the
algorithm picks one source as the representative. Your job is to judge
whether the selection is reasonable.

VERDICT options:
- "ok": selection is reasonable
- "split": cluster contains values that look like different real-world entities (e.g. different seasons, different people with same name)
- "merge": values across cluster look identical but algorithm did not unify them (rare in this context, leave to context interpretation)
- "wrong_value": a lower-tier source value looks more correct than the selected one
- "low_confidence": cannot judge from given context, recommend human review

INPUT:
{record_json}

Respond ONLY with JSON: {{"verdict": "<one of above>", "reason": "<short Japanese or English explanation>"}}.
No prose, no markdown.
"""


def call_llm(record: dict[str, Any], *, base_url: str, model: str, api_key: str, timeout: float = 60.0) -> dict[str, Any]:
    """OpenAI 互換 chat completions endpoint を呼ぶ。失敗時 low_confidence 返却。"""
    import httpx

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You output strict JSON only."},
            {"role": "user", "content": PROMPT_TEMPLATE.format(record_json=json.dumps(record, ensure_ascii=False))},
        ],
        "temperature": 0.0,
        "max_tokens": 200,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(f"{base_url.rstrip('/')}/chat/completions", json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        # 軽い JSON 抽出 (LLM が ```json ``` を被せる場合)
        if text.startswith("```"):
            text = text.strip("`").lstrip("json\n").strip()
        verdict = json.loads(text)
        v = verdict.get("verdict", "low_confidence")
        if v not in {"ok", "split", "merge", "wrong_value", "low_confidence"}:
            v = "low_confidence"
        return {"verdict": v, "reason": verdict.get("reason", ""), "raw": text}
    except Exception as exc:
        return {"verdict": "low_confidence", "reason": f"llm_call_failed: {exc}", "raw": None}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", dest="out", required=True)
    p.add_argument("--max", type=int, default=0, help="0 = 全件")
    p.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1"))
    p.add_argument("--model", default=os.environ.get("LLM_MODEL", "qwen2.5:7b"))
    p.add_argument("--api-key", default=os.environ.get("LLM_API_KEY", ""))
    p.add_argument("--dry-run", action="store_true", help="LLM 呼出せず record をそのまま転記")
    args = p.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    n_total = n_reviewed = n_skipped = 0
    with open(inp, encoding="utf-8") as fin, open(out, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            n_total += 1
            if args.max and n_reviewed >= args.max:
                fout.write(line + "\n")
                n_skipped += 1
                continue
            record = json.loads(line)
            if record.get("llm_review") is not None:
                fout.write(line + "\n")
                continue
            if args.dry_run:
                record["llm_review"] = {"verdict": "low_confidence", "reason": "dry-run", "raw": None}
            else:
                record["llm_review"] = call_llm(
                    record, base_url=args.base_url, model=args.model, api_key=args.api_key
                )
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            n_reviewed += 1
            if n_reviewed % 50 == 0:
                print(f"  reviewed: {n_reviewed} / total seen: {n_total}", file=sys.stderr)

    print(f"done. total={n_total} reviewed={n_reviewed} skipped={n_skipped} → {out}")


if __name__ == "__main__":
    main()

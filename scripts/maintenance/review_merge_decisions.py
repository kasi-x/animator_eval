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

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

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


_VALID_VERDICTS = {"ok", "split", "merge", "wrong_value", "low_confidence"}


def _extract_json(text: str) -> dict[str, Any] | None:
    """text から最初の {...} JSON を抽出。失敗時 None。"""
    if not text:
        return None
    # ```json ... ``` を剥がす
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.startswith("json"):
            t = t[4:]
        t = t.strip()
    # 直接 parse
    try:
        return json.loads(t)
    except Exception:
        pass
    # 最初の { から最後の } までを抽出
    i = t.find("{")
    j = t.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(t[i : j + 1])
        except Exception:
            return None
    return None


def call_llm(
    record: dict[str, Any],
    *,
    base_url: str,
    model: str,
    api_key: str,
    timeout: float = 120.0,
    max_tokens: int = 1024,
) -> dict[str, Any]:
    """LLM を呼んで verdict + reason を取得。

    Ollama (`/api/chat`) と OpenAI 互換 (`/chat/completions`) を base_url で
    自動判定。Ollama native は `think:false` で Qwen3 系の thinking 完全抑止可能。

    base_url 末尾規約:
      - `http://host:port`            → ollama native (/api/chat)
      - `http://host:port/v1`         → OpenAI 互換 (/chat/completions)
      - その他                          → OpenAI 互換 (suffix /chat/completions)
    """
    import httpx

    bu = base_url.rstrip("/")
    is_ollama_native = (
        bu.endswith(":11434")
        or bu.endswith("/api")
        or "/v1" not in bu
    )

    sys_msg = "You output strict JSON only. No reasoning, no prose."
    user_msg = PROMPT_TEMPLATE.format(record_json=json.dumps(record, ensure_ascii=False))

    if is_ollama_native:
        url = f"{bu.removesuffix('/api')}/api/chat"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": user_msg},
            ],
            "think": False,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.0, "num_predict": max_tokens},
        }
    else:
        url = f"{bu}/chat/completions"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": sys_msg + " /no_think"},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0.0,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
        }

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
        if is_ollama_native:
            text = (data.get("message", {}).get("content") or "").strip()
        else:
            msg = data["choices"][0]["message"]
            text = (msg.get("content") or "").strip()
            if not text:
                text = (msg.get("reasoning") or "").strip()
        parsed = _extract_json(text)
        if parsed is None:
            return {"verdict": "low_confidence", "reason": "json_parse_failed", "raw": text[:500]}
        v = parsed.get("verdict", "low_confidence")
        if v not in _VALID_VERDICTS:
            v = "low_confidence"
        return {"verdict": v, "reason": parsed.get("reason", ""), "raw": text[:500]}
    except Exception as exc:
        return {"verdict": "low_confidence", "reason": f"llm_call_failed: {exc}", "raw": None}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", dest="out", required=True)
    p.add_argument("--max", type=int, default=0, help="0 = 全件")
    p.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL", "http://localhost:11434"))
    p.add_argument("--model", default=os.environ.get("LLM_MODEL", "qwen2.5:14b-instruct"))
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

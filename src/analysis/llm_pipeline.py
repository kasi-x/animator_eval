"""LLM-assisted data cleaning — batch classification, normalization, and matching.

Uses local Ollama (Qwen3) for tasks that rule-based approaches cannot handle:
1. Person vs Organization classification
2. Name normalization (parenthetical removal, multi-person splitting)
3. Entity resolution candidates (similarity 0.85-0.95 range)

All operations cache results in the SQLite database (llm_decisions table)
to avoid redundant LLM calls.
Graceful degradation: returns empty results if Ollama unavailable.
"""

import json
import os
import re
import sqlite3
from dataclasses import dataclass, field

import httpx
import structlog

from src.models import Person
from src.utils.config import (
    LLM_BASE_URL,
    LLM_BATCH_SIZE,
    LLM_MAX_TOKENS,
    LLM_MODEL_NAME,
    LLM_TEMPERATURE,
    LLM_TIMEOUT,
)

logger = structlog.get_logger()

# Task identifiers for llm_decisions table
_TASK_ORG = "org_classification"
_TASK_NORM = "name_normalization"


def is_llm_enabled() -> bool:
    """Check if LLM operations are enabled.

    Set ANIMETOR_LLM=1 to enable LLM calls in the pipeline.
    Disabled by default to avoid slow/unexpected API calls in tests.
    """
    return os.environ.get("ANIMETOR_LLM", "0") == "1"


# ---------------------------------------------------------------------------
# LLM call infrastructure
# ---------------------------------------------------------------------------


def _ollama_base() -> str:
    return LLM_BASE_URL.replace("/v1", "")


def check_llm_available() -> bool:
    """Check if Ollama endpoint is reachable."""
    try:
        r = httpx.get(f"{_ollama_base()}/api/tags", timeout=5.0)
        return r.status_code == 200
    except Exception:
        return False


def _call_llm(prompt: str, max_tokens: int | None = None) -> str:
    """Single LLM call. Returns response text or empty string on error.

    For structured output, prepends /no_think to disable Qwen3's thinking mode
    and get direct JSON responses.
    """
    try:
        r = httpx.post(
            f"{_ollama_base()}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "think": False,  # Disable Qwen3 thinking mode for structured output
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": max_tokens or LLM_MAX_TOKENS,
                },
            },
            timeout=LLM_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        # Qwen3 may put content in "response" or "thinking" field
        response = data.get("response", "").strip()
        thinking = data.get("thinking", "").strip()
        # Prefer response field; fall back to thinking if response is empty
        return response or thinking or ""
    except Exception as e:
        logger.warning("llm_call_failed", error=str(e))
        return ""


def _extract_json_array(text: str) -> list[dict] | None:
    """Extract a JSON array from LLM output (handles markdown fences)."""
    # Strip markdown code fences
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = re.sub(r"```", "", text)
    # Find JSON array
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        return None
    try:
        result = json.loads(match.group())
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass
    return None


# ---------------------------------------------------------------------------
# DB cache helpers
# ---------------------------------------------------------------------------


def _load_db_cache(conn: sqlite3.Connection, task: str) -> dict[str, dict]:
    """Load all cached decisions for a task from DB."""
    from src.database import get_all_llm_decisions

    try:
        return get_all_llm_decisions(conn, task)
    except Exception:
        # Table may not exist yet (pre-migration)
        return {}


def _save_db_decision(
    conn: sqlite3.Connection, name: str, task: str, result: dict
) -> None:
    """Save a single decision to DB."""
    from src.database import upsert_llm_decision

    upsert_llm_decision(conn, name, task, result, model=LLM_MODEL_NAME)


# ---------------------------------------------------------------------------
# Task 1: Person vs Organization classification
# ---------------------------------------------------------------------------

_ORG_CLASSIFICATION_PROMPT = """以下の名前リストについて、それぞれ「個人名(person)」か「組織名(org)」か判定してください。

判定基準:
- アニメスタジオ、制作会社、ポストプロダクション会社 → org
- 声優事務所、音響スタジオ → org
- 個人のアニメーター、監督、脚本家 → person
- 芸名・ペンネーム（個人）→ person
- 不明な場合は person（保守的に）

JSON配列で回答してください。他の文章は不要です:
[{{"name": "...", "type": "person" or "org"}}]

名前リスト:
"""


@dataclass
class OrgClassificationResult:
    """Result of batch person/org classification."""

    org_ids: set[str] = field(default_factory=set)
    person_ids: set[str] = field(default_factory=set)
    total_classified: int = 0
    from_cache: int = 0
    from_llm: int = 0
    from_studio_db: int = 0


def classify_person_or_org(
    persons: list[Person],
    studio_names: set[str] | None = None,
    conn: sqlite3.Connection | None = None,
) -> OrgClassificationResult:
    """Classify ambiguous person entries as individual or organization.

    Strategy:
    1. Cross-reference with known studio names (instant, no LLM needed)
    2. Check DB cache for previous decisions
    3. Send remaining ambiguous names to LLM in batches

    Only processes names that are ambiguous (no hiragana, potentially org-like).

    Args:
        persons: All person entries to check
        studio_names: Known studio/company names from DB
        conn: Database connection for caching (optional)

    Returns:
        OrgClassificationResult with org_ids and person_ids
    """
    result = OrgClassificationResult()
    org_cache: dict[str, dict] = {}
    if conn is not None:
        org_cache = _load_db_cache(conn, _TASK_ORG)

    hiragana_re = re.compile(r"[\u3041-\u3096]")

    if not is_llm_enabled():
        # Still use studio DB cross-reference (no LLM needed)
        studio_names = studio_names or set()
        for p in persons:
            name = p.name_ja or p.name_en or ""
            if name and name in studio_names:
                result.org_ids.add(p.id)
                result.from_studio_db += 1
        if result.org_ids:
            logger.info("org_classification_studio_db_only", count=len(result.org_ids))
        return result

    studio_names = studio_names or set()

    # Identify ambiguous candidates
    candidates: list[Person] = []
    for p in persons:
        name = p.name_ja or p.name_en or ""
        if not name:
            continue

        # Names with hiragana are almost always persons
        if hiragana_re.search(name):
            result.person_ids.add(p.id)
            continue

        # Check studio DB first
        if name in studio_names:
            result.org_ids.add(p.id)
            result.from_studio_db += 1
            continue

        # Check DB cache
        if name in org_cache:
            classification = org_cache[name].get("type", "person")
            if classification == "org":
                result.org_ids.add(p.id)
            else:
                result.person_ids.add(p.id)
            result.from_cache += 1
            continue

        candidates.append(p)

    if not candidates:
        result.total_classified = result.from_cache + result.from_studio_db
        logger.info(
            "org_classification_no_llm_needed",
            from_cache=result.from_cache,
            from_studio_db=result.from_studio_db,
        )
        return result

    # Check LLM availability
    if not check_llm_available():
        logger.warning("org_classification_skipped", reason="llm_not_available")
        # Default: treat all as persons (conservative)
        for p in candidates:
            result.person_ids.add(p.id)
        return result

    # Batch LLM classification
    logger.info("org_classification_start", candidates=len(candidates))

    for batch_start in range(0, len(candidates), LLM_BATCH_SIZE):
        batch = candidates[batch_start : batch_start + LLM_BATCH_SIZE]
        name_list = "\n".join(
            f"{i + 1}. {p.name_ja or p.name_en}" for i, p in enumerate(batch)
        )
        prompt = _ORG_CLASSIFICATION_PROMPT + name_list

        response = _call_llm(prompt, max_tokens=LLM_MAX_TOKENS * 2)
        items = _extract_json_array(response)

        if not items:
            logger.warning(
                "org_classification_parse_failed",
                batch_start=batch_start,
                response_snippet=response[:200],
            )
            # Default to person for failed batches
            for p in batch:
                result.person_ids.add(p.id)
            continue

        # Map responses back to persons
        name_to_persons: dict[str, list[Person]] = {}
        for p in batch:
            name = p.name_ja or p.name_en or ""
            name_to_persons.setdefault(name, []).append(p)

        for item in items:
            name = item.get("name", "")
            classification = item.get("type", "person")

            # Save to DB cache
            if conn is not None:
                _save_db_decision(conn, name, _TASK_ORG, {"type": classification})

            for p in name_to_persons.get(name, []):
                if classification == "org":
                    result.org_ids.add(p.id)
                else:
                    result.person_ids.add(p.id)
                result.from_llm += 1

        # Handle any names not returned by LLM
        classified_names = {item.get("name") for item in items}
        for p in batch:
            name = p.name_ja or p.name_en or ""
            if name not in classified_names:
                result.person_ids.add(p.id)

        logger.debug(
            "org_classification_batch",
            batch=batch_start,
            classified=len(items),
        )

    # Commit batch of decisions
    if conn is not None:
        conn.commit()

    result.total_classified = (
        result.from_cache + result.from_llm + result.from_studio_db
    )
    logger.info(
        "org_classification_complete",
        orgs=len(result.org_ids),
        persons=len(result.person_ids),
        from_cache=result.from_cache,
        from_llm=result.from_llm,
        from_studio_db=result.from_studio_db,
    )

    return result


# ---------------------------------------------------------------------------
# Task 2: Name normalization
# ---------------------------------------------------------------------------

_NAME_NORM_PROMPT = """以下のアニメクレジット名を正規化してください。

ルール:
- 括弧内の話数情報(例: "(1~15話)")は分離してepisode_infoに
- 括弧内の所属情報(例: "(フジテレビ)")は除去し、人物名のみ残す
- 複数人が含まれる場合(例: "高畑勲、宮崎駿")は分割
- "(株)"等の法人格表記は組織名として is_org=true
- 元の名前がそのまま正しければ names に元の名前を1つだけ入れる

JSON配列で回答してください。他の文章は不要です:
[{{"original": "...", "names": ["正規化名1", ...], "episode_info": "..." or null, "is_org": true/false}}]

クレジット名:
"""


@dataclass
class NameNormResult:
    """Single name normalization result."""

    original: str
    names: list[str]
    episode_info: str | None = None
    is_org: bool = False


def normalize_names(
    persons: list[Person],
    conn: sqlite3.Connection | None = None,
) -> list[NameNormResult]:
    """Normalize person names using LLM for ambiguous cases.

    Detects and processes names with:
    - Parenthetical annotations (episode info, affiliations)
    - Multiple persons in one entry
    - Organization prefixes

    Args:
        persons: Person entries to normalize
        conn: Database connection for caching (optional)

    Returns:
        List of NameNormResult for entries that need changes
    """
    paren_re = re.compile(r"[（(「]")
    slash_re = re.compile(r"\s*/\s*")
    multi_re = re.compile(
        r"[、，,]\s*(?=[^\d])"
    )  # comma not before digits (episode lists)

    # Identify candidates needing normalization
    candidates: list[Person] = []
    for p in persons:
        name = p.name_ja or ""
        if not name:
            continue
        if paren_re.search(name) or slash_re.search(name):
            candidates.append(p)
        elif multi_re.search(name) and len(name) > 10:
            candidates.append(p)

    if not candidates:
        return []

    if not is_llm_enabled():
        return []

    norm_cache: dict[str, dict] = {}
    if conn is not None:
        norm_cache = _load_db_cache(conn, _TASK_NORM)

    results: list[NameNormResult] = []
    uncached: list[Person] = []

    for p in candidates:
        name = p.name_ja or p.name_en or ""
        if name in norm_cache:
            cached = norm_cache[name]
            results.append(
                NameNormResult(
                    original=name,
                    names=cached["names"],
                    episode_info=cached.get("episode_info"),
                    is_org=cached.get("is_org", False),
                )
            )
        else:
            uncached.append(p)

    if not uncached:
        logger.info("name_normalization_all_cached", count=len(results))
        return results

    if not check_llm_available():
        logger.warning("name_normalization_skipped", reason="llm_not_available")
        return results

    logger.info("name_normalization_start", candidates=len(uncached))

    for batch_start in range(0, len(uncached), LLM_BATCH_SIZE):
        batch = uncached[batch_start : batch_start + LLM_BATCH_SIZE]
        name_list = "\n".join(
            f"{i + 1}. {p.name_ja or p.name_en}" for i, p in enumerate(batch)
        )
        prompt = _NAME_NORM_PROMPT + name_list

        response = _call_llm(prompt, max_tokens=LLM_MAX_TOKENS * 3)
        items = _extract_json_array(response)

        if not items:
            logger.warning(
                "name_normalization_parse_failed",
                batch_start=batch_start,
                response_snippet=response[:200],
            )
            continue

        for item in items:
            original = item.get("original", "")
            names = item.get("names", [])
            episode_info = item.get("episode_info")
            is_org = item.get("is_org", False)

            if not original or not names:
                continue

            # Validate: names should be non-empty strings
            names = [n.strip() for n in names if isinstance(n, str) and n.strip()]
            if not names:
                continue

            decision = {
                "names": names,
                "episode_info": episode_info,
                "is_org": is_org,
            }
            if conn is not None:
                _save_db_decision(conn, original, _TASK_NORM, decision)

            results.append(
                NameNormResult(
                    original=original,
                    names=names,
                    episode_info=episode_info,
                    is_org=is_org,
                )
            )

        logger.debug("name_normalization_batch", batch=batch_start, results=len(items))

    # Commit batch of decisions
    if conn is not None:
        conn.commit()

    logger.info(
        "name_normalization_complete",
        total=len(results),
        multi_person=sum(1 for r in results if len(r.names) > 1),
        orgs_detected=sum(1 for r in results if r.is_org),
    )

    return results


# ---------------------------------------------------------------------------
# Task 3: Entity resolution — candidate generation for AI matching
# ---------------------------------------------------------------------------


def find_ai_match_candidates(
    persons: list[Person],
    already_matched: set[str],
    similarity_threshold_low: float = 0.85,
    similarity_threshold_high: float = 0.95,
    max_candidates: int = 500,
) -> list[tuple[Person, Person, float]]:
    """Find candidate pairs for AI-assisted entity resolution.

    These are pairs with similarity in [0.85, 0.95) — too low for automatic
    matching but high enough to warrant LLM verification.

    Args:
        persons: Remaining unmatched persons
        already_matched: Set of person_ids already resolved
        similarity_threshold_low: Minimum similarity for candidates
        similarity_threshold_high: Maximum similarity (above this, already matched)
        max_candidates: Maximum number of pairs to return

    Returns:
        List of (person1, person2, similarity) tuples
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning("rapidfuzz_not_available", msg="skipping AI match candidates")
        return []

    # Filter to unmatched persons with name_ja
    remaining = [p for p in persons if p.id not in already_matched and p.name_ja]

    if len(remaining) < 2:
        return []

    # Group by first character for blocking (same as similarity_based_cluster)
    from collections import defaultdict

    blocks: dict[str, list[Person]] = defaultdict(list)
    for p in remaining:
        first_char = p.name_ja[0] if p.name_ja else ""
        if first_char:
            blocks[first_char].append(p)

    candidates: list[tuple[Person, Person, float]] = []

    for block in blocks.values():
        if len(block) < 2:
            continue
        for i in range(len(block)):
            if len(candidates) >= max_candidates:
                break
            for j in range(i + 1, len(block)):
                name_i = block[i].name_ja or ""
                name_j = block[j].name_ja or ""
                if abs(len(name_i) - len(name_j)) > 3:
                    continue

                sim = fuzz.ratio(name_i, name_j) / 100.0
                if similarity_threshold_low <= sim < similarity_threshold_high:
                    candidates.append((block[i], block[j], sim))

    # Sort by similarity descending (most likely matches first)
    candidates.sort(key=lambda x: -x[2])
    return candidates[:max_candidates]

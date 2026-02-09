"""AI-assisted entity resolution using local LLM (vLLM + Qwen).

LLMを使用して、ルールベースでは判定困難な名寄せケースを処理する。
- 漢字の読み違い
- 表記ゆれ（旧字体・新字体、異体字）
- 跨言語の名前マッチング

法的要件: false positive を避けるため、LLM の回答も保守的に解釈する。
"""

import os
from dataclasses import dataclass

import structlog
from openai import OpenAI
from openai import OpenAIError

from src.models import Person
from src.utils.config import (
    LLM_BASE_URL,
    LLM_MAX_TOKENS,
    LLM_MODEL_NAME,
    LLM_TEMPERATURE,
    LLM_TIMEOUT,
)

logger = structlog.get_logger()


@dataclass
class NameMatchDecision:
    """LLMによる名前マッチング判定結果."""

    is_match: bool
    confidence: float  # 0.0-1.0
    reasoning: str


# Few-shot examples for the LLM
FEW_SHOT_EXAMPLES = """# Examples:

Person A: 渡辺信一郎 (Watanabe Shinichiro)
Person B: 渡邊信一郎 (Watanabe Shinichiro)
Decision: SAME
Reason: Same person - 渡辺 and 渡邊 are variant kanji forms

Person A: 宮崎駿 (Miyazaki Hayao)
Person B: 宮崎駿 (Hayao Miyazaki)
Decision: SAME
Reason: Same person - name order difference only

Person A: 佐藤大 (Satou Dai)
Person B: 佐藤大輔 (Satou Daisuke)
Decision: DIFFERENT
Reason: Different people - 大 vs 大輔 are different names

Person A: 田中宏 (Tanaka Hiroshi)
Person B: 田中博 (Tanaka Hiroshi)
Decision: UNCERTAIN
Reason: Same reading but different kanji - likely different people

Person A: 山田太郎 (Yamada Tarou)
Person B: 山田太朗 (Yamada Tarou)
Decision: UNCERTAIN
Reason: Same reading, variant kanji (郎/朗) - could be same or different"""


def _build_prompt(person1: Person, person2: Person) -> str:
    """Build prompt for LLM name matching."""
    p1_names = f"{person1.name_ja or ''} ({person1.name_en or ''})".strip()
    p2_names = f"{person2.name_ja or ''} ({person2.name_en or ''})".strip()

    if person1.aliases:
        p1_names += f" [aliases: {', '.join(person1.aliases)}]"
    if person2.aliases:
        p2_names += f" [aliases: {', '.join(person2.aliases)}]"

    return f"""{FEW_SHOT_EXAMPLES}

# Now judge this pair:

Person A: {p1_names}
Person B: {p2_names}
Decision: """


def check_llm_available() -> bool:
    """Check if LLM endpoint is available.

    Returns:
        True if LLM is accessible, False otherwise
    """
    try:
        client = OpenAI(
            base_url=LLM_BASE_URL,
            api_key=os.getenv("OPENAI_API_KEY", "dummy-key"),  # vLLM doesn't require real key
            timeout=LLM_TIMEOUT,
        )
        # Try a minimal request
        client.models.list()
        return True
    except Exception as e:
        logger.info("llm_not_available", error=str(e))
        return False


def ask_llm_if_same_person(person1: Person, person2: Person) -> NameMatchDecision:
    """Ask LLM if two persons are the same.

    Args:
        person1: First person
        person2: Second person

    Returns:
        NameMatchDecision with is_match, confidence, and reasoning

    Raises:
        OpenAIError: If LLM API call fails
    """
    client = OpenAI(
        base_url=LLM_BASE_URL,
        api_key=os.getenv("OPENAI_API_KEY", "dummy-key"),
        timeout=LLM_TIMEOUT,
    )

    prompt = _build_prompt(person1, person2)

    try:
        response = client.chat.completions.create(
            model=LLM_MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at Japanese names and anime industry credits. "
                    "Judge if two person records refer to the same individual. "
                    "Answer SAME, DIFFERENT, or UNCERTAIN followed by your reasoning. "
                    "Be conservative - if unsure, answer UNCERTAIN.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS,
        )

        answer = response.choices[0].message.content.strip()
        logger.debug("llm_response", person1=person1.id, person2=person2.id, answer=answer)

        # Parse response
        answer_upper = answer.upper()
        if answer_upper.startswith("SAME"):
            is_match = True
            confidence = 0.85  # High but not perfect (LLM can be wrong)
        elif answer_upper.startswith("DIFFERENT"):
            is_match = False
            confidence = 0.9
        else:  # UNCERTAIN or unparseable
            is_match = False
            confidence = 0.5

        return NameMatchDecision(is_match=is_match, confidence=confidence, reasoning=answer)

    except OpenAIError as e:
        logger.error("llm_api_error", error=str(e), person1=person1.id, person2=person2.id)
        raise


def ai_assisted_cluster(
    persons: list[Person],
    min_confidence: float = 0.8,
    same_source_only: bool = True,
) -> dict[str, str]:
    """AI-assisted entity resolution using local LLM.

    Args:
        persons: List of Person objects to match
        min_confidence: Minimum confidence threshold (0.0-1.0)
        same_source_only: If True, only match within same source (conservative)

    Returns:
        {person_id: canonical_id} mapping

    Notes:
        - Requires vLLM server running at LLM_BASE_URL
        - If LLM is unavailable, returns empty dict (graceful degradation)
        - Conservative: only accepts SAME with high confidence
        - O(n²) complexity - use sparingly on small candidate sets
    """
    if not check_llm_available():
        logger.warning("ai_entity_resolution_skipped", reason="llm_not_available")
        return {}

    if min_confidence > 1.0 or min_confidence < 0.5:
        logger.warning("invalid_min_confidence", value=min_confidence, using=0.8)
        min_confidence = 0.8

    # Group by source if same_source_only
    if same_source_only:
        from collections import defaultdict

        persons_by_source: dict[str, list[Person]] = defaultdict(list)
        for p in persons:
            source = p.id.split(":")[0] if ":" in p.id else "unknown"
            persons_by_source[source].append(p)
        person_groups = list(persons_by_source.values())
    else:
        person_groups = [persons]

    canonical_map: dict[str, str] = {}

    for group in person_groups:
        if len(group) < 2:
            continue

        # Track all persons involved in merges (both canonical and mapped)
        merged_persons: set[str] = set()

        # Compare all pairs in this group
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                p1, p2 = group[i], group[j]

                # Skip if either person is already involved in a merge
                if p1.id in merged_persons or p2.id in merged_persons:
                    continue

                try:
                    decision = ask_llm_if_same_person(p1, p2)

                    if decision.is_match and decision.confidence >= min_confidence:
                        canonical_map[p2.id] = p1.id
                        merged_persons.add(p1.id)
                        merged_persons.add(p2.id)
                        logger.info(
                            "entity_merged",
                            source=p2.id,
                            canonical=p1.id,
                            strategy="ai_assisted",
                            confidence=f"{decision.confidence:.2f}",
                            reasoning=decision.reasoning[:100],
                        )
                except OpenAIError as e:
                    logger.error("ai_match_failed", person1=p1.id, person2=p2.id, error=str(e))
                    continue

    return canonical_map

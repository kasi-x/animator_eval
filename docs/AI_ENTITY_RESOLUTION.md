# AI-Assisted Entity Resolution

AI-assisted entity resolution uses a local LLM (Qwen via vLLM) to handle challenging name matching cases that rule-based systems struggle with.

## Use Cases

- **Kanji variants**: 渡辺 vs 渡邊 (variant forms)
- **Reading ambiguity**: 田中宏 vs 田中博 (same reading, different kanji)
- **Name order**: "Miyazaki Hayao" vs "Hayao Miyazaki"
- **Old/new kanji**: 國 vs 国, 齋藤 vs 斎藤

## Setup

### 1. Install vLLM

```bash
pip install vllm
```

### 2. Download a model (e.g., Qwen2.5-7B-Instruct)

```bash
# Using HuggingFace
huggingface-cli download Qwen/Qwen2.5-7B-Instruct
```

### 3. Start vLLM server

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --dtype auto \
  --max-model-len 4096
```

The server will be available at `http://localhost:8000/v1` (OpenAI-compatible API).

### 4. Configure endpoint (optional)

Edit `src/utils/config.py` if using a different endpoint:

```python
LLM_BASE_URL = "http://localhost:8000/v1"
LLM_MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
```

## Usage

### Python API

```python
from src.analysis.ai_entity_resolution import ai_assisted_cluster, check_llm_available
from src.models import Person

# Check if LLM is available
if check_llm_available():
    persons = [
        Person(id="mal:1", name_ja="渡辺信一郎"),
        Person(id="mal:2", name_ja="渡邊信一郎"),  # variant kanji
    ]

    # Run AI-assisted matching
    result = ai_assisted_cluster(
        persons,
        min_confidence=0.8,      # require 80% confidence
        same_source_only=True,   # only match within same source (conservative)
    )

    print(result)  # {'mal:2': 'mal:1'}
else:
    print("LLM not available, skipping AI-assisted resolution")
```

### Integration with pipeline

The AI-assisted resolution is not automatically integrated into the main pipeline to avoid external dependencies. To use it:

```python
from src.analysis.entity_resolution import resolve_all
from src.analysis.ai_entity_resolution import ai_assisted_cluster

# Standard resolution (exact, cross-source, romaji, similarity)
standard_matches = resolve_all(persons)

# AI-assisted resolution on remaining persons
matched_ids = set(standard_matches.keys()) | set(standard_matches.values())
remaining = [p for p in persons if p.id not in matched_ids]

ai_matches = ai_assisted_cluster(remaining, min_confidence=0.8)

# Merge results
all_matches = {**standard_matches, **ai_matches}
```

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_confidence` | 0.8 | Minimum confidence threshold (0.0-1.0) |
| `same_source_only` | True | Only match within same source (MAL↔MAL, AniList↔AniList) |
| `LLM_BASE_URL` | `http://localhost:8000/v1` | vLLM API endpoint |
| `LLM_MODEL_NAME` | `Qwen/Qwen2.5-7B-Instruct` | Model name |
| `LLM_TEMPERATURE` | 0.1 | Temperature (lower = more deterministic) |
| `LLM_MAX_TOKENS` | 50 | Max tokens in response |
| `LLM_TIMEOUT` | 10.0 | API timeout in seconds |

## How It Works

### 1. Few-shot prompting

The LLM is given examples of name matching decisions:

```
Person A: 渡辺信一郎 (Watanabe Shinichiro)
Person B: 渡邊信一郎 (Watanabe Shinichiro)
Decision: SAME
Reason: Variant kanji forms

Person A: 佐藤大 (Satou Dai)
Person B: 佐藤大輔 (Satou Daisuke)
Decision: DIFFERENT
Reason: Different names
```

### 2. Conservative decisions

The LLM can respond:
- **SAME**: High confidence match (confidence=0.85)
- **DIFFERENT**: Not the same person (confidence=0.9)
- **UNCERTAIN**: Not enough information (confidence=0.5, treated as no match)

Only **SAME** responses with `confidence >= min_confidence` result in a merge.

### 3. Legal compliance

- **False positive avoidance**: Requires high confidence (0.8+ default)
- **Same-source only**: By default, only matches within same data source
- **Graceful degradation**: If LLM unavailable, returns empty dict (no errors)
- **1-to-1 matching**: Each person can only be matched once

## Performance

- **Time complexity**: O(n²) within each source group
- **API calls**: n*(n-1)/2 per source (all pairs)
- **Typical latency**: 50-200ms per API call (depends on model size)

**Recommendation**: Use AI-assisted resolution on small candidate sets (< 100 persons) after applying rule-based filters.

## Testing

Tests use mocking to avoid requiring a running LLM:

```bash
pixi run pytest tests/test_ai_entity_resolution.py -v
```

All 16 tests should pass without a running vLLM server.

## Troubleshooting

### "LLM not available" warning

1. Check vLLM server is running: `curl http://localhost:8000/v1/models`
2. Check `LLM_BASE_URL` in config
3. Check firewall/network settings

### Low accuracy

1. Increase `min_confidence` (0.85-0.9)
2. Use a larger model (e.g., 14B instead of 7B)
3. Add more examples to `FEW_SHOT_EXAMPLES`
4. Lower `LLM_TEMPERATURE` for more deterministic output

### Slow performance

1. Use GPU acceleration for vLLM
2. Reduce candidate set size before AI matching
3. Use batch processing (not yet implemented)
4. Use smaller model (trade-off with accuracy)

## Example: Full Resolution Pipeline

```python
from src.analysis.entity_resolution import (
    exact_match_cluster,
    cross_source_match,
    romaji_match,
    similarity_based_cluster,
)
from src.analysis.ai_entity_resolution import ai_assisted_cluster, check_llm_available

def resolve_with_ai(persons: list[Person]) -> dict[str, str]:
    """5-step entity resolution with AI assistance."""

    # Step 1: Exact match
    exact = exact_match_cluster(persons)

    # Step 2: Cross-source match
    cross = cross_source_match(persons)

    # Step 3: Romaji match
    matched = set(exact) | set(cross)
    remaining = [p for p in persons if p.id not in matched]
    romaji = romaji_match(remaining)

    # Step 4: Similarity-based (Jaro-Winkler)
    matched = matched | set(romaji)
    remaining = [p for p in persons if p.id not in matched]
    similarity = similarity_based_cluster(remaining, threshold=0.95)

    # Step 5: AI-assisted (if available)
    ai_matches = {}
    if check_llm_available():
        matched = matched | set(similarity)
        remaining = [p for p in persons if p.id not in matched]
        ai_matches = ai_assisted_cluster(remaining, min_confidence=0.85)

    # Merge all results
    return {**exact, **cross, **romaji, **similarity, **ai_matches}
```

## Alternative Models

While Qwen2.5-7B-Instruct is recommended, other models can work:

- **Qwen2.5-14B-Instruct**: Better accuracy, slower
- **Qwen2.5-3B-Instruct**: Faster, lower accuracy
- **Other OpenAI-compatible models**: Adjust `LLM_MODEL_NAME`

Japanese-specific models or models trained on names may perform better.

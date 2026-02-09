# LLM Setup for AI Entity Resolution

## Overview

The AI entity resolution feature requires an LLM capable of **reasoning about Japanese names** and detecting same-person matches. This requires a general-purpose instruction-following model, not a translation-specific model.

## Your Existing Setup

You have **PLaMo-2-translate** (10B, Mamba) which is excellent for translation but designed specifically for EN↔JA translation, not for reasoning tasks like entity resolution.

## Recommended Models for Entity Resolution

### Option 1: Qwen2.5-7B-Instruct (Recommended)
- **Model**: `Qwen/Qwen2.5-7B-Instruct`
- **Size**: 7B parameters (~14GB VRAM in bfloat16)
- **Strengths**: Strong Japanese support, good reasoning, handles kanji variants well
- **Setup**:

```bash
# Download model
huggingface-cli download Qwen/Qwen2.5-7B-Instruct

# Start vLLM server
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --host 0.0.0.0 \
  --port 8001 \
  --dtype bfloat16 \
  --max-model-len 4096 \
  --gpu-memory-utilization 0.9
```

### Option 2: Qwen2.5-3B-Instruct (Faster, less accurate)
- **Model**: `Qwen/Qwen2.5-3B-Instruct`
- **Size**: 3B parameters (~6GB VRAM)
- **Tradeoff**: Faster but less accurate for complex cases

```bash
vllm serve Qwen/Qwen2.5-3B-Instruct \
  --port 8001 \
  --dtype bfloat16 \
  --max-model-len 4096
```

### Option 3: Swallow-7B-Instruct (Japanese-specific)
- **Model**: `tokyotech-llm/Swallow-7b-instruct-v0.1`
- **Size**: 7B parameters
- **Strengths**: Strong Japanese focus, understands cultural context

### Option 4: Use PLaMo (Not Ideal)
You can try using PLaMo-2-translate, but you'll need to adapt the prompts:

```python
# Update config.py
LLM_BASE_URL = "http://localhost:8000/v1"  # Your PLaMo server
LLM_MODEL_NAME = "pfnet/plamo-2-translate"
```

However, PLaMo is optimized for translation, not reasoning, so accuracy may be lower.

## Configuration

### 1. Update `src/utils/config.py`

```python
# AI-assisted entity resolution
LLM_BASE_URL = "http://localhost:8001/v1"  # Separate from PLaMo
LLM_MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"
LLM_TEMPERATURE = 0.1
LLM_MAX_TOKENS = 50
LLM_TIMEOUT = 10.0
```

### 2. Start vLLM server (different port from PLaMo)

```bash
# Terminal 1: Your PLaMo server (port 8000)
cd /home/user/dev/shinar/nlp
python plamo_translate_server_v2.py

# Terminal 2: Qwen for entity resolution (port 8001)
vllm serve Qwen/Qwen2.5-7B-Instruct --port 8001
```

### 3. Test the setup

```bash
cd /home/user/dev/animetor_eval

# Test connection
python -c "
from src.analysis.ai_entity_resolution import check_llm_available
print('LLM available:', check_llm_available())
"

# Test entity resolution
python -c "
from src.analysis.ai_entity_resolution import ai_assisted_cluster
from src.models import Person

persons = [
    Person(id='mal:1', name_ja='渡辺信一郎'),
    Person(id='mal:2', name_ja='渡邊信一郎'),  # Variant kanji
]

result = ai_assisted_cluster(persons, min_confidence=0.8)
print('Result:', result)
"
```

## Multi-Model Setup

You can run both models simultaneously (if you have enough VRAM):

| Model | Purpose | Port | VRAM |
|-------|---------|------|------|
| PLaMo-2-translate | EN↔JA translation | 8000 | ~20GB |
| Qwen2.5-7B-Instruct | Entity resolution | 8001 | ~14GB |

**Total VRAM needed**: ~34GB (requires 2x A100 or H100, or split across GPUs)

If VRAM is limited, consider:
- Use Qwen2.5-3B-Instruct (6GB) instead of 7B
- Run models sequentially (stop/start as needed)
- Use 4-bit quantization: `--quantization awq` or `--load-format bitsandbytes`

## Integration with Animetor Eval

### Standalone usage:

```python
from src.analysis.entity_resolution import resolve_all
from src.analysis.ai_entity_resolution import ai_assisted_cluster, check_llm_available

# Standard 4-step resolution
standard_matches = resolve_all(persons)

# Add AI-assisted as 5th step
if check_llm_available():
    matched_ids = set(standard_matches.keys()) | set(standard_matches.values())
    remaining = [p for p in persons if p.id not in matched_ids]

    ai_matches = ai_assisted_cluster(
        remaining,
        min_confidence=0.85,
        same_source_only=True
    )

    all_matches = {**standard_matches, **ai_matches}
else:
    print("LLM not available, using standard resolution only")
    all_matches = standard_matches
```

### Pipeline integration (optional):

Edit `src/analysis/entity_resolution.py` to add optional AI step:

```python
def resolve_all(persons: list[Person], use_ai: bool = False) -> dict[str, str]:
    # ... existing steps ...

    if use_ai:
        from src.analysis.ai_entity_resolution import ai_assisted_cluster, check_llm_available

        if check_llm_available():
            already_matched = already_matched | set(similarity)
            remaining = [p for p in persons if p.id not in already_matched]
            ai = ai_assisted_cluster(remaining, min_confidence=0.85)
            merged = {**exact, **cross, **romaji, **similarity, **ai}
        else:
            logger.warning("ai_entity_resolution_requested_but_unavailable")

    return merged
```

## Troubleshooting

### "Connection refused" error
```bash
# Check if server is running
curl http://localhost:8001/v1/models

# Check logs
# vLLM logs will show in the terminal where you started the server
```

### CUDA out of memory
```bash
# Use smaller model
vllm serve Qwen/Qwen2.5-3B-Instruct --port 8001

# Or reduce GPU memory utilization
vllm serve Qwen/Qwen2.5-7B-Instruct --gpu-memory-utilization 0.7 --port 8001

# Or use CPU (very slow)
vllm serve Qwen/Qwen2.5-7B-Instruct --device cpu --port 8001
```

### Wrong model being called
```bash
# Check which models are available
curl http://localhost:8001/v1/models

# Verify config
python -c "from src.utils.config import LLM_BASE_URL, LLM_MODEL_NAME; print(f'{LLM_BASE_URL} / {LLM_MODEL_NAME}')"
```

## Performance Notes

- **First request**: Slow (~5-10s) due to model warm-up
- **Subsequent requests**: Fast (~50-200ms per comparison)
- **Batch processing**: O(n²) comparisons - use sparingly
- **Recommended**: Apply AI resolution only to ambiguous cases after rule-based filtering

## Alternative: OpenAI API

If you don't want to run a local model:

```python
# Update config.py
LLM_BASE_URL = "https://api.openai.com/v1"
LLM_MODEL_NAME = "gpt-4o-mini"  # or gpt-4o

# Set API key
export OPENAI_API_KEY="sk-..."
```

Cost: ~$0.10-0.15 per 1000 entity resolution decisions (GPT-4o-mini)

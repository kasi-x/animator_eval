# Ollama Quick Start for AI Entity Resolution

## Your Setup

You already have **Qwen3** models installed via Ollama:
- `qwen3:32b` (20GB) - Best accuracy
- `qwen3:8b` (5.2GB) - Good balance (default)
- `qwen2.5:72b` (47GB) - Highest accuracy (if VRAM available)

## Configuration

Already configured in `src/utils/config.py`:
```python
LLM_BASE_URL = "http://localhost:11434/v1"  # Ollama default port
LLM_MODEL_NAME = "qwen3:8b"  # or qwen3:32b
```

## Usage

### 1. Start Ollama (if not running)

```bash
# Check if Ollama is running
curl http://localhost:11434/v1/models

# If not running, start it
ollama serve
```

### 2. Test AI Entity Resolution

```bash
cd /home/user/dev/animetor_eval

# Test connection
python -c "
from src.analysis.ai_entity_resolution import check_llm_available
print('✓ LLM available' if check_llm_available() else '✗ LLM not available')
"

# Test entity resolution with kanji variants
python -c "
from src.analysis.ai_entity_resolution import ai_assisted_cluster
from src.models import Person

persons = [
    Person(id='mal:1', name_ja='渡辺信一郎'),
    Person(id='mal:2', name_ja='渡邊信一郎'),  # Variant kanji (辺/邊)
]

result = ai_assisted_cluster(persons, min_confidence=0.8)
print('Result:', result)
print('✓ Match found!' if result else '✗ No match')
"
```

### 3. Use in Pipeline

```python
from src.analysis.entity_resolution import resolve_all
from src.analysis.ai_entity_resolution import ai_assisted_cluster, check_llm_available
from src.models import Person

# Your persons list
persons = [...]  # Load from database

# Standard 4-step resolution
standard_matches = resolve_all(persons)

# Add AI-assisted as 5th step (if Ollama is running)
if check_llm_available():
    matched_ids = set(standard_matches.keys()) | set(standard_matches.values())
    remaining = [p for p in persons if p.id not in matched_ids]

    ai_matches = ai_assisted_cluster(
        remaining,
        min_confidence=0.85,  # High confidence for legal safety
        same_source_only=True  # Conservative matching
    )

    all_matches = {**standard_matches, **ai_matches}
    print(f"Total matches: {len(all_matches)} (AI: {len(ai_matches)})")
else:
    print("⚠ Ollama not available, using standard resolution only")
    all_matches = standard_matches
```

## Model Comparison

| Model | VRAM | Speed | Accuracy | Best For |
|-------|------|-------|----------|----------|
| qwen3:8b | 5GB | Fast (~100ms) | Good | Quick matching |
| qwen3:32b | 20GB | Medium (~300ms) | Better | Production use |
| qwen2.5:72b | 47GB | Slow (~1s) | Best | High-stakes matching |

## Switch Models

```bash
# Edit src/utils/config.py
LLM_MODEL_NAME = "qwen3:32b"  # Use 32B for better accuracy
```

Or set environment variable:
```bash
export ANIMETOR_LLM_MODEL="qwen3:32b"
```

## Example Use Cases

### Case 1: Kanji Variants
```python
Person A: 渡辺信一郎 (Watanabe Shinichiro)
Person B: 渡邊信一郎 (Watanabe Shinichiro)
→ AI Decision: SAME (variant kanji: 辺/邊)
```

### Case 2: Reading Ambiguity
```python
Person A: 田中宏 (Tanaka Hiroshi)
Person B: 田中博 (Tanaka Hiroshi)
→ AI Decision: UNCERTAIN (same reading, different kanji → likely different people)
```

### Case 3: Name Order
```python
Person A: 宮崎駿 (Miyazaki Hayao)
Person B: Hayao Miyazaki
→ AI Decision: SAME (name order difference only)
```

## Performance

- **First request**: ~2-3 seconds (model loading)
- **Subsequent requests**: 100-300ms per comparison
- **Batch processing**: O(n²) - use sparingly
- **Recommended**: Process < 100 persons at a time

## Troubleshooting

### "Connection refused"
```bash
# Check Ollama status
systemctl status ollama  # If using systemd
# or
ps aux | grep ollama

# Restart Ollama
ollama serve
```

### Model not found
```bash
# List available models
ollama list

# Pull qwen3 if needed
ollama pull qwen3:8b
```

### Slow performance
```bash
# Use smaller model
LLM_MODEL_NAME = "qwen3:8b"  # Instead of 32b

# Or check GPU usage
nvidia-smi
```

### Wrong matches (false positives)
```python
# Increase confidence threshold
ai_assisted_cluster(persons, min_confidence=0.90)  # Default: 0.8
```

## Integration with Shinar Project

Your Ollama server is shared with the shinar project:
- **Shinar**: Translation (qwen3:32b for quality)
- **Animetor Eval**: Entity resolution (qwen3:8b for speed)

Both can use the same Ollama instance simultaneously.

## Next Steps

1. **Test**: Run the test commands above
2. **Evaluate**: Check accuracy on your dataset
3. **Tune**: Adjust `min_confidence` based on false positive rate
4. **Deploy**: Integrate into pipeline if results are satisfactory

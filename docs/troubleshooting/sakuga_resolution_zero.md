# Troubleshooting: sakuga_work_title_resolution — 0 rows

**Date diagnosed**: 2026-05-02
**Card**: `TASK_CARDS/18_data_integrity/04_sakuga_resolution_zero_fix`
**File fixed**: `src/etl/silver_loaders/sakuga_atwiki.py`

---

## Symptom

```sql
SELECT COUNT(*) FROM sakuga_work_title_resolution;
-- returns 0
```

Table exists but has no rows, even though:
- BRONZE `source=sakuga_atwiki/table=credits` has ~90 K rows with 7,546 distinct work titles
- `sakuga_work_title_resolution` DDL was applied (table present)
- 130 sakuga persons were inserted successfully (earlier step in the same `integrate()` call)
- 6,267 sakuga credits exist in SILVER (inserted by the `_credits_loaders` loop in `integrate_duckdb.py`, which runs before `_source_loaders`)

---

## Root Cause

The original `_resolve_work_titles()` in `src/etl/silver_loaders/sakuga_atwiki.py`
used a **Python-level O(N×M) loop**:

```python
anime_rows = conn.execute("SELECT id, title_ja, title_en, year FROM anime").fetchall()
# anime_rows = 562,191 rows fetched into Python

for work_title, work_year, work_format in distinct_titles:   # N = 7,546
    aid, method, score = match_title(work_title, work_year, anime_rows)  # M = 562,191
    rows.append(...)
```

With N=7,546 titles and M=562,191 anime rows, this is **~4.2 billion Python comparisons**.
The process was killed by the OS (OOM or timeout) mid-run.

When killed, the exception propagated upward through `sakuga_atwiki.integrate()` which had
already completed the persons INSERT (step 2). The exception was then caught silently by
`integrate_duckdb.integrate()` at the `silver_source_skip` log level:

```python
except Exception as exc:
    logger.warning("silver_source_skip", source=source_name, error=str(exc))
```

This left `sakuga_work_title_resolution` with its DDL applied but 0 rows.

---

## Fix

Replace the Python loop with a **DuckDB SQL bulk-match** using pre-computed
normalized-title temp tables.

### Strategy

1. **Pre-compute**: Build `_swtr_bronze` temp table with distinct `(work_title, norm_title, work_year, work_format)`, calling `normalize_title()` UDF once per distinct work title (~7,546 calls).
2. **Pre-compute**: Build `_swtr_anime` temp table with `(id, title_ja, title_en, norm_ja, norm_en, year)`, calling `normalize_title()` once per anime title (~1.1 M calls, but runs inside DuckDB in a single scan).
3. **SQL JOIN**: Pass 1 exact match (pure SQL, no UDF in join predicate). Pass 2 normalized match (joins on pre-computed `norm_ja`/`norm_en` columns — pure SQL, no UDF in join predicate).
4. **INSERT**: Single `INSERT ... ON CONFLICT DO NOTHING` covering all bronze titles (resolved + unresolved).

### Matching logic preserved (H3)

The SQL replication is faithful to `sakuga_title_matcher.match_title()`:
- Year guard: `ABS(a.year - bt.work_year) <= 1` or either side NULL
- Exact: `title_ja = work_title OR title_en = work_title`
- Normalized: `norm_ja = norm_title OR norm_en = norm_title` (using same `_normalize()` function)
- Conservative: require `COUNT(DISTINCT anime_id) = 1` for both exact and normalized
- Priority: exact wins over normalized wins over unresolved

`src/analysis/entity_resolution.py` is untouched (H3).

### Performance

| Approach | Estimated comparisons | Observed runtime |
|---|---|---|
| Python O(N×M) loop | ~4.2 billion | ∞ (killed) |
| SQL with pre-computed temps | ~1.1 M UDF calls + SQL JOINs | **7.8 seconds** |

---

## Result

After fix, against the production SILVER DB:

```
sakuga_work_title_resolution: 7,550 rows  (was 0)
  resolved_anime_id IS NOT NULL: 1,233 rows  (16.3% resolution rate)
credits WHERE evidence_source='sakuga_atwiki' AND anime_id IS NOT NULL: 5,071
```

The low resolution rate (16%) is expected and correct for this conservative matcher:
- Many titles appear multiple times across different anime (e.g. sequels)
- Sakuga work_title values sometimes contain newlines or extra text
- The matcher is intentionally conservative (requires exactly 1 hit) to avoid false positives

---

## Prevention

The `integrate_duckdb.py` `silver_source_skip` exception handler swallows errors silently.
If the sakuga loader ever fails again, the symptom will be:
- `sakuga_work_title_resolution` = 0 rows
- `sakuga persons` > 0 (persons step ran before resolution step)
- `credits WHERE evidence_source='sakuga_atwiki'` > 0 (separate `_credits_loaders` loop)

To diagnose: run `pixi run python -m src.etl.integrate_duckdb 2>&1 | grep -i "sakuga\|warning\|skip"`.

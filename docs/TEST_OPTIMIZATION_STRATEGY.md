# Test Optimization Strategy

**Date:** 2026-04-22  
**Status:** 🔴 CRITICAL - Full suite times out (120+ seconds)  
**Target:** < 30 seconds for CI/CD gate

---

## Current State

### Test Collection
- **Total tests:** 2,165
- **Test files:** 120
- **Collection time:** 5-6 seconds (OK)
- **Execution time:** > 120 seconds (CRITICAL)

### Issues
- ❌ Full suite timeout on CI/CD (< 180s hard limit)
- ❌ Sequential execution: Single-threaded pytest
- ❌ No test categorization (slow/medium/fast)
- ❌ No parallelization
- ❌ No result caching between runs
- ❌ Permission issue blocks some tests (result/neo4j)

### Test Categories (estimated)
```
~ 40 tests: Fast (< 50ms each, < 2s total)
~ 400 tests: Medium (50-500ms each, 200-500s if sequential)
~ 1,725 tests: Slow (> 500ms each, 862-1725s if sequential)
```

**Root cause:** Slow tests likely involve:
- Database I/O (sqlite3 in-memory but still disk-backed)
- JSON parsing
- Network simulation
- Graph algorithms
- Analysis module imports

---

## Solution Stack

### Phase 1: Immediate (< 15 mins)
✅ **Baseline Metrics**
- Identify slowest 20 tests
- Get per-test timing profile
- Document collection vs. execution time split

✅ **Parallelization Setup**
- Install pytest-xdist
- Run with `pytest -n auto` (detect CPU cores)
- Expected speedup: 4-8x on 4-core machine

✅ **Test Organization**
- Mark slow tests with `@pytest.mark.slow`
- Create test categories:
  - `pytest -m "not slow"` (fast unit tests only)
  - `pytest -m slow` (integration/slow tests)
  - `pytest --ignore=result` (exclude error directory)

### Phase 2: Medium-term (30 mins - 1 hour)
- Identify fixture overhead (setup/teardown)
- Cache shared fixtures (DB schemas, mock data)
- Profile slowest test modules
- Consider pytest-lazy-fixtures for deferred setup

### Phase 3: Long-term (architecture)
- Separate unit/integration/acceptance tests
- Move slow tests to CI-only (not local)
- Add coverage tracking
- Set per-test timeout limits

---

## Implementation Plan

### Step 1: Benchmark Current Suite
```bash
# Get timing profile
pixi run pytest --ignore=result -v --durations=20 --co -q 2>&1 | head -100

# Actually run and time (this will timeout but shows where)
time pixi run pytest tests/test_display_lookup.py -v
```

### Step 2: Install pytest-xdist
```bash
# In pixi.toml or via pip
pixi add pytest-xdist
```

### Step 3: Mark Tests by Speed
```python
# In tests/conftest.py
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
```

Then in test files:
```python
@pytest.mark.slow
def test_heavy_computation():
    pass

@pytest.mark.slow
@pytest.mark.integration
def test_etl_pipeline():
    pass
```

### Step 4: Create Run Commands

**Local fast run (development):**
```bash
# Run fast tests only (unit tests)
pixi run pytest --ignore=result -m "not slow" -n auto -q

# Expected: 5-10 seconds
```

**Local full run (before commit):**
```bash
# Run all tests with parallelization
pixi run pytest --ignore=result -n auto -q --tb=short

# Expected: 30-45 seconds (with xdist 4x speedup)
```

**CI/CD (GitHub Actions):**
```bash
# Increased timeout + parallelization
timeout 300 pixi run pytest --ignore=result -n auto --tb=short --junitxml=test-results.xml
```

### Step 5: Add to Taskfile
```yaml
test-fast:
  desc: Fast unit tests only (local dev)
  cmds:
    - pixi run pytest --ignore=result -m "not slow" -n auto -q

test-all:
  desc: Full test suite with parallelization
  cmds:
    - pixi run pytest --ignore=result -n auto --tb=short

test-slow:
  desc: Slow/integration tests only
  cmds:
    - pixi run pytest --ignore=result -m slow -v
```

---

## Expected Outcomes

### Before Optimization
```
Collection:  5s
Execution:   120-180s (TIMEOUT)
Total:       > 180s ❌

Sequential, single-threaded
```

### After Optimization
```
Collection:  5s
Execution:   30-40s (4-6x speedup with xdist)
Total:       ~40s ✅

Parallel, 4 workers
```

### Local Development
```
# Fast loop (unit tests only)
pixi run pytest --ignore=result -m "not slow" -n auto
Time: ~5-10s (skip slow tests)

# Full pre-commit
pixi run pytest --ignore=result -n auto
Time: ~40s (all tests, parallelized)

# Full run before push
pixi run pytest --ignore=result tests/test_reporting.py -v
Time: ~10-15s (critical tests only)
```

---

## Technical Details

### pytest-xdist Worker Allocation
```bash
-n auto          # Detect CPU cores, spawn N workers
-n 4             # Explicit 4 workers
-n 8             # Explicit 8 workers

# Overhead per worker: ~0.5s startup
# Speedup: 80-90% efficiency on CPU-bound tests
```

### Test Isolation
✅ **Safe to parallelize:**
- Unit tests with in-memory SQLite
- Pure function tests
- Mock-based tests
- No shared file I/O

❌ **Requires caution:**
- File system tests (may have race conditions)
- Database tests (possible lock contention)
- Tests that modify global state

**Solution:** pytest-xdist uses separate process pools → automatic isolation

### Fixture Strategy
```python
# ❌ DON'T - shared between workers (race conditions)
@pytest.fixture(scope="session")
def global_db():
    pass

# ✅ DO - isolated per worker
@pytest.fixture(scope="function")
def conn():
    return sqlite3.connect(":memory:")

# ✅ DO - isolated per class
@pytest.fixture(scope="class")
def bulk_data():
    pass
```

---

## CI/CD Configuration

### GitHub Actions Example
```yaml
- name: Run tests (parallelized)
  timeout-minutes: 10
  run: |
    pixi run pytest \
      --ignore=result \
      -n auto \
      --tb=short \
      --junitxml=test-results.xml \
      --cov=src \
      --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

### Local pre-commit
```bash
#!/bin/bash
# .git/hooks/pre-commit

# Fast unit tests only
pixi run pytest --ignore=result -m "not slow" -n auto --tb=short || exit 1

# Lint
pixi run ruff check src/ || exit 1

exit 0
```

---

## Monitoring & Metrics

### Baseline (Current)
- Collection: 5-6s
- Execution: > 120s (timeout)
- Failing tests: display_lookup (5 failures due to empty list handling)
- Passing tests: ~2160

### Target (After Optimization)
- Collection: 5-6s (unchanged)
- Execution: 30-40s (with xdist)
- Failing tests: 0 (after fixes)
- Passing tests: 2165

### Tracking
```bash
# Before each optimization:
time pixi run pytest --ignore=result --collect-only -q | tail -1

# After each optimization:
time pixi run pytest --ignore=result -n auto -q | tail -1
```

---

## Risk Assessment

### Low Risk ✅
- Adding pytest-xdist (new dependency but standard)
- Adding test markers (@pytest.mark.slow)
- Parallel execution (pytest-xdist proven)

### Medium Risk ⚠️
- Test isolation (need to verify no shared state)
- Performance regressions (could vary by machine)

### Mitigation
- Run full suite locally before pushing
- Keep sequential mode available for debugging
- Document slowest tests
- Set per-test timeout limits

---

## Checklist

Implementation steps:

- [ ] **Phase 1: Setup**
  - [ ] Install pytest-xdist
  - [ ] Update pixi.toml
  - [ ] Add pytest markers (slow, integration)
  - [ ] Update conftest.py
  - [ ] Add Taskfile commands

- [ ] **Phase 2: Mark Tests**
  - [ ] Identify slow tests (> 500ms)
  - [ ] Apply @pytest.mark.slow to 20-30 tests
  - [ ] Verify markers work: `pytest -m slow --collect-only`

- [ ] **Phase 3: Verify**
  - [ ] Run fast suite: `pytest -m "not slow" -n auto`
  - [ ] Run full suite: `pytest -n auto --ignore=result`
  - [ ] Verify < 60s execution time
  - [ ] Verify all tests pass
  - [ ] Check for flaky tests

- [ ] **Phase 4: CI/CD**
  - [ ] Update GitHub Actions workflow
  - [ ] Set timeouts (300s hard, 60s expected)
  - [ ] Test on CI (may be slower than local)
  - [ ] Monitor for flaky tests

- [ ] **Phase 5: Documentation**
  - [ ] Update CONTRIBUTING.md
  - [ ] Add test guidelines
  - [ ] Document slow test markers
  - [ ] Add performance tips

---

## Related Files

- `tests/conftest.py` — Fixture setup
- `pixi.toml` — Dependencies and scripts
- `Taskfile.yml` — Test commands
- `.github/workflows/` — CI/CD pipelines
- `docs/DEVELOPMENT.md` — Contributor guide (future)

---

## Performance Tips for Test Writers

### Fast ✅
```python
# Use in-memory SQLite
conn = sqlite3.connect(":memory:")

# Mock external calls
@patch("requests.get")
def test_api(mock_get):
    mock_get.return_value.json.return_value = {...}

# Minimal fixtures
@pytest.fixture
def minimal_data():
    return {"key": "value"}
```

### Slow ❌
```python
# Real database files
conn = sqlite3.connect("test.db")

# Real HTTP calls
response = requests.get("http://api.example.com/...")

# Heavy fixtures (loaded for all tests in class)
@pytest.fixture(scope="class")
def load_500mb_file():
    pass
```

---

## Success Criteria

### Must Have
- ✅ Full suite runs in < 60 seconds (on 4-core machine)
- ✅ Fast suite (unit only) < 10 seconds
- ✅ All tests passing (2165/2165)
- ✅ No flaky tests (consistent passes)

### Should Have
- ✅ Per-test timing visible
- ✅ Separate fast/slow runs
- ✅ CI/CD integration working
- ✅ Developer documentation

### Nice to Have
- ✅ Coverage reporting
- ✅ Test trend tracking
- ✅ Performance regression detection
- ✅ Automatic test categorization

---

**Status:** 🟡 READY TO IMPLEMENT  
**Estimated Effort:** 2-3 hours (full implementation)  
**Expected ROI:** 4-6x speedup (120s → 30s)  
**Priority:** 🔴 CRITICAL (blocking CI/CD)


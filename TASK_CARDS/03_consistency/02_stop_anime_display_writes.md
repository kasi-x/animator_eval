# Task: `upsert_anime_display()` 呼び出しを全停止

**ID**: `03_consistency/02_stop_anime_display_writes`
**Priority**: 🟠 Major
**Estimated changes**: 約 -15 lines, 2-3 files
**Requires senior judgment**: no (機械的削除)
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/04_anime_display_removal`, `03_consistency/01_scraper_unification`

---

## Goal

`upsert_anime_display()` の **呼び出し箇所を全て削除**する (関数本体は残す — 呼び出しがなければ実質 dead code)。`anime_display` テーブルは `01_schema_fix/04` で既に drop されているため、書き込みは無意味かつ将来的にエラー源になる。

---

## Hard constraints

- H1 anime.score を scoring に使わない
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- 代替の表示データ取得は `src/utils/display_lookup.py` 経由で bronze から
- 関数 `upsert_anime_display()` **の本体は今は残す**。dead code 削除は別タスクで扱う

---

## Pre-conditions

- [ ] `01_schema_fix/04_anime_display_removal` 完了 (anime_display テーブル drop 済み)
- [ ] `03_consistency/01_scraper_unification` 完了 (scraper 書き込みパス統一)
- [ ] `pixi run test` pass

---

## Files to modify

`upsert_anime_display` の呼び出し箇所を `rg` で列挙し、1 箇所ずつ削除:

```bash
rg -n 'upsert_anime_display' src/
```

**典型的な呼び出し箇所**:
- `src/etl/integrate.py` の `integrate_anilist()` 等
- `src/scrapers/*.py` (03_consistency/01 で既に変換された可能性あり)

---

## Files to NOT touch

- `src/database.py` の `upsert_anime_display` 関数定義本体
- `src/utils/display_lookup.py`

---

## Steps

### Step 0: 呼び出し箇所の棚卸し

```bash
rg -n 'upsert_anime_display' src/ tests/
```

出力をログに保存。

### Step 1: 各呼び出しを削除

例: `src/etl/integrate.py`

**Before**:
```python
def integrate_anilist(conn, bronze_row):
    anime = _build_anime_from_anilist(bronze_row)
    upsert_anime(conn, anime)
    upsert_anime_display(conn, _anime_to_display(bronze_row))
    ...
```

**After**:
```python
def integrate_anilist(conn, bronze_row):
    anime = _build_anime_from_anilist(bronze_row)
    upsert_anime(conn, anime)
    # anime_display is deprecated; display data is read from bronze
    # via src.utils.display_lookup when needed.
    ...
```

不要な import (`upsert_anime_display`) も削除。

### Step 2: ヘルパー関数 `_anime_to_display` の呼び出しも削除

呼び出しが消えたら `_anime_to_display` は dead code 化するが、**関数本体は残してよい** (削除は別タスク `03_consistency/05` or ε phase)。

### Step 3: テスト

```bash
pixi run test -- -k "integrate or etl" -v
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "integrate or etl or anime_display"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile src/etl/integrate.py

# 2. 呼び出しが全て消えた
rg -n 'upsert_anime_display(' src/
# 期待: 0 件 (関数定義行は別途 `def upsert_anime_display` として残る)

# 関数定義は残っていること
rg -n '^def upsert_anime_display' src/database.py
# 期待: 1 件

# 3. テスト
pixi run test-scoped tests/ -k "integrate or etl or anime_display"

# 4. Lint
pixi run lint

# 5. フレッシュ DB で pipeline smoke が通る (02_phase4/02 のテスト再実行)
pixi run test -- tests/test_pipeline_v55_smoke.py -v

# 6. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
rg 'display_lookup' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] 呼び出し削除後にテスト fail (表示経路が壊れた可能性 → `display_lookup` 経由が未整備)
- [ ] `upsert_anime_display` 関数定義が**消えている** (本タスクでは触らない)
- [ ] pipeline smoke test fail

---

## Rollback

```bash
git checkout src/etl/integrate.py src/scrapers/
pixi run test-scoped tests/ -k "integrate or etl or anime_display"
```

---

## Completion signal

- [ ] 呼び出し 0 件、関数定義は残存
- [ ] テスト全件 pass
- [ ] `git commit`:
  ```
  Stop calling upsert_anime_display

  anime_display table was dropped in v55 (01_schema_fix/04).
  Display data now comes from bronze via src/utils/display_lookup.
  The upsert_anime_display function body is preserved as dead code
  for now; removal is a separate cleanup task.
  ```

# Task: `src/etl/__init__.py` の exports 整理

**ID**: `03_consistency/05_etl_init_exports`
**Priority**: 🟡 Minor
**Estimated changes**: 約 +15 / -0 lines, 1 file
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `03_consistency/01_scraper_unification` (`upsert_canonical_anime` が存在する前提)

---

## Goal

`src/etl/__init__.py` が空の状態を解消し、public API (`upsert_canonical_anime`, `integrate_anilist`, `integrate_ann`, 等) を明示的に export する。これにより外部からの import 経路が明確になり、誤った internal 関数の使用を防ぐ。

---

## Hard constraints

- H5 既存テスト green 維持

**本タスク固有**:
- `__init__.py` は **pure re-export のみ**。ロジックを書かない
- Private 関数 (`_build_*`, `_anime_to_*`) は export しない

---

## Pre-conditions

- [ ] `03_consistency/01_scraper_unification` 完了
- [ ] `pixi run test` pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/__init__.py` | public API の `__all__` 定義と re-export |

---

## Files to NOT touch

- `src/etl/integrate.py` (実装は変えない)

---

## Steps

### Step 0: 現状確認

```bash
cat src/etl/__init__.py
# 期待: 空 or コメントのみ

# integrate.py の public 関数を列挙 (アンダースコアで始まらないもの)
grep -n '^def [a-z]' src/etl/integrate.py
```

### Step 1: `__init__.py` 作成

`src/etl/__init__.py`:

```python
"""ETL layer: bronze → silver integration.

Public API:
- upsert_canonical_anime: unified write path for scrapers (silver + bronze)
- integrate_anilist / integrate_ann / integrate_allcinema / integrate_seesaawiki /
  integrate_keyframe / integrate_mal: source-specific integration entry points

Internal helpers (names starting with '_') are not exported and should not
be imported by code outside src/etl/.
"""
from src.etl.integrate import (
    integrate_allcinema,
    integrate_anilist,
    integrate_ann,
    integrate_keyframe,
    integrate_mal,
    integrate_seesaawiki,
    upsert_canonical_anime,
)

__all__ = [
    "upsert_canonical_anime",
    "integrate_allcinema",
    "integrate_anilist",
    "integrate_ann",
    "integrate_keyframe",
    "integrate_mal",
    "integrate_seesaawiki",
]
```

**注意**: 実際に `integrate.py` に存在する関数のみを import する。Step 0 の出力で確認した名前に合わせる。存在しない関数をリストに入れると import エラー。

### Step 2: 外部からの利用を整理 (任意)

```bash
rg -n 'from src.etl.integrate import' src/ tests/ scripts/
```

内部への直接 import をしているコードがあれば `from src.etl import ...` に統一してもよい(ただしこれは別タスクに分離できる)。本タスクでは **export の定義のみ** を goal とする。

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "etl"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile src/etl/__init__.py

# 2. Import が動く
pixi run python -c "
from src.etl import upsert_canonical_anime, integrate_anilist
print('etl public API ok')
"
# 期待: etl public API ok

# 3. __all__ が正しく定義
pixi run python -c "
import src.etl as etl
print(etl.__all__)
"

# 4. テスト全件
pixi run test-scoped tests/ -k "etl"

# 5. Lint
pixi run lint
```

---

## Stop-if conditions

- [ ] `integrate.py` に想定した関数が存在しない
- [ ] Import がテストで失敗

---

## Rollback

```bash
git checkout src/etl/__init__.py
```

---

## Completion signal

- [ ] 全 verification pass
- [ ] `git commit`:
  ```
  Export public ETL API from src/etl/__init__.py

  Defines __all__ with upsert_canonical_anime and the 6
  integrate_{source} entry points. Private helpers remain
  unexposed.
  ```

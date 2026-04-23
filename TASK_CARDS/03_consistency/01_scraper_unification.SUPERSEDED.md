# SUPERSEDED by 04_duckdb/02_scraper_migration

本カードは scraper を SQLite ラッパー (`upsert_canonical_anime`) 経由に
統一する案だったが、scraper を Parquet 出力に切替える方針 (04_duckdb/02)
に統合された。改修を 1 度で済ませるため。

---

# Task: 6 Scraper の書き込みパスを統一

**ID**: `03_consistency/01_scraper_unification`
**Priority**: 🟠 Major
**Estimated changes**: 約 +30 / -40 lines, 6-7 files
**Requires senior judgment**: **yes** (integrate.py のラッパー設計)
**Blocks**: `03_consistency/02_stop_anime_display_writes`
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

6 scraper が `upsert_anime()` を直接呼んでいる状態を統一。各 scraper はモデルを組み立て、`src/etl/integrate.py` の `integrate_*()` 関数 (または新ラッパー `upsert_canonical_anime()`) 経由で書き込むように変更する。

**効果**:
- `anime` テーブルのスキーマ変更時、scraper 側を個別に追従する必要がなくなる
- dual-write (silver + bronze) の整合性が一箇所で管理される

---

## Hard constraints

- H1 anime.score を scoring に使わない
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **scraper のフェッチ・パースロジックは変えない** (ネットワーク挙動不変)
- 書き込み直前の `upsert_*()` 呼び出しのみ差し替え
- entity resolution (H3) に関連する `persons` / `credits` の書き込みは変えない

---

## Pre-conditions

- [ ] `01_schema_fix/` 全完了
- [ ] `pixi run test` pass
- [ ] scraper テストが baseline で pass
  ```bash
  pixi run test -- -k "scraper" -v
  ```

---

## Files to modify

### 対象 6 scraper
| File | `upsert_anime()` 呼び出し回数 (現状) |
|------|------|
| `src/scrapers/seesaawiki_scraper.py` | 2 |
| `src/scrapers/anilist_scraper.py` | 1 |
| `src/scrapers/keyframe_scraper.py` | 1 |
| `src/scrapers/jvmg_fetcher.py` | 1 |
| `src/scrapers/mal_scraper.py` | 1 |
| `src/scrapers/mediaarts_scraper.py` | 1 |

### ラッパー定義
| File | 変更内容 |
|------|---------|
| `src/etl/integrate.py` | `upsert_canonical_anime()` を新設 (または既存の `integrate_*()` を export) |

---

## Files to NOT touch

- `src/scrapers/*` のフェッチ・パースロジック (requests, GraphQL, HTML 解析部)
- `src/analysis/` / `src/pipeline_phases/`
- `persons` / `credits` 書き込み (entity resolution 経路)

---

## Steps

### Step 0: 現状把握

```bash
# 各 scraper での upsert_anime 呼び出し位置
for f in src/scrapers/seesaawiki_scraper.py src/scrapers/anilist_scraper.py \
         src/scrapers/keyframe_scraper.py src/scrapers/jvmg_fetcher.py \
         src/scrapers/mal_scraper.py src/scrapers/mediaarts_scraper.py; do
  echo "=== $f ==="
  grep -n 'upsert_anime(' "$f"
done

# integrate.py の既存 API
grep -n '^def ' src/etl/integrate.py
```

### Step 1: `integrate.py` にラッパー追加

**Option A** (推奨): 新関数 `upsert_canonical_anime()` を追加し、scraper から呼ばせる

```python
# src/etl/integrate.py

def upsert_canonical_anime(conn, anime_model, *, evidence_source: str) -> str:
    """Single entry point for scrapers to persist an anime record.

    Handles silver (canonical) write and bronze snapshot in one place,
    ensuring schema evolution only touches this function.

    Args:
        conn: SQLite connection
        anime_model: Pydantic Anime / BronzeAnime model
        evidence_source: one of 'anilist', 'ann', 'allcinema', 'seesaawiki',
                         'keyframe', 'mal' (must exist in sources lookup)

    Returns:
        The anime.id (canonical) that was upserted.
    """
    from src.database import upsert_anime
    # Silver (canonical; no score/popularity/description fields)
    anime_id = upsert_anime(conn, anime_model)

    # Bronze snapshot — routes to the appropriate src_{source}_anime table
    _upsert_bronze(conn, anime_model, evidence_source=evidence_source)

    return anime_id


def _upsert_bronze(conn, anime_model, *, evidence_source: str) -> None:
    """Dispatch bronze upsert to the source-specific src_*_anime table."""
    if evidence_source == "anilist":
        _upsert_src_anilist(conn, anime_model)
    elif evidence_source == "ann":
        _upsert_src_ann(conn, anime_model)
    # ... 他 4 ソース
```

**Option B**: 既存 `integrate_anilist()`, `integrate_ann()` 等を export し、scraper が直接呼ぶ。ただし各 scraper のコンテキストで bronze テーブル特化ロジックが必要なので、Option A の汎用ラッパー方が保守性が高い。

### Step 2: 各 scraper の呼び出しを置換

例: `src/scrapers/anilist_scraper.py`

**Before**:
```python
from src.database import upsert_anime
...
upsert_anime(conn, anime)
```

**After**:
```python
from src.etl.integrate import upsert_canonical_anime
...
upsert_canonical_anime(conn, anime, evidence_source="anilist")
```

同様に 6 scraper 全てに適用。`evidence_source` は各 scraper ごとに固定:
- `anilist_scraper.py` → `"anilist"`
- `mal_scraper.py` → `"mal"`
- `seesaawiki_scraper.py` → `"seesaawiki"`
- `keyframe_scraper.py` → `"keyframe"`
- `jvmg_fetcher.py` → 調査要 (`grep -i 'source\|evidence' src/scrapers/jvmg_fetcher.py`)
- `mediaarts_scraper.py` → 調査要 (`allcinema` or `madb` の可能性)

### Step 3: import 整理

各 scraper の不要になった `from src.database import upsert_anime` を削除 (他の用途で使っていなければ)。

### Step 4: scraper テストで回帰がないこと確認

```bash
pixi run test -- -k "scraper or etl or integrate" -v
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "scraper or etl or integrate"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile src/etl/integrate.py src/scrapers/*.py

# 2. 直接呼び出しが消えたか (想定: 0 件)
for f in src/scrapers/*.py; do
  cnt=$(grep -c 'upsert_anime(' "$f")
  if [ "$cnt" -gt 0 ]; then
    echo "REMAINING in $f: $cnt"
    grep -n 'upsert_anime(' "$f"
  fi
done
# 期待: REMAINING が 1 行も出ない

# 3. ラッパー経由の呼び出しが存在
rg -n 'upsert_canonical_anime' src/scrapers/
# 期待: 6+ 件

# 4. テスト
pixi run test-scoped tests/ -k "scraper or etl or integrate"

# 5. Lint
pixi run lint

# 6. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] `pixi run test` で scraper 系テストが fail
- [ ] `evidence_source` の値が不明な scraper がある → ユーザ確認
- [ ] Verification 2 で `upsert_anime(` が scraper 内に残存
- [ ] `upsert_canonical_anime` が既存の別機能と名前衝突

---

## Rollback

```bash
git checkout src/scrapers/ src/etl/integrate.py
pixi run test-scoped tests/ -k "scraper or etl or integrate"
```

---

## Completion signal

- [ ] Verification 全項目 pass
- [ ] `git diff --stat`: 6 scrapers + integrate.py、約 ±70 lines
- [ ] `git commit`:
  ```
  Unify scraper writes through upsert_canonical_anime

  Scrapers now persist via a single entry point in src/etl/integrate.py,
  which handles silver (canonical) + bronze dual-write consistently.
  Schema evolution only touches this function instead of 6 scrapers.
  ```

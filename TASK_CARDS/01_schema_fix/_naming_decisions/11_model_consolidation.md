# Task: `Anime / AnimeAnalysis / AnimeDisplay / BronzeAnime` 4 モデル整理

**ID**: `01_schema_fix/11_model_consolidation`
**Priority**: 🟡 Medium
**Estimated changes**: `src/models_v2.py` -100 / +30 lines
**Requires senior judgment**: yes（削除するモデルの外部利用確認が必須）
**Blocks**: なし
**Blocked by**: `01_schema_fix/04_anime_display_removal`（anime_display テーブル廃止後に実施）

---

## Goal

`src/models_v2.py` に重複・廃止モデルが共存している:

| クラス | 状態 | 対応 |
|--------|------|------|
| `Anime` | SILVER canonical ✅ | 維持 |
| `AnimeAnalysis` | 廃止予定（`anime_analysis` テーブルは残存するが書き込みなし） | 削除 or deprecate |
| `AnimeDisplay` | `anime_display` テーブル廃止済み → 使用不能 | 削除 |
| `BronzeAnime` / `SrcAnilistAnime` | BRONZE canonical ✅ | 名前を統一（BronzeAnime は別名か？） |

このタスクでは `AnimeDisplay` を完全削除し、`AnimeAnalysis` を deprecate コメント付きで保留扱いにする。

---

## Hard constraints

- H1 anime.score を scoring に使わない
- H5 全テスト green 維持
- H8 行番号を信じず **クラス名で探す**

**本タスク固有**:
- `AnimeDisplay` は `anime_display` テーブルが廃止済み → 完全削除してよい
- `AnimeAnalysis` は `anime_analysis` テーブルが migration 内で参照されている → 削除せず deprecate コメント追加のみ
- `Anime` モデルは絶対に変更しない

---

## Pre-conditions

- [ ] `git status` が clean（前タスクコミット済み）
- [ ] `anime_display` テーブルが DB から削除済み（v55 migration で確認）
- [ ] `rg "AnimeDisplay" src/ tests/` で外部使用箇所を全部確認済み

---

## Step-by-Step

### Step 1: 外部使用を確認してから削除判断

```bash
rg "AnimeDisplay" src/ tests/ scripts/
rg "AnimeAnalysis" src/ tests/ scripts/
```

- `AnimeDisplay` の使用が `src/models_v2.py` 定義のみ → 完全削除可
- `AnimeAnalysis` の使用が migration 内のみ → deprecate コメント追加のみ

### Step 2: `AnimeDisplay` を削除

`src/models_v2.py` から `class AnimeDisplay(SQLModel, table=True):` ブロック全体を削除。

### Step 3: `AnimeAnalysis` に deprecate コメントを追加

```python
class AnimeAnalysis(SQLModel, table=True):
    # DEPRECATED: anime_analysis table is write-frozen (no new writes after v53).
    # Retained for migration compatibility only. Do not use in new code.
    # Will be fully removed in a future cleanup task.
    __tablename__ = "anime_analysis"
    ...
```

### Step 4: `BronzeAnime` vs `SrcAnilistAnime` 名前確認

```bash
rg "class BronzeAnime\|class SrcAnilistAnime" src/models_v2.py
```

もし `BronzeAnime` が `SrcAnilistAnime` の alias か別定義なら、重複を解消してどちらか一本に統一。

---

## Verification

```bash
rg "AnimeDisplay" src/ tests/  # 0 件
pixi run lint                  # clean
pixi run test                  # all pass
```

---

## Commit message

```
Consolidate anime models: delete AnimeDisplay, deprecate AnimeAnalysis
```

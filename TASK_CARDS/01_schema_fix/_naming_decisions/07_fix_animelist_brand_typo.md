# Task: `SrcAnimelist*` → `SrcAnilist*` (ブランド誤記修正)

**ID**: `01_schema_fix/07_fix_animelist_brand_typo`
**Priority**: 🔴 Critical (命名不整合、クエリ経路ミスの危険)
**Estimated changes**: 約 +3 / -3 lines, 1 file
**Requires senior judgment**: no (機械的置換)
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/06_v56_defer_comment` (01-06 完了後、並行開始可)

---

## Goal

`src/models_v2.py` の 3 クラス `SrcAnimelistAnime` / `SrcAnimelistPersons` / `SrcAnimelistCredits` を `SrcAnilistAnime` / `SrcAnilistPersons` / `SrcAnilistCredits` にリネーム。テーブル名(物理DBの `src_anilist_*`)と一致させる。

**背景**: AniList (`https://anilist.co`) と animelist (無関係な文字列、誤記) を混同。テーブル名は正しく `src_anilist_*` だが SQLModel クラス名だけ誤記。クラス経由 ORM で新規コードを書くと、テーブル名不一致で **FK / クエリが壊れる**リスク。

---

## Hard constraints

- H5 既存テスト green 維持

**本タスク固有**:
- **DB テーブル名は既に `src_anilist_*` で正しい**。本タスクは **Python クラス名のみ**変更
- migration 不要(DDL には触らない)

---

## Pre-conditions

- [ ] `01_schema_fix/01`〜`06` 完了・コミット済み
- [ ] `git status` clean
- [ ] `pixi run test-scoped tests/ -k "migration or schema"` pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/models_v2.py` | 3 クラス名を `Animelist` → `Anilist` に置換 (`SrcAnimelistAnime`, `SrcAnimelistPersons`, `SrcAnimelistCredits`) |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/database.py` の DDL | テーブル名 `src_anilist_*` は既に正しい |
| 他の `src_anilist_*` 参照コード | クラス経由ではなく直接 SQL 文字列なので影響なし |

---

## Steps

### Step 0: 現状棚卸し

```bash
# 対象の 3 クラスを確認
grep -n "SrcAnimelist\|src_animelist" /home/user/dev/animetor_eval/src/models_v2.py

# 他ファイルでの使用箇所を確認(想定: 0 件 or 非常に少ない、SQLModel 経由使用が限定的なため)
rg -n "SrcAnimelist" src/ tests/ scripts/
```

**期待**: `src/models_v2.py` に 3 箇所(クラス定義)、他ファイルからの参照は 0 件か極少。

### Step 1: クラス名を置換

`Edit` ツールで `src/models_v2.py` 内の以下 3 クラスをリネーム。**`__tablename__` は変えない** (既に `src_anilist_*` で正しい):

**Before**:
```python
class SrcAnimelistAnime(SQLModel, table=True):
    __tablename__ = "src_anilist_anime"
    ...

class SrcAnimelistPersons(SQLModel, table=True):
    __tablename__ = "src_anilist_persons"
    ...

class SrcAnimelistCredits(SQLModel, table=True):
    __tablename__ = "src_anilist_credits"
    ...
```

**After**:
```python
class SrcAnilistAnime(SQLModel, table=True):
    __tablename__ = "src_anilist_anime"
    ...

class SrcAnilistPersons(SQLModel, table=True):
    __tablename__ = "src_anilist_persons"
    ...

class SrcAnilistCredits(SQLModel, table=True):
    __tablename__ = "src_anilist_credits"
    ...
```

`Edit` の `replace_all=true` オプションで安全:
- `SrcAnimelistAnime` → `SrcAnilistAnime`
- `SrcAnimelistPersons` → `SrcAnilistPersons`
- `SrcAnimelistCredits` → `SrcAnilistCredits`

### Step 2: 他ファイルでの参照更新(Step 0 で見つかった場合のみ)

```bash
# もし Step 0 で参照が見つかったら、該当ファイルを手動で置換
# 典型的には scripts/ や tests/ の少数ファイル
```

0 件ならスキップ。

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact`
- **T2 (失敗直後)**: `pixi run test-quick`
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "models or sqlmodel or schema"`
- **T4 (commit 直前 1 回)**: `pixi run test`

```bash
# 1. 構文
python -m py_compile src/models_v2.py

# 2. Animelist が消えた
rg -n 'SrcAnimelist|src_animelist' src/ tests/ scripts/
# 期待: 0 件

# 3. Anilist クラスが 3 つ存在
rg -n "^class SrcAnilist" src/models_v2.py
# 期待: 3 件

# 4. __tablename__ が変わっていない (DDL と不整合になると破滅)
rg -n '__tablename__ = "src_anilist_' src/models_v2.py
# 期待: 3 件

# 5. テスト
pixi run test-scoped tests/ -k "models or sqlmodel or schema"

# 6. Lint
pixi run lint

# 7. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] Step 0 で他ファイルから大量に `SrcAnimelist` が import されている (予想外、要調査)
- [ ] Verification 4 で `__tablename__` が "animelist" に変わってしまっている (最悪のミス)
- [ ] テスト失敗

---

## Rollback

```bash
git checkout src/models_v2.py
pixi run test-scoped tests/ -k "models"
```

---

## Completion signal

- [ ] Verification 全項目 pass
- [ ] `git diff --stat` が 1 file, 約 ±3-6 lines
- [ ] `git commit`:
  ```
  Fix brand typo: SrcAnimelist* → SrcAnilist*

  AniList is the correct brand (https://anilist.co); "animelist"
  was a typo. Table names (src_anilist_*) were already correct —
  only Python class names needed the fix.
  ```

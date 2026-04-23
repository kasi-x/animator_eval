# Task: `DisplayLookup` クラス名/テーブル名の分離

**ID**: `01_schema_fix/13_display_lookup_naming`
**Priority**: 🟢 Low
**Estimated changes**: `src/models_v2.py` +10 lines, `src/utils/display_lookup.py` 軽微修正
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: `01_schema_fix/04_anime_display_removal`

---

## Goal

`src/utils/display_lookup.py` の `DisplayLookup` クラスと、`src/models_v2.py` の `DisplayLookup` SQLModel テーブルが同じ名前を使っている。これにより:

1. `from src.utils.display_lookup import DisplayLookup` と
   `from src.models_v2 import DisplayLookup` が衝突する危険がある
2. 「ヘルパー関数クラス」と「DB テーブル定義クラス」が同名で混乱を招く

---

## Hard constraints

- H5 全テスト green 維持
- H8 行番号を信じず **クラス名で探す**

**本タスク固有**:
- DB テーブル名 `display_lookup` 自体は変更しない（`__tablename__` は維持）
- Python クラス名のみを分離する:
  - `models_v2.py` の SQLModel クラス: `DisplayLookup` → `DisplayLookupRow`
  - `utils/display_lookup.py` のヘルパー関数: 変更なし（関数ベースのまま）

---

## Pre-conditions

- [ ] `git status` が clean
- [ ] `rg "DisplayLookup" src/ tests/ scripts/` で全使用箇所を確認済み

---

## Step-by-Step

### Step 1: 影響範囲確認

```bash
rg "DisplayLookup" src/ tests/ scripts/
```

`models_v2.py` の定義と import 箇所をリストアップ。

### Step 2: `models_v2.py` でクラス名を変更

```python
# Before
class DisplayLookup(SQLModel, table=True):
    __tablename__ = "display_lookup"
    ...

# After
class DisplayLookupRow(SQLModel, table=True):
    __tablename__ = "display_lookup"  # DB テーブル名は変更しない
    ...
```

### Step 3: 全 import 箇所を更新

```bash
rg -l "from src.models_v2 import.*DisplayLookup\|import DisplayLookup" src/ tests/ scripts/
```

各ファイルで `DisplayLookup` → `DisplayLookupRow` に置換。

### Step 4: `utils/display_lookup.py` の確認

関数ベースのヘルパーであれば変更不要。もし同名クラスが定義されていれば
`DisplayLookupHelper` などに改名して衝突を排除する。

---

## Verification

```bash
rg "from src.models_v2 import.*DisplayLookup[^R]" src/ tests/  # 0 件 (非 Row suffix)
pixi run lint
pixi run test
```

---

## Commit message

```
Rename DisplayLookup SQLModel → DisplayLookupRow to avoid naming collision
```

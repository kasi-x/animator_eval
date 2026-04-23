# Task: [タスクタイトル]

**ID**: `NN_category/MM_slug`
**Priority**: 🔴 / 🟠 / 🟡 / 🟢
**Estimated changes**: 約 +X / -Y lines, Z files
**Requires senior judgment**: yes / no
**Blocks**: [次タスク ID 列]
**Blocked by**: [前タスク ID 列]

---

## Goal

[1 文で完了状態を記述]

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を scoring に使わない
- H3 entity resolution ロジック不変
- (本タスク固有の制約があればここに)

---

## Pre-conditions

- [ ] 前タスク `NN_X` 完了
- [ ] `git status` が clean
- [ ] `pixi run test` が pass (baseline 2161 tests)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `path/to/file.py` | [具体的に] |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `path/to/other.py` | [なぜ触らないか] |

---

## Steps

### Step 1: [ステップ名]

[シンボル名ベースの具体的指示]

```bash
# 現在位置確認
grep -n "def target_function" path/to/file.py
```

期待する変更:

```python
# Before
def target_function(...):
    ...

# After
def target_function(...):
    ...  # 具体的変更
```

### Step 2: [ステップ名]

...

---

## Verification

Steps 完了後、以下を順に実行し **全て pass すること**:

```bash
# 1. Unit test
pixi run test

# 2. Lint
pixi run lint

# 3. タスク固有の検証
[specific commands]

# 4. invariant 確認
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
```

---

## Stop-if conditions

以下のいずれかに該当したら **即中断し、Rollback 手順** を実行:

- [ ] `pixi run test` が失敗
- [ ] `pixi run lint` が失敗
- [ ] `git diff --stat` が想定 (±X/Y lines) の 2 倍を超える
- [ ] [タスク固有の Stop 条件]

---

## Rollback

中断時に状態を戻す:

```bash
git checkout src/path/to/file.py
# または新規作成ファイルなら:
rm src/path/to/new_file.py
```

その後:

```bash
pixi run test
# baseline に戻ったこと確認
```

---

## Completion signal

全て満たしたら完了:

- [ ] 全 Verification コマンドが pass
- [ ] `git diff --stat` が想定通り
- [ ] 作業ログに `DONE: NN_category/MM_slug` と記録

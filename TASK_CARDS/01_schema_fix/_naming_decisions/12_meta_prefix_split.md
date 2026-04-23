# Task: `meta_*` プレフィックス分割 (`ops_*` 導入)

**ID**: `01_schema_fix/12_meta_prefix_split`
**Priority**: 🟡 Medium
**Estimated changes**: `src/database.py` +1 migration, 全 caller 置換
**Requires senior judgment**: yes（テーブルリネームは既存 DB への影響大）
**Blocks**: なし
**Blocked by**: `01_schema_fix/02_register_v55_migration`

---

## Goal

現在 `meta_*` プレフィックスのテーブルが 2 種類の意味で使われている:

| 現在の名前 | 意味 | 新しい名前 |
|------------|------|-----------|
| `meta_lineage` | データ品質・系譜（分析メタ） | 維持 (`meta_lineage`) |
| `meta_policy_score` | 政策スコア出力（GOLD 層） | `ops_policy_score` |
| `meta_hr_observation` | HR 観察出力（GOLD 層） | `ops_hr_observation` |
| `meta_*` (他 GOLD 出力) | 業務用スコア出力 | `ops_*` |

`ops_*` = "operational outputs" — 業務用スコア・推薦など。
`meta_*` = "metadata" — データ系譜・品質・バリデーション情報のみ。

---

## Hard constraints

- H1 anime.score を scoring に使わない
- H5 全テスト green 維持
- H8 行番号を信じず **テーブル名で探す**

**本タスク固有**:
- `meta_lineage` は metadata 系として **変更しない**
- `meta_policy_score` と `meta_hr_observation` のみ `ops_*` にリネーム
- API レスポンスの JSON キー名は後方互換を保つ（内部テーブル名のみ変更）

---

## Pre-conditions

- [ ] `git status` が clean
- [ ] `rg "meta_policy_score\|meta_hr_observation" src/ tests/ scripts/` で全使用箇所を把握済み

---

## Step-by-Step

### Step 1: 影響範囲確認

```bash
rg "meta_policy_score\|meta_hr_observation" src/ tests/ scripts/
```

使用箇所がゼロまたは少数であることを確認してから進む。

### Step 2: v57 migration を追加

```python
def _migrate_v56_to_v57_ops_prefix(conn: sqlite3.Connection) -> None:
    """v57: meta_policy_score / meta_hr_observation → ops_* リネーム."""
    cursor = conn.cursor()
    for old, new in [
        ("meta_policy_score", "ops_policy_score"),
        ("meta_hr_observation", "ops_hr_observation"),
    ]:
        if cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (old,)
        ).fetchone():
            cursor.execute(f"ALTER TABLE {old} RENAME TO {new}")
            logger.info("table_renamed", old=old, new=new)
    conn.commit()
    _set_schema_version(conn, 57)
```

### Step 3: init_db() DDL 更新

`CREATE TABLE IF NOT EXISTS meta_policy_score` → `ops_policy_score` に変更。

### Step 4: 全 caller 更新

```bash
sed -i 's/meta_policy_score/ops_policy_score/g' src/api.py src/cli.py src/models_v2.py
sed -i 's/meta_hr_observation/ops_hr_observation/g' src/api.py src/cli.py src/models_v2.py
```

### Step 5: テスト更新

```bash
sed -i 's/meta_policy_score/ops_policy_score/g' tests/
sed -i 's/meta_hr_observation/ops_hr_observation/g' tests/
```

---

## Verification

```bash
rg "meta_policy_score\|meta_hr_observation" src/ tests/  # 0 件
pixi run test
```

---

## Commit message

```
Rename meta_policy_score/hr_observation → ops_* (operational outputs prefix)
```

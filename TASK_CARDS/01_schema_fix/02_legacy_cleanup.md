# Task: `src/database.py` の legacy migration を削除

**ID**: `01_schema_fix/02_legacy_cleanup`
**Priority**: 🟡 Medium (後回しでも動作に影響なし)
**Estimated changes**: `src/database.py` -5000〜-7000 lines
**Blocks**: なし
**Blocked by**: `00_target_schema` 完了、`01_one_shot_copy` で移行成功確認後

---

## Goal

新 DB (v2) が動き始めたら、旧 `src/database.py` の 54+ migration 関数と legacy DDL を削除。`src/database.py` → `src/database_v2.py` への全面置き換え。

旧 DB へのアクセスは `scripts/migrate_to_v2.py` でコピー済みなので、もう旧コードは不要。

---

## 方針(重要)

- **細かいテストをしない**。fresh init + scraping smoke (カード `03`, `04`) で十分
- `database.py` の行数最小化より、**「新コードだけで動く」**を優先
- legacy migration 関数は全削除、`migrations` dict は空 or 廃止
- コメント・docstring の歴史的記述も削除してよい

---

## Files to modify

| File | 内容 |
|---|---|
| `src/database.py` | `database_v2.py` の内容に置き換え、または削除して `v2` を rename |
| `src/models.py` | 旧 `Anime` / `AnimeAnalysis` / `AnimeDisplay` / `BronzeAnime` を統合、`models_v2.py` を source of truth に |
| 呼び出し側 (`src/api.py`, `src/cli.py`, `src/pipeline_phases/*`) | `database_v2` 参照に切替 |

---

## Steps

1. `src/database_v2.py` の内容を `src/database.py` に上書き(または `git mv`)
2. `src/models.py` 内の重複モデルを整理 (`_naming_decisions/11` 参照)
3. 全ての `from src.database import ...` 呼び出し側で、import 先が新版であることを確認
4. `pixi run test` を走らせて既存テストがどれくらい壊れるか確認
5. **壊れるテストは割り切って削除 or skip**(新 schema 前提のテストだけ残す)

---

## Verification (最小限)

```bash
# 1. legacy migration 関数が消えた
grep -c '^def _migrate_v' src/database.py
# 期待: 0

# 2. 行数が激減
wc -l src/database.py
# 期待: 1000-2000 行程度 (旧 9000+ 行から激減)

# 3. Lint
pixi run lint

# 4. fresh init が動く (後続の 03 で本格検証)
pixi run python -c "
import sqlite3, tempfile, pathlib
from src.database import init_db
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = sqlite3.connect(p)
init_db(conn)
print('OK')
"
```

---

## Completion signal

- [ ] `database.py` から `_migrate_v*` が全消去
- [ ] fresh init が動く
- [ ] コミット: `Remove legacy migrations; database.py now has target schema only`

---

## 捨ててよいもの

以下は本タスクで気にせず削除してよい:
- 旧 migration 関数の内容(`_migrate_v1_*` 〜 `_migrate_v54_to_v55`)
- `ensure_phase1_schema()` 関数
- 旧 `anime_display` / `anime_analysis` の DDL と upsert 関数
- `va_scores`, `data_sources`, `scores` を参照するコード(置換済 or 新 schema 使用)
- 旧 schema 前提のテストファイル(新 schema で意味不明になったもの)

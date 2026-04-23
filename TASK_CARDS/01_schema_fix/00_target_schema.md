# Task: 新 schema を DDL として書き下ろす

**ID**: `01_schema_fix/00_target_schema`
**Priority**: 🔴 Critical (本セクションの起点)
**Estimated changes**: 新 DDL 1 本 (~200-400 lines)
**Blocks**: `01`, `02`, `03`, `04`
**Blocked by**: なし

---

## Goal

`_naming_decisions/` の全ての命名決定を反映した新 schema を、**単一の SQL ファイル** または **`init_db()` の新版関数** として書き下ろす。漸進 migration は書かない。

---

## 方針(重要)

- 旧 `database.py` の 54 個の migration 関数は**全部無視**して、新しく 1 から書く
- 新 schema は `v55` や `v56` のような version 名を付けず、**target schema** としてシンプルに定義
- テーブル prefix は実態と一致: `src_*` / (prefix なし) / `feat_*` / `agg_*` / `meta_*` / `ops_*`
- `_naming_decisions/` の決定を全て反映(もう「migration path」を気にしない)

---

## Files to modify

| File | 内容 |
|---|---|
| `src/database_v2.py` (新規推奨) または `schema/target.sql` | 新 schema の完全 DDL |

**注意**: 既存 `src/database.py` は触らない(`02_one_shot_copy` で参照するため)。新コードは別ファイルとして独立して書く。

---

## 新 schema に含めるべきテーブル (_naming_decisions を反映済み)

### Source layer (外部スナップショット)
- `src_anilist_anime`, `src_anilist_persons`, `src_anilist_credits`
- `src_ann_anime`, `src_ann_persons`, `src_ann_credits`
- `src_allcinema_anime`, `src_allcinema_persons`, `src_allcinema_credits`
- `src_seesaawiki_anime`, `src_seesaawiki_credits`
- `src_keyframe_anime`, `src_keyframe_credits`
- `src_mal_anime`, `src_mal_characters`
- `src_madb_anime`

### Canonical layer (分析入力)
- `anime` (※ score / popularity / description / cover_* / genres JSON 含まない)
  - `original_work_type` 列 (旧 `source` 列のリネーム、_naming_decisions/10)
- `persons`
- `credits` (`evidence_source` 列使用、`episode INTEGER NULL`)
- `studios`, `anime_studios`
- `anime_genres`, `anime_tags` (JSON 正規化展開)
- `anime_external_ids`, `person_external_ids`
- `person_aliases`
- `characters`, `character_voice_actors`, `anime_relations`

### Lookup
- `sources` (PK=`code`, 列: `name_ja`, `base_url`, `license`, `description`)
- `roles` (24 種、role_groups.py を DB 化)

### Feature layer
- `feat_person_scores`, `feat_career`, `feat_network`, ...
- 既存 feat_* テーブルはそのまま踏襲 (命名は既に機能的)

### Aggregation layer
- `agg_director_circles`, `agg_milestones`, ...

### Report layer (audience 別)
- `meta_policy_*` (attrition, monopsony, gender, generation)
- `meta_hr_*` (attrition_risk, mentor_card, studio_benchmark, succession)
- `meta_biz_*` (whitespace, undervalued, trust_entry, independent_unit, team_template)
- `meta_common_person_parameters`

### Ops layer (旧 `meta_*` から分離、_naming_decisions/12)
- `ops_lineage` (旧 `meta_lineage`)
- `ops_entity_resolution_audit` (旧 `meta_entity_resolution_audit`)
- `ops_quality_snapshot` (旧 `meta_quality_snapshot`)
- `ops_source_scrape_status` (旧 `data_sources`、_naming_decisions/09)
- `schema_meta` (単体テーブル、バージョン追跡用)

### 廃止するテーブル(新 schema には**作らない**)
- `anime_display` (廃止)
- `anime_analysis` (廃止、`anime` が canonical)
- `scores` (`person_scores` に置換済)
- `va_scores` (`voice_actor_scores` に置換、_naming_decisions/08)
- `data_sources` (`ops_source_scrape_status` に置換)

---

## Python モデル側 (`src/models_v2.py` 書き直し)

- `Anime` クラス 1 つに統合 (_naming_decisions/11)
- `SrcAnilistAnime` 等、ブランドタイポを修正 (_naming_decisions/07)
- `__tablename__` を全クラスで明示、テーブル名と完全一致
- `DisplayLookup` クラス名を `BronzeAccessLog` などにリネーム、helper module (`src/utils/display_lookup.py`) との衝突回避 (_naming_decisions/13)

---

## Steps

### Step 1: 既存 `src/database.py` から canonical 部分だけ抜き出す

```bash
# 現状 init_db の中身を確認、使える DDL を抽出
grep -A200 '^def init_db' src/database.py | less
```

### Step 2: 新 DDL を `src/database_v2.py` に書く

- `init_db_v2(conn)` 関数として実装
- `_naming_decisions/` の決定を全反映
- 旧 `_migrate_v*` は**一切参照しない**
- index と FK は canonical に必要な分だけ (CHECK 制約も書ける範囲で)

### Step 3: 動作確認 (new schema が空 DB に作れる)

```bash
pixi run python -c "
import tempfile, sqlite3, pathlib
from src.database_v2 import init_db_v2
p = pathlib.Path(tempfile.mktemp(suffix='.db'))
conn = sqlite3.connect(p)
init_db_v2(conn)
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print(f'{len(tables)} tables created')
print(sorted(tables))
"
```

---

## Verification (最小限)

```bash
# 1. 新 DDL が空 DB から実行できる
# (Step 3 のスクリプトで確認)

# 2. 禁止テーブル名が含まれていない
rg -n 'anime_display|anime_analysis|va_scores|data_sources' src/database_v2.py
# 期待: 0 件(コメント除く)

# 3. 期待テーブル名が全て含まれている
rg -n 'CREATE TABLE IF NOT EXISTS (anime|persons|credits|sources|roles|voice_actor_scores|ops_lineage)' src/database_v2.py
# 期待: 7 件
```

---

## Completion signal

- [ ] `src/database_v2.py` に `init_db_v2()` が書かれ、空 DB から全テーブル作成成功
- [ ] 禁止テーブル名が含まれていない
- [ ] `_naming_decisions/` の 8 項目が全て反映済み
- [ ] コミット: `Add target schema (init_db_v2) with clean naming`

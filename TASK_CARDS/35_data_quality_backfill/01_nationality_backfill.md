# Task: resolved.persons.nationality 流入路修復

**ID**: `35_data_quality_backfill/01_nationality_backfill`
**Priority**: 🟠 High
**Created**: 2026-05-15
**Estimated changes**: 約 +150 / -20 lines, 4 files
**Requires senior judgment**: yes (scalar vs array スキーマ判断、resolver SQLite→DuckDB 移植)
**Blocks**: `15_extension_reports/04_o4_foreign_talent` 実データ動作、`26_industry_structure/02_international_collab` 実データ動作
**Blocked by**: なし

---

## Goal

`resolved.persons.nationality` を実データから流入させる。現状全件 `'[]'` (seesaa 由来の空 JSON 配列文字列) で nationality 0% 充足、O4 / international_collab 共に動作不能。

---

## Root cause (確定済)

調査結果 (2026-05-15、sonnet agent 報告):

- conformed.persons.nationality の値: anilist / ann / tmdb / mal / bgm = 全行 NULL、seesaa 148,606 行 = `'[]'` (空 JSON array 文字列)
- `src/etl/resolved/_select.py:30-41` の `_is_empty()` は `None` と `""` のみを空扱い、**`'[]'` は非空として通過**
- `_value_validators.is_invalid_for_field()` に `nationality` 用 validator 未定義
- 結果: priority list (anilist → ann → tmdb → mal → bgm → seesaa) で上位 5 source が NULL スキップされ、seesaa の `'[]'` が「唯一の非空値」として採用される
- 上流: anilist scraper `parse_anilist_native_name()` → `infer_nationalities()` が hometown → ISO コード推定実装済だが、結果は `PersonRow.nationality` (list) に保存されるのみ、**conformed.persons.nationality 列への書き込みパス無し**
- `db/rows.py:64` で `nationality: str = "[]"` がデフォルト値 → seesaa scraper が空 list を JSON 化して BRONZE に書き、`seesaawiki.py:207` の COALESCE で conformed に流入

---

## Hard constraints

- **H3**: entity resolution merge logic 不変 (本タスクは select_representative_value の validator 強化のみ)
- スキーマ変更時は migration plan を明示 (resolved.persons.nationality は現状 VARCHAR scalar)
- 影響を受けるのは下流レポート (O4 / international_collab) のみ。AKM / scoring の input には nationality 不使用

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `result/resolved.duckdb` バックアップ
- [ ] baseline 確認: `SELECT nationality, COUNT(*) FROM resolved.persons GROUP BY 1` で `'[]'` 146,211 件 + NULL 459,094 件

---

## 修正方針 (4 step)

### Step 1: `_value_validators` に nationality validator 追加

```python
# src/etl/resolved/_value_validators.py
def _is_invalid_nationality(value: Any) -> bool:
    """空 JSON array '[]' や空 list を invalid 扱い。"""
    if isinstance(value, str):
        s = value.strip()
        if s in ("[]", "{}"):
            return True
        if s.startswith("["):
            import json
            try:
                return len(json.loads(s)) == 0
            except json.JSONDecodeError:
                return False
    if isinstance(value, (list, tuple)):
        return len(value) == 0
    return False
```

`is_invalid_for_field("nationality", v)` で呼出。

### Step 2: anilist conformed loader に persons.nationality UPDATE パス追加

`src/etl/conformed_loaders/anilist.py` (または相当 module):

- BRONZE parquet `persons.nationality` 列 (anilist scraper が `json.dumps(list)` で書いた値) を読込
- conformed.persons.nationality を `COALESCE(target.nationality, source.nationality)` で更新
- 非空 JSON array のみ採用 (`json.loads(v)` が non-empty list)

同様に tmdb / ann / mal scraper が hometown を持つ場合は `nationality_resolver.infer_from_hometown(hometown_text)` で ISO コード推定して書込 (オプション、別 step に切出可)。

### Step 3: nationality_resolver の DuckDB 対応

`src/analysis/network/nationality_resolver.py:load_nationality_records()` は現状 SQLite 接続前提。Resolved 層は DuckDB なので以下 2 択:

(a) DuckDB 版 `load_nationality_records_duckdb(conn: duckdb.DuckDBPyConnection)` 追加
(b) 既存関数を `Union[sqlite3.Connection, duckdb.DuckDBPyConnection]` 対応に統合

推奨 (a): SQLite 版を deprecated として残し、新コード passes DuckDB。

### Step 4: スキーマ正規化 (scalar vs array 決定)

resolved.persons.nationality の型を確定:

- **案 A (scalar VARCHAR)**: 代表値選抜後 `json.loads(v)[0]` で第 1 国籍のみ採用、JSON array で来た値を flatten
- **案 B (TEXT[] / JSON)**: 複数国籍 (二重国籍 / 中華圏 CN+TW 等) を保持、resolver で集約

推奨: **案 A**。下流 O4 / international_collab は国籍グループ判定 (JP/CN/KR/SE_ASIA/OTHER) しか使わない。複雑度上昇に見合う情報量ゲイン薄い。多重国籍は別途 metadata 列で扱う余地を残す。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/resolved/_value_validators.py` | `nationality` field 用 `_is_invalid_nationality()` 追加 + `is_invalid_for_field()` dispatch 登録 |
| `src/etl/conformed_loaders/anilist.py` | persons UPDATE: BRONZE `nationality` (JSON array str) → conformed.persons.nationality 非空時のみ COALESCE 書込 |
| `src/analysis/network/nationality_resolver.py` | DuckDB connection 対応 (新関数 or 既存統合) |
| `src/etl/resolved/resolve_persons.py` | 代表値選抜後の scalar 化 (JSON array → 第 1 要素) — 案 A 採用時 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/db/rows.py` | `nationality: str = "[]"` default は seesaa 旧仕様、scraper 全体の互換性影響大、別タスクで整理 |
| `src/analysis/entity_resolution.py` | H3 |
| 既存 AKM / scoring pipeline | nationality 非依存 |

---

## Files to create

| File | 内容 |
|------|------|
| `tests/etl/resolved/test_nationality_validator.py` | `'[]'` / `'[\"JP\"]'` / `'invalid'` / `None` 各 case の `is_invalid_for_field("nationality", v)` 結果検証 |
| `tests/etl/conformed_loaders/test_anilist_nationality_update.py` | BRONZE nationality JSON → conformed.persons.nationality flow の smoke test |
| `tests/unit/test_nationality_resolver.py` (existing gap、O4 検証時に未と判明) | resolver 単体テスト |

---

## Verification

```bash
# 1. lint + test
pixi run lint
pixi run test-scoped tests/etl/resolved/test_nationality_validator.py \
                     tests/etl/conformed_loaders/test_anilist_nationality_update.py \
                     tests/unit/test_nationality_resolver.py

# 2. ETL 再走 (resolve_persons)
pixi run python -m src.etl.resolved.resolve_persons  # 既存 entry point に合わせる

# 3. resolved.persons.nationality 充足率 確認
pixi run python -c "
import duckdb
c = duckdb.connect('result/resolved.duckdb', read_only=True)
print(c.execute(\"SELECT nationality, COUNT(*) FROM resolved.persons GROUP BY 1 ORDER BY 2 DESC LIMIT 20\").fetchall())
print('non-empty rate:', c.execute(\"SELECT 100.0*COUNT(*) FILTER (WHERE nationality NOT IN ('[]','') AND nationality IS NOT NULL) / COUNT(*) FROM resolved.persons\").fetchone())
"
# 期待: '[]' = 0、JP/CN/KR/... の値が非ゼロ件出現、非空率 > 10%

# 4. O4 / international_collab レポート再生成で nationality 由来 group 出現確認
pixi run python -m scripts.generate_reports --only o4_foreign_talent
pixi run python -m scripts.generate_reports --only structure_international
```

---

## Success criteria

- [ ] resolved.persons.nationality に `'[]'` 行ゼロ
- [ ] 非空 nationality 行数 ≥ 50,000 (anilist 15,139 hometown + tmdb hometown 51,785 推定 → 国コード変換後)
- [ ] O4 report: JP 以外の group (CN / KR / SE_ASIA / OTHER) が violin に出現
- [ ] international_collab report: 海外比率が時系列で非ゼロ
- [ ] nationality_resolver 単体テスト >= 10 件 pass
- [ ] 既存全テスト不退行 (test-scoped で関連 module 全 pass)

---

## Stop-if

- スキーマ scalar/array 判断が未確定のまま実装 → 必ず先に Step 4 で決定し PR 説明に明記
- anilist conformed loader 改修で BRONZE re-ingest が必要になった場合 → 別タスクに切出 (本タスクは UPDATE のみ)
- nationality_resolver の SQLite → DuckDB 移植で既存呼出 (O4 / international_collab) が動かなくなる → 旧関数残してデュアル運用

---

## Rollback

```bash
git checkout src/etl/resolved/_value_validators.py \
             src/etl/conformed_loaders/anilist.py \
             src/analysis/network/nationality_resolver.py \
             src/etl/resolved/resolve_persons.py
mv result/resolved.duckdb.bak.before-nationality-fix result/resolved.duckdb
```

---

## 関連

- `15_extension_reports/04_o4_foreign_talent` (本タスクの主下流、現状 nationality=UNKNOWN 一色で動作)
- `26_industry_structure/02_international_collab` (同上)
- `19_resolved_cluster_fix/02_persons_tmdb_homonym` (validator 強化と相補)
- `_value_validators.py` 既存 invalid logic との整合 (役職 suffix / 機関名等)

---

## ステークホルダー

- 経産省コンテンツ産業課 (海外人材比率データの提供根拠)
- 海外スタジオ (Studio Mir / Toei Phils 等) 個人クレジット可視化

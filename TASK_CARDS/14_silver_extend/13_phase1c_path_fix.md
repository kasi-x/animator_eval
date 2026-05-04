# Task: Phase 1c path 統一 + orphan v2 (mal/keyframe/sakuga 真 fix)

**ID**: `14_silver_extend/13_phase1c_path_fix`
**Priority**: 🔴 (Phase 1c 移行漏れの後遺症)

---

## 問題発覚 (2026-05-05)

Phase 1c で `silver.duckdb` → `animetor.duckdb` (conformed schema) に統合したが、**`integrate_duckdb.py` の path 更新漏れ** が判明:

- `src/etl/integrate_duckdb.py:38-39` で `DEFAULT_SILVER_PATH = "result/silver.duckdb"` のまま
- `src/analysis/io/conformed_reader.py` は新 `animetor.duckdb` を参照
- 結果: ETL は **silver.duckdb (旧)** に書込み、reader は **animetor.duckdb (新)** から読込 → loader 修正が反映されない見え方
- 仮復旧: silver→animetor.conformed の手動コピー (毎回必要)

## 課題リスト

### Issue 1: integrate_duckdb path 更新

`integrate_duckdb.DEFAULT_SILVER_PATH` を `result/animetor.duckdb` に変更 + schema を `main` でなく `conformed` に書込む対応。

ただし integrate は schema 変更を多用 (`CREATE TABLE`、`ALTER TABLE`、`INSERT`) するため `SET schema='conformed'` ではなく **明示的 `conformed.<table>` prefix** で書く方が安全。

### Issue 2: mal persons 0

仮復旧後数値: `mal persons = 0`
- BRONZE: `result/bronze/source=mal/table=persons/` 不在
- 14/12 agent: `_PERSONS_FROM_CREDITS_SQL` で staff_credits + va_credits から抽出する SQL 追加
- 失敗: integrate が silver.duckdb に書いたが animetor へコピー時に schema mismatch or 列不足で skip 推定

検証要:
- 14/12 commit (`116299d`) の mal.py で `_PERSONS_FROM_CREDITS_SQL` が走るか
- silver.duckdb 内 `mal:p%` row count
- silver で 0 なら code bug、silver にあり animetor で 0 なら resync bug

### Issue 3: keyframe anime/persons 0

- BRONZE: `result/bronze/source=keyframe/table=anime/`、`persons/` 存在
- 14/12 agent: 動的 SQL builder で BinderException 解消したと報告
- 仮復旧後 silver.duckdb で `kf:%` 0 → code bug 確実

検証 + 修正:
- 動的 builder が実 BRONZE schema で正しい SQL 生成しているか
- `kf:a<id>` / `kf:p<id>` の prefix 規約確認 (or `keyframe:` 等?)

### Issue 4: sakuga_atwiki anime 0

- 14/12 agent: `pages WHERE page_kind='work'` から `sakuga:a<page_id>` で INSERT 追加
- 仮復旧後 0 → code bug

検証要: pages テーブル schema、page_kind フィルタ。

---

## 修正範囲

| File | 内容 |
|------|------|
| `src/etl/integrate_duckdb.py` | DEFAULT_SILVER_PATH → `result/animetor.duckdb`、INSERT 文に `conformed.` prefix 追加 |
| `src/etl/conformed_loaders/mal.py` | persons fallback SQL の動作確認 + 修正 |
| `src/etl/conformed_loaders/keyframe.py` | 動的 SQL builder の bug fix |
| `src/etl/conformed_loaders/sakuga_atwiki.py` | anime INSERT の bug fix |
| `tests/` | 各 source の row count 検証強化 |

---

## 完了条件

- ETL 単独実行 (`pixi run python -m src.etl.integrate_duckdb`) で:
  - mal persons > 100,000
  - keyframe anime > 1,000、persons > 5,000
  - sakuga_atwiki anime > 100
- silver→animetor の **手動コピー不要** (integrate が直接 animetor.conformed に書く)
- AKM 再計算で connected_set / n_observations 大幅増加
- 14/12 で起票した課題が真に解消

---

## 参考

- 14/12 commit: `116299d` (5 source 一括だが効果不完全)
- 14/11 commit: `0fcdaef` (ANN は成功)
- 仮復旧経路: `result/animetor.duckdb` の `conformed` schema を `silver.duckdb` の `main` schema からコピー

# Task: TMDb BRONZE → Conformed 統合

**ID**: `14_silver_extend/09_tmdb_conformed`
**Priority**: 🟠

## 動機

TMDb BRONZE scrape 済 (`6b99f16`) だが conformed 未統合:
- `result/bronze/source=tmdb/table=anime/` 79,658 行
- `table=persons/` 293,115 行
- `table=credits/` (要確認)

統合で得られるもの:
- **alternative_titles** + **translations** (国別・言語別 title) → IMDb Akas 相当
- `imdb_id` / `tvdb_id` mapping → IMDb への link out + cross-source ID linkage
- TMDb 国際配信メタ → O8 soft_power 充実
- TMDb credits → AKM connected_set 拡大期待

## 範囲

- 新規: `src/etl/conformed_loaders/tmdb.py`
- 修正: `src/db/schema.py` 末尾に tmdb extension (`alternative_titles_json` / `translations_json` / `imdb_id` / `tvdb_id` 列、`source='tmdb'` row 用)
- 修正: `tests/test_etl/test_silver_tmdb.py` (新規)
- 修正: `src/etl/integrate_duckdb.py` で tmdb dispatcher 追加

## ID 規約

- anime: `tmdb:m<media_type>:<id>` (例 `tmdb:movie:12345`、TMDb は movie/tv 分離)
  - or `tmdb:a<id>` 統一でも可、内部 `media_type` 列で識別
- person: `tmdb:p<id>`
- credit `evidence_source = 'tmdb'`

## H1 隔離

TMDb の `vote_average` / `popularity` / `vote_count` は `display_*_tmdb` prefix のみ。
scoring 経路には絶対入れない。

## 完了条件

- `pixi run lint` clean
- `pixi run test-scoped tests/test_etl/test_silver_tmdb.py` pass
- `conformed.anime` に `tmdb:` prefix row 大量追加 (79K)
- `conformed.persons` に `tmdb:` prefix row 追加 (293K)
- `imdb_id` 列 で IMDb URL 構成可能 sample 確認
- AKM 再計算で connected_set 増加

# 08_bangumi_scraper

bangumi.tv (https://bgm.tv) を新規 BRONZE ソースとして統合。

## 方針

**Archive dump 方式**。公式 `bangumi/Archive` リポジトリが subject / person / character / relation を週次 jsonlines dump として配布している。

- https://github.com/bangumi/Archive
- release zip を DL → 展開 → jsonlines → BRONZE parquet

API sequential scrape は rate limit が kitsui (5xx/公序良俗で疎な spec)。dump で一撃取得 → 差分のみ `/v0` API で cron 更新する hybrid が最小コスト。

## BRONZE 出力

```
result/bronze/source=bangumi/table={subjects,subject_persons,subject_characters,persons,characters}/date=YYYYMMDD/*.parquet
```

## Phase 構成

| Card | Phase | 優先 | 内容 |
|---|---|---|---|
| `01_archive_dl.md` | 1 | 🔴 | dump zip DL + 展開 + manifest 書き出し |
| `02_subjects_parquet.md` | 2 | 🔴 | subject.jsonlines → `src_bangumi_subjects` (type=2 anime のみ) |
| `03_subject_relations.md` | 3+4 | 🔴 | `subject-persons.jsonlines` / `subject-characters.jsonlines` → relation parquet |
| `04_person_detail.md` | 5 | 🟠 | person.jsonlines → `src_bangumi_persons` (relation 参照 id のみ filter) |
| `05_character_detail.md` | 6 | 🟢 | character.jsonlines → `src_bangumi_characters` (最後回し) |
| `06_incremental_update.md` | 7 | 🟢 | 差分更新 API (日次 cron、任意) |

## Hard constraints

- **H1**: `score`, `rank`, `rating` 等 viewer-facing 指標は parquet に raw で保存してよいが scoring path 流入禁止
- **H3**: entity resolution 不変 (既存 AniList/MAL/ANN の五段階に bangumi は一次ソースとして後続で追加、この card では触らない)
- **破壊的操作禁止**: 既存 BRONZE parquet 上書きしない (`date=YYYYMMDD` パーティション分離で共存)

## 生データ保全方針

- role label (中文「导演」「原画」等) は **raw 文字列のまま** parquet column に保存
- role_groups.py への正規化 mapping は SILVER 移行タスクで別途起票 (この card 群では触らない)
- 日本語題 / 中文題 / 英題 は全列保全 (`name`, `name_cn`, `name_ja` 等 dump column 名をそのまま継承)

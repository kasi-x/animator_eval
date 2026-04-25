# 08_bangumi_scraper

bangumi.tv (https://bgm.tv) を新規 BRONZE ソースとして統合。

## 方針 (2026-04-25 修正: hybrid)

当初 **Archive dump 一括方式** で設計したが、**実環境検証で dump は subject.jsonlines のみ含む** と判明 (README は 9 種類記載だが weekly zip は subject のみ、local file header scan で 1 member 確認済)。

→ 新方針: **subject は dump、relation/person/character は `/v0` API scrape**。

| データ | 取得方法 | 根拠 |
|---|---|---|
| subject (anime メタ) | Archive dump (週次) | weekly zip 内 `subject.jsonlines` 218,994 行 (全 type)、type=2 anime ~3-5k |
| subject × persons 関係 | `/v0/subjects/{id}/persons` | anime subject のみ ~3-5k req |
| subject × characters 関係 (+ 声優 actors ネスト) | `/v0/subjects/{id}/characters` | 同上 |
| person detail | `/v0/persons/{id}` | relation 参照 id のみ |
| character detail | `/v0/characters/{id}` | relation 参照 id のみ |

rate limit: **1 req/sec** 厳守 (bangumi ガイド曖昧 → 安全側)。anime 限定なので初回 backfill ~5-8 時間で完走想定。

## BRONZE 出力

```
result/bronze/source=bangumi/table={subjects,subject_persons,subject_characters,persons,characters}/date=YYYYMMDD/*.parquet
```

## Phase 構成

| Card | 方式 | 優先 | 内容 |
|---|---|---|---|
| `01_archive_dl.md` ✅ | dump | 🔴 | dump zip DL + 展開 + manifest 書き出し (commit 9d3578e) |
| `02_subjects_parquet.md` ✅ | dump | 🔴 | subject.jsonlines → `src_bangumi_subjects` (type=2 anime のみ、commit 84dda39) |
| `03_subject_relations.md` | **API** | 🔴 | `/v0/subjects/{id}/persons` + `/characters` → subject_persons / subject_characters / person_characters 3 parquet |
| `04_person_detail.md` | **API** | 🟠 | `/v0/persons/{id}` → `src_bangumi_persons` (relation 参照 id 集合のみ) |
| `05_character_detail.md` | **API** | 🟢 | `/v0/characters/{id}` → `src_bangumi_characters` (最後回し) |
| `06_incremental_update.md` | **API** | 🟢 | 差分更新 (週次 dump diff + 関係 re-scrape、cron) |
| `08_image_download.md` | **CDN DL** | 🟢 | BRONZE 画像 URL → local files + image_manifest BRONZE parquet |
| `09_graphql_migration.md` | **GraphQL** | 🟢 | GraphQL クライアント実装 (主経路化) + v0 fallback |

## Hard constraints

- **H1**: `score`, `rank`, `rating` 等 viewer-facing 指標は parquet に raw で保存してよいが scoring path 流入禁止
- **H3**: entity resolution 不変 (既存 AniList/MAL/ANN の五段階に bangumi は一次ソースとして後続で追加、この card では触らない)
- **破壊的操作禁止**: 既存 BRONZE parquet 上書きしない (`date=YYYYMMDD` パーティション分離で共存)

## 関連ドキュメント

- [`08_image_download.md`](08_image_download.md) — 画像 DL フェーズ (Card 01-05 全完了後に着手)
- [`09_graphql_migration.md`](09_graphql_migration.md) — GraphQL クライアント実装 (主経路化・v0 フォールバック設計)
- [`FUTURE.md`](FUTURE.md) — 未取得 bangumi 公開情報の整理 (エンドポイント / コスト / 判断マトリックス)

## 生データ保全方針

- role label (中文「导演」「原画」等) は **raw 文字列のまま** parquet column に保存
- role_groups.py への正規化 mapping は SILVER 移行タスクで別途起票 (この card 群では触らない)
- 日本語題 / 中文題 / 英題 は全列保全 (`name`, `name_cn`, `name_ja` 等 dump column 名をそのまま継承)

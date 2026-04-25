# Card 09: bangumi GraphQL クライアント実装

## 目標

bangumi GraphQL endpoint (`https://api.bgm.tv/v0/graphql`) を主経路として採用し、
v0 REST クライアントをレガシーフォールバック (`--client v0`) に格下げする。

主な効率化:
- `scrape_bangumi_relations.py`: batch_size=25 で 1 POST → ~25x スループット
- `scrape_bangumi_persons.py` / `scrape_bangumi_characters.py`: 単一 GraphQL POST (v0 と同等だが将来の batch 化を見越した基盤)

## 前提条件 (Pre-conditions)

- Card 01–05 の backfill 完走後に A/B 検証を実施 (v0 出力と GraphQL 出力の一致確認)
- PID 2817958 の v0 backfill 完走後に GraphQL を本番経路に切り替え
- 本 Card 実装は backfill 並走中でも安全 (新ファイルを追加・変更するのみ、既存プロセスは影響なし)

## 実装済みファイル (2026-04-25)

| ファイル | 役割 |
|---|---|
| `src/scrapers/queries/bangumi_graphql.py` | クエリ文字列定数 (`SUBJECT_FULL_QUERY`, `SUBJECT_BATCH_QUERY`, `PERSON_QUERY`, `CHARACTER_QUERY`) |
| `src/scrapers/bangumi_graphql_scraper.py` | `BangumiGraphQLClient` + response adapter 関数群 |
| `src/scrapers/bangumi_scraper.py` | `_HOST_RATE_LIMITER` (module-level 共有 limiter) 追加、DEPRECATED docstring 追加 |
| `scripts/scrape_bangumi_relations.py` | `--client graphql|v0` フラグ追加 (default: graphql) |
| `scripts/scrape_bangumi_persons.py` | `--client graphql|v0` フラグ追加 |
| `scripts/scrape_bangumi_characters.py` | `--client graphql|v0` フラグ追加 |
| `tests/scrapers/test_bangumi_graphql_scraper.py` | 15 テストケース (mock ベース) |

## スキーマ参照

- 公式リポジトリ: https://github.com/bangumi/server-private
- Altair UI (ブラウザ): https://api.bgm.tv/v0/altair/
- Introspection 有効 — `{__schema{queryType{name}}}` で疎通確認可

## Hard Constraints

- **H1**: `SubjectRating.score / rank / total` は BRONZE display metadata のみ。
  GraphQL レスポンスに含まれるが scoring path には流入させない。
  `bangumi_graphql.py` の `SUBJECT_FULL_QUERY` / `SUBJECT_BATCH_QUERY` にコメントで明記済み。

- **レート予算共有**: GraphQL client と v0 REST client は `_HOST_RATE_LIMITER`
  (`bangumi_scraper.py` の module-level singleton) を共有する。
  両クライアントが同時に動作しても `api.bgm.tv` への総 req/sec は 1 を超えない。

- **BRONZE immutability**: GraphQL path も UUID parquet 書き込み方式を踏襲。
  既存 v0 出力ファイルは上書きしない。

## Stop-if Conditions

- GraphQL endpoint が HTTP 401/403 を返す → 認証要件の変更を疑い作業停止
- `/v0` 以外のパスでレート制限が別カウントになっている証拠が出た場合 → limiter 設計見直し
- バッチクエリで `allowBatchedQueries` エラーが返る → batch_size=1 にフォールバックして報告

## Rollback

```bash
# 即座にフォールバック
pixi run python scripts/scrape_bangumi_relations.py --client v0 ...
pixi run python scripts/scrape_bangumi_persons.py   --client v0 ...
pixi run python scripts/scrape_bangumi_characters.py --client v0 ...
```

v0 REST client は削除せず `bangumi_scraper.py` に保持する。

## 今後のタスク (Card 10 以降への引き継ぎ)

1. **A/B 検証**: v0 backfill 完走後に GraphQL path で同一 subject_id を再取得し、
   BRONZE parquet の行内容を比較 (id / name / persons count 等)。
2. **Card 06 (incremental_update) への統合**: 差分更新スクリプトも `--client graphql` に切り替え。
   batch_size=25 で週次差分 (~数十件) を 1-2 req で処理可能になる。
3. **pagination 対応**: persons > 50 件 / characters > 50 件の subject は
   `persons(limit:50, offset:50)` 等で追加取得が必要。現状は offset=0 のみ。

## 検証コマンド

```bash
# テスト (mock ベース、実 API 不使用)
pixi run test-scoped tests/scrapers/test_bangumi_graphql_scraper.py tests/scrapers/test_bangumi_scraper.py -v

# Lint
pixi run lint

# Dry-run 動作確認 (実 API 不使用)
pixi run python scripts/scrape_bangumi_relations.py --client graphql --dry-run --limit 5
pixi run python scripts/scrape_bangumi_relations.py --client v0      --dry-run --limit 5
pixi run python scripts/scrape_bangumi_persons.py   --client graphql --dry-run --limit 5
pixi run python scripts/scrape_bangumi_characters.py --client graphql --dry-run --limit 5
```

*作成: 2026-04-25*

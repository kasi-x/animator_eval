# 13_scraper_runner_refactor — scraper 共通化第 3 弾

**Owner**: Sonnet
**Started**: 2026-04-25
**Goal**: scraper 6 箇所 (allcinema / ann / mal / anilist / keyframe / seesaawiki) で重複している `_run_scrape_*` ループ骨格を共通 `ScrapeRunner` に抽出。HTTP client 二重定義解消、cache_store 全 fetcher 拡張、BronzeSink パターン化を含む。

## 背景

11_scraper_unification 完了後、共通基盤 (`http_client` / `checkpoint` / `cli_common` / `bronze_writer` / `cache_store` / `progress`) は揃ったが、**ループ部分は依然として各 scraper で再実装**。重複パターン:

```
1. ID 列挙 (sitemap/masterlist/search/bronze 参照)
2. checkpoint resolve → pending 計算
3. for id in pending:
     fetch → parse → bronze.append → completed.add → progress.advance
     if N% == 0: flush + cp.save
4. final flush + cp.save + compact
```

allcinema/ann/mal/keyframe/seesaawiki で骨格ほぼ同型。差は fetch URL と parser のみ。

## 設計 (3 層分離)

```
Source   : ID 列挙          → list[ID]   (Runner 外部、scraper 固有)
Fetcher  : ID → raw payload (cache + retry + rate)
Parser   : raw → Record     (純関数、既に parsers/ に分離済、触らない)
Sink     : Record → BRONZE  (asdict + hash + writer)
Runner   : 上 4 つ + ループ + checkpoint + progress
```

## サブカード

| Card | Title | Priority | Risk | Independence |
|---|---|---|---|---|
| `01_http_client_dedupe` | `RetryingHttpClient` 二重定義の統合 | 🔴 | 中 (anilist 挙動繊細) | 独立 |
| `02_cache_store_expansion` | `cache_store` を全 fetcher に拡張 + `Fetcher` ヘルパ抽出 | 🟠 | 低 | 01 後 |
| `03_bronze_sink` | `BronzeSink` パターン化 (asdict + hash 自動付与) | 🟠 | 低 | 独立 |
| `04_runner_abstraction` | `ScrapeRunner` 抽象 + 全 scraper 移行 | 🔴 | 高 (6 scraper 触る) | 02 + 03 後 |

## 実行順

1. `01_http_client_dedupe` (独立、anilist の `retrying_http_client.py` 削除で 174 行減)
2. `02_cache_store_expansion` (Fetcher ヘルパ整理。cache を全 GET に効かせる)
3. `03_bronze_sink` (Sink クラス整理)
4. `04_runner_abstraction` (Runner 本体 + 各 scraper 移行)

## 共通の Hard Rules

- **既存 CLI flag 名を破壊しない** (本番 scrape が並列実行中の可能性)
- **rate limit / retry policy を緩めない** (各 scraper の現在値を維持)
- **既存テスト pass** が invariant
- **`anime.score` を scoring に使わない** (`_hard_constraints.md` H1)
- **structlog 必須** (stdlib logging 不使用)
- **test 走らせ方**: `pixi run test-scoped tests/test_<file>.py`。フル `pixi run test` は最後の `04` verify 時のみ
- **dead code 即削除** (memory feedback): `retrying_http_client.py` 削除等

## 移行対象外 (今回触らない)

- **mal**: 30 endpoint × sub-endpoint で骨格特殊。Runner 適合困難
- **anilist**: GraphQL pagination が特殊。`01` の `RetryingHttpClient` 統合のみ
- **seesaawiki**: LLM fallback あり、特殊処理多い
- **bangumi_main**: 直前のコミット (`4bdf4cf`) で既にリファクタ済

`04` で **allcinema / ann / keyframe** のみ Runner 移行。残りは後続カードで判断。

## 期待効果

- `retrying_http_client.py` (174 行) 削除
- 各 scraper の `_run_scrape_*` 関数: 80〜100 行 → 30〜50 行
- 新 source 追加コスト: parser + fetcher 設定 30 行程度

# FUTURE: bangumi 公開情報の未取得範囲

現在の Card 01-08 で取得している情報の**外側**にある bangumi 公開データを整理する。
各項目について: エンドポイント/ソース、レスポンス形状スケッチ、スキーマ設計案、コスト見積もり、価値評価、判断ステータスを記載する。

---

## 1. 未着手 API エンドポイント

### 1-A. Subject 間関係 (`/v0/subjects/{id}/subjects`)

**エンドポイント**: `GET /v0/subjects/{id}/subjects`

**レスポンス形状**:
```json
[
  {
    "id": 3437,
    "name": "続・はじめの一歩",
    "relation": "sequel",
    "type": 2
  },
  ...
]
```

**スキーマ設計案** (`table=subject_relations`):
```
subject_id:    int64    # from subject
related_id:    int64    # related subject
relation_type: string   # "sequel" / "prequel" / "alternate" / "side_story" など
related_type:  int32    # subject type (2=anime)
fetched_at:    timestamp
```

**コスト**: anime subject ~3-5k × 1 req/sec ≈ 50-80 分 (初回 backfill)。週次差分 ~数十件。

**価値**: `HIGH` — 続編/前日譚ネットワークはスタッフ継続性の追跡に使える (同一製作チームが
どの作品に継続参加するか)。シリーズ集約ノード構築にも必要。

**判断**: DEFER — AniList の `relations` フィールドで代替可能。bangumi 固有の関係タイプ
(中文圏独自分類) が必要になったタイミングで追加実装。

---

### 1-B. エピソード一覧 (`/v0/episodes?subject_id=X`)

**エンドポイント**: `GET /v0/episodes?subject_id={id}&type=0&limit=100&offset=0`

**レスポンス形状**:
```json
{
  "data": [
    {
      "id": 1234,
      "type": 0,
      "name": "第1話",
      "name_cn": "第1集",
      "sort": 1.0,
      "ep": 1,
      "airdate": "2006-10-06",
      "duration": "00:23:40",
      "desc": "...",
      "disc": 0,
      "subject_id": 1234,
      "comment": 42
    }
  ],
  "total": 26,
  "limit": 100,
  "offset": 0
}
```

**スキーマ設計案** (`table=episodes`):
```
id:          int64
subject_id:  int64
type:        int32     # 0=本编, 1=特别篇, 2=OP, 3=ED, 4=预告...
sort:        float64
ep:          int32
name:        string
name_cn:     string | null
airdate:     date | null
duration:    string | null  # "00:23:40" 形式 raw 保存
comment:     int32
fetched_at:  timestamp
```

**コスト**: anime subject ~3-5k × 複数ページ (pagination 必要) × 1 req/sec ≈ 1.5-3 時間。
週次差分は新 subject のみなので ~数分。

**価値**: `MEDIUM` — 話数・尺は production_scale 計算に使えるが AniList `episodes` + `duration`
フィールドで概算可能。bangumi の実測 `duration` は OVA/特別篇の精度が高い点で優位。

**判断**: DEFER — AniList duration で不足が生じた場合に追加。

---

### 1-C. Subject detail diff (dump vs API gap)

**エンドポイント**: `GET /v0/subjects/{id}`

dump の `subject.jsonlines` と `/v0/subjects/{id}` レスポンスの差分フィールド:

| フィールド | dump に含まれるか | v0 API のみ |
|---|---|---|
| `tags` (ユーザータグ + count) | NO | YES |
| `collection` (収集状態別 count) | NO | YES |
| `rating` (評価分布 + score + count) | 含まれる | 同等 |
| `images` (全バリアント) | YES | YES (同等) |
| `infobox` (展開済み) | YES (raw wiki) | YES (同等) |

**取得対象**: `tags` (ユーザータグ) と `collection` (収集者数) の 2 フィールドのみ追加取得する価値あり。

**コスト**: ~3-5k req × 1 req/sec ≈ 50-80 分。

**価値**: `LOW` — `tags` はジャンル推論に使えるが AniList genres/tags で代替可能。
`collection` は人気指標 (scoring 禁止フィールド相当) → BRONZE には保存してよいが scoring path 流入禁止。

**判断**: NEVER (scoring 用途不可) / DEFER (display 用途のみなら低優先)。

---

### 1-D. Person → Subject 逆引き (`/v0/persons/{id}/subjects`)

**エンドポイント**: `GET /v0/persons/{id}/subjects`

**レスポンス形状**:
```json
[
  {
    "id": 328,
    "name": "...",
    "staff": "原画",
    "subject_id": 328,
    "subject": { "id": 1, "name": "..." }
  }
]
```

**価値**: `LOW` — subject_persons (Card 03) の逆引きと等価。追加情報なし。

**判断**: NEVER — subject_persons から JOIN で代替可能、冗長。

---

### 1-E. Person → Character 逆引き (`/v0/persons/{id}/characters`)

**エンドポイント**: `GET /v0/persons/{id}/characters`

**価値**: `LOW` — person_characters (Card 03) の逆引きと等価。

**判断**: NEVER — 同上。

---

### 1-F. Character → Subject 逆引き (`/v0/characters/{id}/subjects`)

**エンドポイント**: `GET /v0/characters/{id}/subjects`

**価値**: `LOW` — subject_characters から JOIN で代替可能。

**判断**: NEVER — 冗長。

---

### 1-G. Character → Person 逆引き (`/v0/characters/{id}/persons`)

**エンドポイント**: `GET /v0/characters/{id}/persons`

**価値**: `LOW` — person_characters の逆引きと等価。

**判断**: NEVER — 冗長。

---

## 2. 画像ダウンロード

Card 08 (`08_image_download.md`) で実装予定。スコープ:

- **対象**: `persons.images.large` + `characters.images.large` のみ (small/grid/medium は除外)
- **推定**: ~35-60k ユニーク URL、10-17 時間、3.5-6 GB
- **前提**: Card 01-05 全 backfill 完走後に着手

詳細は Card 08 を参照。

---

## 3. 外部参照: bangumi/common コードラベル yaml

**リポジトリ**: https://github.com/bangumi/common

ここで管理されているラベル情報:

| ファイル | 内容 | 用途 |
|---|---|---|
| `subject-relations.yaml` | subject 間の関係タイプコード → ラベル (日/中/英) | 1-A の `relation_type` の正規化 |
| `staff-position.yaml` | staff ポジションコード → ラベル (日/中/英) | subject_persons の `position` の正規化 |
| `platform.yaml` | 放送プラットフォームコード → ラベル | subjects の platform 列の正規化 |

**これはスクレイパータスクではない。** yaml を一度ダウンロードして `src/utils/` または
`src/etl/` に配置するだけ。SILVER 移行 (bangumi → SILVER 統合) の**前提タスク**として
別カードで起票する。

**コスト**: GitHub API 1 リクエスト + yaml パース。数秒。

**判断**: DEFER — SILVER 統合カード起票時に同梱する。

---

## 4. 明示的除外 (理由あり)

| 情報 | エンドポイント/ソース | 除外理由 |
|---|---|---|
| ユーザーディスカッション / コメント | `/v0/subjects/{id}/comments`, `/v0/episodes/{id}/comments` | PII (ユーザーIDが含まれる)、ネットワーク分析の対象外 |
| Wiki 改定履歴 | web スクレイプのみ (API 未公開) | scrape コスト高、構造化困難、法的グレーゾーン |
| ユーザーコレクション個別 | `/v0/users/{username}/collections` | PII (個人の視聴履歴)、Permitted Data 外 |
| 個人 blog / SNS | 外部リンク (bangumi infobox 内) | 構造化不可、PII リスク |
| 評価分布 / rating.score | dump / `/v0/subjects/{id}` に含まれる | CLAUDE.md H1: viewer metric、scoring path 流入禁止。BRONZE raw 保存は許可だが使用しない |

---

## 5. 非公開 / レガシー / 半公開 API (探索余地あり)

公式 `/v0` 以外で取得可能な経路。仕様変更リスクや規約解釈グレーゾーン含む → 全て **DEFER 判断**、利用前に bangumi 規約再確認必須。

### 5-A. レガシー API (`/v0` 接頭辞なし)

- エンドポイント: `https://api.bgm.tv/{subject,person,character}/{id}?responseGroup=large`
- 状態: 公式公開、deprecated 扱いだが現役 (現サイト UI も参照)
- 価値: 1 req で staff + cast + 評価分布 + topics + blogs + collection 統計まとめて取得 → スループット改善
- リスク: 仕様変更で破棄される可能性、現公式ドキュメントは `/v0` のみ
- 用途: rare 補完。`/v0` で取れない統計フィールド (e.g., topics_count) が必要な場合のみ
- コスト: 既存 v0 + dump で代替可能なので追加価値は低
- 判断: **DEFER** (公式 v0 で困った時のみ)

### 5-B. GraphQL (`https://api.bgm.tv/v0/graphql`) — DISCOVERED — Card 09 で実装中

- **状態**: **DISCOVERED — Card 09 として実装中** (2026-04-25)
- **確認済み endpoint**: `https://api.bgm.tv/v0/graphql` (POST, Content-Type: application/json)
  - サーバー: `bangumi/server-private` (Fastify + mercurius)
  - Altair UI: `/v0/altair/` (ブラウザで動作確認済み)
  - Introspection: **ENABLED**
- **スキーマサマリ** (introspection 確認済み):
  - Top-level Query: `me`, `character(id: Int!)`, `person(id: Int!)`, `subject(id: Int!)`
  - 主要型: `Subject`, `Person`, `Character`, `Episode`, `SlimPerson`, `SlimSubject`
  - Subject の paginated sub-resolvers: `persons(limit, offset)`, `characters(limit, offset)`, `relations(limit, offset, includeTypes, excludeTypes)`
  - `SubjectRating` フィールド: `score`, `rank`, `total` — **H1 により scoring path 流入禁止、BRONZE display のみ**
- **バッチクエリ確認済み**: `allowBatchedQueries: true` — N 件を alias (`s100: subject(id:100) {...}`) で 1 POST に束ねられる
- **実装内容** (Card 09):
  - `src/scrapers/queries/bangumi_graphql.py` — クエリ文字列定数
  - `src/scrapers/bangumi_graphql_scraper.py` — `BangumiGraphQLClient` (主経路)
  - orchestrator scripts に `--client graphql|v0` フラグ追加 (default: graphql)
  - レート予算共有: `_HOST_RATE_LIMITER` を v0 client と共用
- **スループット**: batch_size=25 で ~25x (relations backfill)、persons/characters は 1:1 だが latency 削減
- **rollback**: `--client v0` フラグで即座にフォールバック可能
- 詳細は `09_graphql_migration.md` 参照

### 5-C. HTML scrape (`https://bgm.tv/subject/{id}` 系)

- 状態: 公開 web、`robots.txt` 確認必須、rate limit は API 並みに守る
- HTML から取れて API に無いもの:
  - **評価分布** (1-10 のヒストグラム data) — H1 viewer metric なので **scoring 禁止**、display のみ可
  - **編集履歴** (「时光机」/ revision log) — wiki 編集者ネットワーク分析
  - **多役職スタッフ表記** — v0 の `relation: "导演/原画"` 複合 string で大方カバー、HTML はより rich
  - **tag 全件** (公式 API は top N、HTML は全件)
- 価値: 編集履歴は wiki 編集者 → 作品 のメタネットワーク化に使えるが、現用途 (アニメスタッフ評価) からは外れる
- リスク: HTML 構造変更で parser 全壊、scrape 規約違反リスク
- 判断: **DEFER** (編集履歴は将来 wiki 編集者研究時に着手、tag 全件は v0 で不足時のみ)

### 5-D. Search API (`https://api.bgm.tv/search/subject/{keyword}`)

- 状態: 公開だが `/v0` 統合なし
- 価値: keyword 検索 → entity resolution 補強 (AniList / MAL 等の名前から bangumi id 解決)
- 用途: H3 entity resolution で五段階解決の **第 6 段** として組み込み余地
- 判断: **DEFER** (entity resolution 改修時に着手)

### 5-E. モバイルアプリ内部 API (リバースエンジ系)

- 状態: 非公開、リバースエンジニアリングで発掘可能
- 例: `/api/v3/...` 等の追加 endpoint があるとされる
- リスク: **規約違反明確**、ban / 法的問題リスク
- 判断: **NEVER** (触らない方針)

---

## 6. 判断マトリックス

| 情報 | 取得コスト | ネットワーク分析価値 | 判断 |
|---|---|---|---|
| Subject 間関係 (1-A) | 低 (50-80 min) | HIGH (続編継続性) | **DEFER** (AniList で代替中) |
| エピソード詳細 (1-B) | 中 (1.5-3 h) | MEDIUM (尺精度) | **DEFER** (AniList で概算可能) |
| Subject detail diff (1-C) | 低 (50-80 min) | LOW | **NEVER** (scoring 禁止) / DEFER (display) |
| Person/Character 逆引き (1-D〜G) | 低 | LOW (冗長) | **NEVER** |
| 画像 DL (Card 08) | 高 (10-17 h, 3-6 GB) | なし (display 専用) | **NOW** (Card 08) |
| bangumi/common ラベル yaml (3) | 極低 | MEDIUM (正規化精度) | **DEFER** (SILVER 統合と同梱) |
| ユーザーコメント | 中 | なし (PII) | **NEVER** |
| Wiki 改定履歴 | 高 | なし | **NEVER** |
| ユーザーコレクション | 中 | なし (PII) | **NEVER** |
| レガシー API responseGroup=large (5-A) | 低 | LOW (v0+dump で代替) | **DEFER** |
| GraphQL (5-B) | 低 (実装済み) | HIGH (batch ~25x) | **NOW** (Card 09 実装中) |
| HTML 編集履歴 (5-C) | 中-高 (HTML parser) | MEDIUM (将来 wiki 研究) | **DEFER** |
| HTML 評価分布 (5-C) | 低 | なし (H1 違反) | **NEVER** |
| Search API (5-D) | 低 | MEDIUM (ER 補強) | **DEFER** (ER 改修時) |
| モバイル内部 API (5-E) | 低 | — | **NEVER** (規約違反) |

---

*最終更新: 2026-04-25*

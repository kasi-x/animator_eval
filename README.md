# Animetor Eval

> アニメ業界人物評価システム — ネットワーク密度・位置指標に基づく客観的スコアリング

[![Tests](https://img.shields.io/badge/tests-955%20passing-success)](https://github.com/kasi-x/animetor_eval)
[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## 概要

**Animetor Eval** は、アニメ業界の制作者（アニメーター、監督など）を**クレジットデータ**に基づいて評価するシステムです。業界を信頼ネットワークとしてモデル化し、以下の3軸でスコアリングします：

| 軸 | アルゴリズム | 測定対象 |
|---|---|---|
| **Authority（権威）** | Weighted PageRank | 著名な監督・作品との協業距離 |
| **Trust（信頼）** | Repeat Engagement | 同じ監督からの継続起用回数 |
| **Skill（技量）** | OpenSkill (PlackettLuce) | 近年のプロジェクト貢献・成長軌道 |

### 重要な原則

- ⚖️ **客観性**: 公開されたクレジットデータのみを使用
- 🔢 **ネットワーク指標**: スコアは「能力」ではなく「ネットワーク上の位置・密度」を表す
- 🎯 **用途**: 制作スタジオの最適なキャスティング支援（公益目的）
- ⚠️ **法的配慮**: 名寄せの精度は信用毀損リスクに直結するため保守的に実装

## 主な機能

- 📊 **3軸スコアリング**: Authority × Trust × Skill の統合評価
- 🔍 **エンティティ解決**: 5段階の名寄せ（完全一致 → クロスソース → ローマ字 → 類似度 → AI支援）
- 📈 **キャリア分析**: 役職遷移、成長トレンド、マイルストーン検出
- 🕸️ **ネットワーク分析**: コラボレーション強度、監督サークル、ブリッジ検出
- 🎨 **可視化**: 23種のmatplotlib静的チャート + 6種のPlotlyインタラクティブ可視化
- 🚀 **並列実行**: ThreadPoolExecutorによる20モジュール同時実行（4-6倍高速化）
- 🔌 **REST API**: 35エンドポイント（FastAPI）
- 💻 **CLI**: 16コマンド（typer + Rich）

## クイックスタート

### 必要環境

- Python 3.12+
- [pixi](https://pixi.sh/) パッケージマネージャー

### インストール

```bash
# リポジトリをクローン
git clone https://github.com/kasi-x/animetor_eval.git
cd animetor_eval

# 依存パッケージをインストール
pixi install

# テスト実行（955件、約60秒）
pixi run test
```

### パイプライン実行

```bash
# 全パイプライン実行（スコア計算 + JSON出力）
pixi run pipeline

# 可視化付き実行
pixi run pipeline-viz

# データ検証のみ（--dry-run）
pixi run validate
```

### CLI 使用例

```bash
# ランキング表示（トップ20）
pixi run ranking

# 特定人物のプロフィール
pixi run profile "宮崎駿"

# 人物検索
pixi run search "田中"

# 類似人物検索
pixi run similar "person_id_123"

# スコア比較（2人）
pixi run compare "person_1" "person_2"

# キャリアタイムライン
pixi run timeline "person_id_123"

# データベース統計
pixi run stats
```

### API サーバー起動

```bash
pixi run api

# ブラウザで http://localhost:8000/docs にアクセス
# OpenAPI (Swagger) ドキュメントが表示されます
```

## アーキテクチャ

### パイプライン構成（10フェーズ）

```
src/pipeline_phases/
├── data_loading.py          # Phase 1: DBからデータロード
├── validation.py            # Phase 2: データ品質チェック
├── entity_resolution.py     # Phase 3: 名寄せ（5段階）
├── graph_construction.py    # Phase 4: NetworkXグラフ構築
├── core_scoring.py          # Phase 5: Authority/Trust/Skill計算
├── supplementary_metrics.py # Phase 6: 8種の補助メトリクス
├── result_assembly.py       # Phase 7: 結果データ組み立て
├── post_processing.py       # Phase 8: パーセンタイル、信頼区間
├── analysis_modules.py      # Phase 9: 20分析モジュール（並列実行）
└── export_and_viz.py        # Phase 10: JSON出力 + 可視化
```

### グラフモデル

- **ノード**: アニメーター、監督、作品（アニメタイトル）
- **エッジ**: 作品参加関係、役職（24種類）、協業関係
- **エッジ重み**: 監督著名度ボーナス、継続協業ボーナス、役職ベース重み付け

### データソース

- [AniList](https://anilist.co/) GraphQL API
- [Jikan](https://jikan.moe/) (MAL非公式REST API)
- [メディア芸術データベース](https://mediaarts-db.bunka.go.jp/) SPARQL
- [Wikidata](https://www.wikidata.org/) SPARQL (JVMG)

## パフォーマンス最適化

プロジェクトは包括的なリファクタリング（Phase 1-4 + 並列化）を完了し、以下の最適化を実現：

| 最適化 | 手法 | 効果 |
|---|---|---|
| **グラフ構築** | エッジの事前集約 | 3-5倍高速化 |
| **名寄せ** | 先頭文字ブロッキング + LRUキャッシュ | 10-100倍高速化 |
| **Trust計算** | 定数の巻き上げ + 事前計算 | 40-50%高速化 |
| **分析フェーズ** | ThreadPoolExecutor（20並列） | 4-6倍高速化 |
| **API応答** | JSON読み込みLRUキャッシュ | 30-50%高速化 |

## 出力ファイル

### JSON (26ファイル)

```
result/json/
├── scores.json              # 全人物スコア（composite降順）
├── circles.json             # 監督サークル
├── anime_stats.json         # アニメ品質統計
├── summary.json             # パイプライン実行サマリー
├── transitions.json         # 役職遷移分析
├── influence.json           # 影響力ツリー
├── crossval.json            # クロスバリデーション結果
├── studios.json             # スタジオ分析
├── seasonal.json            # 季節トレンド
├── collaborations.json      # 協業ペア（トップ500）
├── outliers.json            # 統計的外れ値
├── teams.json               # チーム構成パターン
├── growth.json              # 成長トレンド
├── time_series.json         # 時系列分析
├── decades.json             # 年代別分析
├── tags.json                # 人物タグ（自動ラベリング）
├── role_flow.json           # 役職フロー分析
├── bridges.json             # ブリッジノード検出
├── mentorships.json         # メンター関係推論
├── milestones.json          # キャリアマイルストーン
├── network_evolution.json   # ネットワーク進化
├── genre_affinity.json      # ジャンル親和性
├── productivity.json        # 生産性メトリクス
├── performance.json         # パフォーマンスモニタリング
├── graphml/                 # GraphMLエクスポート（Neo4j互換）
└── ...
```

### その他

- **CSV**: `scores.csv` (UTF-8 BOM、パーセンタイル付き)
- **SQLite**: `result/db/animetor_eval.db` (スコア履歴、実行履歴)
- **Visualization**: `result/visualizations/*.png` (23種 + 6種HTML)

## 開発

### テスト実行

```bash
pixi run test              # 全テスト（955件）
pixi run lint              # ruff lint
pixi run format            # ruff format
```

### 合成データ生成

```bash
# テスト・デモ用の合成データ生成
pixi run python -c "
from src.synthetic import generate_synthetic_data
persons, anime, credits = generate_synthetic_data(
    n_directors=5,
    n_animators=30,
    n_anime=15
)
print(f'Generated {len(persons)} persons, {len(anime)} anime, {len(credits)} credits')
"
```

### Jupyter Lab

```bash
pixi run lab
# 分析ノートブックは result/notebooks/ に保存
```

## API エンドポイント

35エンドポイントを提供（詳細は http://localhost:8000/docs 参照）：

### 人物関連
- `GET /api/v1/persons` - 全人物スコア一覧（ページネーション）
- `GET /api/v1/persons/search` - 人物検索
- `GET /api/v1/persons/{id}` - プロフィール詳細
- `GET /api/v1/persons/{id}/similar` - 類似人物
- `GET /api/v1/persons/{id}/history` - スコア履歴
- `GET /api/v1/persons/{id}/network` - ネットワーク分析
- `GET /api/v1/persons/{id}/milestones` - キャリアマイルストーン

### ランキング・統計
- `GET /api/v1/ranking` - ランキング（フィルタ対応）
- `GET /api/v1/stats` - DB統計
- `GET /api/v1/summary` - パイプラインサマリー
- `GET /api/v1/data-quality` - データ品質レポート

### アニメ関連
- `GET /api/v1/anime` - アニメ統計一覧
- `GET /api/v1/anime/{id}` - アニメ詳細

### 分析
- `GET /api/v1/transitions` - 役職遷移
- `GET /api/v1/crossval` - クロスバリデーション
- `GET /api/v1/influence` - 影響力ツリー
- `GET /api/v1/studios` - スタジオ分析
- `GET /api/v1/seasonal` - 季節トレンド
- `GET /api/v1/collaborations` - 協業ペア
- `GET /api/v1/outliers` - 外れ値検出
- `GET /api/v1/teams` - チーム分析
- `GET /api/v1/growth` - 成長トレンド
- `GET /api/v1/time-series` - 時系列
- `GET /api/v1/decades` - 年代別
- `GET /api/v1/tags` - 人物タグ
- `GET /api/v1/role-flow` - 役職フロー
- `GET /api/v1/bridges` - ブリッジ検出
- `GET /api/v1/mentorships` - メンター関係
- `GET /api/v1/network-evolution` - ネットワーク進化
- `GET /api/v1/genre-affinity` - ジャンル親和性
- `GET /api/v1/productivity` - 生産性

### ユーティリティ
- `GET /api/v1/compare` - 2人のスコア比較
- `GET /api/v1/recommend` - 推薦
- `GET /api/v1/predict` - 予測
- `GET /api/v1/health` - ヘルスチェック

## CLI コマンド

16コマンドを提供：

- `stats` - データベース統計
- `ranking` - スコアランキング
- `profile` - 人物プロフィール
- `search` - 人物検索
- `compare` - スコア比較
- `similar` - 類似人物検索
- `timeline` - キャリアタイムライン
- `history` - スコア履歴
- `crossval` - クロスバリデーション
- `influence` - 影響力ツリー
- `export` - エクスポート
- `validate` - データ検証
- `bridges` - ブリッジ検出
- `mentorships` - メンター関係
- `milestones` - マイルストーン
- `net-evolution` - ネットワーク進化
- `genre-affinity` - ジャンル親和性
- `productivity` - 生産性分析

## 技術スタック

- **言語**: Python 3.12
- **パッケージ管理**: pixi (conda-forge + pypi)
- **グラフ**: NetworkX
- **スコアリング**: OpenSkill (PlackettLuce)
- **データモデル**: Pydantic v2
- **HTTP**: httpx (async)
- **ログ**: structlog
- **CLI**: typer + Rich
- **API**: FastAPI + uvicorn
- **可視化**: matplotlib + Plotly
- **DB**: SQLite (WAL mode)
- **テスト**: pytest (955 tests)
- **Lint/Format**: ruff

## ディレクトリ構成

```
animetor_eval/
├── src/
│   ├── pipeline_phases/     # 10フェーズモジュール
│   ├── analysis/            # 37+ 分析モジュール
│   ├── scrapers/            # データ収集（4ソース）
│   ├── utils/               # ユーティリティ
│   ├── models.py            # Pydantic データモデル
│   ├── database.py          # SQLite DAO
│   ├── pipeline.py          # オーケストレーター
│   ├── api.py               # FastAPI サーバー
│   ├── cli.py               # CLI エントリーポイント
│   └── ...
├── tests/                   # 955 テスト
├── result/
│   ├── db/                  # SQLite DB
│   ├── json/                # 26 JSON出力
│   ├── visualizations/      # 可視化ファイル
│   └── notebooks/           # Jupyter ノートブック
├── docs/                    # ドキュメント
├── CLAUDE.md                # Claude Code 向け指示
├── TODO.md                  # タスク管理
└── pixi.toml                # 依存関係定義
```

## 法的留意事項

### データソース
- 公開されたクレジットデータのみを使用
- スクレイピングはレート制限を厳守（AniList: 90req/min, Jikan: 3req/s）

### スコアの解釈
- スコアは**ネットワーク上の位置・密度指標**であり、「能力」や「才能」を測定するものではありません
- 出力には必ず以下の免責事項を含めます：

> **免責事項**: 本スコアはクレジットデータに基づくネットワーク密度・位置指標であり、個人の能力や才能を評価するものではありません。制作スタジオのキャスティング最適化支援を目的としています。

### 名寄せの精度
- 誤った名寄せ（false positive）は信用毀損リスクに直結
- 5段階の保守的な名寄せプロセスを実装
- AI支援は最小信頼度0.8、同一ソース内のみで適用

## ライセンス

MIT License

## 貢献

Issue・Pull Requestを歓迎します。

## 参考文献

- Page, L., Brin, S., et al. (1999). "The PageRank Citation Ranking"
- Weng, R.C., Lin, C.J. (2011). "A Bayesian Approximation Method for Online Ranking"
- Newman, M.E.J. (2010). "Networks: An Introduction"

## リンク

- [ドキュメント](docs/)
- [API仕様](http://localhost:8000/docs) (サーバー起動後)
- [Issues](https://github.com/kasi-x/animetor_eval/issues)

---

**Animetor Eval** - アニメ業界のネットワーク分析による客観的評価システム

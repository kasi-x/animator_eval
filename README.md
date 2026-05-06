# Animetor Eval

> アニメ業界の個人 (アニメーター、監督等) を **公開クレジットデータ** から構造的に評価するサービス。

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## What This Does

業界を信頼ネットワークとしてモデル化し、**構造的な指標** (協業密度、ネットワーク位置、役職進行) からスコアを算出する。視聴者レビュー (anime.score) のような主観指標は scoring formula に一切使わない。

目的は個人の貢献を可視化することで、公正な報酬交渉とアニメ業界の健全化を支援すること。

**Two-layer Evaluation**:

| Layer | 用途 | 指標 |
|-------|------|------|
| 1. Network Profile | 参照 | Authority (Weighted PageRank) / Trust (繰り返し協業) / Credit Density (クレジット頻度×役職進行) |
| 2. Individual Contribution | 補償根拠 | peer_percentile / opportunity_residual / consistency / independent_value |

詳細は [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) と [`docs/CALCULATION_COMPENDIUM.md`](docs/CALCULATION_COMPENDIUM.md)。

## Quick Start

Python 3.12 と [pixi](https://pixi.sh/) が必要。

```bash
git clone https://github.com/kasi-x/animator_eval.git
cd animator_eval
pixi install

pixi run test             # テスト
pixi run pipeline         # フルパイプライン
pixi run serve            # API サーバー (localhost:8000)
```

## Architecture

- **3-layer DB**: BRONZE (scraper raw、`anime.score` 含む) / SILVER (canonical、score-free) / GOLD (analysis output)
- **10-phase pipeline**: `src/pipeline_phases/` (data_loading → validation → entity_resolution → graph_construction → core_scoring → supplementary_metrics → result_assembly → post_processing → analysis_modules → export_and_viz)
- **Rust acceleration**: `rust_ext/` (Brandes betweenness、collaboration edge aggregation) に Python fallback
- **Data sources**: AniList GraphQL / Jikan (MAL) / ANN / allcinema / SeesaaWiki / Media Arts Database

## Reports

3 audience + technical appendix:

- Policy brief (政策立案者向け)
- HR brief (スタジオ管理職向け)
- Business brief (投資家向け)
- Technical appendix (15 reports)

`task report-briefs` で生成。詳細は [`docs/REPORT_PHILOSOPHY.md`](docs/REPORT_PHILOSOPHY.md)、レポート一覧は [`docs/REPORT_INVENTORY.md`](docs/REPORT_INVENTORY.md)。

## v3 Visualization System

Animetor Eval v3 introduces a unified visualization layer in `src/viz/` that enforces `REPORT_PHILOSOPHY.md §3` requirements (CI / null model / shrinkage badge) at the chart-construction level rather than relying on each report to implement them correctly.

### 11 Chart Primitives (P1-P11)

| ID | Primitive | 用途 |
|----|-----------|------|
| P1 | `CIScatter` | 点推定 + 誤差バー / forest plot |
| P2 | `KMCurve` | 生存曲線 (Greenwood band) |
| P3 | `EventStudyPanel` | 介入前後 dynamic effect |
| P4 | `SmallMultiples` | facet grid (cohort × role 等) |
| P5 | `RidgePlot` | 分布の重ね (theta_i コホート比較) |
| P6 | `BoxStripCI` | 分布要約 + raw 点 + 95% CI |
| P7 | `SankeyFlow` | キャリア段階遷移 |
| P8 | `RadialNetwork` | ego-network 局所図 |
| P9 | `HeatMap` | 相関 / 共起行列 |
| P10 | `ParallelCoords` | 多軸 parallel coordinates |
| P11 | `ChoroplethJP` | 都道府県 choropleth |

全 primitive は `auto_ci=True` / `auto_null=True` / `shrinkage_badge=True` をデフォルトとし、CI band と null envelope の描画漏れを構造的に防ぐ。

### src/viz/ 構造

```
src/viz/
├── primitives/          # P1-P11 chart primitive 実装
├── theme.py             # Plotly layout テンプレート (全レポート共通)
├── palettes.py          # Okabe-Ito 8色 + 460-hex アクセシビリティテーブル
├── typography.py        # フォント / サイズ規定
├── ci.py                # CI band 描画ヘルパー
├── null_overlay.py      # null model envelope 描画ヘルパー
├── shrinkage_badge.py   # 縮約済み値 badge
├── interactivity.py     # linked brushing (brief 内 primitive 横断)
└── export.py            # HTML / SVG / PDF 並走 export (kaleido)
```

### SPEC 強制ゲート

各レポートは `ReportSpec` データクラス (7 フィールド: `claim` / `identifying_assumption` / `null_model` / `method_gate` / `sensitivity_grid` / `interpretation_guard` / `data_lineage`) を宣言しなければならない。未宣言のレポートは Pipeline Phase 5 でブロックされる。

strict mode チェック:

```bash
pixi run check-report-spec-strict
```

### Glossary v3

`docs/GLOSSARY_v3.md` — 全 45 レポートで使用する用語の canonical 定義。`forbidden_vocab` の 19 件の例外 (rationale 付き) を管理する。

詳細: [`docs/VIZ_SYSTEM_v3.md`](docs/VIZ_SYSTEM_v3.md) / [`docs/REPORT_DESIGN_v3.md`](docs/REPORT_DESIGN_v3.md) / [`docs/GLOSSARY_v3.md`](docs/GLOSSARY_v3.md)

## Documentation

- [`CLAUDE.md`](CLAUDE.md) — プロジェクト原則 (エージェント向け)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — システム設計
- [`docs/CALCULATION_COMPENDIUM.md`](docs/CALCULATION_COMPENDIUM.md) — 指標の数式
- [`docs/REPORT_PHILOSOPHY.md`](docs/REPORT_PHILOSOPHY.md) — レポート執筆原則
- [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) — DB スキーマ (auto-generated)
- [`docs/schema.dbml`](docs/schema.dbml) — ER 図 (dbdiagram.io 互換)
- [`TODO.md`](TODO.md) / [`DONE.md`](DONE.md) — 進捗管理
- API 仕様: サーバー起動後 http://localhost:8000/docs

## Legal

- **Data**: 公開クレジットのみ。rate-limit 遵守 (AniList 90/min, Jikan 3/s)
- **Score interpretation**: スコアは「ネットワーク位置と密度」の指標であり、「能力」や「才能」の測定ではない。全レポートに以下の免責を付与する。

> **Disclaimer**: これらのスコアはクレジットデータに基づくネットワーク密度と位置の指標です。個人の能力や才能を評価するものではありません。個人の貢献を可視化して公正な報酬と業界の健全化を支援することを目的としています。

- **Entity resolution**: 誤マッチは信用毀損となりうる。5 段階 (exact → cross-source → romaji → similarity 0.95 → AI-assisted 0.8) で保守的に照合。AI 補助は min_confidence=0.8、同一ソース内のみ。

## Tech Stack

Python 3.12 · pixi · NetworkX · Pydantic v2 · httpx async · structlog · typer + Rich · FastAPI + WebSocket · matplotlib + Plotly · SQLite WAL (→ DuckDB 移行予定) · Rust/PyO3/maturin · ruff · pytest

## License

MIT. Issues / PR 歓迎。

---

**References**:
- Page, L. et al. (1999). *The PageRank Citation Ranking*
- Newman, M.E.J. (2010). *Networks: An Introduction*
- Abowd, Kramarz, Margolis (1999). *High Wage Workers and High Wage Firms* — AKM 分解

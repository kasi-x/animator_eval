# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Animetor Eval** is a service that evaluates anime industry professionals (animators, directors, etc.) by analyzing credit data from publicly released works. It models the industry as a trust network and produces quantitative scores based on collaboration patterns, not subjective opinion.

The core principle: credit data is the objective source of truth. Scores reflect network position and collaboration density, never subjective "ability" judgments.

## Architecture

### Three-Axis Evaluation Model

| Axis | Algorithm | What It Measures |
|------|-----------|-----------------|
| **Authority** | Weighted PageRank | Proximity to high-profile directors/works in the collaboration graph |
| **Trust** | Cumulative edge weight | Repeat engagements — being called back by the same supervisors |
| **Skill** | OpenSkill (PlackettLuce) | Recent project contributions and growth trajectory |

### Graph Model

- **Nodes**: Animators, directors, works (anime titles)
- **Edges**: Participation in a work, role held (animation director, key animator, in-between, etc.), co-credit relationships
- Edge weights are influenced by: director prominence bonus, repeat collaboration bonus, role-based weighting (24 role types)

### Key Algorithms

**Weighted PageRank** for Authority score:

```
PR(u) = (1-d)/N + d * Σ [PR(v) * W(v,u) / L(v)]   for v in B_u
```

- `d` = 0.85 (damping factor)
- `W(v,u)` = edge weight from v to u (affected by director rank, collaboration count, role)
- `B_u` = set of nodes linking to u

**Engagement Decay**: When an animator stops appearing in a director's projects, the edge weight `W(v,u)` decays exponentially over time (half-life = 3 years). Detection compares expected co-appearance rate over the last `n` works against actual results.

**Director Circles**: Groups of animators consistently working with the same director (min 2 shared works, 3+ director works required).

### Data Pipeline

1. **Collection**: Credit data from AniList GraphQL, Jikan API (MAL), Media Arts DB SPARQL, Wikidata SPARQL
2. **Validation**: Referential integrity, data completeness, credit distribution checks
3. **Entity Resolution**: Name deduplication — exact match, cross-source match, romaji normalization (conservative: false positive avoidance)
4. **Graph Construction**: NetworkX bipartite, collaboration, and director-animator graphs
5. **Score Computation**: PageRank (Authority) + Trust (repeat engagement) + OpenSkill (Skill) + centrality metrics
6. **Role Classification**: Primary role category per person (director/animator/designer/etc.)
7. **Director Circles**: Identify recurring collaborator groups
8. **Presentation**: JSON/CSV/text reports, CLI (stats/ranking/profile/export/validate), matplotlib visualizations

## Build & Run Commands

```bash
pixi install          # 依存パッケージのインストール
pixi run test         # pytest tests/ -v (168 tests)
pixi run lint         # ruff check src/ tests/
pixi run format       # ruff format src/ tests/
pixi run pipeline     # 全パイプライン実行
pixi run pipeline-viz # パイプライン + 可視化
pixi run validate     # データバリデーション
pixi run stats        # DB統計
pixi run ranking      # スコアランキング
pixi run export-all   # JSON/CSV/テキスト全形式エクスポート
pixi run lab          # JupyterLab 起動
```

## Directory Structure

```
animetor_eval/
├── data/
│   ├── raw/           # スクレイピング生データ
│   ├── interim/       # 中間処理データ（名寄せ後等）
│   └── processed/     # 最終処理済みデータ（グラフ入力用）
├── result/
│   ├── notebooks/     # Jupyter レポート
│   ├── db/            # SQLite (animetor_eval.db)
│   └── json/          # scores.json, circles.json, report.json
├── src/
│   ├── scrapers/      # データ収集 (anilist, mal, mediaarts, jvmg)
│   ├── analysis/      # graph, pagerank, trust, skill, entity_resolution, circles, career, visualize
│   ├── utils/         # config.py (パス定数, 役職重み)
│   ├── pipeline.py    # オーケストレーター
│   ├── validation.py  # データ品質チェック
│   ├── report.py      # レポート生成 (JSON/CSV/text)
│   ├── synthetic.py   # 合成テストデータ生成
│   ├── cli.py         # CLI (typer + Rich)
│   ├── log.py         # structlog 設定
│   ├── models.py      # Pydantic v2 データモデル
│   └── database.py    # SQLite DAO
└── tests/             # pytest テスト (168件)
```

## Tech Stack

- Python 3.12, pixi (conda-forge + pypi)
- NetworkX (graph), OpenSkill (Skill scoring), Pydantic v2 (models)
- httpx (async HTTP), structlog (logging), typer + Rich (CLI)
- matplotlib (visualization), SQLite (storage)
- ruff (lint/format), pytest (testing)

## Legal Constraints

These are hard requirements, not suggestions:

- **Never frame low scores as "lack of ability"** — scores represent network density and position only
- **Public benefit framing**: The service's stated purpose is "casting optimization for production studios" (公益目的)
- **Data source restriction**: Only publicly available credit data from released works
- **Entity resolution accuracy**: Name matching errors can constitute defamation (信用毀損) under Japanese law — treat this as a blocking quality gate
- **Disclaimers**: All reports include JA + EN disclaimers

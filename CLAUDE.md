# CLAUDE.md

Claude Code 向けのプロジェクト指針。詳細は `docs/` と `TODO.md` / `DONE.md` を参照。

## Project Overview

**Animetor Eval** はアニメ業界の個人 (アニメーター、監督等) を公開クレジットデータから評価するサービス。業界を信頼ネットワークとしてモデル化し、協業パターンから **構造的** なスコアを算出する。主観的な「能力」評価は扱わない。

目的: 個人の貢献を可視化して公正な報酬につなげ、業界全体の健全化に寄与する。スコアは常にネットワーク位置と協業密度の指標であり、主観的な「能力」判断ではない。

## Hard Rules (legal + design)

1. **No viewer ratings in scoring**: `anime.score` は scoring formula / edge weight / optimization target のいずれにも使わない。Display のみ可。
2. **No "ability" framing**: 低スコアを「能力不足」と表現しない。常に「ネットワーク位置と密度の指標」として扱う。
3. **Entity resolution accuracy**: 誤マッチは信用毀損になりうる。5 段階解決 (exact → cross-source → romaji → similarity 0.95 → AI-assisted 0.8) を必ず通す。
4. **Compensation basis**: 補償根拠として提示する数値は analytical CI (`SE = σ/√n`) を必須、ヒューリスティック不可。
5. **Disclaimers**: 全レポートに JA + EN 免責を付与。
6. **Data sources**: 公開されたクレジットのみ使用。

## Permitted / Prohibited Data Sources

| 区分 | 例 |
|------|------|
| **Permitted** (構造的事実) | credit records, roles (24 種), 作品メタ (話数/尺/形式), 制作スタジオ, 制作規模 (クレジット数), タイムライン, 共クレジット関係, ネットワーク位置 |
| **Prohibited** (主観) | anime.score / anime.popularity / external reviews |

例外: `anime.score` は BRONZE に保持し display metadata として表示してよいが、scoring path には一切入れない。

## Report Writing Philosophy

`docs/REPORT_PHILOSOPHY.md` 参照。要点:

- **透明な遠近法**: 「客観的開示」は不可能。指標・時間窓・閾値の選択を可視化せよ。
- **Findings / Interpretation 分離**: Findings 層 = 評価的形容詞なし、Interpretation 層 = 一人称明示・対案併記。
- **Implied advocacy 禁止**: 事実の配列で結論を誘導しない。結論は Interpretation で明示。
- **観客別ドキュメント**: Exec brief / Main / Technical appendix / Data statement を混ぜない。
- **Method gate 必須**: 個人レベル推定には CI、グループ主張には null model、予測には holdout。
- **狭い名前で呼ぶ**: 「翌年クレジット可視性喪失率」 (not「離職率」)。

## Architecture

### Three-Layer Database Model

- **BRONZE** (`src_*` テーブル): scraper 生データ。`anime.score` 含む。immutable。
- **SILVER** (`anime`, `persons`, `credits`, `roles`, ...): 正規化済み score-free データ。全 scoring が読む唯一の層。
- **GOLD** (`scores`, `score_history`, `meta_*`): 計算結果と lineage (`meta_lineage`)。

Bronze への唯一の経路は `src/utils/display_lookup.py` (UI 用のみ)。`src/analysis/` / `src/pipeline_phases/` は SILVER のみを読む。

### Two-Layer Evaluation

| Layer | 指標 | 算出 |
|-------|------|------|
| **Network Profile** (reference) | Authority / Trust / Credit Density | Weighted PageRank / 累積エッジ重み / クレジット頻度×役職進行 |
| **Individual Contribution** (compensation) | peer_percentile / opportunity_residual / consistency / independent_value | コホートランク / OLS 残差 / CV / spillover |

### AKM (person fixed effect)

```
log(production_scale_ij) = theta_i + psi_j + epsilon_ij
production_scale = staff_count × episodes × duration_mult
```

- `theta_i` = person FE, `psi_j` = studio FE。結果変数は純粋に構造的 (NOT anime.score)。

### Integrated Value (IV)

```
IV_i = (λ1·theta_i + λ2·birank_i + λ3·studio_exp_i + λ4·awcc_i + λ5·patronage_i) × D_i
```

λ は固定事前重み (anime.score 最適化は削除済み)。D は dormancy 乗算。

### 10-Phase Pipeline

`src/pipeline_phases/` に 1 phase 1 ファイル (data_loading → validation → entity_resolution → graph_construction → core_scoring → supplementary_metrics → result_assembly → post_processing → analysis_modules → export_and_viz)。共有は `PipelineContext` (将来 Hamilton に置換予定、`TODO.md §5`)。

### Graph

- Nodes: person / anime、Edges: 参加・共クレジット関係
- Edge weight: `role_weight × episode_coverage × duration_mult` (構造的のみ)
- `_work_importance()` は尺/形式のみを使用

### Rust Extension

`rust_ext/` (PyO3/maturin): Brandes betweenness (rayon 並列) / collaboration edge aggregation / degree / eigenvector。`src/analysis/graph_rust.py` で Python/NetworkX に graceful fallback。Build: `pixi run build-rust`。

## Schema (SQLModel + Atlas)

- **Single source of truth**: `src/models_v2.py` (37 tables、全制約/FK/index を Python で宣言)
- **Atlas config**: `atlas.hcl`
- **Auto-generated**: `docs/schema.dbml` (ER図), `docs/DATA_DICTIONARY.md` (列単位)

BRONZE/SILVER/GOLD 3 層の詳細は `docs/ARCHITECTURE.md`。

## Report System

3 audience + technical appendix:

- **Policy Brief**: 政策立案者向け (市場集中・ジェンダー・流出・政策推奨)
- **HR Brief**: スタジオ管理職向け (チーム化学・後継・報酬公正・離職リスク)
- **Business Brief**: 投資家向け (ホワイトスペース・新興チーム・過小評価人材)
- **Technical Appendix**: 15 reports、3 brief へ相互参照

各 brief は 4 section × 3 method gate を最小構成として持つ。詳細: `scripts/report_generators/` / `docs/REPORT_INVENTORY.md`。

Vocabulary enforcement: `ability`, `skill`, `talent`, `competence`, `capability` は正規表現で blocking (`scripts/report_generators/lint_vocab.py`)。

### Report System Status (2026-04)

**3 系統が並存**しており統廃合計画中 (`TODO.md §8.1`):

1. `scripts/generate_all_reports.py` (v1, monolith, 24k 行) = `pixi run reports`
2. `scripts/generate_reports_v2.py` + `scripts/report_generators/reports/*.py` (v2 orchestrator) = `task report-*`
3. `src/reporting/` (v3 class-based、~2700 行) = `pixi run reports-new`

新規レポートは v2 (`scripts/report_generators/reports/`) に追加する。

## Build & Run Commands

```bash
pixi install              # 依存インストール
pixi run lint             # ruff check
pixi run format           # ruff format
pixi run pipeline         # フルパイプライン
pixi run pipeline-inc     # 差分実行 (データ変化なしなら skip)
pixi run pipeline-resume  # checkpoint から再開
pixi run bench            # パフォーマンスベンチ
pixi run build-rust       # Rust 拡張ビルド
pixi run serve            # API サーバー (localhost:8000)
pixi run lab              # JupyterLab
```

### テストの走らせ方 (重要: デフォルトでフル実行しないこと)

作業中は必ず **変更影響のあるテストだけ** を走らせる。2450+ 件の全件走行は PR/ship 直前のみ。

| コマンド | 用途 | 備考 |
|---|---|---|
| `pixi run test-scoped tests/test_foo.py` | **デフォルト**: 明示ターゲット | パス or `-k` 指定で最小実行。複数ファイルはスペース区切りで渡す |
| `pixi run test-quick` | デバッグ反復中 | `pytest -x --lf` (前回失敗のみ + 即停止) |
| `pixi run test` | PR/ship 直前のみ | フル 2450+、並列 (`-n auto --dist loadscope`) |

- Claude は「とりあえずテスト」で `pixi run test` を選ばない。まず **`test-scoped`** で変更箇所に対応するテストファイルを明示指定する。
- `pixi run test-impact` (testmon) は **使わない**: 変更と無関係なテストを引き込み、このリポジトリでは現実的に完走しない (2026-04-23 ユーザー確定)。
- 対象テストファイルが分からない時は `grep -rn "from src.foo" tests/` などで touched module の dependents を探して列挙する。

Task 系統は `task --list` で確認。

## Directory Structure

```
animetor_eval/
├── src/
│   ├── pipeline_phases/   # 10 phase pipeline
│   ├── analysis/          # analysis modules (scoring/, network/, genre/, studio/, va/, causal/ ほか)
│   ├── scrapers/          # AniList / MAL / ANN / SeesaaWiki / allcinema 等
│   ├── utils/             # config, json_io, role_groups, display_lookup 等
│   ├── viz/               # v2 report architecture (chart_spec, renderers)
│   ├── i18n/              # EN/JA translations
│   ├── reporting/         # v3 class-based report system
│   ├── etl/               # integrate.py (integrate_anilist 等)
│   ├── models.py          # Pydantic v2 モデル
│   ├── models_v2.py       # SQLModel schema (single source of truth)
│   ├── database.py        # SQLite DAO (55 migration、`TODO.md §1 Maintenance` で分割予定)
│   ├── pipeline.py        # オーケストレーター
│   ├── api.py             # FastAPI (42+ endpoint)
│   └── cli.py             # typer + Rich (33 commands)
├── scripts/
│   ├── generate_briefs_v2.py       # brief orchestrator
│   ├── generate_reports_v2.py      # v2 class dispatcher
│   ├── generate_all_reports.py     # v1 monolith (分解中)
│   ├── report_generators/          # v2 の実装 + lint / ci / template / base class
│   ├── monitoring/                 # quality snapshot / anomaly detection
│   └── maintenance/                # 一度きりスクリプト
├── tests/                 # 140 ファイル (pytest、構造整理予定 `TODO.md §6.4`)
├── docs/                  # ARCHITECTURE / CALCULATION_COMPENDIUM / REPORT_PHILOSOPHY ほか
├── static/                # frontend (portfolio, pipeline monitor)
├── rust_ext/              # PyO3 Rust 拡張
├── TASK_CARDS/            # 作業カード (弱いモデル向け自己完結手順)
├── TODO.md / DONE.md      # 進捗管理 (TODO.md が単一の未完了タスク管理ファイル)
└── CLAUDE.md              # これ
```

## Key Patterns

### Testing

- **Monkeypatch `DEFAULT_DB_PATH`** (関数 `get_connection` ではなく): pipeline が module load 時に import するため、関数を差し替えても効かない。DuckDB 移行後は `DEFAULT_SILVER_PATH` (silver.duckdb) と `DEFAULT_GOLD_DB_PATH` (gold.duckdb) も同様に monkeypatch が必要 (`TODO.md §4` Phase D 完了後に対応)
- **JSON_DIR の patch**: `src.pipeline.JSON_DIR`, `src.analysis.visualize.JSON_DIR`, `src.utils.config.JSON_DIR` の 3 箇所
- **structlog + pytest**: `cache_logger_on_first_use=False` + `PrintLoggerFactory()` で "I/O operation on closed file" を回避 (`tests/conftest.py`)
- **Dataclass 戻り値**: analysis 関数は dataclass を返す。attr access (`result.field`)、dict 化は `asdict()`
- **E2E**: 合成データ (`src/synthetic.py`、5 directors / 30 animators / 15 anime)

### Code Conventions

- **structlog**: stdlib logging 不使用
- **Pydantic v2**: `computed_field` で派生属性
- **httpx async**: 全 scraper は async
- **Role constants**: `src/utils/role_groups.py` が single source
- **JSON I/O**: `src/utils/json_io.py` (22+ named loaders、TTL cache)

### Docstring language policy

- 構造的説明 (Args/Returns/Raises) は英語推奨
- ドメイン概念 (役職の俗称、制作慣行、作品名) は日本語 OK
- 混在は容認するが、一つの docstring 内で切り替えない

## Tech Stack

Python 3.12, pixi (conda-forge + pypi), NetworkX, Pydantic v2, httpx, structlog, typer + Rich, FastAPI + uvicorn + WebSocket, matplotlib + Plotly, Rust/PyO3/maturin, **sf-hamilton** (Phase 9 DAG PoC, H-1), SQLite WAL (BRONZE/SILVER/GOLD 現在も SQLite; DuckDB 移行中 `TODO.md §4` — Phase A ✅ Cards 03/04/05 完了、次: Card 06 GOLD DuckDB 化), ruff, pytest (2450+ tests)。

## Known Issues

- 現行タスク: `TODO.md` (schema 修復 / DuckDB / Hamilton / レポート統廃合 / アーキテクチャ整理 / ドキュメント整理 — すべてを一元管理)
- 完了済み: `DONE.md` (anime.score 除去 16 pathway、計算ロジック監査、Phase 1-4 基盤)

## 禁止事項 (再提案しない)

以下は `DONE.md` / `feedback_framework_rejections.md` で却下済み:

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言する方針
- **Polars**: DuckDB で冗長 (`TODO.md §4` 移行後)
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

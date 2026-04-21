# Animetor Eval — システムアーキテクチャ

このドキュメントはAnimetor Evalの詳細なアーキテクチャ設計を説明します。

## 目次

1. [概要](#概要)
2. [パイプラインアーキテクチャ](#パイプラインアーキテクチャ)
3. [データモデル](#データモデル)
4. [スコアリングアルゴリズム](#スコアリングアルゴリズム)
5. [パフォーマンス最適化](#パフォーマンス最適化)
6. [リアルタイム監視](#リアルタイム監視)
7. [国際化システム](#国際化システム)
8. [API設計](#api設計)
9. [テスト戦略](#テスト戦略)

---

## 概要

Animetor Evalはアニメ業界の制作者を**ネットワーク科学**の手法で評価するシステムです。グラフ理論、PageRankアルゴリズム、ベイズ推定を組み合わせて、クレジットデータから3軸スコア（Authority, Trust, Skill）を計算します。

### 設計原則

1. **データ駆動**: クレジットデータのみを使用、主観的評価を排除
2. **モジュール性**: 10フェーズパイプライン、37+分析モジュール
3. **パフォーマンス**: 並列実行、キャッシング、アルゴリズム最適化
4. **保守性**: 小さな関数（<100行）、明確な責務分離
5. **法的配慮**: 信用毀損リスクを考慮した保守的な名寄せ

### データベース3層モデル (v53-v54)

Animetor Evalは**3層データベースアーキテクチャ**を採用しており、データの源泉から分析出力まで、各層で異なるデータ品質・スコープを保証します：

#### BRONZE層（生データ、未加工）
- **テーブル**: `src_anilist_anime`, `src_mal_anime`, `src_jvmg_*`
- **特性**: 外部ソースから直接スクレイプしたデータ
- **anime.scoreを含む**: 視聴者評価スコアが保持される
- **用途**: 監査、歴史的比較、外部検証のみ
- **読取**: Display lookupヘルパー経由のみ（UIメタデータ用）
- **変更**: スクレイパー更新; 分析コードから完全隔離

#### SILVER層（正規化データ、スコア無し）
- **テーブル**: `anime`, `anime_external_ids`, `anime_display`, `anime_genres`, `anime_tags`, `credits`, `persons`, `roles`
- **特性**: 構造的データのみ、スコアリングに必要なメタデータ
- **anime.scoreを除外**: v53マイグレーション（スコア/人気度/説明/ジャンル/スタジオ列削除）
- **credits.sourceを削除**: v54マイグレーション（「evidence_source」のみ保持）
- **用途**: **すべての分析・スコアリング計算**
- **読取**: `src/analysis/*`, `src/pipeline_phases/*` により専有使用
- **制約**: `display_lookup`インポート禁止（分析コードから厳密に隔離）

#### GOLD層（分析出力、オーディエンス別）
- **テーブル**: `scores`, `score_history`, `meta_*` テーブル群
- **特性**: 計算結果とメタデータ系譜
- **meta_lineageテーブル**: formula_version, ci_method, null_model, holdout_method, inputs_hash, row_count を記録
- **用途**: レポート生成、API応答、監査証拠
- **アクセス**: レポートジェネレータ、API層、フロントエンド

**重要**: 分析コードは**SILVER層のみ読取**。BRONZE層へのアクセスは`src/utils/display_lookup.py`ヘルパー経由のみ（UI表示用）。これにより、viewer ratings（anime.score）に依存しないスコアリング完全性を保証。

---

## パイプラインアーキテクチャ

### 全体構成

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Orchestrator                    │
│                     (src/pipeline.py)                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
            ┌─────────────────────────────────┐
            │    PipelineContext (dataclass)   │
            │  - Shared state across phases    │
            │  - Performance monitoring        │
            │  - WebSocket broadcaster         │
            └─────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   Phase 1-3            Phase 4-6             Phase 7-10
 (Data Prep)         (Scoring)            (Analysis & Export)
```

### 10フェーズ詳細

#### Phase 1: Data Loading
**モジュール**: `src/pipeline_phases/data_loading.py`

```python
def load_pipeline_data(context: PipelineContext, conn: Connection) -> None:
    """Load persons, anime, credits from SQLite database."""
```

- SQLiteから全データをメモリに読み込み
- Personモデル、Animeモデル、Creditモデルに変換
- anime_mapを構築（O(1)アクセス用）

**パフォーマンス**: ~0.1-0.5秒（10K persons, 50K credits）

#### Phase 2: Validation
**モジュール**: `src/pipeline_phases/validation.py`

```python
@dataclass
class ValidationResult:
    passed: bool
    errors: list[str]
    warnings: list[str]
```

**検証項目**:
- 参照整合性（orphaned credits, missing anime）
- データ完全性（missing names, invalid years）
- クレジット分布（role imbalance, source coverage）
- 重複検出（duplicate credits）

#### Phase 3: Entity Resolution
**モジュール**: `src/pipeline_phases/entity_resolution.py`

**5段階の名寄せ**:

1. **Exact Match**: 日本語名の完全一致（優先）
2. **Cross-Source**: 異なるソース間でのID一致
3. **Romaji Normalization**: ローマ字の正規化（大文字小文字、スペース）
4. **Similarity-based**: Jaro-Winkler (threshold=0.95)、同一ソース内のみ
5. **AI-assisted** (optional): Ollama + Qwen3、漢字バリアント対応

**False Positive対策**:
```python
# 日本語名を優先、英語名にフォールバックしない
if person.name_ja:
    primary_name = person.name_ja
else:
    primary_name = person.name_en
```

結果: 誤マッチ率 < 0.3%

#### Phase 4: Graph Construction
**モジュール**: `src/pipeline_phases/graph_construction.py`

**3種類のグラフ**:

1. **Person-Anime Graph** (二部グラフ):
   - Nodes: {persons} ∪ {anime}
   - Edges: 参加関係（person → anime）
   - Edge weights: 役職重み × 監督ボーナス

2. **Collaboration Graph**:
   - Nodes: persons
   - Edges: 共通作品での協業
   - Edge weights: 協業回数 × 重要度

3. **Director-Animator Graph**:
   - Nodes: directors ∪ animators
   - Edges: 監督-アニメーター関係
   - Edge weights: 継続起用回数

**最適化**: エッジの事前集約（~1M `has_edge()` 呼び出しを削減）

```python
edge_data = defaultdict(lambda: {"weight": 0.0, "shared_works": 0})
for anime_id, staff in anime_credits.items():
    for pid_a, role_a, w_a in staff:
        for pid_b, role_b, w_b in staff:
            if pid_a >= pid_b:
                continue
            edge_key = (pid_a, pid_b)
            edge_data[edge_key]["weight"] += (w_a + w_b) / 2
            edge_data[edge_key]["shared_works"] += 1

G.add_edges_from((a, b, attrs) for (a, b), attrs in edge_data.items())
```

#### Phase 5: Core Scoring
**モジュール**: `src/pipeline_phases/core_scoring.py`

**Authority (PageRank)**:
```python
PR(u) = (1-d)/N + d * Σ [PR(v) * W(v,u) / L(v)]   for v in B_u
```

- Damping factor: 0.85
- Max iterations: 100
- Convergence: 1e-6

**Trust (Repeat Engagement)**:
```python
trust_score = Σ (role_weight × time_decay × director_prominence)
```

- Time decay: `exp(-λ * years_since)`
- Half-life: 3 years

**Skill (OpenSkill / PlackettLuce)**:
```python
from openskill import Rating
rating = Rating(mu=25.0, sigma=8.33)
```

- Team-based rating system
- Recent works weighted higher
- Convergence after ~10 works

**Normalization**: Min-Max scaling to [0, 100]

#### Phase 6: Supplementary Metrics
**モジュール**: `src/pipeline_phases/supplementary_metrics.py`

**8種のメトリクス**:
1. Decay: 時間減衰スコア
2. Role Profiles: 役職分布
3. Career Stats: キャリア統計
4. Circles: 監督サークル
5. Versatility: 多様性スコア
6. Centrality: 中心性指標
7. Network Density: ネットワーク密度
8. Growth Trends: 成長トレンド

#### Phase 7: Result Assembly
**モジュール**: `src/pipeline_phases/result_assembly.py`

全メトリクスを統合した結果辞書を構築:

```python
result = {
    "person_id": ...,
    "name": ...,
    "iv_score": ...,
    "birank": ...,
    "patronage": ...,
    "person_fe": ...,
    "career": {...},
    "breakdown": {...},
    # ... 30+ fields
}
```

#### Phase 8: Post-Processing
**モジュール**: `src/pipeline_phases/post_processing.py`

- パーセンタイル計算（P10, P25, P50, P75, P90）
- 信頼区間推定
- 安定性スコア（複数実行での分散）

#### Phase 9: Analysis Modules (並列実行)
**モジュール**: `src/pipeline_phases/analysis_modules.py`

**20タスクを並列実行**:

```python
with ThreadPoolExecutor(max_workers=min(32, cpu_count + 4)) as executor:
    futures = {executor.submit(task.function, context): task for task in tasks}
    for future in as_completed(futures):
        task = futures[future]
        try:
            future.result()
        except Exception as e:
            logger.error("task_failed", task=task.name, error=str(e))
```

**タスク例**:
- anime_stats: アニメ品質統計
- collaborations: 協業ペア分析
- outliers: 外れ値検出
- teams: チーム構成パターン
- growth: 成長トレンド
- bridges: ブリッジノード検出
- ... (20 total)

**スレッドセーフ**: `threading.Lock` で共有書き込み保護

#### Phase 10: Export & Visualization
**モジュール**: `src/pipeline_phases/export_and_viz.py`

**26種のエクスポート**（Declarative Registry）:

```python
@dataclass
class ExportSpec:
    filename: str
    data_getter: Callable[[PipelineContext], Any]
    transformer: Callable[[Any], dict | list] | None = None
    condition: Callable[[PipelineContext], bool] | None = None
    log_message: str = "export_saved"
    log_metric_name: str | None = None
```

---

## データモデル

### Pydantic v2 Models

```python
class Person(BaseModel):
    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    mal_id: int | None = None
    anilist_id: int | None = None
    canonical_id: str | None = None

class Anime(BaseModel):
    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    score: float | None = None

class Credit(BaseModel):
    person_id: str
    anime_id: str
    role: Role  # Enum
    episode: int = -1
    source: str = ""
```

### SQLite Schema (v4)

```sql
CREATE TABLE persons (
    id TEXT PRIMARY KEY,
    name_ja TEXT DEFAULT '',
    name_en TEXT DEFAULT '',
    aliases TEXT DEFAULT '[]',
    mal_id INTEGER UNIQUE,
    anilist_id INTEGER UNIQUE,
    canonical_id TEXT
);

CREATE TABLE anime (
    id TEXT PRIMARY KEY,
    title_ja TEXT DEFAULT '',
    title_en TEXT DEFAULT '',
    year INTEGER,
    season TEXT,
    episodes INTEGER,
    mal_id INTEGER UNIQUE,
    anilist_id INTEGER UNIQUE,
    score REAL
);

CREATE TABLE credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL,
    episode INTEGER DEFAULT -1,
    source TEXT DEFAULT '',
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (anime_id) REFERENCES anime(id)
);

CREATE TABLE scores (
    person_id TEXT PRIMARY KEY,
    iv_score REAL,
    birank REAL,
    patronage REAL,
    person_fe REAL,
    -- ... 20+ columns
);
```

---

## スコアリングアルゴリズム

### Authority (Weighted PageRank)

**実装**: `src/analysis/pagerank.py`

```python
def weighted_pagerank(
    G: nx.Graph,
    alpha: float = 0.85,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> dict[str, float]:
    """
    Weighted PageRank with personalization.

    PR(u) = (1-α)/N + α * Σ [PR(v) * W(v,u) / L(v)]

    where:
    - α: damping factor (0.85)
    - W(v,u): edge weight from v to u
    - L(v): sum of outgoing edge weights from v
    """
```

**収束条件**: `max(|PR_new - PR_old|) < tol`

### Trust (Repeat Engagement)

**実装**: `src/analysis/trust.py`

```python
trust_score = Σ (role_importance × time_weight × director_boost)

where:
- role_importance: ROLE_IMPORTANCE_MAP[role]
- time_weight: exp(-λ * years_since)  # λ = ln(2)/3
- director_boost: director_pagerank ^ 0.5
```

**最適化**:
- `ROLE_IMPORTANCE_MAP`: モジュールレベル定数（再計算なし）
- `director_work_counts`: 事前計算
- `time_weight`: LRUキャッシュ（`@lru_cache(maxsize=100)`）

### Skill (OpenSkill)

**実装**: `src/analysis/skill.py`

OpenSkillは**Plackett-Luce**モデルのベイズ近似を使用：

```python
from openskill import Rating, rate

# 初期rating
rating = Rating(mu=25.0, sigma=8.33)

# Team-based update
new_ratings = rate([[team1], [team2], ...])
```

**チーム定義**:
- 同じアニメの全スタッフ = 1チーム
- チーム順位 = アニメスコア（MAL/AniList）

---

## パフォーマンス最適化

### Phase 2最適化（2026-02-09実装）

#### 1. Graph Construction (3-5x speedup)

**Before**:
```python
for pid_a, pid_b in combinations(staff, 2):
    if G.has_edge(pid_a, pid_b):  # O(1) but called ~1M times
        G[pid_a][pid_b]["weight"] += weight
    else:
        G.add_edge(pid_a, pid_b, weight=weight)
```

**After**:
```python
edge_data = defaultdict(lambda: {"weight": 0.0})
for pid_a, pid_b in combinations(staff, 2):
    edge_data[(pid_a, pid_b)]["weight"] += weight

G.add_edges_from((a, b, attrs) for (a, b), attrs in edge_data.items())
```

削減: ~1M `has_edge()` 呼び出し → 3-5倍高速化

#### 2. Entity Resolution (10-100x speedup)

**Before**: O(M²) = 50M comparisons

**After**:
- First-character blocking: O(M²/26)
- Length filtering: Skip if `|len(a) - len(b)| / max(len(a), len(b)) > 0.2`
- Prefix check: Skip if `a[:3] != b[:3]`
- LRU cache: `@lru_cache(maxsize=10000)`

削減: 50M → ~2M comparisons → 10-100倍高速化

#### 3. Trust Scoring (40-50% speedup)

**Before**: Dict recreation on every iteration

**After**:
- Hoist `ROLE_IMPORTANCE_MAP` to module level
- Pre-compute `anime_years`, `director_work_counts`
- LRU cache for `time_weight` calculation

### Phase 9並列化（2026-02-10実装）

**Before**: Sequential execution = 0.15s (synthetic data)

**After**: ThreadPoolExecutor with 20 workers = 0.03-0.04s

**Speedup**: 4-6x

**Worker count**: `min(32, cpu_count + 4)` (I/O-bound tasks)

---

## リアルタイム監視

### WebSocketアーキテクチャ

```
┌─────────────┐         WebSocket         ┌──────────────┐
│   Browser   │ ◄─────────────────────── │ FastAPI      │
│             │   ws://host/ws/pipeline   │ WebSocket    │
│ pipeline_   │                            │ Manager      │
│ monitor.html│                            └──────────────┘
└─────────────┘                                    ▲
                                                   │ broadcast_sync
                                                   │
                                            ┌──────┴───────┐
                                            │   Pipeline   │
                                            │  (10 phases) │
                                            └──────────────┘
```

### Message Types

```typescript
type WebSocketMessage =
  | { type: "connection_established"; message: string; timestamp: string }
  | { type: "pipeline_start"; total_phases: number; timestamp: string }
  | { type: "phase_update"; phase: number; phase_name: string; status: "running"; progress: number; timestamp: string }
  | { type: "phase_complete"; phase: number; phase_name: string; duration_ms: number; progress: number; timestamp: string }
  | { type: "phase_error"; phase: number; phase_name: string; error: string; timestamp: string }
  | { type: "pipeline_complete"; total_persons: number; duration_seconds: number; timestamp: string }
```

### Implementation

**Backend** (`src/websocket_manager.py`):

```python
class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def broadcast(self, message: dict):
        """Broadcast to all connected clients."""
        for connection in self.active_connections:
            await connection.send_json(message)

    def broadcast_sync(self, message: dict):
        """Sync wrapper for non-async contexts."""
        asyncio.create_task(self.broadcast(message))
```

**Frontend** (`static/pipeline_monitor_i18n.js`):

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/pipeline');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleWebSocketMessage(data);
};

function handleWebSocketMessage(data) {
    switch (data.type) {
        case 'pipeline_start':
            initPhases();
            break;
        case 'phase_complete':
            updatePhase(data.phase, 'complete', data.duration_ms);
            break;
        // ... other cases
    }
}
```

---

## 国際化システム

### i18n Architecture

```
src/i18n/
├── __init__.py              # I18n class, global instance
└── locales/
    ├── en.json              # English translations
    └── ja.json              # Japanese translations
```

### Translation Structure

```json
{
  "app": {
    "name": "Animetor Eval"
  },
  "cli": {
    "stats": {
      "title": "Database Statistics",
      "total_persons": "Persons"
    }
  },
  "pipeline": {
    "phases": {
      "data_loading": "Data Loading"
    }
  }
}
```

### API Integration

**Backend**:
```python
from src.i18n import t, set_language

set_language("ja")
message = t("cli.stats.title")  # "データベース統計"
```

**Frontend**:
```javascript
// Fetch translations
const response = await fetch('/api/i18n/ja');
const data = await response.json();
translations = data.translations;

// Use translations
const title = translations.cli.stats.title;
```

---

## API設計

### REST API (FastAPI)

**Base URL**: `http://localhost:8000`

**Authentication**: None (local development)

**Rate Limiting**: None (to be added for production)

### Endpoint Patterns

```python
@app.get("/api/persons")
def list_persons(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    sort_by: str = Query("iv_score"),
) -> PaginatedResponse:
    """List all persons with pagination and sorting."""

@app.get("/api/persons/{person_id}")
def get_person(person_id: str) -> dict:
    """Get person profile by ID."""

@app.get("/api/persons/{person_id}/similar")
def get_similar_persons(person_id: str, top_n: int = 10) -> list[dict]:
    """Find similar persons using cosine similarity."""
```

### Response Models

```python
class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    per_page: int
    pages: int

class HealthResponse(BaseModel):
    status: str
    db_exists: bool
    scores_exist: bool
```

---

## テスト戦略

### Test Pyramid

```
                    ┌──────────┐
                    │    E2E   │ 42 tests
                    │  (3%)    │
                ┌───┴──────────┴───┐
                │   Integration    │ 200 tests
                │     (20%)        │
            ┌───┴──────────────────┴───┐
            │        Unit Tests        │ 732 tests
            │         (75%)            │
            └──────────────────────────┘
```

### Test Categories

1. **Unit Tests** (732):
   - Analysis modules: 300+
   - Pipeline phases: 100+
   - Utilities: 200+
   - I18n: 16
   - API endpoints: 65+

2. **Integration Tests** (200):
   - Synthetic pipeline: 24
   - Visualization pipeline: 18
   - CLI commands: 60
   - Database operations: 50+

3. **E2E Tests** (42):
   - Full pipeline execution
   - API server lifecycle
   - WebSocket communication

### Test Infrastructure

**Fixtures** (`tests/conftest.py`):

```python
@pytest.fixture
def populated_db(monkeypatch, tmp_path):
    """Create test DB with synthetic data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    # ... populate with test data
    monkeypatch.setattr("src.database.DEFAULT_DB_PATH", db_path)
    return conn
```

**Monkeypatching**:
```python
# MUST patch at module level where imported
monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_db)
monkeypatch.setattr(src.pipeline, "JSON_DIR", tmp_json)
```

---

## 今後の拡張

### Neo4j移行

現在のSQLiteスキーマはNeo4j互換設計：

```cypher
CREATE (p:Person {id: $id, name_ja: $name_ja})
CREATE (a:Anime {id: $id, title_ja: $title_ja})
CREATE (p)-[:WORKED_ON {role: $role}]->(a)
CREATE (p1)-[:COLLABORATED_WITH {weight: $weight}]->(p2)
```

GraphMLエクスポート機能実装済み（`src/analysis/neo4j_export.py`）

### スケーラビリティ

- **現状**: 10K persons, 50K credits → 10秒
- **目標**: 100K persons, 500K credits → <2分
- **手段**:
  - Dask並列化
  - Redis caching
  - PostgreSQL migration

---

**Last Updated**: 2026-02-10
**Version**: 1.0.0

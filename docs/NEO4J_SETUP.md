# Neo4j Setup Guide

Animetor Evalは、大規模運用向けにNeo4jグラフデータベースへの直接接続をサポートしています。

## Why Neo4j?

SQLiteは小〜中規模データ（<100K records）には十分ですが、大規模運用では以下の制限があります：

- ❌ 書き込み競合（single-writer lock）
- ❌ グラフクエリが遅い（JOIN多用）
- ❌ 分散環境不可

Neo4jはこれらを解決：

- ✅ 並行書き込み対応
- ✅ ネイティブグラフクエリ（Cypher）
- ✅ スケールアウト可能（Neo4j Cluster）
- ✅ グラフアルゴリズムライブラリ（GDS）

## Requirements

- **Neo4j 5.0+** (Community or Enterprise)
- **Python neo4j driver** (already installed via `pixi install`)
- **Environment variables** (or CLI options):
  - `NEO4J_URI` (default: `bolt://localhost:7687`)
  - `NEO4J_USER` (default: `neo4j`)
  - `NEO4J_PASSWORD` (required)

## Installation

### Option 1: Docker (推奨)

```bash
# Pull Neo4j image
docker pull neo4j:5.26

# Run Neo4j container
docker run \
    --name neo4j-animetor \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/your_password \
    -v $PWD/neo4j_data:/data \
    neo4j:5.26

# Access Neo4j Browser: http://localhost:7474
# Login: neo4j / your_password
```

### Option 2: Desktop

1. Download [Neo4j Desktop](https://neo4j.com/download/)
2. Create a new database
3. Set password
4. Start database

### Option 3: Cloud (Neo4j Aura)

1. Sign up at [Neo4j Aura](https://neo4j.com/cloud/aura/)
2. Create a free instance
3. Get connection URI and credentials

## Usage

### 1. Set Environment Variables

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password"
```

Or create a `.env` file:

```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

### 2. Export Data to Neo4j

```bash
# Export all data (persons, anime, credits, collaborations)
pixi run neo4j-export

# Clear database before export
pixi run neo4j-export --clear

# Specify connection details
pixi run neo4j-export --uri bolt://localhost:7687 --user neo4j --password mypass
```

**Output**:
```
Neo4j Direct Export

Loading data from SQLite...
✓ Loaded 1,234 persons, 567 anime, 12,345 credits

Connecting to Neo4j at bolt://localhost:7687...
✓ Connected to Neo4j

Writing data to Neo4j...
✓ Export complete!

┌────────────────────┬────────┐
│ Entity             │  Count │
├────────────────────┼────────┤
│ Persons            │  1,234 │
│ Anime              │    567 │
│ Credits            │ 12,345 │
│ Collaborations     │  4,567 │
└────────────────────┴────────┘
```

### 3. Query with Cypher

```bash
# Find top 10 persons by composite score
pixi run neo4j-query "MATCH (p:Person) RETURN p.name_en, p.composite ORDER BY p.composite DESC LIMIT 10"

# Find collaborations of a specific person
pixi run neo4j-query "MATCH (p:Person {id: 'person_123'})-[r:COLLABORATED_WITH]-(collab) RETURN collab.name_en, r.shared_works ORDER BY r.shared_works DESC LIMIT 10"

# Find anime from a specific year
pixi run neo4j-query "MATCH (a:Anime) WHERE a.year = 2020 RETURN a.title_en, a.score ORDER BY a.score DESC"
```

### 4. Check Database Stats

```bash
pixi run neo4j-stats

# Output:
# Neo4j Database Statistics
#
# ┌────────────────┬────────┐
# │ Entity         │  Count │
# ├────────────────┼────────┤
# │ Persons        │  1,234 │
# │ Anime          │    567 │
# │ Credits        │ 12,345 │
# │ Collaborations │  4,567 │
# └────────────────┴────────┘
```

## Graph Schema

### Node Labels

#### Person
```cypher
(:Person {
  id: string,
  name_ja: string,
  name_en: string,
  mal_id: int?,
  anilist_id: int?,
  authority: float?,
  trust: float?,
  skill: float?,
  composite: float?
})
```

#### Anime
```cypher
(:Anime {
  id: string,
  title_ja: string,
  title_en: string,
  year: int?,
  season: string?,
  episodes: int?,
  mal_id: int?,
  anilist_id: int?,
  score: float?
})
```

### Relationship Types

#### CREDITED_IN (Person → Anime)
```cypher
(p:Person)-[:CREDITED_IN {
  role: string,
  episode: int?,
  source: string
}]->(a:Anime)
```

#### COLLABORATED_WITH (Person ↔ Person)
```cypher
(p1:Person)-[:COLLABORATED_WITH {
  shared_works: int
}]-(p2:Person)
```

### Indexes & Constraints

Automatically created on export:

- **Unique constraints**: `Person.id`, `Anime.id`
- **Indexes**: `Person.composite`, `Anime.year`, `CREDITED_IN.role`

## Example Cypher Queries

### Find Directors with Highest Authority

```cypher
MATCH (p:Person)-[r:CREDITED_IN]->(a:Anime)
WHERE r.role IN ['DIRECTOR', 'EPISODE_DIRECTOR']
RETURN p.name_en, p.authority, count(a) AS works
ORDER BY p.authority DESC
LIMIT 20
```

### Find Collaboration Network of a Person

```cypher
MATCH (center:Person {id: 'person_123'})-[r:COLLABORATED_WITH*1..2]-(neighbor)
RETURN center, r, neighbor
LIMIT 100
```

### Find Anime with Highest Average Staff Score

```cypher
MATCH (p:Person)-[:CREDITED_IN]->(a:Anime)
WHERE p.composite IS NOT NULL
WITH a, avg(p.composite) AS avg_score, count(p) AS staff_count
WHERE staff_count >= 5
RETURN a.title_en, a.year, avg_score, staff_count
ORDER BY avg_score DESC
LIMIT 20
```

### Find Persons Who Worked Together Most

```cypher
MATCH (p1:Person)-[r:COLLABORATED_WITH]-(p2:Person)
RETURN p1.name_en, p2.name_en, r.shared_works
ORDER BY r.shared_works DESC
LIMIT 50
```

### Find Career Progression (Role Transitions)

```cypher
MATCH (p:Person)-[r:CREDITED_IN]->(a:Anime)
WHERE a.year IS NOT NULL
WITH p, r.role AS role, a.year AS year
ORDER BY p.id, year
RETURN p.name_en, collect({role: role, year: year})[0..10] AS career
LIMIT 20
```

## Python API Usage

```python
from src.analysis.neo4j_direct import Neo4jWriter
from src.database import get_connection, get_all_persons, get_all_anime, get_all_credits, get_all_scores

# Load data from SQLite
conn = get_connection()
persons = get_all_persons(conn)
anime_list = get_all_anime(conn)
credits = get_all_credits(conn)
scores = get_all_scores(conn)
conn.close()

# Write to Neo4j
with Neo4jWriter(password="your_password") as writer:
    writer.write_all(persons, anime_list, credits, scores, clear=True)

    # Run custom Cypher query
    results = writer.run_cypher(
        "MATCH (p:Person) WHERE p.composite > 80 RETURN p.name_en, p.composite"
    )

    for record in results:
        print(f"{record['p.name_en']}: {record['p.composite']}")
```

## Performance Considerations

### Batch Sizes

デフォルトのバッチサイズは最適化済み：

- **Persons**: 1,000 nodes per transaction
- **Anime**: 1,000 nodes per transaction
- **Credits**: 5,000 relationships per transaction
- **Collaborations**: 5,000 relationships per transaction

大規模データ（>100K records）の場合は、バッチサイズを調整可能：

```python
writer.write_persons(persons, scores, batch_size=5000)
writer.write_credits(credits, batch_size=10000)
```

### Indexing

制約とインデックスは自動作成されますが、カスタムインデックスも追加可能：

```cypher
CREATE INDEX person_name_ja IF NOT EXISTS FOR (p:Person) ON (p.name_ja);
CREATE INDEX anime_title_en IF NOT EXISTS FOR (a:Anime) ON (a.title_en);
```

### Memory Settings

大規模データの場合、Neo4jのメモリ設定を調整：

```bash
# neo4j.conf
dbms.memory.heap.initial_size=2G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=4G
```

## Troubleshooting

### Connection Refused

```
neo4j.exceptions.ServiceUnavailable: Failed to establish connection
```

**解決策**:
- Neo4jが起動しているか確認: `docker ps` or Neo4j Desktop
- ポート7687が開いているか確認
- URIが正しいか確認（`bolt://` not `http://`）

### Authentication Failed

```
neo4j.exceptions.AuthError: The client is unauthorized due to authentication failure
```

**解決策**:
- パスワードが正しいか確認
- デフォルトパスワード変更済みか確認（初回ログイン時）

### Out of Memory

```
java.lang.OutOfMemoryError: Java heap space
```

**解決策**:
- Neo4jのメモリ設定を増やす（上記参照）
- バッチサイズを小さくする

## Migration from SQLite

### Full Migration

```bash
# 1. Run pipeline with SQLite (as usual)
pixi run pipeline

# 2. Export to Neo4j
pixi run neo4j-export --clear

# 3. Verify data
pixi run neo4j-stats
```

### Incremental Updates

```python
# Load new credits
new_credits = [...]

# Append to Neo4j
with Neo4jWriter() as writer:
    writer.write_credits(new_credits)
```

## Neo4j Graph Data Science (GDS)

Neo4jはグラフアルゴリズムライブラリを提供（Enterprise or Aura DS）：

```cypher
-- Community detection (Louvain)
CALL gds.louvain.stream('collaborationGraph')
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).name_en, communityId
ORDER BY communityId

-- Betweenness centrality
CALL gds.betweenness.stream('collaborationGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name_en, score
ORDER BY score DESC LIMIT 20

-- PageRank (alternative implementation)
CALL gds.pageRank.stream('collaborationGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name_en, score
ORDER BY score DESC LIMIT 20
```

## Next Steps

- ✅ Neo4j setup complete
- ⏳ Explore Cypher queries in Neo4j Browser
- ⏳ Try GDS algorithms (if available)
- ⏳ Build custom dashboards with Neo4j Bloom

---

**Note**: Neo4j直接接続は大規模運用向けの機能です。小〜中規模データ（<100K records）の場合、SQLiteで十分です。

"""Neo4j 直接接続 — Python driver経由でグラフデータベースの読み書き.

大規模運用向けの機能。CSV exportと違い、リアルタイムでNeo4jに読み書きする。

Requirements:
    - Neo4j 5.0+ running (docker or local)
    - neo4j Python driver (pip install neo4j)
    - Environment variables:
        NEO4J_URI (default: bolt://localhost:7687)
        NEO4J_USER (default: neo4j)
        NEO4J_PASSWORD (required)

Usage:
    from src.analysis.neo4j_direct import Neo4jWriter, Neo4jReader

    # Write data
    writer = Neo4jWriter()
    writer.write_all(persons, anime_list, credits, scores)
    writer.close()

    # Query data
    with Neo4jReader() as reader:
        path = reader.find_shortest_path("p1", "p2")
        stats = reader.get_collaboration_stats()
"""

import os
from typing import Any

import structlog

from src.models import AnimeAnalysis as Anime, Credit, Person, ScoreResult

logger = structlog.get_logger()


class Neo4jWriter:
    """Neo4j データベースへの直接書き込みクラス.

    Attributes:
        driver: Neo4j driver instance
        uri: Neo4j connection URI
        user: Neo4j username
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        """Initialize Neo4j connection.

        Args:
            uri: Neo4j URI (default: bolt://localhost:7687)
            user: Neo4j username (default: neo4j)
            password: Neo4j password (required, or from NEO4J_PASSWORD env)

        Raises:
            ImportError: If neo4j driver not installed
            ValueError: If password not provided
        """
        try:
            from neo4j import GraphDatabase
        except ImportError as e:
            raise ImportError("neo4j driver not installed. Run: pixi install") from e

        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD")

        if not self.password:
            raise ValueError("NEO4J_PASSWORD required (env var or constructor arg)")

        logger.info("neo4j_connecting", uri=self.uri, user=self.user)
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

        # Verify connection
        self.driver.verify_connectivity()
        logger.info("neo4j_connected", uri=self.uri)

    def close(self):
        """Close Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("neo4j_disconnected")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def clear_database(self, confirm: bool = False):
        """Delete all nodes and relationships.

        WARNING: This is destructive! Use with caution.

        Args:
            confirm: Must be True to actually delete

        Raises:
            ValueError: If confirm is False
        """
        if not confirm:
            raise ValueError(
                "clear_database requires confirm=True to prevent accidents"
            )

        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.warning("neo4j_database_cleared")

    def create_constraints(self):
        """Create unique constraints and indexes for optimal query performance."""
        with self.driver.session() as session:
            # Unique constraints (also create indexes)
            constraints = [
                "CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
                "CREATE CONSTRAINT anime_id_unique IF NOT EXISTS FOR (a:Anime) REQUIRE a.id IS UNIQUE",
            ]

            # Additional indexes for common queries
            indexes = [
                "CREATE INDEX person_iv_score IF NOT EXISTS FOR (p:Person) ON (p.iv_score)",
                "CREATE INDEX anime_year IF NOT EXISTS FOR (a:Anime) ON (a.year)",
                "CREATE INDEX credit_role IF NOT EXISTS FOR ()-[r:CREDITED_IN]-() ON (r.role)",
            ]

            for cypher in constraints + indexes:
                try:
                    session.run(cypher)
                except Exception as e:
                    logger.warning(
                        "constraint_or_index_failed", cypher=cypher, error=str(e)
                    )

            logger.info(
                "neo4j_constraints_created", count=len(constraints) + len(indexes)
            )

    def write_persons(
        self,
        persons: list[Person],
        scores: list[ScoreResult] | None = None,
        batch_size: int = 1000,
    ):
        """Write Person nodes to Neo4j.

        Args:
            persons: List of Person objects
            scores: Optional list of ScoreResult objects
            batch_size: Number of nodes per transaction (default: 1000)
        """
        score_map = {}
        if scores:
            score_map = {s.person_id: s for s in scores}

        cypher = """
        UNWIND $persons AS person
        MERGE (p:Person {id: person.id})
        SET p.name_ja = person.name_ja,
            p.name_en = person.name_en,
            p.mal_id = person.mal_id,
            p.anilist_id = person.anilist_id,
            p.birank = person.birank,
            p.patronage = person.patronage,
            p.person_fe = person.person_fe,
            p.iv_score = person.iv_score
        """

        with self.driver.session() as session:
            for i in range(0, len(persons), batch_size):
                batch = persons[i : i + batch_size]
                person_data = [
                    {
                        "id": p.id,
                        "name_ja": p.name_ja,
                        "name_en": p.name_en,
                        "mal_id": p.mal_id,
                        "anilist_id": p.anilist_id,
                        "birank": round(score_map[p.id].birank, 2)
                        if p.id in score_map
                        else None,
                        "patronage": round(score_map[p.id].patronage, 2)
                        if p.id in score_map
                        else None,
                        "person_fe": round(score_map[p.id].person_fe, 2)
                        if p.id in score_map
                        else None,
                        "iv_score": round(score_map[p.id].iv_score, 2)
                        if p.id in score_map
                        else None,
                    }
                    for p in batch
                ]
                session.run(cypher, persons=person_data)

            logger.info("neo4j_persons_written", count=len(persons))

    def write_anime(self, anime_list: list[Anime], batch_size: int = 1000):
        """Write Anime nodes to Neo4j.

        Args:
            anime_list: List of Anime objects
            batch_size: Number of nodes per transaction (default: 1000)
        """
        cypher = """
        UNWIND $anime AS a
        MERGE (anime:Anime {id: a.id})
        SET anime.title_ja = a.title_ja,
            anime.title_en = a.title_en,
            anime.year = a.year,
            anime.season = a.season,
            anime.episodes = a.episodes,
            anime.mal_id = a.mal_id,
            anime.anilist_id = a.anilist_id
        """

        with self.driver.session() as session:
            for i in range(0, len(anime_list), batch_size):
                batch = anime_list[i : i + batch_size]
                anime_data = [
                    {
                        "id": a.id,
                        "title_ja": a.title_ja,
                        "title_en": a.title_en,
                        "year": a.year,
                        "season": a.season,
                        "episodes": a.episodes,
                        "mal_id": a.mal_id,
                        "anilist_id": a.anilist_id,
                    }
                    for a in batch
                ]
                session.run(cypher, anime=anime_data)

            logger.info("neo4j_anime_written", count=len(anime_list))

    def write_credits(self, credits: list[Credit], batch_size: int = 5000):
        """Write CREDITED_IN relationships to Neo4j.

        Args:
            credits: List of Credit objects
            batch_size: Number of relationships per transaction (default: 5000)
        """
        cypher = """
        UNWIND $credits AS credit
        MATCH (p:Person {id: credit.person_id})
        MATCH (a:Anime {id: credit.anime_id})
        MERGE (p)-[r:CREDITED_IN]->(a)
        SET r.role = credit.role,
            r.episode = credit.episode,
            r.source = credit.source
        """

        with self.driver.session() as session:
            for i in range(0, len(credits), batch_size):
                batch = credits[i : i + batch_size]
                credit_data = [
                    {
                        "person_id": c.person_id,
                        "anime_id": c.anime_id,
                        "role": c.role.value,
                        "episode": c.episode,
                        "source": c.source,
                    }
                    for c in batch
                ]
                session.run(cypher, credits=credit_data)

            logger.info("neo4j_credits_written", count=len(credits))

    def write_collaborations(
        self,
        credits: list[Credit],
        min_shared_works: int = 2,
        batch_size: int = 5000,
    ):
        """Write COLLABORATED_WITH relationships (Person ↔ Person).

        Args:
            credits: List of Credit objects
            min_shared_works: Minimum shared works to create edge (default: 2)
            batch_size: Number of relationships per transaction (default: 5000)
        """
        from collections import defaultdict

        # Build collaboration counts
        anime_persons: dict[str, set[str]] = defaultdict(set)
        for c in credits:
            anime_persons[c.anime_id].add(c.person_id)

        collab_counts: dict[tuple[str, str], int] = defaultdict(int)
        for anime_id, pids in anime_persons.items():
            pids_list = sorted(pids)
            for i in range(len(pids_list)):
                for j in range(
                    i + 1, min(i + 100, len(pids_list))
                ):  # Cap to avoid O(n²)
                    pair = (pids_list[i], pids_list[j])
                    collab_counts[pair] += 1

        # Filter by min_shared_works
        collab_edges = [
            {"pid1": pid1, "pid2": pid2, "count": count}
            for (pid1, pid2), count in collab_counts.items()
            if count >= min_shared_works
        ]

        cypher = """
        UNWIND $collabs AS collab
        MATCH (p1:Person {id: collab.pid1})
        MATCH (p2:Person {id: collab.pid2})
        MERGE (p1)-[r:COLLABORATED_WITH]-(p2)
        SET r.shared_works = collab.count
        """

        with self.driver.session() as session:
            for i in range(0, len(collab_edges), batch_size):
                batch = collab_edges[i : i + batch_size]
                session.run(cypher, collabs=batch)

            logger.info("neo4j_collaborations_written", count=len(collab_edges))

    def write_all(
        self,
        persons: list[Person],
        anime_list: list[Anime],
        credits: list[Credit],
        scores: list[ScoreResult] | None = None,
        clear: bool = False,
    ):
        """Write all data to Neo4j (persons, anime, credits, collaborations).

        Args:
            persons: List of Person objects
            anime_list: List of Anime objects
            credits: List of Credit objects
            scores: Optional list of ScoreResult objects
            clear: If True, clear database before writing (default: False)
        """
        logger.info(
            "neo4j_write_all_start",
            persons=len(persons),
            anime=len(anime_list),
            credits=len(credits),
        )

        if clear:
            self.clear_database(confirm=True)

        # Create constraints/indexes first
        self.create_constraints()

        # Write nodes
        self.write_persons(persons, scores)
        self.write_anime(anime_list)

        # Write relationships
        self.write_credits(credits)
        self.write_collaborations(credits)

        logger.info("neo4j_write_all_complete")

    def run_cypher(
        self, cypher: str, parameters: dict[str, Any] | None = None
    ) -> list[dict]:
        """Execute arbitrary Cypher query.

        Args:
            cypher: Cypher query string
            parameters: Optional query parameters

        Returns:
            List of result records as dicts
        """
        with self.driver.session() as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]

    def get_stats(self) -> dict[str, Any]:
        """Get database statistics.

        Returns:
            Dict with node counts, relationship counts, etc.
        """
        with self.driver.session() as session:
            person_count = session.run(
                "MATCH (p:Person) RETURN count(p) AS count"
            ).single()["count"]
            anime_count = session.run(
                "MATCH (a:Anime) RETURN count(a) AS count"
            ).single()["count"]
            credit_count = session.run(
                "MATCH ()-[r:CREDITED_IN]->() RETURN count(r) AS count"
            ).single()["count"]
            collab_count = session.run(
                "MATCH ()-[r:COLLABORATED_WITH]-() RETURN count(r) AS count"
            ).single()["count"]

            return {
                "persons": person_count,
                "anime": anime_count,
                "credits": credit_count,
                "collaborations": collab_count // 2,  # Undirected, so divide by 2
            }


class Neo4jReader:
    """Read-only Neo4j graph queries for richer analysis.

    Provides graph-native queries that exploit Neo4j's traversal engine:
    shortest paths, neighborhood exploration, common collaborators, etc.

    Attributes:
        driver: Neo4j driver instance
        uri: Neo4j connection URI
        user: Neo4j username
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        """Initialize Neo4j read-only connection.

        Args:
            uri: Neo4j URI (default: bolt://localhost:7687)
            user: Neo4j username (default: neo4j)
            password: Neo4j password (required, or from NEO4J_PASSWORD env)

        Raises:
            ImportError: If neo4j driver not installed
            ValueError: If password not provided
        """
        try:
            from neo4j import GraphDatabase
        except ImportError as e:
            raise ImportError("neo4j driver not installed. Run: pixi install") from e

        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD")

        if not self.password:
            raise ValueError("NEO4J_PASSWORD required (env var or constructor arg)")

        logger.info("neo4j_reader_connecting", uri=self.uri, user=self.user)
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

        self.driver.verify_connectivity()
        logger.info("neo4j_reader_connected", uri=self.uri)

    def close(self):
        """Close Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("neo4j_reader_disconnected")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def find_shortest_path(self, person_id_a: str, person_id_b: str) -> dict[str, Any]:
        """Find shortest collaboration path between two persons.

        Uses Neo4j's shortestPath algorithm on COLLABORATED_WITH edges.

        Args:
            person_id_a: Source person ID
            person_id_b: Target person ID

        Returns:
            Dict with path, length, and connection details.
            Empty path if no connection exists.
        """
        cypher = """
        MATCH (a:Person {id: $id_a}), (b:Person {id: $id_b})
        MATCH path = shortestPath((a)-[:COLLABORATED_WITH*]-(b))
        WITH path, nodes(path) AS ns, relationships(path) AS rels
        RETURN
            [n IN ns | n.id] AS node_ids,
            [n IN ns | coalesce(n.name_en, n.name_ja, n.id)] AS node_names,
            [r IN rels | r.shared_works] AS shared_works,
            length(path) AS path_length
        """
        with self.driver.session() as session:
            result = session.run(cypher, id_a=person_id_a, id_b=person_id_b)
            record = result.single()

        if record is None:
            logger.info(
                "neo4j_no_path_found",
                from_id=person_id_a,
                to_id=person_id_b,
            )
            return {"path": [], "length": 0, "connections": []}

        node_ids = list(record["node_ids"])
        node_names = list(record["node_names"])
        shared_works_list = list(record["shared_works"])

        connections = []
        for i in range(len(node_ids) - 1):
            connections.append(
                {
                    "from": node_ids[i],
                    "from_name": node_names[i],
                    "to": node_ids[i + 1],
                    "to_name": node_names[i + 1],
                    "shared_works": shared_works_list[i],
                }
            )

        return {
            "path": node_ids,
            "length": record["path_length"],
            "connections": connections,
        }

    def find_common_collaborators(
        self, person_id_a: str, person_id_b: str
    ) -> list[dict[str, Any]]:
        """Find persons who collaborated with both A and B.

        Args:
            person_id_a: First person ID
            person_id_b: Second person ID

        Returns:
            List of common collaborators with shared work counts.
        """
        cypher = """
        MATCH (a:Person {id: $id_a})-[r1:COLLABORATED_WITH]-(common:Person)
              -[r2:COLLABORATED_WITH]-(b:Person {id: $id_b})
        WHERE common <> a AND common <> b
        RETURN common.id AS person_id,
               coalesce(common.name_en, common.name_ja, common.id) AS name,
               r1.shared_works AS shared_with_a,
               r2.shared_works AS shared_with_b
        ORDER BY (r1.shared_works + r2.shared_works) DESC
        """
        with self.driver.session() as session:
            result = session.run(cypher, id_a=person_id_a, id_b=person_id_b)
            records = [record.data() for record in result]

        logger.info(
            "neo4j_common_collaborators",
            person_a=person_id_a,
            person_b=person_id_b,
            count=len(records),
        )
        return records

    def get_neighborhood(
        self,
        person_id: str,
        depth: int = 2,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Get the collaboration neighborhood around a person.

        Traverses COLLABORATED_WITH edges up to ``depth`` hops and returns
        the induced subgraph (nodes + edges).

        Args:
            person_id: Center person ID
            depth: Maximum traversal depth (default: 2)
            limit: Maximum number of neighbor nodes returned (default: 50)

        Returns:
            Dict with center, nodes, edges, and depth.
        """
        cypher = """
        MATCH (center:Person {id: $pid})
        CALL {
            WITH center
            MATCH path = (center)-[:COLLABORATED_WITH*1..$depth]-(neighbor:Person)
            WITH DISTINCT neighbor
            RETURN neighbor
            ORDER BY neighbor.iv_score DESC
            LIMIT $limit
        }
        WITH center, collect(neighbor) AS neighbors
        WITH center, neighbors, [center] + neighbors AS all_nodes
        UNWIND all_nodes AS n1
        UNWIND all_nodes AS n2
        WITH center, neighbors, all_nodes, n1, n2
        WHERE id(n1) < id(n2)
        OPTIONAL MATCH (n1)-[r:COLLABORATED_WITH]-(n2)
        WHERE r IS NOT NULL
        RETURN
            center.id AS center_id,
            [n IN neighbors | {
                id: n.id,
                name: coalesce(n.name_en, n.name_ja, n.id),
                iv_score: n.iv_score
            }] AS neighbor_nodes,
            collect(CASE WHEN r IS NOT NULL THEN {
                source: n1.id,
                target: n2.id,
                shared_works: r.shared_works
            } END) AS edges
        """
        with self.driver.session() as session:
            result = session.run(
                cypher,
                pid=person_id,
                depth=depth,
                limit=limit,
            )
            record = result.single()

        if record is None:
            logger.info("neo4j_person_not_found", person_id=person_id)
            return {
                "center": person_id,
                "nodes": [],
                "edges": [],
                "depth": depth,
            }

        neighbor_nodes = list(record["neighbor_nodes"])
        edges = [e for e in record["edges"] if e is not None]

        logger.info(
            "neo4j_neighborhood",
            person_id=person_id,
            depth=depth,
            nodes=len(neighbor_nodes),
            edges=len(edges),
        )
        return {
            "center": record["center_id"],
            "nodes": neighbor_nodes,
            "edges": edges,
            "depth": depth,
        }

    def find_influential_paths(
        self,
        person_id: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Find paths to the most influential persons (highest iv_score).

        Discovers shortest paths from the given person to the top-scored
        persons in the graph.

        Args:
            person_id: Source person ID
            limit: Maximum number of influential targets (default: 10)

        Returns:
            List of dicts with target info, iv_score, path length, and path.
        """
        cypher = """
        MATCH (source:Person {id: $pid})
        MATCH (target:Person)
        WHERE target <> source
          AND target.iv_score IS NOT NULL
        WITH source, target
        ORDER BY target.iv_score DESC
        LIMIT $limit
        MATCH path = shortestPath((source)-[:COLLABORATED_WITH*]-(target))
        RETURN
            target.id AS target_id,
            coalesce(target.name_en, target.name_ja, target.id) AS target_name,
            target.iv_score AS iv_score,
            length(path) AS path_length,
            [n IN nodes(path) | n.id] AS path
        ORDER BY target.iv_score DESC
        """
        with self.driver.session() as session:
            result = session.run(cypher, pid=person_id, limit=limit)
            records = []
            for record in result:
                records.append(
                    {
                        "target": record["target_id"],
                        "target_name": record["target_name"],
                        "iv_score": record["iv_score"],
                        "path_length": record["path_length"],
                        "path": list(record["path"]),
                    }
                )

        logger.info(
            "neo4j_influential_paths",
            person_id=person_id,
            paths_found=len(records),
        )
        return records

    def get_community_subgraph(
        self,
        person_ids: list[str],
    ) -> dict[str, Any]:
        """Extract subgraph for a set of persons (e.g., a community).

        Returns nodes and edges within the given set, plus density metric.

        Args:
            person_ids: List of person IDs defining the subgraph

        Returns:
            Dict with nodes, edges, and density.
        """
        cypher = """
        UNWIND $pids AS pid
        MATCH (p:Person {id: pid})
        WITH collect(p) AS persons
        UNWIND persons AS p1
        UNWIND persons AS p2
        WITH persons, p1, p2
        WHERE id(p1) < id(p2)
        OPTIONAL MATCH (p1)-[r:COLLABORATED_WITH]-(p2)
        WITH persons,
             collect(CASE WHEN r IS NOT NULL THEN {
                 source: p1.id,
                 target: p2.id,
                 shared_works: r.shared_works
             } END) AS raw_edges
        WITH persons,
             [e IN raw_edges WHERE e IS NOT NULL] AS edges
        RETURN
            [p IN persons | {
                id: p.id,
                name: coalesce(p.name_en, p.name_ja, p.id),
                iv_score: p.iv_score
            }] AS nodes,
            edges,
            size(persons) AS node_count,
            size(edges) AS edge_count
        """
        with self.driver.session() as session:
            result = session.run(cypher, pids=person_ids)
            record = result.single()

        if record is None:
            return {"nodes": [], "edges": [], "density": 0.0}

        nodes = list(record["nodes"])
        edges = list(record["edges"])
        node_count = record["node_count"]
        edge_count = record["edge_count"]

        # Density = 2 * |E| / (|V| * (|V| - 1)) for undirected graph
        density = 0.0
        if node_count >= 2:
            density = round(2.0 * edge_count / (node_count * (node_count - 1)), 4)

        logger.info(
            "neo4j_community_subgraph",
            person_count=node_count,
            edge_count=edge_count,
            density=density,
        )
        return {"nodes": nodes, "edges": edges, "density": density}

    def get_collaboration_stats(self) -> dict[str, Any]:
        """Get high-level collaboration graph statistics from Neo4j.

        Returns:
            Dict with total_persons, total_anime, total_credits,
            and avg_collaborators.
        """
        cypher = """
        MATCH (p:Person) WITH count(p) AS persons
        MATCH (a:Anime) WITH persons, count(a) AS anime
        MATCH ()-[r:CREDITED_IN]->() WITH persons, anime, count(r) AS credits
        OPTIONAL MATCH (p2:Person)-[c:COLLABORATED_WITH]-()
        WITH persons, anime, credits, count(c) AS collab_edges, count(DISTINCT p2) AS persons_with_collabs
        RETURN persons AS total_persons,
               anime AS total_anime,
               credits AS total_credits,
               CASE WHEN persons_with_collabs > 0
                    THEN toFloat(collab_edges) / persons_with_collabs
                    ELSE 0.0
               END AS avg_collaborators
        """
        with self.driver.session() as session:
            result = session.run(cypher)
            record = result.single()

        if record is None:
            return {
                "total_persons": 0,
                "total_anime": 0,
                "total_credits": 0,
                "avg_collaborators": 0.0,
            }

        stats = {
            "total_persons": record["total_persons"],
            "total_anime": record["total_anime"],
            "total_credits": record["total_credits"],
            "avg_collaborators": round(record["avg_collaborators"], 2),
        }

        logger.info("neo4j_collaboration_stats", **stats)
        return stats

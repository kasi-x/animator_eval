"""Neo4j 直接接続 — Python driver経由でグラフデータベースに直接書き込み.

大規模運用向けの機能。CSV exportと違い、リアルタイムでNeo4jに書き込む。

Requirements:
    - Neo4j 5.0+ running (docker or local)
    - neo4j Python driver (pip install neo4j)
    - Environment variables:
        NEO4J_URI (default: bolt://localhost:7687)
        NEO4J_USER (default: neo4j)
        NEO4J_PASSWORD (required)

Usage:
    from src.analysis.neo4j_direct import Neo4jWriter

    writer = Neo4jWriter()
    writer.write_all(persons, anime_list, credits, scores)
    writer.close()
"""

import os
from typing import Any

import structlog

from src.models import Anime, Credit, Person, ScoreResult

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
            raise ImportError(
                "neo4j driver not installed. Run: pixi install"
            ) from e

        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD")

        if not self.password:
            raise ValueError(
                "NEO4J_PASSWORD required (env var or constructor arg)"
            )

        logger.info("neo4j_connecting", uri=self.uri, user=self.user)
        self.driver = GraphDatabase.driver(
            self.uri, auth=(self.user, self.password)
        )

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
                "CREATE INDEX person_composite IF NOT EXISTS FOR (p:Person) ON (p.composite)",
                "CREATE INDEX anime_year IF NOT EXISTS FOR (a:Anime) ON (a.year)",
                "CREATE INDEX credit_role IF NOT EXISTS FOR ()-[r:CREDITED_IN]-() ON (r.role)",
            ]

            for cypher in constraints + indexes:
                try:
                    session.run(cypher)
                except Exception as e:
                    logger.warning("constraint_or_index_failed", cypher=cypher, error=str(e))

            logger.info("neo4j_constraints_created", count=len(constraints) + len(indexes))

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
            p.authority = person.authority,
            p.trust = person.trust,
            p.skill = person.skill,
            p.composite = person.composite
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
                        "authority": round(score_map[p.id].authority, 2) if p.id in score_map else None,
                        "trust": round(score_map[p.id].trust, 2) if p.id in score_map else None,
                        "skill": round(score_map[p.id].skill, 2) if p.id in score_map else None,
                        "composite": round(score_map[p.id].composite, 2) if p.id in score_map else None,
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
            anime.anilist_id = a.anilist_id,
            anime.score = a.score
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
                        "score": a.score,
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
                for j in range(i + 1, min(i + 100, len(pids_list))):  # Cap to avoid O(n²)
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

    def run_cypher(self, cypher: str, parameters: dict[str, Any] | None = None) -> list[dict]:
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
            person_count = session.run("MATCH (p:Person) RETURN count(p) AS count").single()["count"]
            anime_count = session.run("MATCH (a:Anime) RETURN count(a) AS count").single()["count"]
            credit_count = session.run("MATCH ()-[r:CREDITED_IN]->() RETURN count(r) AS count").single()["count"]
            collab_count = session.run("MATCH ()-[r:COLLABORATED_WITH]-() RETURN count(r) AS count").single()["count"]

            return {
                "persons": person_count,
                "anime": anime_count,
                "credits": credit_count,
                "collaborations": collab_count // 2,  # Undirected, so divide by 2
            }

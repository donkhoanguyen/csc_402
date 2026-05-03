"""
Machine-usable ontology verification script.

Outputs JSON with:
- node counts by required label
- relationship counts by required type
- sample Cypher query results for:
  1) SAFE_JOIN retrievability
  2) MetricDefinition-REQUIRES_TERM retrieval
  3) QueryPattern/FailureMode write-read path
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, List

from neo4j import GraphDatabase
from dotenv import load_dotenv

from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


NODE_LABELS = [
    "Database",
    "Table",
    "Column",
    "MetricDefinition",
    "BusinessTerm",
    "DataDomain",
    "QueryPattern",
    "FailureMode",
]

REL_TYPES = [
    "HAS",
    "FK",
    "SAFE_JOIN",
    "RISKY_JOIN",
    "USES_TABLE",
    "USES_COLUMN",
    "REQUIRES_TERM",
    "IN_DOMAIN",
    "USES_METRIC",
    "TRIGGERED_FAILURE",
    "AFFECTS",
]


def _resolve_credentials(args) -> Dict[str, str]:
    uri = args.neo4j_uri or os.getenv("ONTOLOGY_NEO4J_URI", "")
    username = args.neo4j_username or os.getenv("ONTOLOGY_NEO4J_USERNAME", "")
    password = args.neo4j_password or os.getenv("ONTOLOGY_NEO4J_PASSWORD", "")
    if not uri:
        uri = os.getenv("NEO4J_URI", "")
    if not username:
        username = os.getenv("NEO4J_USERNAME", "")
    if not password:
        password = os.getenv("NEO4J_PASSWORD", "")
    if not (uri and username and password):
        raise ValueError(
            "Missing target Neo4j credentials. Use --neo4j-* args or ONTOLOGY_NEO4J_* env vars."
        )
    return {"uri": uri, "username": username, "password": password}


def _count_by_labels(session, labels: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for label in labels:
        out[label] = session.run(
            f"MATCH (n:{label}) RETURN count(n) AS c"
        ).single()["c"]
    return out


def _count_by_rels(session, rel_types: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for rel in rel_types:
        out[rel] = session.run(
            f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c"
        ).single()["c"]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify ontology v1 graph in Neo4j")
    parser.add_argument("--neo4j-uri", default="")
    parser.add_argument("--neo4j-username", default="")
    parser.add_argument("--neo4j-password", default="")
    parser.add_argument("--db-id", default="", help="Optional db_id scope for sample queries")
    parser.add_argument("--output-json", default="", help="Optional path to write JSON output")
    args = parser.parse_args()

    creds = _resolve_credentials(args)
    driver = GraphDatabase.driver(
        creds["uri"], auth=(creds["username"], creds["password"])
    )

    try:
        with driver.session() as session:
            node_counts = _count_by_labels(session, NODE_LABELS)
            rel_counts = _count_by_rels(session, REL_TYPES)

            safe_join_query = """
            MATCH (a:Table)-[r:SAFE_JOIN]->(b:Table)
            WHERE $db_id = '' OR (a.db_id = $db_id AND b.db_id = $db_id)
            RETURN a.db_id AS db_id, a.name AS table_a, b.name AS table_b,
                   r.join_keys AS join_keys, r.cardinality AS cardinality,
                   r.risk_score AS risk_score, r.confidence AS confidence, r.evidence AS evidence
            LIMIT 5
            """
            safe_join_rows = session.run(safe_join_query, {"db_id": args.db_id}).data()

            metric_term_query = """
            MATCH (m:MetricDefinition)-[:REQUIRES_TERM]->(bt:BusinessTerm)
            WHERE $db_id = '' OR m.db_id = $db_id
            RETURN m.db_id AS db_id, m.name AS metric_name, collect(DISTINCT bt.name) AS required_terms
            LIMIT 5
            """
            metric_term_rows = session.run(metric_term_query, {"db_id": args.db_id}).data()

            write_read_query = """
            MERGE (qp:QueryPattern {db_id: $db_id, pattern_id: 'verify_pattern'})
            SET qp.intent_type = 'verification',
                qp.question_template = 'verification template',
                qp.success_rate = 1.0,
                qp.last_seen_at = datetime()
            MERGE (fm:FailureMode {db_id: $db_id, name: 'verify_failure_mode'})
            SET fm.description = 'verification failure mode'
            MERGE (qp)-[:TRIGGERED_FAILURE]->(fm)
            RETURN qp.pattern_id AS pattern_id, fm.name AS failure_mode
            """
            db_for_write = args.db_id
            if not db_for_write:
                db_row = session.run("MATCH (d:Database) RETURN d.db_id AS db_id LIMIT 1").single()
                db_for_write = db_row["db_id"] if db_row else "UNKNOWN_DB"
            write_read_rows = session.run(write_read_query, {"db_id": db_for_write}).data()

        payload: Dict[str, Any] = {
            "node_counts": node_counts,
            "relationship_counts": rel_counts,
            "samples": {
                "safe_join_retrieval": {
                    "cypher": safe_join_query.strip(),
                    "rows": safe_join_rows,
                },
                "metric_requires_term": {
                    "cypher": metric_term_query.strip(),
                    "rows": metric_term_rows,
                },
                "querypattern_failuremode_write_read": {
                    "cypher": write_read_query.strip(),
                    "rows": write_read_rows,
                },
            },
        }

        text = json.dumps(payload, indent=2, sort_keys=True, default=str)
        if args.output_json:
            with open(args.output_json, "w", encoding="utf-8") as f:
                f.write(text + "\n")
        print(text)
    finally:
        driver.close()


if __name__ == "__main__":
    main()

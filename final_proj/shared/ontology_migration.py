"""
Ontology migration and enrichment for the Neo4j analytics graph.

Implements the ontology/runtime requirements documented in ontology_whiteboard.html:
- Node labels: Database, Table, Column, MetricDefinition, BusinessTerm, DataDomain,
  QueryPattern, FailureMode
- Relationship types: HAS, FK, SAFE_JOIN, RISKY_JOIN, USES_TABLE, USES_COLUMN,
  REQUIRES_TERM, IN_DOMAIN, USES_METRIC, TRIGGERED_FAILURE, AFFECTS
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from neo4j import GraphDatabase
from dotenv import load_dotenv

DEFAULT_DOCS_DIR = Path(__file__).resolve().parents[1] / "external_knowledge_docs"
MAX_TERMS_PER_DOC = 10

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DOMAIN_KEYWORDS: Dict[str, Sequence[str]] = {
    "finance": ("revenue", "cost", "market", "price", "payment", "liquor"),
    "product": ("feature", "event", "retention", "session", "user", "ga4"),
    "healthcare": ("cancer", "genotype", "dicom", "patient", "clinical"),
    "sports": ("basketball", "baseball", "f1", "ncaa"),
    "geospatial": ("streetmap", "st_distance", "gis", "polygon", "haversine"),
    "blockchain": ("ethereum", "bridge", "token", "contract"),
}


def _slug_to_title(slug: str) -> str:
    return re.sub(r"\s+", " ", slug.replace("_", " ").strip()).title()


def _extract_identifiers(content: str) -> List[str]:
    return list(dict.fromkeys(re.findall(r"`([A-Za-z_][A-Za-z0-9_.]*)`", content)))


def _extract_terms(content: str, fallback_name: str) -> List[str]:
    headers = re.findall(r"^#{1,3}\s+(.+)$", content, re.MULTILINE)
    words = re.findall(r"\b[a-z][a-z0-9_]{3,}\b", content.lower())
    candidates = headers + words[:150]
    terms = []
    seen = set()
    for raw in [fallback_name] + candidates:
        term = raw.strip().lower().replace(" ", "_")
        if len(term) < 4:
            continue
        if term in seen:
            continue
        seen.add(term)
        terms.append(term)
        if len(terms) >= MAX_TERMS_PER_DOC:
            break
    return terms


def _infer_domain(name: str, content: str) -> str:
    haystack = f"{name} {content}".lower()
    scores: List[Tuple[int, str]] = []
    for domain, kws in DOMAIN_KEYWORDS.items():
        score = sum(1 for kw in kws if kw in haystack)
        if score:
            scores.append((score, domain))
    if not scores:
        return "general"
    scores.sort(reverse=True)
    return scores[0][1]


def create_indexes(driver) -> None:
    with driver.session() as session:
        # Neo4j 5 syntax; IF NOT EXISTS keeps operation idempotent.
        session.run(
            "CREATE CONSTRAINT metric_definition_key IF NOT EXISTS "
            "FOR (m:MetricDefinition) REQUIRE (m.db_id, m.name) IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT business_term_key IF NOT EXISTS "
            "FOR (b:BusinessTerm) REQUIRE (b.db_id, b.name) IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT data_domain_key IF NOT EXISTS "
            "FOR (d:DataDomain) REQUIRE d.name IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT query_pattern_key IF NOT EXISTS "
            "FOR (q:QueryPattern) REQUIRE (q.db_id, q.pattern_id) IS UNIQUE"
        )
        session.run(
            "CREATE CONSTRAINT failure_mode_key IF NOT EXISTS "
            "FOR (f:FailureMode) REQUIRE (f.db_id, f.name) IS UNIQUE"
        )


def _merge_metric_term_domain(
    tx,
    db_id: str,
    metric_name: str,
    definition: str,
    canonical_sql_template: str,
    domain_name: str,
    terms: Sequence[str],
) -> None:
    tx.run(
        """
        MERGE (d:DataDomain {name: $domain_name})
        MERGE (m:MetricDefinition {db_id: $db_id, name: $metric_name})
        SET m.definition = $definition,
            m.canonical_sql_template = $canonical_sql_template
        MERGE (m)-[:IN_DOMAIN]->(d)
        """,
        db_id=db_id,
        metric_name=metric_name,
        definition=definition,
        canonical_sql_template=canonical_sql_template,
        domain_name=domain_name,
    )
    for term in terms:
        tx.run(
            """
            MERGE (b:BusinessTerm {db_id: $db_id, name: $term})
            MERGE (d:DataDomain {name: $domain_name})
            MERGE (b)-[:IN_DOMAIN]->(d)
            WITH b
            MATCH (m:MetricDefinition {db_id: $db_id, name: $metric_name})
            MERGE (m)-[:REQUIRES_TERM]->(b)
            """,
            db_id=db_id,
            term=term,
            domain_name=domain_name,
            metric_name=metric_name,
        )


def _link_metric_assets(tx, db_id: str, metric_name: str, identifiers: Sequence[str]) -> None:
    table_candidates = set()
    column_candidates = set()
    for ident in identifiers:
        lowered = ident.lower()
        if "." in lowered:
            parts = lowered.split(".")
            if len(parts) >= 2:
                table_candidates.add(parts[-2])
                column_candidates.add(parts[-1])
        else:
            if lowered.endswith("_id") or lowered in {"date", "timestamp", "user", "users"}:
                column_candidates.add(lowered)
            else:
                table_candidates.add(lowered)

    if table_candidates:
        tx.run(
            """
            MATCH (m:MetricDefinition {db_id: $db_id, name: $metric_name})
            MATCH (t:Table {db_id: $db_id})
            WHERE toLower(t.name) IN $table_names
            MERGE (m)-[:USES_TABLE]->(t)
            MERGE (t)-[:IN_DOMAIN]->(:DataDomain {name: $domain_name})
            """,
            db_id=db_id,
            metric_name=metric_name,
            table_names=list(table_candidates),
            domain_name="general",
        )

    if column_candidates:
        tx.run(
            """
            MATCH (m:MetricDefinition {db_id: $db_id, name: $metric_name})
            MATCH (c:Column {db_id: $db_id})
            WHERE toLower(c.name) IN $column_names
            MERGE (m)-[:USES_COLUMN]->(c)
            """,
            db_id=db_id,
            metric_name=metric_name,
            column_names=list(column_candidates),
        )


def ingest_external_knowledge(driver, docs_dir: Path) -> Dict[str, int]:
    stats = {"docs": 0, "metrics": 0, "terms": 0, "uses_table_links": 0, "uses_column_links": 0}
    doc_paths = sorted(docs_dir.glob("*.md"))
    if not doc_paths:
        return stats

    with driver.session() as session:
        db_rows = session.run("MATCH (d:Database) RETURN d.db_id AS db_id").data()
    all_db_ids = [row["db_id"] for row in db_rows if row.get("db_id")]
    if not all_db_ids:
        return stats

    for doc_path in doc_paths:
        content = doc_path.read_text(encoding="utf-8", errors="ignore")
        metric_name = _slug_to_title(doc_path.stem)
        domain_name = _infer_domain(doc_path.stem, content)
        terms = _extract_terms(content, doc_path.stem)
        identifiers = _extract_identifiers(content)
        definition = content.strip().splitlines()[0][:500] if content.strip() else metric_name

        # If explicit db_ids appear in document, scope to those; otherwise apply globally.
        db_scoped = [db for db in all_db_ids if db.lower() in content.lower()]
        target_db_ids = db_scoped if db_scoped else all_db_ids

        with driver.session() as session:
            for db_id in target_db_ids:
                session.execute_write(
                    _merge_metric_term_domain,
                    db_id,
                    metric_name,
                    definition,
                    f"-- Template for {metric_name}",
                    domain_name,
                    terms,
                )
                session.execute_write(_link_metric_assets, db_id, metric_name, identifiers)

        stats["docs"] += 1
        stats["metrics"] += len(target_db_ids)
        stats["terms"] += len(terms) * len(target_db_ids)

    # Count relationship links for quick verification.
    with driver.session() as session:
        stats["uses_table_links"] = session.run(
            "MATCH (:MetricDefinition)-[r:USES_TABLE]->(:Table) RETURN count(r) AS c"
        ).single()["c"]
        stats["uses_column_links"] = session.run(
            "MATCH (:MetricDefinition)-[r:USES_COLUMN]->(:Column) RETURN count(r) AS c"
        ).single()["c"]
    return stats


def build_join_governance(driver) -> Dict[str, int]:
    with driver.session() as session:
        # Normalize FK edge properties for downstream join governance logic.
        session.run(
            """
            MATCH ()-[r:FK]->()
            SET r.enforced = coalesce(r.enforced, false),
                r.confidence = coalesce(r.confidence, CASE WHEN coalesce(r.enforced, false) THEN 1.0 ELSE 0.5 END)
            """
        )

        safe_count = session.run(
            """
            MATCH (src:Column)-[fk:FK]->(dst:Column)
            MATCH (ta:Table {db_id: src.db_id, name: src.table_name})
            MATCH (tb:Table {db_id: dst.db_id, name: dst.table_name})
            MERGE (ta)-[sj:SAFE_JOIN]->(tb)
            SET sj.join_keys = coalesce(sj.join_keys, []) + [src.name + '=' + dst.name],
                sj.cardinality = coalesce(sj.cardinality, CASE WHEN fk.enforced THEN 'N:1' ELSE 'UNKNOWN' END),
                sj.risk_score = CASE WHEN fk.enforced THEN 0.10 ELSE 0.40 END,
                sj.confidence = coalesce(fk.confidence, CASE WHEN fk.enforced THEN 1.0 ELSE 0.5 END),
                sj.evidence = CASE WHEN fk.enforced THEN 'declared_fk' ELSE 'inferred_fk' END
            RETURN count(sj) AS c
            """
        ).single()["c"]

        risky_count = session.run(
            """
            MATCH (src:Column)-[fk:FK]->(dst:Column)
            WHERE coalesce(fk.enforced, false) = false OR coalesce(fk.confidence, 0.0) < 0.6
            MATCH (ta:Table {db_id: src.db_id, name: src.table_name})
            MATCH (tb:Table {db_id: dst.db_id, name: dst.table_name})
            MERGE (ta)-[rj:RISKY_JOIN]->(tb)
            SET rj.reason = 'low_confidence_fk',
                rj.severity = CASE WHEN coalesce(fk.confidence, 0.0) < 0.3 THEN 'high' ELSE 'medium' END
            RETURN count(rj) AS c
            """
        ).single()["c"]

    return {"safe_join_edges": safe_count, "risky_join_edges": risky_count}


def print_counts(driver) -> None:
    node_labels = [
        "Database",
        "Table",
        "Column",
        "MetricDefinition",
        "BusinessTerm",
        "DataDomain",
        "QueryPattern",
        "FailureMode",
    ]
    rel_types = [
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

    print("=== Ontology node counts ===")
    with driver.session() as session:
        for label in node_labels:
            count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
            print(f"{label}\t{count}")

    print("\n=== Ontology relationship counts ===")
    with driver.session() as session:
        for rel in rel_types:
            count = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
            print(f"{rel}\t{count}")


def _resolve_neo4j_credentials(
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str,
) -> Tuple[str, str, str]:
    uri = neo4j_uri or os.getenv("ONTOLOGY_NEO4J_URI", "")
    username = neo4j_username or os.getenv("ONTOLOGY_NEO4J_USERNAME", "")
    password = neo4j_password or os.getenv("ONTOLOGY_NEO4J_PASSWORD", "")
    if not uri:
        uri = os.getenv("NEO4J_URI", "")
    if not username:
        username = os.getenv("NEO4J_USERNAME", "")
    if not password:
        password = os.getenv("NEO4J_PASSWORD", "")
    if not (uri and username and password):
        raise ValueError(
            "Missing target Neo4j credentials. Provide --neo4j-uri/--neo4j-username/--neo4j-password "
            "or set ONTOLOGY_NEO4J_URI/ONTOLOGY_NEO4J_USERNAME/ONTOLOGY_NEO4J_PASSWORD."
        )
    return uri, username, password


def run(
    docs_dir: Path,
    skip_semantic: bool = False,
    skip_join_governance: bool = False,
    neo4j_uri: str = "",
    neo4j_username: str = "",
    neo4j_password: str = "",
) -> None:
    uri, username, password = _resolve_neo4j_credentials(
        neo4j_uri=neo4j_uri,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
    )
    driver = GraphDatabase.driver(uri, auth=(username, password))
    create_indexes(driver)
    print("Created/verified ontology constraints.")

    if not skip_semantic:
        semantic_stats = ingest_external_knowledge(driver, docs_dir)
        print(f"Semantic ingest stats: {semantic_stats}")
    else:
        print("Semantic ingestion skipped.")

    if not skip_join_governance:
        join_stats = build_join_governance(driver)
        print(f"Join governance stats: {join_stats}")
    else:
        print("Join governance skipped.")

    print_counts(driver)
    driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ontology migration for Neo4j graph")
    parser.add_argument(
        "--docs-dir",
        default=str(DEFAULT_DOCS_DIR),
        help="Directory containing external knowledge markdown docs.",
    )
    parser.add_argument("--skip-semantic", action="store_true")
    parser.add_argument("--skip-join-governance", action="store_true")
    parser.add_argument("--neo4j-uri", default="", help="Target Neo4j URI (separate ontology graph).")
    parser.add_argument("--neo4j-username", default="", help="Target Neo4j username.")
    parser.add_argument("--neo4j-password", default="", help="Target Neo4j password.")
    args = parser.parse_args()

    run(
        docs_dir=Path(args.docs_dir),
        skip_semantic=args.skip_semantic,
        skip_join_governance=args.skip_join_governance,
        neo4j_uri=args.neo4j_uri,
        neo4j_username=args.neo4j_username,
        neo4j_password=args.neo4j_password,
    )


if __name__ == "__main__":
    main()

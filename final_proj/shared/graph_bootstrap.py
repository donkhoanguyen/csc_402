"""
Bootstrap Neo4j graph with schema nodes from BIRD and Spider2-lite.

BIRD:   Uses train_tables.json (69 DBs) + introspects validation SQLite (11 DBs)
Spider2-lite: Creates Database nodes only (schemas are cloud/BigQuery, not local)

Node labels:  (:Database), (:Table), (:Column)
Edges:        (:Database)-[:HAS]->(:Table)-[:HAS]->(:Column)
              (:Column)-[:FK]->(:Column)
"""

import json
import sqlite3
import tempfile
import os

from huggingface_hub import hf_hub_download
from datasets import load_dataset

from neo4j_client import get_driver, close

BIRD_REPO = "prem-research/birdbench"


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def merge_database(tx, db_id, benchmark):
    tx.run(
        "MERGE (d:Database {db_id: $db_id, benchmark: $benchmark}) "
        "SET d.name = $db_id",
        db_id=db_id, benchmark=benchmark,
    )


def merge_table(tx, db_id, table_name, benchmark):
    tx.run(
        "MERGE (t:Table {db_id: $db_id, name: $table_name, benchmark: $benchmark})",
        db_id=db_id, table_name=table_name, benchmark=benchmark,
    )
    tx.run(
        "MATCH (d:Database {db_id: $db_id, benchmark: $benchmark}) "
        "MATCH (t:Table {db_id: $db_id, name: $table_name, benchmark: $benchmark}) "
        "MERGE (d)-[:HAS]->(t)",
        db_id=db_id, table_name=table_name, benchmark=benchmark,
    )


def merge_column(tx, db_id, table_name, col_name, col_type, benchmark):
    tx.run(
        "MERGE (c:Column {db_id: $db_id, table_name: $table_name, name: $col_name, benchmark: $benchmark}) "
        "SET c.type = $col_type",
        db_id=db_id, table_name=table_name, col_name=col_name,
        col_type=col_type, benchmark=benchmark,
    )
    tx.run(
        "MATCH (t:Table {db_id: $db_id, name: $table_name, benchmark: $benchmark}) "
        "MATCH (c:Column {db_id: $db_id, table_name: $table_name, name: $col_name, benchmark: $benchmark}) "
        "MERGE (t)-[:HAS]->(c)",
        db_id=db_id, table_name=table_name, col_name=col_name, benchmark=benchmark,
    )


def merge_fk(tx, db_id, src_table, src_col, dst_table, dst_col, benchmark):
    tx.run(
        "MATCH (src:Column {db_id: $db_id, table_name: $src_table, name: $src_col, benchmark: $benchmark}) "
        "MATCH (dst:Column {db_id: $db_id, table_name: $dst_table, name: $dst_col, benchmark: $benchmark}) "
        "MERGE (src)-[:FK]->(dst)",
        db_id=db_id, src_table=src_table, src_col=src_col,
        dst_table=dst_table, dst_col=dst_col, benchmark=benchmark,
    )


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_bird_train_tables():
    """Parse train_tables.json -> list of DB schema dicts."""
    path = hf_hub_download(
        repo_id=BIRD_REPO,
        filename="train/train_databases/train_tables.json",
        repo_type="dataset",
    )
    with open(path) as f:
        return json.load(f)


def load_bird_validation_schemas():
    """
    Introspect validation SQLite files to produce schema dicts
    matching the train_tables.json structure.
    """
    val_dbs = [
        "california_schools", "card_games", "codebase_community",
        "debit_card_specializing", "european_football_2", "financial",
        "formula_1", "student_club", "superhero",
        "thrombosis_prediction", "toxicology",
    ]
    schemas = []
    for db_id in val_dbs:
        try:
            sqlite_path = hf_hub_download(
                repo_id=BIRD_REPO,
                filename=f"validation/dev_databases/{db_id}/{db_id}.sqlite",
                repo_type="dataset",
            )
        except Exception as e:
            print(f"  [WARN] Could not download {db_id}.sqlite: {e}")
            continue

        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]

        column_names_original = [[-1, "*"]]
        column_types = ["text"]
        table_names_original = tables
        fk_pairs = []

        for t_idx, table in enumerate(tables):
            cur.execute(f'PRAGMA table_info("{table}")')
            for col in cur.fetchall():
                column_names_original.append([t_idx, col[1]])
                column_types.append(col[2].lower() if col[2] else "text")

            cur.execute(f'PRAGMA foreign_key_list("{table}")')
            for fk in cur.fetchall():
                # fk: (id, seq, table, from, to, ...)
                src_col = fk[3]
                dst_table = fk[2]
                dst_col = fk[4]
                if dst_table in tables:
                    src_global = next(
                        (i for i, cn in enumerate(column_names_original)
                         if cn[0] == t_idx and cn[1] == src_col), None
                    )
                    dst_t_idx = tables.index(dst_table)
                    dst_global = next(
                        (i for i, cn in enumerate(column_names_original)
                         if cn[0] == dst_t_idx and cn[1] == dst_col), None
                    )
                    if src_global and dst_global:
                        fk_pairs.append([src_global, dst_global])

        conn.close()
        schemas.append({
            "db_id": db_id,
            "table_names_original": table_names_original,
            "column_names_original": column_names_original,
            "column_types": column_types,
            "foreign_keys": fk_pairs,
        })
        print(f"  [BIRD-val] {db_id}: {len(tables)} tables, "
              f"{len(column_names_original)-1} columns")

    return schemas


def write_bird_schemas(driver, schemas, benchmark="BIRD"):
    """Write BIRD schema dicts (train_tables.json format) to Neo4j."""
    for db in schemas:
        db_id = db["db_id"]
        tables = db["table_names_original"]
        col_names = db["column_names_original"]  # [[table_idx, col_name], ...]
        col_types = db["column_types"]
        fk_pairs = db.get("foreign_keys", [])  # [[src_col_idx, dst_col_idx], ...]

        with driver.session() as session:
            session.execute_write(merge_database, db_id, benchmark)
            for tname in tables:
                session.execute_write(merge_table, db_id, tname, benchmark)

            # col index 0 is always [-1, "*"], skip it
            for col_idx, (t_idx, col_name) in enumerate(col_names):
                if t_idx == -1:
                    continue
                if t_idx >= len(tables):
                    continue
                table_name = tables[t_idx]
                col_type = col_types[col_idx] if col_idx < len(col_types) else "text"
                session.execute_write(
                    merge_column, db_id, table_name, col_name, col_type, benchmark
                )

            # Foreign keys
            for src_idx, dst_idx in fk_pairs:
                if src_idx >= len(col_names) or dst_idx >= len(col_names):
                    continue
                src_t, src_c = col_names[src_idx]
                dst_t, dst_c = col_names[dst_idx]
                if src_t == -1 or dst_t == -1:
                    continue
                if src_t >= len(tables) or dst_t >= len(tables):
                    continue
                session.execute_write(
                    merge_fk, db_id,
                    tables[src_t], src_c,
                    tables[dst_t], dst_c,
                    benchmark,
                )

        n_cols = sum(1 for t_idx, _ in col_names if t_idx != -1)
        print(f"  [BIRD] {db_id}: {len(tables)} tables, {n_cols} columns, "
              f"{len(fk_pairs)} FKs")


def write_spider2_lite_dbs(driver):
    """Write Spider2-lite Database nodes (no schema available)."""
    ds = load_dataset("xlangai/spider2-lite", split="train")
    db_ids = sorted(set(ds["db"]))
    benchmark = "Spider2-Lite"

    with driver.session() as session:
        for db_id in db_ids:
            session.execute_write(merge_database, db_id, benchmark)

    print(f"  [Spider2-Lite] {len(db_ids)} database nodes written (no schema)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    driver = get_driver()

    print("\n=== BIRD train schemas ===")
    train_schemas = load_bird_train_tables()
    print(f"Loaded {len(train_schemas)} DBs from train_tables.json")
    write_bird_schemas(driver, train_schemas, benchmark="BIRD")

    print("\n=== BIRD validation schemas ===")
    val_schemas = load_bird_validation_schemas()
    write_bird_schemas(driver, val_schemas, benchmark="BIRD")

    print("\n=== Spider2-Lite databases ===")
    write_spider2_lite_dbs(driver)

    # Summary
    print("\n=== Neo4j node counts ===")
    with driver.session() as session:
        for label in ["Database", "Table", "Column"]:
            count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
            print(f"  {label}: {count:,}")
        fk_count = session.run(
            "MATCH ()-[r:FK]->() RETURN count(r) AS c"
        ).single()["c"]
        print(f"  FK relationships: {fk_count:,}")

    close()
    print("\nBootstrap complete.")


if __name__ == "__main__":
    main()

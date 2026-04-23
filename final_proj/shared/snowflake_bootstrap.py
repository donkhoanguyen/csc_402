"""
Bootstrap Neo4j graph with Spider 2.0 Snowflake schemas.

Connects to the sponsored Spider 2.0 Snowflake account, discovers all
databases/schemas/tables/columns in INFORMATION_SCHEMA, and writes:

  (:Database {benchmark:"Spider2"})
  (:Table)
  (:Column)

  (:Database)-[:HAS]->(:Table)-[:HAS]->(:Column)
  (:Column)-[:FK]->(:Column)   (where REFERENTIAL_CONSTRAINTS are available)

Run from the shared/ directory:
  python snowflake_bootstrap.py

Verification-only mode:
  python snowflake_bootstrap.py --verify
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
try:
    import snowflake.connector as snowflake_connector
except ModuleNotFoundError:
    snowflake_connector = None


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

load_dotenv()

# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

def _load_private_key():
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_conn():
    if snowflake_connector is None:
        raise RuntimeError(
            "snowflake-connector-python is required for bootstrap mode."
        )
    passcode = input("Microsoft Authenticator 6-digit code: ").strip()
    return snowflake_connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        authenticator="username_password_mfa",
        passcode=passcode,
        client_store_temporary_credential=True,
        client_session_keep_alive=True,
    )


def reconnect():
    """Re-authenticate when session expires mid-run."""
    log("Session expired — need a new TOTP code to reconnect.")
    conn = get_snowflake_conn()
    return conn, conn.cursor()


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------

def list_databases(cur):
    cur.execute("SHOW DATABASES")
    rows = cur.fetchall()
    # column 1 is the database name
    return [row[1] for row in rows]


QUERY_TIMEOUT = 120  # seconds per Snowflake query before giving up
SAMPLE_QUERY_TIMEOUT = 20
SAMPLE_VALUES_MAX_DISTINCT = 10
SAMPLE_VALUES_MAX_LEN = 200
SAMPLE_SKIP_NAME_PATTERNS = ("_id", "id_", "uuid", "guid", "created", "updated")
SAMPLE_SUPPORTED_TYPE_SNIPPETS = (
    "CHAR",
    "TEXT",
    "STRING",
    "BOOLEAN",
    "DATE",
    "TIME",
    "TIMESTAMP",
)


def get_tables(cur, database):
    """Return list of (schema_name, table_name, table_type, comment)."""
    try:
        cur.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, COMMENT
            FROM "{database}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """, timeout=QUERY_TIMEOUT)
        return cur.fetchall()
    except Exception as e:
        print(f"    [WARN] Could not read tables for {database}: {e}")
        return []


def get_columns(cur, database):
    """Return list of (schema, table, col_name, data_type, is_nullable, comment, ordinal)."""
    try:
        cur.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
                   DATA_TYPE, IS_NULLABLE, COMMENT, ORDINAL_POSITION
            FROM "{database}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """, timeout=QUERY_TIMEOUT)
        return cur.fetchall()
    except Exception as e:
        print(f"    [WARN] Could not read columns for {database}: {e}")
        return []


def get_foreign_keys(cur, database):
    """Return list of (src_schema, src_table, src_col, dst_schema, dst_table, dst_col)."""
    try:
        cur.execute(f"""
            SELECT
                kcu1.TABLE_SCHEMA  AS src_schema,
                kcu1.TABLE_NAME    AS src_table,
                kcu1.COLUMN_NAME   AS src_col,
                kcu2.TABLE_SCHEMA  AS dst_schema,
                kcu2.TABLE_NAME    AS dst_table,
                kcu2.COLUMN_NAME   AS dst_col
            FROM "{database}".INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN "{database}".INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
                ON kcu1.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
               AND kcu1.TABLE_SCHEMA    = rc.CONSTRAINT_SCHEMA
            JOIN "{database}".INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
               AND kcu2.TABLE_SCHEMA    = rc.UNIQUE_CONSTRAINT_SCHEMA
               AND kcu2.ORDINAL_POSITION = kcu1.ORDINAL_POSITION
            WHERE kcu1.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        """)
        return cur.fetchall()
    except Exception as e:
        # Many Snowflake DBs don't declare FK constraints — that's expected
        print(f"    [INFO] No FK constraints found for {database}: {e}")
        return []


def quote_ident(identifier):
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def normalize_identifier(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def should_collect_sample_values(col_name, data_type, is_view=False):
    if is_view:
        return False
    if not data_type:
        return False
    if not any(token in data_type.upper() for token in SAMPLE_SUPPORTED_TYPE_SNIPPETS):
        return False
    lname = (col_name or "").lower()
    if any(token in lname for token in SAMPLE_SKIP_NAME_PATTERNS):
        return False
    return True


def fetch_sample_values(sf_cur, raw_db_name, schema_name, table_name, col_name):
    """Return <= 10 distinct sample values for likely low-cardinality columns."""
    fq_table = (
        f"{quote_ident(raw_db_name)}.{quote_ident(schema_name)}.{quote_ident(table_name)}"
    )
    q_col = quote_ident(col_name)
    try:
        sf_cur.execute(
            f"""
            SELECT DISTINCT {q_col} AS v
            FROM {fq_table}
            WHERE {q_col} IS NOT NULL
            ORDER BY 1
            LIMIT {SAMPLE_VALUES_MAX_DISTINCT + 1}
            """,
            timeout=SAMPLE_QUERY_TIMEOUT,
        )
        rows = sf_cur.fetchall()
    except Exception as e:
        print(f"    [INFO] Sample values skipped for {schema_name}.{table_name}.{col_name}: {e}")
        return None

    if not rows or len(rows) > SAMPLE_VALUES_MAX_DISTINCT:
        return None

    values = []
    for row in rows:
        value = row[0]
        if value is None:
            continue
        sval = str(value)
        if len(sval) > SAMPLE_VALUES_MAX_LEN:
            sval = sval[:SAMPLE_VALUES_MAX_LEN]
        values.append(sval)

    return values or None


# ---------------------------------------------------------------------------
# Neo4j write helpers
# ---------------------------------------------------------------------------

def write_database(session, db_id, dialect="snowflake"):
    session.run(
        "MERGE (d:Database {db_id: $db_id, benchmark: 'Spider2'}) "
        "SET d.dialect = $dialect, d.name = $db_id",
        db_id=db_id, dialect=dialect,
    )


def write_table(session, db_id, table_name, table_type, description):
    is_view = table_type.upper() == "VIEW"
    session.run(
        "MERGE (t:Table {db_id: $db_id, name: $name, benchmark: 'Spider2'}) "
        "SET t.is_view = $is_view, t.description = $desc",
        db_id=db_id, name=table_name, is_view=is_view, desc=description or "",
    )
    session.run(
        "MATCH (d:Database {db_id: $db_id, benchmark: 'Spider2'}) "
        "MATCH (t:Table    {db_id: $db_id, name: $name, benchmark: 'Spider2'}) "
        "MERGE (d)-[:HAS]->(t)",
        db_id=db_id, name=table_name,
    )


def write_column(session, db_id, table_name, col_name, data_type, nullable, description):
    session.run(
        "MERGE (c:Column {db_id: $db_id, table_name: $tname, name: $cname, benchmark: 'Spider2'}) "
        "SET c.type = $dtype, c.nullable = $nullable, c.description = $desc",
        db_id=db_id, tname=table_name, cname=col_name,
        dtype=data_type, nullable=(nullable == "YES"), desc=description or "",
    )
    session.run(
        "MATCH (t:Table  {db_id: $db_id, name: $tname, benchmark: 'Spider2'}) "
        "MATCH (c:Column {db_id: $db_id, table_name: $tname, name: $cname, benchmark: 'Spider2'}) "
        "MERGE (t)-[:HAS]->(c)",
        db_id=db_id, tname=table_name, cname=col_name,
    )


def write_fk(session, db_id, src_table, src_col, dst_table, dst_col):
    session.run(
        "MATCH (src:Column {db_id: $db_id, table_name: $st, name: $sc, benchmark: 'Spider2'}) "
        "MATCH (dst:Column {db_id: $db_id, table_name: $dt, name: $dc, benchmark: 'Spider2'}) "
        "MERGE (src)-[r:FK]->(dst) "
        "SET r.enforced = true "
        "REMOVE r.inferred_by",
        db_id=db_id, st=src_table, sc=src_col, dt=dst_table, dc=dst_col,
    )


def write_inferred_fk(session, db_id, src_table, src_col, dst_table, dst_col):
    result = session.run(
        "MATCH (src:Column {db_id: $db_id, table_name: $st, name: $sc, benchmark: 'Spider2'}) "
        "MATCH (dst:Column {db_id: $db_id, table_name: $dt, name: $dc, benchmark: 'Spider2'}) "
        "WHERE NOT (src)-[:FK]->(dst) "
        "MERGE (src)-[r:FK]->(dst) "
        "SET r.enforced = false, r.inferred_by = 'name_match' "
        "RETURN count(r) AS written",
        db_id=db_id, st=src_table, sc=src_col, dt=dst_table, dc=dst_col,
    )
    return result.single()["written"]


def write_sample_values(session, db_id, table_name, col_name, values):
    session.run(
        "MATCH (c:Column {db_id: $db_id, table_name: $tname, name: $cname, benchmark: 'Spider2'}) "
        "SET c.sample_values = $values",
        db_id=db_id, tname=table_name, cname=col_name, values=values,
    )


def infer_fk_candidates(schema_cols):
    """Infer deterministic FK candidates from normalized exact column-name match."""
    by_normalized_name = {}
    for schema_name, table_name, col_name, *_ in schema_cols:
        key = normalize_identifier(col_name)
        if not key:
            continue
        by_normalized_name.setdefault(key, []).append((table_name, col_name))

    candidates = set()
    for matches in by_normalized_name.values():
        if len(matches) < 2:
            continue
        sorted_matches = sorted(matches)
        for i in range(len(sorted_matches)):
            for j in range(i + 1, len(sorted_matches)):
                left = sorted_matches[i]
                right = sorted_matches[j]
                if left[0] == right[0]:
                    # Skip self-table links; these are usually not meaningful FK relations.
                    continue
                candidates.add((left[0], left[1], right[0], right[1]))
    return sorted(candidates)


# ---------------------------------------------------------------------------
# Per-database ingestion
# ---------------------------------------------------------------------------

def ingest_database(sf_cur, neo4j_driver, raw_db_name, schema_filter=None):
    """
    Ingest one Snowflake database into Neo4j.

    db_id in Neo4j = "<database>.<schema>" when a DB has multiple schemas,
    or just "<database>" when there's only one non-INFORMATION_SCHEMA schema.
    """
    tables = get_tables(sf_cur, raw_db_name)
    columns = get_columns(sf_cur, raw_db_name)
    fks = get_foreign_keys(sf_cur, raw_db_name)
    totals = {"enforced_fk": 0, "inferred_fk": 0, "sample_columns": 0}

    if not tables:
        print(f"  [SKIP] {raw_db_name} — no tables found")
        return totals

    # Group by schema
    schemas = sorted(set(row[0] for row in tables))
    if schema_filter:
        schemas = [s for s in schemas if s in schema_filter]

    use_schema_suffix = len(schemas) > 1

    col_lookup = {}  # (schema, table, col) -> True  (for FK resolution)

    for schema in schemas:
        db_id = f"{raw_db_name}.{schema}" if use_schema_suffix else raw_db_name

        schema_tables = [(r[1], r[2], r[3]) for r in tables if r[0] == schema]
        schema_cols   = [r for r in columns if r[0] == schema]
        inferred_candidates = infer_fk_candidates(schema_cols)
        table_type_by_name = {tname: ttype for tname, ttype, _ in schema_tables}

        with neo4j_driver.session() as s:
            write_database(s, db_id)

            for tname, ttype, tdesc in schema_tables:
                write_table(s, db_id, tname, ttype, tdesc)

            for _, tname, cname, dtype, nullable, cdesc, _ in schema_cols:
                write_column(s, db_id, tname, cname, dtype, nullable, cdesc)
                col_lookup[(schema, tname, cname)] = db_id
                if should_collect_sample_values(
                    cname, dtype, is_view=(table_type_by_name.get(tname, "").upper() == "VIEW")
                ):
                    sample_values = fetch_sample_values(sf_cur, raw_db_name, schema, tname, cname)
                    if sample_values:
                        write_sample_values(s, db_id, tname, cname, sample_values)
                        totals["sample_columns"] += 1

            inferred_written = 0
            for src_t, src_c, dst_t, dst_c in inferred_candidates:
                inferred_written += write_inferred_fk(s, db_id, src_t, src_c, dst_t, dst_c)
            totals["inferred_fk"] += inferred_written

        print(f"    [{db_id}] {len(schema_tables)} tables, "
              f"{len(schema_cols)} columns, "
              f"{inferred_written} inferred FK edges, "
              f"{totals['sample_columns']} columns with sample values (running total)")

    # Foreign keys
    fk_written = 0
    with neo4j_driver.session() as s:
        for src_sch, src_t, src_c, dst_sch, dst_t, dst_c in fks:
            src_db = col_lookup.get((src_sch, src_t, src_c))
            dst_db = col_lookup.get((dst_sch, dst_t, dst_c))
            if src_db and dst_db and src_db == dst_db:
                write_fk(s, src_db, src_t, src_c, dst_t, dst_c)
                fk_written += 1

    totals["enforced_fk"] = fk_written
    if fk_written:
        print(f"    Enforced FK edges written: {fk_written}")
    return totals


def print_enrichment_counts(neo4j_driver):
    """Print enrichment counts from Neo4j for verification."""
    with neo4j_driver.session() as s:
        enforced_fk = s.run(
            "MATCH (:Column {benchmark:'Spider2'})-[r:FK {enforced:true}]->(:Column {benchmark:'Spider2'}) "
            "RETURN count(r) AS c"
        ).single()["c"]
        inferred_fk = s.run(
            "MATCH (:Column {benchmark:'Spider2'})-[r:FK {enforced:false, inferred_by:'name_match'}]->(:Column {benchmark:'Spider2'}) "
            "RETURN count(r) AS c"
        ).single()["c"]
        sample_columns = s.run(
            "MATCH (c:Column {benchmark:'Spider2'}) "
            "WHERE c.sample_values IS NOT NULL AND size(c.sample_values) > 0 "
            "RETURN count(c) AS c"
        ).single()["c"]

    print("\n=== Snowflake enrichment verification ===")
    print(f"  Enforced FK edges written: {enforced_fk:,}")
    print(f"  Inferred FK edges written: {inferred_fk:,}")
    print(f"  Columns with sample values: {sample_columns:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SKIP_SYSTEM_DBS = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}


def get_spider2_task_dbs():
    """Return set of uppercase DB names referenced by Spider 2.0 Snowflake tasks.

    Source: spider2-snow.jsonl (547 tasks, Snowflake-only slice of full Spider 2.0).
    Field: db_id (already uppercase in the file, but we upper() for safety).
    """
    import urllib.request
    import json
    url = ("https://raw.githubusercontent.com/xlang-ai/spider2/main"
           "/spider2-snow/spider2-snow.jsonl")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        tasks = [json.loads(line)
                 for line in r.read().decode().splitlines() if line.strip()]
    return {t["db_id"].upper() for t in tasks}


def main():
    from neo4j_client import get_driver, close as neo4j_close
    parser = argparse.ArgumentParser(
        description="Bootstrap Spider2 Snowflake schema graph and enrichment data."
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only print enrichment verification counts from Neo4j and exit.",
    )
    args = parser.parse_args()

    print("Connecting to Neo4j...")
    neo4j_driver = get_driver()
    if args.verify:
        print_enrichment_counts(neo4j_driver)
        neo4j_close()
        return

    print("Connecting to Snowflake...")
    sf_conn = get_snowflake_conn()
    sf_cur = sf_conn.cursor()

    print("\nLoading Spider 2.0 Snowflake task DB list (spider2-snow.jsonl)...")
    task_dbs = get_spider2_task_dbs()
    print(f"Spider 2.0 Snowflake tasks reference {len(task_dbs)} unique databases\n")

    print("Discovering Snowflake databases...")
    all_dbs = list_databases(sf_cur)
    # Only ingest DBs that are actually in Spider 2.0 tasks (case-insensitive match)
    spider_dbs = [db for db in all_dbs if db.upper() in task_dbs]
    skipped = [db for db in all_dbs if db not in SKIP_SYSTEM_DBS and db.upper() not in task_dbs]
    print(f"Matched {len(spider_dbs)} Snowflake DBs to Spider 2.0 tasks")
    print(f"Skipping {len(skipped)} non-task DBs\n")

    # Find already-ingested databases so we can resume after a restart
    with neo4j_driver.session() as s:
        done = {
            r["db_id"] for r in s.run(
                "MATCH (d:Database {benchmark:'Spider2'}) RETURN d.db_id AS db_id"
            ).data()
        }
    if done:
        print(f"Resuming — skipping {len(done)} already-ingested databases\n")

    total_enforced_fk = 0
    total_inferred_fk = 0
    total_sample_columns = 0
    for db_name in spider_dbs:
        # Simple check: skip if a db_id matching this name already exists
        already_done = any(d == db_name or d.startswith(f"{db_name}.") for d in done)
        if already_done:
            log(f"  [SKIP] {db_name} — already in Neo4j")
            continue
        log(f"Ingesting: {db_name} ...")
        try:
            counts = ingest_database(sf_cur, neo4j_driver, db_name)
            total_enforced_fk += counts["enforced_fk"]
            total_inferred_fk += counts["inferred_fk"]
            total_sample_columns += counts["sample_columns"]
        except Exception as e:
            is_session_expiry = (
                snowflake_connector is not None
                and isinstance(e, snowflake_connector.errors.DatabaseError)
                and (
                    "Session no longer exists" in str(e)
                    or "390114" in str(e)
                    or "390111" in str(e)
                )
            )
            if is_session_expiry:
                log(f"Session expired during {db_name} — reconnecting...")
                sf_conn, sf_cur = reconnect()
                log(f"Reconnected. Retrying {db_name} ...")
                try:
                    counts = ingest_database(sf_cur, neo4j_driver, db_name)
                    total_enforced_fk += counts["enforced_fk"]
                    total_inferred_fk += counts["inferred_fk"]
                    total_sample_columns += counts["sample_columns"]
                except Exception as e2:
                    log(f"  [ERROR] {db_name} after reconnect: {e2}")
            else:
                log(f"  [ERROR] {db_name}: {e}")

    # Summary
    print("\n=== Neo4j node counts after Snowflake bootstrap ===")
    with neo4j_driver.session() as s:
        for label in ["Database", "Table", "Column"]:
            count = s.run(
                f"MATCH (n:{label} {{benchmark:'Spider2'}}) RETURN count(n) AS c"
            ).single()["c"]
            print(f"  Spider2 {label}: {count:,}")
        fk_count = s.run(
            "MATCH (n:Column {benchmark:'Spider2'})-[r:FK]->() RETURN count(r) AS c"
        ).single()["c"]
        print(f"  Spider2 FK edges: {fk_count:,}")
    print(f"  Enforced FK edges written this run: {total_enforced_fk:,}")
    print(f"  Inferred FK edges written this run: {total_inferred_fk:,}")
    print(f"  Columns with sample values this run: {total_sample_columns:,}")
    print_enrichment_counts(neo4j_driver)

    sf_cur.close()
    sf_conn.close()
    neo4j_close()
    print("\nSnowflake bootstrap complete.")


if __name__ == "__main__":
    main()

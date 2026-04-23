"""
Graph retrieval helpers over Neo4j schema graph.
"""

try:
    from shared.neo4j_client import run_query
except ImportError:
    from neo4j_client import run_query

MAX_LIST_RESULTS = 200
MAX_PATH_HOPS = 10
DEFAULT_PATH_HOPS = 6


def _error(message):
    return {"ok": False, "error": message, "data": None}


def _ok(data, meta=None):
    payload = {"ok": True, "error": None, "data": data}
    if meta is not None:
        payload["meta"] = meta
    return payload


def _validate_non_empty_str(value, field_name):
    if not isinstance(value, str) or not value.strip():
        return f"{field_name} must be a non-empty string"
    return None


def get_tables(db_id):
    err = _validate_non_empty_str(db_id, "db_id")
    if err:
        return _error(err)

    rows = run_query(
        """
        MATCH (:Database {db_id: $db_id})-[:HAS]->(t:Table {db_id: $db_id})
        RETURN t.name AS table_name
        ORDER BY table_name
        LIMIT $limit
        """,
        {"db_id": db_id.strip(), "limit": MAX_LIST_RESULTS},
    )
    tables = [row["table_name"] for row in rows]
    return _ok({"db_id": db_id.strip(), "tables": tables, "count": len(tables)})


def get_columns(db_id, table_name):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        table_name, "table_name"
    )
    if err:
        return _error(err)

    rows = run_query(
        """
        MATCH (t:Table {db_id: $db_id, name: $table_name})-[:HAS]->(c:Column {db_id: $db_id})
        RETURN c.name AS column_name, coalesce(c.type, '') AS column_type
        ORDER BY column_name
        LIMIT $limit
        """,
        {
            "db_id": db_id.strip(),
            "table_name": table_name.strip(),
            "limit": MAX_LIST_RESULTS,
        },
    )
    columns = [
        {"name": row["column_name"], "type": row["column_type"]}
        for row in rows
    ]
    return _ok(
        {
            "db_id": db_id.strip(),
            "table_name": table_name.strip(),
            "columns": columns,
            "count": len(columns),
        }
    )


def find_join_path(db_id, table_a, table_b):
    err = (
        _validate_non_empty_str(db_id, "db_id")
        or _validate_non_empty_str(table_a, "table_a")
        or _validate_non_empty_str(table_b, "table_b")
    )
    if err:
        return _error(err)

    if table_a.strip() == table_b.strip():
        return _ok(
            {
                "db_id": db_id.strip(),
                "table_a": table_a.strip(),
                "table_b": table_b.strip(),
                "found": True,
                "path": [{"label": "Table", "name": table_a.strip()}],
                "relationships": [],
                "hops": 0,
            }
        )

    max_hops = min(MAX_PATH_HOPS, DEFAULT_PATH_HOPS)

    rows = run_query(
        f"""
        MATCH (a:Table {{db_id: $db_id, name: $table_a}})
        MATCH (b:Table {{db_id: $db_id, name: $table_b}})
        MATCH p = shortestPath((a)-[:HAS|FK*..{max_hops}]-(b))
        RETURN [n IN nodes(p) | {{
            label: head(labels(n)),
            name: coalesce(n.name, ''),
            table_name: coalesce(n.table_name, '')
        }}] AS node_path,
        [r IN relationships(p) | type(r)] AS rel_types
        LIMIT 1
        """,
        {
            "db_id": db_id.strip(),
            "table_a": table_a.strip(),
            "table_b": table_b.strip(),
        },
    )

    if not rows:
        return _ok(
            {
                "db_id": db_id.strip(),
                "table_a": table_a.strip(),
                "table_b": table_b.strip(),
                "found": False,
                "path": [],
                "relationships": [],
                "hops": None,
            },
            meta={"message": "No join path found within bounded search."},
        )

    node_path = rows[0]["node_path"]
    rel_types = rows[0]["rel_types"]
    return _ok(
        {
            "db_id": db_id.strip(),
            "table_a": table_a.strip(),
            "table_b": table_b.strip(),
            "found": True,
            "path": node_path,
            "relationships": rel_types,
            "hops": len(rel_types),
        }
    )


def search_columns(db_id, keyword):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        keyword, "keyword"
    )
    if err:
        return _error(err)

    rows = run_query(
        """
        MATCH (c:Column {db_id: $db_id})
        WHERE toLower(c.name) CONTAINS toLower($keyword)
        RETURN c.table_name AS table_name, c.name AS column_name, coalesce(c.type, '') AS column_type
        ORDER BY table_name, column_name
        LIMIT $limit
        """,
        {"db_id": db_id.strip(), "keyword": keyword.strip(), "limit": MAX_LIST_RESULTS},
    )
    matches = [
        {
            "table_name": row["table_name"],
            "column_name": row["column_name"],
            "column_type": row["column_type"],
        }
        for row in rows
    ]
    return _ok({"db_id": db_id.strip(), "keyword": keyword.strip(), "matches": matches, "count": len(matches)})


def get_metric(db_id, metric_name):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        metric_name, "metric_name"
    )
    if err:
        return _error(err)

    support_check = run_query(
        "MATCH (m:Metric {db_id: $db_id}) RETURN count(m) AS metric_count",
        {"db_id": db_id.strip()},
    )
    if not support_check or support_check[0]["metric_count"] == 0:
        return _ok(
            {"db_id": db_id.strip(), "metric_name": metric_name.strip(), "found": False, "metric": None},
            meta={"unsupported": True, "message": "Metric nodes are not available for this database."},
        )

    rows = run_query(
        """
        MATCH (m:Metric {db_id: $db_id, name: $metric_name})
        RETURN m.name AS name, properties(m) AS properties
        LIMIT 1
        """,
        {"db_id": db_id.strip(), "metric_name": metric_name.strip()},
    )
    if not rows:
        return _ok({"db_id": db_id.strip(), "metric_name": metric_name.strip(), "found": False, "metric": None})

    return _ok(
        {
            "db_id": db_id.strip(),
            "metric_name": metric_name.strip(),
            "found": True,
            "metric": rows[0]["properties"],
        }
    )


def get_dbt_lineage(db_id, model_name):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        model_name, "model_name"
    )
    if err:
        return _error(err)

    support_check = run_query(
        """
        MATCH (m)
        WHERE m.db_id = $db_id AND ('DbtModel' IN labels(m) OR 'Model' IN labels(m))
        RETURN count(m) AS model_count
        """,
        {"db_id": db_id.strip()},
    )
    if not support_check or support_check[0]["model_count"] == 0:
        return _ok(
            {
                "db_id": db_id.strip(),
                "model_name": model_name.strip(),
                "found": False,
                "lineage": {"upstream": [], "downstream": []},
            },
            meta={"unsupported": True, "message": "dbt model lineage nodes are not available for this database."},
        )

    rows = run_query(
        """
        MATCH (m)
        WHERE m.db_id = $db_id
          AND (m.name = $model_name OR m.model_name = $model_name)
          AND ('DbtModel' IN labels(m) OR 'Model' IN labels(m))
        OPTIONAL MATCH (up)-[:DEPENDS_ON]->(m)
        OPTIONAL MATCH (m)-[:DEPENDS_ON]->(down)
        RETURN collect(DISTINCT coalesce(up.name, up.model_name)) AS upstream,
               collect(DISTINCT coalesce(down.name, down.model_name)) AS downstream
        LIMIT 1
        """,
        {"db_id": db_id.strip(), "model_name": model_name.strip()},
    )

    if not rows:
        return _ok(
            {
                "db_id": db_id.strip(),
                "model_name": model_name.strip(),
                "found": False,
                "lineage": {"upstream": [], "downstream": []},
            }
        )

    upstream = [x for x in rows[0]["upstream"] if x]
    downstream = [x for x in rows[0]["downstream"] if x]
    return _ok(
        {
            "db_id": db_id.strip(),
            "model_name": model_name.strip(),
            "found": True,
            "lineage": {"upstream": upstream, "downstream": downstream},
        }
    )

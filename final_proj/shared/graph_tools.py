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


def _validate_str_list(values, field_name):
    if values is None:
        return None
    if not isinstance(values, list):
        return f"{field_name} must be a list of strings"
    for item in values:
        if not isinstance(item, str):
            return f"{field_name} must be a list of strings"
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

    # Runtime policy from ontology whiteboard: route joins through SAFE_JOIN.
    rows = run_query(
        f"""
        MATCH (a:Table {{db_id: $db_id, name: $table_a}})
        MATCH (b:Table {{db_id: $db_id, name: $table_b}})
        MATCH p = (a)-[:SAFE_JOIN*..{max_hops}]-(b)
        WITH p,
             reduce(cost = 0.0, r IN relationships(p) | cost + coalesce(r.risk_score, 1.0)) AS path_cost
        RETURN [n IN nodes(p) | {{
            label: head(labels(n)),
            name: coalesce(n.name, ''),
            table_name: coalesce(n.table_name, '')
        }}] AS node_path,
        [r IN relationships(p) | {{
            type: type(r),
            risk_score: coalesce(r.risk_score, 0.0),
            confidence: coalesce(r.confidence, 0.0)
        }}] AS rel_meta,
        path_cost
        ORDER BY path_cost ASC, length(p) ASC
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
            meta={
                "message": "No SAFE_JOIN path found within bounded search.",
                "join_policy": "SAFE_JOIN",
            },
        )

    node_path = rows[0]["node_path"]
    rel_meta = rows[0]["rel_meta"]
    path_cost = rows[0]["path_cost"]
    return _ok(
        {
            "db_id": db_id.strip(),
            "table_a": table_a.strip(),
            "table_b": table_b.strip(),
            "found": True,
            "path": node_path,
            "relationships": [r.get("type", "") for r in rel_meta],
            "relationship_meta": rel_meta,
            "hops": len(rel_meta),
            "join_policy": "SAFE_JOIN",
            "path_cost": path_cost,
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

    # Preferred ontology path: MetricDefinition (+ required business terms).
    md_rows = run_query(
        """
        MATCH (m:MetricDefinition {db_id: $db_id})
        WHERE toLower(coalesce(m.name, '')) = toLower($metric_name)
        OPTIONAL MATCH (m)-[:REQUIRES_TERM]->(bt:BusinessTerm)
        RETURN properties(m) AS properties,
               collect(DISTINCT coalesce(bt.name, '')) AS required_terms
        LIMIT 1
        """,
        {"db_id": db_id.strip(), "metric_name": metric_name.strip()},
    )
    if md_rows:
        required_terms = [x for x in md_rows[0]["required_terms"] if x]
        metric = md_rows[0]["properties"] or {}
        metric["required_terms"] = required_terms
        return _ok(
            {
                "db_id": db_id.strip(),
                "metric_name": metric_name.strip(),
                "found": True,
                "metric": metric,
                "metric_label": "MetricDefinition",
            }
        )

    # Compatibility fallback for legacy graphs.
    legacy_rows = run_query(
        """
        MATCH (m:Metric {db_id: $db_id, name: $metric_name})
        RETURN properties(m) AS properties
        LIMIT 1
        """,
        {"db_id": db_id.strip(), "metric_name": metric_name.strip()},
    )
    if legacy_rows:
        return _ok(
            {
                "db_id": db_id.strip(),
                "metric_name": metric_name.strip(),
                "found": True,
                "metric": legacy_rows[0]["properties"],
                "metric_label": "Metric",
            },
            meta={
                "legacy_fallback": True,
                "message": "Matched legacy Metric node; migrate to MetricDefinition when available.",
            },
        )

    return _ok(
        {"db_id": db_id.strip(), "metric_name": metric_name.strip(), "found": False, "metric": None},
        meta={
            "message": "No matching MetricDefinition found.",
            "expected_label": "MetricDefinition",
        },
    )


def get_metric_constraints(db_id, metric_name):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        metric_name, "metric_name"
    )
    if err:
        return _error(err)

    rows = run_query(
        """
        MATCH (m:MetricDefinition {db_id: $db_id})
        WHERE toLower(coalesce(m.name, '')) = toLower($metric_name)
        OPTIONAL MATCH (m)-[:REQUIRES_TERM]->(bt:BusinessTerm)
        OPTIONAL MATCH (m)-[:IN_DOMAIN]->(dd:DataDomain)
        RETURN properties(m) AS metric,
               collect(DISTINCT bt.name) AS required_terms,
               collect(DISTINCT dd.name) AS domains
        LIMIT 1
        """,
        {"db_id": db_id.strip(), "metric_name": metric_name.strip()},
    )
    if not rows:
        return _ok(
            {
                "db_id": db_id.strip(),
                "metric_name": metric_name.strip(),
                "found": False,
                "constraints": None,
            }
        )

    metric = rows[0]["metric"] or {}
    required_terms = [x for x in rows[0]["required_terms"] if x]
    domains = [x for x in rows[0]["domains"] if x]
    return _ok(
        {
            "db_id": db_id.strip(),
            "metric_name": metric_name.strip(),
            "found": True,
            "constraints": {
                "metric_definition": metric,
                "required_terms": required_terms,
                "domains": domains,
            },
        }
    )


def get_spreading_activation_context(db_id, seed_terms=None, seed_domains=None, limit=20):
    err = _validate_non_empty_str(db_id, "db_id")
    if err:
        return _error(err)
    err = _validate_str_list(seed_terms, "seed_terms") or _validate_str_list(
        seed_domains, "seed_domains"
    )
    if err:
        return _error(err)

    seed_terms = [s.strip().lower() for s in (seed_terms or []) if s.strip()]
    seed_domains = [s.strip().lower() for s in (seed_domains or []) if s.strip()]
    limit = int(limit) if isinstance(limit, (int, float, str)) else 20
    if limit <= 0:
        limit = 20
    if limit > MAX_LIST_RESULTS:
        limit = MAX_LIST_RESULTS

    rows = run_query(
        """
        MATCH (t:Table {db_id: $db_id})
        OPTIONAL MATCH (t)-[:IN_DOMAIN]->(dd:DataDomain)
        OPTIONAL MATCH (m:MetricDefinition {db_id: $db_id})-[:USES_TABLE]->(t)
        OPTIONAL MATCH (m)-[:REQUIRES_TERM]->(bt:BusinessTerm)
        WITH t,
             collect(DISTINCT toLower(coalesce(dd.name, ''))) AS table_domains,
             collect(DISTINCT toLower(coalesce(bt.name, ''))) AS table_terms,
             collect(DISTINCT coalesce(m.name, '')) AS metric_names
        WITH t, table_domains, table_terms, metric_names,
             size([d IN table_domains WHERE d <> '' AND d IN $seed_domains]) AS domain_hits,
             size([bt IN table_terms WHERE bt <> '' AND bt IN $seed_terms]) AS term_hits,
             coalesce(t.pagerank, 0.0) AS pagerank
        WITH t, metric_names, domain_hits, term_hits, pagerank,
             (domain_hits * 2.0) + (term_hits * 3.0) + pagerank AS activation_score
        WHERE $has_seeds = false OR activation_score > 0
        RETURN t.name AS table_name,
               activation_score,
               pagerank,
               metric_names
        ORDER BY activation_score DESC, pagerank DESC, table_name
        LIMIT $limit
        """,
        {
            "db_id": db_id.strip(),
            "seed_terms": seed_terms,
            "seed_domains": seed_domains,
            "has_seeds": bool(seed_terms or seed_domains),
            "limit": limit,
        },
    )

    candidates = [
        {
            "table_name": row["table_name"],
            "activation_score": row["activation_score"],
            "pagerank": row["pagerank"],
            "metric_names": [m for m in (row.get("metric_names") or []) if m],
        }
        for row in rows
    ]
    return _ok(
        {
            "db_id": db_id.strip(),
            "seed_terms": seed_terms,
            "seed_domains": seed_domains,
            "candidates": candidates,
            "count": len(candidates),
        }
    )


def upsert_query_pattern(
    db_id,
    pattern_id,
    intent_type,
    question_template,
    used_tables=None,
    used_metrics=None,
    success=True,
    latency_ms=0,
    failure_mode=None,
):
    err = (
        _validate_non_empty_str(db_id, "db_id")
        or _validate_non_empty_str(pattern_id, "pattern_id")
        or _validate_non_empty_str(intent_type, "intent_type")
        or _validate_non_empty_str(question_template, "question_template")
    )
    if err:
        return _error(err)
    err = _validate_str_list(used_tables, "used_tables") or _validate_str_list(
        used_metrics, "used_metrics"
    )
    if err:
        return _error(err)
    if failure_mode is not None:
        fm_err = _validate_non_empty_str(failure_mode, "failure_mode")
        if fm_err:
            return _error(fm_err)

    used_tables = [x.strip() for x in (used_tables or []) if x.strip()]
    used_metrics = [x.strip() for x in (used_metrics or []) if x.strip()]
    success = bool(success)
    latency_ms = int(latency_ms or 0)

    run_query(
        """
        MERGE (q:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
        ON CREATE SET q.intent_type = $intent_type,
                      q.question_template = $question_template,
                      q.success_count = 0,
                      q.failure_count = 0
        SET q.intent_type = $intent_type,
            q.question_template = $question_template,
            q.last_seen_at = datetime(),
            q.avg_latency_ms = CASE
                WHEN q.avg_latency_ms IS NULL OR q.total_runs IS NULL OR q.total_runs = 0
                    THEN $latency_ms
                ELSE ((q.avg_latency_ms * q.total_runs) + $latency_ms) / (q.total_runs + 1)
            END,
            q.total_runs = coalesce(q.total_runs, 0) + 1,
            q.success_count = coalesce(q.success_count, 0) + CASE WHEN $success THEN 1 ELSE 0 END,
            q.failure_count = coalesce(q.failure_count, 0) + CASE WHEN $success THEN 0 ELSE 1 END,
            q.success_rate = toFloat(coalesce(q.success_count, 0) + CASE WHEN $success THEN 1 ELSE 0 END)
                           / toFloat(coalesce(q.total_runs, 0) + 1)
        """,
        {
            "db_id": db_id.strip(),
            "pattern_id": pattern_id.strip(),
            "intent_type": intent_type.strip(),
            "question_template": question_template.strip(),
            "success": success,
            "latency_ms": latency_ms,
        },
    )

    if used_tables:
        run_query(
            """
            MATCH (q:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
            MATCH (t:Table {db_id: $db_id})
            WHERE t.name IN $used_tables
            MERGE (q)-[:USES_TABLE]->(t)
            """,
            {"db_id": db_id.strip(), "pattern_id": pattern_id.strip(), "used_tables": used_tables},
        )

    if used_metrics:
        run_query(
            """
            MATCH (q:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
            MATCH (m:MetricDefinition {db_id: $db_id})
            WHERE m.name IN $used_metrics
            MERGE (q)-[:USES_METRIC]->(m)
            """,
            {"db_id": db_id.strip(), "pattern_id": pattern_id.strip(), "used_metrics": used_metrics},
        )

    if failure_mode:
        run_query(
            """
            MERGE (f:FailureMode {db_id: $db_id, name: $failure_mode})
            ON CREATE SET f.description = 'auto_created_from_query_pattern'
            WITH f
            MATCH (q:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
            MERGE (q)-[:TRIGGERED_FAILURE]->(f)
            """,
            {
                "db_id": db_id.strip(),
                "pattern_id": pattern_id.strip(),
                "failure_mode": failure_mode.strip(),
            },
        )

    return _ok(
        {
            "db_id": db_id.strip(),
            "pattern_id": pattern_id.strip(),
            "updated": True,
            "used_tables": used_tables,
            "used_metrics": used_metrics,
            "failure_mode": failure_mode.strip() if failure_mode else None,
        }
    )


def upsert_failure_mode(
    db_id,
    name,
    description="",
    detection_rule="",
    mitigation_rule="",
    affected_tables=None,
    affected_metrics=None,
):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(name, "name")
    if err:
        return _error(err)
    err = _validate_str_list(affected_tables, "affected_tables") or _validate_str_list(
        affected_metrics, "affected_metrics"
    )
    if err:
        return _error(err)

    affected_tables = [x.strip() for x in (affected_tables or []) if x.strip()]
    affected_metrics = [x.strip() for x in (affected_metrics or []) if x.strip()]

    run_query(
        """
        MERGE (f:FailureMode {db_id: $db_id, name: $name})
        SET f.description = $description,
            f.detection_rule = $detection_rule,
            f.mitigation_rule = $mitigation_rule,
            f.last_updated_at = datetime()
        """,
        {
            "db_id": db_id.strip(),
            "name": name.strip(),
            "description": description.strip(),
            "detection_rule": detection_rule.strip(),
            "mitigation_rule": mitigation_rule.strip(),
        },
    )

    if affected_tables:
        run_query(
            """
            MATCH (f:FailureMode {db_id: $db_id, name: $name})
            MATCH (t:Table {db_id: $db_id})
            WHERE t.name IN $affected_tables
            MERGE (f)-[:AFFECTS]->(t)
            """,
            {
                "db_id": db_id.strip(),
                "name": name.strip(),
                "affected_tables": affected_tables,
            },
        )

    if affected_metrics:
        run_query(
            """
            MATCH (f:FailureMode {db_id: $db_id, name: $name})
            MATCH (m:MetricDefinition {db_id: $db_id})
            WHERE m.name IN $affected_metrics
            MERGE (f)-[:AFFECTS]->(m)
            """,
            {
                "db_id": db_id.strip(),
                "name": name.strip(),
                "affected_metrics": affected_metrics,
            },
        )

    return _ok(
        {
            "db_id": db_id.strip(),
            "name": name.strip(),
            "updated": True,
            "affected_tables": affected_tables,
            "affected_metrics": affected_metrics,
        }
    )


def get_dbt_lineage(db_id, model_name):
    err = _validate_non_empty_str(db_id, "db_id") or _validate_non_empty_str(
        model_name, "model_name"
    )
    if err:
        return _error(err)

    return _ok(
        {
            "db_id": db_id.strip(),
            "model_name": model_name.strip(),
            "found": False,
            "lineage": {"upstream": [], "downstream": []},
        },
        meta={
            "unsupported": True,
            "deprecated": True,
            "message": "dbt lineage is deprecated in the current ontology runtime.",
        },
    )

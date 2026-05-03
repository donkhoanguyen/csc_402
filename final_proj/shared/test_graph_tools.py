"""
Smoke test script for graph retrieval APIs.

Run:
  python test_graph_tools.py --db-id <db_id> --table-a <table_a> --table-b <table_b> --keyword <keyword>
"""

import argparse
import json

from graph_tools import (
    find_join_path,
    get_columns,
    get_metric,
    get_metric_constraints,
    get_spreading_activation_context,
    get_tables,
    search_columns,
    upsert_failure_mode,
    upsert_query_pattern,
)
from neo4j_client import close


def _print_result(name, payload):
    print(json.dumps({"api": name, "result": payload}, indent=2, sort_keys=True))


def _safe_call(name, fn, *args):
    try:
        _print_result(name, fn(*args))
    except Exception as exc:
        _print_result(name, {"ok": False, "error": str(exc), "data": None})


def main():
    parser = argparse.ArgumentParser(description="Graph tools smoke test")
    parser.add_argument("--db-id", required=True, help="Database identifier in Neo4j")
    parser.add_argument("--table-a", default="", help="Table A for join path")
    parser.add_argument("--table-b", default="", help="Table B for join path")
    parser.add_argument("--table-name", default="", help="Table name for get_columns")
    parser.add_argument("--keyword", default="id", help="Keyword for column search")
    parser.add_argument("--metric-name", default="revenue", help="Metric name lookup")
    parser.add_argument("--pattern-id", default="smoke_pattern", help="QueryPattern id for memory-loop test")
    args = parser.parse_args()

    try:
        _safe_call("get_tables", get_tables, args.db_id)

        table_for_columns = args.table_name
        if not table_for_columns:
            try:
                tables_response = get_tables(args.db_id)
            except Exception:
                tables_response = {"ok": False, "data": {}}
            tables = (
                tables_response.get("data", {}).get("tables", [])
                if tables_response.get("ok")
                else []
            )
            table_for_columns = tables[0] if tables else ""
        _safe_call("get_columns", get_columns, args.db_id, table_for_columns)

        _safe_call("search_columns", search_columns, args.db_id, args.keyword)

        join_table_a = args.table_a or table_for_columns
        join_table_b = args.table_b or table_for_columns
        _safe_call("find_join_path", find_join_path, args.db_id, join_table_a, join_table_b)

        _safe_call("get_metric", get_metric, args.db_id, args.metric_name)
        _safe_call("get_metric_constraints", get_metric_constraints, args.db_id, args.metric_name)
        _safe_call(
            "get_spreading_activation_context",
            get_spreading_activation_context,
            args.db_id,
            ["revenue", "retention"],
            ["finance", "product"],
            10,
        )
        _safe_call(
            "upsert_failure_mode",
            upsert_failure_mode,
            args.db_id,
            "fanout_join",
            "fanout caused duplicate aggregation",
            "join_cardinality_is_many_to_many",
            "choose SAFE_JOIN path with lower risk score",
            [join_table_a] if join_table_a else [],
            [args.metric_name],
        )
        _safe_call(
            "upsert_query_pattern",
            upsert_query_pattern,
            args.db_id,
            args.pattern_id,
            "trend",
            "monthly revenue trend for active users",
            [join_table_a] if join_table_a else [],
            [args.metric_name],
            True,
            1200,
            "fanout_join",
        )
    finally:
        close()


if __name__ == "__main__":
    main()

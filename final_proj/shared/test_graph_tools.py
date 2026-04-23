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
    get_dbt_lineage,
    get_metric,
    get_tables,
    search_columns,
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
    parser.add_argument("--model-name", default="fct_orders", help="dbt model name lookup")
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
        _safe_call("get_dbt_lineage", get_dbt_lineage, args.db_id, args.model_name)
    finally:
        close()


if __name__ == "__main__":
    main()

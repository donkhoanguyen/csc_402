# Context Graph Design

## Purpose

The graph serves as a structured knowledge base that a Claude agent queries as a tool when constructing SQL. Rather than dumping a full schema into the prompt, the agent navigates the graph to retrieve only what it needs — relevant tables, column semantics, join paths, business logic definitions, and sample values.

The target benchmark is Spider 2.0 (DuckDB/dbt slice initially, expanding to full benchmark).

---

## Node Types

### (:Database)
One node per database/dataset.

| Property | Type | Source |
|---|---|---|
| `db_id` | string | benchmark dataset |
| `benchmark` | string | `"Spider2"` / `"BIRD"` |
| `dialect` | string | `"duckdb"` / `"bigquery"` / `"snowflake"` / `"sqlite"` |
| `description` | string | external knowledge docs |

---

### (:Table)
One node per table or view within a database.

| Property | Type | Source |
|---|---|---|
| `name` | string | schema introspection |
| `db_id` | string | schema introspection |
| `description` | string | external knowledge docs |
| `row_count` | int | sampled at bootstrap time |
| `is_view` | bool | schema introspection |
| `partition_key` | string | BigQuery/DuckDB metadata |
| `partition_range` | string | e.g. `"2020-01-01 to 2023-12-31"` |

---

### (:Column)
One node per column within a table.

| Property | Type | Source |
|---|---|---|
| `name` | string | schema introspection |
| `table_name` | string | schema introspection |
| `db_id` | string | schema introspection |
| `type` | string | schema introspection |
| `description` | string | external knowledge docs |
| `sample_values` | list[string] | sampled at bootstrap (top 10 distinct, low-cardinality cols only) |
| `is_primary_key` | bool | schema introspection |
| `nullable` | bool | schema introspection |

---

### (:Metric)
Encodes business logic definitions — e.g. *"active user = triggered any event in the last 7 days"*. These come from Spider 2.0's external knowledge markdown files and are not derivable from schema alone.

| Property | Type | Source |
|---|---|---|
| `name` | string | external knowledge docs |
| `db_id` | string | external knowledge docs |
| `definition` | string | external knowledge docs |
| `formula` | string | SQL expression or description |

---

### (:DbtModel)
Only populated for dbt tasks. Represents a dbt model (a named SQL transformation).

| Property | Type | Source |
|---|---|---|
| `name` | string | dbt project files |
| `db_id` | string | dbt project files |
| `materialization` | string | `"table"` / `"view"` / `"incremental"` |
| `description` | string | dbt `schema.yml` |
| `raw_sql` | string | dbt model `.sql` file |

---

## Edge Types

### (:Database)-[:HAS]->(:Table)
A database contains a table.

### (:Table)-[:HAS]->(:Column)
A table contains a column.

### (:Column)-[:FK]->(:Column)
Foreign key relationship. Properties:
- `enforced` (bool) — whether the constraint is declared in the schema or inferred

### (:Column)-[:REFERENCES]->(:Metric)
A column is used in computing a metric. Links raw data to business definitions.

### (:Table)-[:REFERENCES]->(:Metric)
A table is the primary source for a metric.

### (:DbtModel)-[:DEPENDS_ON]->(:DbtModel)
dbt model dependency — model A's SQL references model B. Enables topological ordering and impact analysis.

### (:DbtModel)-[:READS]->(:Table)
A dbt model reads from a source table.

### (:DbtModel)-[:PRODUCES]->(:Table)
A dbt model materializes into a table (when materialization = `"table"` or `"incremental"`).

---

## What Populates Each Node

| Source | What it gives us |
|---|---|
| `train_tables.json` (BIRD) | Table names, column names, types, FK pairs |
| SQLite `PRAGMA` / DuckDB `INFORMATION_SCHEMA` | Same as above for Spider2 local DBs |
| Spider 2.0 external knowledge `.md` files | Column descriptions, metric definitions, domain context |
| Data sampling at bootstrap | `sample_values` on low-cardinality columns, `row_count` on tables |
| dbt `schema.yml` + model `.sql` files | DbtModel nodes, DEPENDS_ON edges, descriptions |
| BigQuery / Snowflake schema API | Table/column metadata for cloud DBs (if credentials available) |

---

## Tool API (What the Agent Calls)

These are the Cypher-backed functions exposed to the Claude agent as tools:

```
get_tables(db_id)
  → list of table names + descriptions

get_columns(db_id, table_name)
  → list of {name, type, description, sample_values, is_primary_key}

find_join_path(db_id, table_a, table_b)
  → shortest FK path between two tables as a list of join conditions

search_columns(db_id, keyword)
  → columns whose name or description matches keyword

get_metric(db_id, metric_name)
  → metric definition and the columns/tables it references

get_dbt_lineage(db_id, model_name)
  → upstream dependencies and downstream dependents of a dbt model
```

The agent is expected to call 2–4 tools per question rather than receiving the full schema upfront.

---

## What This Solves vs a Flat Schema Dump

| Problem | Flat schema dump | This graph |
|---|---|---|
| Large schemas (50+ tables) bloat prompt | All tables injected regardless of relevance | Agent fetches only relevant tables |
| Opaque column names | Agent guesses meaning | `description` from external knowledge docs |
| Unknown valid filter values | Agent hallucinates values | `sample_values` on categorical columns |
| Multi-table join paths | Agent infers from column names | `find_join_path` traverses FK edges |
| Business metric definitions | Not available | `(:Metric)` nodes with formulas |
| dbt model dependencies | Not available | `DEPENDS_ON` edge traversal |
| Dialect-specific SQL | Agent may write wrong dialect | `dialect` property on `(:Database)` |

---

## What This Does Not Solve

- **Actual data access** — the graph stores metadata, not rows. For questions requiring value lookups (e.g. *"which country has the most users?"*) the agent still needs to query the DB.
- **Cloud DB execution** — for BigQuery/Snowflake tasks, credentials and billing are still required at eval time.
- **Schema drift** — if the underlying DB changes, the graph needs a re-bootstrap.

# Project Progress — Agentic Relational Reasoning with Knowledge Graph Memory

## Status: Midpoint (April 11 2026)

---

## What's Built

### Infrastructure
- **Neo4j Aura** instance live at `neo4j+s://cb117bd7.databases.neo4j.io` (instance: `402_context_layer`)
- **Snowflake** access working via TOTP MFA (Microsoft Authenticator, one code per session)
- All credentials in `.env` (gitignored)

### Code
| File | What it does |
|---|---|
| `shared/neo4j_client.py` | Neo4j connection wrapper |
| `shared/graph_bootstrap.py` | BIRD schema ingestion (SQLite + HuggingFace) |
| `shared/snowflake_bootstrap.py` | Spider 2.0 Snowflake ingestion (resumable) |
| `shared/analyze_external_knowledge.py` | Downloads + analyzes 69 Spider 2.0 knowledge docs |
| `shared/gen_graph_stats.py` | Schema complexity plots from Neo4j |
| `gen_hin_diagram.py` | HIN schema diagram |
| `graph_design.md` | Full graph schema spec (node types, edge types, tool API) |

### Graph State (April 11 2026, ingestion still running)
- **107** Database nodes | **9,093** Table nodes | **56,939** Column nodes | **91** FK edges
- BIRD: 16/80 DBs ingested
- Spider 2.0: 91/114 task DBs ingested

---

## Key Findings So Far

1. **FK subgraph nearly empty** — 91 FK edges across 9,093 tables (<0.01 per table). Heuristic FK inference is not optional.
2. **Schema complexity is heavy-tailed** — median 8 tables/DB, max 4,989. Flat schema injection fails on the tail.
3. **15% of Spider 2.0 tasks require external knowledge docs** (39/260 tasks). Concentrated in a few reused docs.
4. **External knowledge docs are heavy-tailed in length** — 38 to 6,658 words (median 368). Can't inject all into prompts.
5. **SOTA baseline**: Spider 2.0 best agents ~17-19% success rate. BIRD ~60% execution accuracy. These are our targets.

---

## What's NOT Built Yet (critical path to final paper)

In priority order:

- [ ] Finish Snowflake bootstrap (~23 DBs remaining) — **resume tomorrow**
- [ ] Heuristic FK inference (column name matching across tables)
- [ ] Sample values on low-cardinality columns
- [ ] Metric nodes from Spider 2.0 external knowledge docs
- [ ] **Tool API** — 6 Cypher-backed functions (`get_tables`, `get_columns`, `find_join_path`, `search_columns`, `get_metric`, `get_dbt_lineage`)
- [ ] **LangGraph agent** — graph retrieval → SQL generation (Nemotron NIM) → execution → self-correction
- [ ] Baseline evaluation (plain LLM, text-only RAG) on BIRD subset
- [ ] Graph-agent evaluation + ablation
- [ ] Spider 2.0 evaluation (stretch goal)

**Final paper due: May 9 2026**

---

## Snowflake Auth Notes

- Account: `rsrsbdk-ydb67606`
- Key pair auth blocked (PARTICIPANT role can't `ALTER USER`)
- Browser SSO blocked (no SAML configured)
- **Working solution**: `username_password_mfa` + Microsoft Authenticator TOTP, entered once per terminal session
- Run bootstrap in **foreground** (not `nohup`) so it can prompt for TOTP on reconnect
- `client_session_keep_alive=True` keeps session alive for full run

---

## Spider 2.0 DB List

Only 114 of the 155 Snowflake databases are actually used in Spider 2.0 tasks.
Bootstrap filters against `xlangai/spider2-lite` HuggingFace dataset (`db` field, uppercased).

---

## How to Resume Bootstrap

```bash
cd /Users/khoadangnguyen/Desktop/csc_402/final_proj/shared
source ../venv/bin/activate
python snowflake_bootstrap.py
# Enter Microsoft Authenticator 6-digit code when prompted
# It will skip already-ingested DBs and continue from where it left off
```

"""
Ontology-first chat runtime for graph-guided SQL generation.

Canonical runtime flow follows ontology_whiteboard.html:
1) User question
2) Intent/entity extraction
3) Spreading activation over BusinessTerm + DataDomain
4) Candidate table/metric retrieval + ranking (pagerank + community)
5) Join path search over SAFE_JOIN weighted by risk
6) Constraint fetch from MetricDefinition -> REQUIRES_TERM
7) SQL generation with required filters
8) Validation checks (grain, risky joins, cost/fanout)
9) Execute query
10) Persist outcome to QueryPattern or FailureMode
11) Update confidence/score fields
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from shared.graph_tools import get_columns
    from shared.neo4j_client import run_query
except ImportError:  # pragma: no cover
    from graph_tools import get_columns  # type: ignore
    from neo4j_client import run_query  # type: ignore

try:
    import snowflake.connector as snowflake_connector
except Exception:  # pragma: no cover
    snowflake_connector = None


@dataclass
class RuntimeConfig:
    max_retries: int = 2
    trace_root: str = "eval/outputs/chat_runtime/traces"
    default_limit: int = 100
    openai_model: str = "gpt-4o-mini"
    max_output_tokens: int = 450
    use_live_execution: bool = False


def _now_ms() -> int:
    return int(time.time() * 1000)


def _tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[a-z0-9_]{3,}", text.lower())
    stop = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "what",
        "which",
        "where",
        "when",
        "over",
        "into",
        "have",
        "has",
        "want",
        "show",
        "find",
        "list",
        "give",
    }
    return [t for t in tokens if t not in stop]


def _extract_sql_from_response(text: str) -> str:
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    sql = (m.group(1) if m else text).strip()
    sql = re.sub(r"(?i)^\s*sql\s*query\s*:\s*", "", sql).strip()
    return sql.rstrip(";") + ";"


def _first_read_only_statement(sql: str) -> str:
    m = re.search(r"(?is)\b(select|with)\b", sql or "")
    if not m:
        return sql.strip()
    s = sql[m.start() :].strip()
    semi = s.find(";")
    return (s[: semi + 1] if semi >= 0 else s.rstrip(";") + ";").strip()


def _extract_response_text(payload: Dict[str, Any]) -> str:
    text = str(payload.get("output_text", "") or "").strip()
    if text:
        return text
    parts: List[str] = []
    output = payload.get("output", [])
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if (
                    isinstance(block, dict)
                    and block.get("type") in {"output_text", "text"}
                    and isinstance(block.get("text"), str)
                ):
                    parts.append(block["text"].strip())
    return "\n".join([p for p in parts if p]).strip()


class TraceLogger:
    def __init__(self, trace_root: str) -> None:
        self.trace_root = Path(trace_root)
        self.trace_root.mkdir(parents=True, exist_ok=True)
        self.events: List[Dict[str, Any]] = []

    def add(self, stage: str, payload: Dict[str, Any]) -> None:
        self.events.append({"ts_ms": _now_ms(), "stage": stage, "payload": payload})

    def flush(self, interaction_id: str) -> str:
        path = self.trace_root / f"{interaction_id}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump({"interaction_id": interaction_id, "events": self.events}, f, indent=2)
        return str(path)


class GraphRuntimeAdapter:
    """Thin graph adapter; keeps architecture compatible with external Agent 1 adapters."""

    @staticmethod
    def _query(cypher: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        try:
            return run_query(cypher, params or {})
        except Exception:
            return []

    def extract_intent_entities(self, question: str) -> Dict[str, Any]:
        q = question.lower()
        intent = "lookup"
        if any(x in q for x in ("count", "how many", "number of")):
            intent = "aggregation_count"
        elif any(x in q for x in ("average", "avg", "median", "sum", "total")):
            intent = "aggregation_metric"
        elif any(x in q for x in ("top", "highest", "lowest", "rank")):
            intent = "ranking"
        elif any(x in q for x in ("trend", "month", "year", "daily")):
            intent = "time_series"

        entities = _tokenize(question)[:20]
        return {"intent": intent, "entities": entities}

    def spreading_activation(
        self,
        db_id: str,
        seed_terms: Sequence[str],
        limit: int = 25,
    ) -> Dict[str, Any]:
        lowered = [s.lower() for s in seed_terms if s]
        if not lowered:
            return {"activated_terms": [], "domains": [], "tables": [], "metrics": []}

        term_rows = self._query(
            """
            MATCH (bt:BusinessTerm {db_id: $db_id})-[:IN_DOMAIN]->(dd:DataDomain)
            WHERE any(seed IN $seeds WHERE toLower(bt.name) CONTAINS seed)
            RETURN DISTINCT bt.name AS term, dd.name AS domain
            LIMIT $limit
            """,
            {"db_id": db_id, "seeds": lowered, "limit": limit},
        )
        domains = sorted({str(r.get("domain", "")).strip() for r in term_rows if r.get("domain")})

        table_rows = self._query(
            """
            MATCH (bt:BusinessTerm {db_id: $db_id})-[:IN_DOMAIN]->(dd:DataDomain)<-[:IN_DOMAIN]-(t:Table {db_id: $db_id})
            WHERE any(seed IN $seeds WHERE toLower(bt.name) CONTAINS seed)
            RETURN DISTINCT
              t.name AS table_name,
              coalesce(t.pagerank, 0.0) AS pagerank,
              coalesce(toString(t.community_id), '') AS community_id,
              dd.name AS domain,
              bt.name AS seed_term
            LIMIT $limit
            """,
            {"db_id": db_id, "seeds": lowered, "limit": limit},
        )

        metric_rows = self._query(
            """
            MATCH (m:MetricDefinition {db_id: $db_id})-[:REQUIRES_TERM]->(bt:BusinessTerm {db_id: $db_id})
            OPTIONAL MATCH (m)-[:IN_DOMAIN]->(dd:DataDomain)
            WHERE any(seed IN $seeds WHERE toLower(bt.name) CONTAINS seed)
            RETURN DISTINCT
              coalesce(m.name, m.metric_name) AS metric_name,
              coalesce(m.canonical_sql_template, '') AS canonical_sql_template,
              coalesce(dd.name, '') AS domain,
              bt.name AS seed_term
            LIMIT $limit
            """,
            {"db_id": db_id, "seeds": lowered, "limit": limit},
        )
        return {
            "activated_terms": sorted(
                {str(r.get("term", "")).strip() for r in term_rows if r.get("term")}
            ),
            "domains": domains,
            "tables": table_rows,
            "metrics": metric_rows,
        }

    def retrieve_and_rank_candidates(
        self,
        db_id: str,
        entities: Sequence[str],
        activation: Dict[str, Any],
        table_limit: int = 8,
        metric_limit: int = 5,
    ) -> Dict[str, Any]:
        community_votes: Dict[str, int] = {}
        for row in activation.get("tables", []):
            cid = str(row.get("community_id", "")).strip()
            if cid:
                community_votes[cid] = community_votes.get(cid, 0) + 1

        ranked_tables = []
        for row in activation.get("tables", []):
            pagerank = float(row.get("pagerank", 0.0) or 0.0)
            community = str(row.get("community_id", "") or "")
            community_bonus = 0.25 * community_votes.get(community, 0)
            score = pagerank + community_bonus
            ranked_tables.append(
                {
                    "table_name": row.get("table_name", ""),
                    "score": round(score, 4),
                    "pagerank": pagerank,
                    "community_id": community,
                    "domain": row.get("domain", ""),
                }
            )

        if not ranked_tables:
            fallback = self._query(
                """
                MATCH (t:Table {db_id: $db_id})
                WHERE any(seed IN $seeds WHERE toLower(t.name) CONTAINS seed)
                RETURN t.name AS table_name,
                       coalesce(t.pagerank, 0.0) AS pagerank,
                       coalesce(toString(t.community_id), '') AS community_id
                ORDER BY pagerank DESC, table_name
                LIMIT $limit
                """,
                {"db_id": db_id, "seeds": [e.lower() for e in entities], "limit": table_limit},
            )
            ranked_tables = [
                {
                    "table_name": r.get("table_name", ""),
                    "score": float(r.get("pagerank", 0.0) or 0.0),
                    "pagerank": float(r.get("pagerank", 0.0) or 0.0),
                    "community_id": str(r.get("community_id", "") or ""),
                    "domain": "",
                }
                for r in fallback
            ]

        ranked_tables.sort(key=lambda x: (-float(x.get("score", 0.0)), str(x.get("table_name", ""))))
        ranked_metrics = sorted(
            [
                {
                    "metric_name": r.get("metric_name", ""),
                    "score": 1.0,
                    "canonical_sql_template": r.get("canonical_sql_template", ""),
                    "domain": r.get("domain", ""),
                }
                for r in activation.get("metrics", [])
                if r.get("metric_name")
            ],
            key=lambda x: str(x.get("metric_name", "")),
        )
        return {
            "tables": ranked_tables[:table_limit],
            "metrics": ranked_metrics[:metric_limit],
        }

    def find_safe_join_path(self, db_id: str, tables: Sequence[str]) -> Dict[str, Any]:
        clean_tables = [t for t in tables if t]
        if len(clean_tables) < 2:
            return {"paths": [], "risk_score": 0.0}

        paths = []
        total_risk = 0.0
        for i in range(len(clean_tables) - 1):
            a = clean_tables[i]
            b = clean_tables[i + 1]
            rows = self._query(
                """
                MATCH (a:Table {db_id: $db_id, name: $table_a})
                MATCH (b:Table {db_id: $db_id, name: $table_b})
                MATCH p = (a)-[:SAFE_JOIN*1..6]-(b)
                WITH p, relationships(p) AS rels
                WITH p, rels,
                     reduce(cost = 0.0, r IN rels |
                        cost
                        + coalesce(r.risk_score, 0.4)
                        + (1.0 - coalesce(r.confidence, 0.5)) * 0.5
                        + 0.05
                     ) AS weighted_cost
                RETURN [n IN nodes(p) | n.name] AS table_path,
                       [r IN rels | {
                           risk_score: coalesce(r.risk_score, 0.4),
                           confidence: coalesce(r.confidence, 0.5),
                           join_keys: coalesce(r.join_keys, [])
                       }] AS rel_meta,
                       weighted_cost
                ORDER BY weighted_cost ASC, length(p) ASC
                LIMIT 1
                """,
                {"db_id": db_id, "table_a": a, "table_b": b},
            )
            if not rows:
                return {
                    "paths": paths,
                    "risk_score": 1.0,
                    "missing_pair": [a, b],
                    "error": "No SAFE_JOIN path found",
                }
            best = rows[0]
            risk = float(best.get("weighted_cost", 1.0) or 1.0)
            total_risk += risk
            paths.append(
                {
                    "from_table": a,
                    "to_table": b,
                    "table_path": best.get("table_path", []),
                    "relationships": best.get("rel_meta", []),
                    "weighted_cost": risk,
                }
            )
        return {"paths": paths, "risk_score": round(total_risk, 4)}

    def fetch_metric_constraints(
        self, db_id: str, metric_names: Sequence[str]
    ) -> Dict[str, List[str]]:
        if not metric_names:
            return {}
        rows = self._query(
            """
            MATCH (m:MetricDefinition {db_id: $db_id})-[:REQUIRES_TERM]->(bt:BusinessTerm {db_id: $db_id})
            WHERE toLower(coalesce(m.name, m.metric_name, '')) IN $metric_names
            RETURN coalesce(m.name, m.metric_name, '') AS metric_name,
                   collect(DISTINCT bt.name) AS required_terms
            """,
            {"db_id": db_id, "metric_names": [m.lower() for m in metric_names if m]},
        )
        return {
            str(r.get("metric_name", "")): [t for t in r.get("required_terms", []) if t]
            for r in rows
            if r.get("metric_name")
        }

    def persist_outcome(
        self,
        db_id: str,
        question: str,
        generated_sql: str,
        success: bool,
        failure_bucket: str,
        candidate_tables: Sequence[str],
        candidate_metrics: Sequence[str],
        join_paths: Sequence[Dict[str, Any]],
        confidence_score: float,
    ) -> None:
        pattern_id = str(uuid.uuid4())
        if success:
            self._query(
                """
                MERGE (qp:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
                SET qp.question = $question,
                    qp.sql = $generated_sql,
                    qp.success_rate = coalesce(qp.success_rate, 0.0) + 0.05,
                    qp.confidence_score = $confidence_score,
                    qp.updated_at_ms = $ts_ms
                """,
                {
                    "db_id": db_id,
                    "pattern_id": pattern_id,
                    "question": question,
                    "generated_sql": generated_sql,
                    "confidence_score": confidence_score,
                    "ts_ms": _now_ms(),
                },
            )
            for t in candidate_tables:
                self._query(
                    """
                    MATCH (qp:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
                    MATCH (t:Table {db_id: $db_id, name: $table_name})
                    MERGE (qp)-[:USES_TABLE]->(t)
                    """,
                    {"db_id": db_id, "pattern_id": pattern_id, "table_name": t},
                )
            for m in candidate_metrics:
                self._query(
                    """
                    MATCH (qp:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
                    MATCH (md:MetricDefinition {db_id: $db_id})
                    WHERE toLower(coalesce(md.name, md.metric_name, '')) = toLower($metric_name)
                    MERGE (qp)-[:USES_METRIC]->(md)
                    """,
                    {"db_id": db_id, "pattern_id": pattern_id, "metric_name": m},
                )
        else:
            self._query(
                """
                MERGE (fm:FailureMode {db_id: $db_id, name: $failure_name})
                SET fm.last_question = $question,
                    fm.last_error_sql = $generated_sql,
                    fm.count = coalesce(fm.count, 0) + 1,
                    fm.updated_at_ms = $ts_ms
                MERGE (qp:QueryPattern {db_id: $db_id, pattern_id: $pattern_id})
                SET qp.question = $question,
                    qp.sql = $generated_sql,
                    qp.success_rate = coalesce(qp.success_rate, 0.0) * 0.95,
                    qp.confidence_score = $confidence_score,
                    qp.updated_at_ms = $ts_ms
                MERGE (qp)-[:TRIGGERED_FAILURE]->(fm)
                """,
                {
                    "db_id": db_id,
                    "pattern_id": pattern_id,
                    "question": question,
                    "generated_sql": generated_sql,
                    "failure_name": failure_bucket,
                    "confidence_score": confidence_score,
                    "ts_ms": _now_ms(),
                },
            )

        # Update SAFE_JOIN confidence around used paths.
        for path in join_paths:
            nodes = [n for n in path.get("table_path", []) if n]
            for idx in range(len(nodes) - 1):
                delta = 0.02 if success else -0.05
                self._query(
                    """
                    MATCH (a:Table {db_id: $db_id, name: $from_table})-[sj:SAFE_JOIN]-(b:Table {db_id: $db_id, name: $to_table})
                    SET sj.confidence = least(1.0, greatest(0.0, coalesce(sj.confidence, 0.5) + $delta)),
                        sj.risk_score = least(1.0, greatest(0.0, coalesce(sj.risk_score, 0.4) - $delta * 0.2))
                    """,
                    {
                        "db_id": db_id,
                        "from_table": nodes[idx],
                        "to_table": nodes[idx + 1],
                        "delta": delta,
                    },
                )


class SqlExecutor:
    def __init__(self, use_live_execution: bool = False, passcode_env: str = "SNOWFLAKE_MFA_PASSCODE"):
        self.use_live_execution = use_live_execution
        self.passcode_env = passcode_env

    def _connect(self):
        if snowflake_connector is None:
            raise RuntimeError("snowflake-connector-python is unavailable")
        passcode = os.getenv(self.passcode_env, "").strip()
        if not passcode:
            raise RuntimeError(f"Missing {self.passcode_env} for live Snowflake execution")
        return snowflake_connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            authenticator="username_password_mfa",
            passcode=passcode,
            client_store_temporary_credential=True,
            client_session_keep_alive=True,
        )

    def execute(self, db_id: str, question: str, sql: str) -> Dict[str, Any]:
        if not self.use_live_execution:
            key = f"{db_id}|{question}|{sql}".encode("utf-8")
            h = int(hashlib.sha256(key).hexdigest(), 16)
            ok = (h % 7) != 0
            return {
                "success": ok,
                "error": "" if ok else "dry-run simulated execution failure",
                "rowcount": 3 if ok else -1,
                "preview_rows": [["dry_run", "preview"]],
                "mode": "dry-run",
            }

        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(f'USE DATABASE "{db_id}"')
            cur.execute(sql)
            rows = []
            try:
                rows = cur.fetchmany(5)
            except Exception:
                rows = []
            return {
                "success": True,
                "error": "",
                "rowcount": cur.rowcount,
                "preview_rows": rows,
                "mode": "live",
            }
        except Exception as exc:
            return {"success": False, "error": str(exc), "rowcount": -1, "preview_rows": [], "mode": "live"}
        finally:
            cur.close()
            conn.close()


class ChatRuntime:
    def __init__(
        self,
        config: Optional[RuntimeConfig] = None,
        graph_adapter: Optional[GraphRuntimeAdapter] = None,
        sql_executor: Optional[SqlExecutor] = None,
    ) -> None:
        self.config = config or RuntimeConfig()
        self.graph = graph_adapter or GraphRuntimeAdapter()
        self.sql = sql_executor or SqlExecutor(use_live_execution=self.config.use_live_execution)

    def _generate_sql(
        self,
        db_id: str,
        question: str,
        intent_entities: Dict[str, Any],
        ranked_candidates: Dict[str, Any],
        join_plan: Dict[str, Any],
        metric_constraints: Dict[str, List[str]],
        last_error: str = "",
        previous_sql: str = "",
    ) -> str:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        ranked_tables = [r.get("table_name", "") for r in ranked_candidates.get("tables", []) if r.get("table_name")]
        ranked_metrics = [
            r.get("metric_name", "") for r in ranked_candidates.get("metrics", []) if r.get("metric_name")
        ]
        required_terms = sorted({t for terms in metric_constraints.values() for t in terms})

        if not api_key:
            top_table = ranked_tables[0] if ranked_tables else ""
            if not top_table:
                return "SELECT 1 AS no_candidate_table;"
            fallback_filters = []
            cols_resp = get_columns(db_id, top_table)
            if cols_resp.get("ok"):
                col_names = [c.get("name", "") for c in cols_resp.get("data", {}).get("columns", [])]
                for term in required_terms:
                    token = term.split("_")[0]
                    matched = next((c for c in col_names if token and token.lower() in c.lower()), "")
                    if matched:
                        fallback_filters.append(f'"{matched}" IS NOT NULL')
            where_clause = " AND ".join(fallback_filters) if fallback_filters else "1=1"
            return f'SELECT * FROM "{top_table}" WHERE {where_clause} LIMIT {self.config.default_limit};'

        prompt = (
            "You are a SQL planning agent following ontology runtime constraints.\n"
            "Use only read-only Snowflake SQL (SELECT/WITH).\n"
            "Prioritize candidate tables/metrics and SAFE_JOIN plan.\n"
            "Apply required filters from metric constraints when possible.\n"
            "Return SQL only.\n\n"
            f"db_id: {db_id}\n"
            f"question: {question}\n"
            f"intent_entities: {json.dumps(intent_entities, ensure_ascii=True)}\n"
            f"ranked_tables: {json.dumps(ranked_tables, ensure_ascii=True)}\n"
            f"ranked_metrics: {json.dumps(ranked_metrics, ensure_ascii=True)}\n"
            f"join_plan: {json.dumps(join_plan, ensure_ascii=True)}\n"
            f"metric_constraints: {json.dumps(metric_constraints, ensure_ascii=True)}\n"
        )
        if last_error:
            prompt += f"\nlast_error: {last_error}\n"
        if previous_sql:
            prompt += f"\nprevious_sql: {previous_sql}\n"

        body = {
            "model": self.config.openai_model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
            "max_output_tokens": self.config.max_output_tokens,
        }
        req = urllib.request.Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"OpenAI HTTPError {exc.code}: {detail}") from exc

        text = _extract_response_text(payload)
        if not text:
            raise RuntimeError("OpenAI returned no text content")
        return _first_read_only_statement(_extract_sql_from_response(text))

    @staticmethod
    def _validate_sql(sql: str, join_plan: Dict[str, Any]) -> Dict[str, Any]:
        errors = []
        warnings = []
        lower = sql.lower().strip()
        if not (lower.startswith("select") or lower.startswith("with")):
            errors.append("Generated SQL must be read-only (SELECT/WITH).")
        join_count = len(re.findall(r"(?i)\bjoin\b", sql))
        if join_count > 8:
            errors.append("Join fanout risk: too many joins in generated SQL.")
        if len(sql) > 14000:
            errors.append("Estimated query cost too high (SQL length heuristic).")
        if " group by " in lower and re.search(r"(?i)\bcount\(|sum\(|avg\(|median\(", sql):
            pass
        elif re.search(r"(?i)\bcount\(|sum\(|avg\(|median\(", sql):
            warnings.append("Possible grain mismatch: aggregates without explicit GROUP BY.")
        if join_plan.get("risk_score", 0.0) > 2.4:
            warnings.append("SAFE_JOIN weighted risk is elevated.")
        return {"valid": not errors, "errors": errors, "warnings": warnings}

    @staticmethod
    def _bucket_failure(message: str) -> str:
        m = (message or "").lower()
        if not m:
            return "none"
        if "safe_join" in m or "join" in m:
            return "join_path_or_fanout"
        if "read-only" in m or "select/with" in m:
            return "validation_non_readonly"
        if "openai" in m or "api" in m or "credential" in m:
            return "llm_or_auth"
        if "sql compilation error" in m:
            return "sql_compile"
        if "dry-run simulated" in m:
            return "simulated_execution_failure"
        return "other"

    def run_interaction(
        self,
        *,
        db_id: str,
        question: str,
        max_retries: Optional[int] = None,
    ) -> Dict[str, Any]:
        retries = max(0, self.config.max_retries if max_retries is None else int(max_retries))
        interaction_id = f"{db_id}_{uuid.uuid4().hex[:12]}"
        trace = TraceLogger(self.config.trace_root)
        trace.add("stage_1_user_question", {"db_id": db_id, "question": question})

        attempts = 0
        last_error = ""
        generated_sql = ""
        execution_meta: Dict[str, Any] = {}
        validation_meta: Dict[str, Any] = {}
        ranked_candidates: Dict[str, Any] = {"tables": [], "metrics": []}
        join_plan: Dict[str, Any] = {"paths": [], "risk_score": 0.0}
        metric_constraints: Dict[str, List[str]] = {}

        while attempts <= retries:
            # 2) Intent/entity extraction
            intent_entities = self.graph.extract_intent_entities(question)
            trace.add("stage_2_intent_entity_extraction", intent_entities)

            # 3) Spreading activation
            activation = self.graph.spreading_activation(db_id, intent_entities.get("entities", []))
            trace.add("stage_3_spreading_activation", activation)

            # 4) Candidate retrieval + ranking
            ranked_candidates = self.graph.retrieve_and_rank_candidates(
                db_id=db_id,
                entities=intent_entities.get("entities", []),
                activation=activation,
            )
            trace.add("stage_4_candidate_retrieval_ranking", ranked_candidates)

            # 5) Join path search SAFE_JOIN weighted by risk
            top_tables = [t.get("table_name", "") for t in ranked_candidates.get("tables", [])[:3]]
            join_plan = self.graph.find_safe_join_path(db_id=db_id, tables=top_tables)
            trace.add("stage_5_join_path_safe_join", join_plan)
            if join_plan.get("error"):
                last_error = str(join_plan["error"])
                attempts += 1
                if attempts > retries:
                    break
                continue

            # 6) MetricDefinition -> REQUIRES_TERM constraints
            metric_names = [m.get("metric_name", "") for m in ranked_candidates.get("metrics", [])]
            metric_constraints = self.graph.fetch_metric_constraints(db_id, metric_names)
            trace.add("stage_6_metric_constraints", metric_constraints)

            # 7) SQL generation with required filters
            try:
                generated_sql = self._generate_sql(
                    db_id=db_id,
                    question=question,
                    intent_entities=intent_entities,
                    ranked_candidates=ranked_candidates,
                    join_plan=join_plan,
                    metric_constraints=metric_constraints,
                    last_error=last_error,
                    previous_sql=generated_sql,
                )
                trace.add("stage_7_sql_generation", {"sql": generated_sql})
            except Exception as exc:
                last_error = str(exc)
                trace.add("stage_7_sql_generation", {"error": last_error})
                attempts += 1
                continue

            # 8) Validation checks
            validation_meta = self._validate_sql(generated_sql, join_plan)
            trace.add("stage_8_validation_checks", validation_meta)
            if not validation_meta.get("valid"):
                last_error = "; ".join(validation_meta.get("errors", []))
                attempts += 1
                continue

            # 9) Execute query
            execution_meta = self.sql.execute(db_id=db_id, question=question, sql=generated_sql)
            trace.add("stage_9_execute_query", execution_meta)
            if execution_meta.get("success"):
                break

            last_error = str(execution_meta.get("error", "execution_failed"))
            attempts += 1

        success = bool(execution_meta.get("success"))
        failure_bucket = "none" if success else self._bucket_failure(last_error)
        confidence_score = max(0.0, 1.0 - (0.18 * attempts) - (0.06 * float(join_plan.get("risk_score", 0.0))))

        # 10 + 11) Persist outcome + update confidence/score fields
        try:
            self.graph.persist_outcome(
                db_id=db_id,
                question=question,
                generated_sql=generated_sql,
                success=success,
                failure_bucket=failure_bucket,
                candidate_tables=[t.get("table_name", "") for t in ranked_candidates.get("tables", []) if t.get("table_name")],
                candidate_metrics=[m.get("metric_name", "") for m in ranked_candidates.get("metrics", []) if m.get("metric_name")],
                join_paths=join_plan.get("paths", []),
                confidence_score=round(confidence_score, 4),
            )
            trace.add("stage_10_11_persist_update", {"ok": True, "failure_bucket": failure_bucket})
        except Exception as exc:
            trace.add("stage_10_11_persist_update", {"ok": False, "error": str(exc)})

        trace_path = trace.flush(interaction_id)
        return {
            "interaction_id": interaction_id,
            "db_id": db_id,
            "question": question,
            "generated_sql": generated_sql,
            "execution_success": success,
            "execution_error": "" if success else last_error,
            "retries": min(attempts, retries),
            "failure_bucket": failure_bucket,
            "confidence_score": round(confidence_score, 4),
            "validation": validation_meta,
            "execution_meta": execution_meta,
            "trace_path": trace_path,
        }


def summarize_results(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    records = list(rows)
    total = len(records)
    success_count = sum(1 for r in records if r.get("execution_success"))
    retries = [int(r.get("retries", 0)) for r in records]
    breakdown: Dict[str, int] = {}
    for r in records:
        if r.get("execution_success"):
            continue
        b = str(r.get("failure_bucket", "other"))
        breakdown[b] = breakdown.get(b, 0) + 1
    return {
        "total_examples": total,
        "execution_accuracy": (success_count / total) if total else 0.0,
        "success_count": success_count,
        "failure_count": total - success_count,
        "retry_stats": {
            "total_retries_used": sum(retries),
            "avg_retries_used": (sum(retries) / total) if total else 0.0,
            "max_retries_used_single_example": max(retries) if retries else 0,
        },
        "failure_breakdown": breakdown,
    }

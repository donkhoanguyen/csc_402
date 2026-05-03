"""Thin adapter for alternate Agent 1 graph interface shapes."""

from __future__ import annotations

from typing import Any, Dict, Sequence

from .chat_runtime import GraphRuntimeAdapter


class Agent1GraphAdapter(GraphRuntimeAdapter):
    """
    Adapter that wraps an external Agent 1 graph client if available.

    Expected optional methods on external object:
    - extract_intent_entities(question)
    - spreading_activation(db_id, seed_terms, limit=...)
    - retrieve_and_rank_candidates(db_id, entities, activation, table_limit=..., metric_limit=...)
    - find_safe_join_path(db_id, tables)
    - fetch_metric_constraints(db_id, metric_names)
    - persist_outcome(...)
    """

    def __init__(self, external_client: Any) -> None:
        super().__init__()
        self.external = external_client

    def _call_or_fallback(self, method_name: str, *args, **kwargs):
        fn = getattr(self.external, method_name, None)
        if callable(fn):
            return fn(*args, **kwargs)
        return getattr(super(), method_name)(*args, **kwargs)

    def extract_intent_entities(self, question: str) -> Dict[str, Any]:
        return self._call_or_fallback("extract_intent_entities", question)

    def spreading_activation(self, db_id: str, seed_terms: Sequence[str], limit: int = 25) -> Dict[str, Any]:
        return self._call_or_fallback("spreading_activation", db_id, seed_terms, limit=limit)

    def retrieve_and_rank_candidates(
        self,
        db_id: str,
        entities: Sequence[str],
        activation: Dict[str, Any],
        table_limit: int = 8,
        metric_limit: int = 5,
    ) -> Dict[str, Any]:
        return self._call_or_fallback(
            "retrieve_and_rank_candidates",
            db_id,
            entities,
            activation,
            table_limit=table_limit,
            metric_limit=metric_limit,
        )

    def find_safe_join_path(self, db_id: str, tables: Sequence[str]) -> Dict[str, Any]:
        return self._call_or_fallback("find_safe_join_path", db_id, tables)

    def fetch_metric_constraints(self, db_id: str, metric_names: Sequence[str]) -> Dict[str, Any]:
        return self._call_or_fallback("fetch_metric_constraints", db_id, metric_names)

    def persist_outcome(self, **kwargs) -> None:
        self._call_or_fallback("persist_outcome", **kwargs)

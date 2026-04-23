"""Minimal metrics helpers for the MVP evaluation harness."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List


def ensure_output_dir(output_dir: str) -> Path:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_jsonl(records: Iterable[Dict[str, Any]], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_json(record: Dict[str, Any], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=True, sort_keys=True)


def bucket_error(error_message: str) -> str:
    msg = (error_message or "").lower()
    if not msg:
        return "none"
    if "credential" in msg or "api key" in msg or "auth" in msg:
        return "auth"
    if "timeout" in msg:
        return "timeout"
    if "import" in msg or "module" in msg:
        return "dependency"
    if "graph" in msg:
        return "graph"
    return "other"


def compute_summary(
    run_name: str,
    examples: List[Dict[str, Any]],
    max_retries: int,
    dry_run: bool,
) -> Dict[str, Any]:
    total = len(examples)
    succeeded = sum(1 for e in examples if e.get("success"))
    failed = total - succeeded
    retry_counts = [int(e.get("retry_count", 0)) for e in examples]
    error_buckets = Counter(
        bucket_error(str(e.get("error", "")))
        for e in examples
        if not e.get("success")
    )

    return {
        "run_name": run_name,
        "dry_run": dry_run,
        "total_examples": total,
        "execution_accuracy": (succeeded / total) if total else 0.0,
        "success_count": succeeded,
        "failure_count": failed,
        "retry_stats": {
            "max_retries": max_retries,
            "total_retries_used": sum(retry_counts),
            "avg_retries_used": (sum(retry_counts) / total) if total else 0.0,
            "max_retries_used_single_example": max(retry_counts) if retry_counts else 0,
        },
        "error_buckets": dict(error_buckets),
    }

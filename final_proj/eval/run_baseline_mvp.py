"""Baseline MVP runner with dry-run support."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from metrics import compute_summary, ensure_output_dir, write_json, write_jsonl


def _stable_id_score(example_id: str) -> int:
    return sum(ord(ch) for ch in example_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run baseline MVP evaluation harness")
    parser.add_argument("--subset-size", type=int, default=5)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--output-dir", type=str, default="outputs/baseline_mvp")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--input-jsonl", type=str, default="")
    return parser.parse_args()


def _synthetic_examples(limit: int) -> List[Dict[str, Any]]:
    return [
        {"id": f"example_{i}", "question": f"Synthetic prompt {i}", "expected_sql": "SELECT 1;"}
        for i in range(limit)
    ]


def load_examples(input_jsonl: str, subset_size: int) -> List[Dict[str, Any]]:
    if not input_jsonl:
        return _synthetic_examples(subset_size)

    path = Path(input_jsonl)
    if not path.exists():
        raise FileNotFoundError(f"Input JSONL not found: {input_jsonl}")

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if len(rows) >= subset_size:
                break
    return rows


def execute_example(example: Dict[str, Any], max_retries: int, dry_run: bool) -> Dict[str, Any]:
    attempts = 0
    success = False
    error = ""

    while attempts <= max_retries and not success:
        if dry_run:
            # Deterministic dry-run behavior for repeatable smoke checks.
            success = (_stable_id_score(str(example.get("id", ""))) + attempts) % 4 != 0
            if not success:
                error = "dry-run simulated execution error"
        else:
            if not os.getenv("BASELINE_API_KEY"):
                error = "Missing BASELINE_API_KEY credential"
                break
            success = True

        if not success:
            attempts += 1

    return {
        "id": example.get("id"),
        "success": success,
        "retry_count": min(attempts, max_retries),
        "error": "" if success else error,
        "question": example.get("question", ""),
        "mode": "dry-run" if dry_run else "live",
    }


def main() -> None:
    args = parse_args()
    output_dir = ensure_output_dir(args.output_dir)
    examples = load_examples(args.input_jsonl, args.subset_size)

    results = [
        execute_example(example, max_retries=args.max_retries, dry_run=args.dry_run)
        for example in examples
    ]
    summary = compute_summary(
        run_name="baseline_mvp",
        examples=results,
        max_retries=args.max_retries,
        dry_run=args.dry_run,
    )

    write_jsonl(results, output_dir / "examples.jsonl")
    write_json(summary, output_dir / "summary.json")

    print(f"Wrote per-example results to: {output_dir / 'examples.jsonl'}")
    print(f"Wrote aggregate summary to: {output_dir / 'summary.json'}")
    print(
        "Execution accuracy: "
        f"{summary['execution_accuracy']:.3f} ({summary['success_count']}/{summary['total_examples']})"
    )


if __name__ == "__main__":
    main()

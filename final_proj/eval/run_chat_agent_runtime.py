"""Benchmark runner for ontology-runtime chat agent on Spider2 hard40."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from metrics import ensure_output_dir, write_json, write_jsonl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.chat_runtime import ChatRuntime, RuntimeConfig, summarize_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ontology chat runtime benchmark")
    parser.add_argument(
        "--input-jsonl",
        type=str,
        default=str(ROOT / "tasks" / "spider2_snow_hard40_with_answer_keys.jsonl"),
    )
    parser.add_argument("--subset-size", type=int, default=40)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--output-dir", type=str, default="outputs/chat_agent_runtime_hard40")
    parser.add_argument("--trace-root", type=str, default="")
    parser.add_argument("--live-execution", action="store_true")
    return parser.parse_args()


def load_examples(path: str, subset_size: int) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input JSONL not found: {path}")
    rows = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows[: max(0, int(subset_size))]


def main() -> None:
    args = parse_args()
    output_dir = ensure_output_dir(args.output_dir)
    examples = load_examples(args.input_jsonl, args.subset_size)

    runtime = ChatRuntime(
        RuntimeConfig(
            max_retries=max(0, int(args.max_retries)),
            trace_root=(str(Path(args.trace_root)) if args.trace_root.strip() else str(output_dir / "traces")),
            use_live_execution=bool(args.live_execution),
        )
    )

    out_rows: List[Dict[str, Any]] = []
    total = len(examples)
    for idx, ex in enumerate(examples, start=1):
        question = str(ex.get("question", "")).strip()
        db_id = str(ex.get("db_id", "")).strip()
        result = runtime.run_interaction(db_id=db_id, question=question)
        row = {
            "id": ex.get("id"),
            "db_id": db_id,
            "question": question,
            "generated_sql": result.get("generated_sql", ""),
            "execution_success": bool(result.get("execution_success")),
            "execution_failure": not bool(result.get("execution_success")),
            "retries": int(result.get("retries", 0)),
            "failure_bucket": result.get("failure_bucket", "none"),
            "answer_key_reference_presence": bool(str(ex.get("answer_key_sql", "")).strip()),
            "trace_path": result.get("trace_path", ""),
            "confidence_score": result.get("confidence_score", 0.0),
            "execution_error": result.get("execution_error", ""),
        }
        out_rows.append(row)
        status = "ok" if row["execution_success"] else "fail"
        print(
            f"[{idx}/{total}] {status} db={db_id or '-'} id={row.get('id','-')} "
            f"retries={row['retries']} failure_bucket={row['failure_bucket']}",
            flush=True,
        )

    summary = summarize_results(out_rows)
    summary["run_name"] = "chat_agent_runtime_hard40"
    summary["subset_size"] = len(out_rows)
    summary["max_retries"] = int(args.max_retries)
    summary["input_jsonl"] = str(Path(args.input_jsonl))

    write_jsonl(out_rows, output_dir / "examples.jsonl")
    write_json(summary, output_dir / "summary.json")

    print(f"Wrote per-example outputs to: {output_dir / 'examples.jsonl'}")
    print(f"Wrote aggregate summary to: {output_dir / 'summary.json'}")
    print(
        "Execution accuracy: "
        f"{summary['execution_accuracy']:.3f} "
        f"({summary['success_count']}/{summary['total_examples']})"
    )


if __name__ == "__main__":
    main()

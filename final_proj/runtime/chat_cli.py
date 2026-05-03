from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from .chat_runtime import ChatRuntime, RuntimeConfig
except ImportError:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from chat_runtime import ChatRuntime, RuntimeConfig  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ontology runtime chat CLI")
    parser.add_argument("--db-id", required=True, help="Target db_id in ontology graph")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--trace-root", type=str, default="eval/outputs/chat_runtime/traces")
    parser.add_argument("--live-execution", action="store_true")
    parser.add_argument(
        "--question",
        type=str,
        default="",
        help="Optional single-turn mode; if omitted, starts interactive chat loop.",
    )
    parser.add_argument(
        "--save-transcript",
        type=str,
        default="",
        help="Optional JSON transcript output path.",
    )
    return parser.parse_args()


def _single_turn(runtime: ChatRuntime, db_id: str, question: str) -> dict:
    result = runtime.run_interaction(db_id=db_id, question=question)
    print(f"\nQuestion: {question}")
    print(f"SQL: {result.get('generated_sql', '')}")
    print(
        "Result: "
        f"success={result.get('execution_success')} "
        f"retries={result.get('retries')} "
        f"confidence={result.get('confidence_score')} "
        f"failure_bucket={result.get('failure_bucket')}"
    )
    print(f"Trace: {result.get('trace_path')}")
    if result.get("execution_error"):
        print(f"Error: {result.get('execution_error')}")
    return result


def main() -> None:
    args = parse_args()
    runtime = ChatRuntime(
        RuntimeConfig(
            max_retries=max(0, int(args.max_retries)),
            trace_root=args.trace_root,
            use_live_execution=bool(args.live_execution),
        )
    )

    transcript = []
    if args.question.strip():
        transcript.append(_single_turn(runtime, args.db_id.strip(), args.question.strip()))
    else:
        print("Interactive mode started. Type 'exit' to quit.\n")
        while True:
            q = input("> ").strip()
            if q.lower() in {"exit", "quit"}:
                break
            if not q:
                continue
            transcript.append(_single_turn(runtime, args.db_id.strip(), q))

    if args.save_transcript.strip():
        out = Path(args.save_transcript)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", encoding="utf-8") as f:
            json.dump({"db_id": args.db_id.strip(), "turns": transcript}, f, indent=2)
        print(f"\nSaved transcript to: {out}")


if __name__ == "__main__":
    main()

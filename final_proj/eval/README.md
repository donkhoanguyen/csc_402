# Evaluation Harness MVP

This directory contains a minimal experiment harness with:

- baseline runner (`run_baseline_mvp.py`)
- graph-mode runner (`run_graph_mvp.py`)
- shared metrics/output helpers (`metrics.py`)

Both runners support dry-run mode so they can execute without model credentials.

## Quick start (dry-run)

From `final_proj/`:

```bash
python eval/run_baseline_mvp.py --subset-size 5 --max-retries 2 --dry-run --output-dir eval/outputs/baseline
python eval/run_graph_mvp.py --subset-size 5 --max-retries 2 --dry-run --output-dir eval/outputs/graph
```

## Optional input file

You can provide a JSONL input file with one example per line:

```bash
python eval/run_baseline_mvp.py --input-jsonl path/to/examples.jsonl --subset-size 10 --dry-run
python eval/run_graph_mvp.py --input-jsonl path/to/examples.jsonl --subset-size 10 --dry-run
```

If `--input-jsonl` is omitted, each script generates a synthetic subset.

## CLI flags

Both scripts support:

- `--subset-size` number of examples to process
- `--max-retries` max retry attempts per example
- `--output-dir` output directory for metrics files
- `--dry-run` force dry-run mode (no credentials needed)
- `--input-jsonl` optional JSONL dataset path

## Output files

Each run writes:

- `examples.jsonl`: per-example results
  - includes success/failure, retries, error text, and mode
- `summary.json`: aggregate metrics
  - execution accuracy
  - retry statistics
  - error buckets/categories

Example output layout:

```text
eval/outputs/
  baseline/
    examples.jsonl
    summary.json
  graph/
    examples.jsonl
    summary.json
```

## Graph runner behavior

`run_graph_mvp.py` attempts to import `shared.graph_tools` and verify expected functions are present.

- If available, it runs in graph mode (or dry-run if `--dry-run` is set).
- If unavailable/missing functions, it logs a clear info message and continues with dry-run fallback so the run still completes end-to-end.

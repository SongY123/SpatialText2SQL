#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

FLOODSQL_UPDATED_BENCHMARK_PATH="${FLOODSQL_UPDATED_BENCHMARK_PATH:-benchmark/bechmark_updated.jsonl}"

python -m src.datasets.benchmark_formatter \
  --floodsql-updated-benchmark "${FLOODSQL_UPDATED_BENCHMARK_PATH}" \
  "$@"

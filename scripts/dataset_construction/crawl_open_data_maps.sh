#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# Default behavior is append/skip; pass --override to force re-download.
ARGS=()
if [[ $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
  ARGS+=(--sample "$1")
  shift
fi

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.dataset_construction.crawl.cli ${ARGS[@]+"${ARGS[@]}"} "$@"

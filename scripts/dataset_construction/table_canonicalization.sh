#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <metadata.json> [additional args...]" >&2
  exit 1
fi

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.dataset_construction.canonicalization.cli "$@"

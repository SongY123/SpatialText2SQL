#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <metadata.json> [additional args...]" >&2
  exit 1
fi

if [[ $# -ge 1 && ( "${1}" == "--help" || "${1}" == "-h" ) ]]; then
  cat <<EOF
Usage: $(basename "$0") <metadata.json> [extra python args...]

Examples:
  $(basename "$0") data/raw/metadata.json
  $(basename "$0") data/raw/metadata.json --cities nyc,sf

City selection follows crawl_open_data_maps.sh:
  --cities all
  --cities nyc,lacity,chicago,seattle,sf,boston,phoenix
EOF
  exit 0
fi

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.dataset_construction.canonicalization.cli "$@"

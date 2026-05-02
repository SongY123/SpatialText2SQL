#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

DEFAULT_INPUT="${REPO_ROOT}/data/raw/metadata_canonicalized.json"
DEFAULT_OUTPUT="${REPO_ROOT}/data/processed/synthesized_spatial_databases.jsonl"

if [[ $# -ge 1 && ( "${1}" == "--help" || "${1}" == "-h" ) ]]; then
  cat <<EOF
Usage: $(basename "$0") [input_json_or_jsonl] [output_jsonl] [extra python args...]

Default input : ${DEFAULT_INPUT}
Default output: ${DEFAULT_OUTPUT}

Default environment-backed parameters:
  TARGET_AVG_DEGREE=4
  EXPLORATION_PROB=0.1
  SIZE_MEAN=8
  SIZE_STD=2
  MIN_TABLES=2
  MAX_TABLES=12
  EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
  RANDOM_SEED=42
  MAX_SAMPLING_STEPS=100
  LOG_LEVEL=INFO

Examples:
  $(basename "$0")
  $(basename "$0") data/raw/metadata_canonicalized.json data/processed/synthesized_spatial_databases.jsonl
  EXPLORATION_PROB=0.2 $(basename "$0") --cities nyc,sf --embedding-model sentence-transformers/all-MiniLM-L6-v2
EOF
  exit 0
fi

INPUT_PATH="${DEFAULT_INPUT}"
OUTPUT_PATH="${DEFAULT_OUTPUT}"

if [[ $# -ge 1 && "${1}" != -* ]]; then
  INPUT_PATH="${1}"
  shift
fi
if [[ $# -ge 1 && "${1}" != -* ]]; then
  OUTPUT_PATH="${1}"
  shift
fi

TARGET_AVG_DEGREE="${TARGET_AVG_DEGREE:-4}"
EXPLORATION_PROB="${EXPLORATION_PROB:-0.1}"
SIZE_MEAN="${SIZE_MEAN:-8}"
SIZE_STD="${SIZE_STD:-2}"
MIN_TABLES="${MIN_TABLES:-2}"
MAX_TABLES="${MAX_TABLES:-12}"
EMBEDDING_MODEL="${EMBEDDING_MODEL:-sentence-transformers/all-MiniLM-L6-v2}"
RANDOM_SEED="${RANDOM_SEED:-42}"
MAX_SAMPLING_STEPS="${MAX_SAMPLING_STEPS:-100}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.synthesis.database.cli \
    --input "${INPUT_PATH}" \
    --output "${OUTPUT_PATH}" \
    --target-avg-degree "${TARGET_AVG_DEGREE}" \
    --exploration-prob "${EXPLORATION_PROB}" \
    --size-mean "${SIZE_MEAN}" \
    --size-std "${SIZE_STD}" \
    --min-tables "${MIN_TABLES}" \
    --max-tables "${MAX_TABLES}" \
    --embedding-model "${EMBEDDING_MODEL}" \
    --random-seed "${RANDOM_SEED}" \
    --max-sampling-steps "${MAX_SAMPLING_STEPS}" \
    --log-level "${LOG_LEVEL}" \
    "$@"

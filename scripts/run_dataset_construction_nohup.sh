#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

NUM_SQL_PER_DATABASE="${NUM_SQL_PER_DATABASE:-140}"
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/logs/dataset_construction}"
WORKER_MODE=0
FOREGROUND_MODE=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --num-sql-per-database N   Override SQL sample count per database. Default: ${NUM_SQL_PER_DATABASE}
  --foreground               Run in the current shell instead of launching with nohup
  --worker                   Internal flag used by the nohup launcher
  -h, --help                 Show this help message

Environment overrides:
  NUM_SQL_PER_DATABASE       Default SQL sample count per database
  LOG_DIR                    Directory for nohup log files

Examples:
  bash scripts/run_dataset_construction_nohup.sh
  bash scripts/run_dataset_construction_nohup.sh --num-sql-per-database 140
  bash scripts/run_dataset_construction_nohup.sh --foreground
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

run_pipeline() {
  cd "${REPO_ROOT}"

  log "Starting dataset construction pipeline"
  log "Step 1/3: synthesize_sql_queries.sh --num-sql-per-database ${NUM_SQL_PER_DATABASE}"
  bash scripts/dataset_construction/synthesize_sql_queries.sh --num-sql-per-database "${NUM_SQL_PER_DATABASE}"

  log "Step 2/3: synthesize_questions.sh"
  bash scripts/dataset_construction/synthesize_questions.sh

  log "Step 3/3: quality_control.sh"
  bash scripts/dataset_construction/quality_control.sh

  log "Dataset construction pipeline finished successfully"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --num-sql-per-database)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --num-sql-per-database" >&2
        exit 1
      fi
      NUM_SQL_PER_DATABASE="$2"
      shift 2
      ;;
    --foreground)
      FOREGROUND_MODE=1
      shift
      ;;
    --worker)
      WORKER_MODE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${WORKER_MODE}" -eq 1 || "${FOREGROUND_MODE}" -eq 1 ]]; then
  run_pipeline
  exit 0
fi

mkdir -p "${LOG_DIR}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="${LOG_DIR}/dataset_construction_${TIMESTAMP}.log"

nohup bash "${SCRIPT_PATH}" \
  --worker \
  --num-sql-per-database "${NUM_SQL_PER_DATABASE}" \
  >"${LOG_FILE}" 2>&1 &

PID=$!

echo "Started dataset construction pipeline in background."
echo "PID: ${PID}"
echo "Log: ${LOG_FILE}"

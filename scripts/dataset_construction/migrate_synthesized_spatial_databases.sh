#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

DEFAULT_CONFIG="${REPO_ROOT}/config/migrate.yaml"

if [[ $# -ge 1 && ( "${1}" == "--help" || "${1}" == "-h" ) ]]; then
  cat <<EOF
Usage: $(basename "$0") [input_jsonl] [extra python args...]

Default config: ${DEFAULT_CONFIG}

Config-backed defaults live in:
  ${DEFAULT_CONFIG}

Optional environment overrides:
  MIGRATE_CONFIG
  PGHOST
  PGPORT
  PGUSER
  PGPASSWORD
  PGCATALOG
  PGBOOTSTRAP_DB
  PGMAINTENANCE_DB (legacy alias for PGBOOTSTRAP_DB)
  INSERT_BATCH_SIZE
  LOG_LEVEL

Examples:
  $(basename "$0")
  $(basename "$0") data/processed/synthesized_spatial_databases.jsonl
  $(basename "$0") --cities nyc,sf
EOF
  exit 0
fi

CONFIG_PATH="${MIGRATE_CONFIG:-${DEFAULT_CONFIG}}"
INPUT_PATH=""
if [[ $# -ge 1 && "${1}" != -* ]]; then
  INPUT_PATH="${1}"
  shift
fi

EXTRA_ARGS=(--config "${CONFIG_PATH}")

if [[ -n "${INPUT_PATH}" ]]; then
  EXTRA_ARGS+=(--input "${INPUT_PATH}")
fi
if [[ -n "${PGHOST:-}" ]]; then
  EXTRA_ARGS+=(--host "${PGHOST}")
fi
if [[ -n "${PGPORT:-}" ]]; then
  EXTRA_ARGS+=(--port "${PGPORT}")
fi
if [[ -n "${PGUSER:-}" ]]; then
  EXTRA_ARGS+=(--user "${PGUSER}")
fi
if [[ -n "${PGPASSWORD:-}" ]]; then
  EXTRA_ARGS+=(--password "${PGPASSWORD}")
fi
if [[ -n "${PGCATALOG:-}" ]]; then
  EXTRA_ARGS+=(--catalog "${PGCATALOG}")
fi
if [[ -n "${PGBOOTSTRAP_DB:-}" ]]; then
  EXTRA_ARGS+=(--bootstrap-db "${PGBOOTSTRAP_DB}")
fi
if [[ -n "${PGMAINTENANCE_DB:-}" ]]; then
  EXTRA_ARGS+=(--maintenance-db "${PGMAINTENANCE_DB}")
fi
if [[ -n "${INSERT_BATCH_SIZE:-}" ]]; then
  EXTRA_ARGS+=(--insert-batch-size "${INSERT_BATCH_SIZE}")
fi
if [[ -n "${LOG_LEVEL:-}" ]]; then
  EXTRA_ARGS+=(--log-level "${LOG_LEVEL}")
fi

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.synthesis.database.migration.cli \
    "${EXTRA_ARGS[@]}" \
    "$@"

#!/usr/bin/env bash
set -euo pipefail

export NO_PROXY=100.64.0.0/10,100.126.198.114,localhost,127.0.0.1
export no_proxy=100.64.0.0/10,100.126.198.114,localhost,127.0.0.1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

DEFAULT_CONFIG="${REPO_ROOT}/config/question_synthesis.yaml"
CLI_ARGS=("$@")

CONFIG_OVERRIDE=""
OUTPUT_OVERRIDE=""

for ((i=1; i<=$#; i++)); do
  arg="${!i}"
  if [[ "${arg}" == "--config" && $((i + 1)) -le $# ]]; then
    next_index=$((i + 1))
    CONFIG_OVERRIDE="${!next_index}"
  elif [[ "${arg}" == "--output" && $((i + 1)) -le $# ]]; then
    next_index=$((i + 1))
    OUTPUT_OVERRIDE="${!next_index}"
  fi
done

if [[ $# -ge 1 && ( "${1}" == "--help" || "${1}" == "-h" ) ]]; then
  cat <<EOF
Usage: $(basename "$0") [extra python args...]

Default config: ${DEFAULT_CONFIG}

Optional environment overrides:
  QUESTION_GENERATION_CONFIG

Examples:
  $(basename "$0")
  $(basename "$0") --style conversational
  $(basename "$0") --sql-input data/processed/synthesized_sql_queries.jsonl --output data/processed/synthesized_questions.jsonl
EOF
  exit 0
fi

CONFIG_PATH="${CONFIG_OVERRIDE:-${QUESTION_GENERATION_CONFIG:-${DEFAULT_CONFIG}}}"

cleanup_lock_files() {
  mapfile -t LOCK_TARGETS < <(
    PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
      CONFIG_PATH="${CONFIG_PATH}" \
      OUTPUT_OVERRIDE="${OUTPUT_OVERRIDE}" \
      python - <<'PY'
from src.synthesis.question.config import load_question_generation_config, override_question_generation_config
import os

config = load_question_generation_config(os.environ["CONFIG_PATH"])
output_override = os.environ.get("OUTPUT_OVERRIDE", "").strip()
if output_override:
    config = override_question_generation_config(
        config,
        generation={"output_path": output_override},
    )
print(config.generation.output_path)
PY
  )

  for target in "${LOCK_TARGETS[@]}"; do
    [[ -n "${target}" ]] || continue
    rm -f "${target}.lock"
  done
}

trap cleanup_lock_files EXIT

PYTHONPATH="${REPO_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
  python -m src.synthesis.question.cli \
    --config "${CONFIG_PATH}" \
    "${CLI_ARGS[@]}"

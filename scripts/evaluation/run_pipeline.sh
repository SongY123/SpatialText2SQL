#!/bin/bash
# Configured shell entrypoint for the Spatial Text2SQL pipeline.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Edit the values in this block for the common evaluation workflow.
# Set RUN_SAMPLE=1 to run sample mode via the same script entry.
CONFIG_DIR="${CONFIG_DIR:-config}"
RUN_UTILS="${RUN_UTILS:-0}"
RUN_BUILD_RAG="${RUN_BUILD_RAG:-0}"
RUN_INFERENCE="${RUN_INFERENCE:-1}"
RUN_EVALUATE="${RUN_EVALUATE:-1}"
RUN_BENCHMARK="${RUN_BENCHMARK:-0}"
RUN_SAMPLE="${RUN_SAMPLE:-0}"

DATASETS=(
  "${DATASETS_0:-spatialqueryqa}"
  "${DATASETS_1:-spatialsql}"
  "${DATASETS_2:-floodsql}"
)

MODELS=(
  # "qwen2.5-coder-7b"
)

BACKEND="${BACKEND:-}"
CONFIGS=(
  "${CONFIGS_0:-base}"
)

ENABLE_PREDICTION_POSTPROCESS="${ENABLE_PREDICTION_POSTPROCESS:-1}"
ALLOW_CLI_OVERRIDES="${ALLOW_CLI_OVERRIDES:-1}"
PYTHON_BIN="${PYTHON_BIN:-python}"
CONDA_ENV="${CONDA_ENV:-}"
SAMPLE_DATASET="${SAMPLE_DATASET:-}"
SAMPLE_GROUP_VALUE="${SAMPLE_GROUP_VALUE:-}"
SAMPLE_ID="${SAMPLE_ID:-}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-1}"
SAMPLE_MODEL="${SAMPLE_MODEL:-}"
SAMPLE_BACKEND="${SAMPLE_BACKEND:-}"
SAMPLE_CONFIG="${SAMPLE_CONFIG:-}"
SAMPLE_SHOW_PROMPT="${SAMPLE_SHOW_PROMPT:-1}"
SAMPLE_NO_EVAL="${SAMPLE_NO_EVAL:-0}"
SAMPLE_PREVIEW_CHARS="${SAMPLE_PREVIEW_CHARS:-12000}"

append_many() {
  local flag="$1"
  shift
  local values=()
  local value
  for value in "$@"; do
    if [[ -n "${value// /}" ]]; then
      values+=("$value")
    fi
  done
  if [[ "${#values[@]}" -gt 0 ]]; then
    CMD+=("$flag")
    CMD+=("${values[@]}")
  fi
}

if [[ "$RUN_SAMPLE" == "1" ]]; then
  EFFECTIVE_SAMPLE_DATASET="$SAMPLE_DATASET"
  if [[ -z "$EFFECTIVE_SAMPLE_DATASET" && "${#DATASETS[@]}" -gt 0 ]]; then
    EFFECTIVE_SAMPLE_DATASET="${DATASETS[0]}"
  fi

  EFFECTIVE_SAMPLE_MODEL="$SAMPLE_MODEL"
  if [[ -z "$EFFECTIVE_SAMPLE_MODEL" && "${#MODELS[@]}" -gt 0 ]]; then
    EFFECTIVE_SAMPLE_MODEL="${MODELS[0]}"
  fi

  EFFECTIVE_SAMPLE_BACKEND="$SAMPLE_BACKEND"
  if [[ -z "$EFFECTIVE_SAMPLE_BACKEND" ]]; then
    EFFECTIVE_SAMPLE_BACKEND="$BACKEND"
  fi
  if [[ -z "$EFFECTIVE_SAMPLE_BACKEND" ]]; then
    EFFECTIVE_SAMPLE_BACKEND="vllm"
  fi

  EFFECTIVE_SAMPLE_CONFIG="$SAMPLE_CONFIG"
  if [[ -z "$EFFECTIVE_SAMPLE_CONFIG" && "${#CONFIGS[@]}" -gt 0 ]]; then
    EFFECTIVE_SAMPLE_CONFIG="${CONFIGS[0]}"
  fi
  if [[ -z "$EFFECTIVE_SAMPLE_CONFIG" ]]; then
    EFFECTIVE_SAMPLE_CONFIG="base"
  fi

  if [[ -z "$EFFECTIVE_SAMPLE_DATASET" ]]; then
    echo "RUN_SAMPLE=1 requires SAMPLE_DATASET or a non-empty DATASETS array." >&2
    exit 1
  fi
  if [[ -z "$EFFECTIVE_SAMPLE_MODEL" ]]; then
    echo "RUN_SAMPLE=1 requires SAMPLE_MODEL or a non-empty MODELS array." >&2
    exit 1
  fi

  CMD=(
    "$PYTHON_BIN"
    "scripts/evaluation/run_single_sample.py"
    "--dataset" "$EFFECTIVE_SAMPLE_DATASET"
    "--model" "$EFFECTIVE_SAMPLE_MODEL"
    "--backend" "$EFFECTIVE_SAMPLE_BACKEND"
    "--config" "$EFFECTIVE_SAMPLE_CONFIG"
    "--preview-chars" "$SAMPLE_PREVIEW_CHARS"
  )

  if [[ -n "$SAMPLE_GROUP_VALUE" ]]; then
    CMD+=("--group-value" "$SAMPLE_GROUP_VALUE")
  fi
  if [[ -n "$SAMPLE_ID" ]]; then
    CMD+=("--sample-id" "$SAMPLE_ID")
  else
    CMD+=("--sample-limit" "$SAMPLE_LIMIT")
  fi
  if [[ "$SAMPLE_SHOW_PROMPT" == "1" ]]; then
    CMD+=("--show-prompt")
  fi
  if [[ "$SAMPLE_NO_EVAL" == "1" ]]; then
    CMD+=("--no-eval")
  fi
else
  CMD=("$PYTHON_BIN" "-m" "src.pipeline.main" "--config-dir" "$CONFIG_DIR")

  if [[ "$RUN_UTILS" == "1" ]]; then
    CMD+=("--utils")
  fi
  if [[ "$RUN_BUILD_RAG" == "1" ]]; then
    CMD+=("--build-rag")
  fi
  if [[ "$RUN_INFERENCE" == "1" ]]; then
    CMD+=("--inference")
  fi
  if [[ "$RUN_EVALUATE" == "1" ]]; then
    CMD+=("--evaluate")
  fi
  if [[ "$RUN_BENCHMARK" == "1" ]]; then
    CMD+=("--benchmark")
  fi

  append_many "--dataset" "${DATASETS[@]}"
  append_many "--models" "${MODELS[@]}"
  append_many "--configs" "${CONFIGS[@]}"

  if [[ -n "$BACKEND" ]]; then
    CMD+=("--backend" "$BACKEND")
  fi
  if [[ "$ENABLE_PREDICTION_POSTPROCESS" == "1" ]]; then
    CMD+=("--enable-prediction-postprocess")
  fi
fi

if [[ "$ALLOW_CLI_OVERRIDES" == "1" && "$#" -gt 0 ]]; then
  CMD+=("$@")
fi

echo "Running pipeline with command:"
printf '  %q' "${CMD[@]}"
printf '\n'

if [[ -n "$CONDA_ENV" ]]; then
  conda run -n "$CONDA_ENV" "${CMD[@]}"
else
  "${CMD[@]}"
fi

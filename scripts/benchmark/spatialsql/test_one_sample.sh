#!/bin/bash
# Single-sample test helper for scripts/evaluation/run_single_sample.py.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

DATASET="${DATASET:-spatialsql_pg}"
GROUP_VALUE="${GROUP_VALUE:-dataset1_ada}"
SAMPLE_ID="${SAMPLE_ID:-1}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-1}"
MODEL="${MODEL:-qwen2.5-coder-7b}"
BACKEND="${BACKEND:-vllm}"
CONFIG_TYPE="${CONFIG_TYPE:-base}"
PREVIEW_CHARS="${PREVIEW_CHARS:-12000}"
SHOW_PROMPT="${SHOW_PROMPT:-1}"
NO_EVAL="${NO_EVAL:-0}"
CONDA_ENV="${CONDA_ENV:-}"

CMD=(python scripts/evaluation/run_single_sample.py
  --dataset "$DATASET"
  --group-value "$GROUP_VALUE"
  --model "$MODEL"
  --backend "$BACKEND"
  --config "$CONFIG_TYPE"
  --preview-chars "$PREVIEW_CHARS")

if [[ -n "$SAMPLE_ID" ]]; then
  CMD+=(--sample-id "$SAMPLE_ID")
else
  CMD+=(--sample-limit "$SAMPLE_LIMIT")
fi

if [[ "$SHOW_PROMPT" == "1" ]]; then
  CMD+=(--show-prompt)
fi

if [[ "$NO_EVAL" == "1" ]]; then
  CMD+=(--no-eval)
fi

echo "Running single-sample test with:"
echo "  dataset       : $DATASET"
echo "  group_value   : ${GROUP_VALUE:-<none>}"
echo "  sample_id     : ${SAMPLE_ID:-<none>}"
echo "  sample_limit  : $SAMPLE_LIMIT"
echo "  model         : $MODEL"
echo "  backend       : $BACKEND"
echo "  config_type   : $CONFIG_TYPE"
echo "  show_prompt   : $SHOW_PROMPT"
echo "  no_eval       : $NO_EVAL"

if [[ -n "$CONDA_ENV" ]]; then
  conda run -n "$CONDA_ENV" "${CMD[@]}"
else
  "${CMD[@]}"
fi

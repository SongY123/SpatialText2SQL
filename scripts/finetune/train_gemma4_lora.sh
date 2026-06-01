#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CONFIG_PATH="${FINETUNE_CONFIG:-${REPO_ROOT}/config/finetune_gemma4_lora.yaml}"
MODEL_NAME_OR_PATH="${MODEL_NAME_OR_PATH:-/data/llm/google/gemma-4-31B-it}"
OUTPUT_DIR="${OUTPUT_DIR:-/data/llm/finetuned/google/gemma-4-31B-it-lora}"
GPUS="${GPUS:-0,1,2,3,4,5,6,7}"
NUM_MACHINES="${NUM_MACHINES:-1}"
MACHINE_RANK="${MACHINE_RANK:-0}"
MAIN_PROCESS_IP="${MAIN_PROCESS_IP:-}"
MAIN_PROCESS_PORT="${MAIN_PROCESS_PORT:-29500}"

args=(
  --model-name-or-path "${MODEL_NAME_OR_PATH}"
  --output-dir "${OUTPUT_DIR}"
  --nvidia-gpu-indices "${GPUS}"
  --num-machines "${NUM_MACHINES}"
  --machine-rank "${MACHINE_RANK}"
  --main-process-port "${MAIN_PROCESS_PORT}"
)

if [[ -n "${TOKENIZER_NAME_OR_PATH:-}" ]]; then
  args+=(--tokenizer-name-or-path "${TOKENIZER_NAME_OR_PATH}")
fi
if [[ -n "${PROCESSOR_NAME_OR_PATH:-}" ]]; then
  args+=(--processor-name-or-path "${PROCESSOR_NAME_OR_PATH}")
fi
if [[ -n "${MAIN_PROCESS_IP}" ]]; then
  args+=(--main-process-ip "${MAIN_PROCESS_IP}")
fi

FINETUNE_CONFIG="${CONFIG_PATH}" exec bash "${SCRIPT_DIR}/train.sh" "${args[@]}" "$@"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_CONFIG="${SCRIPT_DIR}/qwen25_coder_7b_nl2sql_full_sft.yaml"
if [[ -n "${LLAMAFACTORY_CONFIG:-}" ]]; then
  CONFIG_PATH="${LLAMAFACTORY_CONFIG}"
else
  CONFIG_PATH="${1:-${DEFAULT_CONFIG}}"
fi
if [[ -z "${LLAMAFACTORY_CONFIG:-}" && $# -gt 0 ]]; then
  shift
fi

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-3,6,7}"
export TOKENIZERS_PARALLELISM=false
export NCCL_DEBUG=WARN
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

llamafactory-cli train "${CONFIG_PATH}" "$@"

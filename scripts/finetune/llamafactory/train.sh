#!/usr/bin/env bash
set -euo pipefail

export CUDA_VISIBLE_DEVICES=3,6,7
export TOKENIZERS_PARALLELISM=false
export NCCL_DEBUG=WARN
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

llamafactory-cli train examples/train_full/qwen25_coder_7b_nl2sql_full_sft.yaml
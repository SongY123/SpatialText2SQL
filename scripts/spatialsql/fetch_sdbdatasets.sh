#!/usr/bin/env bash
# 从官方 SpatialSQL 仓库拉取 sdbdatasets/（迁移与预处理需要，体积较大，勿打进迁移小包）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -d sdbdatasets/dataset1 && -d sdbdatasets/dataset2 ]]; then
    echo "sdbdatasets 已存在，跳过: $ROOT/sdbdatasets"
    exit 0
fi

REPO_URL="${SPATIALSQL_GIT_URL:-https://github.com/beta512/SpatialSQL.git}"
TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo "克隆 SpatialSQL → sdbdatasets/ ..."
cd "$TMP"
if git clone --depth 1 --filter=blob:none --sparse "$REPO_URL" repo 2>/dev/null; then
    cd repo
    git sparse-checkout set sdbdatasets
    SRC="$TMP/repo/sdbdatasets"
else
    echo "（使用浅克隆全仓库，仅保留 sdbdatasets）"
    git clone --depth 1 "$REPO_URL" repo
    SRC="$TMP/repo/sdbdatasets"
fi
cd "$ROOT"
mv "$SRC" "$ROOT/"
trap - EXIT
cleanup

echo "完成: $ROOT/sdbdatasets"

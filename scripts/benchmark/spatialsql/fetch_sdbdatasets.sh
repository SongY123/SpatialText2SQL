#!/usr/bin/env bash
# Fetch sdbdatasets/ from the official SpatialSQL repository.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -d sdbdatasets/dataset1 && -d sdbdatasets/dataset2 ]]; then
    echo "sdbdatasets already exists, skipping: $ROOT/sdbdatasets"
    exit 0
fi

REPO_URL="${SPATIALSQL_GIT_URL:-https://github.com/beta512/SpatialSQL.git}"
TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo "Cloning SpatialSQL and extracting sdbdatasets/ ..."
cd "$TMP"
if git clone --depth 1 --filter=blob:none --sparse "$REPO_URL" repo 2>/dev/null; then
    cd repo
    git sparse-checkout set sdbdatasets
    SRC="$TMP/repo/sdbdatasets"
else
    echo "(falling back to a shallow full-repository clone and keeping only sdbdatasets)"
    git clone --depth 1 "$REPO_URL" repo
    SRC="$TMP/repo/sdbdatasets"
fi
cd "$ROOT"
mv "$SRC" "$ROOT/"
trap - EXIT
cleanup

echo "Done: $ROOT/sdbdatasets"

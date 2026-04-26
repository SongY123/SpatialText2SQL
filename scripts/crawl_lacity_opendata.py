#!/usr/bin/env python3
"""Wrapper: download map-related datasets from Los Angeles open data (data.lacity.org)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MAIN = _HERE / "download_socrata_map_datasets.py"
_DEFAULT_OUT = _HERE / "artifacts" / "socrata_maps" / "lacity"


def main() -> None:
    argv = [
        sys.executable,
        str(_MAIN),
        "--portal",
        "lacity",
        "--out-dir",
        str(_DEFAULT_OUT),
        "--max-catalog",
        "500",
        "--max-datasets",
        "200",
        *sys.argv[1:],
    ]
    raise SystemExit(subprocess.call(argv))


if __name__ == "__main__":
    main()

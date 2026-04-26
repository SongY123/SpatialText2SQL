#!/usr/bin/env python3
"""Wrapper: download map-related datasets from San Francisco open data."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MAIN = _HERE / "download_sfgov_maps_geojson.py"
_DEFAULT_OUT = _HERE / "artifacts" / "socrata_maps" / "sf"


def main() -> None:
    argv = [sys.executable, str(_MAIN), "--out-dir", str(_DEFAULT_OUT), *sys.argv[1:]]
    raise SystemExit(subprocess.call(argv))


if __name__ == "__main__":
    main()

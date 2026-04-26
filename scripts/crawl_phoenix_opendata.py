#!/usr/bin/env python3
"""Wrapper: download map-related datasets from Phoenix open data."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MAIN = _HERE / "download_open_data_4cities.py"


def main() -> None:
    argv = [sys.executable, str(_MAIN), "--city", "phoenix", *sys.argv[1:]]
    raise SystemExit(subprocess.call(argv))


if __name__ == "__main__":
    main()

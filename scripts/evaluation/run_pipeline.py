#!/usr/bin/env python3
"""评测流水线脚本入口。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.chdir(REPO_ROOT)

from src.pipeline.main import main


if __name__ == "__main__":
    main()

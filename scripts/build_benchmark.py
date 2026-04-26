from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spatial_benchmark.clustering import run_clustering_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Legacy entrypoint for the clustering stage of the spatial benchmark workflow.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path(r"D:\nyc-data\data\nyc-opendata\nyc-opendata"),
        help="Directory containing the 293 NYC OpenData CSV files and nyc_opendata_maps.json.",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory where benchmark artifacts should be written.",
    )
    args = parser.parse_args()

    summary = run_clustering_pipeline(args.raw_dir.resolve(), args.artifacts_dir.resolve())
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(r"next_step: python scripts/build_databases.py --raw-dir D:\nyc-data\data\nyc-opendata\nyc-opendata --artifacts-dir artifacts")


if __name__ == "__main__":
    main()

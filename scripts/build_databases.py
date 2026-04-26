from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spatial_benchmark.clustering import run_multi_city_clustering_pipeline
from spatial_benchmark.database import run_database_blueprint_pipeline
from spatial_benchmark.etl import CONTAINER_LOAD_READY_URI_PREFIX, run_etl


def main() -> None:
    parser = argparse.ArgumentParser(description="Build scenario database blueprints and ETL artifacts from clustered sources.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path(r"D:\nyc-data\data\nyc-opendata\nyc-opendata"),
        help="Directory containing the NYC OpenData CSV files.",
    )
    parser.add_argument(
        "--chicago-manifest",
        type=Path,
        default=Path("artifacts/socrata_maps/chicago/manifest.json"),
        help="Path to Chicago Socrata manifest.json.",
    )
    parser.add_argument(
        "--lacity-manifest",
        type=Path,
        default=Path("artifacts/socrata_maps/lacity/manifest.json"),
        help="Path to LA City Socrata manifest.json.",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory where database and ETL artifacts should be written.",
    )
    parser.add_argument(
        "--seattle-manifest",
        type=Path,
        default=Path("scripts/artifacts/socrata_maps/seattle/manifest.json"),
        help="Path to Seattle manifest.json.",
    )
    parser.add_argument(
        "--boston-manifest",
        type=Path,
        default=Path("scripts/artifacts/socrata_maps/boston/manifest.json"),
        help="Path to Boston manifest.json.",
    )
    parser.add_argument(
        "--sf-manifest",
        type=Path,
        default=Path("scripts/artifacts/socrata_maps/sf/manifest.json"),
        help="Path to San Francisco manifest.json.",
    )
    parser.add_argument(
        "--phoenix-manifest",
        type=Path,
        default=Path("scripts/artifacts/socrata_maps/phoenix/manifest.json"),
        help="Path to Phoenix manifest.json.",
    )
    parser.add_argument(
        "--skip-etl",
        action="store_true",
        help="Only write database blueprints and skip ETL materialization.",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load the generated ETL artifacts into PostGIS after materialization.",
    )
    parser.add_argument(
        "--load-backend",
        choices=("auto", "docker", "psql"),
        default="auto",
        help="How to execute the PostGIS load step when --load is used.",
    )
    parser.add_argument("--container-name", default="spatial-postgis", help="Docker container name for PostGIS.")
    parser.add_argument("--db-name", default="spatial_benchmark", help="Database name.")
    parser.add_argument("--db-user", default="postgres", help="Database user.")
    parser.add_argument("--db-password", default="postgres", help="Database password for Docker-backed loads.")
    parser.add_argument("--db-host", default="localhost", help="Database host for psql-backed loads.")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port.")
    parser.add_argument("--docker-image", default="postgis/postgis:16-3.4", help="Docker image for PostGIS.")
    parser.add_argument(
        "--sql-use-host-copy-paths",
        action="store_true",
        help="Write resolved host absolute paths in postgis_load.sql (for local psql -f). Default uses POSIX paths under /nyc-data/load_ready/ for container mounts.",
    )
    parser.add_argument(
        "--sql-load-ready-prefix",
        default="",
        help=f"Override \\copy path prefix in postgis_load.sql (default: {CONTAINER_LOAD_READY_URI_PREFIX}). Ignored if --sql-use-host-copy-paths is set.",
    )
    args = parser.parse_args()

    summary = run_multi_city_clustering_pipeline(
        nyc_raw_dir=args.raw_dir.resolve(),
        artifacts_dir=args.artifacts_dir.resolve(),
        chicago_manifest_path=args.chicago_manifest.resolve() if args.chicago_manifest else None,
        lacity_manifest_path=args.lacity_manifest.resolve() if args.lacity_manifest else None,
        seattle_manifest_path=args.seattle_manifest.resolve() if args.seattle_manifest else None,
        boston_manifest_path=args.boston_manifest.resolve() if args.boston_manifest else None,
        sf_manifest_path=args.sf_manifest.resolve() if args.sf_manifest else None,
        phoenix_manifest_path=args.phoenix_manifest.resolve() if args.phoenix_manifest else None,
    )
    summary.update(run_database_blueprint_pipeline(args.artifacts_dir.resolve()))
    if not args.skip_etl:
        load_ready_uri_prefix = (
            None
            if args.sql_use_host_copy_paths
            else (args.sql_load_ready_prefix.strip() or CONTAINER_LOAD_READY_URI_PREFIX)
        )
        summary.update(
            run_etl(
                args.raw_dir.resolve(),
                args.artifacts_dir.resolve(),
                load=args.load,
                load_backend=args.load_backend,
                container_name=args.container_name,
                db_name=args.db_name,
                db_user=args.db_user,
                db_password=args.db_password,
                db_host=args.db_host,
                db_port=args.db_port,
                docker_image=args.docker_image,
                load_ready_uri_prefix=load_ready_uri_prefix,
            )
        )
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()

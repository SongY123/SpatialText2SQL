import os
from typing import List, Optional

import geopandas as gpd
from tqdm import tqdm

from .logging_config import init_pbf_logging, pbf_logger
from .shp2db import get_importer


def read_pbf_layers(pbf_path: str) -> List[tuple]:
    """
    Read the standard OSM layers from a PBF file.

    Returns:
        A list of ``(layer_name, GeoDataFrame)`` tuples for non-empty layers.
    """
    osm_layers = [
        "points",
        "lines",
        "multilinestrings",
        "multipolygons",
        "other_relations",
    ]
    valid_layers = []

    pbf_logger.info(f"Reading PBF file: {pbf_path}")

    for layer in osm_layers:
        try:
            gdf = gpd.read_file(pbf_path, layer=layer, engine="pyogrio")
            if not gdf.empty:
                pbf_logger.info(f"Layer '{layer}' found with {len(gdf)} features.")
                valid_layers.append((layer, gdf))
        except Exception:
            continue

    return valid_layers


def pbf2db(
    input_path: str,
    db_url: str,
    schema: Optional[str] = None,
    if_exists: str = "replace",
):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Path not found: {input_path}")

    importer = get_importer(db_url)

    if os.path.isdir(input_path):
        files = [
            os.path.join(input_path, f)
            for f in os.listdir(input_path)
            if f.endswith(".pbf")
        ]
    else:
        files = [input_path]

    for pbf_file in tqdm(files, desc="PBF Files Progress"):
        file_base_name = os.path.splitext(os.path.basename(pbf_file))[0]
        try:
            layers = read_pbf_layers(pbf_file)
            for layer_name, gdf in layers:
                target_table = f"{file_base_name}_{layer_name}".lower()
                if gdf.crs is None:
                    gdf.set_crs(epsg=4326, inplace=True)
                gdf["geometry"] = gdf.geometry.make_valid()
                importer.write(gdf, target_table, schema, if_exists)
        except Exception as e:
            pbf_logger.error(f"Failed to process {pbf_file}: {e}")


if __name__ == "__main__":
    init_pbf_logging(use_tqdm=True)
    DB_URL = "postgresql://postgres:1234@localhost:5432/postgis_test_db"

    try:
        pbf2db(
            input_path="osm_dir",
            db_url=DB_URL,
            schema="public",
            if_exists="replace",
        )
        print("PBF import completed.")
    except Exception as e:
        print(f"Task failed: {e}")

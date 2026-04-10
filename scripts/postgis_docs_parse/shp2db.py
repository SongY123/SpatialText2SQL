import os
import chardet
from typing import Literal, Optional
from abc import ABC, abstractmethod
import geopandas as gpd
import fiona
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from osgeo import gdal
from tqdm import tqdm

from logging_config import logger, init_spatial_logging

gdal.SetConfigOption("SHAPE_RESTORE_SHX", "YES")
gdal.SetConfigOption("SHAPE_ENCODING", "")


class SpatialDBImporter(ABC):
    def __init__(self, db_url: str):
        self.db_url = db_url

    @abstractmethod
    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
    ):
        pass


class PostGISImporter(SpatialDBImporter):
    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
    ):
        try:
            engine: Engine = create_engine(self.db_url)
            # noinspection PyTypeChecker
            gdf.to_postgis(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
            )
            target = f"{schema}.{table_name}" if schema else table_name
            logger.info(f"Data successfully written to PostGIS table: {target}")
        except Exception as pg_err:
            logger.error(f"Failed to write to PostGIS: {pg_err}")
            raise


class SpatiaLiteImporter(SpatialDBImporter):
    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
    ):
        db_path = self.db_url.split(":///")[-1]
        existing_layers = []
        if os.path.exists(db_path):
            try:
                existing_layers = fiona.listlayers(db_path)
            except Exception:
                pass

        mode = "w" if not os.path.exists(db_path) else "a"

        if table_name in existing_layers and if_exists == "replace":
            import sqlite3

            try:
                with sqlite3.connect(db_path) as conn:
                    conn.enable_load_extension(True)
                    try:
                        conn.load_extension("mod_spatialite")
                    except Exception:
                        pass
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    try:
                        conn.execute(
                            "DELETE FROM geometry_columns WHERE f_table_name = ?",
                            (table_name,),
                        )
                        conn.execute(
                            "DELETE FROM spatial_ref_sys WHERE srid NOT IN "
                            "(SELECT srid FROM geometry_columns)"
                        )
                    except sqlite3.OperationalError:
                        pass
                    conn.commit()
                logger.info(f"Dropped existing layer '{table_name}' for replacement.")
            except Exception as e:
                logger.warning(f"Could not drop existing layer via sqlite3: {e}")

        try:
            gdf.to_file(
                db_path,
                layer=table_name,
                driver="SQLite",
                engine="fiona",
                spatialite=True,
                mode=mode,
            )
            logger.info(
                f"Data successfully written to SpatiaLite: {db_path} (Layer: {table_name})"
            )
        except Exception as e:
            logger.error(f"Failed to write to SpatiaLite: {e}")
            raise


def get_importer(db_url: str) -> SpatialDBImporter:
    if db_url.startswith("postgresql"):
        return PostGISImporter(db_url)
    if db_url.startswith("sqlite"):
        return SpatiaLiteImporter(db_url)
    raise ValueError(f"Unsupported database type: {db_url}")


def detect_shp_encoding(shp_path: str) -> str:
    dbf_path = shp_path.replace(".shp", ".dbf")
    if not os.path.exists(dbf_path):
        return "utf-8"
    try:
        with open(dbf_path, "rb") as f:
            raw_data = f.read(10000)
            result = chardet.detect(raw_data)
            return result["encoding"] or "utf-8"
    except Exception:
        return "utf-8"


def read_shp_with_fallback_encoding(
    shp_path: str, layer_name: Optional[str] = None
) -> gpd.GeoDataFrame:
    detected_enc = detect_shp_encoding(shp_path)
    candidate_encodings = [
        "utf-8",
        "gb18030",
        "gbk",
        detected_enc,
        "cp936",
        "MacRoman",
        "latin-1",
    ]
    candidate_encodings = list(dict.fromkeys(candidate_encodings))

    for enc in candidate_encodings:
        try:
            if layer_name:
                gdf = gpd.read_file(shp_path, layer=layer_name, encoding=enc)
            else:
                gdf = gpd.read_file(shp_path, encoding=enc)
            logger.info(f"使用编码 {enc} 成功读取数据")
            return gdf
        except Exception:
            continue

    logger.warning("所有编码均失败，使用容错模式读取")
    with fiona.open(
        shp_path, layer=layer_name, encoding="utf-8", errors="replace"
    ) as src:
        features = []
        for feat in src:
            clean_properties = {}
            for k, v in feat["properties"].items():
                if isinstance(v, str):
                    try:
                        clean_properties[k] = v.encode("utf-8", errors="replace").decode(
                            "utf-8"
                        )
                    except Exception:
                        clean_properties[k] = str(v)
                else:
                    clean_properties[k] = v
            features.append(
                {"geometry": feat["geometry"], "properties": clean_properties}
            )
        return gpd.GeoDataFrame.from_features(features, crs=src.crs)


def _process_and_import(
    shp_path: str,
    importer: SpatialDBImporter,
    table_name: str,
    schema: Optional[str],
    if_exists: str,
):
    try:
        layers = fiona.listlayers(shp_path)
    except Exception:
        layers = []

    if len(layers) > 1:
        logger.info(f"检测到文件包含 {len(layers)} 个图层，开始逐个导入...")
        for layer_name in layers:
            try:
                current_table = layer_name
                logger.info(f"正在处理图层: {layer_name}")
                gdf = read_shp_with_fallback_encoding(shp_path, layer_name=layer_name)
                if gdf.empty:
                    continue
                if gdf.crs is None:
                    gdf.set_crs(epsg=4326, inplace=True)
                elif gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
                gdf["geometry"] = gdf.geometry.make_valid()
                for col in gdf.columns:
                    if gdf[col].dtype == "object":
                        gdf[col] = gdf[col].str.replace("\x00", "", regex=False)
                importer.write(gdf, current_table, schema, if_exists)
            except Exception as layer_err:
                logger.error(f"图层 {layer_name} 导入失败: {layer_err}")
                continue
    else:
        logger.info(f"Processing standard SHP file: {table_name}")
        try:
            gdf = read_shp_with_fallback_encoding(shp_path)
        except Exception as read_err:
            logger.error(f"Failed to read SHP file: {read_err}")
            raise
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        gdf["geometry"] = gdf.geometry.make_valid()
        for col in gdf.columns:
            if gdf[col].dtype == "object":
                gdf[col] = gdf[col].str.replace("\x00", "", regex=False)
        importer.write(gdf, table_name, schema, if_exists)


def shp2db(
    input_path: str,
    db_url: str,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    if_exists: Literal["fail", "replace", "append"] = "replace",
):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Path not found: {input_path}")

    importer = get_importer(db_url)

    if os.path.isdir(input_path):
        logger.info(f"Scanning directory recursively: {input_path}")
        shp_files_full_paths = []
        for root, _dirs, files in os.walk(input_path):
            for f in files:
                if f.lower().endswith(".shp"):
                    shp_files_full_paths.append(os.path.join(root, f))
        if not shp_files_full_paths:
            logger.warning(f"No SHP files found in directory tree: {input_path}")
            return
        for full_path in tqdm(shp_files_full_paths, desc="Total Progress", unit="file"):
            file_name = os.path.basename(full_path)
            derived_table_name = os.path.splitext(file_name)[0]
            try:
                _process_and_import(
                    full_path, importer, derived_table_name, schema, if_exists
                )
            except Exception as e:
                logger.error(f"Failed to import {file_name}: {e}")
                continue
    else:
        if not input_path.lower().endswith(".shp"):
            logger.warning(f"Input file extension is not .shp: {input_path}")
        target_table = (
            table_name
            if table_name
            else os.path.splitext(os.path.basename(input_path))[0]
        )
        _process_and_import(input_path, importer, target_table, schema, if_exists)


if __name__ == "__main__":
    init_spatial_logging(use_tqdm=True)
    DB_URL = "postgresql://postgres:1234@localhost:5432/osm"
    INPUT_PATH = "osm_dir"
    SCHEMA = "public"
    IF_EXISTS = "append"

    try:
        shp2db(
            input_path=INPUT_PATH,
            db_url=DB_URL,
            schema=SCHEMA,
            if_exists=IF_EXISTS,
        )
        tqdm.write("🎉 所有任务执行完毕！")
    except Exception as e:
        tqdm.write(f"❌ 程序执行出错：{e}")

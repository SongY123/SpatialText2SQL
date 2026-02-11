import os
import logging
from typing import Literal, Optional, Union
from abc import ABC, abstractmethod

import fiona
import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    from src.utils.logger import get_logger
    logger = get_logger("preprocess.db_importer")
except Exception:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("preprocess.db_importer")


def _resolve_overwrite_flag(if_exists: Union[str, bool]) -> bool:
    """
    统一解析覆盖策略:
    - True / 'replace' => 覆盖（先删表再导入）
    - False / 'skip' / 'fail' => 跳过已存在表
    """
    if isinstance(if_exists, bool):
        return if_exists

    v = str(if_exists).strip().lower()
    if v in {"replace", "true", "1", "yes", "y", "overwrite"}:
        return True
    if v in {"false", "0", "no", "n", "skip", "fail"}:
        return False
    raise ValueError(f"Unsupported if_exists value: {if_exists}")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class SpatialDBImporter(ABC):
    """Abstract base class for spatial database importers."""

    def __init__(self, db_url: str):
        self.db_url = db_url

    @abstractmethod
    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: Union[str, bool] = True,
    ):
        """Writes the GeoDataFrame to the database."""
        raise NotImplementedError


class PostGISImporter(SpatialDBImporter):
    """Importer for PostGIS databases."""

    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: Union[str, bool] = True,
    ):
        try:
            overwrite = _resolve_overwrite_flag(if_exists)
            engine: Engine = create_engine(self.db_url)
            target = f"{schema}.{table_name}" if schema else table_name

            exists = self._table_exists(engine, table_name, schema)
            if exists and not overwrite:
                logger.info("Table exists and if_exists=false, skip table: %s", target)
                return

            if exists and overwrite:
                self._drop_table(engine, table_name, schema)
                logger.info("Dropped existing PostGIS table before import: %s", target)

            # overwrite=True 时使用 replace；overwrite=False 且表不存在时 append 会创建新表
            write_mode = "replace" if overwrite else "append"
            # noinspection PyTypeChecker
            gdf.to_postgis(name=table_name, con=engine, schema=schema, if_exists=write_mode, index=False)
            logger.info("Data successfully written to PostGIS table: %s", target)
        except Exception as pg_err:
            logger.error("Failed to write to PostGIS: %s", pg_err)
            raise

    @staticmethod
    def _table_exists(engine: Engine, table_name: str, schema: Optional[str]) -> bool:
        with engine.connect() as conn:
            if schema:
                q = text(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM information_schema.tables
                      WHERE table_schema = :schema
                        AND table_name = :table_name
                    )
                    """
                )
                return bool(conn.execute(q, {"schema": schema, "table_name": table_name}).scalar())

            q = text(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema = current_schema()
                    AND table_name = :table_name
                )
                """
            )
            return bool(conn.execute(q, {"table_name": table_name}).scalar())

    @staticmethod
    def _drop_table(engine: Engine, table_name: str, schema: Optional[str]) -> None:
        qualified = _quote_ident(table_name)
        if schema:
            qualified = f"{_quote_ident(schema)}.{_quote_ident(table_name)}"
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qualified} CASCADE"))


class SpatiaLiteImporter(SpatialDBImporter):
    """Importer for SpatiaLite databases."""

    def write(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: Union[str, bool] = True,
    ):
        db_path = self.db_url.split(":///")[-1]
        overwrite = _resolve_overwrite_flag(if_exists)

        existing_layers = []
        if os.path.exists(db_path):
            try:
                existing_layers = fiona.listlayers(db_path)
            except Exception as e:
                logger.debug("Could not list layers in %s: %s", db_path, e)

        if table_name in existing_layers:
            if not overwrite:
                logger.info("Layer exists and if_exists=false, skip layer: %s", table_name)
                return

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
                        conn.execute("DELETE FROM geometry_columns WHERE f_table_name = ?", (table_name,))
                        conn.execute(
                            "DELETE FROM spatial_ref_sys WHERE srid NOT IN (SELECT srid FROM geometry_columns)"
                        )
                    except sqlite3.OperationalError:
                        pass
                    conn.commit()

                logger.info("Dropped existing layer '%s' for replacement.", table_name)
            except Exception as e:
                logger.warning(
                    "Could not drop existing layer '%s' via sqlite3: %s. 'replace' might fail.",
                    table_name,
                    e,
                )

        try:
            gdf.to_file(db_path, layer=table_name, driver="SQLite", engine="fiona", spatialite=True)
            logger.info("Data successfully written to SpatiaLite: %s (Layer: %s)", db_path, table_name)
        except Exception as e:
            logger.error("Failed to write to SpatiaLite: %s", e)
            raise


def get_importer(db_url: str) -> SpatialDBImporter:
    """Factory function to get the appropriate importer based on the database URL."""
    if db_url.startswith("postgresql"):
        return PostGISImporter(db_url)
    if db_url.startswith("sqlite"):
        return SpatiaLiteImporter(db_url)

    error_msg = f"Unsupported database type: {db_url}"
    logger.error(error_msg)
    raise ValueError(error_msg)


def shp2db(
    input_path: str,
    db_url: str,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    if_exists: Union[Literal["fail", "replace", "append"], bool] = True,
):
    """
    Reads SHP file(s) and imports them into a spatial database using polymorphism.

    :param input_path: Path to SHP file or directory containing SHP files.
    :param db_url: Database connection URL (e.g., postgresql://... or sqlite:///...).
    :param table_name: Target table name. If input_path is a directory, this is ignored and filenames are used.
    :param schema: Target schema (PostGIS only).
    :param if_exists: 表存在策略。True/replace=覆盖；False/skip/fail=跳过该表。
    """
    if not os.path.exists(input_path):
        logger.error("Path not found: %s", input_path)
        raise FileNotFoundError(f"Path not found: {input_path}")

    importer = get_importer(db_url)

    if os.path.isdir(input_path):
        logger.info("Input is a directory. Scanning for SHP files in: %s", input_path)
        shp_files = [f for f in os.listdir(input_path) if f.lower().endswith(".shp")]

        if not shp_files:
            logger.warning("No SHP files found in directory: %s", input_path)
            return

        for shp_file in shp_files:
            full_path = os.path.join(input_path, shp_file)
            derived_table_name = os.path.splitext(shp_file)[0]
            _process_and_import(full_path, importer, derived_table_name, schema, if_exists)
    else:
        if not input_path.lower().endswith(".shp"):
            logger.warning("Input file does not have .shp extension: %s", input_path)

        target_table = table_name if table_name else os.path.splitext(os.path.basename(input_path))[0]
        _process_and_import(input_path, importer, target_table, schema, if_exists)


def _process_and_import(
    shp_path: str,
    importer: SpatialDBImporter,
    table_name: str,
    schema: Optional[str],
    if_exists: Union[str, bool],
):
    """Reads, preprocesses, and imports a single SHP file using the provided importer."""
    logger.info("Reading SHP file: %s", shp_path)
    try:
        gdf = gpd.read_file(shp_path)
    except Exception as read_err:
        logger.error("Failed to read SHP file: %s", read_err)
        raise

    logger.info("Successfully read %d features", len(gdf))

    if gdf.crs is None:
        logger.warning("Input data missing CRS, assuming EPSG:4326")
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        logger.info("Converting CRS from %s to EPSG:4326", gdf.crs.to_epsg())
        gdf = gdf.to_crs(epsg=4326)

    gdf["geometry"] = gdf.geometry.make_valid()
    importer.write(gdf, table_name, schema, if_exists)

import os
from logging_config import logger
from typing import Literal, Optional
from abc import ABC, abstractmethod
import geopandas as gpd
import fiona
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Use the package-level `logger` from src.logging_config. Other modules can
# either `from src.logging_config import logger` or use `get_logger(__name__)`.

class SpatialDBImporter(ABC):
    """
    Abstract base class for spatial database importers.
    """
    def __init__(self, db_url: str):
        self.db_url = db_url

    @abstractmethod
    def write(self, gdf: gpd.GeoDataFrame, table_name: str, schema: Optional[str] = None, if_exists: str = 'replace'):
        """
        Writes the GeoDataFrame to the database.
        """
        pass

class PostGISImporter(SpatialDBImporter):
    """
    Importer for PostGIS databases.
    """
    def write(self, gdf: gpd.GeoDataFrame, table_name: str, schema: Optional[str] = None, if_exists: str = 'replace'):
        try:
            engine: Engine = create_engine(self.db_url)
            # noinspection PyTypeChecker
            gdf.to_postgis(name=table_name, con=engine, schema=schema, if_exists=if_exists, index=False)
            target = f"{schema}.{table_name}" if schema else table_name
            logger.info(f"Data successfully written to PostGIS table: {target}")
        except Exception as pg_err:
            logger.error(f"Failed to write to PostGIS: {pg_err}")
            raise

class SpatiaLiteImporter(SpatialDBImporter):
    """
    Importer for SpatiaLite databases.
    """
    def write(self, gdf: gpd.GeoDataFrame, table_name: str, schema: Optional[str] = None, if_exists: str = 'replace'):
        db_path = self.db_url.split(":///")[-1]

        # Determine existing layers (if any) so we can support writing multiple
        # layers into the same SQLite DB and honor `if_exists` semantics.
        existing_layers = []
        if os.path.exists(db_path):
            try:
                existing_layers = fiona.listlayers(db_path)
            except Exception as e:
                # If fiona can't list layers, continue and let write attempts fail
                logger.debug("Could not list layers in %s: %s", db_path, e)

        # Decide default mode for Fiona's to_file: 'w' creates DB, 'a' appends layer
        mode = 'w' if not os.path.exists(db_path) else 'a'

        # Handle existing layer name according to if_exists
        if table_name in existing_layers:
            if if_exists == 'fail':
                raise FileExistsError(f"Layer '{table_name}' already exists in {db_path}")
            elif if_exists == 'replace':
                import sqlite3
                try:
                    with sqlite3.connect(db_path) as conn:
                        # Enable spatialite extension if possible, though for DROP TABLE it might not be strictly necessary
                        # providing we clean up geometry_columns too.
                        conn.enable_load_extension(True)
                        # Attempt to load spatialite to properly clean up metadata
                        # This path varies by system; if it fails, we just do a raw drop
                        try:
                            conn.load_extension("mod_spatialite")
                        except:
                            pass

                        # Drop the table
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        # Clean up geometry_columns if it exists
                        try:
                            conn.execute("DELETE FROM geometry_columns WHERE f_table_name = ?", (table_name,))
                            conn.execute("DELETE FROM spatial_ref_sys WHERE srid NOT IN (SELECT srid FROM geometry_columns)") # Optional cleanup
                        except sqlite3.OperationalError:
                            pass # geometry_columns might not exist

                        conn.commit()

                    # After dropping, we switch to append mode
                    mode = 'a'
                    logger.info(f"Dropped existing layer '{table_name}' for replacement.")
                except Exception as e:
                    logger.warning(f"Could not drop existing layer '{table_name}' via sqlite3: {e}. 'replace' might fail.")

        try:
            gdf.to_file(db_path, layer=table_name, driver='SQLite', engine='fiona', spatialite=True)
            logger.info(f"Data successfully written to SpatiaLite: {db_path} (Layer: {table_name})")
        except Exception as e:
            logger.error(f"Failed to write to SpatiaLite: {e}")
            raise

def get_importer(db_url: str) -> SpatialDBImporter:
    """
    Factory function to get the appropriate importer based on the database URL.
    """
    if db_url.startswith("postgresql"):
        return PostGISImporter(db_url)
    elif db_url.startswith("sqlite"):
        return SpatiaLiteImporter(db_url)
    else:
        error_msg = f"Unsupported database type: {db_url}"
        logger.error(error_msg)
        raise ValueError(error_msg)

def shp2db(input_path: str, db_url: str, table_name: Optional[str] = None, schema: Optional[str] = None, if_exists: Literal['fail', 'replace', 'append'] = 'replace'):
    """
    Reads SHP file(s) and imports them into a spatial database using polymorphism.
    
    :param input_path: Path to SHP file or directory containing SHP files.
    :param db_url: Database connection URL (e.g., postgresql://... or sqlite:///...).
    :param table_name: Target table name. If input_path is a directory, this is ignored and filenames are used.
    :param schema: Target schema (PostGIS only).
    :param if_exists: Behavior if table exists ('fail', 'replace', 'append').
    """
    if not os.path.exists(input_path):
        logger.error(f"Path not found: {input_path}")
        raise FileNotFoundError(f"Path not found: {input_path}")

    try:
        importer = get_importer(db_url)
    except ValueError as e:
        logger.error(e)
        raise

    if os.path.isdir(input_path):
        logger.info(f"Input is a directory. Scanning for SHP files in: {input_path}")
        shp_files = [f for f in os.listdir(input_path) if f.lower().endswith('.shp')]
        
        if not shp_files:
            logger.warning(f"No SHP files found in directory: {input_path}")
            return

        for shp_file in shp_files:
            full_path = os.path.join(input_path, shp_file)
            # Derive table name from filename without extension
            derived_table_name = os.path.splitext(shp_file)[0]
            try:
                _process_and_import(full_path, importer, derived_table_name, schema, if_exists)
            except Exception as e:
                logger.error(f"Failed to import {shp_file}: {e}")
                raise
    else:
        # Single file
        if not input_path.lower().endswith('.shp'):
             logger.warning(f"Input file does not have .shp extension: {input_path}")
        
        target_table = table_name if table_name else os.path.splitext(os.path.basename(input_path))[0]
        _process_and_import(input_path, importer, target_table, schema, if_exists)

def _process_and_import(shp_path: str, importer: SpatialDBImporter, table_name: str, schema: Optional[str], if_exists: str):
    """
    Reads, preprocesses, and imports a single SHP file using the provided importer.
    """
    logger.info(f"Reading SHP file: {shp_path}")
    try:
        gdf = gpd.read_file(shp_path)
    except Exception as read_err:
        logger.error(f"Failed to read SHP file: {read_err}")
        raise

    logger.info(f"Successfully read {len(gdf)} features")

    # Preprocessing
    # Ensure CRS is WGS 84 (EPSG:4326)
    if gdf.crs is None:
        logger.warning("Input data missing CRS, assuming EPSG:4326")
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"Converting CRS from {gdf.crs.to_epsg()} to EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)

    # Fix invalid geometries
    gdf['geometry'] = gdf.geometry.make_valid()

    # Delegate writing to the importer
    importer.write(gdf, table_name, schema, if_exists)

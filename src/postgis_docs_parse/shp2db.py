"""Compatibility wrapper for the canonical importer implementation in src.utils."""

from src.utils.shp2db import (
    IfExistsMode,
    PostGISImporter,
    SpatialDBImporter,
    SpatiaLiteImporter,
    collect_input_files,
    detect_shp_encoding,
    get_importer,
    iter_with_progress,
    normalize_geodataframe,
    read_shp_with_fallback_encoding,
    shp2db,
)

__all__ = [
    "IfExistsMode",
    "PostGISImporter",
    "SpatialDBImporter",
    "SpatiaLiteImporter",
    "collect_input_files",
    "detect_shp_encoding",
    "get_importer",
    "iter_with_progress",
    "normalize_geodataframe",
    "read_shp_with_fallback_encoding",
    "shp2db",
]

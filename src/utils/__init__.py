"""Lightweight utils package exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["geojson2db", "pbf2db", "shp2db"]


def __getattr__(name: str) -> Any:
    if name == "geojson2db":
        return import_module(".geojson2db", __name__).geojson2db
    if name == "pbf2db":
        return import_module(".pbf2db", __name__).pbf2db
    if name == "shp2db":
        return import_module(".shp2db", __name__).shp2db
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

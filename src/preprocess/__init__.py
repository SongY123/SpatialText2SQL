from typing import Any

__all__ = ["shp2db", "run_all"]


def shp2db(*args: Any, **kwargs: Any):
    from .db_Importer import shp2db as _shp2db
    return _shp2db(*args, **kwargs)


def run_all(*args: Any, **kwargs: Any):
    from .main import run_all as _run_all
    return _run_all(*args, **kwargs)

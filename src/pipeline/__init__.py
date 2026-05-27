"""主流程编排模块。"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["MainPipeline", "main"]


def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(".main", __name__)
    return getattr(module, name)

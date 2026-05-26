"""Registry for dataset-specific prompt enhancement adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from src.datasets.names import canonicalize_dataset_name

from .floodsql.adapter import FloodSQLPromptEnhancement
from .spatialqueryqa.adapter import SpatialQAPromptEnhancement
from .spatialsql.adapter import SpatialSQLPromptEnhancement


class PromptEnhancementRegistry:
    """Resolve dataset-specific prompt assets from prompt enhancement packages."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self._enhancements = {
            "floodsql": FloodSQLPromptEnhancement(self.project_root),
            "spatialqueryqa": SpatialQAPromptEnhancement(self.project_root),
            "spatialsql": SpatialSQLPromptEnhancement(self.project_root),
        }

    def resolve_dataset_override(self, dataset_name: str) -> Dict[str, Any]:
        enhancement = self._enhancements.get(canonicalize_dataset_name(dataset_name))
        if enhancement is None:
            return {}
        return enhancement.get_prompt_style_override()

    def build_grounding_block(
        self,
        dataset_name: str,
        metadata: Dict[str, Any],
    ) -> str:
        enhancement = self._enhancements.get(canonicalize_dataset_name(dataset_name))
        if enhancement is None:
            return ""
        return enhancement.build_grounding_block(metadata or {})

    def build_schema_semantics_block(
        self,
        dataset_name: str,
        metadata: Dict[str, Any],
        compact_schema: str,
    ) -> str:
        enhancement = self._enhancements.get(canonicalize_dataset_name(dataset_name))
        if enhancement is None:
            return ""
        build_fn = getattr(enhancement, "build_schema_semantics_block", None)
        if build_fn is None:
            return ""
        return build_fn(metadata or {}, compact_schema or "")

"""Registry for dataset-specific prompt enhancement adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .floodsql_pg.adapter import FloodSQLPromptEnhancement
from .spatial_qa.adapter import SpatialQAPromptEnhancement
from .spatialsql_pg.adapter import SpatialSQLPromptEnhancement


class PromptEnhancementRegistry:
    """Resolve dataset-specific prompt assets from prompt enhancement packages."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self._enhancements = {
            "floodsql_pg": FloodSQLPromptEnhancement(self.project_root),
            "spatial_qa": SpatialQAPromptEnhancement(self.project_root),
            "spatialsql_pg": SpatialSQLPromptEnhancement(self.project_root),
        }

    def resolve_dataset_override(self, dataset_name: str) -> Dict[str, Any]:
        enhancement = self._enhancements.get(dataset_name)
        if enhancement is None:
            return {}
        return enhancement.get_prompt_style_override()

    def build_grounding_block(
        self,
        dataset_name: str,
        metadata: Dict[str, Any],
    ) -> str:
        enhancement = self._enhancements.get(dataset_name)
        if enhancement is None:
            return ""
        return enhancement.build_grounding_block(metadata or {})

    def build_schema_semantics_block(
        self,
        dataset_name: str,
        metadata: Dict[str, Any],
        compact_schema: str,
    ) -> str:
        enhancement = self._enhancements.get(dataset_name)
        if enhancement is None:
            return ""
        build_fn = getattr(enhancement, "build_schema_semantics_block", None)
        if build_fn is None:
            return ""
        return build_fn(metadata or {}, compact_schema or "")

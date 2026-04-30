"""Prompt enhancement adapter for the FloodSQL PostgreSQL benchmark."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .context_provider import FloodSQLContextProvider


class FloodSQLPromptEnhancement:
    """Provide dataset-specific prompt assets for `floodsql_pg`."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self.context_provider = FloodSQLContextProvider(self.project_root)

    def get_prompt_style_override(self) -> Dict[str, Any]:
        return {
            "template_path": "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
            "include_sample_data": True,
            "use_dataset_context": True,
        }

    def build_grounding_block(self, metadata: Dict[str, Any]) -> str:
        return self.context_provider.build_grounding(metadata or {})

    def build_schema_semantics_block(
        self,
        metadata: Dict[str, Any],
        compact_schema: str,
    ) -> str:
        del metadata
        return self.context_provider.build_schema_semantics(compact_schema or "")

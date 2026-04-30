"""Prompt enhancement adapter for the Spatial QA benchmark."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .context_provider import SpatialQAContextProvider


class SpatialQAPromptEnhancement:
    """Provide dataset-specific prompt assets for `spatial_qa`."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self.context_provider = SpatialQAContextProvider()

    def get_prompt_style_override(self) -> Dict[str, Any]:
        return {
            "template_path": "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
            "include_sample_data": True,
            "use_dataset_context": True,
        }

    def build_grounding_block(self, metadata: Dict[str, Any]) -> str:
        context = self.context_provider.get_context(metadata)
        lines: List[str] = []

        level = str(context.get("level") or "").strip()
        focus = str(context.get("focus") or "").strip()
        if level:
            lines.append(f"- Spatial QA difficulty level: {level}")
        if focus:
            lines.append(f"- Level focus: {focus}")

        for hint in context.get("hints", []):
            text = str(hint).strip()
            if text:
                lines.append(f"- {text}")

        return "\n".join(lines)

    def build_schema_semantics_block(
        self,
        metadata: Dict[str, Any],
        compact_schema: str,
    ) -> str:
        del metadata
        return self.context_provider.build_schema_semantics(compact_schema)

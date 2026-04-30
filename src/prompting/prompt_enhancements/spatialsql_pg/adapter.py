"""Prompt enhancement adapter for the SpatialSQL PostgreSQL benchmark."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .context_provider import SpatialSQLContextProvider


class SpatialSQLPromptEnhancement:
    """Provide dataset-specific prompt assets for `spatialsql_pg`."""

    def __init__(self, project_root: Path | str):
        self.project_root = Path(project_root).resolve()
        self.context_provider = SpatialSQLContextProvider(self.project_root)

    def get_prompt_style_override(self) -> Dict[str, Any]:
        return {
            "template_path": "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
            "include_sample_data": True,
            "use_dataset_context": True,
        }

    def build_grounding_block(self, metadata: Dict[str, Any]) -> str:
        context = self.context_provider.get_context("spatialsql_pg", metadata)
        if not context:
            return ""

        lines: List[str] = []
        self._append_hint_line(lines, "Chinese question", context.get("questionCHI"))
        self._append_hint_line(lines, "English evidence", context.get("evidence"))
        self._append_hint_line(lines, "Chinese evidence", context.get("evidenceCHI"))
        self._append_hint_line(lines, "English value grounding", context.get("name"))
        self._append_hint_line(lines, "Chinese value grounding", context.get("nameCHI"))
        return "\n".join(lines)

    @staticmethod
    def _append_hint_line(lines: List[str], label: str, value: Optional[str]) -> None:
        text = (value or "").strip()
        if text:
            lines.append(f"- {label}: {text}")

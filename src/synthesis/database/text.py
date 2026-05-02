"""Text construction for table embeddings."""

from __future__ import annotations

from .models import CanonicalSpatialTable
from .utils import stable_json_dumps


def build_table_text(table: CanonicalSpatialTable) -> str:
    """Build a stable text representation used for embeddings."""
    parts = [
        f"table_name: {table.table_name}",
        f"semantic_summary: {table.semantic_summary or ''}",
        f"normalized_schema: {stable_json_dumps(table.normalized_schema)}",
        f"representative_values: {stable_json_dumps(table.representative_values)}",
        f"themes: {stable_json_dumps(table.themes)}",
    ]
    return "\n".join(parts)


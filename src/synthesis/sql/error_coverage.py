"""Error-driven coverage profiles for SQL synthesis.

The profiles mirror recurring benchmark failures summarized in ``errors.md``.
They intentionally cover mutually incompatible dataset conventions by rotating
explicit scenarios instead of applying one global spatial-measurement rule.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from src.synthesis.database.models import SynthesizedSpatialDatabase
from src.synthesis.database.utils import stable_jsonify, to_text

from .function_library import PostGISFunctionLibrary
from .models import DIFFICULTY_LEVELS, PostGISFunction

DIFFICULTY_RANK = {level: index for index, level in enumerate(DIFFICULTY_LEVELS)}

ERROR_COVERAGE_PROFILES: list[dict[str, Any]] = [
    {
        "profile_id": "spatialsql_geography_spheroid_measurement",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_Area", "ST_Length", "ST_Distance"],
        "signature_preference": "geography_spheroid",
        "target_errors": ["measurement_strategy_conflict", "missing_spheroid_flag"],
        "query_shape": "Measure the actual geometry column as geography with spheroid=true; avoid ST_Transform.",
        "constraints": [
            "Use ST_Length/ST_Area/ST_Distance on actual_geom::geography with true when the function accepts it.",
            "Do not mix ST_Transform with geography casts in this profile.",
        ],
    },
    {
        "profile_id": "spatialqueryqa_projected_units_measurement",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_Transform", "ST_Area", "ST_Length"],
        "signature_preference": "geometry",
        "target_errors": ["measurement_strategy_conflict", "unit_conversion_mismatch"],
        "query_shape": "Use projected geometry for planar area/length/ranking when the task needs projected units.",
        "constraints": [
            "Use ST_Transform(actual_geom, target_srid) before planar ST_Length/ST_Area.",
            "Do not add geography casts in this profile.",
        ],
    },
    {
        "profile_id": "floodsql_valid_geometry_measurement",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_IsValid", "ST_Area", "ST_Intersects"],
        "signature_preference": "geometry",
        "target_errors": ["invalid_geometry_execution_error", "geometry_column_hallucination"],
        "query_shape": "Filter invalid geometries before geometry area or spatial joins.",
        "constraints": [
            "Add ST_IsValid(actual_geom) before geometry measurement or spatial join.",
            "Use only schema-listed geometry columns; do not invent geom/shape aliases.",
        ],
    },
    {
        "profile_id": "predicate_direction_join",
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_Contains", "ST_Within", "ST_Intersects"],
        "signature_preference": "geometry",
        "target_errors": ["predicate_direction_reversal", "join_condition_mismatch"],
        "query_shape": "Join two spatial tables with a direction-sensitive predicate.",
        "constraints": [
            "Keep container geometry as the first ST_Contains argument and contained geometry as the second.",
            "Use ST_Within only when the subject is inside the reference geometry.",
        ],
    },
    {
        "profile_id": "distance_threshold_join",
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_DWithin", "ST_Distance"],
        "signature_preference": "geometry",
        "target_errors": ["distance_threshold_loss", "nearest_ranking_mismatch"],
        "query_shape": "Use one distance threshold or nearest/farthest ranking with matching units.",
        "constraints": [
            "Preserve the exact distance threshold and unit strategy.",
            "If ranking by distance, ORDER BY the distance expression and project only requested output.",
        ],
    },
    {
        "profile_id": "intersection_measurement",
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_Intersects", "ST_Intersection", "ST_Area", "ST_Length"],
        "signature_preference": "geometry",
        "target_errors": ["intersection_measurement_mismatch", "output_shape_mismatch"],
        "query_shape": "Measure intersected overlap length/area while returning the requested scalar or label.",
        "constraints": [
            "Use ST_Intersection only for the overlap being measured.",
            "Do not return the intersection geometry unless geometry output is requested.",
        ],
    },
    {
        "profile_id": "boundary_adjacency",
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_Touches", "ST_Intersection", "ST_Length"],
        "signature_preference": "geometry",
        "target_errors": ["boundary_relation_confusion", "line_polygon_measurement_mismatch"],
        "query_shape": "Cover touching/boundary cases and boundary-length measurements.",
        "constraints": [
            "Use ST_Touches for boundary contact, not ST_Intersects when interiors must not overlap.",
            "Measure boundary overlap with ST_Length(ST_Intersection(...)) only when semantically required.",
        ],
    },
    {
        "profile_id": "aggregation_output_shape",
        "min_difficulty": "hard",
        "min_spatial_tables": 2,
        "function_names": ["ST_Intersects", "ST_Collect"],
        "signature_preference": "geometry",
        "target_errors": ["aggregate_vs_row_output_mismatch", "geometry_output_leak"],
        "query_shape": "Use spatial aggregation only when the requested answer is aggregate or grouped.",
        "constraints": [
            "Return scalar aggregates by default; include grouped labels only when asked.",
            "Avoid ST_Collect output unless the intended answer is geometry.",
        ],
    },
]


def select_error_coverage_profile(
    *,
    database: SynthesizedSpatialDatabase,
    sample_index: int,
    difficulty_level: str,
) -> dict[str, Any] | None:
    """Select a deterministic coverage profile compatible with the prompt schema."""

    spatial_table_count = sum(1 for table in database.selected_tables if table.spatial_fields)
    total_table_count = len(database.selected_tables)
    difficulty_rank = DIFFICULTY_RANK.get(difficulty_level, 0)
    eligible: list[dict[str, Any]] = []
    for profile in ERROR_COVERAGE_PROFILES:
        min_difficulty = to_text(profile.get("min_difficulty")) or "easy"
        if difficulty_rank < DIFFICULTY_RANK.get(min_difficulty, 0):
            continue
        if spatial_table_count < int(profile.get("min_spatial_tables") or 1):
            continue
        if total_table_count < int(profile.get("min_tables") or 1):
            continue
        eligible.append(profile)
    if not eligible:
        return None
    selected = eligible[int(sample_index) % len(eligible)]
    return stable_jsonify(selected)


def augment_functions_for_error_coverage(
    *,
    function_library: PostGISFunctionLibrary,
    sampled_functions: Sequence[PostGISFunction],
    coverage_profile: Mapping[str, Any] | None,
    database: SynthesizedSpatialDatabase | None = None,
    difficulty_level: str = "",
    rng: Any | None = None,
    st_function_only: bool = False,
    max_added_functions: int = 4,
    max_total_functions: int = 6,
    min_general_functions: int = 1,
) -> list[PostGISFunction]:
    """Blend deterministic error coverage with general ST_Function diversity."""

    base_functions = _dedupe_functions_by_name(sampled_functions)
    if not coverage_profile:
        return base_functions

    profile_functions: list[PostGISFunction] = []
    seen: set[str] = set()
    signature_preference = to_text(coverage_profile.get("signature_preference"))
    added = 0
    for function_name in stable_jsonify(coverage_profile.get("function_names")) or []:
        if added >= max_added_functions:
            break
        text_name = to_text(function_name)
        if not text_name or text_name.lower() in seen:
            continue
        candidate = _select_function_signature(
            function_library,
            text_name,
            st_function_only=st_function_only,
            signature_preference=signature_preference,
        )
        if candidate is None:
            continue
        profile_functions.append(candidate)
        seen.add(candidate.function_name.lower())
        added += 1
    if not profile_functions:
        return base_functions

    profile_names = {item.function_name.lower() for item in profile_functions if item.function_name}
    merged: list[PostGISFunction] = []
    _append_unique_functions(
        merged,
        [item for item in base_functions if item.function_name.lower() not in profile_names],
        max_total_functions=max_total_functions,
    )
    _append_unique_functions(
        merged,
        profile_functions,
        max_total_functions=max_total_functions,
    )
    _append_unique_functions(
        merged,
        [item for item in base_functions if item.function_name.lower() in profile_names],
        max_total_functions=max_total_functions,
    )

    general_function_count = sum(
        1 for item in merged if item.function_name and item.function_name.lower() not in profile_names
    )
    if (
        general_function_count < max(0, int(min_general_functions))
        and database is not None
        and difficulty_level in DIFFICULTY_LEVELS
        and rng is not None
    ):
        excluded = profile_names | {
            item.function_name.lower()
            for item in merged
            if item.function_name
        }
        supplemental = function_library.sample_functions(
            database=database,
            difficulty_level=difficulty_level,
            rng=rng,
            st_function_only=st_function_only,
            exclude_function_names=sorted(excluded),
            desired_count=max(0, int(min_general_functions) - general_function_count),
        )
        _append_unique_functions(
            merged,
            supplemental,
            max_total_functions=max_total_functions,
        )

    return merged or profile_functions or base_functions


def _dedupe_functions_by_name(functions: Sequence[PostGISFunction]) -> list[PostGISFunction]:
    deduped: list[PostGISFunction] = []
    seen: set[str] = set()
    for item in functions:
        name = to_text(getattr(item, "function_name", "")).lower()
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(item)
    return deduped


def _append_unique_functions(
    target: list[PostGISFunction],
    candidates: Sequence[PostGISFunction],
    *,
    max_total_functions: int,
) -> None:
    seen = {
        to_text(getattr(item, "function_name", "")).lower()
        for item in target
        if to_text(getattr(item, "function_name", ""))
    }
    for item in candidates:
        if len(target) >= max_total_functions:
            return
        name = to_text(getattr(item, "function_name", "")).lower()
        if not name or name in seen:
            continue
        target.append(item)
        seen.add(name)


def _select_function_signature(
    function_library: PostGISFunctionLibrary,
    function_name: str,
    *,
    st_function_only: bool,
    signature_preference: str = "",
) -> PostGISFunction | None:
    candidates = function_library.get_function_signatures(function_name)
    if st_function_only:
        candidates = [
            item
            for item in candidates
            if any(to_text(source).strip() == "ST_Function.md" for source in item.source)
        ]
    if not candidates:
        return None
    preferred = [
        item
        for item in candidates
        if any(to_text(source).strip() == "ST_Function.md" for source in item.source)
    ]
    pool = _filter_by_signature_preference(preferred or candidates, signature_preference)
    return sorted(pool, key=lambda item: item.signature.lower())[0]


def _filter_by_signature_preference(
    candidates: Sequence[PostGISFunction],
    signature_preference: str,
) -> list[PostGISFunction]:
    preference = to_text(signature_preference).lower()
    if not candidates or not preference:
        return list(candidates)

    if preference == "geometry":
        geometry_candidates = [
            item for item in candidates if "geometry" in item.signature.lower()
        ]
        return geometry_candidates or list(candidates)

    if preference == "geography_spheroid":
        geography_candidates = [
            item for item in candidates if "geography" in item.signature.lower()
        ]
        if not geography_candidates:
            return list(candidates)
        spheroid_candidates = [
            item
            for item in geography_candidates
            if "spheroid" in item.signature.lower() or "boolean" in item.signature.lower()
        ]
        return spheroid_candidates or geography_candidates

    return list(candidates)

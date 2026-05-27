"""Benchmark-driven coverage profiles for SQL synthesis.

The profiles cover two sources:
- recurring failure modes summarized in ``errors.md``
- dominant SQL shapes observed in ``data/benchmark/normalized``

Core benchmark patterns remain the primary target. Low-frequency benchmark tail
functions are sampled through a smaller secondary profile pool so coverage can
expand without overwhelming the main synthesis loop.
"""

from __future__ import annotations

import zlib
from typing import Any, Mapping, Sequence

from src.synthesis.database.models import SynthesizedSpatialDatabase
from src.synthesis.database.utils import stable_jsonify, to_text

from .function_library import PostGISFunctionLibrary
from .models import DIFFICULTY_LEVELS, PostGISFunction

DIFFICULTY_RANK = {level: index for index, level in enumerate(DIFFICULTY_LEVELS)}

CORE_BENCHMARK_FUNCTION_NAMES: tuple[str, ...] = (
    "ST_Area",
    "ST_Centroid",
    "ST_Contains",
    "ST_DWithin",
    "ST_Distance",
    "ST_Intersection",
    "ST_Intersects",
    "ST_IsValid",
    "ST_Length",
    "ST_Overlaps",
    "ST_Touches",
    "ST_Transform",
    "ST_Within",
)

TAIL_BENCHMARK_FUNCTION_NAMES: tuple[str, ...] = (
    "ST_AsText",
    "ST_Buffer",
    "ST_CollectionExtract",
    "ST_Collect",
    "ST_Crosses",
    "ST_GeometryType",
    "ST_IsEmpty",
    "ST_MakeValid",
    "ST_Perimeter",
    "ST_SRID",
    "ST_UnaryUnion",
    "ST_X",
    "ST_XMax",
    "ST_XMin",
    "ST_Y",
    "ST_YMax",
    "ST_YMin",
)

TAIL_PROFILE_INTERVAL = 5

CORE_ERROR_COVERAGE_PROFILES: list[dict[str, Any]] = [
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
        "profile_id": "unit_scaled_measurement_output",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_Area", "ST_Length", "ST_Distance", "ST_Transform"],
        "signature_preference": "geometry",
        "target_errors": ["unit_conversion_mismatch", "measurement_output_scale_loss"],
        "query_shape": "Return one scaled measurement in kilometers or square kilometers when the sampled shape needs unit conversion.",
        "constraints": [
            "Apply the unit conversion in SQL, such as divide area by 1000000.0 or divide length/distance by 1000.0.",
            "Keep the scaling outside ST_Area/ST_Length/ST_Distance and return a scalar numeric result.",
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
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_Intersects", "ST_Area", "ST_IsValid"],
        "signature_preference": "geometry",
        "target_errors": ["aggregate_vs_row_output_mismatch", "geometry_output_leak"],
        "query_shape": "Use spatial aggregation only when the requested answer is aggregate or grouped.",
        "constraints": [
            "Return scalar aggregates by default; include grouped labels only when asked.",
            "Use GROUP BY or aggregate expressions only when the question asks for grouped or aggregate output.",
        ],
    },
    {
        "profile_id": "distinct_grouped_result_shape",
        "min_difficulty": "medium",
        "min_spatial_tables": 1,
        "function_names": ["ST_Intersects", "ST_Contains", "ST_IsValid"],
        "signature_preference": "geometry",
        "target_errors": ["missing_distinct", "grouping_shape_mismatch"],
        "query_shape": "Return DISTINCT rows or grouped aggregates when duplicate-producing joins are likely.",
        "constraints": [
            "Use DISTINCT or GROUP BY when the intended answer is deduplicated or grouped.",
            "Project only the requested label columns or aggregate outputs.",
        ],
    },
    {
        "profile_id": "offset_ranked_result",
        "min_difficulty": "medium",
        "min_spatial_tables": 1,
        "function_names": ["ST_Distance", "ST_Intersects"],
        "signature_preference": "geometry",
        "target_errors": ["offset_loss", "rank_window_mismatch"],
        "query_shape": "Return the second or nth ranked row with ORDER BY, LIMIT 1, and OFFSET.",
        "constraints": [
            "Use ORDER BY with LIMIT 1 and a positive OFFSET for nth-result queries.",
            "Do not add OFFSET to scalar aggregate queries.",
        ],
    },
    {
        "profile_id": "nested_cte_subquery_shape",
        "min_difficulty": "extra-hard",
        "min_spatial_tables": 2,
        "function_names": ["ST_Intersects", "ST_IsValid", "ST_Area"],
        "signature_preference": "geometry",
        "target_errors": ["missing_nested_shape", "flattened_scope_mismatch"],
        "query_shape": "Use one CTE or one nested subquery to isolate candidates before the final aggregate or ranking.",
        "constraints": [
            "Include exactly one CTE or one nested subquery when the sampled shape requires staged filtering.",
            "Keep the final SELECT compact and return only the intended answer columns.",
        ],
    },
]

TAIL_BENCHMARK_PROFILES: list[dict[str, Any]] = [
    {
        "profile_id": "bbox_extent_accessor_tail",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_XMin", "ST_XMax", "ST_YMin", "ST_YMax"],
        "rotation_function_groups": [
            ["ST_XMin"],
            ["ST_XMax"],
            ["ST_YMin"],
            ["ST_YMax"],
        ],
        "signature_preference": "geometry",
        "query_shape": "Use one extent accessor to rank or filter far-west, far-east, far-south, or far-north geometries.",
        "constraints": [
            "Use exactly one of ST_XMin/ST_XMax/ST_YMin/ST_YMax in ORDER BY or a scalar filter.",
            "Keep the result compact: return the requested label or one scalar aggregate.",
        ],
    },
    {
        "profile_id": "point_coordinate_accessor_tail",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_X", "ST_Y"],
        "rotation_function_groups": [
            ["ST_X"],
            ["ST_Y"],
        ],
        "rotation_seed_mode": "weighted",
        "signature_preference": "geometry",
        "required_geometry_kinds": ["point"],
        "query_shape": "Return or rank point features by longitude or latitude using ST_X or ST_Y.",
        "constraints": [
            "Use ST_X or ST_Y only on an actual point geometry column.",
            "Do not wrap the point accessor in unnecessary joins or geometry transforms.",
        ],
    },
    {
        "profile_id": "geometry_metadata_tail",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_SRID", "ST_AsText", "ST_GeometryType"],
        "rotation_function_groups": [
            ["ST_SRID"],
            ["ST_AsText"],
            ["ST_GeometryType"],
        ],
        "signature_preference": "geometry",
        "query_shape": "Return geometry metadata or one WKT text value derived from an existing geometry expression.",
        "constraints": [
            "Use ST_SRID, ST_AsText, or ST_GeometryType directly on a schema-listed geometry expression.",
            "Do not return raw geometry when a metadata scalar or WKT string is the intended answer.",
        ],
    },
    {
        "profile_id": "buffered_relation_tail",
        "min_difficulty": "medium",
        "min_spatial_tables": 1,
        "function_names": ["ST_Buffer", "ST_Within", "ST_Intersects"],
        "signature_preference": "geometry",
        "query_shape": "Build one buffer from an existing geometry and use it in one spatial relation.",
        "constraints": [
            "Create the buffer from a schema-listed geometry only; do not invent anonymous points.",
            "Use the buffered geometry in one ST_Within or ST_Intersects predicate.",
        ],
    },
    {
        "profile_id": "collection_union_tail",
        "min_difficulty": "hard",
        "min_spatial_tables": 1,
        "function_names": ["ST_Collect", "ST_UnaryUnion", "ST_CollectionExtract", "ST_IsEmpty"],
        "rotation_function_groups": [
            ["ST_CollectionExtract", "ST_IsEmpty"],
            ["ST_CollectionExtract"],
            ["ST_Collect"],
            ["ST_Collect", "ST_UnaryUnion"],
        ],
        "rotation_seed_mode": "none",
        "signature_preference": "geometry",
        "query_shape": "Stage one collected or extracted geometry in a CTE or subquery before the final measurement or join.",
        "constraints": [
            "Use ST_Collect, ST_UnaryUnion, or ST_CollectionExtract in one staged intermediate result.",
            "Use ST_IsEmpty only to filter the staged geometry result when needed.",
        ],
    },
    {
        "profile_id": "crosses_predicate_tail",
        "min_difficulty": "medium",
        "min_spatial_tables": 2,
        "function_names": ["ST_Crosses"],
        "signature_preference": "geometry",
        "required_geometry_kinds": ["line", "polygon"],
        "query_shape": "Join two geometries with ST_Crosses when the relation is crossing rather than containment or overlap.",
        "constraints": [
            "Use ST_Crosses as the main spatial predicate for the join.",
            "Do not replace ST_Crosses with ST_Intersects or ST_Overlaps in this profile.",
        ],
    },
    {
        "profile_id": "repair_valid_intersection_tail",
        "min_difficulty": "hard",
        "min_spatial_tables": 2,
        "function_names": ["ST_MakeValid", "ST_Intersection", "ST_Area", "ST_IsValid"],
        "signature_preference": "geometry",
        "required_geometry_kinds": ["polygon"],
        "query_shape": "Repair polygon geometry with ST_MakeValid before an overlap area calculation.",
        "constraints": [
            "Use ST_MakeValid on at least one schema-listed geometry before ST_Intersection or ST_Area.",
            "Keep ST_IsValid filters when they fit the sampled join path.",
        ],
    },
    {
        "profile_id": "perimeter_measurement_tail",
        "min_difficulty": "easy",
        "min_spatial_tables": 1,
        "function_names": ["ST_Perimeter"],
        "signature_preference": "geometry",
        "required_geometry_kinds": ["polygon"],
        "query_shape": "Measure polygon perimeter as a scalar result or grouped average.",
        "constraints": [
            "Use ST_Perimeter on a polygon geometry column or geography cast only when perimeter is the intended metric.",
            "Apply /1000 scaling only when the output is explicitly in kilometers.",
        ],
    },
]


def benchmark_function_name_allowlist() -> tuple[str, ...]:
    return CORE_BENCHMARK_FUNCTION_NAMES


def benchmark_tail_function_name_allowlist() -> tuple[str, ...]:
    return TAIL_BENCHMARK_FUNCTION_NAMES


def select_error_coverage_profile(
    *,
    database: SynthesizedSpatialDatabase,
    sample_index: int,
    difficulty_level: str,
    database_runtime_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Select a deterministic coverage profile compatible with the prompt schema."""

    spatial_table_count = sum(1 for table in database.selected_tables if table.spatial_fields)
    total_table_count = len(database.selected_tables)
    difficulty_rank = DIFFICULTY_RANK.get(difficulty_level, 0)
    geometry_kinds = _extract_geometry_kinds(
        database=database,
        database_runtime_metadata=database_runtime_metadata,
    )
    core_eligible: list[dict[str, Any]] = []
    for profile in CORE_ERROR_COVERAGE_PROFILES:
        min_difficulty = to_text(profile.get("min_difficulty")) or "easy"
        if difficulty_rank < DIFFICULTY_RANK.get(min_difficulty, 0):
            continue
        if spatial_table_count < int(profile.get("min_spatial_tables") or 1):
            continue
        if total_table_count < int(profile.get("min_tables") or 1):
            continue
        if not _profile_supports_geometry_kinds(profile, geometry_kinds):
            continue
        core_eligible.append(profile)
    tail_eligible: list[dict[str, Any]] = []
    for profile in TAIL_BENCHMARK_PROFILES:
        min_difficulty = to_text(profile.get("min_difficulty")) or "easy"
        if difficulty_rank < DIFFICULTY_RANK.get(min_difficulty, 0):
            continue
        if spatial_table_count < int(profile.get("min_spatial_tables") or 1):
            continue
        if total_table_count < int(profile.get("min_tables") or 1):
            continue
        if not _profile_supports_geometry_kinds(profile, geometry_kinds):
            continue
        tail_eligible.append(profile)
    if not core_eligible and not tail_eligible:
        return None
    tail_turn_index = ((int(sample_index) + 1) // TAIL_PROFILE_INTERVAL) - 1
    tail_turn = tail_eligible and ((int(sample_index) + 1) % TAIL_PROFILE_INTERVAL == 0)
    if tail_turn:
        point_profile = None
        weighted_seed = _stable_weighted_seed(
            database.database_id,
            difficulty_level,
            ",".join(sorted(geometry_kinds)),
        )
        specific_geometry_kinds = geometry_kinds & {"point", "line", "polygon", "collection"}
        if "point" in geometry_kinds:
            point_profile = next(
                (
                    profile
                    for profile in tail_eligible
                    if to_text(profile.get("profile_id")) == "point_coordinate_accessor_tail"
                ),
                None,
            )
        point_only_geometry = "point" in specific_geometry_kinds and len(specific_geometry_kinds) == 1
        if point_profile is not None and (point_only_geometry or weighted_seed % 5 in {1, 3}):
            selected = _specialize_tail_profile(
                point_profile,
                rotation_index=tail_turn_index,
                database_id=database.database_id,
                difficulty_level=difficulty_level,
                geometry_kinds=geometry_kinds,
            )
        else:
            collection_profile = next(
                (
                    profile
                    for profile in tail_eligible
                    if to_text(profile.get("profile_id")) == "collection_union_tail"
                ),
                None,
            )
            if collection_profile is not None and "point" not in geometry_kinds and weighted_seed % 5 == 2:
                selected = _specialize_tail_profile(
                    collection_profile,
                    rotation_index=tail_turn_index,
                    database_id=database.database_id,
                    difficulty_level=difficulty_level,
                    geometry_kinds=geometry_kinds,
                )
            else:
                tail_index = (
                    tail_turn_index
                    + _stable_text_seed(
                        database.database_id,
                        difficulty_level,
                        ",".join(sorted(geometry_kinds)),
                    )
                ) % len(tail_eligible)
                selected = _specialize_tail_profile(
                    tail_eligible[tail_index],
                    rotation_index=tail_turn_index,
                    database_id=database.database_id,
                    difficulty_level=difficulty_level,
                    geometry_kinds=geometry_kinds,
                )
    elif core_eligible:
        selected = core_eligible[int(sample_index) % len(core_eligible)]
    else:
        selected = _specialize_tail_profile(
            tail_eligible[int(sample_index) % len(tail_eligible)],
            rotation_index=max(0, tail_turn_index),
            database_id=database.database_id,
            difficulty_level=difficulty_level,
            geometry_kinds=geometry_kinds,
        )
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
    """Blend benchmark/error coverage with core benchmark ST function diversity."""

    base_functions = _dedupe_functions_by_name(sampled_functions)
    if not coverage_profile:
        return base_functions

    profile_functions: list[PostGISFunction] = []
    seen: set[str] = set()
    signature_preference = to_text(coverage_profile.get("signature_preference"))
    prioritize_profile_functions = bool(coverage_profile.get("prioritize_profile_functions"))
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
    if prioritize_profile_functions:
        _append_unique_functions(
            merged,
            profile_functions,
            max_total_functions=max_total_functions,
        )
        _append_unique_functions(
            merged,
            [item for item in base_functions if item.function_name.lower() not in profile_names],
            max_total_functions=max_total_functions,
        )
    else:
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
            include_function_names=benchmark_function_name_allowlist(),
            desired_count=max(0, int(min_general_functions) - general_function_count),
        )
        _append_unique_functions(
            merged,
            supplemental,
            max_total_functions=max_total_functions,
        )

    return merged or profile_functions or base_functions


def _extract_geometry_kinds(
    *,
    database: SynthesizedSpatialDatabase,
    database_runtime_metadata: Mapping[str, Any] | None = None,
) -> set[str]:
    kinds: set[str] = set()
    if isinstance(database_runtime_metadata, Mapping):
        for table_meta in database_runtime_metadata.get("tables", []) or []:
            if not isinstance(table_meta, Mapping):
                continue
            for spatial_field in table_meta.get("spatial_fields", []) or []:
                if not isinstance(spatial_field, Mapping):
                    continue
                _add_geometry_kind_hints(
                    kinds,
                    to_text(spatial_field.get("geometry_type")),
                    to_text(spatial_field.get("column_type")),
                    to_text(spatial_field.get("spatial_type")),
                )
            representative_rows = table_meta.get("representative_values") or []
            if isinstance(representative_rows, list):
                for row in representative_rows:
                    if not isinstance(row, Mapping):
                        continue
                    for value in row.values():
                        _add_geometry_kind_hints(kinds, to_text(value))
    for table in database.selected_tables:
        for spatial_field in getattr(table, "spatial_fields", []) or []:
            if not isinstance(spatial_field, Mapping):
                continue
            _add_geometry_kind_hints(
                kinds,
                to_text(spatial_field.get("geometry_type")),
                to_text(spatial_field.get("column_type")),
                to_text(spatial_field.get("crs")),
            )
    return kinds


def _add_geometry_kind_hints(kinds: set[str], *texts: str) -> None:
    combined = " ".join(text for text in texts if text).lower()
    if not combined:
        return
    if "point" in combined:
        kinds.add("point")
    if "line" in combined or "curve" in combined:
        kinds.add("line")
    if "polygon" in combined or "surface" in combined:
        kinds.add("polygon")
    if "collection" in combined:
        kinds.add("collection")
    if "geometry" in combined or "geography" in combined:
        kinds.add("geometry")


def _specialize_tail_profile(
    profile: Mapping[str, Any],
    *,
    rotation_index: int,
    database_id: str,
    difficulty_level: str,
    geometry_kinds: set[str],
) -> dict[str, Any]:
    specialized = dict(stable_jsonify(profile))
    specialized["prioritize_profile_functions"] = True
    rotation_groups = stable_jsonify(specialized.get("rotation_function_groups")) or []
    if not rotation_groups:
        return specialized
    rotation_seed_mode = to_text(specialized.get("rotation_seed_mode")).lower()
    if rotation_seed_mode == "none":
        rotation_offset = 0
    elif to_text(profile.get("profile_id")) == "point_coordinate_accessor_tail":
        rotation_offset = _stable_crc_seed(
            database_id,
            to_text(profile.get("profile_id")),
            difficulty_level,
            ",".join(sorted(geometry_kinds)),
        )
    elif rotation_seed_mode == "weighted":
        rotation_offset = _stable_weighted_seed(
            database_id,
            to_text(profile.get("profile_id")),
            difficulty_level,
            ",".join(sorted(geometry_kinds)),
        )
    else:
        rotation_offset = _stable_text_seed(
            database_id,
            to_text(profile.get("profile_id")),
            difficulty_level,
            ",".join(sorted(geometry_kinds)),
        )
    chosen_group = stable_jsonify(
        rotation_groups[(int(rotation_index) + rotation_offset) % len(rotation_groups)]
    ) or []
    chosen_names = [to_text(item) for item in chosen_group if to_text(item)]
    if not chosen_names:
        return specialized
    specialized["function_names"] = chosen_names
    specialized["required_exact_function_names"] = chosen_names
    constraints = list(stable_jsonify(specialized.get("constraints")) or [])
    constraints.append(
        "Use the exact target tail function(s) for this sample: " + ", ".join(chosen_names) + "."
    )
    specialized["constraints"] = constraints
    return specialized


def _stable_text_seed(*parts: str) -> int:
    seed_text = "|".join(to_text(part) for part in parts if to_text(part))
    if not seed_text:
        return 0
    return sum(ord(char) for char in seed_text)


def _stable_weighted_seed(*parts: str) -> int:
    seed_text = "|".join(to_text(part) for part in parts if to_text(part))
    if not seed_text:
        return 0
    value = 0
    for index, char in enumerate(seed_text, start=1):
        value = (value * 131 + ord(char) + index) % 2147483647
    return value


def _stable_crc_seed(*parts: str) -> int:
    seed_text = "|".join(to_text(part) for part in parts if to_text(part))
    if not seed_text:
        return 0
    return zlib.crc32(seed_text.encode("utf-8"))


def _profile_supports_geometry_kinds(profile: Mapping[str, Any], geometry_kinds: set[str]) -> bool:
    required = {
        to_text(item).lower()
        for item in stable_jsonify(profile.get("required_geometry_kinds")) or []
        if to_text(item)
    }
    if not required or not geometry_kinds:
        return True
    specific_kinds = geometry_kinds & {"point", "line", "polygon", "collection"}
    if specific_kinds:
        return bool(required & specific_kinds)
    if required == {"geometry"}:
        return "geometry" in geometry_kinds
    return "geometry" in geometry_kinds and not specific_kinds


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

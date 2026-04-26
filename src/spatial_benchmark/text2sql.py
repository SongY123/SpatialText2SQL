from __future__ import annotations

from .scenario_specs import SCENARIO_DATABASE_SPECS


SAMPLE_VALUE_POOLS = {
    "traffic_mobility": {"polygon_value": "'BX206'", "point_value": "101", "radius_meters": "500", "date_value": "DATE '2024-01-01'"},
    "emergency_response": {"polygon_value": "'Hurricane Evacuation Zone 1'", "point_value": "201", "radius_meters": "800", "date_value": "DATE '2024-06-01'"},
    "public_service_accessibility": {"polygon_value": "'District 15'", "point_value": "301", "radius_meters": "600", "date_value": "DATE '2024-09-01'"},
    "environmental_resilience": {"polygon_value": "'100-year floodplain'", "point_value": "401", "radius_meters": "1000", "date_value": "TIMESTAMP '2024-05-01 00:00:00'"},
    "urban_planning_land_use": {"polygon_value": "'Special Midtown District'", "point_value": "501", "radius_meters": "750", "date_value": "DATE '2025-01-01'"},
    "housing_demographics": {"polygon_value": "'36061000100'", "point_value": "601", "radius_meters": "900", "date_value": "DATE '2023-01-01'"},
    "parks_recreation_poi": {"polygon_value": "'Prospect Park'", "point_value": "701", "radius_meters": "700", "date_value": "DATE '2024-04-01'"},
}


def humanize(identifier: str) -> str:
    return identifier.replace("_", " ")


def build_query_templates() -> list[dict[str, str | list[str]]]:
    templates: list[dict[str, str | list[str]]] = []
    for spec in SCENARIO_DATABASE_SPECS:
        ctx = spec.query_context
        s = ctx["schema"]
        point_label = humanize(ctx["point_table"])
        polygon_label = humanize(ctx["polygon_table"])
        distance_label = humanize(ctx["distance_table"])
        line_label = humanize(ctx["line_table"])
        templates.extend(
            [
                {
                    "database_id": spec.id,
                    "scenario": spec.label,
                    "query_type": "topological_within",
                    "difficulty": "medium",
                    "spatial_operations": ["ST_Within"],
                    "sql_template": f"SELECT p.{ctx['point_id']}, p.{ctx['point_name']}\nFROM {s}.{ctx['point_table']} p\nJOIN {s}.{ctx['polygon_table']} z ON ST_Within(p.geom, z.geom)\nWHERE z.{ctx['polygon_id']} = {{polygon_value}};",
                    "nl_question_template": f"Which {point_label} records are within the selected {polygon_label} {{polygon_value}}?",
                },
                {
                    "database_id": spec.id,
                    "scenario": spec.label,
                    "query_type": "distance_nearest_neighbor",
                    "difficulty": "medium",
                    "spatial_operations": ["ST_Distance"],
                    "sql_template": f"WITH anchor AS (\n  SELECT geom FROM {s}.{ctx['point_table']} WHERE {ctx['point_id']} = {{point_value}}\n)\nSELECT d.{ctx['distance_id']}, d.{ctx['distance_name']},\n       ST_Distance(d.geom::geography, anchor.geom::geography) AS distance_m\nFROM {s}.{ctx['distance_table']} d, anchor\nORDER BY d.geom <-> anchor.geom\nLIMIT 5;",
                    "nl_question_template": f"What are the five nearest {distance_label} records to {point_label} {{point_value}}?",
                },
                {
                    "database_id": spec.id,
                    "scenario": spec.label,
                    "query_type": "topological_intersects",
                    "difficulty": "hard",
                    "spatial_operations": ["ST_Intersects"],
                    "sql_template": f"SELECT l.{ctx['line_id']}, l.{ctx['line_name']}\nFROM {s}.{ctx['line_table']} l\nJOIN {s}.{ctx['polygon_table']} z ON ST_Intersects(l.geom, z.geom)\nWHERE z.{ctx['polygon_id']} = {{polygon_value}};",
                    "nl_question_template": f"Which {line_label} features intersect the selected {polygon_label} {{polygon_value}}?",
                },
            ]
        )
    return templates


def build_benchmark_samples() -> list[dict[str, str | list[str]]]:
    samples: list[dict[str, str | list[str]]] = []
    for spec in SCENARIO_DATABASE_SPECS:
        ctx = spec.query_context
        values = SAMPLE_VALUE_POOLS[spec.id]
        s = ctx["schema"]
        point_label = humanize(ctx["point_table"])
        polygon_label = humanize(ctx["polygon_table"])
        samples.extend(
            [
                {
                    "database_id": spec.id,
                    "scenario": spec.label,
                    "question": f"Which {point_label} records are located inside {polygon_label} {values['polygon_value'].strip(chr(39))}?",
                    "sql": f"SELECT p.{ctx['point_id']}, p.{ctx['point_name']}\nFROM {s}.{ctx['point_table']} p\nJOIN {s}.{ctx['polygon_table']} z ON ST_Within(p.geom, z.geom)\nWHERE z.{ctx['polygon_id']} = {values['polygon_value']};",
                    "spatial_operations": ["ST_Within"],
                    "difficulty": "medium",
                    "tables_used": [ctx["point_table"], ctx["polygon_table"]],
                },
                {
                    "database_id": spec.id,
                    "scenario": spec.label,
                    "question": f"What are the five nearest {humanize(ctx['distance_table'])} records to {point_label} ID {values['point_value']}?",
                    "sql": f"WITH anchor AS (\n  SELECT geom FROM {s}.{ctx['point_table']} WHERE {ctx['point_id']} = {values['point_value']}\n)\nSELECT d.{ctx['distance_id']}, d.{ctx['distance_name']},\n       ST_Distance(d.geom::geography, anchor.geom::geography) AS distance_m\nFROM {s}.{ctx['distance_table']} d, anchor\nORDER BY d.geom <-> anchor.geom\nLIMIT 5;",
                    "spatial_operations": ["ST_Distance"],
                    "difficulty": "medium",
                    "tables_used": [ctx["point_table"], ctx["distance_table"]],
                },
            ]
        )
    return samples

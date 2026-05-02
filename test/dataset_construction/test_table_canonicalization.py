import json
import tempfile
import unittest
from pathlib import Path

from src.dataset_construction.canonicalization import (
    canonicalize_metadata_file,
    canonicalize_tables,
    detect_coordinate_pairs,
    detect_geojson_geometry,
    detect_wkt_geometry,
    deduplicate_spatial_fields,
    infer_column_type,
    infer_crs,
    normalize_column_name,
    normalize_table_name,
)


class TableCanonicalizationTests(unittest.TestCase):
    def test_normalize_column_name_handles_reserved_digit_and_duplicates(self):
        existing = {"order_col", "col_2024_status"}
        self.assertEqual(normalize_column_name("Order", existing_names=existing, column_index=1), "order_col_2")
        self.assertEqual(normalize_column_name("2024 Status", existing_names=existing, column_index=2), "col_2024_status_2")
        self.assertEqual(normalize_column_name("%%% ", existing_names=set(), column_index=3), "col_3")
        self.assertEqual(normalize_column_name("Point__  X", existing_names=set(), column_index=4), "point_x")

    def test_normalize_table_name_uses_dataset_name_rules(self):
        self.assertEqual(
            normalize_table_name("NYC__  Hydrants (Citywide) 2024!!"),
            "nyc_hydrants_citywide_2024",
        )

    def test_infer_column_type_prefers_values(self):
        self.assertEqual(infer_column_type("number", ["1", "2", "3"])[0], "integer")
        self.assertEqual(infer_column_type("", ["1.5", "2", None])[0], "double")
        self.assertEqual(infer_column_type("", ["true", "false", "yes"])[0], "boolean")
        self.assertEqual(infer_column_type("", ["2024-05-01T10:30:00", "2024-05-02T11:00:00"])[0], "timestamp")
        self.assertEqual(infer_column_type("", [{"x": 1}, {"y": 2}])[0], "unk")

    def test_detect_geojson_geometry_uses_declared_geometry_field(self):
        raw_table = {
            "raw_schema": [{"name": "the_geom", "type": "geometry"}, {"name": "id", "type": "number"}],
            "rows": [{"id": 1}, {"id": 2}],
            "geojson_geometry": [
                {"type": "Point", "coordinates": [-73.9, 40.7]},
                {"type": "Point", "coordinates": [-73.8, 40.8]},
            ],
        }
        fields = detect_geojson_geometry(raw_table, raw_to_canonical_columns={"the_geom": "the_geom"})
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0].field_name, "the_geom")
        self.assertEqual(fields[0].source_kind, "geojson")

    def test_detect_wkt_geometry(self):
        raw_table = {
            "raw_schema": [{"name": "shape_wkt", "type": "text"}],
            "rows": [
                {"shape_wkt": "POINT(-73.9 40.7)"},
                {"shape_wkt": "POINT(-73.8 40.8)"},
            ],
        }
        fields = detect_wkt_geometry(raw_table, raw_to_canonical_columns={"shape_wkt": "shape_wkt"})
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0].field_name, "shape_wkt")

    def test_detect_coordinate_pairs(self):
        raw_table = {
            "raw_schema": [{"name": "longitude", "type": "number"}, {"name": "latitude", "type": "number"}],
            "rows": [
                {"longitude": -73.9, "latitude": 40.7},
                {"longitude": -73.8, "latitude": 40.8},
            ],
        }
        fields = detect_coordinate_pairs(raw_table)
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0].raw_columns, ("longitude", "latitude"))
        self.assertEqual(fields[0].source_kind, "coordinate_pair")

    def test_infer_crs_defaults_for_coordinate_pairs(self):
        coord_field = detect_coordinate_pairs(
            {
                "raw_schema": [{"name": "lon", "type": "number"}, {"name": "lat", "type": "number"}],
                "rows": [{"lon": -73.9, "lat": 40.7}],
            }
        )[0]
        self.assertEqual(infer_crs({"crs": None}, coord_field), "EPSG:4326")
        self.assertEqual(
            infer_crs({"crs": {"properties": {"name": "EPSG:3857"}}}, coord_field),
            "EPSG:3857",
        )

    def test_deduplicate_spatial_fields_prefers_geojson(self):
        raw_table = {
            "table_name": "hydrants",
            "source_city": "New York City",
            "source_description": "Hydrant assets",
            "raw_schema": [
                {"name": "the_geom", "type": "geometry"},
                {"name": "shape_wkt", "type": "text"},
                {"name": "longitude", "type": "number"},
                {"name": "latitude", "type": "number"},
            ],
            "rows": [
                {
                    "shape_wkt": "POINT(-73.9 40.7)",
                    "longitude": -73.9,
                    "latitude": 40.7,
                },
                {
                    "shape_wkt": "POINT(-73.8 40.8)",
                    "longitude": -73.8,
                    "latitude": 40.8,
                },
            ],
            "geojson_geometry": [
                {"type": "Point", "coordinates": [-73.9, 40.7]},
                {"type": "Point", "coordinates": [-73.8, 40.8]},
            ],
            "geojson_geometry_field_name": "the_geom",
        }
        canonical = canonicalize_tables([raw_table])[0]
        self.assertEqual([field["field_name"] for field in canonical["spatial_fields"]], ["the_geom"])

    def test_metadata_flow_drops_spatial_hint_columns_when_geometry_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_path = tmp_path / "sample.geojson"
            dataset_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "longitude": -73.9,
                                    "latitude": 40.7,
                                    "point_x": 100.0,
                                    "point_y": 200.0,
                                    "status": "active",
                                },
                                "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            metadata_path = tmp_path / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "New York City",
                            "city_id": "nyc",
                            "datasets": [
                                {
                                    "id": "hydrants",
                                    "name": "NYC Hydrants",
                                    "description": "Hydrant assets in the city.",
                                    "path": str(dataset_path),
                                    "columns": [
                                        {"name": "the_geom", "type": "geometry"},
                                        {"name": "longitude", "type": "number"},
                                        {"name": "latitude", "type": "number"},
                                        {"name": "point_x", "type": "number"},
                                        {"name": "point_y", "type": "number"},
                                        {"name": "status", "type": "text"},
                                    ],
                                }
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            output_path = canonicalize_metadata_file(metadata_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        dataset = payload[0]["datasets"][0]
        self.assertEqual(dataset["canonical_name"], "nyc_hydrants")
        self.assertEqual(
            [column["name"] for column in dataset["columns"]],
            ["the_geom", "status"],
        )
        self.assertEqual(
            [column["canonical_name"] for column in dataset["columns"]],
            ["the_geom", "status"],
        )
        self.assertTrue(all("nullable" not in column for column in dataset["columns"]))
        self.assertEqual(
            dataset["spatial_fields"],
            [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
        )

    def test_metadata_file_flow_preserves_original_metadata_and_writes_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dataset_path = tmp_path / "sample.geojson"
            dataset_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "longitude": -73.9,
                                    "latitude": 40.7,
                                    "status": "active",
                                    "ignored_extra": "should_not_be_used",
                                },
                                "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            metadata_path = tmp_path / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "New York City",
                            "city_id": "nyc",
                            "datasets": [
                                {
                                    "id": "hydrants",
                                    "name": "NYC Hydrants (Citywide)!!",
                                    "description": "Hydrant assets in the city.",
                                    "path": str(dataset_path),
                                    "columns": [
                                        {"name": "the_geom", "type": "geometry"},
                                        {"name": "longitude", "type": "number"},
                                        {"name": "latitude", "type": "number"},
                                        {"name": "status", "type": "text"},
                                    ],
                                }
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            output_path = canonicalize_metadata_file(metadata_path)
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertTrue(output_path.name.endswith("metadata_canonicalized.json"))
        dataset = payload[0]["datasets"][0]
        self.assertEqual(dataset["name"], "NYC Hydrants (Citywide)!!")
        self.assertNotIn("canonical_table", dataset)
        self.assertEqual(dataset["canonical_name"], "nyc_hydrants_citywide")
        self.assertEqual(
            dataset["spatial_fields"],
            [{"canonical_name": "the_geom", "crs": "EPSG:4326"}],
        )
        self.assertIn("semantic_summary", dataset)
        self.assertIn("themes", dataset)
        schema_names = [column["name"] for column in dataset["columns"]]
        self.assertNotIn("ignored_extra", schema_names)
        self.assertEqual(schema_names, ["the_geom", "status"])
        self.assertEqual(
            [column["canonical_name"] for column in dataset["columns"]],
            ["the_geom", "status"],
        )
        self.assertEqual(
            [column["canonical_type"] for column in dataset["columns"]],
            ["spatial", "text"],
        )
        self.assertTrue(all("nullable" not in column for column in dataset["columns"]))

    def test_metadata_file_flow_filters_selected_cities(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            nyc_path = tmp_path / "nyc.geojson"
            nyc_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"status": "active"},
                                "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            sf_path = tmp_path / "sf.geojson"
            sf_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"status": "open"},
                                "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            metadata_path = tmp_path / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "New York City",
                            "city_id": "nyc",
                            "datasets": [
                                {
                                    "id": "hydrants",
                                    "name": "NYC Hydrants",
                                    "path": str(nyc_path),
                                    "columns": [
                                        {"name": "the_geom", "type": "geometry"},
                                        {"name": "status", "type": "text"},
                                    ],
                                }
                            ],
                        },
                        {
                            "City": "San Francisco",
                            "city_id": "sf",
                            "datasets": [
                                {
                                    "id": "facilities",
                                    "name": "SF Facilities",
                                    "path": str(sf_path),
                                    "columns": [
                                        {"name": "the_geom", "type": "geometry"},
                                        {"name": "status", "type": "text"},
                                    ],
                                }
                            ],
                        },
                    ]
                ),
                encoding="utf-8",
            )

            output_path = canonicalize_metadata_file(metadata_path, selected_city_ids=["nyc"])
            payload = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["city_id"], "nyc")
        self.assertEqual(payload[0]["datasets"][0]["canonical_name"], "nyc_hydrants")


if __name__ == "__main__":
    unittest.main()

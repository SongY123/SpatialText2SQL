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

    def test_normalize_table_name_uses_dataset_name_rules(self):
        self.assertEqual(
            normalize_table_name("NYC Hydrants (Citywide) 2024!!"),
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
        self.assertIn("canonical_table", dataset)
        self.assertEqual(dataset["canonical_table"]["table_name"], "nyc_hydrants_citywide")
        self.assertNotIn("raw_to_canonical_columns", dataset["canonical_table"])
        self.assertEqual(
            [field["field_name"] for field in dataset["canonical_table"]["spatial_fields"]],
            ["the_geom"],
        )
        schema_names = [column["raw_name"] for column in dataset["canonical_table"]["schema"]]
        self.assertNotIn("ignored_extra", schema_names)


if __name__ == "__main__":
    unittest.main()

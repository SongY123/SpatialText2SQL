import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.utils.geojson2db import geojson2db
from src.utils import logging_config as utils_logging_config
from src.utils.pbf2db import pbf2db
from src.utils.shp2db import (
    PostGISImporter,
    SpatiaLiteImporter,
    get_importer,
    shp2db,
)


class SpatialImportUtilsTests(unittest.TestCase):
    def test_logging_wrappers_preserve_expected_logger_exports(self):
        from src import logging_config as src_logging_config
        from src.postgis_docs_parse import logging_config as postgis_logging_config

        self.assertIs(src_logging_config.setup_logging, utils_logging_config.setup_logging)
        self.assertEqual(src_logging_config.logger.name, "src")
        self.assertEqual(postgis_logging_config.logger.name, "spatial_importer")
        self.assertEqual(postgis_logging_config.pbf_logger.name, "osm_pbf_importer")

    def test_get_importer_selects_supported_backends(self):
        self.assertIsInstance(
            get_importer("postgresql://user:pw@localhost:5432/db"),
            PostGISImporter,
        )
        self.assertIsInstance(
            get_importer("sqlite:////tmp/demo.sqlite"),
            SpatiaLiteImporter,
        )

    def test_shp2db_scans_directories_recursively(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nested = root / "nested"
            nested.mkdir()
            (root / "roads.shp").write_text("", encoding="utf-8")
            (nested / "parks.SHP").write_text("", encoding="utf-8")
            (nested / "ignore.geojson").write_text("{}", encoding="utf-8")

            with (
                patch("src.utils.shp2db.get_importer", return_value=object()),
                patch("src.utils.shp2db._process_and_import") as process_mock,
            ):
                shp2db(tmpdir, "sqlite:////tmp/test.sqlite")

            processed = [(Path(call.args[0]).name, call.args[2]) for call in process_mock.call_args_list]
            self.assertEqual(processed, [("parks.SHP", "parks"), ("roads.shp", "roads")])

    def test_geojson2db_uses_explicit_table_name_for_single_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "districts.geojson"
            path.write_text("{}", encoding="utf-8")

            with (
                patch("src.utils.geojson2db.get_importer", return_value=object()),
                patch("src.utils.geojson2db._process_and_import") as process_mock,
            ):
                geojson2db(
                    str(path),
                    "postgresql://user:pw@localhost:5432/db",
                    table_name="custom_table",
                )

            process_mock.assert_called_once_with(
                str(path),
                unittest.mock.ANY,
                "custom_table",
                None,
                "replace",
            )

    def test_geojson2db_scans_geojson_and_json_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nested = root / "nested"
            nested.mkdir()
            (root / "buildings.geojson").write_text("{}", encoding="utf-8")
            (nested / "parks.json").write_text("{}", encoding="utf-8")
            (nested / "ignore.shp").write_text("", encoding="utf-8")

            with (
                patch("src.utils.geojson2db.get_importer", return_value=object()),
                patch("src.utils.geojson2db._process_and_import") as process_mock,
            ):
                geojson2db(tmpdir, "sqlite:////tmp/test.sqlite")

            processed = [(os.path.basename(call.args[0]), call.args[2]) for call in process_mock.call_args_list]
            self.assertEqual(processed, [("buildings.geojson", "buildings"), ("parks.json", "parks")])

    def test_pbf2db_scans_directories_recursively(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nested = root / "nested"
            nested.mkdir()
            (root / "city.pbf").write_text("", encoding="utf-8")
            (nested / "region.PBF").write_text("", encoding="utf-8")
            (nested / "ignore.geojson").write_text("{}", encoding="utf-8")

            fake_gdf = object()
            importer = unittest.mock.Mock()

            with (
                patch("src.utils.pbf2db.get_importer", return_value=importer),
                patch(
                    "src.utils.pbf2db.read_pbf_layers",
                    return_value=[("points", fake_gdf), ("lines", fake_gdf)],
                ),
                patch("src.utils.pbf2db.normalize_geodataframe", side_effect=lambda gdf: gdf),
            ):
                pbf2db(tmpdir, "sqlite:////tmp/test.sqlite")

            calls = [
                (call.args[1], call.args[2], call.args[3])
                for call in importer.write.call_args_list
            ]
            self.assertEqual(
                calls,
                [
                    ("city_points", None, "replace"),
                    ("city_lines", None, "replace"),
                    ("region_points", None, "replace"),
                    ("region_lines", None, "replace"),
                ],
            )


if __name__ == "__main__":
    unittest.main()

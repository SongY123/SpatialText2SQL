import tempfile
import threading
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.dataset_construction.crawl import cli as crawl_cli
from src.dataset_construction.crawl.ckan import (
    CkanGeoJsonCrawler,
    extract_arcgis_columns,
    pick_geojson_resource,
)
from src.dataset_construction.crawl.common import (
    _normalize_browser_redirect_url,
    default_headers,
    download_geojson,
    make_geojson_filename,
    sanitize_filename,
)
from src.dataset_construction.crawl.metadata import (
    analyze_geojson_file,
    build_city_metadata,
    build_column_type_metadata,
)
from src.dataset_construction.crawl.profiles import (
    CITY_PROFILES,
    DEFAULT_CITY_ORDER,
    parse_city_list,
    should_skip_dataset_name,
)
from src.dataset_construction.crawl.socrata import (
    SocrataMapCrawler,
    build_export_candidate_ids,
    extract_last_updated,
    extract_tags,
    extract_view_columns,
    extract_view_tags,
    extract_views,
)
from src.spatial_benchmark.clustering import load_metadata


class TestOpenDataCrawlHelpers(unittest.TestCase):
    def test_profiles_cover_all_seven_cities(self):
        self.assertEqual(len(DEFAULT_CITY_ORDER), 7)
        self.assertEqual(
            [profile.city_id for profile in parse_city_list("all")],
            list(DEFAULT_CITY_ORDER),
        )
        self.assertEqual(
            [profile.city_id for profile in parse_city_list("nyc,boston")],
            ["nyc", "boston"],
        )
        self.assertEqual(CITY_PROFILES["boston"].skip_dataset_names, ("2011 Contours- 1ft",))
        self.assertTrue(should_skip_dataset_name(CITY_PROFILES["boston"], "  2011   contours- 1ft "))
        self.assertFalse(should_skip_dataset_name(CITY_PROFILES["chicago"], "One Foot Contours"))

    def test_extract_tags_merges_and_deduplicates(self):
        classification = {
            "domain_tags": ["parks", "open space"],
            "tags": ["parks", "recreation"],
        }
        self.assertEqual(
            extract_tags(classification),
            ["parks", "open space", "recreation"],
        )

    def test_extract_last_updated_prefers_updated_at(self):
        resource = {
            "updatedAt": "2026-04-01T00:00:00.000Z",
            "data_updated_at": "2026-03-31T00:00:00.000Z",
        }
        self.assertEqual(extract_last_updated(resource), "2026-04-01T00:00:00.000Z")

    def test_extract_view_tags_reads_socrata_view_metadata(self):
        view_metadata = {"tags": ["crime", "police", ""]}
        self.assertEqual(extract_view_tags(view_metadata), ["crime", "police"])

    def test_extract_view_columns_normalizes_socrata_metadata(self):
        view_metadata = {
            "columns": [
                {
                    "name": "LATITUDE",
                    "description": "Latitude coordinate",
                    "dataTypeName": "number",
                    "fieldName": "latitude",
                },
                {
                    "name": "LOCATION",
                    "description": "Point location",
                    "dataTypeName": "location",
                    "fieldName": "location",
                },
            ]
        }
        self.assertEqual(
            extract_view_columns(view_metadata),
            [
                {"name": "LATITUDE", "description": "Latitude coordinate", "type": "number"},
                {"name": "LOCATION", "description": "Point location", "type": "location"},
            ],
        )

    def test_extract_views_reads_total_page_views(self):
        resource = {"page_views": {"page_views_total": 1234}}
        self.assertEqual(extract_views(resource), 1234)

    def test_build_export_candidate_ids_preserves_priority(self):
        resource = {"parent_fxf": ["abcd-1234", {"id": "wxyz-9876"}]}
        view_metadata = {
            "tableId": "lmno-0001",
            "parentUid": "wxyz-9876",
            "query": {"originalViewId": "qrst-5555"},
        }
        self.assertEqual(
            build_export_candidate_ids("zzzz-9999", resource, view_metadata),
            ["abcd-1234", "wxyz-9876", "lmno-0001", "qrst-5555", "zzzz-9999"],
        )

    def test_socrata_existing_record_matches_export_candidate_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_root = Path(tmpdir)
            existing_path = out_root / "geojson" / "x2n5-8w5q.geojson"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_text("{}", encoding="utf-8")

            crawler = SocrataMapCrawler(
                CITY_PROFILES["chicago"],
                output_dir=out_root,
                existing_datasets={
                    "x2n5-8w5q": {
                        "id": "x2n5-8w5q",
                        "path": str(existing_path),
                        "geojson_path": str(existing_path),
                    }
                },
            )
            resource = {"id": "dfnk-7re6", "parent_fxf": ["x2n5-8w5q"]}

            matched = crawler._match_existing_record(resource)

        self.assertIsNotNone(matched)
        self.assertEqual(matched["id"], "x2n5-8w5q")

    def test_socrata_sample_does_not_backfill_past_existing_first_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_root = Path(tmpdir)
            existing_path = out_root / "geojson" / "x2n5-8w5q.geojson"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_text("{}", encoding="utf-8")
            download_calls: list[str] = []

            class FakeCrawler(SocrataMapCrawler):
                def __init__(self):
                    super().__init__(
                        CITY_PROFILES["chicago"],
                        output_dir=out_root,
                        existing_datasets={
                            "x2n5-8w5q": {
                                "id": "x2n5-8w5q",
                                "path": str(existing_path),
                                "geojson_path": str(existing_path),
                                "download_url": "https://example.com/existing.geojson",
                            }
                        },
                    )

                def iter_catalog_map_assets(self):
                    yield {"resource": {"id": "dfnk-7re6", "name": "Crimes - Map", "parent_fxf": ["x2n5-8w5q"]}}
                    yield {"resource": {"id": "cauq-8yn6", "name": "Boundaries - Community Areas - Map"}}

                def _hydrate_existing_record(self, existing_record, asset_payload):
                    return SimpleNamespace(
                        to_dict=lambda: {
                            "id": "x2n5-8w5q",
                            "name": "Crimes - Map",
                            "path": str(existing_path),
                            "geojson_path": str(existing_path),
                            "bytes": existing_path.stat().st_size,
                            "skipped_existing": True,
                        }
                    )

                def _process_asset(self, asset_payload):
                    download_calls.append(str((asset_payload.get("resource") or {}).get("id") or ""))
                    raise AssertionError("Should not download past the first sampled candidate when it already exists.")

            result = FakeCrawler().run(download_limit=1)

        self.assertEqual(download_calls, [])
        self.assertEqual(result["meta"]["downloaded_count"], 0)
        self.assertEqual(result["meta"]["skipped_existing_count"], 1)
        self.assertEqual([dataset["id"] for dataset in result["datasets"]], ["x2n5-8w5q"])

    def test_make_geojson_filename_is_stable(self):
        self.assertEqual(sanitize_filename("NYC Parks & Recreation Map"), "nyc_parks_recreation_map")
        self.assertEqual(
            make_geojson_filename("NYC Parks & Recreation Map", "abcd-1234"),
            "nyc_parks_recreation_map_abcd-1234.geojson",
        )

    def test_ckan_resource_selection_ignores_csv(self):
        dataset = {
            "resources": [
                {"id": "csv", "format": "CSV", "url": "https://example.com/data.csv"},
                {"id": "geo", "format": "GeoJSON", "url": "https://example.com/data.geojson"},
            ]
        }
        self.assertEqual(pick_geojson_resource(dataset)["id"], "geo")

    def test_extract_arcgis_columns_appends_geometry(self):
        layer_metadata = {
            "fields": [
                {"name": "OBJECTID", "alias": "Object ID", "type": "esriFieldTypeOID"},
                {"name": "SHAPE_Area", "alias": "Shape Area", "type": "esriFieldTypeDouble"},
            ],
            "geometryType": "esriGeometryPolygon",
        }
        self.assertEqual(
            extract_arcgis_columns(layer_metadata),
            [
                {"name": "OBJECTID", "description": "Object ID", "type": "esriFieldTypeOID"},
                {"name": "SHAPE_Area", "description": "Shape Area", "type": "esriFieldTypeDouble"},
                {"name": "geometry", "description": "Feature geometry", "type": "esriGeometryPolygon"},
            ],
        )

    def test_ckan_sample_uses_deterministic_sorted_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            download_order: list[str] = []

            class FakeCrawler(CkanGeoJsonCrawler):
                def __init__(self):
                    super().__init__(CITY_PROFILES["boston"], output_dir=Path(tmpdir))

                def iter_packages(self):
                    yield {
                        "id": "z-last",
                        "title": "Z Last",
                        "resources": [{"id": "zres", "format": "GeoJSON", "url": "https://example.com/z.geojson"}],
                    }
                    yield {
                        "id": "a-first",
                        "title": "A First",
                        "resources": [{"id": "ares", "format": "GeoJSON", "url": "https://example.com/a.geojson"}],
                    }

                def _download_dataset(self, dataset):
                    download_order.append(str(dataset["id"]))
                    path = self.geojson_dir / f'{dataset["id"]}.geojson'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("{}", encoding="utf-8")
                    return {
                        "city": self.profile.city_id,
                        "domain": self.profile.domain,
                        "id": dataset["id"],
                        "dataset_id": dataset["id"],
                        "resource_id": (dataset.get("resources") or [])[0].get("id") if (dataset.get("resources")) else "",
                        "name": dataset["title"],
                        "description": "",
                        "resource_name": "",
                        "portal_format_detected": "GeoJSON",
                        "download_format": "geojson",
                        "download_url": (dataset.get("resources") or [])[0].get("url") if (dataset.get("resources")) else "",
                        "source_link": self.profile.base_url,
                        "geojson_filename": path.name,
                        "path": str(path.resolve()),
                        "geojson_path": str(path.resolve()),
                        "bytes": 2,
                        "skipped_existing": False,
                    }

            result = FakeCrawler().run(download_limit=1)

        self.assertEqual(download_order, ["a-first"])
        self.assertEqual(result["meta"]["downloaded_count"], 1)
        self.assertEqual([dataset["id"] for dataset in result["datasets"]], ["a-first"])

    def test_ckan_skip_names_are_removed_before_sample_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            download_order: list[str] = []

            class FakeCrawler(CkanGeoJsonCrawler):
                def __init__(self):
                    super().__init__(CITY_PROFILES["boston"], output_dir=Path(tmpdir))

                def iter_packages(self):
                    yield {
                        "id": "one-foot-contours",
                        "title": "2011 Contours- 1ft",
                        "resources": [{"id": "cres", "format": "GeoJSON", "url": "https://example.com/c.geojson"}],
                    }
                    yield {
                        "id": "other",
                        "title": "Other",
                        "resources": [{"id": "ores", "format": "GeoJSON", "url": "https://example.com/o.geojson"}],
                    }

                def _download_dataset(self, dataset):
                    download_order.append(str(dataset["id"]))
                    path = self.geojson_dir / f'{dataset["id"]}.geojson'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("{}", encoding="utf-8")
                    return {
                        "city": self.profile.city_id,
                        "id": dataset["id"],
                        "dataset_id": dataset["id"],
                        "name": dataset["title"],
                        "download_format": "geojson",
                        "path": str(path.resolve()),
                        "geojson_path": str(path.resolve()),
                        "bytes": 2,
                        "skipped_existing": False,
                    }

            result = FakeCrawler().run(download_limit=1)

        self.assertEqual(download_order, ["other"])
        self.assertEqual(result["meta"]["downloaded_count"], 1)
        self.assertEqual(result["meta"]["skipped_name_count"], 1)

    def test_ckan_sample_does_not_backfill_past_existing_first_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_root = Path(tmpdir)
            existing_path = out_root / "geojson" / "a-first.geojson"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_text("{}", encoding="utf-8")

            download_order: list[str] = []

            class FakeCrawler(CkanGeoJsonCrawler):
                def __init__(self):
                    super().__init__(
                        CITY_PROFILES["boston"],
                        output_dir=out_root,
                        existing_datasets={
                            "a-first": {
                                "id": "a-first",
                                "path": str(existing_path),
                                "geojson_path": str(existing_path),
                            }
                        },
                    )

                def iter_packages(self):
                    yield {
                        "id": "z-last",
                        "title": "Z Last",
                        "resources": [{"id": "zres", "format": "GeoJSON", "url": "https://example.com/z.geojson"}],
                    }
                    yield {
                        "id": "a-first",
                        "title": "A First",
                        "resources": [{"id": "ares", "format": "GeoJSON", "url": "https://example.com/a.geojson"}],
                    }

                def _get_dataset_metadata(self, dataset):
                    return dict(dataset)

                def _download_dataset(self, dataset):
                    download_order.append(str(dataset["id"]))
                    self.fail("Should not download past the first sampled candidate when it already exists.")

            result = FakeCrawler().run(download_limit=1)

        self.assertEqual(download_order, [])
        self.assertEqual(result["meta"]["downloaded_count"], 0)
        self.assertEqual(result["meta"]["skipped_existing_count"], 1)
        self.assertEqual([dataset["id"] for dataset in result["datasets"]], ["a-first"])

    def test_ckan_existing_record_refreshes_tags_and_columns_from_package_show(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_root = Path(tmpdir)
            existing_path = out_root / "geojson" / "dataset-1.geojson"
            existing_path.parent.mkdir(parents=True, exist_ok=True)
            existing_path.write_text("{}", encoding="utf-8")
            schema_url = "https://services.arcgis.com/example/FeatureServer/0"

            class FakeCrawler(CkanGeoJsonCrawler):
                def __init__(self):
                    super().__init__(
                        CITY_PROFILES["boston"],
                        output_dir=out_root,
                        existing_datasets={
                            "dataset-1": {
                                "id": "dataset-1",
                                "path": str(existing_path),
                                "geojson_path": str(existing_path),
                            }
                        },
                    )

                def iter_packages(self):
                    yield {
                        "id": "dataset-1",
                        "name": "dataset-1",
                        "title": "Dataset 1",
                        "resources": [{"id": "geo", "format": "GeoJSON", "url": "https://example.com/data.geojson"}],
                    }

                def _get_dataset_metadata(self, dataset):
                    return {
                        "id": "dataset-1",
                        "name": "dataset-1",
                        "title": "Dataset 1",
                        "notes": "Dataset description",
                        "tags": [{"name": "flooding"}],
                        "resources": [
                            {"id": "geo", "format": "GeoJSON", "url": "https://example.com/data.geojson"},
                            {
                                "id": "schema",
                                "format": "ArcGIS GeoServices REST API",
                                "url": schema_url,
                            },
                        ],
                    }

                def _get_layer_metadata(self, url):
                    if url != schema_url:
                        raise AssertionError(url)
                    return {
                        "fields": [
                            {"name": "OBJECTID", "alias": "Object ID", "type": "esriFieldTypeOID"},
                        ],
                        "geometryType": "esriGeometryPolygon",
                    }

            result = FakeCrawler().run(download_limit=1)

        self.assertEqual(result["meta"]["downloaded_count"], 0)
        self.assertEqual(result["meta"]["skipped_existing_count"], 1)
        record = result["datasets"][0]
        self.assertEqual(record["tags"], [{"name": "flooding"}])
        self.assertEqual(record["columns"], [{"name": "OBJECTID", "description": "Object ID", "type": "esriFieldTypeOID"}, {"name": "geometry", "description": "Feature geometry", "type": "esriGeometryPolygon"}])
        self.assertEqual(record["source_link"], schema_url)

    def test_download_headers_are_browser_like_for_ckan_redirects(self):
        headers = CkanGeoJsonCrawler._download_headers("https://data.boston.gov/dataset/example")
        self.assertIn("Mozilla/5.0", headers["User-Agent"])
        self.assertEqual(headers["Referer"], "https://data.boston.gov/dataset/example")
        self.assertNotIn("X-App-Token", headers)
        self.assertIn("text/html", headers["Accept"])
        self.assertEqual(headers["Upgrade-Insecure-Requests"], "1")

    def test_default_headers_use_browser_user_agent(self):
        self.assertIn("Mozilla/5.0", default_headers()["User-Agent"])

    def test_redirect_url_normalization_removes_default_ports(self):
        self.assertEqual(
            _normalize_browser_redirect_url("https://s3.amazonaws.com:443/bucket/file.geojson?sig=1"),
            "https://s3.amazonaws.com/bucket/file.geojson?sig=1",
        )
        self.assertEqual(
            _normalize_browser_redirect_url("http://example.com:80/path"),
            "http://example.com/path",
        )

    def test_download_geojson_streams_until_eof(self):
        class FakeResponse:
            headers = {"Content-Type": "application/json", "Content-Disposition": 'attachment; filename="x.geojson"'}

            def __init__(self):
                self._chunks = [
                    b'{"type":"FeatureCollection","features":',
                    b'[]} trailing bytes remain until eof',
                ]

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _size):
                if self._chunks:
                    return self._chunks.pop(0)
                return b""

        with tempfile.TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "x.geojson"
            with patch("src.dataset_construction.crawl.common._open_with_browser_redirects", return_value=FakeResponse()):
                nbytes, _ = download_geojson("https://example.com/x.geojson", dest)
            payload = dest.read_bytes()

        self.assertEqual(
            payload,
            b'{"type":"FeatureCollection","features":[]} trailing bytes remain until eof',
        )
        self.assertEqual(nbytes, len(payload))

    def test_clustering_metadata_loader_accepts_geojson_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = Path(tmpdir) / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "Chicago",
                            "city_id": "chicago",
                            "datasets": [
                                {
                                    "path": str(Path(tmpdir) / "geojson" / "parks_abcd-1234.geojson"),
                                    "name": "Parks",
                                }
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            metadata = load_metadata(metadata_path)
        self.assertEqual(metadata["parks_abcd-1234.geojson"]["name"], "Parks")

    def test_geojson_stats_count_geometry_and_spatial_properties(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "points.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "id": 1,
                                    "latitude": 40.0,
                                    "longitude": -73.0,
                                    "name": "A",
                                },
                                "geometry": {"type": "Point", "coordinates": [-73.0, 40.0]},
                            },
                            {
                                "type": "Feature",
                                "properties": {
                                    "id": 2,
                                    "latitude": 41.0,
                                    "longitude": -74.0,
                                    "name": "B",
                                },
                                "geometry": {"type": "Point", "coordinates": [-74.0, 41.0]},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stats = analyze_geojson_file(data_path)
        self.assertEqual(stats["row_count"], 2)
        self.assertEqual(stats["field_count"], 5)
        self.assertEqual(stats["spatial_field_count"], 3)

    def test_geojson_stats_counts_geometry_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "geometry_only.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {},
                                "geometry": {"type": "Point", "coordinates": [-73.0, 40.0]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stats = analyze_geojson_file(data_path)
        self.assertEqual(stats["row_count"], 1)
        self.assertEqual(stats["field_count"], 1)
        self.assertEqual(stats["spatial_field_count"], 1)

    def test_geojson_stats_detects_wkt_by_property_value(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "wkt_value.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "id": 1,
                                    "wkt_column": "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))",
                                    "name": "A",
                                },
                                "geometry": {"type": "Point", "coordinates": [-73.0, 40.0]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stats = analyze_geojson_file(data_path)
        self.assertEqual(stats["row_count"], 1)
        self.assertEqual(stats["field_count"], 4)
        self.assertEqual(stats["spatial_field_count"], 2)

    def test_geojson_stats_detects_wkt_value_with_srid_and_case(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "wkt_srid.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "id": 1,
                                    "geom_text": "srid=4326; point (-71.1 42.2)",
                                },
                                "geometry": {"type": "Point", "coordinates": [-71.1, 42.2]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stats = analyze_geojson_file(data_path)
        self.assertEqual(stats["row_count"], 1)
        self.assertEqual(stats["field_count"], 3)
        self.assertEqual(stats["spatial_field_count"], 2)

    def test_city_metadata_uses_required_summary_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "parks.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"name": "A"},
                                "geometry": {"type": "Polygon", "coordinates": []},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            city_meta = build_city_metadata(
                CITY_PROFILES["chicago"],
                {
                    "data_dir": tmpdir,
                    "datasets": [{"id": "abcd-1234", "name": "Parks", "path": str(data_path)}],
                    "errors": [],
                },
            )
        self.assertEqual(city_meta["City"], "Chicago")
        self.assertEqual(city_meta["#Table"], 1)
        self.assertEqual(city_meta["#Field/Table"], 2)
        self.assertEqual(city_meta["#Spatial Field/Table"], 1)
        self.assertEqual(city_meta["#Row/Table"], 1)

    def test_city_metadata_filters_columns_by_geojson_properties_case_insensitively(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "crimes.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"case number": "A123", "LOCATION": "POINT (-87.6 41.8)"},
                                "geometry": {"type": "Point", "coordinates": [-87.6, 41.8]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            columns = [
                {"name": "Case Number", "description": "Incident id", "type": "text"},
                {"name": "Location", "description": "Point location", "type": "location"},
                {"name": "the_geom", "description": "Geometry column", "type": "point"},
                {"name": "Latitude", "description": "Latitude", "type": "number"},
                {"name": "geometry", "description": "Feature geometry", "type": "esriGeometryPoint"},
            ]
            city_meta = build_city_metadata(
                CITY_PROFILES["chicago"],
                {
                    "data_dir": tmpdir,
                    "datasets": [
                        {
                            "id": "abcd-1234",
                            "name": "Crimes",
                            "path": str(data_path),
                            "source_link": "https://data.cityofchicago.org/api/views/abcd-1234.json",
                            "tags": [{"name": "crime"}, {"display_name": "police"}],
                            "columns": columns,
                        }
                    ],
                    "errors": [],
                },
            )

        self.assertEqual(city_meta["#Table"], 1)
        self.assertEqual(city_meta["#Field/Table"], 4)
        self.assertEqual(city_meta["#Spatial Field/Table"], 3)
        self.assertEqual(city_meta["#Row/Table"], 1)
        self.assertEqual(
            city_meta["datasets"][0]["columns"],
            [
                {"name": "Case Number", "description": "Incident id", "type": "text"},
                {"name": "Location", "description": "Point location", "type": "location"},
                {"name": "the_geom", "description": "Geometry column", "type": "point"},
                {"name": "geometry", "description": "Feature geometry", "type": "esriGeometryPoint"},
            ],
        )
        self.assertEqual(city_meta["datasets"][0]["tags"], ["crime", "police"])
        self.assertEqual(city_meta["datasets"][0]["field_count"], 4)
        self.assertEqual(city_meta["datasets"][0]["spatial_field_count"], 3)

    def test_city_metadata_adds_the_geom_when_geometry_column_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "zoning.geojson"
            data_path.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"ZONE": "R1"},
                                "geometry": {"type": "Polygon", "coordinates": []},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            city_meta = build_city_metadata(
                CITY_PROFILES["phoenix"],
                {
                    "data_dir": tmpdir,
                    "datasets": [
                        {
                            "id": "zoning",
                            "name": "Zoning",
                            "path": str(data_path),
                            "columns": [
                                {"name": "ZONE", "description": "Zone", "type": "text"},
                                {"name": "SHAPE", "description": "Shape", "type": "esriFieldTypeGeometry"},
                            ],
                        }
                    ],
                    "errors": [],
                },
            )

        self.assertEqual(
            city_meta["datasets"][0]["columns"],
            [
                {"name": "ZONE", "description": "Zone", "type": "text"},
                {"name": "the_geom", "description": "", "type": "polygon"},
            ],
        )
        self.assertEqual(city_meta["datasets"][0]["field_count"], 2)
        self.assertEqual(city_meta["datasets"][0]["spatial_field_count"], 1)
        self.assertEqual(city_meta["#Field/Table"], 2)
        self.assertEqual(city_meta["#Spatial Field/Table"], 1)

    def test_city_metadata_records_invalid_geojson_without_raising(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "broken.geojson"
            data_path.write_text('{"type":"FeatureCollection","features":[', encoding="utf-8")
            city_meta = build_city_metadata(
                CITY_PROFILES["boston"],
                {
                    "data_dir": tmpdir,
                    "datasets": [{"id": "broken", "name": "Broken", "path": str(data_path)}],
                    "errors": [],
                },
            )

        self.assertEqual(city_meta["#Table"], 0)
        self.assertEqual(city_meta["errors"][0]["error"], "invalid_geojson")
        self.assertEqual(city_meta["errors"][0]["name"], "Broken")

    def test_build_column_type_metadata_collects_unique_types_per_city(self):
        payload = [
            {
                "City": "Chicago",
                "city_id": "chicago",
                "datasets": [
                    {
                        "columns": [
                            {"name": "A", "description": "", "type": "text"},
                            {"name": "B", "description": "", "type": "location"},
                        ]
                    },
                    {
                        "columns": [
                            {"name": "C", "description": "", "type": "Text"},
                            {"name": "D", "description": "", "type": "number"},
                        ]
                    },
                ],
            }
        ]

        self.assertEqual(
            build_column_type_metadata(payload),
            [
                {
                    "City": "Chicago",
                    "city_id": "chicago",
                    "column_types": ["location", "number", "text"],
                }
            ],
        )

    def test_cli_appends_using_existing_metadata_and_writes_after_completion(self):
        events: list[tuple[str, str | int]] = []
        events_lock = threading.Lock()
        run_barrier = threading.Barrier(2)

        class FakeCrawler:
            def __init__(self, city_id: str, existing_datasets: dict[str, dict[str, object]]):
                self.city_id = city_id
                self.existing_datasets = existing_datasets

            def run(self, *, download_limit=None):
                with events_lock:
                    events.append(("run-start", self.city_id))
                run_barrier.wait(timeout=3)
                new_path = Path(tmpdir) / self.city_id / "geojson" / f"new-{self.city_id}.geojson"
                new_path.parent.mkdir(parents=True, exist_ok=True)
                new_path.write_text(
                    json.dumps(
                        {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "properties": {"name": self.city_id},
                                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                datasets = list(self.existing_datasets.values())
                datasets.append(
                    {
                        "id": f"new-{self.city_id}",
                        "name": f"new-{self.city_id}",
                        "path": str(new_path),
                        "row_count": 1,
                        "field_count": 2,
                        "spatial_field_count": 1,
                    }
                )
                with events_lock:
                    events.append(("run-finish", self.city_id))
                return {"meta": {"downloaded_count": 1, "error_count": 0}, "datasets": datasets, "errors": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            out_root = Path(tmpdir)
            boston_geojson = out_root / "boston" / "geojson" / "existing-boston.geojson"
            boston_geojson.parent.mkdir(parents=True, exist_ok=True)
            boston_geojson.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"name": "A"},
                                "geometry": {"type": "Point", "coordinates": [0, 0]},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            metadata_path = out_root / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    [
                        {
                            "City": "Boston",
                            "city_id": "boston",
                            "data_dir": str(out_root / "boston"),
                            "#Table": 1,
                            "#Field/Table": 2.0,
                            "#Spatial Field/Table": 1.0,
                            "#Row/Table": 1.0,
                            "datasets": [
                                {
                                    "id": "existing-boston",
                                    "name": "Existing Boston",
                                    "path": str(boston_geojson),
                                    "row_count": 1,
                                    "field_count": 2,
                                    "spatial_field_count": 1,
                                }
                            ],
                            "errors": [
                                {
                                    "id": "skipped-contour",
                                    "name": "2011 Contours- 1ft",
                                    "error": "invalid_geojson",
                                }
                            ],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            original_write = crawl_cli.write_root_metadata

            def fake_build_crawler(profile, args, *, existing_datasets=None):
                with events_lock:
                    events.append(("build", profile.city_id))
                if profile.city_id == "boston":
                    self.assertIn("existing-boston", existing_datasets)
                else:
                    self.assertEqual(existing_datasets, {})
                return FakeCrawler(profile.city_id, existing_datasets or {})

            def wrapped_write(path, city_metadata):
                with events_lock:
                    events.append(("write", len(city_metadata)))
                return original_write(path, city_metadata)

            args = crawl_cli.build_argument_parser().parse_args(
                ["--cities", "boston,chicago", "--out-root", str(out_root)]
            )
            with patch.object(crawl_cli, "_build_crawler", side_effect=fake_build_crawler), patch.object(
                crawl_cli, "write_root_metadata", side_effect=wrapped_write
            ):
                summary = crawl_cli.run(args)

            written = json.loads(metadata_path.read_text(encoding="utf-8"))
            column_type_written = json.loads((out_root / "columntype.json").read_text(encoding="utf-8"))

        self.assertTrue(summary["metadata_written"])
        self.assertEqual(events[-1][0], "write")
        self.assertCountEqual(
            [event for event in events if event[0] == "build"],
            [("build", "boston"), ("build", "chicago")],
        )
        self.assertCountEqual(
            [event for event in events if event[0] == "run-start"],
            [("run-start", "boston"), ("run-start", "chicago")],
        )
        first_finish = min(index for index, event in enumerate(events) if event[0] == "run-finish")
        run_start_positions = [index for index, event in enumerate(events) if event[0] == "run-start"]
        self.assertTrue(all(index < first_finish for index in run_start_positions))
        self.assertEqual(summary["cities"]["boston"]["status"], "ok")
        self.assertEqual(summary["cities"]["chicago"]["status"], "ok")
        self.assertEqual(len(written), 2)
        written_by_city = {entry["city_id"]: entry for entry in written}
        self.assertEqual(set(written_by_city), {"boston", "chicago"})
        self.assertEqual(len(written_by_city["boston"]["datasets"]), 2)
        self.assertEqual(written_by_city["boston"]["errors"], [])
        self.assertEqual(len(written_by_city["chicago"]["datasets"]), 1)
        self.assertEqual(len(column_type_written), 2)
        self.assertEqual(summary["columntype"], str((out_root / "columntype.json").resolve()))


if __name__ == "__main__":
    unittest.main()

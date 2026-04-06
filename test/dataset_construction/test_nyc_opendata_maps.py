import json
import tempfile
import unittest

from src.dataset_construction.crawl.nyc_opendata_maps import (
    NycOpenDataMapRecord,
    NycOpenDataMapsCrawler,
    build_export_candidate_ids,
    extract_last_updated,
    extract_tags,
    extract_views,
    make_csv_filename,
    sanitize_filename,
)


class TestNycOpenDataMapsHelpers(unittest.TestCase):
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

    def test_make_csv_filename_is_stable(self):
        self.assertEqual(sanitize_filename("NYC Parks & Recreation Map"), "nyc_parks_recreation_map")
        self.assertEqual(
            make_csv_filename("NYC Parks & Recreation Map", "abcd-1234"),
            "nyc_parks_recreation_map_abcd-1234.csv",
        )

    def test_manifest_append_and_resume_loading(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            crawler = NycOpenDataMapsCrawler(output_dir=tmpdir, expected_count=None)
            crawler._prepare_manifest_file({})

            first = NycOpenDataMapRecord(
                asset_id="aaaa-1111",
                csv_view_id="bbbb-2222",
                name="Dataset One",
                description="desc1",
                tags=["tag1"],
                last_updated="2026-04-01T00:00:00.000Z",
                views=10,
                asset_url="https://example.com/1",
                csv_download_url="https://example.com/1.csv",
                csv_filename="dataset_one_bbbb-2222.csv",
            )
            second = NycOpenDataMapRecord(
                asset_id="cccc-3333",
                csv_view_id="dddd-4444",
                name="Dataset Two",
                description="desc2",
                tags=["tag2"],
                last_updated="2026-04-02T00:00:00.000Z",
                views=20,
                asset_url="https://example.com/2",
                csv_download_url="https://example.com/2.csv",
                csv_filename="dataset_two_dddd-4444.csv",
            )

            crawler._append_manifest_record(first)
            crawler._append_manifest_record(second)

            loaded = crawler._load_existing_records()
            self.assertEqual(set(loaded), {"aaaa-1111", "cccc-3333"})

            payload = json.loads(crawler.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(payload), 2)
            self.assertEqual(payload[0]["asset_id"], "aaaa-1111")
            self.assertEqual(payload[1]["asset_id"], "cccc-3333")

    def test_prepare_manifest_file_migrates_old_object_shape(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            crawler = NycOpenDataMapsCrawler(output_dir=tmpdir, expected_count=None)
            crawler.manifest_path.write_text(
                json.dumps(
                    {
                        "record_count": 1,
                        "records": [
                            {
                                "asset_id": "eeee-5555",
                                "csv_view_id": "ffff-6666",
                                "name": "Old Shape",
                                "description": "",
                                "tags": [],
                                "last_updated": None,
                                "views": None,
                                "asset_url": None,
                                "csv_download_url": "https://example.com/old.csv",
                                "csv_filename": "old_shape_ffff-6666.csv",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            existing = crawler._load_existing_records()
            crawler._prepare_manifest_file(existing)

            payload = json.loads(crawler.manifest_path.read_text(encoding="utf-8"))
            self.assertIsInstance(payload, list)
            self.assertEqual(payload[0]["asset_id"], "eeee-5555")


if __name__ == "__main__":
    unittest.main()

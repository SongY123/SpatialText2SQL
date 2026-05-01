import unittest
import os

from src.postgis_docs_parse.postgis_doc_extract import PostGISFormalParser
from src.postgis_docs_parse.validate_postgis import PostGISValidator


class MissingTableLogicTests(unittest.TestCase):
    def test_extract_normalize_missing_tables_compat_list_str(self):
        parser = PostGISFormalParser()
        mts = parser._normalize_missing_tables(["nyc_streets", "public.roads"])
        self.assertEqual(len(mts), 2)
        self.assertEqual(mts[0]["table"], "nyc_streets")
        self.assertIsNone(mts[0]["missing_type"])
        self.assertIn("table_features", mts[0])

    def test_extract_enrich_cross_example_deps_same_function(self):
        parser = PostGISFormalParser()
        dataset = [
            {
                "function_id": "FUNC_A",
                "examples": [
                    {
                        "example_id": 1,
                        "steps": [
                            {"step_id": 1, "sql": "CREATE TABLE t1(id int);", "missing_tables": []}
                        ],
                    },
                    {
                        "example_id": 2,
                        "steps": [
                            {
                                "step_id": 1,
                                "sql": "SELECT * FROM t1;",
                                "missing_tables": [{"table": "t1", "missing_type": "cross_example"}],
                            }
                        ],
                    },
                ],
            }
        ]
        parser._enrich_cross_example_deps(dataset)
        mt = dataset[0]["examples"][1]["steps"][0]["missing_tables"][0]
        self.assertEqual(mt["dep_scope"], "same_func_dep")
        self.assertEqual(mt["dep_example_id"], 1)
        self.assertIsNone(mt["dep_function_id"])

    def test_extract_enrich_cross_example_deps_cross_function(self):
        parser = PostGISFormalParser()
        dataset = [
            {
                "function_id": "FUNC_B",
                "examples": [
                    {
                        "example_id": 1,
                        "steps": [
                            {"step_id": 1, "sql": "CREATE TABLE t2(id int);", "missing_tables": []}
                        ],
                    }
                ],
            },
            {
                "function_id": "FUNC_A",
                "examples": [
                    {
                        "example_id": 1,
                        "steps": [
                            {
                                "step_id": 1,
                                "sql": "SELECT * FROM t2;",
                                "missing_tables": [{"table": "t2", "missing_type": "external"}],
                            }
                        ],
                    }
                ],
            },
        ]
        parser._enrich_cross_example_deps(dataset)
        mt = dataset[1]["examples"][0]["steps"][0]["missing_tables"][0]
        self.assertEqual(mt["missing_type"], "cross_example")
        self.assertEqual(mt["dep_scope"], "cross_func_dep")
        self.assertEqual(mt["dep_function_id"], "FUNC_B")
        self.assertEqual(mt["dep_example_id"], 1)

    def test_validator_extract_missing_table_name(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        name = v._extract_missing_table_name('relation "public.roads" does not exist')
        self.assertEqual(name, "public.roads")
        name_cn = v._extract_missing_table_name('错误:  关系 "sometable" 不存在')
        self.assertEqual(name_cn, "sometable")

    def test_validator_find_create_table_in_steps(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        steps = [
            {"sql": "CREATE TABLE t3(id int);"},
            {"sql": "SELECT * FROM t3;"},
        ]
        ddl = v._find_create_table_sql_in_steps(steps, "t3", before_step_index=2)
        self.assertEqual(ddl, "CREATE TABLE t3(id int);")

    def test_validator_preprocess_sql_cast_wkt_to_geometry(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        sql, meta = v._preprocess_sql_for_validation("SELECT ST_XMax('LINESTRING(1 3, 5 6)');")
        self.assertIn("::geometry", sql)
        self.assertTrue(meta.get("sql_rewritten"))

    def test_validator_preprocess_sql_force3dz_rename(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        sql, meta = v._preprocess_sql_for_validation("SELECT ST_Force_3DZ(geom) FROM t;")
        self.assertIn("ST_Force3DZ", sql)
        self.assertTrue(meta.get("sql_rewritten"))

    def test_validator_preprocess_sql_cast_wkt_for_extent(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        sql, meta = v._preprocess_sql_for_validation("SELECT ST_Extent('LINESTRING(1 3, 5 6)');")
        self.assertIn("::geometry", sql)
        self.assertTrue(meta.get("sql_rewritten"))

    def test_load_external_table_sources_how_to_get_link(self):
        import tempfile
        import json
        with tempfile.TemporaryDirectory() as td:
            p = os.path.join(td, "src.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "tables": {
                            "t1": "https://example.com/data"
                        }
                    },
                    f,
                    ensure_ascii=False,
                )
            v = PostGISValidator(
                db_config={},
                input_file="",
                output_file="",
                manual_review_file="",
                external_table_sources_file=p,
            )
            got = v._lookup_known_external_dataset("t1")
            self.assertIn("example.com", got.get("source_hint") or "")

    def test_validator_compare_numeric_with_tolerance(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        status, _ = v._compare_results([{"st_xmax": 220288.24878054656}], "220288.248780547")
        self.assertEqual(status, "match")

    def test_validator_compare_box_with_tolerance(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        status, _ = v._compare_results(
            [{"box2d": "BOX(220186.99512189245 150406,220288.24878054656 150506.12682932706)"}],
            "BOX(220186.984375 150406,220288.25 150506.140625)"
        )
        self.assertEqual(status, "match")

    def test_validator_lookup_known_external_dataset(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        d1 = v._lookup_known_external_dataset("nyc_streets")
        self.assertIsNotNone(d1)
        self.assertEqual(d1["dataset_key"], "postgis_workshop_nyc")
        d2 = v._lookup_known_external_dataset("public.nyc_streets")
        self.assertIsNotNone(d2)
        self.assertEqual(d2["dataset_key"], "postgis_workshop_nyc")

    def test_validator_decide_missing_case_context_ddl_found(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        decision = v._decide_missing_case(
            "t4",
            missing_info={},
            context={
                "function_id": "FUNC_A",
                "example_id": 1,
                "step_index": 2,
                "steps": [
                    {"sql": "CREATE TABLE t4(id int, geom geometry);"},
                    {"sql": "SELECT * FROM t4;"},
                ],
                "example_index": {},
                "table_create_index": {},
            }
        )
        self.assertEqual(decision["missing_case"], "context_ddl_found")
        self.assertEqual(decision["ddl_source"], "intra_previous_step")

    def test_validator_generate_inserts_from_expected_result_geometry(self):
        v = PostGISValidator(
            db_config={},
            input_file="",
            output_file="",
            manual_review_file="",
        )
        inserts = v._generate_inserts_from_expected_result(
            "public.t5",
            expected_rows=[{"id": 1, "geom": "POINT(0 0)"}],
            geometry_column="geom",
        )
        self.assertEqual(len(inserts), 1)
        self.assertIn('INSERT INTO "public"."t5"', inserts[0])
        self.assertIn("ST_GeomFromText('POINT(0 0)')", inserts[0])


if __name__ == "__main__":
    unittest.main()

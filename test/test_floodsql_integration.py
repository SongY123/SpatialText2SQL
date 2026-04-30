import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_ensure_package("src", ROOT / "src")
_ensure_package("src.datasets", ROOT / "src" / "datasets")
_ensure_package("src.datasets.loaders", ROOT / "src" / "datasets" / "loaders")
_ensure_package("src.prompting", ROOT / "src" / "prompting")
_ensure_package("src.retrieval", ROOT / "src" / "retrieval")
_ensure_package("src.sql", ROOT / "src" / "sql")
_ensure_package("scripts", ROOT / "scripts")
_ensure_package("scripts.floodsql", ROOT / "scripts" / "floodsql")
_ensure_package(
    "src.prompting.prompt_enhancements",
    ROOT / "src" / "prompting" / "prompt_enhancements",
)
_ensure_package(
    "src.prompting.prompt_enhancements.spatialsql_pg",
    ROOT / "src" / "prompting" / "prompt_enhancements" / "spatialsql_pg",
)
_ensure_package(
    "src.prompting.prompt_enhancements.floodsql_pg",
    ROOT / "src" / "prompting" / "prompt_enhancements" / "floodsql_pg",
)

_load_module("src.datasets.base", ROOT / "src" / "datasets" / "base.py")
floodsql_loader_module = _load_module(
    "src.datasets.loaders.floodsql_loader",
    ROOT / "src" / "datasets" / "loaders" / "floodsql_loader.py",
)
prompt_builder_module = _load_module(
    "src.prompting.prompt_builder",
    ROOT / "src" / "prompting" / "prompt_builder.py",
)
floodsql_context_module = sys.modules[
    "src.prompting.prompt_enhancements.floodsql_pg.context_provider"
]
retriever_module = _load_module(
    "src.retrieval.floodsql_metadata_retriever",
    ROOT / "src" / "retrieval" / "floodsql_metadata_retriever.py",
)
sql_dialect_adapter = _load_module(
    "src.sql.sql_dialect_adapter",
    ROOT / "src" / "sql" / "sql_dialect_adapter.py",
)
floodsql_validate_script = _load_module(
    "scripts.floodsql.validate_gold_sql",
    ROOT / "scripts" / "floodsql" / "validate_gold_sql.py",
)
floodsql_consistency_script = _load_module(
    "scripts.floodsql.build_execution_consistency",
    ROOT / "scripts" / "floodsql" / "build_execution_consistency.py",
)


class StaticSampleDataProvider:
    def __init__(self, sample_text: str = ""):
        self.sample_text = sample_text

    def build_sample_data(self, dataset_name: str, metadata: dict | None, compact_schema: str) -> str:
        del dataset_name, metadata, compact_schema
        return self.sample_text


def _build_prompt_builder(sample_text: str = "- claims\n  {\"geoid\": \"48201\", \"geometry\": \"<geometry>\"}"):
    return prompt_builder_module.PromptBuilder(
        {"sample_data_provider": StaticSampleDataProvider(sample_text)}
    )


class FloodSQLLoaderTests(unittest.TestCase):
    def test_merges_category_json_with_official_results_jsonl(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_dir = Path(tmpdir) / "benchmark" / "single_table"
            benchmark_dir.mkdir(parents=True, exist_ok=True)
            (benchmark_dir / "50.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "L0_0001",
                            "question": "How many claims are there?",
                            "sql": "SELECT COUNT(*) AS num_claims FROM claims;",
                            "level": "L0",
                            "category": "single table",
                            "output_type": "scalar(column='num_claims')",
                            "expected_columns": ["num_claims"],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (benchmark_dir / "50_results.jsonl").write_text(
                json.dumps(
                    {
                        "id": "L0_0001",
                        "question": "How many claims are there?",
                        "sql": "SELECT COUNT(*) AS num_claims FROM claims;",
                        "elapsed": 0.01,
                        "row_count": 1,
                        "result": [[42]],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            loader = floodsql_loader_module.FloodSQLLoader(
                {
                    "data_path": tmpdir,
                    "benchmark_specs": [
                        {
                            "family": "single_table",
                            "questions_file": "50.json",
                            "results_file": "50_results.jsonl",
                        }
                    ],
                }
            )
            raw_data = loader.load_raw_data(tmpdir)
            extracted = loader.extract_questions_and_sqls(raw_data)

            self.assertEqual(len(extracted), 1)
            item = extracted[0]
            self.assertEqual(item["id"], "L0_0001")
            self.assertEqual(item["source_backend"], "duckdb")
            self.assertEqual(item["metadata"]["family"], "single_table")
            self.assertEqual(item["metadata"]["official_row_count"], 1)
            self.assertEqual(item["metadata"]["official_result"], [[42]])
            self.assertEqual(item["metadata"]["expected_columns"], ["num_claims"])

    def test_loader_falls_back_to_repo_local_floodsql_bench(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "repo"
            benchmark_dir = repo_root / "FloodSQL-Bench" / "benchmark" / "single_table"
            benchmark_dir.mkdir(parents=True, exist_ok=True)
            (benchmark_dir / "50.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "L0_0001",
                            "question": "How many claims are there?",
                            "sql": "SELECT COUNT(*) FROM claims;",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (benchmark_dir / "50_results.jsonl").write_text(
                json.dumps({"id": "L0_0001", "row_count": 1, "result": [[1]]}) + "\n",
                encoding="utf-8",
            )

            loader = floodsql_loader_module.FloodSQLLoader(
                {
                    "data_path": "missing-floodsql-bench",
                    "benchmark_specs": [
                        {
                            "family": "single_table",
                            "questions_file": "50.json",
                            "results_file": "50_results.jsonl",
                        }
                    ],
                }
            )
            with mock.patch.object(floodsql_loader_module, "REPO_ROOT", repo_root):
                raw_data = loader.load_raw_data(loader.data_path)

            self.assertEqual(len(raw_data), 1)
            self.assertEqual(raw_data[0]["id"], "L0_0001")


class FloodSQLDialectAdapterTests(unittest.TestCase):
    def test_converts_duckdb_specific_datetime_cast_and_point_usage(self):
        sql = (
            "SELECT STRFTIME('%Y', c.dateOfLoss) AS year, "
            "AVG(CAST(c.amountPaidOnBuildingClaim AS DOUBLE)) AS avg_building "
            "FROM claims c JOIN floodplain f "
            "ON ST_Contains(f.geometry, ST_Point(cast(1 as double), cast(2 as double))) "
            "GROUP BY year"
        )
        converted, issues = sql_dialect_adapter.convert_duckdb_to_postgis(sql)

        self.assertIn("TO_CHAR(c.dateOfLoss, 'YYYY') AS year", converted)
        self.assertIn("CAST(c.amountPaidOnBuildingClaim AS DOUBLE PRECISION)", converted)
        self.assertIn(
            "ST_SetSRID(ST_Point(CAST(1 AS DOUBLE PRECISION), CAST(2 AS DOUBLE PRECISION)), 4326)",
            converted,
        )
        self.assertEqual(issues, [])

    def test_rewrites_lon_lat_point_calls_to_materialized_geometry(self):
        sql = (
            "SELECT b.GEOID AS zcta_id "
            "FROM zcta b JOIN schools a "
            "ON ST_Contains(b.geometry, ST_Point(a.LON, a.LAT)) "
            "WHERE a.STATEFP = '12' "
            "GROUP BY b.GEOID "
            "ORDER BY COUNT(DISTINCT a.UNIQUE_ID) DESC LIMIT 1"
        )
        converted, issues = sql_dialect_adapter.convert_duckdb_to_postgis(sql)

        self.assertIn("ST_Contains(b.geometry, a.geometry)", converted)
        self.assertNotIn("ST_SetSRID(ST_Point(a.LON, a.LAT), 4326)", converted)
        self.assertEqual(issues, [])

    def test_adds_nulls_last_for_desc_order_by(self):
        sql = (
            "SELECT c.NAME AS county_name "
            "FROM claims cl JOIN nri n ON cl.GEOID = n.GEOID JOIN county c ON LEFT(cl.GEOID,5)=c.GEOID "
            "WHERE c.STATEFP='48' "
            "GROUP BY c.NAME "
            "ORDER BY (AVG(n.CFLD_RISKS) - AVG(n.RFLD_RISKS)) DESC LIMIT 5"
        )

        converted, issues = sql_dialect_adapter.convert_duckdb_to_postgis(sql)

        self.assertIn("ORDER BY (AVG(n.CFLD_RISKS) - AVG(n.RFLD_RISKS)) DESC NULLS LAST LIMIT 5", converted)
        self.assertEqual(issues, [])

    def test_preserves_existing_nulls_ordering(self):
        sql = "SELECT geoid FROM svi ORDER BY m_disabl DESC NULLS FIRST LIMIT 1"

        converted, issues = sql_dialect_adapter.convert_duckdb_to_postgis(sql)

        self.assertIn("ORDER BY m_disabl DESC NULLS FIRST LIMIT 1", converted)
        self.assertEqual(issues, [])


class FloodSQLValidationScriptTests(unittest.TestCase):
    def test_rows_equal_ignoring_order_treats_reordered_results_as_match(self):
        left = [["33157"], ["32308"]]
        right = [["32308"], ["33157"]]

        self.assertTrue(
            floodsql_validate_script._rows_equal_ignoring_order(left, right, 1e-6)
        )

    def test_detects_nondeterministic_topk_query(self):
        sql = (
            "SELECT c.NAME AS county_name "
            "FROM hospitals h JOIN county c ON h.COUNTYFIPS = c.GEOID "
            "WHERE h.STATEFP = '22' AND h.TYPE = 'CRITICAL ACCESS' "
            "GROUP BY c.NAME ORDER BY COUNT(*) DESC LIMIT 1;"
        )

        self.assertTrue(
            floodsql_validate_script._is_probably_nondeterministic_topk_query(sql)
        )

    def test_does_not_flag_deterministic_limit_query_as_nondeterministic(self):
        sql = "SELECT GEOID FROM zcta ORDER BY GEOID DESC LIMIT 1;"

        self.assertFalse(
            floodsql_validate_script._is_probably_nondeterministic_topk_query(sql)
        )


class _StaticExecutor:
    def __init__(self, responses: dict[str, dict]):
        self.responses = responses

    def execute(self, sql: str) -> dict:
        return self.responses[sql]


class FloodSQLExecutionConsistencyTests(unittest.TestCase):
    def test_build_execution_consistency_report_matches_spatialsql_style_statuses(self):
        items = [
            {
                "id": "L0_0001",
                "source_sql": "SELECT 1",
                "metadata": {"source_id": "L0_0001", "level": "L0", "family": "single_table", "official_result": [[1]], "official_row_count": 1},
            },
            {
                "id": "L1_0002",
                "source_sql": "SELECT geoid FROM county ORDER BY geoid",
                "metadata": {
                    "source_id": "L1_0002",
                    "level": "L1",
                    "family": "double_table_key",
                    "official_result": [["001"], ["003"]],
                    "official_row_count": 2,
                },
            },
            {
                "id": "L2_0003",
                "source_sql": (
                    "SELECT county_name FROM claims "
                    "GROUP BY county_name ORDER BY COUNT(*) DESC LIMIT 1"
                ),
                "metadata": {
                    "source_id": "L2_0003",
                    "level": "L2",
                    "family": "double_table_spatial",
                    "official_result": [["Harris"]],
                    "official_row_count": 1,
                },
            },
        ]

        target_executor = _StaticExecutor(
            {
                "SELECT 1": {"status": "ok", "rows": [(1,)]},
                "SELECT geoid FROM county ORDER BY geoid": {
                    "status": "ok",
                    "rows": [("003",), ("001",)],
                },
                "SELECT county_name FROM claims GROUP BY county_name ORDER BY COUNT(*) DESC NULLS LAST LIMIT 1": {
                    "status": "ok",
                    "rows": [("Galveston",)],
                },
            }
        )

        with mock.patch.object(
            floodsql_consistency_script,
            "convert_duckdb_to_postgis",
            side_effect=lambda sql: (sql.replace(" DESC LIMIT", " DESC NULLS LAST LIMIT"), []),
        ):
            report = floodsql_consistency_script.build_execution_consistency_report(
                items,
                target_executor,
                source_mode="official_results",
            )

        self.assertEqual(report["summary"]["validated"], 1)
        self.assertEqual(report["summary"]["format_difference"], 1)
        self.assertEqual(report["summary"]["semantic_mismatch"], 1)
        self.assertEqual(report["details"][0]["status"], "exact_match")
        self.assertEqual(report["details"][1]["status"], "format_difference")
        self.assertEqual(report["details"][2]["mismatch_subtype"], "nondeterministic_topk_difference")
        self.assertEqual(report["summary"]["by_level"]["L2"]["semantic_mismatch"], 1)

    def test_build_execution_consistency_report_supports_duckdb_source_mode(self):
        items = [
            {
                "id": "L0_0001",
                "source_sql": "SELECT 1",
                "metadata": {"source_id": "L0_0001", "level": "L0", "family": "single_table"},
            }
        ]
        source_executor = _StaticExecutor({"SELECT 1": {"status": "ok", "rows": [(1,)]}})
        target_executor = _StaticExecutor({"SELECT 1": {"status": "ok", "rows": [(1,)]}})

        with mock.patch.object(
            floodsql_consistency_script,
            "convert_duckdb_to_postgis",
            return_value=("SELECT 1", []),
        ):
            report = floodsql_consistency_script.build_execution_consistency_report(
                items,
                target_executor,
                source_mode="duckdb",
                source_executor=source_executor,
            )

        self.assertEqual(report["summary"]["validated"], 1)
        self.assertEqual(report["details"][0]["source_mode"], "duckdb")


class FloodSQLPromptBuilderTests(unittest.TestCase):
    def test_floodsql_prompt_uses_sample_data_instead_of_sample_context(self):
        builder = _build_prompt_builder()
        prompt = builder.build_prompt(
            question="How many claims are there in Harris County?",
            schema="table claims(GEOID text, geometry geometry)",
            config_type="base",
            dataset_name="floodsql_pg",
            metadata={
                "level": "L0",
                "family": "single_table",
                "category": "single table",
                "output_type": "scalar(column='num_claims')",
                "expected_columns": ["num_claims"],
            },
        )

        self.assertIn("## Sample Data", prompt)
        self.assertIn("- claims", prompt)
        self.assertIn("\"geometry\": \"<geometry>\"", prompt)
        self.assertIn("## Output Requirements", prompt)
        self.assertNotIn("## FloodSQL 约束", prompt)
        self.assertNotIn("DuckDB-specific", prompt)
        self.assertNotIn("sample_level", prompt)
        self.assertNotIn("sample_family", prompt)
        self.assertNotIn("expected_output_type", prompt)
        self.assertNotIn("expected_columns", prompt)

    def test_floodsql_prompt_enhanced_uses_dataset_template_without_answer_metadata(self):
        builder = prompt_builder_module.PromptBuilder(
            {
                "project_root": ROOT,
                "sample_data_provider": StaticSampleDataProvider(
                    "- claims\n  {\"geoid\": \"48201\", \"geometry\": \"<geometry>\"}"
                ),
                "ablation_configs": {
                    "prompt_enhanced": {
                        "use_rag": False,
                        "use_keyword": False,
                        "prompt_style": "prompt_enhanced",
                    }
                },
                "prompt_styles": {
                    "default": {
                        "template_path": "prompts/text2sql_prompt.txt",
                        "include_sample_data": True,
                        "use_dataset_context": False,
                    },
                    "prompt_enhanced": {
                        "template_path": "prompts/text2sql_prompt.txt",
                        "include_sample_data": True,
                        "use_dataset_context": False,
                        "dataset_specific": True,
                    },
                },
            }
        )

        prompt = builder.build_prompt(
            question="How many claims are there in Harris County?",
            schema="table claims(geoid text, dateofloss date, geometry geometry)",
            config_type="prompt_enhanced",
            dataset_name="floodsql_pg",
            metadata={
                "level": "L0",
                "family": "single_table",
                "category": "single table",
                "output_type": "scalar(column='num_claims')",
                "expected_columns": ["num_claims"],
                "official_row_count": 1,
                "official_result": [[42]],
            },
        )

        self.assertIn("## SQL Construction Guidelines", prompt)
        self.assertIn("## Content Information", prompt)
        self.assertNotIn("## FloodSQL Reasoning Guide", prompt)
        self.assertNotIn("## Schema Semantics", prompt)
        self.assertNotIn("## Grounding Hints", prompt)
        self.assertNotIn("## Retrieved Context", prompt)
        self.assertNotIn("## Keyword Context", prompt)
        self.assertNotIn("FloodSQL-Bench covers flood-management data layers", prompt)
        self.assertNotIn("FloodSQL difficulty tier: L0", prompt)
        self.assertIn("\"geoid\": \"48201\"", prompt)
        self.assertIn("```sql\nSELECT ...\n```", prompt)
        self.assertIn("PostgreSQL + PostGIS SELECT statement or a WITH query whose final statement is a SELECT statement", prompt)
        self.assertIn("Do not include SQL comments, explanations, JSON, XML tags, natural language, or any text outside the code fence.", prompt)
        self.assertNotIn("The first token must be SELECT or WITH.", prompt)
        self.assertNotIn("expected_columns", prompt)
        self.assertNotIn("num_claims", prompt)
        self.assertNotIn("official_result", prompt)
        self.assertNotIn("official_row_count", prompt)
        self.assertNotIn("scalar(column", prompt)
        self.assertNotIn("ST_Point", prompt)
        self.assertNotIn("ST_Contains", prompt)


class FloodSQLPromptEnhancementContextTests(unittest.TestCase):
    def test_context_provider_keeps_metadata_but_filters_function_and_answer_hints(self):
        metadata = {
            "claims": {
                "_meta": "Key-based table containing National Flood Insurance Program (NFIP) flood claim records.",
                "key_identifier": ["GEOID"],
                "spatial_identifier": [],
                "layer_category": "tract-level",
                "schema": [
                    {
                        "column_name": "GEOID",
                        "description": "11-digit census tract GEOID used as the primary join key.",
                    },
                    {
                        "column_name": "dateOfLoss",
                        "description": "Date of the flood insurance claim loss event.",
                    },
                    {
                        "column_name": "geometry",
                        "description": "Point geometry that should not be converted with ST_Point in the prompt.",
                    },
                ],
            },
            "county": {
                "_meta": "County-level administrative polygon layer.",
                "key_identifier": ["GEOID"],
                "spatial_identifier": ["geometry"],
                "layer_category": "polygon + county-level",
                "schema": [
                    {"column_name": "GEOID", "description": "5-digit county FIPS code."},
                    {"column_name": "NAME", "description": "Official county name."},
                    {"column_name": "geometry", "description": "Polygon geometry."},
                ],
            },
            "hospitals": {
                "_meta": "Point-based layer representing hospital facilities. Geometry can be constructed dynamically using ST_Point(LON, LAT).",
                "key_identifier": ["COUNTYFIPS"],
                "spatial_identifier": ["LAT", "LON"],
                "layer_category": "point + ZIP-level",
                "schema": [
                    {"column_name": "COUNTYFIPS", "description": "5-digit county FIPS code."},
                    {"column_name": "LAT", "description": "Latitude coordinate."},
                    {"column_name": "LON", "description": "Longitude coordinate."},
                    {"column_name": "geometry", "description": "Construct via ST_Point(LON, LAT)."},
                ],
            },
            "_global": {
                "join_rules": {
                    "key_based": {
                        "direct": [{"pair": ["claims.GEOID", "county.GEOID"]}],
                        "concat": [{"pair": ["LEFT(claims.GEOID,5)", "county.GEOID"]}],
                    },
                    "spatial": {
                        "point_polygon": [
                            {"pair": ["ST_Point(hospitals.LON, hospitals.LAT)", "county.geometry"]}
                        ],
                        "polygon_polygon": [],
                    },
                },
                "spatial_function_notes": ["ST_Contains(geomA, geomB): function hint."],
                "basic_function_notes": ["COUNT(*): function hint."],
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = Path(tmpdir) / "metadata_parquet.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            provider = floodsql_context_module.FloodSQLContextProvider(
                ROOT,
                metadata_path=metadata_path,
            )

            grounding = provider.build_grounding(
                {
                    "level": "L2",
                    "output_type": "scalar(column='num_claims')",
                    "expected_columns": ["num_claims"],
                    "official_row_count": 1,
                    "official_result": [[1]],
                }
            )
            schema_semantics = provider.build_schema_semantics(
                "table claims(geoid text, dateofloss date, geometry geometry)\n"
                "table county(geoid text, name text, geometry geometry)\n"
                "table hospitals(countyfips text, lat double, lon double, geometry geometry)"
            )
            combined = grounding + "\n" + schema_semantics

        self.assertIn("claims.geoid = county.geoid", grounding)
        self.assertIn("claims.geoid corresponds to county.geoid", grounding)
        self.assertIn("point facility layers (hospitals)", grounding)
        self.assertIn("FloodSQL difficulty tier: L2", grounding)
        self.assertIn("claims: Key-based table containing National Flood Insurance Program", schema_semantics)
        self.assertIn("claims.geoid: 11-digit census tract GEOID", schema_semantics)
        self.assertIn("claims.geometry: Geometry column is available", schema_semantics)
        self.assertIn("hospitals.geometry: Point geometry for the facility record", schema_semantics)
        self.assertNotIn("ST_Point", combined)
        self.assertNotIn("ST_Contains", combined)
        self.assertNotIn("COUNT(*)", combined)
        self.assertNotIn("expected_columns", combined)
        self.assertNotIn("num_claims", combined)
        self.assertNotIn("official_result", combined)
        self.assertNotIn("official_row_count", combined)
        self.assertNotIn("scalar(column", combined)


class FloodSQLRetrievalTests(unittest.TestCase):
    def test_keyword_retrieval_formats_table_column_and_join_rules(self):
        metadata = {
            "claims": {
                "schema": [
                    {"column_name": "GEOID", "description": "tract geoid"},
                    {"column_name": "dateOfLoss", "description": "claim loss date"},
                ],
                "_meta": "NFIP claims table",
            },
            "county": {
                "schema": [
                    {"column_name": "GEOID", "description": "county geoid"},
                    {"column_name": "NAME", "description": "county name"},
                ],
                "_meta": "County polygon table",
            },
            "hospitals": {
                "schema": [
                    {"column_name": "COUNTYFIPS", "description": "county join key"},
                    {"column_name": "NAME", "description": "hospital name"},
                ],
                "_meta": "Hospital point table",
            },
            "_global": {
                "join_rules": {
                    "key_based": {"direct": [{"pair": ["hospitals.COUNTYFIPS", "county.GEOID"]}]},
                    "spatial": {"point_polygon": [], "polygon_polygon": []},
                },
                "rules": {"COUNTYFIPS": "5-digit county FIPS code"},
                "notes": ["Use county GEOID for county-level aggregation."],
                "spatial_function_notes": ["Use ST_Contains for point-in-polygon."],
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = Path(tmpdir) / "metadata_parquet.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            retriever = retriever_module.FloodSQLMetadataKeywordSearcher(
                {
                    "doc_source": str(metadata_path),
                    "vector_db_path": str(Path(tmpdir) / "vector"),
                    "table_top_k_by_level": {"L0": 2},
                    "column_top_k": 1,
                }
            )
            retriever.build_index()

            result = retriever.search(
                "How many hospitals are in each county?",
                item={"metadata": {"level": "L0"}},
            )
            context = retriever.format_context(result)

            self.assertTrue((Path(tmpdir) / "vector" / "floodsql_metadata_manifest.json").exists())
            self.assertEqual(retriever._get_table_top_k({"metadata": {"level": "L0"}}), 2)
            self.assertIn("[TABLES SELECTED]", context)
            self.assertIn("[COLUMNS SELECTED]", context)
            self.assertIn("[JOIN RULES: KEY-BASED DIRECT]", context)
            self.assertIn("hospitals.COUNTYFIPS <-> county.GEOID", context)


if __name__ == "__main__":
    unittest.main()

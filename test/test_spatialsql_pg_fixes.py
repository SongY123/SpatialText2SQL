import importlib.util
import sys
import types
import unittest
from pathlib import Path


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
_ensure_package("src.sql", ROOT / "src" / "sql")
_ensure_package("src.prompting", ROOT / "src" / "prompting")
_ensure_package(
    "src.prompting.prompt_enhancements",
    ROOT / "src" / "prompting" / "prompt_enhancements",
)
_ensure_package(
    "src.prompting.prompt_enhancements.spatialsql_pg",
    ROOT / "src" / "prompting" / "prompt_enhancements" / "spatialsql_pg",
)

sql_dialect_adapter = _load_module(
    "src.sql.sql_dialect_adapter",
    ROOT / "src" / "sql" / "sql_dialect_adapter.py",
)
prompt_builder_module = _load_module(
    "src.prompting.prompt_builder",
    ROOT / "src" / "prompting" / "prompt_builder.py",
)


class StaticSampleDataProvider:
    def __init__(self, sample_text: str = ""):
        self.sample_text = sample_text
        self.calls = []

    def build_sample_data(self, dataset_name: str, metadata: dict | None, compact_schema: str) -> str:
        self.calls.append(
            {
                "dataset_name": dataset_name,
                "metadata": metadata or {},
                "compact_schema": compact_schema,
            }
        )
        return self.sample_text


class FakePromptEnhancementRegistry:
    def __init__(self, overrides=None, grounding_blocks=None):
        self.overrides = overrides or {}
        self.grounding_blocks = grounding_blocks or {}
        self.resolve_calls = []
        self.grounding_calls = []

    def resolve_dataset_override(self, dataset_name: str):
        self.resolve_calls.append(dataset_name)
        return self.overrides.get(dataset_name, {})

    def build_grounding_block(self, dataset_name: str, metadata: dict | None):
        metadata = metadata or {}
        key = (dataset_name, metadata.get("split"), metadata.get("source_id"))
        self.grounding_calls.append(key)
        return self.grounding_blocks.get(key, "")

    def build_schema_semantics_block(
        self,
        dataset_name: str,
        metadata: dict | None,
        compact_schema: str,
    ):
        del dataset_name, metadata, compact_schema
        return ""


def _build_prompt_builder(
    sample_text: str = "- sample_table\n  {\"name\": \"demo\"}",
    extra_config: dict | None = None,
):
    config = {"sample_data_provider": StaticSampleDataProvider(sample_text)}
    if extra_config:
        config.update(extra_config)
    return prompt_builder_module.PromptBuilder(config)


class SpatialDialectAdapterTests(unittest.TestCase):
    def test_converts_mbr_and_measurement_functions(self):
        sql = (
            "Select name from cities "
            "order by MbrMinY(Shape) asc limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql,
            table_prefix="dataset1_ada_",
        )

        self.assertEqual(
            converted,
            "Select name from dataset1_ada_cities order by ST_YMin(shape) asc limit 1",
        )
        self.assertEqual(issues, [])

    def test_wraps_binary_geometry_calls_and_preserves_split(self):
        sql = (
            "Select distinct provinces.name from dataset2_edu_provinces "
            "inner join lakes On ST_Intersects(provinces.Shape, lakes.shape) = 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql,
            table_prefix="dataset1_ada_",
        )

        self.assertIn("from dataset1_ada_provinces", converted.lower())
        self.assertIn("join dataset1_ada_lakes", converted.lower())
        self.assertIn(
            "ST_Intersects(dataset1_ada_provinces.shape, dataset1_ada_lakes.shape)",
            converted,
        )
        self.assertNotIn("= 1", converted)
        self.assertEqual(issues, [])

    def test_converts_length_area_flags_to_geography_signature(self):
        sql = (
            "Select Sum(ST_Length(ST_Intersection(provinces.shape, rivers.shape), 1)) "
            "from provinces inner join rivers On Intersects(provinces.Shape, rivers.Shape) = 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql,
            table_prefix="dataset1_ada_",
        )

        self.assertIn(
            "ST_Intersection(dataset1_ada_provinces.shape, dataset1_ada_rivers.shape)",
            converted,
        )
        self.assertIn("ST_Length(ST_Intersection(", converted)
        self.assertIn(")::geography, true)", converted)
        self.assertEqual(issues, [])

    def test_converts_distance_three_arg_signature_to_geography(self):
        sql = (
            "Select hotels.name, ST_Distance(busstops.Location, hotels.Location, 1) as d "
            "from busstops inner join hotels On ST_Distance(busstops.Location, hotels.Location, 1) < 1000"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql,
            table_prefix="dataset2_traffic_",
        )

        self.assertIn("from dataset2_traffic_busstops", converted.lower())
        self.assertIn("join dataset2_traffic_hotels", converted.lower())
        self.assertIn(
            "ST_Distance(dataset2_traffic_busstops.location::geography, dataset2_traffic_hotels.location::geography, true)",
            converted,
        )
        self.assertEqual(issues, [])

    def test_fixes_inner_join_without_on(self):
        sql = (
            "Select dataset1_ada_airports.name, min(ST_Distance(dataset1_ada_cities.shape, "
            "dataset1_ada_airports.location, 1)) as distance from dataset1_ada_cities "
            "inner join dataset1_ada_airports where dataset1_ada_cities.name = '苏州市' "
            "order by distance limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_ada_"
        )
        self.assertIn("inner join dataset1_ada_airports on true where", converted.lower())
        self.assertEqual(issues, [])

    def test_fixes_inner_join_without_on_with_table_alias(self):
        sql = (
            "Select b.name, ST_Distance(a.location, b.location, 1) from universities a "
            "inner join universities b where a.name = '北京大学' order by "
            "ST_Distance(a.location, b.location, 1) asc limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_edu_"
        )
        low = converted.lower()
        self.assertIn("inner join dataset1_edu_universities b on true where", low)
        self.assertEqual(issues, [])

    def test_fixes_count_star_group_by_not_inside_subquery(self):
        sql = (
            "Select dataset1_ada_provinces.name, count(*) from dataset1_ada_provinces "
            "inner join dataset1_ada_airports On ST_Contains(dataset1_ada_provinces.shape, "
            "dataset1_ada_airports.location) where dataset1_ada_provinces.name = "
            "(Select name from dataset1_ada_provinces order by POPU desc limit 1)"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_ada_"
        )
        self.assertRegex(
            converted,
            r"(?is)order\s+by\s+POPU\s+desc\s+NULLS\s+LAST\s+limit\s+1\)\s+GROUP\s+BY\s+dataset1_ada_provinces\.name",
        )
        self.assertEqual(issues, [])

    def test_fixes_tourism_scenicspots_table_casing(self):
        sql = (
            "Select count(*) from dataset1_tourism_ScenicSpots inner join dataset1_tourism_cities "
            "On ST_Within(dataset1_tourism_ScenicSpots.location, dataset1_tourism_cities.shape)"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_tourism_"
        )
        self.assertNotIn("ScenicSpots", converted)
        self.assertIn("dataset1_tourism_scenicSpots", converted)
        self.assertEqual(issues, [])

    def test_fixes_edu_triple_inner_join(self):
        sql = (
            "Select dataset2_edu_cities.name, count(*) from dataset2_edu_provinces "
            "inner join dataset2_edu_cities inner join dataset2_edu_universities "
            "on ST_Contains(dataset2_edu_cities.shape, dataset2_edu_universities.location) "
            "where dataset2_edu_cities.name = '武汉市'"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset2_edu_"
        )
        low = converted.lower()
        self.assertIn(
            "inner join dataset2_edu_cities on st_contains(dataset2_edu_provinces.shape, dataset2_edu_cities.shape)",
            low,
        )
        self.assertIn(
            "inner join dataset2_edu_universities on st_contains(dataset2_edu_cities.shape, dataset2_edu_universities.location)",
            low,
        )
        self.assertEqual(issues, [])

    def test_fixes_on_clause_using_alias_d(self):
        sql = (
            "Select dataset1_traffic_hotels.name, "
            "ST_Distance(dataset1_traffic_agencies.location, dataset1_traffic_hotels.location, 1) as d "
            "from dataset1_traffic_agencies inner join dataset1_traffic_hotels On d < 500 "
            "where dataset1_traffic_agencies.NAME = 'x'"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_traffic_"
        )
        self.assertNotRegex(converted, r"(?i)On\s+d\s*<")
        self.assertIn("::geography", converted)
        self.assertIn("< 500", converted)
        self.assertEqual(issues, [])

    def test_fixes_select_count_star_adds_group_by(self):
        sql = (
            "Select dataset1_ada_provinces.name, count(*) from dataset1_ada_provinces "
            "inner join dataset1_ada_airports On ST_Contains(dataset1_ada_provinces.shape, "
            "dataset1_ada_airports.location) where dataset1_ada_provinces.name = '河南省'"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_ada_"
        )
        self.assertRegex(
            converted,
            r"(?i)group\s+by\s+dataset1_ada_provinces\.name",
        )
        self.assertEqual(issues, [])

    def test_fixes_select_city_count_qualifies_group_by(self):
        sql = (
            "Select city, count(*) from dataset1_edu_universities where city = '武汉市'"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_edu_"
        )
        self.assertRegex(
            converted,
            r"(?i)group\s+by\s+dataset1_edu_universities\.city",
        )
        self.assertEqual(issues, [])

    def test_fixes_srid_mapping_to_postgis_function(self):
        sql = "Select distinct SRID(Location) from universities"
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_edu_"
        )
        self.assertEqual(
            converted,
            "Select distinct ST_SRID(location) from dataset1_edu_universities",
        )
        self.assertEqual(issues, [])

    def test_fixes_group_by_spacing_before_order_by(self):
        sql = (
            "Select airports.name, min(Distance(cities.Shape, airports.Location, 1)) as distance "
            "from cities inner join airports where cities.name = '苏州市' order by distance limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_ada_"
        )
        self.assertIn("GROUP BY dataset1_ada_airports.name order by distance limit 1", converted)
        self.assertNotIn("nameorder by", converted)
        self.assertEqual(issues, [])

    def test_fixes_missing_join_on_before_order_by(self):
        sql = (
            "Select airports.name, Distance(airports.Location, Intersection(a.Shape, b.Shape), 1) as d "
            "from provinces a inner join provinces b On a.name = '河南省' and b.name = '湖北省' "
            "and Intersects(a.Shape, b.Shape) = 1 inner join airports order by d asc limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_ada_"
        )
        self.assertIn("inner join dataset1_ada_airports on true order by d asc limit 1", converted.lower())
        self.assertEqual(issues, [])

    def test_fixes_incomplete_group_by_with_join_key(self):
        sql = (
            "Select districts.name, count(*) from subwaystations "
            "inner join districts On subwaystations.adcode = districts.administrative_division_code "
            "group by subwaystations.adcode"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_traffic_"
        )
        self.assertRegex(
            converted,
            r"(?i)group\s+by\s+dataset1_traffic_subwaystations\.adcode,\s*dataset1_traffic_districts\.name",
        )
        self.assertEqual(issues, [])

    def test_fixes_distinct_field_not_duplicated_in_group_by(self):
        sql = (
            "Select distinct subways.line, count(*) from subways "
            "inner join districts on Intersects(subways.Shape, districts.Shape) = 1 "
            "group by subways.line"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_traffic_"
        )
        self.assertIn("group by dataset1_traffic_subways.line", converted.lower())
        self.assertNotIn("group by dataset1_traffic_subways.line, distinct", converted.lower())
        self.assertEqual(issues, [])

    def test_fixes_incomplete_group_by_spacing_before_order_by(self):
        sql = (
            "Select districts.name, count(*) as count from subwaystations "
            "inner join districts On subwaystations.adcode = districts.administrative_division_code "
            "group by subwaystations.adcode order by count desc limit 1"
        )
        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_traffic_"
        )
        self.assertIn(
            "GROUP BY dataset1_traffic_subwaystations.adcode, dataset1_traffic_districts.name ORDER BY count desc NULLS LAST limit 1",
            converted,
        )
        self.assertNotIn("nameorder by", converted)
        self.assertEqual(issues, [])

    def test_adds_nulls_last_for_desc_order_by(self):
        sql = "Select province from GDPs order by year_2023 desc limit 1"

        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_tourism_"
        )

        self.assertEqual(
            converted,
            "Select province from dataset1_tourism_GDPs ORDER BY year_2023 desc NULLS LAST limit 1",
        )
        self.assertEqual(issues, [])

    def test_preserves_existing_nulls_ordering_for_spatialsql(self):
        sql = "Select city, star_hotel_number from tours order by star_hotel_number desc NULLS FIRST limit 1"

        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_tourism_"
        )

        self.assertEqual(
            converted,
            "Select city, star_hotel_number from dataset1_tourism_tours order by star_hotel_number desc NULLS FIRST limit 1",
        )
        self.assertEqual(issues, [])

    def test_adds_nulls_last_inside_subquery_order_by(self):
        sql = (
            "Select cities.name, count(*) from cities inner join ScenicSpots "
            "On Contains(cities.Shape, ScenicSpots.Location) = 1 "
            "where cities.name in (Select city from tours where year = 2020 "
            "order by international_tourists_number_Wan desc limit 10) "
            "and ScenicSpots.level = '5A' group by cities.name"
        )

        converted, issues = sql_dialect_adapter.convert_spatialite_to_postgis(
            sql, table_prefix="dataset1_tourism_"
        )

        self.assertIn(
            "(Select city from dataset1_tourism_tours where year = 2020 ORDER BY international_tourists_number_Wan desc NULLS LAST limit 10)",
            converted,
        )
        self.assertEqual(issues, [])


class SpatialPromptBuilderTests(unittest.TestCase):
    def test_spatialsql_prompt_uses_unified_english_template(self):
        builder = _build_prompt_builder()
        prompt = builder.build_prompt(
            question="Which provinces is Dongting Lake located in?",
            schema="table dataset1_ada_lakes(name, shape)",
            config_type="base",
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_ada"},
        )

        self.assertIn("## Database Schema", prompt)
        self.assertIn("## Sample Data", prompt)
        self.assertIn("## User Question", prompt)
        self.assertIn("## Output Requirements", prompt)
        self.assertIn("- sample_table", prompt)
        self.assertIn("- dataset1_ada_lakes(", prompt)
        self.assertIn("Return one PostgreSQL + PostGIS SQL query only.", prompt)
        self.assertNotIn("<answer_sql>", prompt)
        self.assertNotIn("</answer_sql>", prompt)
        self.assertNotIn("Do not use Markdown code fences.", prompt)
        self.assertNotIn("Do not append a semicolon at the end of the SQL query.", prompt)
        self.assertNotIn("The first token of the response must be a SQL keyword", prompt)
        self.assertNotIn("## SpatialSQL 约束", prompt)
        self.assertNotIn("## FloodSQL 约束", prompt)
        self.assertNotIn("Ground entity mentions to the exact database literals", prompt)
        self.assertNotIn("Match spatial functions and casts to the question semantics", prompt)
        self.assertNotIn("dataset1_ada", prompt.split("## Sample Data", 1)[1])

    def test_spatialsql_prompt_filters_to_split_business_tables(self):
        builder = _build_prompt_builder()
        schema = (ROOT / "data" / "schemas" / "spatial_sql_schema.txt").read_text(
            encoding="utf-8"
        )
        prompt = builder.build_prompt(
            question="List the GDP information for tourism provinces.",
            schema=schema,
            config_type="base",
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_tourism"},
        )

        self.assertIn("- dataset1_tourism_GDPs(", prompt)
        self.assertIn("- dataset1_tourism_scenicSpots(", prompt)
        self.assertNotIn("dataset1_tourism_gdps", prompt)
        self.assertNotIn("dataset1_tourism_geometry_columns_field_infos", prompt)
        self.assertNotIn("dataset1_tourism_sql_statements_log", prompt)
        self.assertNotIn("dataset2_tourism_cities", prompt)

    def test_spatialsql_prompt_keeps_schema_pure_without_hints(self):
        builder = _build_prompt_builder()
        schema = (ROOT / "data" / "schemas" / "spatial_sql_schema.txt").read_text(
            encoding="utf-8"
        )
        prompt = builder.build_prompt(
            question="What is the area of Lake Tai?",
            schema=schema,
            config_type="base",
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_ada"},
        )

        self.assertNotIn("semantic hints:", prompt)
        self.assertNotIn("value hints:", prompt)
        self.assertNotIn("records the area of the lake", prompt)
        self.assertNotIn("'洞庭湖'", prompt)
        self.assertIn("- dataset1_ada_lakes(", prompt)

    def test_non_spatialsql_dataset_keeps_generic_prompt_without_spatialsql_rules(self):
        builder = _build_prompt_builder()
        schema = "table pois(id integer, name text, geom geometry)"
        prompt = builder.build_prompt(
            question="Find all POIs.",
            schema=schema,
            config_type="base",
            dataset_name="spatial_qa",
            metadata={},
        )

        self.assertIn("table pois(id integer, name text, geom geometry)", prompt)
        self.assertIn("Return one PostgreSQL + PostGIS SQL query only.", prompt)
        self.assertNotIn("Do not output any reasoning, explanation, analysis, comments, or surrounding text.", prompt)
        self.assertNotIn("Do not append a semicolon at the end of the SQL query.", prompt)
        self.assertNotIn("## SpatialSQL 约束", prompt)
        self.assertNotIn("## FloodSQL 约束", prompt)

    def test_spatialqa_prompt_excludes_only_irrelevant_tables(self):
        builder = _build_prompt_builder()
        schema = (ROOT / "data" / "schemas" / "postgres_schema.txt").read_text(
            encoding="utf-8"
        )
        prompt = builder.build_prompt(
            question="Which roads are in the block group with geoid '421010336004'?",
            schema=schema,
            config_type="base",
            dataset_name="spatial_qa",
            metadata={"level": 2},
        )

        self.assertIn("- roads(", prompt)
        self.assertIn("- blockgroups(", prompt)
        self.assertIn("- poi(", prompt)
        self.assertIn("- ne_time_zones(", prompt)
        self.assertNotIn("- spatial_ref_sys(", prompt)

    def test_floodsql_prompt_keeps_all_business_tables(self):
        builder = _build_prompt_builder()
        schema = """
CREATE TABLE claims (
    geoid character varying,
    statefp character varying,
    amountPaidOnBuildingClaim double precision,
    geometry USER-DEFINED
);

CREATE TABLE county (
    geoid character varying,
    name character varying,
    statefp character varying,
    geometry USER-DEFINED
);

CREATE TABLE hospitals (
    countyfips character varying,
    name character varying,
    geometry USER-DEFINED
);

CREATE TABLE zcta (
    geoid character varying,
    statefp character varying,
    geometry USER-DEFINED
);
""".strip()
        prompt = builder.build_prompt(
            question="How many claims are there in Harris County?",
            schema=schema,
            config_type="base",
            dataset_name="floodsql_pg",
            metadata={"level": "L0"},
        )

        self.assertIn("- claims(", prompt)
        self.assertIn("- county(", prompt)
        self.assertIn("- hospitals(", prompt)
        self.assertIn("- zcta(", prompt)

    def test_template_renders_retrieved_and_keyword_context_blocks(self):
        builder = _build_prompt_builder()
        prompt = builder.build_prompt(
            question="Find all POIs.",
            schema="table pois(id integer, name text, geom geometry)",
            config_type="full",
            rag_context="[RAG] Use ST_Contains for point-in-polygon checks.",
            keyword_context="[KEYWORD] pois.geom, districts.geom",
            dataset_name="spatial_qa",
            metadata={},
        )

        self.assertIn("## Retrieved Context", prompt)
        self.assertIn("[RAG] Use ST_Contains for point-in-polygon checks.", prompt)
        self.assertIn("## Keyword Context", prompt)
        self.assertIn("[KEYWORD] pois.geom, districts.geom", prompt)

    def test_metadata_is_not_rendered_but_still_passed_to_internal_components(self):
        provider = StaticSampleDataProvider("- dataset1_ada_lakes\n  {\"name\": \"太湖\", \"shape\": \"<geometry>\"}")
        builder = prompt_builder_module.PromptBuilder({"sample_data_provider": provider})
        prompt = builder.build_prompt(
            question="What is the area of Lake Tai?",
            schema="table dataset1_ada_lakes(name text, shape geometry)",
            config_type="base",
            dataset_name="spatialsql_pg",
            metadata={
                "split": "dataset1_ada",
                "level": "L0",
                "family": "single_table",
                "category": "single table",
                "output_type": "scalar(column='area')",
                "expected_columns": ["area"],
            },
        )

        self.assertEqual(provider.calls[0]["metadata"]["split"], "dataset1_ada")
        self.assertNotIn("sample_level", prompt)
        self.assertNotIn("single_table", prompt)
        self.assertNotIn("expected_output_type", prompt)
        self.assertNotIn("expected_columns", prompt)
        self.assertNotIn("geometry_columns", prompt)

    def test_prompt_enhanced_uses_dataset_specific_template(self):
        registry = FakePromptEnhancementRegistry(
            overrides={
                "spatialsql_pg": {
                    "template_path": "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
                    "include_sample_data": True,
                    "use_dataset_context": True,
                }
            },
            grounding_blocks={
                ("spatialsql_pg", "dataset1_ada", "ada05"): (
                    "- Chinese question: 长江在湖北省境内的长度是多少？只需给出长度。\n"
                    "- English evidence: The Yangtze River is composed of multiple sections of the same name.\n"
                    "- Chinese value grounding: 长江以'长江'为名称表示，湖北省以'湖北省'为名称表示。"
                )
            },
        )
        builder = _build_prompt_builder(
            extra_config={
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
                "prompt_enhancement_registry": registry,
            }
        )

        prompt = builder.build_prompt(
            question="What is the length of the Yangtze River within Hubei Province?",
            schema=(
                "table dataset1_ada_provinces(name text, shape geometry)\n"
                "table dataset1_ada_rivers(name text, shape geometry)"
            ),
            config_type="prompt_enhanced",
            dataset_name="spatialsql_pg",
            metadata={"split": "dataset1_ada", "source_id": "ada05"},
        )

        self.assertIn("## SQL Construction Guidelines", prompt)
        self.assertNotIn("Use standard PostgreSQL for non-spatial questions and PostGIS only when spatial reasoning is required", prompt)
        self.assertNotIn("spatial predicates", prompt)
        self.assertNotIn("ST_Intersection(...)", prompt)
        self.assertNotIn("ST_DWithin", prompt)
        self.assertNotIn("## Grounding Hints", prompt)
        self.assertNotIn("## Retrieved Context", prompt)
        self.assertNotIn("## Keyword Context", prompt)
        self.assertNotIn("## Schema Semantics", prompt)
        self.assertNotIn("Chinese question", prompt)
        self.assertNotIn("湖北省", prompt)
        self.assertIn("## Content Information", prompt)
        self.assertIn("- sample_table", prompt)
        self.assertIn("```sql\nSELECT ...\n```", prompt)
        self.assertIn("PostgreSQL + PostGIS SELECT statement or a WITH query whose final statement is a SELECT statement", prompt)
        self.assertIn("Do not include SQL comments, explanations, JSON, XML tags, natural language, or any text outside the code fence.", prompt)
        self.assertNotIn("The first token must be SELECT or WITH.", prompt)
        self.assertEqual(
            registry.resolve_calls,
            ["spatialsql_pg"],
        )
        self.assertEqual(
            registry.grounding_calls,
            [("spatialsql_pg", "dataset1_ada", "ada05")],
        )


if __name__ == "__main__":
    unittest.main()

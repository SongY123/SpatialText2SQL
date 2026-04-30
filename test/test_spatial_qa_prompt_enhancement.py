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
_ensure_package("src.prompting", ROOT / "src" / "prompting")
_ensure_package(
    "src.prompting.prompt_enhancements",
    ROOT / "src" / "prompting" / "prompt_enhancements",
)

registry_module = _load_module(
    "src.prompting.prompt_enhancements.registry",
    ROOT / "src" / "prompting" / "prompt_enhancements" / "registry.py",
)
prompt_builder_module = _load_module(
    "src.prompting.prompt_builder",
    ROOT / "src" / "prompting" / "prompt_builder.py",
)


class StaticSampleDataProvider:
    def __init__(self, sample_text: str):
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


class SpatialQAPromptEnhancementTests(unittest.TestCase):
    def test_registry_exposes_spatial_qa_override_level_grounding_and_schema_semantics(self):
        registry = registry_module.PromptEnhancementRegistry(ROOT)

        override = registry.resolve_dataset_override("spatial_qa")
        grounding = registry.build_grounding_block("spatial_qa", {"level": 3})
        schema_semantics = registry.build_schema_semantics_block(
            "spatial_qa",
            {"level": 3},
            "table states(name text, region text, geom geometry)\n"
            "table poi(gid integer, name text, fclass text, geom geometry)",
        )

        self.assertEqual(
            override,
            {
                "template_path": "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
                "include_sample_data": True,
                "use_dataset_context": True,
            },
        )
        self.assertIn("Spatial QA difficulty level: 3", grounding)
        self.assertIn("per entity", grounding)
        self.assertIn("aggregation logic", grounding)
        self.assertNotIn("ST_", grounding)
        self.assertNotIn("LEFT JOIN", grounding)
        self.assertIn("states: U.S. Census state boundaries", schema_semantics)
        self.assertIn("states.region: U.S. Census region label.", schema_semantics)
        self.assertIn("poi: OpenStreetMap points of interest", schema_semantics)
        self.assertIn("poi.fclass: OpenStreetMap feature class or category label.", schema_semantics)

    def test_prompt_enhanced_uses_spatial_qa_template_and_keeps_sample_data(self):
        provider = StaticSampleDataProvider(
            "- states\n"
            "  {\"name\": \"Ohio\", \"region\": \"Midwest\", \"geom\": \"<geometry>\"}\n"
            "- counties\n"
            "  {\"name\": \"Winn\", \"geom\": \"<geometry>\"}"
        )
        builder = prompt_builder_module.PromptBuilder(
            {
                "project_root": ROOT,
                "sample_data_provider": provider,
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
            question="Count the number of points of interest within each state and list the states ordered by the count descending.",
            schema=(
                "table states(name text, region text, geom geometry)\n"
                "table poi(gid integer, name text, fclass text, geom geometry)"
            ),
            config_type="prompt_enhanced",
            dataset_name="spatial_qa",
            metadata={"level": 3},
        )

        self.assertIn("## SQL Construction Guidelines", prompt)
        self.assertNotIn("## Schema Semantics", prompt)
        self.assertNotIn("states: U.S. Census state boundaries", prompt)
        self.assertNotIn("states.region: U.S. Census region label.", prompt)
        self.assertNotIn("poi.fclass: OpenStreetMap feature class or category label.", prompt)
        self.assertIn("## Content Information", prompt)
        self.assertIn("\"region\": \"Midwest\"", prompt)
        self.assertNotIn("## Grounding Hints", prompt)
        self.assertNotIn("## Retrieved Context", prompt)
        self.assertNotIn("## Keyword Context", prompt)
        self.assertNotIn("Spatial QA difficulty level: 3", prompt)
        self.assertIn("Return only the columns or aggregate values needed", prompt)
        self.assertNotIn("ST_", prompt)
        self.assertNotIn("LEFT JOIN", prompt)
        self.assertIn("Return only one SQL code block in the following format:", prompt)
        self.assertIn("```sql\nSELECT ...\n```", prompt)
        self.assertIn("PostgreSQL + PostGIS SELECT statement or a WITH query whose final statement is a SELECT statement", prompt)
        self.assertIn("Do not include SQL comments, explanations, JSON, XML tags, natural language, or any text outside the code fence.", prompt)
        self.assertNotIn("The first token must be SELECT or WITH.", prompt)
        self.assertEqual(provider.calls[0]["metadata"]["level"], 3)

    def test_base_prompt_for_spatial_qa_remains_default(self):
        provider = StaticSampleDataProvider("- poi\n  {\"name\": \"Bench\", \"fclass\": \"bench\"}")
        builder = prompt_builder_module.PromptBuilder(
            {
                "project_root": ROOT,
                "sample_data_provider": provider,
            }
        )

        prompt = builder.build_prompt(
            question="Find all points of interest classified as a bench.",
            schema="table poi(osm_id integer, name text, fclass text, geom geometry)",
            config_type="base",
            dataset_name="spatial_qa",
            metadata={"level": 1},
        )

        self.assertNotIn("## Grounding Hints", prompt)
        self.assertNotIn("Spatial QA difficulty level", prompt)
        self.assertIn("## Sample Data", prompt)
        self.assertIn("Return one PostgreSQL + PostGIS SQL query only.", prompt)

    def test_spatialsql_pg_override_still_available(self):
        registry = registry_module.PromptEnhancementRegistry(ROOT)

        override = registry.resolve_dataset_override("spatialsql_pg")

        self.assertEqual(
            override["template_path"],
            "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
        )
        self.assertTrue(override["include_sample_data"])
        self.assertTrue(override["use_dataset_context"])

    def test_floodsql_pg_override_is_available(self):
        registry = registry_module.PromptEnhancementRegistry(ROOT)

        override = registry.resolve_dataset_override("floodsql_pg")

        self.assertEqual(
            override["template_path"],
            "prompts/prompt_enhancements/text2sql_prompt_enhanced.txt",
        )
        self.assertTrue(override["include_sample_data"])
        self.assertTrue(override["use_dataset_context"])


if __name__ == "__main__":
    unittest.main()

import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


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


try:
    import yaml as _yaml  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    _yaml = types.ModuleType("yaml")
    _yaml.safe_load = lambda stream: json.loads(stream.read())
    sys.modules["yaml"] = _yaml

try:
    import openpyxl as _openpyxl  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    sys.modules["openpyxl"] = types.ModuleType("openpyxl")

try:
    import psycopg2 as _psycopg2  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.connect = lambda *args, **kwargs: None
    sys.modules["psycopg2"] = psycopg2_stub


_ensure_package("src", ROOT / "src")
_ensure_package("src.datasets", ROOT / "src" / "datasets")
_ensure_package("src.datasets.loaders", ROOT / "src" / "datasets" / "loaders")
_ensure_package("src.sql", ROOT / "src" / "sql")
_ensure_package("src.pipeline", ROOT / "src" / "pipeline")

_load_module("src.datasets.base", ROOT / "src" / "datasets" / "base.py")
_load_module("src.datasets.path_utils", ROOT / "src" / "datasets" / "path_utils.py")
_load_module(
    "src.datasets.loaders.spatial_qa_loader",
    ROOT / "src" / "datasets" / "loaders" / "spatial_qa_loader.py",
)
_load_module(
    "src.datasets.loaders.spatial_sql_loader",
    ROOT / "src" / "datasets" / "loaders" / "spatial_sql_loader.py",
)
_load_module("src.sql.schema_extractor", ROOT / "src" / "sql" / "schema_extractor.py")

processing_module = _load_module(
    "src.datasets.processing",
    ROOT / "src" / "datasets" / "processing.py",
)
pipeline_module = _load_module(
    "src.pipeline.main",
    ROOT / "src" / "pipeline" / "main.py",
)


class PreprocessedSchemaCompactionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)
        self.config_dir = self.temp_path / "config"
        self.config_dir.mkdir()

        (self.config_dir / "dataset_config.yaml").write_text(
            json.dumps(
                {
                    "default_dataset": "spatialsql_pg",
                    "datasets": {
                        "spatialsql_pg": {
                            "loader_class": "SpatialSQLLoader",
                            "database": "spatial_sql",
                            "grouping": {
                                "fields": ["split"],
                                "values": {"split": ["dataset1_ada"]},
                            },
                        }
                    },
                    "preprocessing": {
                        "schema_cache_dir": "data/schemas",
                        "output_dir": str(self.temp_path / "data" / "preprocessed"),
                    },
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "db_config.yaml").write_text(
            json.dumps(
                {
                    "database": {},
                    "databases": {
                        "spatial_sql": {
                            "database": "spatial_sql",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "model_config.yaml").write_text(
            json.dumps(
                {
                    "default_models": ["qwen3-8b"],
                    "default_backend": "vllm",
                }
            ),
            encoding="utf-8",
        )
        (self.config_dir / "eval_config.yaml").write_text(
            json.dumps(
                {
                    "default_configs": ["base"],
                    "results": {
                        "output_dir": str(self.temp_path / "results"),
                        "tasks_dir": str(self.temp_path / "results" / "tasks"),
                        "benchmarks_dir": str(self.temp_path / "results" / "benchmarks"),
                        "sessions_dir": str(self.temp_path / "results" / "sessions"),
                    },
                }
            ),
            encoding="utf-8",
        )

    def test_preprocessor_saves_schema_reference_instead_of_inline_schema(self):
        preprocessor = processing_module.DataPreprocessor(
            dataset_config_path=str(self.config_dir / "dataset_config.yaml"),
            db_config_path=str(self.config_dir / "db_config.yaml"),
        )
        output_dir = self.temp_path / "data" / "preprocessed" / "spatialsql_pg"
        output_dir.mkdir(parents=True, exist_ok=True)

        extracted_data = [
            {
                "id": 1,
                "question": "q",
                "gold_sql": "SELECT 1;",
                "metadata": {"split": "dataset1_ada"},
                "schema": "INLINE_SCHEMA_SHOULD_BE_REMOVED",
            }
        ]

        preprocessor._save_single_file(
            extracted_data=extracted_data,
            schema="SCHEMA_TEXT",
            schema_file="data/schemas/spatial_sql_schema.txt",
            output_dir=str(output_dir),
            dataset_name="spatialsql_pg",
        )

        saved = json.loads(
            (output_dir / "samples.json").read_text(encoding="utf-8")
        )
        self.assertEqual(saved[0]["schema_file"], "data/schemas/spatial_sql_schema.txt")
        self.assertEqual(saved[0]["dataset"], "spatialsql_pg")
        self.assertNotIn("schema", saved[0])

    def test_pipeline_hydrates_schema_from_schema_file_reference(self):
        schema_dir = self.temp_path / "data" / "schemas"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_text = "-- schema\nCREATE TABLE demo(id integer);"
        (schema_dir / "spatial_sql_schema.txt").write_text(
            schema_text,
            encoding="utf-8",
        )

        dataset_dir = self.temp_path / "data" / "preprocessed" / "spatialsql_pg"
        (dataset_dir / "dataset1").mkdir(parents=True, exist_ok=True)
        (dataset_dir / "dataset1" / "ada_samples.json").write_text(
            json.dumps(
                [
                    {
                        "id": 1,
                        "question": "q",
                        "gold_sql": "SELECT 1;",
                        "metadata": {"split": "dataset1_ada"},
                        "schema_file": "data/schemas/spatial_sql_schema.txt",
                    }
                ]
            ),
            encoding="utf-8",
        )

        args = SimpleNamespace(
            config_dir=str(self.config_dir),
            dataset="spatialsql_pg",
            models=None,
            backend=None,
            configs=None,
            resume=True,
            overwrite=False,
            preprocess=False,
            build_rag=False,
            inference=False,
            evaluate=False,
        )
        pipeline = pipeline_module.MainPipeline(args)
        pipeline.project_root = str(self.temp_path)

        loaded = pipeline._load_preprocessed_data()

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["schema"], schema_text)
        self.assertEqual(
            loaded[0]["schema_file"],
            "data/schemas/spatial_sql_schema.txt",
        )


if __name__ == "__main__":
    unittest.main()

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.finetune.config import DEFAULT_TRL_FINETUNE_CONFIG_PATH
from src.finetune.config import (
    FinetuneDataConfig,
    FinetuneModelConfig,
    FinetuneRuntimeConfig,
    load_trl_finetune_config,
    override_trl_finetune_config,
)
from src.finetune.cli import (
    _apply_runtime_environment,
    _build_accelerate_command,
    _effective_num_processes,
    _validate_deepspeed_setup,
)
from src.finetune.dataset import SpatialText2SQLDatasetBuilder
from src.finetune.formatter import NL2SQLAlpacaFormatter
from src.finetune.io import write_alpaca_finetune_samples
from src.finetune.models import AlpacaFinetuneSample, RawFinetuneSample
from src.finetune.prompting import FinetunePromptRenderer
from src.finetune.trainer import TRLFullFinetuner


class TRLFinetuneTests(unittest.TestCase):
    def test_default_finetune_config_path_matches_repo_config(self):
        self.assertTrue(str(DEFAULT_TRL_FINETUNE_CONFIG_PATH).endswith("config/finetune.yaml"))

    def test_default_finetune_model_matches_repo_default(self):
        self.assertEqual(FinetuneModelConfig().model_name_or_path, "Qwen/Qwen2.5-Coder-7B-Instruct")

    def test_runtime_gpu_indices_override_sets_visible_devices(self):
        config = override_trl_finetune_config(
            load_trl_finetune_config(),
            runtime={"nvidia_gpu_indices": [2, 5]},
        )
        original_cuda = os.environ.get("CUDA_VISIBLE_DEVICES")
        original_nvidia = os.environ.get("NVIDIA_VISIBLE_DEVICES")
        try:
            _apply_runtime_environment(config)
            self.assertEqual(os.environ.get("CUDA_VISIBLE_DEVICES"), "2,5")
            self.assertEqual(os.environ.get("NVIDIA_VISIBLE_DEVICES"), "2,5")
        finally:
            if original_cuda is None:
                os.environ.pop("CUDA_VISIBLE_DEVICES", None)
            else:
                os.environ["CUDA_VISIBLE_DEVICES"] = original_cuda
            if original_nvidia is None:
                os.environ.pop("NVIDIA_VISIBLE_DEVICES", None)
            else:
                os.environ["NVIDIA_VISIBLE_DEVICES"] = original_nvidia

    def test_default_runtime_uses_all_eight_gpus_with_accelerate(self):
        runtime = FinetuneRuntimeConfig()
        self.assertEqual(runtime.nvidia_gpu_indices, list(range(8)))
        self.assertEqual(runtime.distributed_backend, "accelerate")

    def test_accelerate_command_uses_input_and_alpaca_output(self):
        config = override_trl_finetune_config(
            load_trl_finetune_config(),
            runtime={"nvidia_gpu_indices": [0, 1], "num_processes": 2},
            training={"deepspeed_config_path": "configs/ds_zero2.json"},
        )
        args = SimpleNamespace(config="config/finetune.yaml")
        command = _build_accelerate_command(config, args)
        self.assertIn("accelerate.commands.launch", command)
        self.assertIn("--multi_gpu", command)
        self.assertIn("--mixed_precision", command)
        self.assertIn("bf16", command)
        self.assertIn("--dynamo_backend", command)
        self.assertIn(config.runtime.dynamo_backend, command)
        self.assertIn("--input", command)
        self.assertIn(config.data.input_path, command)
        self.assertIn("--alpaca-output", command)
        self.assertIn(config.data.alpaca_output_path, command)
        self.assertIn("--num_processes", command)
        self.assertIn("2", command)
        self.assertIn("--nvidia-gpu-indices", command)
        self.assertIn("0,1", command)
        self.assertIn("--deepspeed-config-path", command)

    def test_repo_default_finetune_config_enables_zero3(self):
        config = load_trl_finetune_config()
        self.assertTrue(config.training.deepspeed_config_path.endswith("config/deepspeed/zero3_bf16.json"))
        self.assertTrue(Path(config.training.deepspeed_config_path).is_file())

    def test_validate_deepspeed_setup_requires_existing_config(self):
        config = override_trl_finetune_config(
            load_trl_finetune_config(),
            training={"deepspeed_config_path": "/tmp/does-not-exist-zero3.json"},
        )
        with self.assertRaises(FileNotFoundError):
            _validate_deepspeed_setup(config)

    def test_raw_finetune_sample_accepts_nl2sql_metadata(self):
        row = RawFinetuneSample.from_dict(
            {
                "question_id": "nyc_0001_0001",
                "database_id": "nyc_0001",
                "city": "new york",
                "question": "Which parks intersect schools?",
                "sql": "SELECT p.name FROM parks p JOIN schools s ON ST_Intersects(p.geom, s.geom)",
                "source_difficulty_level": "medium",
                "used_tables": ["parks", "schools"],
                "used_columns": ["name", "geom"],
                "used_spatial_functions": ["ST_Intersects"],
                "sql_features": {"tables": ["parks", "schools"]},
                "metadata": {
                    "quality_control": {"passed": True},
                    "database_context": {"tables": []},
                },
            }
        )
        self.assertEqual(row.difficulty, "medium")
        self.assertIn("database_context", row.metadata)

    def test_prompt_renderer_includes_required_sections(self):
        renderer = FinetunePromptRenderer(
            task_description="Translate the question to SQL.",
            max_representative_rows=3,
        )
        instruction = renderer.render_instruction()
        input_text = renderer.render_input(
            question="Which parks intersect schools?",
            schema_lines=["- parks(id integer, geom geometry(Point,4326))"],
            representative_values={"parks": [{"id": 1, "geom": "POINT"}]},
        )
        prompt = renderer.compose_prompt(instruction, input_text)
        self.assertIn("## Task Description", instruction)
        self.assertIn("## Response Requirements", instruction)
        self.assertIn("```sql``` code block", instruction)
        self.assertNotIn("reasoning summary", instruction)
        self.assertIn("## Schema", input_text)
        self.assertIn("## Representative Values", input_text)
        self.assertIn("## Question", input_text)
        self.assertNotIn("## Spatial Field Metadata", prompt)
        self.assertIn("Which parks intersect schools?", prompt)
        self.assertTrue(prompt.endswith("## Response\n"))

    def test_alpaca_formatter_splits_instruction_input_and_output(self):
        runtime_metadata = {
            "tables": [
                {
                    "table_name": "parks",
                    "columns": [
                        {"column_name": "id", "column_type": "integer"},
                        {"column_name": "name", "column_type": "text"},
                        {"column_name": "geom", "column_type": "geometry(Point,4326)"},
                    ],
                    "spatial_fields": [
                        {
                            "column_name": "geom",
                            "column_type": "geometry(Point,4326)",
                            "spatial_type": "geometry",
                            "geometry_type": "POINT",
                            "srid": 4326,
                        }
                    ],
                    "representative_values": [{"id": 1, "name": "alpha", "geom": "POINT (0 0)"}],
                }
            ]
        }
        formatter = NL2SQLAlpacaFormatter(
            data_config=FinetuneDataConfig(
                input_path="",
                alpaca_output_path="",
                task_description="Translate to SQL.",
                question_id_start=0,
                max_representative_rows=3,
            ),
        )
        raw = RawFinetuneSample.from_dict(
            {
                "question_id": "nyc_0001_0001",
                "database_id": "nyc_0001",
                "city": "new york",
                "question": "Which park names should be returned?",
                "sql": "SELECT name FROM parks LIMIT 5",
                "source_difficulty_level": "easy",
                "sql_reasoning_summary": "Use the parks table and return the name column.",
                "used_tables": ["parks"],
                "used_columns": ["name"],
                "metadata": {
                    "database_context": runtime_metadata,
                    "quality_control": {"passed": True},
                },
            }
        )
        rows = formatter.format_samples([raw])
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0], AlpacaFinetuneSample)
        self.assertIn("## Task Description", rows[0].instruction)
        self.assertIn("## Schema", rows[0].input_text)
        self.assertNotIn("## Spatial Field Metadata", rows[0].input_text)
        self.assertEqual(rows[0].output_text.strip(), "```sql\nSELECT name FROM parks LIMIT 5\n```")
        self.assertIn("```sql", rows[0].output_text)
        self.assertIn("SELECT name FROM parks LIMIT 5", rows[0].output_text)
        self.assertEqual(set(rows[0].to_dict().keys()), {"instruction", "input", "output"})

    def test_alpaca_formatter_uses_full_database_context_not_used_tables_subset(self):
        formatter = NL2SQLAlpacaFormatter(
            data_config=FinetuneDataConfig(
                input_path="",
                alpaca_output_path="",
                task_description="Translate to SQL.",
                question_id_start=0,
                max_representative_rows=3,
            ),
        )
        raw = RawFinetuneSample.from_dict(
            {
                "question_id": "nyc_0001_0002",
                "database_id": "nyc_0001",
                "city": "new york",
                "question": "Which park names should be returned?",
                "sql": "SELECT name FROM parks LIMIT 5",
                "source_difficulty_level": "easy",
                "used_tables": ["parks"],
                "used_columns": ["name"],
                "metadata": {
                    "database_context": {
                        "schema_ddls": [
                            "CREATE TABLE parks (\n    id integer,\n    name text\n);",
                            "CREATE TABLE schools (\n    id integer,\n    name text\n);",
                        ],
                        "representative_values": {
                            "parks": [{"id": 1, "name": "alpha"}],
                            "schools": [{"id": 9, "name": "ps 1"}],
                        },
                    }
                },
            }
        )
        rows = formatter.format_samples([raw])
        self.assertEqual(len(rows), 1)
        self.assertIn("CREATE TABLE parks", rows[0].input_text)
        self.assertIn("CREATE TABLE schools", rows[0].input_text)
        self.assertIn('"parks"', rows[0].input_text)
        self.assertIn('"schools"', rows[0].input_text)

    def test_alpaca_writer_emits_instruction_input_output_only(self):
        row = AlpacaFinetuneSample(
            instruction="Do the task.",
            input_text="## Schema\n- parks(id integer)\n\n## Representative Values\n{}\n\n## Question\nWhich parks?",
            output_text="```sql\nSELECT id FROM parks\n```",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "alpaca.jsonl"
            write_alpaca_finetune_samples(str(output_path), [row])
            payload = json.loads(output_path.read_text(encoding="utf-8").strip())
        self.assertEqual(payload, row.to_dict())

    def test_completion_mask_is_built_from_single_tokenization_pass(self):
        class FakeTokenizer:
            is_fast = True

            def __call__(self, text, add_special_tokens=True, return_offsets_mapping=False, return_special_tokens_mask=False):
                input_ids = list(range(len(text)))
                payload = {"input_ids": input_ids}
                if return_offsets_mapping:
                    payload["offset_mapping"] = [(index, index + 1) for index in range(len(text))]
                if return_special_tokens_mask:
                    payload["special_tokens_mask"] = [0] * len(text)
                return payload

        full_text = "PROMPT## Response\nSQL"
        payload = TRLFullFinetuner._tokenize_with_completion_mask(
            tokenizer=FakeTokenizer(),
            full_text=full_text,
            completion_start=len("PROMPT## Response\n"),
        )
        self.assertEqual(len(payload["input_ids"]), len(full_text))
        self.assertEqual(payload["completion_mask"][-3:], [1, 1, 1])
        self.assertEqual(payload["completion_mask"][: len("PROMPT## Response\n")], [0] * len("PROMPT## Response\n"))

    def test_persist_training_artifacts_runs_collective_model_save_but_main_process_only_side_effects(self):
        class FakeAccelerator:
            def __init__(self):
                self.calls = 0
                self.is_main_process = False

            def wait_for_everyone(self):
                self.calls += 1

        class FakeTrainer:
            def __init__(self):
                self.accelerator = FakeAccelerator()
                self.saved_model = 0
                self.saved_state = 0

            def is_world_process_zero(self):
                return False

            def save_model(self, _):
                self.saved_model += 1

            def save_state(self):
                self.saved_state += 1

        class FakeTokenizer:
            def __init__(self):
                self.saved = 0

            def save_pretrained(self, _):
                self.saved += 1

        finetuner = TRLFullFinetuner(load_trl_finetune_config())
        trainer = FakeTrainer()
        tokenizer = FakeTokenizer()
        with tempfile.TemporaryDirectory() as temp_dir:
            finetuner._persist_training_artifacts(
                trainer=trainer,
                tokenizer=tokenizer,
                output_dir=Path(temp_dir),
                metrics={"train_loss": 1.0},
            )
            self.assertFalse((Path(temp_dir) / "train_metrics.json").exists())
        self.assertEqual(trainer.saved_model, 1)
        self.assertEqual(trainer.saved_state, 0)
        self.assertEqual(tokenizer.saved, 0)
        self.assertEqual(trainer.accelerator.calls, 3)

    def test_resolve_warmup_steps_uses_ratio_when_explicit_steps_are_zero(self):
        config = override_trl_finetune_config(
            load_trl_finetune_config(),
            training={
                "per_device_train_batch_size": 2,
                "gradient_accumulation_steps": 2,
                "num_train_epochs": 3.0,
                "max_steps": -1,
                "warmup_steps": 0,
                "warmup_ratio": 0.1,
            },
        )
        finetuner = TRLFullFinetuner(config)
        original_world_size = os.environ.get("WORLD_SIZE")
        try:
            os.environ["WORLD_SIZE"] = "2"
            self.assertEqual(finetuner._estimate_total_training_steps(17), 9)
            self.assertEqual(finetuner._resolve_warmup_steps(17), 1)
        finally:
            if original_world_size is None:
                os.environ.pop("WORLD_SIZE", None)
            else:
                os.environ["WORLD_SIZE"] = original_world_size

    def test_gradient_accumulation_prefers_explicit_deepspeed_value(self):
        config = override_trl_finetune_config(
            load_trl_finetune_config(),
            training={
                "gradient_accumulation_steps": 3,
                "deepspeed_config_path": "config/deepspeed/zero3_bf16.json",
            },
        )
        finetuner = TRLFullFinetuner(config)
        self.assertEqual(finetuner._resolve_gradient_accumulation_steps(), 16)

    def test_dataset_builder_normalizes_question_id_and_difficulty(self):
        runtime_metadata = {
            "tables": [
                {
                    "table_name": "table_1",
                    "columns": [
                        {"column_name": "id", "column_type": "integer"},
                        {"column_name": "name", "column_type": "text"},
                        {"column_name": "geom", "column_type": "geometry(Point,4326)"},
                    ],
                    "spatial_fields": [
                        {
                            "column_name": "geom",
                            "column_type": "geometry(Point,4326)",
                            "spatial_type": "geometry",
                            "geometry_type": "POINT",
                            "srid": 4326,
                        }
                    ],
                    "representative_values": {"name": ["alpha"], "geom": ["POINT (0 0)"]},
                }
            ]
        }
        builder = SpatialText2SQLDatasetBuilder(
            data_config=FinetuneDataConfig(
                input_path="",
                alpaca_output_path="",
                task_description="Translate to SQL.",
                question_id_start=0,
                max_representative_rows=3,
            ),
        )
        raw = RawFinetuneSample(
            question_id="nyc_0001_q_001",
            database_id="nyc_0001",
            city="nyc",
            sql="SELECT name FROM table_1 LIMIT 5",
            question="Which names should be returned?",
            difficulty="medium",
            instruction="Do the task.",
            input_text="## Schema\n- table_1(id integer, name text, geom geometry(Point,4326))\n\n## Representative Values\n{}\n\n## Question\nWhich names should be returned?",
            output_text="Return the names.\n\n```sql\nSELECT name FROM table_1 LIMIT 5\n```",
            sql_reasoning_summary="Return the names.",
            used_tables=["table_1"],
            used_columns=["name"],
            used_spatial_functions=["ST_Buffer"],
            sql_features={"limit": 5},
            metadata={"database_context": runtime_metadata},
        )
        prepared = builder.prepare_samples([raw])
        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared[0].question_id, 0)
        self.assertEqual(prepared[0].difficulty, "medium")
        self.assertIn("```sql", prepared[0].completion)
        self.assertEqual(prepared[0].sql_reasoning_summary, "Return the names.")
        self.assertEqual(prepared[0].instruction, "Do the task.")
        self.assertIn("## Schema", prepared[0].input_text)
        self.assertIn("## Representative Values", prepared[0].prompt)
        self.assertNotIn("## Spatial Field Metadata", prepared[0].prompt)

    def test_dataset_builder_uses_embedded_nl2sql_metadata_only(self):
        runtime_metadata = {
            "tables": [
                {
                    "table_name": "parks",
                    "columns": [
                        {"column_name": "id", "column_type": "integer"},
                        {"column_name": "name", "column_type": "text"},
                        {"column_name": "geom", "column_type": "geometry(Point,4326)"},
                    ],
                    "spatial_fields": [
                        {
                            "column_name": "geom",
                            "column_type": "geometry(Point,4326)",
                            "spatial_type": "geometry",
                            "geometry_type": "POINT",
                            "srid": 4326,
                        }
                    ],
                    "representative_values": [{"id": 1, "name": "alpha", "geom": "POINT (0 0)"}],
                }
            ]
        }
        builder = SpatialText2SQLDatasetBuilder(
            data_config=FinetuneDataConfig(
                input_path="",
                alpaca_output_path="",
                task_description="Translate to SQL.",
                question_id_start=0,
                max_representative_rows=3,
            ),
        )
        raw = RawFinetuneSample.from_dict(
            {
                "question_id": "nyc_0001_0001",
                "database_id": "nyc_0001",
                "city": "new york",
                "question": "Which park names should be returned?",
                "sql": "SELECT name FROM parks LIMIT 5",
                "source_difficulty_level": "easy",
                "sql_reasoning_summary": "Use the parks table and return the name column.",
                "used_tables": ["parks"],
                "used_columns": ["name"],
                "metadata": {
                    "database_context": runtime_metadata,
                    "quality_control": {"passed": True},
                },
            }
        )
        prepared = builder.prepare_samples([raw])
        self.assertEqual(len(prepared), 1)
        self.assertIn("- parks(id integer, name text, geom geometry(Point,4326))", prepared[0].prompt)
        self.assertNotIn("## Spatial Field Metadata", prepared[0].prompt)
        self.assertIn("```sql", prepared[0].completion)

    def test_dataset_builder_does_not_fallback_to_database_when_metadata_missing(self):
        builder = SpatialText2SQLDatasetBuilder(
            data_config=FinetuneDataConfig(
                input_path="",
                alpaca_output_path="",
                task_description="Translate to SQL.",
                question_id_start=0,
                max_representative_rows=3,
            ),
        )
        raw = RawFinetuneSample.from_dict(
            {
                "question_id": "nyc_0001_0002",
                "database_id": "nyc_0001",
                "question": "Which park names should be returned?",
                "sql": "SELECT name FROM parks LIMIT 5",
                "source_difficulty_level": "easy",
                "sql_reasoning_summary": "Use the parks table and return the name column.",
                "used_tables": ["parks"],
                "used_columns": ["name"],
                "metadata": {"quality_control": {"passed": True}},
            }
        )
        prepared = builder.prepare_samples([raw])
        self.assertEqual(len(prepared), 1)
        self.assertIn("No schema available.", prepared[0].prompt)
        self.assertIn("{}", prepared[0].prompt)
        self.assertIn("```sql", prepared[0].completion)


if __name__ == "__main__":
    unittest.main()

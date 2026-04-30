"""主流程控制 - 参数解析，流程编排，结果汇总。"""
import argparse
import hashlib
from collections import Counter
from datetime import datetime
import json
import os
import shutil
import sys
from typing import Any, Dict, List, Optional

import yaml

from src.datasets.path_utils import (
    get_expected_preprocessed_files,
    get_preprocessed_output_dir,
)


class MainPipeline:
    """主流程控制器。"""

    DEFAULT_ALL_DATASETS = ["spatial_qa", "spatialsql_pg", "floodsql_pg"]

    def __init__(self, args):
        self._validate_action_selection(args)
        self.args = args
        self.config_dir = args.config_dir
        self.project_root = os.path.abspath(os.path.join(self.config_dir, os.pardir))

        self.dataset_config = self._load_config("dataset_config.yaml")
        self.db_config = self._load_config("db_config.yaml")
        self.model_config = self._load_config("model_config.yaml")
        self.eval_config = self._load_config("eval_config.yaml")

        self.backend = args.backend or self.model_config.get("default_backend", "vllm")
        self.enable_prediction_postprocess = bool(
            getattr(args, "enable_prediction_postprocess", False)
        )

        self.dataset_names = self._resolve_dataset_names(args.dataset)
        self.model_names = self._resolve_model_names(args.models)
        if args.configs:
            self.config_types = args.configs
        else:
            self.config_types = self.eval_config.get(
                "default_configs",
                ["base", "rag", "keyword", "full"],
            )
        self._validate_config_types()

        self._apply_runtime_eval_overrides()

        self.dataset_name = self.dataset_names[0]
        self.dataset_info_dict = self.dataset_config["datasets"][self.dataset_name]
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        self.run_started_at = datetime.now().isoformat(timespec="seconds")
        self._session_predictions: Dict[str, Dict[str, Any]] = {}
        self._session_evaluations: Dict[str, Dict[str, Any]] = {}
        self._session_report_paths: List[str] = []
        self._dataset_index_status: Dict[str, Any] = {}
        self._evaluation_validation: Dict[str, Any] = {}
        self._benchmark_prompt_checks: List[Dict[str, Any]] = []
        self._benchmark_prompt_issues: List[Dict[str, Any]] = []

    @staticmethod
    def _validate_action_selection(args) -> None:
        if not getattr(args, "benchmark", False):
            return

        conflicting_actions = []
        if getattr(args, "preprocess", False):
            conflicting_actions.append("--preprocess")
        if getattr(args, "build_rag", False):
            conflicting_actions.append("--build-rag")
        if getattr(args, "inference", False):
            conflicting_actions.append("--inference")
        if getattr(args, "evaluate", False):
            conflicting_actions.append("--evaluate")

        if conflicting_actions:
            conflicts = ", ".join(conflicting_actions)
            raise ValueError(
                "--benchmark 仅汇总各 task 的 latest 结果，"
                f"不能与 {conflicts} 同时使用"
            )

    def _load_config(self, filename: str) -> dict:
        config_path = os.path.join(self.config_dir, filename)
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _resolve_dataset_names(self, dataset_args) -> List[str]:
        available = self.dataset_config.get("datasets", {})
        if not dataset_args:
            return [self.dataset_config.get("default_dataset", "spatial_qa")]

        requested = dataset_args if isinstance(dataset_args, list) else [dataset_args]
        if "all" in requested:
            return [name for name in self.DEFAULT_ALL_DATASETS if name in available]
        return requested

    def _resolve_model_names(self, model_args) -> List[str]:
        if not model_args:
            return self.model_config.get("default_models", [])

        requested = model_args if isinstance(model_args, list) else [model_args]
        if "all" not in requested:
            return requested

        resolved = []
        for model_name, model_info in self.model_config.get("models", {}).items():
            backends = model_info.get("backends", {})
            if not backends:
                if self.backend == "transformers":
                    resolved.append(model_name)
                continue
            if self.backend in backends:
                resolved.append(model_name)
        return resolved

    def _apply_runtime_eval_overrides(self):
        prediction_postprocess = self.eval_config.setdefault("prediction_postprocess", {})
        prediction_postprocess["enable_spatialsql_normalization"] = (
            self.enable_prediction_postprocess
        )
        prediction_postprocess["enable_floodsql_normalization"] = (
            self.enable_prediction_postprocess
        )

    def _validate_config_types(self) -> None:
        available = set((self.eval_config.get("ablation_configs") or {}).keys())
        if not available:
            available = {"base", "rag", "keyword", "full"}
        unknown = [config for config in self.config_types if config not in available]
        if unknown:
            raise ValueError(
                f"未知配置类型: {unknown}. 可用配置: {sorted(available)}"
            )

    def _get_ablation_config(self, config_type: str) -> Dict[str, Any]:
        ablation_configs = self.eval_config.get("ablation_configs", {})
        config = ablation_configs.get(config_type)
        if config is not None:
            return config
        return {
            "use_rag": config_type in ["rag", "full"],
            "use_keyword": config_type in ["keyword", "full"],
            "prompt_style": "default",
        }

    def _config_uses_rag(self, config_type: str) -> bool:
        return bool(self._get_ablation_config(config_type).get("use_rag"))

    def _config_uses_keyword(self, config_type: str) -> bool:
        return bool(self._get_ablation_config(config_type).get("use_keyword"))

    def _set_dataset_context(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.dataset_info_dict = self.dataset_config["datasets"][dataset_name]

    def _get_dataset_db_config(self) -> dict:
        return self._get_dataset_db_config_by_name(self.dataset_name)

    def _get_dataset_db_config_by_name(self, dataset_name: str) -> dict:
        dataset_info = self.dataset_config["datasets"][dataset_name]
        db_name = dataset_info.get("database", "default")
        if db_name != "default" and "databases" in self.db_config:
            databases = self.db_config["databases"]
            if db_name in databases:
                return databases[db_name]
        return self.db_config.get("database", {})

    def _get_results_root(self) -> str:
        return self.eval_config.get("results", {}).get("output_dir", "results")

    def _get_tasks_root(self) -> str:
        return self.eval_config.get("results", {}).get(
            "tasks_dir",
            os.path.join(self._get_results_root(), "tasks"),
        )

    def _get_benchmarks_root(self) -> str:
        return self.eval_config.get("results", {}).get(
            "benchmarks_dir",
            os.path.join(self._get_results_root(), "benchmarks"),
        )

    def _get_sessions_root(self) -> str:
        return self.eval_config.get("results", {}).get(
            "sessions_dir",
            os.path.join(self._get_results_root(), "sessions"),
        )

    def _get_task_dir(
        self,
        model_name: str,
        config_type: str,
        dataset_name: Optional[str] = None,
    ) -> str:
        dataset = dataset_name or self.dataset_name
        return os.path.join(
            self._get_tasks_root(),
            dataset,
            self.backend,
            model_name,
            config_type,
        )

    def _get_task_latest_dir(
        self,
        model_name: str,
        config_type: str,
        dataset_name: Optional[str] = None,
    ) -> str:
        return os.path.join(
            self._get_task_dir(model_name, config_type, dataset_name=dataset_name),
            "latest",
        )

    def _get_task_history_dir(
        self,
        model_name: str,
        config_type: str,
        dataset_name: Optional[str] = None,
    ) -> str:
        return os.path.join(
            self._get_task_dir(model_name, config_type, dataset_name=dataset_name),
            "runs",
            self.run_id,
        )

    def _get_benchmark_latest_dir(self) -> str:
        return os.path.join(self._get_benchmarks_root(), "latest")

    def _get_benchmark_history_dir(self) -> str:
        return os.path.join(self._get_benchmarks_root(), "runs", self.run_id)

    @staticmethod
    def _get_prediction_filename() -> str:
        return "predictions.json"

    @staticmethod
    def _get_prediction_temp_filename() -> str:
        return "predictions_temp.json"

    @staticmethod
    def _get_prompt_filename() -> str:
        return "prompts.json"

    @staticmethod
    def _get_prompt_temp_filename() -> str:
        return "prompts_temp.json"

    @staticmethod
    def _get_evaluation_filename() -> str:
        return "evaluation.json"

    @staticmethod
    def _get_task_summary_filename() -> str:
        return "summary.json"

    def _get_task_prediction_file(
        self,
        model_name: str,
        config_type: str,
        *,
        latest: bool,
        dataset_name: Optional[str] = None,
        temporary: bool = False,
    ) -> str:
        base_dir = (
            self._get_task_latest_dir(model_name, config_type, dataset_name=dataset_name)
            if latest
            else self._get_task_history_dir(model_name, config_type, dataset_name=dataset_name)
        )
        filename = (
            self._get_prediction_temp_filename()
            if temporary
            else self._get_prediction_filename()
        )
        return os.path.join(base_dir, filename)

    def _get_task_prompt_file(
        self,
        model_name: str,
        config_type: str,
        *,
        latest: bool,
        dataset_name: Optional[str] = None,
        temporary: bool = False,
    ) -> str:
        base_dir = (
            self._get_task_latest_dir(model_name, config_type, dataset_name=dataset_name)
            if latest
            else self._get_task_history_dir(model_name, config_type, dataset_name=dataset_name)
        )
        filename = (
            self._get_prompt_temp_filename()
            if temporary
            else self._get_prompt_filename()
        )
        return os.path.join(base_dir, filename)

    def _get_task_evaluation_file(
        self,
        model_name: str,
        config_type: str,
        *,
        latest: bool,
        dataset_name: Optional[str] = None,
    ) -> str:
        base_dir = (
            self._get_task_latest_dir(model_name, config_type, dataset_name=dataset_name)
            if latest
            else self._get_task_history_dir(model_name, config_type, dataset_name=dataset_name)
        )
        return os.path.join(base_dir, self._get_evaluation_filename())

    def _get_task_summary_file(
        self,
        model_name: str,
        config_type: str,
        *,
        latest: bool,
        dataset_name: Optional[str] = None,
    ) -> str:
        base_dir = (
            self._get_task_latest_dir(model_name, config_type, dataset_name=dataset_name)
            if latest
            else self._get_task_history_dir(model_name, config_type, dataset_name=dataset_name)
        )
        return os.path.join(base_dir, self._get_task_summary_filename())

    def _find_prediction_file_for_evaluation(
        self,
        model_name: str,
        config_type: str,
        dataset_name: Optional[str] = None,
    ) -> Optional[str]:
        latest_candidates = [
            self._get_task_prediction_file(
                model_name,
                config_type,
                latest=True,
                dataset_name=dataset_name,
            ),
            self._get_task_prediction_file(
                model_name,
                config_type,
                latest=True,
                dataset_name=dataset_name,
                temporary=True,
            ),
        ]
        for candidate in latest_candidates:
            if os.path.exists(candidate):
                return candidate

        runs_root = os.path.join(
            self._get_task_dir(model_name, config_type, dataset_name=dataset_name),
            "runs",
        )
        if not os.path.isdir(runs_root):
            return None

        history_candidates = []
        for entry in os.scandir(runs_root):
            if not entry.is_dir():
                continue
            for filename in (
                self._get_prediction_filename(),
                self._get_prediction_temp_filename(),
            ):
                candidate = os.path.join(entry.path, filename)
                if os.path.exists(candidate):
                    history_candidates.append((os.path.getmtime(candidate), candidate))

        if not history_candidates:
            return None

        history_candidates.sort(key=lambda item: item[0], reverse=True)
        return history_candidates[0][1]

    def _build_task_key(self, dataset_name: str, model_name: str, config_type: str) -> str:
        return f"{dataset_name}::{model_name}::{config_type}"

    def _record_prediction_artifact(
        self,
        dataset_name: str,
        model_name: str,
        config_type: str,
        history_file: str,
        latest_file: str,
        prompt_history_file: str,
        prompt_latest_file: str,
        results: List[Dict[str, Any]],
    ):
        key = self._build_task_key(dataset_name, model_name, config_type)
        self._session_predictions[key] = {
            "dataset": dataset_name,
            "backend": self.backend,
            "model": model_name,
            "config": config_type,
            "history_file": history_file,
            "latest_file": latest_file,
            "prompt_history_file": prompt_history_file,
            "prompt_latest_file": prompt_latest_file,
            "count": len(results),
        }

    def _record_evaluation_artifact(
        self,
        dataset_name: str,
        model_name: str,
        config_type: str,
        history_file: str,
        latest_file: str,
        summary_history_file: str,
        summary_latest_file: str,
        eval_result: Dict[str, Any],
    ):
        key = self._build_task_key(dataset_name, model_name, config_type)
        overall = eval_result.get("statistics", {}).get("overall", {})
        self._session_evaluations[key] = {
            "dataset": dataset_name,
            "backend": self.backend,
            "model": model_name,
            "config": config_type,
            "history_file": history_file,
            "latest_file": latest_file,
            "history_summary_file": summary_history_file,
            "latest_summary_file": summary_latest_file,
            "total": overall.get("total", 0) or 0,
        }

    def _get_floodsql_metadata_config(self) -> dict:
        return self.eval_config.get("floodsql_pg", {}).get("metadata", {})

    def _create_rag_retriever(self):
        if self.dataset_name == "floodsql_pg":
            from src.retrieval.floodsql_metadata_retriever import FloodSQLMetadataRAGRetriever

            return FloodSQLMetadataRAGRetriever(self._get_floodsql_metadata_config())

        from src.retrieval.rag_retriever import RAGRetriever

        return RAGRetriever(self.eval_config.get("rag", {}))

    def _create_keyword_searcher(self):
        if self.dataset_name == "floodsql_pg":
            from src.retrieval.floodsql_metadata_retriever import FloodSQLMetadataKeywordSearcher

            return FloodSQLMetadataKeywordSearcher(self._get_floodsql_metadata_config())

        from src.retrieval.keyword_searcher import KeywordSearcher

        return KeywordSearcher(self.eval_config.get("keyword_search", {}))

    def run(self):
        print("\n" + "=" * 80)
        print("Spatial Text2SQL 推理评估框架")
        print("=" * 80 + "\n")

        if self.args.preprocess:
            for dataset_name in self.dataset_names:
                self._set_dataset_context(dataset_name)
                self._run_preprocessing()

        if self.args.build_rag:
            for dataset_name in self.dataset_names:
                self._set_dataset_context(dataset_name)
                self._build_rag_index()

        if self.args.inference:
            for dataset_name in self.dataset_names:
                self._set_dataset_context(dataset_name)
                self._run_inference_and_evaluation()
        elif self.args.evaluate:
            for dataset_name in self.dataset_names:
                self._set_dataset_context(dataset_name)
                self._run_evaluation_only()

        if self.args.benchmark:
            benchmark_results = self._collect_task_eval_results()
            if benchmark_results:
                self._generate_benchmark_report(benchmark_results)
            else:
                print("\n警告: benchmark 生成已跳过，未找到任何 task 评估结果")

        self._write_run_session_metadata(status="completed")

        print("\n" + "=" * 80)
        print("所有任务完成!")
        print("=" * 80 + "\n")

    def _run_preprocessing(self):
        from src.datasets.processing import DataPreprocessor

        print("\n" + "=" * 80)
        print("步骤 1: 数据预处理")
        print("=" * 80 + "\n")

        preprocessor = DataPreprocessor(
            dataset_config_path=os.path.join(self.config_dir, "dataset_config.yaml"),
            db_config_path=os.path.join(self.config_dir, "db_config.yaml"),
        )
        preprocessor.preprocess(self.dataset_name)

    def _build_rag_index(self):
        print("\n" + "=" * 80)
        print("步骤 2: 构建RAG索引")
        print("=" * 80 + "\n")

        rag_retriever = self._create_rag_retriever()
        rag_retriever.build_index()

    def _run_inference_and_evaluation(self) -> List[Dict[str, Any]]:
        from src.datasets.processing import DataLoaderFactory
        from src.evaluation.evaluator import Evaluator
        from src.inference.model_inference import ModelInference
        from src.prompting.prompt_builder import PromptBuilder

        print("\n" + "=" * 80)
        print("步骤 3: 模型推理和评估")
        print("=" * 80 + "\n")

        if not self._has_preprocessed_data():
            print(f"检测到数据集 {self.dataset_name} 缺少预处理产物，自动开始预处理...")
            self._run_preprocessing()

        preprocessed_data = self._load_preprocessed_data()

        rag_retriever = None
        keyword_searcher = None
        if any(self._config_uses_rag(config_type) for config_type in self.config_types):
            rag_retriever = self._create_rag_retriever()
            rag_retriever.build_index()

        if any(
            self._config_uses_keyword(config_type)
            for config_type in self.config_types
        ):
            keyword_searcher = self._create_keyword_searcher()
            keyword_searcher.load_documents()

        prompt_builder = PromptBuilder(self.eval_config)
        model_inference = ModelInference(
            model_config_path=os.path.join(self.config_dir, "model_config.yaml"),
            eval_config_path=os.path.join(self.config_dir, "eval_config.yaml"),
            eval_config_override=self.eval_config,
        )
        dataset_db_config = self._get_dataset_db_config()
        evaluator = Evaluator(
            db_config=dataset_db_config,
            eval_config=self.eval_config,
        )

        loader_class_name = self.dataset_info_dict["loader_class"]
        data_loader = DataLoaderFactory.create(loader_class_name, self.dataset_info_dict)
        dataset_info = data_loader.get_dataset_info()

        all_eval_results = []
        total_tasks = len(self.model_names) * len(self.config_types)
        current_task = 0

        for model_name in self.model_names:
            for config_type in self.config_types:
                current_task += 1
                run_name = model_inference.get_run_name(model_name, self.backend)
                print(f"\n[{current_task}/{total_tasks}] 处理: {run_name} - {config_type}")

                prompts = self._prepare_prompts(
                    preprocessed_data,
                    config_type,
                    prompt_builder,
                    rag_retriever,
                    keyword_searcher,
                )

                history_dir = self._get_task_history_dir(model_name, config_type)
                predictions = model_inference.run_inference(
                    model_name=model_name,
                    config_type=config_type,
                    prompts=prompts,
                    data_items=preprocessed_data,
                    save_dir=history_dir,
                    backend=self.backend,
                )
                history_prediction_file = self._get_task_prediction_file(
                    model_name,
                    config_type,
                    latest=False,
                )
                latest_prediction_file = self._get_task_prediction_file(
                    model_name,
                    config_type,
                    latest=True,
                )
                history_prompt_file = self._get_task_prompt_file(
                    model_name,
                    config_type,
                    latest=False,
                )
                latest_prompt_file = self._get_task_prompt_file(
                    model_name,
                    config_type,
                    latest=True,
                )
                self._publish_latest_file(history_prediction_file, latest_prediction_file)
                self._publish_latest_file(history_prompt_file, latest_prompt_file)
                self._record_prediction_artifact(
                    self.dataset_name,
                    model_name,
                    config_type,
                    history_prediction_file,
                    latest_prediction_file,
                    history_prompt_file,
                    latest_prompt_file,
                    predictions,
                )

                if self.args.evaluate:
                    eval_result = evaluator.evaluate(
                        predictions=predictions,
                        dataset_info=dataset_info,
                        model_name=run_name,
                        config_type=config_type,
                        output_dir=self._get_task_dir(model_name, config_type),
                        resume=getattr(self.args, "resume", False),
                        overwrite=getattr(self.args, "overwrite", False),
                    )
                    eval_result["dataset_info"] = dataset_info
                    self._save_task_evaluation_outputs(
                        evaluator=evaluator,
                        eval_result=eval_result,
                        dataset_info=dataset_info,
                        model_name=model_name,
                        config_type=config_type,
                    )
                    all_eval_results.append(eval_result)

        return all_eval_results

    def _load_preprocessed_data(self) -> list:
        preprocessing_config = self.dataset_config.get("preprocessing", {})
        dataset_dir = get_preprocessed_output_dir(preprocessing_config, self.dataset_name)

        grouping = self.dataset_info_dict.get("grouping", {})
        grouping_fields = grouping.get("fields", [])
        grouping_values = grouping.get("values", {})

        all_data = []
        expected_files = get_expected_preprocessed_files(
            self.dataset_name,
            dataset_dir,
            grouping_fields,
            grouping_values,
        )
        for file_path in expected_files:
            if not os.path.exists(file_path):
                continue
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                all_data.extend(self._hydrate_schema_references(data))

        if not all_data:
            raise ValueError(
                f"数据集 {self.dataset_name} 的预处理数据为空，请先检查原始数据或重新运行预处理。"
            )

        print(f"加载预处理数据: {len(all_data)} 条")
        return all_data

    def _has_preprocessed_data(self) -> bool:
        preprocessing_config = self.dataset_config.get("preprocessing", {})
        dataset_dir = get_preprocessed_output_dir(preprocessing_config, self.dataset_name)

        grouping = self.dataset_info_dict.get("grouping", {})
        grouping_fields = grouping.get("fields", [])
        grouping_values = grouping.get("values", {})
        expected_files = get_expected_preprocessed_files(
            self.dataset_name,
            dataset_dir,
            grouping_fields,
            grouping_values,
        )
        if not expected_files:
            return False
        return all(os.path.exists(file_path) for file_path in expected_files)

    def _hydrate_schema_references(self, data_items: list) -> list:
        hydrated = []
        for item in data_items:
            if not isinstance(item, dict):
                hydrated.append(item)
                continue

            schema_file = item.get("schema_file")
            if schema_file and not item.get("schema"):
                schema_path = schema_file
                if not os.path.isabs(schema_path):
                    schema_path = os.path.join(self.project_root, schema_path)
                try:
                    with open(schema_path, "r", encoding="utf-8") as f:
                        item["schema"] = f.read()
                except OSError:
                    item["schema"] = "-- Schema加载失败"
            hydrated.append(item)
        return hydrated

    def _prepare_prompts(
        self,
        data_items: list,
        config_type: str,
        prompt_builder,
        rag_retriever,
        keyword_searcher,
    ) -> list:
        print(f"  准备prompts ({config_type})...")

        config_spec = self._get_ablation_config(config_type)
        use_rag = bool(config_spec.get("use_rag"))
        use_keyword = bool(config_spec.get("use_keyword"))
        prompts = []
        for item in data_items:
            question = item["question"]
            schema = item["schema"]

            rag_context = None
            if use_rag and rag_retriever:
                retrieved_docs = rag_retriever.retrieve(question, item=item)
                rag_context = rag_retriever.format_context(retrieved_docs)

            keyword_context = None
            if use_keyword and keyword_searcher:
                retrieved_docs = keyword_searcher.search(question, item=item)
                keyword_context = keyword_searcher.format_context(retrieved_docs)

            prompt = prompt_builder.build_prompt(
                question=question,
                schema=schema,
                config_type=config_type,
                rag_context=rag_context,
                keyword_context=keyword_context,
                dataset_name=self.dataset_name,
                metadata=item.get("metadata", {}),
            )
            prompts.append(prompt)

        return prompts

    def _run_evaluation_only(self) -> List[Dict[str, Any]]:
        from src.datasets.processing import DataLoaderFactory
        from src.evaluation.evaluator import Evaluator

        print("\n" + "=" * 80)
        print("步骤: 评估已有预测结果")
        print("=" * 80 + "\n")

        dataset_db_config = self._get_dataset_db_config()
        evaluator = Evaluator(
            db_config=dataset_db_config,
            eval_config=self.eval_config,
        )

        loader_class_name = self.dataset_info_dict["loader_class"]
        data_loader = DataLoaderFactory.create(loader_class_name, self.dataset_info_dict)
        dataset_info = data_loader.get_dataset_info()

        all_eval_results = []
        for model_name in self.model_names:
            model_root = os.path.join(
                self._get_tasks_root(),
                self.dataset_name,
                self.backend,
                model_name,
            )
            if not os.path.isdir(model_root):
                print(f"警告: 模型 {model_name} 的 task 目录不存在: {model_root}")
                continue

            for config_type in self.config_types:
                prediction_file = self._find_prediction_file_for_evaluation(
                    model_name,
                    config_type,
                )
                if not prediction_file or not os.path.exists(prediction_file):
                    missing_path = self._get_task_prediction_file(
                        model_name,
                        config_type,
                        latest=True,
                    )
                    print(f"警告: 预测结果文件不存在: {missing_path}")
                    continue

                run_name = f"{model_name}__{self.backend}"
                print(f"\n评估: {run_name} - {config_type}")
                print(f"  加载预测结果: {prediction_file}")

                try:
                    with open(prediction_file, "r", encoding="utf-8") as f:
                        predictions = json.load(f)

                    if not predictions:
                        print("  警告: 预测结果文件为空")
                        continue

                    eval_result = evaluator.evaluate(
                        predictions=predictions,
                        dataset_info=dataset_info,
                        model_name=run_name,
                        config_type=config_type,
                        output_dir=self._get_task_dir(model_name, config_type),
                        resume=getattr(self.args, "resume", False),
                        overwrite=getattr(self.args, "overwrite", False),
                    )
                    eval_result["dataset_info"] = dataset_info
                    self._save_task_evaluation_outputs(
                        evaluator=evaluator,
                        eval_result=eval_result,
                        dataset_info=dataset_info,
                        model_name=model_name,
                        config_type=config_type,
                    )
                    all_eval_results.append(eval_result)
                except Exception as e:
                    print(f"  错误: 评估失败 - {str(e)}")
                    import traceback

                    traceback.print_exc()
                    continue

        if not all_eval_results:
            print("\n警告: 没有找到任何可评估的预测结果文件")

        return all_eval_results

    def _save_task_evaluation_outputs(
        self,
        *,
        evaluator,
        eval_result: Dict[str, Any],
        dataset_info: Dict[str, Any],
        model_name: str,
        config_type: str,
    ) -> None:
        from src.evaluation.report_generator import ReportGenerator

        history_dir = self._get_task_history_dir(model_name, config_type)
        latest_dir = self._get_task_latest_dir(model_name, config_type)

        evaluator.save_evaluation(eval_result, history_dir)
        history_eval_file = self._get_task_evaluation_file(
            model_name,
            config_type,
            latest=False,
        )
        latest_eval_file = self._get_task_evaluation_file(
            model_name,
            config_type,
            latest=True,
        )
        self._publish_latest_file(history_eval_file, latest_eval_file)

        report_gen = ReportGenerator(dataset_info)
        history_summary_file = self._get_task_summary_file(
            model_name,
            config_type,
            latest=False,
        )
        latest_summary_file = self._get_task_summary_file(
            model_name,
            config_type,
            latest=True,
        )
        report_gen.save_summary([eval_result], history_summary_file)
        self._publish_latest_file(history_summary_file, latest_summary_file)

        self._record_evaluation_artifact(
            self.dataset_name,
            model_name,
            config_type,
            history_eval_file,
            latest_eval_file,
            history_summary_file,
            latest_summary_file,
            eval_result,
        )

    @staticmethod
    def _publish_latest_file(source: str, target: str) -> None:
        if not os.path.exists(source):
            return
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(source, target)

    def _collect_benchmark_setup_metadata(self) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for dataset_name in self.dataset_names:
            if dataset_name == "spatial_qa":
                try:
                    from src.sql.spatial_qa_benchmark_setup import inspect_spatial_qa_benchmark_setup

                    metadata[dataset_name] = inspect_spatial_qa_benchmark_setup(
                        self._get_dataset_db_config_by_name(dataset_name)
                    )
                except Exception as exc:
                    metadata[dataset_name] = {
                        "dataset": dataset_name,
                        "status": "check_failed",
                        "checked_at": datetime.now().isoformat(timespec="seconds"),
                        "error": str(exc),
                    }
                continue

            if dataset_name == "floodsql_pg":
                metadata[dataset_name] = {
                    "dataset": dataset_name,
                    "status": "managed_by_migration",
                    "checked_at": datetime.now().isoformat(timespec="seconds"),
                    "index_profile": "floodsql_geometry_v1",
                    "source": "src/sql/floodsql_migration.py",
                    "reason": "Indexes are provisioned by the FloodSQL migration pipeline",
                }
                continue

            metadata[dataset_name] = {
                "dataset": dataset_name,
                "status": "not_required",
                "checked_at": datetime.now().isoformat(timespec="seconds"),
                "reason": "No benchmark-specific database setup profile is currently required",
            }
        return metadata

    @staticmethod
    def _benchmark_issue_type(detail: Dict[str, Any]) -> Optional[str]:
        error_type = detail.get("error_type")
        if error_type in {"pred_ok_gold_timeout", "both_timeout", "gold_execution_error"}:
            return error_type

        if error_type == "connection_error":
            execution_error = detail.get("execution_error") or {}
            stage = str(execution_error.get("stage") or "")
            if stage.startswith("标准SQL") or detail.get("gold_execution_errors"):
                return "gold_connection_error"

        execution_error = detail.get("execution_error") or {}
        exec_stage = str(execution_error.get("stage") or "")
        if exec_stage.startswith("标准SQL"):
            return "legacy_gold_execution_error"

        if detail.get("gold_execution_errors"):
            first_status = (detail["gold_execution_errors"][0] or {}).get("status")
            return first_status or "gold_execution_error"

        message = str(detail.get("error_message") or "")
        if "标准SQL执行失败" in message:
            return "legacy_gold_execution_error"

        return None

    def _collect_benchmark_validation(self, eval_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        issues = []
        for result in eval_results:
            counter: Counter[str] = Counter()
            for detail in result.get("details", []):
                issue_type = self._benchmark_issue_type(detail)
                if issue_type:
                    counter[issue_type] += 1
            if counter:
                issues.append(
                    {
                        "dataset": result.get("dataset"),
                        "model": result.get("model"),
                        "config": result.get("config"),
                        "sample_count": sum(counter.values()),
                        "issue_breakdown": dict(counter),
                    }
                )

        return {
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "status": "ready" if not issues else "gold_unstable",
            "issues": issues,
        }

    @staticmethod
    def _prompt_instruction_prefix(prompt_text: str) -> str:
        marker = "\n## User Question"
        if marker in prompt_text:
            prompt_text = prompt_text.split(marker, 1)[0]
        return prompt_text.strip()

    @staticmethod
    def _hash_prompt_prefix(prompt_text: str) -> str:
        prefix = MainPipeline._prompt_instruction_prefix(prompt_text)
        return hashlib.sha256(prefix.encode("utf-8")).hexdigest()

    def _resolve_prompt_template_path(
        self,
        dataset_name: str,
        config_type: str,
    ) -> str:
        config_spec = self._get_ablation_config(config_type)
        prompt_style = str(config_spec.get("prompt_style") or "default")

        default_style = self.eval_config.get("prompt_styles", {}).get("default", {})
        style_spec = dict(default_style)
        style_spec.update(self.eval_config.get("prompt_styles", {}).get(prompt_style, {}))

        if style_spec.get("dataset_specific"):
            from src.prompting.prompt_enhancements.registry import PromptEnhancementRegistry

            style_spec.update(
                PromptEnhancementRegistry(self.project_root).resolve_dataset_override(
                    dataset_name
                )
            )

        template_path = style_spec.get("template_path") or self.eval_config.get(
            "prompt_template_path"
        )
        if template_path:
            path = template_path
            if not os.path.isabs(path):
                path = os.path.join(self.project_root, path)
            return os.path.abspath(path)

        return os.path.join(self.project_root, "prompts", "text2sql_prompt.txt")

    def _read_expected_prompt_signature(
        self,
        dataset_name: str,
        config_type: str,
    ) -> Dict[str, Any]:
        template_path = self._resolve_prompt_template_path(dataset_name, config_type)
        signature: Dict[str, Any] = {
            "template_path": template_path,
        }
        if not os.path.exists(template_path):
            signature["status"] = "missing_expected_template"
            return signature

        with open(template_path, "r", encoding="utf-8") as f:
            template_text = f.read()

        signature.update(
            {
                "status": "ready",
                "prompt_prefix_sha256": self._hash_prompt_prefix(template_text),
            }
        )
        return signature

    def _read_saved_prompt_signature(self, prompt_file: str) -> Dict[str, Any]:
        signature: Dict[str, Any] = {
            "prompt_file": prompt_file,
        }
        if not os.path.exists(prompt_file):
            signature["status"] = "missing_prompt_file"
            return signature

        with open(prompt_file, "r", encoding="utf-8") as f:
            prompts = json.load(f)

        if not isinstance(prompts, list) or not prompts:
            signature["status"] = "empty_prompt_file"
            return signature

        first_prompt = prompts[0].get("prompt", "")
        signature.update(
            {
                "status": "ready",
                "count": len(prompts),
                "prompt_prefix_sha256": self._hash_prompt_prefix(first_prompt),
            }
        )
        return signature

    def _check_latest_prompt_consistency(
        self,
        dataset_name: str,
        model_name: str,
        config_type: str,
    ) -> Dict[str, Any]:
        expected = self._read_expected_prompt_signature(dataset_name, config_type)
        actual = self._read_saved_prompt_signature(
            self._get_task_prompt_file(
                model_name,
                config_type,
                latest=True,
                dataset_name=dataset_name,
            )
        )
        check = {
            "dataset": dataset_name,
            "backend": self.backend,
            "model": model_name,
            "config": config_type,
            "expected": expected,
            "actual": actual,
        }

        if expected.get("status") != "ready" or actual.get("status") != "ready":
            check["status"] = "unchecked"
            check["reason"] = (
                expected.get("status")
                if expected.get("status") != "ready"
                else actual.get("status")
            )
            return check

        if expected.get("prompt_prefix_sha256") != actual.get("prompt_prefix_sha256"):
            check["status"] = "mismatch"
            check["reason"] = "prompt_prefix_sha256_mismatch"
            return check

        check["status"] = "match"
        return check

    def _build_benchmark_prompt_consistency_metadata(self) -> Dict[str, Any]:
        return {
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "status": "mismatch" if self._benchmark_prompt_issues else "ready",
            "checks": list(self._benchmark_prompt_checks),
            "issues": list(self._benchmark_prompt_issues),
        }

    def _build_benchmark_run_metadata(self) -> Dict[str, Any]:
        model_catalog = {}
        for model_name in self.model_names:
            model_info = self.model_config.get("models", {}).get(model_name, {})
            model_catalog[model_name] = {
                "display_name": model_info.get("display_name", model_name),
                "size_label": model_info.get("size_label", "Unk."),
                "paper_group": model_info.get("paper_group"),
                "paper_order": model_info.get("paper_order"),
            }

        dataset_catalog = {}
        for dataset_name in self.dataset_names:
            dataset_cfg = self.dataset_config.get("datasets", {}).get(dataset_name, {})
            grouping = dataset_cfg.get("grouping", {})
            dataset_catalog[dataset_name] = {
                "name": dataset_name,
                "grouping_fields": list(grouping.get("fields", [])),
                "grouping_values": dict(grouping.get("values", {})),
            }

        return {
            "datasets": list(self.dataset_names),
            "models": list(self.model_names),
            "configs": list(self.config_types),
            "backend": self.backend,
            "model_catalog": model_catalog,
            "dataset_catalog": dataset_catalog,
            "task_source": "task latest",
            "benchmark_mode": "aggregate_only",
            "dataset_index_status": self._dataset_index_status,
            "validation_notes": self._evaluation_validation,
            "prompt_consistency": self._build_benchmark_prompt_consistency_metadata(),
        }

    def _collect_task_eval_results(self) -> List[Dict[str, Any]]:
        print("\n" + "=" * 80)
        print("步骤: 汇总 task 中的评估结果")
        print("=" * 80 + "\n")

        collected = []
        for dataset_name in self.dataset_names:
            self._set_dataset_context(dataset_name)
            for model_name in self.model_names:
                for config_type in self.config_types:
                    candidate = self._get_task_evaluation_file(
                        model_name,
                        config_type,
                        latest=True,
                    )
                    loaded = False
                    if os.path.exists(candidate):
                        try:
                            with open(candidate, "r", encoding="utf-8") as f:
                                result = json.load(f)
                        except Exception as exc:
                            print(f"警告: 读取评估结果失败: {candidate} ({exc})")
                            result = None
                        run_name = f"{model_name}__{self.backend}"
                        if (
                            result
                            and result.get("dataset") == dataset_name
                            and result.get("config") == config_type
                            and result.get("model") == run_name
                        ):
                            prompt_check = self._check_latest_prompt_consistency(
                                dataset_name,
                                model_name,
                                config_type,
                            )
                            self._benchmark_prompt_checks.append(prompt_check)
                            if prompt_check.get("status") == "mismatch":
                                self._benchmark_prompt_issues.append(prompt_check)
                                expected_hash = prompt_check["expected"].get(
                                    "prompt_prefix_sha256", ""
                                )[:12]
                                actual_hash = prompt_check["actual"].get(
                                    "prompt_prefix_sha256", ""
                                )[:12]
                                print(
                                    "跳过结果: latest prompt 与当前模板不一致 "
                                    f"dataset={dataset_name} model={model_name} "
                                    f"config={config_type} "
                                    f"expected={expected_hash} actual={actual_hash}"
                                )
                                loaded = True
                                continue
                            if prompt_check.get("status") == "unchecked":
                                print(
                                    "提示: 未校验 latest prompt "
                                    f"dataset={dataset_name} model={model_name} "
                                    f"config={config_type} "
                                    f"reason={prompt_check.get('reason')}"
                                )
                            result = dict(result)
                            result["prompt_consistency"] = prompt_check
                            result["source_files"] = {
                                "evaluation": candidate,
                                "prompts": prompt_check.get("actual", {}).get(
                                    "prompt_file"
                                ),
                            }
                            collected.append(result)
                            loaded = True
                            print(f"已载入: {candidate}")

                    if not loaded:
                        print(
                            f"缺失结果: dataset={dataset_name} model={model_name} config={config_type}"
                        )

        return collected

    def _generate_benchmark_report(self, eval_results: List[Dict]) -> None:
        from src.evaluation.report_generator import BenchmarkReportGenerator

        print("\n" + "=" * 80)
        print("步骤: 生成 Benchmark 汇总")
        print("=" * 80 + "\n")

        report_gen = BenchmarkReportGenerator()
        self._dataset_index_status = self._collect_benchmark_setup_metadata()
        self._evaluation_validation = self._collect_benchmark_validation(eval_results)
        run_metadata = self._build_benchmark_run_metadata()
        report_text = report_gen.generate_report(eval_results, run_metadata)
        print(report_text)

        history_dir = self._get_benchmark_history_dir()
        latest_dir = self._get_benchmark_latest_dir()
        history_paths = report_gen.save_summary(eval_results, history_dir, run_metadata)
        latest_paths = report_gen.save_summary(eval_results, latest_dir, run_metadata)
        self._session_report_paths.extend(history_paths)
        self._session_report_paths.extend(latest_paths)

    def _write_run_session_metadata(self, status: str):
        os.makedirs(self._get_sessions_root(), exist_ok=True)
        metadata = {
            "run_id": self.run_id,
            "started_at": self.run_started_at,
            "completed_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "backend": self.backend,
            "datasets": list(self.dataset_names),
            "models": list(self.model_names),
            "configs": list(self.config_types),
            "actions": {
                "preprocess": bool(self.args.preprocess),
                "build_rag": bool(self.args.build_rag),
                "inference": bool(self.args.inference),
                "evaluate": bool(self.args.evaluate),
                "benchmark": bool(self.args.benchmark),
            },
            "prediction_postprocess_enabled": self.enable_prediction_postprocess,
            "predictions": list(self._session_predictions.values()),
            "evaluations": list(self._session_evaluations.values()),
            "report_paths": list(self._session_report_paths),
            "dataset_index_status": self._dataset_index_status,
            "validation_notes": self._evaluation_validation,
        }
        output_file = os.path.join(self._get_sessions_root(), f"{self.run_id}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Spatial Text2SQL 推理评估框架")

    parser.add_argument("--preprocess", action="store_true", help="运行数据预处理")
    parser.add_argument("--build-rag", action="store_true", help="构建RAG索引")
    parser.add_argument("--inference", action="store_true", help="运行推理")
    parser.add_argument("--evaluate", action="store_true", help="运行评估")
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="仅从各 task 的 latest 结果生成 benchmark 汇总（需独立运行）",
    )

    parser.add_argument("--config-dir", type=str, default="config", help="配置文件目录")
    parser.add_argument("--dataset", nargs="+", help="数据集名称，支持 all 或多个数据集")
    parser.add_argument("--models", nargs="+", help="模型列表")
    parser.add_argument(
        "--backend",
        choices=["vllm", "transformers"],
        help="推理后端，默认读取 model_config.yaml 中的 default_backend",
    )
    parser.add_argument(
        "--configs",
        nargs="+",
        help="配置类型列表，读取 eval_config.yaml 中的 ablation_configs",
    )
    parser.add_argument(
        "--enable-prediction-postprocess",
        action="store_true",
        help="显式开启预测后归一化；默认关闭",
    )

    args = parser.parse_args()
    if not (
        args.preprocess
        or args.build_rag
        or args.inference
        or args.evaluate
        or args.benchmark
    ):
        parser.print_help()
        sys.exit(0)

    try:
        MainPipeline._validate_action_selection(args)
    except ValueError as exc:
        parser.error(str(exc))

    pipeline = MainPipeline(args)
    pipeline.run()


if __name__ == "__main__":
    main()

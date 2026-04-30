"""结果报告生成器 - 动态适配数据集结构生成报告"""
import csv
from datetime import datetime
import json
import math
import os
import re
from typing import List, Dict, Any


class ReportGenerator:
    """报告生成器 - 根据数据集元信息动态生成报告"""
    
    def __init__(self, dataset_info: Dict):
        """
        初始化报告生成器
        
        Args:
            dataset_info: 数据集元信息（来自BaseDataLoader.get_dataset_info()）
        """
        self.dataset_info = dataset_info
        self.dataset_name = dataset_info.get('name', 'unknown')
        self.grouping_fields = dataset_info.get('grouping_fields', [])
        self.grouping_values = dataset_info.get('grouping_values', {})
    
    def generate_report(self, eval_results: List[Dict]) -> str:
        """
        根据数据集结构动态生成报告
        
        Args:
            eval_results: 评估结果列表
            
        Returns:
            格式化的报告字符串
        """
        if self.grouping_fields:
            # 有分层结构，按维度统计
            return self._generate_grouped_report(eval_results)
        else:
            # 无分层结构，全量统计
            return self._generate_flat_report(eval_results)
    
    def _generate_grouped_report(self, eval_results: List[Dict]) -> str:
        """生成分组报告"""
        lines = []
        
        lines.append("="*80)
        lines.append("Spatial Text2SQL Evaluation Results")
        lines.append("="*80)
        lines.append(f"Dataset: {self.dataset_name}")
        
        # 显示分组信息
        if self.grouping_fields:
            grouping_desc = []
            for group_field in self.grouping_fields:
                group_values = self.grouping_values.get(group_field, [])
                grouping_desc.append(
                    f"{group_field} ({len(group_values)} groups: {', '.join(map(str, group_values))})"
                )
            lines.append(f"Grouping: {'; '.join(grouping_desc)}")
        
        lines.append("")
        
        # 按模型组织结果
        model_results = {}
        for result in eval_results:
            model_name = result['model']
            if model_name not in model_results:
                model_results[model_name] = []
            model_results[model_name].append(result)
        
        # 为每个模型生成表格
        for model_name, results in sorted(model_results.items()):
            lines.append(f"\nModel: {model_name}")
            for group_field in self.grouping_fields:
                lines.append(f"\nBy {group_field}")
                lines.append(self._generate_table(results, group_field))

        lines.append("\n" + "="*80)
        return "\n".join(lines)
    
    def _generate_table(self, results: List[Dict], group_field: str) -> str:
        """生成结果表格"""
        group_values = self.grouping_values.get(group_field, [])
        
        # 准备表头
        header = ["Config"]
        for gv in group_values:
            header.append(f"{group_field.capitalize()}{gv}")
        header.append("Avg")
        
        # 计算列宽
        col_widths = [max(12, len(h)) for h in header]
        
        # 绘制表头
        lines = []
        lines.append("┌" + "┬".join("─" * (w+2) for w in col_widths) + "┐")
        lines.append("│" + "│".join(f" {h:<{w}} " for h, w in zip(header, col_widths)) + "│")
        lines.append("├" + "┼".join("─" * (w+2) for w in col_widths) + "┤")
        
        # 绘制数据行
        for result in results:
            config_name = self._get_config_display_name(result['config'])
            row_data = [config_name]
            
            stats = result['statistics']
            overall_acc = stats['overall']['accuracy']
            
            grouped_stats = self._get_grouped_stats(stats, group_field)
            if grouped_stats is not None:
                for gv in group_values:
                    gv_str = str(gv)
                    if gv_str in grouped_stats:
                        acc = grouped_stats[gv_str]['accuracy']
                        row_data.append(f"{acc:.1%}")
                    else:
                        row_data.append("N/A")
            else:
                row_data.extend(["N/A"] * len(group_values))
            
            row_data.append(f"{overall_acc:.1%}")
            
            lines.append("│" + "│".join(f" {d:<{w}} " for d, w in zip(row_data, col_widths)) + "│")
        
        # 绘制表尾
        lines.append("└" + "┴".join("─" * (w+2) for w in col_widths) + "┘")

        return "\n".join(lines)

    def _get_grouped_stats(self, stats: Dict[str, Any], group_field: str) -> Any:
        """
        兼容新旧统计结构读取 grouped 数据。

        新结构优先使用 all_samples.grouped，旧结构回退到根级 grouped。
        """
        candidates = [stats.get('all_samples', {}), stats]
        for candidate in candidates:
            grouped = candidate.get('grouped')
            if grouped and group_field in grouped:
                return grouped[group_field]
        return None
    
    def _generate_simple_table(self, results: List[Dict]) -> str:
        """生成简单表格（无分组）"""
        # 准备表头
        header = ["Config", "Correct", "Avg"]
        col_widths = [12, 10, 10]
        
        # 绘制表头
        lines = []
        lines.append("┌" + "┬".join("─" * (w+2) for w in col_widths) + "┐")
        lines.append("│" + "│".join(f" {h:<{w}} " for h, w in zip(header, col_widths)) + "│")
        lines.append("├" + "┼".join("─" * (w+2) for w in col_widths) + "┤")
        
        # 绘制数据行
        for result in results:
            config_name = self._get_config_display_name(result['config'])
            stats = result['statistics']['overall']
            
            row_data = [
                config_name,
                f"{stats['correct']}/{stats['total']}",
                f"{stats['accuracy']:.1%}"
            ]
            
            lines.append("│" + "│".join(f" {d:<{w}} " for d, w in zip(row_data, col_widths)) + "│")
        
        # 绘制表尾
        lines.append("└" + "┴".join("─" * (w+2) for w in col_widths) + "┘")
        
        return "\n".join(lines)
    
    def _generate_flat_report(self, eval_results: List[Dict]) -> str:
        """生成无分层报告"""
        lines = []
        
        lines.append("="*80)
        lines.append("Spatial Text2SQL Evaluation Results")
        lines.append("="*80)
        lines.append(f"Dataset: {self.dataset_name}")
        lines.append("Grouping: none (全量评估)")
        lines.append("")
        
        # 按模型组织结果
        model_results = {}
        for result in eval_results:
            model_name = result['model']
            if model_name not in model_results:
                model_results[model_name] = []
            model_results[model_name].append(result)
        
        # 为每个模型生成表格
        for model_name, results in sorted(model_results.items()):
            lines.append(f"\nModel: {model_name}")
            lines.append(self._generate_simple_table(results))
        
        lines.append("\n" + "="*80)
        return "\n".join(lines)
    
    def _get_config_display_name(self, config_type: str) -> str:
        """获取配置的显示名称"""
        names = {
            'base': 'Base',
            'rag': '+RAG',
            'keyword': '+Keyword',
            'full': '+Both'
        }
        return names.get(config_type, config_type)
    
    def save_summary(self, eval_results: List[Dict], output_file: str):
        """
        保存汇总结果到JSON文件
        
        Args:
            eval_results: 评估结果列表
            output_file: 输出文件路径
        """
        summary = {
            'dataset': self.dataset_name,
            'dataset_info': self.dataset_info,
            'results': []
        }
        
        for result in eval_results:
            summary['results'].append({
                'model': result['model'],
                'config': result['config'],
                'statistics': result['statistics']
            })
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"\n汇总结果已保存: {output_file}")

    @staticmethod
    def _slugify(value: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized or "unknown"


class BenchmarkReportGenerator:
    """跨模型、跨数据集总览报告生成器。"""

    def __init__(self):
        self.generated_at = datetime.now().isoformat(timespec="seconds")

    def generate_report(self, eval_results: List[Dict], run_metadata: Dict[str, Any]) -> str:
        summary = self.build_summary(eval_results, run_metadata)
        datasets = summary["requested"]["datasets"]

        lines = []
        lines.append("=" * 80)
        lines.append("Spatial Text2SQL Benchmark Summary")
        lines.append("=" * 80)
        lines.append(f"Backend: {summary['run_metadata']['backend']}")
        lines.append(f"Configs: {', '.join(summary['requested']['configs'])}")
        lines.append(f"Source: {summary['run_metadata'].get('task_source', 'task latest')}")
        benchmark_mode = summary["run_metadata"].get("benchmark_mode")
        if benchmark_mode == "aggregate_only":
            lines.append("Mode: aggregate only (does not run inference or evaluation)")

        prompt_consistency = summary["run_metadata"].get("prompt_consistency") or {}
        prompt_issues = prompt_consistency.get("issues") or []
        if prompt_consistency:
            lines.append("")
            lines.append(
                f"Prompt Consistency: {prompt_consistency.get('status', 'unknown')}"
            )
            for issue in prompt_issues:
                expected_hash = (
                    issue.get("expected", {}).get("prompt_prefix_sha256", "")[:12]
                )
                actual_hash = (
                    issue.get("actual", {}).get("prompt_prefix_sha256", "")[:12]
                )
                lines.append(
                    "- "
                    f"dataset={issue.get('dataset')} "
                    f"model={issue.get('model')} "
                    f"config={issue.get('config')} "
                    f"expected={expected_hash} actual={actual_hash}"
                )

        dataset_index_status = (
            summary["run_metadata"].get("dataset_index_status")
            or summary["run_metadata"].get("benchmark_setup", {})
        )
        if dataset_index_status:
            lines.append("")
            lines.append("Index Status")
            for dataset_name in datasets:
                setup = dataset_index_status.get(dataset_name)
                if not setup:
                    continue
                profile = setup.get("index_profile")
                setup_status = setup.get("status", "unknown")
                status_line = f"- {dataset_name}: {setup_status}"
                if profile:
                    status_line += f" ({profile})"
                missing = setup.get("missing_indexes") or []
                if missing:
                    status_line += f", missing_indexes={len(missing)}"
                lines.append(status_line)

        validation_notes = (
            summary["run_metadata"].get("validation_notes")
            or summary["run_metadata"].get("benchmark_validation", {})
        )
        issues = validation_notes.get("issues") or []
        if validation_notes:
            lines.append("")
            lines.append(f"Validation Notes: {validation_notes.get('status', 'unknown')}")
            for issue in issues:
                breakdown = ", ".join(
                    f"{error_type}={count}"
                    for error_type, count in sorted((issue.get("issue_breakdown") or {}).items())
                )
                lines.append(
                    "- "
                    f"dataset={issue.get('dataset')} "
                    f"model={issue.get('model')} "
                    f"config={issue.get('config')} "
                    f"samples={issue.get('sample_count')}"
                    + (f" ({breakdown})" if breakdown else "")
                )
        lines.append("")
        lines.append("Overall")
        lines.append(self._build_matrix_table(summary["matrices"]["overall"], datasets))

        dataset_breakdowns = summary.get("dataset_breakdowns", {})
        for dataset_name in datasets:
            breakdown = dataset_breakdowns.get(dataset_name, {})
            grouping_fields = breakdown.get("grouping_fields", [])
            matrices = breakdown.get("matrices", {})
            if not grouping_fields or not matrices:
                continue

            lines.append("")
            lines.append(f"Dataset: {dataset_name}")
            for group_field in grouping_fields:
                matrix = matrices.get(group_field)
                if not matrix:
                    continue
                group_values = breakdown.get("grouping_values", {}).get(group_field, [])
                lines.append(f"By {group_field}")
                lines.append(
                    self._build_grouped_matrix_table(
                        dataset_name=dataset_name,
                        group_field=group_field,
                        group_values=group_values,
                        matrix=matrix,
                        overall_matrix=summary["matrices"]["overall"],
                    )
                )

        if summary["missing_results"]:
            lines.append("")
            lines.append("Missing Results")
            for item in summary["missing_results"]:
                lines.append(
                    f"- dataset={item['dataset']} model={item['model']} config={item['config']}"
                )

        lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)

    def save_summary(self, eval_results: List[Dict], output_dir: str, run_metadata: Dict[str, Any]) -> List[str]:
        os.makedirs(output_dir, exist_ok=True)
        summary = self.build_summary(eval_results, run_metadata)
        json_path = os.path.join(output_dir, "summary.json")
        txt_path = os.path.join(output_dir, "summary.txt")
        markdown_path = os.path.join(output_dir, "paper_tables.md")
        table4_csv_path = os.path.join(output_dir, "overall_performance.csv")
        table6_csv_path = os.path.join(output_dir, "avg_tokens.csv")
        table7_csv_path = os.path.join(output_dir, "avg_latency.csv")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(self.generate_report(eval_results, run_metadata))

        with open(markdown_path, "w", encoding="utf-8") as f:
            f.write(self._build_paper_tables_markdown(summary))

        self._write_csv(
            table4_csv_path,
            summary["paper_tables"]["table4_overall_performance"],
        )
        self._write_csv(
            table6_csv_path,
            summary["paper_tables"]["table6_avg_tokens"],
        )
        self._write_csv(
            table7_csv_path,
            summary["paper_tables"]["table7_avg_latency"],
        )

        print(f"\n总览结果已保存: {json_path}")
        print(f"总览文本已保存: {txt_path}")
        print(f"论文表格 Markdown 已保存: {markdown_path}")
        print(f"论文表格 CSV 已保存: {table4_csv_path}, {table6_csv_path}, {table7_csv_path}")
        return [
            json_path,
            txt_path,
            markdown_path,
            table4_csv_path,
            table6_csv_path,
            table7_csv_path,
        ]

    def build_summary(self, eval_results: List[Dict], run_metadata: Dict[str, Any]) -> Dict[str, Any]:
        requested = {
            "datasets": list(run_metadata.get("datasets", [])),
            "models": list(run_metadata.get("models", [])),
            "configs": list(run_metadata.get("configs", [])),
        }
        model_catalog = run_metadata.get("model_catalog", {})

        details = []
        for result in eval_results:
            stats = result.get("statistics", {})
            logical_model = self._logical_model_name(result.get("model", ""))
            inference_metrics = self._extract_inference_metrics(stats)
            details.append(
                {
                    "dataset": result.get("dataset"),
                    "model": result.get("model"),
                    "logical_model": logical_model,
                    "config": result.get("config"),
                    "display_name": model_catalog.get(logical_model, {}).get("display_name", logical_model),
                    "size_label": model_catalog.get(logical_model, {}).get("size_label", "Unk."),
                    "paper_group": model_catalog.get(logical_model, {}).get("paper_group"),
                    "paper_order": model_catalog.get(logical_model, {}).get("paper_order"),
                    "avg_input_tokens": inference_metrics.get("avg_input_tokens"),
                    "avg_output_tokens": inference_metrics.get("avg_output_tokens"),
                    "avg_total_tokens": inference_metrics.get("avg_total_tokens"),
                    "avg_latency_ms": inference_metrics.get("avg_latency_ms"),
                    "sum_input_tokens": inference_metrics.get("sum_input_tokens"),
                    "sum_output_tokens": inference_metrics.get("sum_output_tokens"),
                    "sum_total_tokens": inference_metrics.get("sum_total_tokens"),
                    "sum_latency_ms": inference_metrics.get("sum_latency_ms"),
                    "question_count": inference_metrics.get("question_count"),
                    "statistics": stats,
                    "prompt_consistency": result.get("prompt_consistency"),
                    "source_files": result.get("source_files"),
                }
            )

        overall_matrix: Dict[str, Dict[str, Any]] = {}

        for result in eval_results:
            row_label = self._build_row_label(
                result["model"],
                result["config"],
                requested["configs"],
                eval_results,
            )
            overall_matrix.setdefault(row_label, {})
            overall_matrix[row_label][result["dataset"]] = result["statistics"]["overall"]

        missing_results = []
        for dataset in requested["datasets"]:
            for logical_model in requested["models"]:
                for config in requested["configs"]:
                    matched = False
                    for result in eval_results:
                        if (
                            result.get("dataset") == dataset
                            and self._logical_model_name(result.get("model", "")) == logical_model
                            and result.get("config") == config
                        ):
                            matched = True
                            break
                    if not matched:
                        missing_results.append(
                            {"dataset": dataset, "model": logical_model, "config": config}
                        )

        summary = {
            "kind": "benchmark_summary",
            "generated_at": self.generated_at,
            "run_metadata": run_metadata,
            "requested": requested,
            "results": details,
            "matrices": {
                "overall": overall_matrix,
            },
            "missing_results": missing_results,
        }
        summary["dataset_breakdowns"] = self._build_dataset_breakdowns(
            eval_results=eval_results,
            requested=requested,
            dataset_catalog=run_metadata.get("dataset_catalog", {}),
        )
        summary["paper_tables"] = self._build_paper_tables(summary)
        return summary

    def _build_dataset_breakdowns(
        self,
        *,
        eval_results: List[Dict],
        requested: Dict[str, Any],
        dataset_catalog: Dict[str, Any],
    ) -> Dict[str, Any]:
        breakdowns: Dict[str, Any] = {}
        for dataset_name in requested.get("datasets", []):
            dataset_meta = dataset_catalog.get(dataset_name, {})
            breakdowns[dataset_name] = {
                "grouping_fields": list(dataset_meta.get("grouping_fields", [])),
                "grouping_values": dict(dataset_meta.get("grouping_values", {})),
                "matrices": {},
            }

        for result in eval_results:
            dataset_name = result.get("dataset")
            if dataset_name not in breakdowns:
                continue

            row_label = self._build_row_label(
                result["model"],
                result["config"],
                requested["configs"],
                eval_results,
            )
            grouping_fields = breakdowns[dataset_name]["grouping_fields"]
            for group_field in grouping_fields:
                grouped_stats = self._get_grouped_stats(result, group_field)
                if not grouped_stats:
                    continue
                field_matrix = breakdowns[dataset_name]["matrices"].setdefault(group_field, {})
                row = field_matrix.setdefault(row_label, {})
                for group_value, stat in grouped_stats.items():
                    row[str(group_value)] = stat

        return breakdowns

    @staticmethod
    def _extract_inference_metrics(stats: Dict[str, Any]) -> Dict[str, Any]:
        return stats.get("inference_metrics") or {}

    def _build_paper_tables(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        paper_rows = self._build_paper_rows(summary)
        return {
            "table4_overall_performance": self._build_table4(paper_rows),
            "table6_avg_tokens": self._build_table6(paper_rows),
            "table7_avg_latency": self._build_table7(paper_rows),
        }

    def _build_paper_rows(self, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        model_catalog = summary.get("run_metadata", {}).get("model_catalog", {})
        requested_models = summary.get("requested", {}).get("models", [])
        requested_configs = summary.get("requested", {}).get("configs", [])
        target_config = "base" if "base" in requested_configs else (requested_configs[0] if requested_configs else None)
        detail_index = {
            (item["logical_model"], item["dataset"], item["config"]): item
            for item in summary.get("results", [])
        }

        def sort_key(model_name: str) -> Any:
            meta = model_catalog.get(model_name, {})
            paper_order = meta.get("paper_order")
            if not isinstance(paper_order, (int, float)):
                paper_order = 10**6
            return (
                paper_order,
                requested_models.index(model_name) if model_name in requested_models else 10**6,
                meta.get("display_name", model_name),
            )

        rows = []
        for logical_model in sorted(requested_models, key=sort_key):
            meta = model_catalog.get(logical_model, {})
            if meta.get("paper_group") != "open_source":
                continue
            rows.append(
                {
                    "logical_model": logical_model,
                    "display_name": meta.get("display_name", logical_model),
                    "size_label": meta.get("size_label", "Unk."),
                    "spatial_qa": detail_index.get((logical_model, "spatial_qa", target_config)),
                    "spatialsql_pg": detail_index.get((logical_model, "spatialsql_pg", target_config)),
                    "floodsql_pg": detail_index.get((logical_model, "floodsql_pg", target_config)),
                }
            )
        return rows

    def _build_table4(self, paper_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        columns = [
            "Model",
            "Size",
            "SpatialSQL Avg",
            "SpatialQueryQA Basic",
            "SpatialQueryQA Interm.",
            "SpatialQueryQA Adv.",
            "SpatialQueryQA Avg",
            "FloodSQL-Bench L0",
            "FloodSQL-Bench L1",
            "FloodSQL-Bench L2",
            "FloodSQL-Bench L3",
            "FloodSQL-Bench L4",
            "FloodSQL-Bench L5",
            "FloodSQL-Bench Avg",
            "GeoSQL-Bench Sim.",
            "GeoSQL-Bench Mod.",
            "GeoSQL-Bench Chall.",
            "GeoSQL-Bench Avg",
        ]
        rows = []
        for row in paper_rows:
            spatialsql = row.get("spatialsql_pg")
            spatialqa = row.get("spatial_qa")
            floodsql = row.get("floodsql_pg")
            spatialqa_levels = self._get_grouped_stats(spatialqa, "level")
            flood_levels = self._get_grouped_stats(floodsql, "level")
            rows.append(
                [
                    row["display_name"],
                    row["size_label"],
                    self._format_accuracy_value(self._extract_accuracy(spatialsql, None)),
                    self._format_accuracy_value(self._extract_accuracy(spatialqa, "1", grouped=spatialqa_levels)),
                    self._format_accuracy_value(self._extract_accuracy(spatialqa, "2", grouped=spatialqa_levels)),
                    self._format_accuracy_value(self._extract_accuracy(spatialqa, "3", grouped=spatialqa_levels)),
                    self._format_accuracy_value(self._extract_accuracy(spatialqa, None)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L0", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L1", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L2", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L3", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L4", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, "L5", grouped=flood_levels)),
                    self._format_accuracy_value(self._extract_accuracy(floodsql, None)),
                    "-",
                    "-",
                    "-",
                    "-",
                ]
            )
        return {
            "title": "Table 4: Overall performance on spatial text-to-SQL benchmarks.",
            "columns": columns,
            "rows": rows,
        }

    def _build_table6(self, paper_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        columns = [
            "Model",
            "Size",
            "SpatialSQL In",
            "SpatialSQL Out",
            "SpatialSQL Total",
            "SpatialQueryQA In",
            "SpatialQueryQA Out",
            "SpatialQueryQA Total",
            "FloodSQL-Bench In",
            "FloodSQL-Bench Out",
            "FloodSQL-Bench Total",
            "GeoSQL-Bench In",
            "GeoSQL-Bench Out",
            "GeoSQL-Bench Total",
        ]
        rows = []
        for row in paper_rows:
            rows.append(
                [
                    row["display_name"],
                    row["size_label"],
                    self._format_integer_metric(self._extract_metric(row.get("spatialsql_pg"), "avg_input_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("spatialsql_pg"), "avg_output_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("spatialsql_pg"), "avg_total_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("spatial_qa"), "avg_input_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("spatial_qa"), "avg_output_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("spatial_qa"), "avg_total_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("floodsql_pg"), "avg_input_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("floodsql_pg"), "avg_output_tokens")),
                    self._format_integer_metric(self._extract_metric(row.get("floodsql_pg"), "avg_total_tokens")),
                    "-",
                    "-",
                    "-",
                ]
            )
        return {
            "title": "Table 6: Average token consumption on spatial text-to-SQL benchmarks.",
            "columns": columns,
            "rows": rows,
        }

    def _build_table7(self, paper_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        columns = [
            "Model",
            "Size",
            "SpatialSQL",
            "SpatialQueryQA",
            "FloodSQL-Bench",
            "GeoSQL-Bench",
        ]
        rows = []
        for row in paper_rows:
            rows.append(
                [
                    row["display_name"],
                    row["size_label"],
                    self._format_integer_metric(self._extract_metric(row.get("spatialsql_pg"), "avg_latency_ms")),
                    self._format_integer_metric(self._extract_metric(row.get("spatial_qa"), "avg_latency_ms")),
                    self._format_integer_metric(self._extract_metric(row.get("floodsql_pg"), "avg_latency_ms")),
                    "-",
                ]
            )
        return {
            "title": "Table 7: Average inference latency on spatial text-to-SQL benchmarks.",
            "columns": columns,
            "rows": rows,
        }

    def _build_paper_tables_markdown(self, summary: Dict[str, Any]) -> str:
        sections = []
        for table_key in (
            "table4_overall_performance",
            "table6_avg_tokens",
            "table7_avg_latency",
        ):
            table = summary["paper_tables"][table_key]
            sections.append(f"## {table['title']}")
            sections.append("")
            sections.append(self._markdown_table(table["columns"], table["rows"]))
            sections.append("")
        return "\n".join(sections).strip() + "\n"

    @staticmethod
    def _write_csv(path: str, table: Dict[str, Any]) -> None:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(table["columns"])
            writer.writerows(table["rows"])

    @staticmethod
    def _markdown_table(columns: List[str], rows: List[List[Any]]) -> str:
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        lines = [header, separator]
        for row in rows:
            lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
        return "\n".join(lines)

    @staticmethod
    def _get_grouped_stats(result: Dict[str, Any] | None, group_field: str) -> Dict[str, Any] | None:
        if not result:
            return None
        stats = result.get("statistics", {})
        for candidate in (stats.get("all_samples", {}), stats):
            grouped = candidate.get("grouped")
            if grouped and group_field in grouped:
                return grouped[group_field]
        return None

    @staticmethod
    def _extract_accuracy(
        result: Dict[str, Any] | None,
        group_key: str | None,
        grouped: Dict[str, Any] | None = None,
    ) -> Any:
        if not result:
            return None
        if group_key is None:
            return result.get("statistics", {}).get("overall", {}).get("accuracy")
        if grouped and group_key in grouped:
            return grouped[group_key].get("accuracy")
        return None

    @staticmethod
    def _extract_metric(result: Dict[str, Any] | None, key: str) -> Any:
        if not result:
            return None
        if key in result:
            return result.get(key)
        stats = result.get("statistics", {})
        return (stats.get("inference_metrics") or {}).get(key)

    @staticmethod
    def _format_accuracy_value(value: Any) -> str:
        if value is None:
            return "-"
        return f"{float(value) * 100:.1f}"

    @staticmethod
    def _format_integer_metric(value: Any) -> str:
        if value is None:
            return "-"
        rounded = int(math.floor(float(value) + 0.5))
        return str(rounded)

    def _build_matrix_table(self, matrix: Dict[str, Dict[str, Any]], datasets: List[str]) -> str:
        header = ["Model"] + datasets
        col_widths = [max(12, len(h)) for h in header]

        for row_name in matrix:
            col_widths[0] = max(col_widths[0], len(row_name))
            for idx, dataset in enumerate(datasets, start=1):
                cell = self._format_accuracy(matrix.get(row_name, {}).get(dataset))
                col_widths[idx] = max(col_widths[idx], len(cell))

        lines = []
        lines.append("┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐")
        lines.append("│" + "│".join(f" {h:<{w}} " for h, w in zip(header, col_widths)) + "│")
        lines.append("├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤")

        for row_name in sorted(matrix):
            row = [row_name]
            for dataset in datasets:
                row.append(self._format_accuracy(matrix.get(row_name, {}).get(dataset)))
            lines.append("│" + "│".join(f" {v:<{w}} " for v, w in zip(row, col_widths)) + "│")

        lines.append("└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘")
        return "\n".join(lines)

    def _build_grouped_matrix_table(
        self,
        *,
        dataset_name: str,
        group_field: str,
        group_values: List[Any],
        matrix: Dict[str, Dict[str, Any]],
        overall_matrix: Dict[str, Dict[str, Any]],
    ) -> str:
        normalized_group_values = [str(value) for value in group_values]
        if not normalized_group_values:
            discovered = set()
            for row in matrix.values():
                discovered.update(row.keys())
            normalized_group_values = sorted(discovered)

        header = ["Model"] + normalized_group_values + ["Avg"]
        col_widths = [max(12, len(str(h))) for h in header]

        for row_name in sorted(matrix):
            col_widths[0] = max(col_widths[0], len(row_name))
            for idx, group_value in enumerate(normalized_group_values, start=1):
                cell = self._format_accuracy(matrix.get(row_name, {}).get(group_value))
                col_widths[idx] = max(col_widths[idx], len(cell))
            avg_cell = self._format_accuracy(overall_matrix.get(row_name, {}).get(dataset_name))
            col_widths[-1] = max(col_widths[-1], len(avg_cell))

        lines = []
        lines.append("┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐")
        lines.append("│" + "│".join(f" {str(h):<{w}} " for h, w in zip(header, col_widths)) + "│")
        lines.append("├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤")

        for row_name in sorted(matrix):
            row = [row_name]
            for group_value in normalized_group_values:
                row.append(self._format_accuracy(matrix.get(row_name, {}).get(group_value)))
            row.append(self._format_accuracy(overall_matrix.get(row_name, {}).get(dataset_name)))
            lines.append("│" + "│".join(f" {str(v):<{w}} " for v, w in zip(row, col_widths)) + "│")

        lines.append("└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘")
        return "\n".join(lines)

    def _format_accuracy(self, stat: Any) -> str:
        if not stat:
            return "N/A"
        accuracy = stat.get("accuracy")
        if accuracy is None:
            return "N/A"
        return f"{accuracy:.1%}"

    def _build_row_label(
        self,
        model_name: str,
        config_type: str,
        requested_configs: List[str],
        eval_results: List[Dict],
    ) -> str:
        logical = self._logical_model_name(model_name)
        if len(set(requested_configs)) > 1:
            return f"{logical} [{config_type}]"

        backends = {
            self._backend_name(item.get("model", ""))
            for item in eval_results
            if self._logical_model_name(item.get("model", "")) == logical
        }
        backends.discard("")
        if len(backends) > 1:
            return model_name.replace("__", " / ")
        return logical

    @staticmethod
    def _logical_model_name(model_name: str) -> str:
        return model_name.split("__", 1)[0]

    @staticmethod
    def _backend_name(model_name: str) -> str:
        return model_name.split("__", 1)[1] if "__" in model_name else ""

    @staticmethod
    def _slugify(value: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized or "unknown"

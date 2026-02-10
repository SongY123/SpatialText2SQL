"""结果报告生成器 - 动态适配数据集结构生成报告"""
import json
import os
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
            group_field = self.grouping_fields[0]  # 假设只有一个分组字段
            group_values = self.grouping_values.get(group_field, [])
            lines.append(f"Grouping: {group_field} ({len(group_values)} groups: {', '.join(map(str, group_values))})")
        
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
            lines.append(self._generate_table(results))
        
        lines.append("\n" + "="*80)
        return "\n".join(lines)
    
    def _generate_table(self, results: List[Dict]) -> str:
        """生成结果表格"""
        if not self.grouping_fields:
            return self._generate_simple_table(results)
        
        group_field = self.grouping_fields[0]
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
            
            if 'grouped' in stats and group_field in stats['grouped']:
                grouped_stats = stats['grouped'][group_field]
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

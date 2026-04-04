"""评估模块 - EX指标计算，动态统计，适配PostgreSQL"""
import psycopg2
import json
import os
from decimal import Decimal
from typing import List, Dict, Any, Optional
from collections import defaultdict


def _json_default(obj):
    """JSON序列化时处理不可序列化类型（如Decimal）"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


class Evaluator:
    """评估器 - 计算EX（Execution Accuracy）指标"""
    
    def __init__(self, db_config: Dict, eval_config: Dict):
        """
        初始化评估器
        
        Args:
            db_config: 数据库配置
            eval_config: 评估配置
        """
        self.db_config = db_config
        self.eval_config = eval_config
        self.timeout = eval_config.get('evaluation', {}).get('timeout', 60)
    
    def evaluate(self, predictions: List[Dict], dataset_info: Dict,
                model_name: str, config_type: str) -> Dict:
        """
        评估预测结果
        
        Args:
            predictions: 预测结果列表
            dataset_info: 数据集元信息
            model_name: 模型名称
            config_type: 配置类型
            
        Returns:
            评估结果字典
        """
        print(f"\n{'='*70}")
        print(f"开始评估: {model_name} | {config_type}")
        print(f"{'='*70}\n")
        
        # 计算每条数据的准确率
        detailed_results = []
        
        for idx, pred in enumerate(predictions, 1):
            gold_candidates = pred.get('gold_sql_candidates')
            accuracy_info = self._execution_accuracy(
                pred['predicted_sql'],
                pred['gold_sql'],
                gold_sql_candidates=gold_candidates if isinstance(gold_candidates, list) and gold_candidates else None
            )
            
            result_item = {
                **pred,
                'correct': accuracy_info['correct'],
                'error_type': accuracy_info['error_type'],
                'error_message': accuracy_info['error_message'],
                'pred_result_count': accuracy_info['pred_result_count'],
                'gold_result_count': accuracy_info['gold_result_count']
            }
            
            # 如果有执行错误详情，添加到结果中
            if accuracy_info.get('execution_error'):
                result_item['execution_error'] = accuracy_info['execution_error']
            
            # 如果有差异详情，添加到结果中
            if accuracy_info.get('diff_details'):
                result_item['diff_details'] = accuracy_info['diff_details']
            
            # 多 gold 候选时记录命中的候选索引（便于回溯）
            if accuracy_info.get('matched_gold_index') is not None:
                result_item['matched_gold_index'] = accuracy_info['matched_gold_index']
            
            detailed_results.append(result_item)
            
            if idx % 10 == 0:
                print(f"  已评估 {idx}/{len(predictions)} 条")
        
        # 计算统计结果
        stats = self._compute_statistics(detailed_results, dataset_info)
        
        # 组装最终结果
        eval_result = {
            'model': model_name,
            'config': config_type,
            'dataset': dataset_info['name'],
            'dataset_info': dataset_info,
            'statistics': stats,
            'details': detailed_results
        }
        
        # 打印结果摘要
        self._print_summary(stats, dataset_info)
        
        return eval_result
    
    def _execution_accuracy(
        self,
        predicted_sql: str,
        gold_sql: str,
        gold_sql_candidates: Optional[List[str]] = None,
    ) -> Dict:
        """
        计算执行准确率。若提供 gold_sql_candidates，预测结果与任一候选结果一致即判对。
        
        Args:
            predicted_sql: 预测的SQL
            gold_sql: 标准SQL（主 gold）
            gold_sql_candidates: 可选，Eval 多候选 SQL 列表；无或空时仅用 gold_sql（与原逻辑一致）
            
        Returns:
            包含 correct, error_type, error_message, pred_result_count, gold_result_count,
            execution_error, diff_details；多候选命中时含 matched_gold_index（0=gold_sql，1+=candidates 索引）。
        """
        result_info = {
            'correct': 0,
            'error_type': None,
            'error_message': None,
            'pred_result_count': None,
            'gold_result_count': None,
            'execution_error': None,
        }
        
        if not predicted_sql or not predicted_sql.strip():
            result_info['error_type'] = 'empty_sql'
            result_info['error_message'] = '预测SQL为空'
            return result_info
        
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                options=f'-c statement_timeout={self.timeout * 1000}'
            )
            cursor = conn.cursor()
            
            try:
                cursor.execute(predicted_sql)
                pred_result = set(cursor.fetchall())
                result_info['pred_result_count'] = len(pred_result)
            except Exception as e:
                result_info['error_type'] = 'execution_error'
                result_info['error_message'] = f'预测SQL执行失败: {str(e)}'
                result_info['execution_error'] = {
                    'sql': predicted_sql,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
                cursor.close()
                conn.close()
                return result_info
            
            # 先与主 gold_sql 比较
            gold_list = [gold_sql]
            if gold_sql_candidates:
                gold_list = [gold_sql] + [c for c in gold_sql_candidates if c and c.strip() and c != gold_sql]
            
            gold_result = None
            matched_index = None
            last_gold_error = None
            
            for i, gsql in enumerate(gold_list):
                try:
                    cursor.execute(gsql)
                    gr = set(cursor.fetchall())
                    if gold_result is None:
                        result_info['gold_result_count'] = len(gr)
                    if pred_result == gr:
                        result_info['correct'] = 1
                        result_info['error_type'] = None
                        result_info['error_message'] = None
                        matched_index = i
                        gold_result = gr
                        break
                    if gold_result is None:
                        gold_result = gr
                except Exception as e:
                    last_gold_error = e
                    if gold_result is None and i == 0:
                        result_info['error_type'] = 'execution_error'
                        result_info['error_message'] = f'标准SQL执行失败: {str(e)}'
                        result_info['execution_error'] = {
                            'sql': gsql,
                            'error': str(e),
                            'error_type': type(e).__name__
                        }
                        cursor.close()
                        conn.close()
                        return result_info
                    continue
            
            cursor.close()
            conn.close()
            
            if result_info['correct'] == 1:
                if matched_index is not None and (gold_sql_candidates or matched_index > 0):
                    result_info['matched_gold_index'] = matched_index
                return result_info
            
            if gold_result is None and last_gold_error:
                result_info['error_type'] = 'execution_error'
                result_info['error_message'] = f'标准SQL执行失败: {str(last_gold_error)}'
                result_info['execution_error'] = {
                    'sql': gold_list[0],
                    'error': str(last_gold_error),
                    'error_type': type(last_gold_error).__name__
                }
                return result_info
            
            result_info['error_type'] = 'result_mismatch'
            result_info['error_message'] = f'结果集不匹配: 预测{len(pred_result)}条，标准{result_info["gold_result_count"]}条'
            if gold_result is not None and len(pred_result) <= 10 and len(gold_result) <= 10:
                only_in_pred = pred_result - gold_result
                only_in_gold = gold_result - pred_result
                result_info['diff_details'] = {
                    'only_in_predicted': list(only_in_pred) if only_in_pred else None,
                    'only_in_gold': list(only_in_gold) if only_in_gold else None
                }
            return result_info
            
        except Exception as e:
            result_info['error_type'] = 'execution_error'
            result_info['error_message'] = f'评估过程出错: {str(e)}'
            result_info['execution_error'] = {
                'error': str(e),
                'error_type': type(e).__name__
            }
            return result_info
    
    def _compute_statistics(self, results: List[Dict], dataset_info: Dict) -> Dict:
        """
        根据数据集元信息动态计算统计结果
        
        Args:
            results: 详细评估结果
            dataset_info: 数据集元信息
            
        Returns:
            统计结果字典（overall + grouped）
        """
        stats = {
            'overall': self._compute_overall_accuracy(results)
        }
        
        # 如果有分组字段，按字段统计
        grouping_fields = dataset_info.get('grouping_fields', [])
        if grouping_fields:
            stats['grouped'] = {}
            for field in grouping_fields:
                stats['grouped'][field] = self._compute_grouped_accuracy(results, field)
        
        return stats
    
    def _compute_overall_accuracy(self, results: List[Dict]) -> Dict:
        """计算总体准确率"""
        total = len(results)
        correct = sum(r['correct'] for r in results)
        accuracy = correct / total if total > 0 else 0.0
        
        return {
            'total': total,
            'correct': correct,
            'accuracy': accuracy
        }
    
    def _compute_grouped_accuracy(self, results: List[Dict], field: str) -> Dict:
        """按分组字段计算准确率"""
        grouped_data = defaultdict(list)
        
        # 按字段分组
        for result in results:
            group_value = result.get('metadata', {}).get(field)
            if group_value is not None:
                grouped_data[group_value].append(result)
        
        # 计算每组的准确率
        grouped_stats = {}
        for group_value, group_results in grouped_data.items():
            total = len(group_results)
            correct = sum(r['correct'] for r in group_results)
            accuracy = correct / total if total > 0 else 0.0
            
            grouped_stats[str(group_value)] = {
                'total': total,
                'correct': correct,
                'accuracy': accuracy
            }
        
        return grouped_stats
    
    def _print_summary(self, stats: Dict, dataset_info: Dict):
        """打印评估摘要"""
        print(f"\n{'='*70}")
        print("评估结果摘要")
        print(f"{'='*70}\n")
        
        # 总体准确率
        overall = stats['overall']
        print(f"总体准确率: {overall['correct']}/{overall['total']} = {overall['accuracy']:.2%}")
        
        # 分组准确率
        if 'grouped' in stats:
            for field, group_stats in stats['grouped'].items():
                print(f"\n按{field}分组:")
                for group_value, group_result in sorted(group_stats.items()):
                    print(f"  {field}={group_value}: {group_result['correct']}/{group_result['total']} = {group_result['accuracy']:.2%}")
        
        print(f"\n{'='*70}\n")
    
    def save_evaluation(self, eval_result: Dict, output_dir: str):
        """
        保存评估结果
        
        Args:
            eval_result: 评估结果
            output_dir: 输出目录
        """
        os.makedirs(output_dir, exist_ok=True)
        
        model_name = eval_result['model']
        config_type = eval_result['config']
        
        # 保存详细结果
        output_file = os.path.join(output_dir, f"{model_name}_{config_type}_eval.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(eval_result, f, ensure_ascii=False, indent=2, default=_json_default)
        
        print(f"评估结果已保存: {output_file}")

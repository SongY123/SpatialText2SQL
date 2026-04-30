"""评估模块 - EX指标计算，动态统计，适配PostgreSQL"""
import psycopg2
import json
import os
import time
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional
from collections import Counter, defaultdict


def _json_default(obj):
    """JSON序列化时处理评估结果中的常见标量类型。"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            scalar = obj.item()
        except Exception:
            scalar = None
        else:
            if scalar is obj:
                scalar = None
        if scalar is not None:
            return _json_default(scalar) if not isinstance(scalar, (str, int, float, bool, list, dict, type(None))) else scalar
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


class Evaluator:
    """评估器 - 计算EX（Execution Accuracy）指标"""

    INDETERMINATE_ERROR_TYPES = {
        'pred_ok_gold_timeout',
        'both_timeout',
        'connection_error',
        'gold_execution_error',
    }
    BENCHMARK_GOLD_UNSTABLE_ERROR_TYPES = {
        'pred_ok_gold_timeout',
        'both_timeout',
        'gold_execution_error',
    }
    
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
        evaluation_cfg = eval_config.get('evaluation', {})
        self.connect_timeout = evaluation_cfg.get('connect_timeout', 10)
        self.retry_backoff_sec = evaluation_cfg.get('retry_backoff_sec', 1.0)
        self.max_retry_backoff_sec = evaluation_cfg.get('max_retry_backoff_sec', 30.0)
        self.network_recovery_timeout = evaluation_cfg.get('network_recovery_timeout')
        self.retry_forever_on_connection_error = evaluation_cfg.get(
            'retry_forever_on_connection_error',
            False,
        )

    def evaluate(
        self,
        predictions: List[Dict],
        dataset_info: Dict,
        model_name: str,
        config_type: str,
        output_dir: Optional[str] = None,
        resume: bool = False,
        overwrite: bool = False,
    ) -> Dict:
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
        del output_dir, resume, overwrite
        print(f"\n{'='*70}")
        print(f"开始评估: {model_name} | {config_type}")
        print(f"{'='*70}\n")

        # 计算每条数据的准确率
        detailed_results = []
        
        for idx, pred in enumerate(predictions, 1):
            gold_candidates = pred.get('gold_sql_candidates')
            accuracy_info = self._finalize_result_info(self._execution_accuracy(
                pred['predicted_sql'],
                pred['gold_sql'],
                gold_sql_candidates=gold_candidates if isinstance(gold_candidates, list) and gold_candidates else None
            ))
            
            result_item = {
                **pred,
                'correct': accuracy_info['correct'],
                'error_type': accuracy_info['error_type'],
                'error_message': accuracy_info['error_message'],
                'pred_result_count': accuracy_info['pred_result_count'],
                'gold_result_count': accuracy_info['gold_result_count'],
                'judgement_status': accuracy_info['judgement_status'],
                'is_indeterminate': accuracy_info['is_indeterminate'],
            }
            
            # 如果有执行错误详情，添加到结果中
            if accuracy_info.get('execution_error'):
                result_item['execution_error'] = accuracy_info['execution_error']

            if accuracy_info.get('gold_execution_errors'):
                result_item['gold_execution_errors'] = accuracy_info['gold_execution_errors']
            
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
        stats = self._compute_statistics(
            detailed_results,
            dataset_info,
        )
        
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
        sample_label: Optional[str] = None,
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
            return self._finalize_result_info(result_info)

        pred_exec = self._execute_sql(
            predicted_sql,
            stage_name='预测SQL',
            sample_label=sample_label,
        )
        if pred_exec['status'] == 'ok':
            pred_result = pred_exec['result']
            result_info['pred_result_count'] = pred_exec['result_count']
        else:
            result_info['execution_error'] = self._build_execution_payload(pred_exec)
            if pred_exec['status'] == 'connection_error':
                result_info['error_type'] = 'connection_error'
                result_info['error_message'] = self._format_connection_error('预测SQL执行失败', pred_exec)
                return self._finalize_result_info(result_info)
            if pred_exec['status'] != 'timeout':
                result_info['error_type'] = 'execution_error'
                result_info['error_message'] = self._format_execution_status('预测SQL执行失败', pred_exec)
                return self._finalize_result_info(result_info)
            pred_result = None

        gold_list = [gold_sql]
        if gold_sql_candidates:
            gold_list = [gold_sql] + [c for c in gold_sql_candidates if c and c.strip() and c != gold_sql]

        gold_result = None
        matched_index = None
        gold_execution_errors = []
        gold_success = False

        for i, gsql in enumerate(gold_list):
            gold_exec = self._execute_sql(
                gsql,
                stage_name='标准SQL' if i == 0 else '标准SQL候选',
                sample_label=sample_label,
            )
            if gold_exec['status'] == 'ok':
                if gold_result is None:
                    gold_result = gold_exec['result']
                    result_info['gold_result_count'] = gold_exec['result_count']
                gold_success = True
                if pred_exec['status'] == 'timeout':
                    break
                if pred_result == gold_exec['result']:
                    result_info['correct'] = 1
                    result_info['error_type'] = None
                    result_info['error_message'] = None
                    matched_index = i
                    gold_result = gold_exec['result']
                    result_info['gold_result_count'] = gold_exec['result_count']
                    break
                continue

            gold_execution_errors.append(
                self._build_gold_error_record(i, gold_exec)
            )

        if gold_execution_errors:
            result_info['gold_execution_errors'] = gold_execution_errors

        if pred_exec['status'] == 'timeout':
            gold_outcome = self._summarize_gold_outcome(gold_execution_errors, gold_success)
            if gold_outcome == 'ok':
                result_info['error_type'] = 'pred_timeout_gold_ok'
                result_info['error_message'] = '预测SQL执行超时，但标准SQL可正常执行'
            elif gold_outcome == 'timeout':
                result_info['error_type'] = 'both_timeout'
                result_info['error_message'] = '预测SQL与标准SQL均执行超时'
            elif gold_outcome == 'connection_error':
                result_info['error_type'] = 'connection_error'
                result_info['error_message'] = self._format_connection_error('标准SQL执行失败', gold_execution_errors[0])
            else:
                result_info['error_type'] = 'gold_execution_error'
                result_info['error_message'] = '预测SQL执行超时，且标准SQL无法稳定执行'
            return self._finalize_result_info(result_info)

        if result_info['correct'] == 1:
            if matched_index is not None and (gold_sql_candidates or matched_index > 0):
                result_info['matched_gold_index'] = matched_index
            return self._finalize_result_info(result_info)

        gold_outcome = self._summarize_gold_outcome(gold_execution_errors, gold_success)
        if gold_outcome == 'ok':
            result_info['error_type'] = 'result_mismatch'
            result_info['error_message'] = f'结果集不匹配: 预测{len(pred_result)}条，标准{result_info["gold_result_count"]}条'
            if gold_result is not None and len(pred_result) <= 10 and len(gold_result) <= 10:
                only_in_pred = pred_result - gold_result
                only_in_gold = gold_result - pred_result
                result_info['diff_details'] = {
                    'only_in_predicted': list(only_in_pred) if only_in_pred else None,
                    'only_in_gold': list(only_in_gold) if only_in_gold else None
                }
        elif gold_outcome == 'timeout':
            result_info['error_type'] = 'pred_ok_gold_timeout'
            result_info['error_message'] = '预测SQL执行成功，但标准SQL执行超时'
        elif gold_outcome == 'connection_error':
            result_info['error_type'] = 'connection_error'
            result_info['error_message'] = self._format_connection_error('标准SQL执行失败', gold_execution_errors[0])
        else:
            result_info['error_type'] = 'gold_execution_error'
            result_info['error_message'] = '标准SQL无法稳定执行，当前样本无法可靠判定'
            if not result_info.get('execution_error') and gold_execution_errors:
                result_info['execution_error'] = {
                    'sql': gold_execution_errors[0].get('sql'),
                    'error': gold_execution_errors[0].get('error'),
                    'error_type': gold_execution_errors[0].get('error_type'),
                    'status': gold_execution_errors[0].get('status'),
                    'stage': gold_execution_errors[0].get('stage'),
                }

        return self._finalize_result_info(result_info)

    def _connect_with_retry(self, sample_label: Optional[str] = None):
        """带简单重试的数据库连接。"""
        backoff = self.retry_backoff_sec
        recovery_start = None
        while True:
            try:
                return psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    connect_timeout=self.connect_timeout,
                    options=f'-c statement_timeout={self.timeout * 1000}'
                )
            except Exception as exc:
                if not self._is_connection_exception(exc):
                    raise
                if recovery_start is None:
                    recovery_start = time.monotonic()
                if self._should_stop_connection_retry(recovery_start):
                    raise
                time.sleep(backoff)
                backoff = min(max(backoff * 2, self.retry_backoff_sec), self.max_retry_backoff_sec)

    def _format_execution_error(self, prefix: str, error: Exception) -> str:
        message = str(error)
        if 'statement timeout' in message.lower():
            return f'{prefix}: 执行超时'
        return f'{prefix}: {message}'

    def _format_execution_status(self, prefix: str, execution_info: Dict[str, Any]) -> str:
        if execution_info.get('status') == 'timeout':
            return f'{prefix}: 执行超时'
        if execution_info.get('status') == 'connection_error':
            return self._format_connection_error(prefix, execution_info)
        return f'{prefix}: {execution_info.get("error")}'

    def _format_connection_error(self, prefix: str, execution_info: Dict[str, Any]) -> str:
        return f'{prefix}: 数据库连接失败 ({execution_info.get("error")})'

    def _execute_sql(
        self,
        sql: str,
        *,
        stage_name: str,
        sample_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        info = {
            'stage': stage_name,
            'sql': sql,
            'status': None,
            'result': None,
            'result_count': None,
            'error': None,
            'error_type': None,
        }
        conn = None
        cursor = None
        try:
            conn = self._connect_with_retry(sample_label=sample_label)
            cursor = conn.cursor()
            cursor.execute(sql)
            result = set(cursor.fetchall())
            info['status'] = 'ok'
            info['result'] = result
            info['result_count'] = len(result)
            return info
        except Exception as exc:
            info['error'] = str(exc)
            info['error_type'] = type(exc).__name__
            if self._is_statement_timeout_error(exc):
                info['status'] = 'timeout'
            elif self._is_connection_exception(exc):
                info['status'] = 'connection_error'
            else:
                info['status'] = 'execution_error'
            return info
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _build_execution_payload(self, execution_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'sql': execution_info.get('sql'),
            'error': execution_info.get('error'),
            'error_type': execution_info.get('error_type'),
            'status': execution_info.get('status'),
            'stage': execution_info.get('stage'),
        }

    def _build_gold_error_record(self, candidate_index: int, execution_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'stage': '标准SQL' if candidate_index == 0 else '标准SQL候选',
            'candidate_index': candidate_index,
            'sql': execution_info.get('sql'),
            'error': execution_info.get('error'),
            'error_type': execution_info.get('error_type'),
            'status': execution_info.get('status'),
        }

    def _summarize_gold_outcome(self, gold_execution_errors: List[Dict[str, Any]], gold_success: bool) -> str:
        if gold_success:
            return 'ok'
        statuses = {item.get('status') for item in gold_execution_errors if item.get('status')}
        if 'connection_error' in statuses:
            return 'connection_error'
        if statuses == {'timeout'}:
            return 'timeout'
        return 'execution_error'

    def _should_stop_connection_retry(self, recovery_start: float) -> bool:
        timeout_limit = self.network_recovery_timeout
        if timeout_limit is not None and timeout_limit >= 0:
            if time.monotonic() - recovery_start >= float(timeout_limit):
                return True
        return not self.retry_forever_on_connection_error and timeout_limit is None

    def _is_statement_timeout_error(self, error: Exception) -> bool:
        return 'statement timeout' in str(error).lower()

    def _is_connection_exception(self, error: Exception) -> bool:
        return isinstance(
            error,
            (
                getattr(psycopg2, 'OperationalError', Exception),
                getattr(psycopg2, 'InterfaceError', Exception),
            ),
        )

    def _finalize_result_info(self, result_info: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {
            'correct': 0,
            'error_type': None,
            'error_message': None,
            'pred_result_count': None,
            'gold_result_count': None,
            'execution_error': None,
            **result_info,
        }
        judgement_status = self._judgement_status(normalized)
        normalized['judgement_status'] = judgement_status
        normalized['is_indeterminate'] = judgement_status == 'indeterminate'
        return normalized

    def _judgement_status(self, result_info: Dict[str, Any]) -> str:
        if int(result_info.get('correct') or 0) == 1:
            return 'correct'
        if result_info.get('error_type') in self.INDETERMINATE_ERROR_TYPES:
            return 'indeterminate'
        return 'wrong'
    
    def _compute_statistics(
        self,
        results: List[Dict],
        dataset_info: Dict,
    ) -> Dict:
        """
        根据数据集元信息动态计算统计结果
        
        Args:
            results: 详细评估结果
            dataset_info: 数据集元信息
            
        Returns:
            统计结果字典（overall + grouped）
        """
        all_results = self._compute_scoped_statistics(results, dataset_info)
        stats = {
            'overall': all_results['overall'],
            'all_samples': all_results,
            'inference_metrics': self._compute_inference_metrics_summary(results),
        }
        return stats

    def _compute_scoped_statistics(self, results: List[Dict], dataset_info: Dict) -> Dict:
        scoped = {
            'available': True,
            'overall': self._compute_overall_accuracy(results)
        }
        grouping_fields = dataset_info.get('grouping_fields', [])
        if grouping_fields:
            scoped['grouped'] = {}
            for field in grouping_fields:
                scoped['grouped'][field] = self._compute_grouped_accuracy(results, field)
        return scoped
    
    def _compute_overall_accuracy(self, results: List[Dict]) -> Dict:
        """计算总体准确率"""
        total = len(results)
        correct = sum(int(r.get('correct', 0)) for r in results)
        accuracy = correct / total if total > 0 else 0.0
        judgement_counter = Counter(
            (r.get('judgement_status') or self._judgement_status(r))
            for r in results
        )
        error_counter = Counter(
            r.get('error_type')
            for r in results
            if r.get('error_type')
        )
        indeterminate_total = judgement_counter.get('indeterminate', 0)
        judged_total = total - indeterminate_total
        judged_accuracy = correct / judged_total if judged_total > 0 else 0.0

        return {
            'total': total,
            'correct': correct,
            'accuracy': accuracy,
            'strict_accuracy': accuracy,
            'judged_total': judged_total,
            'judged_accuracy': judged_accuracy,
            'indeterminate_total': indeterminate_total,
            'indeterminate_rate': (indeterminate_total / total) if total > 0 else 0.0,
            'judgement_breakdown': dict(judgement_counter),
            'error_breakdown': dict(error_counter),
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
            grouped_stats[str(group_value)] = self._compute_overall_accuracy(group_results)
        
        return grouped_stats

    def _compute_inference_metrics_summary(self, results: List[Dict]) -> Dict:
        """汇总推理 token 与耗时指标，平均值按样本总数归一。"""
        question_count = len(results)
        sum_input_tokens = 0.0
        sum_output_tokens = 0.0
        sum_total_tokens = 0.0
        sum_latency_ms = 0.0
        input_measurements = 0
        output_measurements = 0
        total_measurements = 0
        latency_measurements = 0

        for result in results:
            metrics = result.get('inference_metrics') or {}
            input_tokens = metrics.get('input_tokens')
            output_tokens = metrics.get('output_tokens')
            total_tokens = metrics.get('total_tokens')
            latency_ms = metrics.get('latency_ms')

            if input_tokens is not None:
                sum_input_tokens += float(input_tokens)
                input_measurements += 1
            if output_tokens is not None:
                sum_output_tokens += float(output_tokens)
                output_measurements += 1
            if total_tokens is not None:
                sum_total_tokens += float(total_tokens)
                total_measurements += 1
            if latency_ms is not None:
                sum_latency_ms += float(latency_ms)
                latency_measurements += 1

        return {
            'question_count': question_count,
            'avg_input_tokens': sum_input_tokens / question_count if question_count else 0.0,
            'avg_output_tokens': sum_output_tokens / question_count if question_count else 0.0,
            'avg_total_tokens': sum_total_tokens / question_count if question_count else 0.0,
            'avg_latency_ms': sum_latency_ms / question_count if question_count else 0.0,
            'sum_input_tokens': sum_input_tokens,
            'sum_output_tokens': sum_output_tokens,
            'sum_total_tokens': sum_total_tokens,
            'sum_latency_ms': sum_latency_ms,
            'input_measurements': input_measurements,
            'output_measurements': output_measurements,
            'total_measurements': total_measurements,
            'latency_measurements': latency_measurements,
        }
    
    def _print_summary(self, stats: Dict, dataset_info: Dict):
        """打印评估摘要"""
        print(f"\n{'='*70}")
        print("评估结果摘要")
        print(f"{'='*70}\n")
        
        # 总体准确率
        overall = stats['overall']
        print(f"总体准确率: {overall['correct']}/{overall['total']} = {overall['accuracy']:.2%}")
        if overall.get('indeterminate_total'):
            print(
                "可判定样本准确率: "
                f"{overall['correct']}/{overall['judged_total']} = {overall['judged_accuracy']:.2%}"
            )
            print(
                "不确定样本占比: "
                f"{overall['indeterminate_total']}/{overall['total']} = {overall['indeterminate_rate']:.2%}"
            )
            if overall.get('error_breakdown'):
                breakdown = ", ".join(
                    f"{name}={count}"
                    for name, count in sorted(overall['error_breakdown'].items())
                )
                print(f"错误分布: {breakdown}")
        inference_metrics = stats.get('inference_metrics')
        if inference_metrics:
            print(
                "平均推理开销: "
                f"in={inference_metrics['avg_input_tokens']:.1f}, "
                f"out={inference_metrics['avg_output_tokens']:.1f}, "
                f"total={inference_metrics['avg_total_tokens']:.1f}, "
                f"latency={inference_metrics['avg_latency_ms']:.1f} ms"
            )
        
        # 分组准确率
        grouped_source = stats.get('all_samples', stats)
        if 'grouped' in grouped_source:
            for field, group_stats in grouped_source['grouped'].items():
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

        # 结果路径已经通过 dataset/model/config 层级唯一确定，因此文件名保持固定。
        output_file = os.path.join(output_dir, "evaluation.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(eval_result, f, ensure_ascii=False, indent=2, default=_json_default)
        
        print(f"评估结果已保存: {output_file}")

"""
BIRD Evaluation - PostgreSQL适配版本
计算ves (Valid Efficiency Score) 指标
"""

import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Tuple, Optional
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import math
import numpy as np
from func_timeout import func_timeout, FunctionTimedOut


class PostgreSQLVESEvaluator:
    """PostgreSQL VES评估器，用于计算BIRD的ves指标"""
    
    def __init__(
        self,
        db_config: Dict,
        num_cpus: int = 16,
        iterate_num: int = 100,
        meta_time_out: float = 30.0
    ):
        """
        初始化评估器
        
        Args:
            db_config: PostgreSQL数据库配置
            num_cpus: 并行处理的CPU数量
            iterate_num: 每次评估的迭代次数
            meta_time_out: 单次查询超时时间（秒）
        """
        self.db_config = db_config
        self.num_cpus = num_cpus
        self.iterate_num = iterate_num
        self.meta_time_out = meta_time_out
    
    def connect_db(self, db_name: str):
        """
        连接到指定的数据库
        
        Args:
            db_name: 数据库名称
            
        Returns:
            数据库连接对象
        """
        config = self.db_config.copy()
        config["dbname"] = db_name
        try:
            conn = psycopg2.connect(**config)
            return conn
        except Exception as e:
            print(f"Error connecting to database {db_name}: {e}")
            return None
    
    def execute_sql(
        self,
        conn,
        sql: str,
        return_time: bool = False,
        timeout_seconds: Optional[float] = None,
        lock_timeout_seconds: Optional[float] = None,
    ) -> Tuple[bool, Optional[List], Optional[float], Optional[str]]:
        """
        执行SQL查询
        
        Args:
            conn: 数据库连接
            sql: SQL查询语句
            return_time: 是否返回执行时间
            
        Returns:
            (success, result, execution_time, error_message)
        """
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            if timeout_seconds is not None:
                cursor.execute(f"SET statement_timeout = {int(timeout_seconds * 1000)}")
            if lock_timeout_seconds is not None:
                cursor.execute(f"SET lock_timeout = {int(lock_timeout_seconds * 1000)}")

            start_time = time.time()
            cursor.execute(sql)
            
            if cursor.description:
                result = cursor.fetchall()
                result = [dict(row) for row in result]
            else:
                result = []
            
            exec_time = time.time() - start_time
            conn.commit()
            cursor.close()
            
            if return_time:
                return True, result, exec_time, None
            else:
                return True, result, None, None
            
        except psycopg2.errors.QueryCanceled:
            return False, None, None, "Query timeout"
        except Exception as e:
            conn.rollback()
            return False, None, None, str(e)
    
    def normalize_result(self, result: List[Dict]) -> List[Tuple]:
        """
        规范化查询结果，转换为可比较的元组格式
        
        Args:
            result: 查询结果列表
            
        Returns:
            规范化后的结果（元组列表）
        """
        if not result:
            return []
        
        normalized = []
        for row in result:
            keys = sorted(row.keys())
            values = []
            for k in keys:
                v = row.get(k)
                if v is None:
                    values.append("<NULL>")
                elif isinstance(v, (int, float, str, bool)):
                    values.append(str(v))
                else:
                    values.append(str(v))
            normalized.append(tuple(values))

        normalized.sort()
        
        return normalized
    
    def compare_results(self, result1: List[Dict], result2: List[Dict]) -> bool:
        """
        比较两个查询结果是否相同
        
        Args:
            result1: 第一个结果
            result2: 第二个结果
            
        Returns:
            是否相同
        """
        norm1 = set(self.normalize_result(result1))
        norm2 = set(self.normalize_result(result2))
        return norm1 == norm2
    
    def clean_abnormal(self, input_list: List[float]) -> List[float]:
        """
        清理异常值（使用3-sigma规则）
        
        Args:
            input_list: 输入列表
            
        Returns:
            清理后的列表
        """
        if not input_list:
            return []
        
        input_array = np.asarray(input_list)
        mean = np.mean(input_array)
        std = np.std(input_array)
        
        processed_list = []
        for x in input_list:
            if mean - 3 * std <= x <= mean + 3 * std:
                processed_list.append(x)
        
        return processed_list if processed_list else input_list
    
    def iterated_execute_sql(
        self,
        predicted_sql: str,
        ground_truth_sql: str,
        db_name: str
    ) -> float:
        """
        迭代执行SQL并计算效率奖励
        
        Args:
            predicted_sql: 预测的SQL
            ground_truth_sql: 标准答案SQL
            db_name: 数据库名称
            
        Returns:
            奖励分数 (0, 0.25, 0.5, 0.75, 1.0, 1.25)
        """
        conn = self.connect_db(db_name)
        if not conn:
            return 0.0
        
        try:
            # 先验证结果是否正确
            success1, result1, _, error1 = self.execute_sql(
                conn,
                predicted_sql,
                timeout_seconds=self.meta_time_out,
                lock_timeout_seconds=min(5.0, self.meta_time_out),
            )
            if not success1:
                return 0.0
            
            success2, result2, _, error2 = self.execute_sql(
                conn,
                ground_truth_sql,
                timeout_seconds=self.meta_time_out,
                lock_timeout_seconds=min(5.0, self.meta_time_out),
            )
            if not success2:
                return 0.0
            
            # 如果结果不一致，返回0
            if not self.compare_results(result1, result2):
                return 0.0
            
            # 迭代执行，收集执行时间
            diff_list = []
            for _ in range(self.iterate_num):
                # 执行预测SQL
                success1, _, time1, _ = self.execute_sql(
                    conn,
                    predicted_sql,
                    return_time=True,
                    timeout_seconds=self.meta_time_out,
                    lock_timeout_seconds=min(5.0, self.meta_time_out),
                )
                if not success1:
                    continue
                
                # 执行标准答案SQL
                success2, _, time2, _ = self.execute_sql(
                    conn,
                    ground_truth_sql,
                    return_time=True,
                    timeout_seconds=self.meta_time_out,
                    lock_timeout_seconds=min(5.0, self.meta_time_out),
                )
                if not success2:
                    continue
                
                # 计算时间比率（ground_truth_time / predicted_time）
                if time1 > 0:
                    time_ratio = time2 / time1
                    diff_list.append(time_ratio)
            
            if not diff_list:
                return 0.0
            
            # 清理异常值
            processed_diff_list = self.clean_abnormal(diff_list)
            if not processed_diff_list:
                return 0.0
            
            # 计算平均时间比率
            avg_time_ratio = sum(processed_diff_list) / len(processed_diff_list)
            
            # 根据时间比率计算奖励
            if avg_time_ratio == 0:
                reward = 0.0
            elif avg_time_ratio >= 2.0:
                reward = 1.25  # 预测SQL比标准答案快2倍以上
            elif avg_time_ratio >= 1.0:
                reward = 1.0   # 预测SQL与标准答案相当或更快
            elif avg_time_ratio >= 0.5:
                reward = 0.75  # 预测SQL是标准答案的0.5-1倍速度
            elif avg_time_ratio >= 0.25:
                reward = 0.5   # 预测SQL是标准答案的0.25-0.5倍速度
            else:
                reward = 0.25  # 预测SQL比标准答案慢很多
            
            return reward
            
        except Exception as e:
            print(f"Error in iterated_execute_sql for {db_name}: {e}")
            return 0.0
        finally:
            conn.close()
    
    def execute_model(
        self,
        predicted_sql: str,
        ground_truth_sql: str,
        db_name: str,
        idx: int
    ) -> Dict:
        """
        执行单个模型的评估（带超时）
        
        Args:
            predicted_sql: 预测的SQL
            ground_truth_sql: 标准答案SQL
            db_name: 数据库名称
            idx: 索引
            
        Returns:
            评估结果
        """
        try:
            total_timeout = self.meta_time_out * (self.iterate_num * 2 + 2)
            reward = func_timeout(
                total_timeout,
                self.iterated_execute_sql,
                args=(predicted_sql, ground_truth_sql, db_name)
            )
        except FunctionTimedOut:
            reward = 0.0
        except Exception as e:
            print(f"Error in execute_model for idx {idx}: {e}")
            reward = 0.0
        
        return {"sql_idx": idx, "reward": reward}
    
    def evaluate_batch(
        self,
        data: List[Dict]
    ) -> List[Dict]:
        """
        批量评估
        
        Args:
            data: 评估数据列表
            
        Returns:
            评估结果列表
        """
        results = []
        
        with ProcessPoolExecutor(max_workers=self.num_cpus) as executor:
            futures = []
            for i, item in enumerate(data):
                future = executor.submit(
                    self.execute_model,
                    item["predicted_sql"],
                    item["ground_truth_sql"],
                    item["db_name"],
                    i
                )
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error getting result: {e}")
        
        # 按索引排序
        results.sort(key=lambda x: x["sql_idx"])
        
        return results
    
    def compute_ves(self, exec_results: List[Dict]) -> float:
        """
        计算VES分数
        
        Args:
            exec_results: 执行结果列表
            
        Returns:
            VES分数
        """
        num_queries = len(exec_results)
        if num_queries == 0:
            return 0.0
        
        total_reward = 0.0
        for result in exec_results:
            reward = result.get("reward", 0.0)
            # 使用平方根加权
            total_reward += math.sqrt(reward) * 100
        
        ves = total_reward / num_queries
        return ves
    
    def compute_ves_by_difficulty(
        self,
        exec_results: List[Dict],
        difficulty_info: Dict[int, str]
    ) -> Tuple[float, float, float, float, List[int]]:
        """
        按难度级别计算VES
        
        Args:
            exec_results: 执行结果列表
            difficulty_info: 索引到难度的映射
            
        Returns:
            (simple_ves, moderate_ves, challenging_ves, all_ves, counts)
        """
        simple_results = []
        moderate_results = []
        challenging_results = []
        
        for i, result in enumerate(exec_results):
            difficulty = difficulty_info.get(i, "unknown")
            if difficulty == "simple":
                simple_results.append(result)
            elif difficulty == "moderate":
                moderate_results.append(result)
            elif difficulty == "challenging":
                challenging_results.append(result)
        
        simple_ves = self.compute_ves(simple_results)
        moderate_ves = self.compute_ves(moderate_results)
        challenging_ves = self.compute_ves(challenging_results)
        all_ves = self.compute_ves(exec_results)
        
        counts = [
            len(simple_results),
            len(moderate_results),
            len(challenging_results),
            len(exec_results)
        ]
        
        return simple_ves, moderate_ves, challenging_ves, all_ves, counts


def load_data(
    predicted_sql_path: str,
    ground_truth_path: str,
    data_mode: str = "dev",
    diff_json_path: Optional[str] = None,
    force_dbname: str = ""
) -> Tuple[List[Dict], Dict[int, str]]:
    """
    加载评估数据
    
    Args:
        predicted_sql_path: 预测SQL文件路径
        ground_truth_path: 标准答案路径
        data_mode: 数据模式（dev/test）
        diff_json_path: diff json文件路径（包含难度信息）
        
    Returns:
        (数据列表, 难度信息字典)
    """
    # 加载标准答案
    gt_file = os.path.join(ground_truth_path, f"{data_mode}.json")
    with open(gt_file, 'r', encoding='utf-8') as f:
        ground_truth = json.load(f)
    
    difficulty_by_qid: Dict[str, str] = {}
    if diff_json_path and os.path.exists(diff_json_path):
        with open(diff_json_path, "r", encoding="utf-8") as f:
            diff_data = json.load(f)
        for item in diff_data:
            qid = str(item.get("question_id", "")).strip()
            if not qid:
                continue
            difficulty_by_qid[qid] = item.get("difficulty", "unknown")

    data: List[Dict] = []
    difficulty_info: Dict[int, str] = {}

    for gt_item in ground_truth:
        question_id = gt_item.get("question_id")
        if question_id is None:
            continue

        predicted_file = os.path.join(predicted_sql_path, f"{question_id}.sql")
        if not os.path.exists(predicted_file):
            continue

        with open(predicted_file, "r", encoding="utf-8") as f:
            predicted_sql = f.read().strip()

        data_idx = len(data)
        if difficulty_by_qid:
            difficulty_info[data_idx] = difficulty_by_qid.get(str(question_id), "unknown")
        else:
            difficulty_info[data_idx] = gt_item.get("difficulty", "unknown")

        gt_sql = gt_item.get("SQL")
        if gt_sql is None:
            gt_sql = gt_item.get("sql")
        if not isinstance(gt_sql, str) or not gt_sql.strip():
            continue

        data.append(
            {
                "question_id": question_id,
                "db_name": force_dbname.strip() or gt_item["db_id"],
                "predicted_sql": predicted_sql,
                "ground_truth_sql": gt_sql,
            }
        )

    return data, difficulty_info


def print_results(
    score_lists: List[float],
    count_lists: List[int],
    output_path: Optional[str] = None
):
    """
    打印评估结果
    
    Args:
        score_lists: 分数列表 [simple, moderate, challenging, all]
        count_lists: 数量列表 [simple, moderate, challenging, all]
        output_path: 输出文件路径
    """
    labels = ["Simple", "Moderate", "Challenging", "All"]
    
    print("\n" + "="*80)
    print("VES Evaluation Results")
    print("="*80)
    print(f"{'Difficulty':<15} {'Count':<10} {'VES Score':<15}")
    print("-"*80)
    
    for label, score, count in zip(labels, score_lists, count_lists):
        print(f"{label:<15} {count:<10} {score:.4f}")
    
    print("="*80)
    
    # 保存到文件
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        results = {
            "simple": {"count": count_lists[0], "ves": score_lists[0]},
            "moderate": {"count": count_lists[1], "ves": score_lists[1]},
            "challenging": {"count": count_lists[2], "ves": score_lists[2]},
            "all": {"count": count_lists[3], "ves": score_lists[3]}
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="BIRD Evaluation for PostgreSQL - VES Metric")
    parser.add_argument("--db_config", type=str, required=True, help="Database config JSON file")
    parser.add_argument("--predicted_sql_path", type=str, required=True, help="Path to predicted SQL files")
    parser.add_argument("--ground_truth_path", type=str, required=True, help="Path to ground truth files")
    parser.add_argument("--data_mode", type=str, default="dev", help="Data mode: dev or test")
    parser.add_argument("--num_cpus", type=int, default=16, help="Number of CPUs for parallel processing")
    parser.add_argument("--iterate_num", type=int, default=100, help="Number of iterations for each query")
    parser.add_argument("--meta_time_out", type=float, default=30.0, help="Single query timeout in seconds")
    parser.add_argument("--diff_json_path", type=str, default="", help="Path to diff json file (for difficulty)")
    parser.add_argument("--output_path", type=str, default="./ves_results.json", help="Output results path")
    parser.add_argument("--output_log_path", type=str, default="./ves_log.txt", help="Output log path")
    parser.add_argument("--force_dbname", type=str, default="", help="If set, ignore db_id and always connect to this database")
    
    args = parser.parse_args()
    
    # 加载数据库配置
    with open(args.db_config, 'r', encoding='utf-8') as f:
        db_config = json.load(f)
    
    # 初始化评估器
    evaluator = PostgreSQLVESEvaluator(
        db_config=db_config,
        num_cpus=args.num_cpus,
        iterate_num=args.iterate_num,
        meta_time_out=args.meta_time_out
    )
    
    # 加载数据
    print("Loading data...")
    data, difficulty_info = load_data(
        predicted_sql_path=args.predicted_sql_path,
        ground_truth_path=args.ground_truth_path,
        data_mode=args.data_mode,
        diff_json_path=args.diff_json_path if args.diff_json_path else None,
        force_dbname=args.force_dbname
    )
    
    print(f"Loaded {len(data)} items for evaluation")
    
    # 执行评估
    print("Starting VES evaluation...")
    print(f"Iterations per query: {args.iterate_num}")
    print(f"Timeout per query: {args.meta_time_out} seconds")
    print("This may take a while...")
    
    start_time = time.time()
    results = evaluator.evaluate_batch(data)
    end_time = time.time()
    
    # 计算VES
    print("\nCalculating VES scores...")
    if difficulty_info:
        simple_ves, moderate_ves, challenging_ves, all_ves, counts = evaluator.compute_ves_by_difficulty(
            results, difficulty_info
        )
    else:
        all_ves = evaluator.compute_ves(results)
        simple_ves = moderate_ves = challenging_ves = 0.0
        counts = [0, 0, 0, len(results)]
    
    score_lists = [simple_ves, moderate_ves, challenging_ves, all_ves]
    
    # 打印结果
    print_results(score_lists, counts, args.output_log_path)
    
    # 保存详细结果
    detailed_results = {
        "evaluation_time": end_time - start_time,
        "total_queries": len(results),
        "simple": {"count": counts[0], "ves": simple_ves},
        "moderate": {"count": counts[1], "ves": moderate_ves},
        "challenging": {"count": counts[2], "ves": challenging_ves},
        "all": {"count": counts[3], "ves": all_ves},
        "detailed_results": results
    }
    
    os.makedirs(os.path.dirname(os.path.abspath(args.output_path)), exist_ok=True)
    with open(args.output_path, 'w', encoding='utf-8') as f:
        json.dump(detailed_results, f, indent=2, ensure_ascii=False)
    
    print(f"\nEvaluation completed in {end_time - start_time:.2f} seconds")
    print(f"Results saved to {args.output_path}")


if __name__ == "__main__":
    main()

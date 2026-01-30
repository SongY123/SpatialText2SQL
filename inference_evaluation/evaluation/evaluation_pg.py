"""
BIRD Evaluation - PostgreSQL适配版本
计算ex (Execution Accuracy) 指标
"""

import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Tuple, Optional
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import time


class PostgreSQLEvaluator:
    """PostgreSQL评估器，用于计算BIRD的ex指标"""
    
    def __init__(
        self,
        db_config: Dict,
        num_cpus: int = 16,
        meta_time_out: float = 30.0
    ):
        """
        初始化评估器
        
        Args:
            db_config: PostgreSQL数据库配置
            num_cpus: 并行处理的CPU数量
            meta_time_out: 元数据查询超时时间（秒）
        """
        self.db_config = db_config
        self.num_cpus = num_cpus
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
    
    def execute_sql(self, conn, sql: str, timeout: Optional[float] = None) -> Tuple[bool, Optional[List], Optional[str]]:
        """
        执行SQL查询
        
        Args:
            conn: 数据库连接
            sql: SQL查询语句
            timeout: 超时时间（秒）
            
        Returns:
            (success, result, error_message)
        """
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            if timeout:
                cursor.execute(f"SET statement_timeout = {int(timeout * 1000)}")
            
            cursor.execute(sql)
            
            # 获取结果
            if cursor.description:
                result = cursor.fetchall()
                # 转换为列表字典格式
                result = [dict(row) for row in result]
            else:
                result = []
            
            conn.commit()
            cursor.close()
            
            return True, result, None
            
        except psycopg2.errors.QueryCanceled:
            return False, None, "Query timeout"
        except Exception as e:
            conn.rollback()
            return False, None, str(e)
    
    def normalize_result(self, result: List[Dict]) -> List[Dict]:
        """
        规范化查询结果，确保格式一致
        
        Args:
            result: 查询结果列表
            
        Returns:
            规范化后的结果
        """
        if not result:
            return []
        
        # 对结果进行排序（基于所有列的值）
        # 转换为可比较的格式
        normalized = []
        for row in result:
            # 确保所有值都是可序列化的
            normalized_row = {}
            for key, value in row.items():
                if value is None:
                    normalized_row[key] = None
                elif isinstance(value, (int, float, str, bool)):
                    normalized_row[key] = value
                else:
                    # 转换为字符串
                    normalized_row[key] = str(value)
            normalized.append(normalized_row)
        
        # 按所有列的值排序
        if normalized:
            keys = sorted(normalized[0].keys())
            normalized.sort(key=lambda x: tuple(
                x.get(k) if x.get(k) is not None else "" 
                for k in keys
            ))
        
        return normalized
    
    def compare_results(
        self,
        result1: List[Dict],
        result2: List[Dict]
    ) -> bool:
        """
        比较两个查询结果是否相同
        
        Args:
            result1: 第一个结果
            result2: 第二个结果
            
        Returns:
            是否相同
        """
        norm1 = self.normalize_result(result1)
        norm2 = self.normalize_result(result2)
        
        if len(norm1) != len(norm2):
            return False
        
        # 比较每一行
        for row1, row2 in zip(norm1, norm2):
            if row1 != row2:
                return False
        
        return True
    
    def evaluate_single(
        self,
        db_name: str,
        predicted_sql: str,
        ground_truth_sql: str
    ) -> Tuple[bool, Optional[str]]:
        """
        评估单个SQL查询
        
        Args:
            db_name: 数据库名称
            predicted_sql: 预测的SQL
            ground_truth_sql: 标准答案SQL
            
        Returns:
            (is_correct, error_message)
        """
        conn = self.connect_db(db_name)
        if not conn:
            return False, "Failed to connect to database"
        
        try:
            # 执行预测SQL
            success1, result1, error1 = self.execute_sql(
                conn, predicted_sql, timeout=self.meta_time_out
            )
            if not success1:
                return False, f"Predicted SQL error: {error1}"
            
            # 执行标准答案SQL
            success2, result2, error2 = self.execute_sql(
                conn, ground_truth_sql, timeout=self.meta_time_out
            )
            if not success2:
                return False, f"Ground truth SQL error: {error2}"
            
            # 比较结果
            is_correct = self.compare_results(result1, result2)
            
            return is_correct, None
            
        finally:
            conn.close()
    
    def evaluate_batch(
        self,
        data: List[Dict]
    ) -> Dict:
        """
        批量评估
        
        Args:
            data: 评估数据列表，每个元素包含db_name, predicted_sql, ground_truth_sql
            
        Returns:
            评估结果统计
        """
        results = {
            "total": len(data),
            "correct": 0,
            "incorrect": 0,
            "errors": []
        }
        
        with ProcessPoolExecutor(max_workers=self.num_cpus) as executor:
            futures = []
            for item in data:
                future = executor.submit(
                    self.evaluate_single,
                    item["db_name"],
                    item["predicted_sql"],
                    item["ground_truth_sql"]
                )
                futures.append((future, item))
            
            for future, item in futures:
                try:
                    is_correct, error = future.result()
                    if is_correct:
                        results["correct"] += 1
                    else:
                        results["incorrect"] += 1
                        if error:
                            results["errors"].append({
                                "db_name": item["db_name"],
                                "error": error
                            })
                except Exception as e:
                    results["incorrect"] += 1
                    results["errors"].append({
                        "db_name": item.get("db_name", "unknown"),
                        "error": str(e)
                    })
        
        results["accuracy"] = results["correct"] / results["total"] if results["total"] > 0 else 0.0
        
        return results


def load_data(
    predicted_sql_path: str,
    ground_truth_path: str,
    data_mode: str = "dev",
    diff_json_path: Optional[str] = None
) -> List[Dict]:
    """
    加载评估数据
    
    Args:
        predicted_sql_path: 预测SQL文件路径
        ground_truth_path: 标准答案路径
        data_mode: 数据模式（dev/test）
        diff_json_path: diff json文件路径
        
    Returns:
        数据列表
    """
    # 加载标准答案
    gt_file = os.path.join(ground_truth_path, f"{data_mode}.json")
    with open(gt_file, 'r', encoding='utf-8') as f:
        ground_truth = json.load(f)
    
    # 创建问题ID到标准答案的映射
    gt_map = {item["question_id"]: item for item in ground_truth}
    
    # 加载预测SQL
    data = []
    for question_id, gt_item in gt_map.items():
        predicted_file = os.path.join(predicted_sql_path, f"{question_id}.sql")
        
        if not os.path.exists(predicted_file):
            continue
        
        with open(predicted_file, 'r', encoding='utf-8') as f:
            predicted_sql = f.read().strip()
        
        data.append({
            "question_id": question_id,
            "db_name": gt_item["db_id"],
            "predicted_sql": predicted_sql,
            "ground_truth_sql": gt_item["SQL"]
        })
    
    return data


def main():
    parser = argparse.ArgumentParser(description="BIRD Evaluation for PostgreSQL - EX Metric")
    parser.add_argument("--db_config", type=str, required=True, help="Database config JSON file")
    parser.add_argument("--predicted_sql_path", type=str, required=True, help="Path to predicted SQL files")
    parser.add_argument("--ground_truth_path", type=str, required=True, help="Path to ground truth files")
    parser.add_argument("--data_mode", type=str, default="dev", help="Data mode: dev or test")
    parser.add_argument("--num_cpus", type=int, default=16, help="Number of CPUs for parallel processing")
    parser.add_argument("--meta_time_out", type=float, default=30.0, help="Metadata query timeout in seconds")
    parser.add_argument("--output_path", type=str, default="./evaluation_results.json", help="Output results path")
    
    args = parser.parse_args()
    
    # 加载数据库配置
    with open(args.db_config, 'r', encoding='utf-8') as f:
        db_config = json.load(f)
    
    # 初始化评估器
    evaluator = PostgreSQLEvaluator(
        db_config=db_config,
        num_cpus=args.num_cpus,
        meta_time_out=args.meta_time_out
    )
    
    # 加载数据
    print("Loading data...")
    data = load_data(
        predicted_sql_path=args.predicted_sql_path,
        ground_truth_path=args.ground_truth_path,
        data_mode=args.data_mode
    )
    
    print(f"Loaded {len(data)} items for evaluation")
    
    # 执行评估
    print("Starting evaluation...")
    start_time = time.time()
    results = evaluator.evaluate_batch(data)
    end_time = time.time()
    
    results["evaluation_time"] = end_time - start_time
    
    # 保存结果
    with open(args.output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # 打印结果
    print("\n" + "="*50)
    print("Evaluation Results (EX Metric)")
    print("="*50)
    print(f"Total: {results['total']}")
    print(f"Correct: {results['correct']}")
    print(f"Incorrect: {results['incorrect']}")
    print(f"Accuracy (EX): {results['accuracy']:.4f} ({results['accuracy']*100:.2f}%)")
    print(f"Evaluation Time: {results['evaluation_time']:.2f} seconds")
    print("="*50)


if __name__ == "__main__":
    main()

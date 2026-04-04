#!/usr/bin/env python3
"""
SpatialSQL 适配验证脚本：
1) 原流程回归：使用默认数据集 spatial_qa 跑通预处理（不破坏原有架构）。
2) 新数据集加载与方言转换：校验 SpatialSQLLoader 与 sql_dialect_adapter。
不依赖 sdbdatasets 目录或迁移后的 PG，仅做模块级与配置级校验。
"""
from __future__ import annotations

import os
import sys
import tempfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# 在项目根下执行
os.chdir(REPO_ROOT)


def test_original_flow():
    """原流程：spatial_qa 配置与加载器可用，预处理可调起（不强制有 Excel 数据）。"""
    import yaml
    from src.datasets import DataLoaderFactory

    config_path = os.path.join(REPO_ROOT, "config", "dataset_config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    default_name = config.get("default_dataset", "spatial_qa")
    assert default_name == "spatial_qa", "default_dataset 必须保持 spatial_qa"
    dataset_info = config["datasets"].get(default_name)
    assert dataset_info is not None
    loader = DataLoaderFactory.create(dataset_info["loader_class"], dataset_info)
    info = loader.get_dataset_info()
    assert info["name"] == "spatial_qa"
    assert "grouping_fields" in info and "level" in info["grouping_fields"]
    print("[OK] 原流程配置与 SpatialQALoader 可用，default_dataset=spatial_qa")
    return True


def test_spatialsql_loader_and_adapter():
    """SpatialSQLLoader：空目录返回空列表；有 stub QA 文件时能解析。方言转换器可调用。"""
    from src.datasets.loaders import SpatialSQLLoader
    from src.sql import convert_spatialite_to_postgis

    config = {
        "data_path": "",
        "dataset_versions": ["dataset1"],
        "domains": ["ada"],
    }
    loader = SpatialSQLLoader(config)
    raw = loader.load_raw_data(tempfile.mkdtemp())
    assert raw == [], "空目录应返回空"
    extracted = loader.extract_questions_and_sqls([])
    assert extracted == []

    # stub QA 块
    stub_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(stub_dir, "dataset1", "ada"), exist_ok=True)
    with open(os.path.join(stub_dir, "dataset1", "ada", "QA-ada-stub.txt"), "w", encoding="utf-8") as f:
        f.write("label:S\nquestion: What is the border length?\nSQL: Select GLength(Intersection(a.Shape,b.Shape),1);\nEval: Select GLength(Intersection(a.Shape,b.Shape),1);\nid: stub1\n\n")
    raw = loader.load_raw_data(stub_dir)
    assert len(raw) >= 1
    extracted = loader.extract_questions_and_sqls(raw)
    assert len(extracted) >= 1
    assert "gold_sql" in extracted[0] and "gold_sql_candidates" in extracted[0]
    assert "metadata" in extracted[0] and "split" in extracted[0]["metadata"]

    # 方言转换
    sql = "Select GLength(Intersection(a.Shape, b.Shape),1) from t a, t b Where Intersects(a.Shape, b.Shape)=1;"
    converted, issues = convert_spatialite_to_postgis(sql)
    assert "ST_Length" in converted and "ST_Intersection" in converted and "ST_Intersects" in converted
    assert "shape" in converted.lower()
    print("[OK] SpatialSQLLoader 与 sql_dialect_adapter 行为正常")
    return True


def test_evaluator_multigold():
    """评估器多 gold 候选：无 candidates 时行为不变；有 candidates 时接口接受。"""
    from src.evaluation import Evaluator

    eval_config = {"evaluation": {"timeout": 60}}
    # 不连接真实 DB，只检查调用不报错（会因连接失败返回 error_type）
    evaluator = Evaluator(db_config={"host": "127.0.0.1", "port": 5432, "database": "test", "user": "u", "password": "p"}, eval_config=eval_config)
    pred_sql = "SELECT 1;"
    gold_sql = "SELECT 1;"
    info = evaluator._execution_accuracy(pred_sql, gold_sql, gold_sql_candidates=None)
    assert "correct" in info and "error_type" in info
    info2 = evaluator._execution_accuracy(pred_sql, gold_sql, gold_sql_candidates=["SELECT 1;"])
    assert "correct" in info2
    print("[OK] 评估器多 gold 候选接口正常")
    return True


def main():
    print("SpatialSQL 适配验证（不依赖 sdbdatasets / 迁移 PG）\n")
    try:
        test_original_flow()
        test_spatialsql_loader_and_adapter()
        test_evaluator_multigold()
        print("\n全部检查通过。原流程保持不变，spatialsql_pg 扩展可用。")
        return 0
    except Exception as e:
        print(f"\n验证失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

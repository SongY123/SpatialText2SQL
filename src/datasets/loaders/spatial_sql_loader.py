"""SpatialSQL 数据集加载器 - 解析 QA-*.txt（SpatialSQL 仓库格式）"""
import os
import re
from typing import List, Dict, Any, Optional

from src.datasets.base import BaseDataLoader


# dataset1/dataset2 下各 domain 的目录名
SPATIALSQL_DOMAINS = ["ada", "edu", "tourism", "traffic"]
SPATIALSQL_VERSIONS = ["dataset1", "dataset2"]


def _parse_qa_txt_block(block: str) -> Optional[Dict[str, Any]]:
    """
    解析一个 QA 块（key: value 行，空行分隔块）。
    返回包含 label, question, questionCHI, SQL, Eval, id 等字段的字典。
    """
    lines = [ln.strip() for ln in block.strip().splitlines() if ln.strip()]
    if not lines:
        return None
    record = {}
    for line in lines:
        idx = line.find(":")
        if idx <= 0:
            continue
        key = line[:idx].strip()
        value = line[idx + 1 :].strip()
        record[key] = value
    if "id" not in record:
        return None
    return record


def _split_sql_candidates(sql_field: str) -> List[str]:
    """将 SQL 或 Eval 字段按 %%% 拆成多条 SQL，并做空白规范化。"""
    if not sql_field or not sql_field.strip():
        return []
    parts = [p.strip() for p in sql_field.split("%%%") if p.strip()]
    return parts


class SpatialSQLLoader(BaseDataLoader):
    """
    SpatialSQL 数据集加载器：从 sdbdatasets 目录下的 QA-*.txt 加载数据。
    输出统一格式：question, gold_sql, gold_sql_candidates, metadata（dataset_version, domain, label, source_id）。
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get("data_path", "sdbdatasets")
        self.use_chinese_question = config.get("use_chinese_question", False)
        self.dataset_versions = config.get("dataset_versions", SPATIALSQL_VERSIONS)
        self.domains = config.get("domains", SPATIALSQL_DOMAINS)

    def load_raw_data(self, data_path: str) -> List[Dict]:
        """
        扫描 data_path 下的 dataset1/dataset2 及各 domain，读取所有 QA-*.txt，解析为原始记录列表。
        """
        root = data_path or self.data_path
        all_data = []
        for version in self.dataset_versions:
            version_dir = os.path.join(root, version)
            if not os.path.isdir(version_dir):
                print(f"警告: 目录不存在 {version_dir}")
                continue
            for domain in self.domains:
                domain_dir = os.path.join(version_dir, domain)
                if not os.path.isdir(domain_dir):
                    print(f"警告: 目录不存在 {domain_dir}")
                    continue
                for fname in os.listdir(domain_dir):
                    if not fname.startswith("QA-") or not fname.endswith(".txt"):
                        continue
                    filepath = os.path.join(domain_dir, fname)
                    if not os.path.isfile(filepath):
                        continue
                    try:
                        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                            content = f.read()
                        blocks = re.split(r"\n\s*\n", content)
                        for block in blocks:
                            record = _parse_qa_txt_block(block)
                            if not record:
                                continue
                            record["_dataset_version"] = version
                            record["_domain"] = domain
                            all_data.append(record)
                    except Exception as e:
                        print(f"错误: 读取 {filepath} 失败 - {e}")
                        continue
        if all_data:
            print(f"成功加载 SpatialSQL 原始记录: {len(all_data)} 条")
        return all_data

    def extract_questions_and_sqls(self, raw_data: List[Dict]) -> List[Dict]:
        """
        转为统一格式：id, question, gold_sql, gold_sql_candidates, metadata。
        question 默认英文，可配置为中文；gold_sql 取 SQL 字段第一条；gold_sql_candidates 来自 Eval 拆分。
        """
        question_key = "questionCHI" if self.use_chinese_question else "question"
        extracted = []
        for idx, row in enumerate(raw_data, start=1):
            question = (row.get(question_key) or "").strip()
            sql_field = (row.get("SQL") or "").strip()
            eval_field = (row.get("Eval") or "").strip()
            if not question:
                continue
            sql_list = _split_sql_candidates(sql_field)
            eval_list = _split_sql_candidates(eval_field)
            gold_sql = sql_list[0] if sql_list else ""
            if not gold_sql and eval_list:
                gold_sql = eval_list[0]
            gold_sql_candidates = list(eval_list) if eval_list else (list(sql_list) if sql_list else [])
            if not gold_sql and not gold_sql_candidates:
                continue
            if not gold_sql and gold_sql_candidates:
                gold_sql = gold_sql_candidates[0]
            dataset_version = row.get("_dataset_version", "")
            domain = row.get("_domain", "")
            label = (row.get("label") or "").strip()
            source_id = (row.get("id") or "").strip()
            split_value = f"{dataset_version}_{domain}" if dataset_version and domain else "unknown"
            item = {
                "id": idx,
                "question": question,
                "source_sql": gold_sql,
                "source_sql_candidates": list(gold_sql_candidates),
                "gold_sql": gold_sql,
                "gold_sql_candidates": gold_sql_candidates,
                "source_backend": "sqlite",
                "target_backend": "postgres",
                "source_split": split_value,
                "target_table_prefix": f"{split_value}_" if split_value != "unknown" else "",
                "repair_status": "raw",
                "repair_source": "source",
                "metadata": {
                    "dataset_version": dataset_version,
                    "domain": domain,
                    "label": label,
                    "source_id": source_id,
                    "split": split_value,
                },
            }
            extracted.append(item)
        return extracted

    def get_dataset_info(self) -> Dict:
        """返回 SpatialSQL 数据集元信息；分组字段为 split（dataset_version_domain）。"""
        splits = []
        for v in self.dataset_versions:
            for d in self.domains:
                splits.append(f"{v}_{d}")
        return {
            "name": "spatialsql_pg",
            "grouping_fields": ["split"],
            "grouping_values": {
                "split": splits,
            },
        }

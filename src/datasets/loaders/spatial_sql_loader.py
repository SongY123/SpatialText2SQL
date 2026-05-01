"""SpatialSQL dataset loader for QA-*.txt files from the upstream repository."""
import os
import re
from typing import List, Dict, Any, Optional

from src.datasets.base import BaseDataLoader


# Domain directory names under dataset1/dataset2.
SPATIALSQL_DOMAINS = ["ada", "edu", "tourism", "traffic"]
SPATIALSQL_VERSIONS = ["dataset1", "dataset2"]


def _parse_qa_txt_block(block: str) -> Optional[Dict[str, Any]]:
    """
    Parse one QA block separated by blank lines.

    Returns a dictionary containing fields such as label, question,
    questionCHI, SQL, Eval, and id.
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
    """Split SQL or Eval fields on %%% and normalize whitespace."""
    if not sql_field or not sql_field.strip():
        return []
    parts = [p.strip() for p in sql_field.split("%%%") if p.strip()]
    return parts


class SpatialSQLLoader(BaseDataLoader):
    """
    Load SpatialSQL data from QA-*.txt files under sdbdatasets.

    The loader returns normalized records with question, gold_sql,
    gold_sql_candidates, and metadata fields.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get("data_path", "sdbdatasets")
        self.use_chinese_question = config.get("use_chinese_question", False)
        self.dataset_versions = config.get("dataset_versions", SPATIALSQL_VERSIONS)
        self.domains = config.get("domains", SPATIALSQL_DOMAINS)

    def load_raw_data(self, data_path: str) -> List[Dict]:
        """Read QA-*.txt files under dataset1/dataset2 and return raw records."""
        root = data_path or self.data_path
        all_data = []
        for version in self.dataset_versions:
            version_dir = os.path.join(root, version)
            if not os.path.isdir(version_dir):
                print(f"Warning: directory does not exist: {version_dir}")
                continue
            for domain in self.domains:
                domain_dir = os.path.join(version_dir, domain)
                if not os.path.isdir(domain_dir):
                    print(f"Warning: directory does not exist: {domain_dir}")
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
                        print(f"Error: failed to read {filepath} - {e}")
                        continue
        if all_data:
            print(f"Loaded {len(all_data)} raw SpatialSQL records")
        return all_data

    def extract_questions_and_sqls(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Convert raw records into the normalized benchmark format.

        The default question field is English. gold_sql uses the first SQL
        candidate, and gold_sql_candidates are derived from Eval when present.
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
        """Return SpatialSQL dataset metadata grouped by split."""
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

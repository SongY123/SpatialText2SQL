"""数据加载统一入口 - 工厂模式。"""
import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.datasets.path_utils import (
    get_expected_preprocessed_files,
    get_group_samples_file,
    get_preprocessed_output_dir,
    get_schema_cache_dir,
    get_schema_cache_file,
    get_single_dataset_samples_file,
)
from src.datasets.loaders.spatial_qa_loader import SpatialQALoader
from src.datasets.loaders.floodsql_loader import FloodSQLLoader
from src.datasets.loaders.spatial_sql_loader import SpatialSQLLoader
from src.sql.schema_extractor import SchemaExtractor
from src.sql.sql_dialect_adapter import (
    classify_spatialsql_failure,
    convert_duckdb_to_postgis,
    convert_spatialite_to_postgis,
)


class DataLoaderFactory:
    """数据加载器工厂类"""
    
    # 注册数据加载器映射（仅扩展，不修改原有键）
    _loaders = {
        'SpatialQALoader': SpatialQALoader,
        'SpatialSQLLoader': SpatialSQLLoader,
        'FloodSQLLoader': FloodSQLLoader,
    }
    
    @classmethod
    def create(cls, dataset_type: str, config: Dict[str, Any]):
        """
        创建数据加载器实例
        
        Args:
            dataset_type: 数据集类型对应的加载器类名
            config: 数据集配置
            
        Returns:
            数据加载器实例
        """
        loader_class = cls._loaders.get(dataset_type)
        if not loader_class:
            raise ValueError(f"未知的数据加载器类型: {dataset_type}")
        return loader_class(config)
    
    @classmethod
    def register_loader(cls, name: str, loader_class):
        """
        注册新的数据加载器
        
        Args:
            name: 加载器名称
            loader_class: 加载器类
        """
        cls._loaders[name] = loader_class


class DataPreprocessor:
    """数据预处理器 - 统一的数据预处理流程"""
    
    def __init__(self, dataset_config_path: str, db_config_path: str):
        """
        初始化数据预处理器
        
        Args:
            dataset_config_path: 数据集配置文件路径
            db_config_path: 数据库配置文件路径
        """
        # 加载配置
        with open(dataset_config_path, 'r', encoding='utf-8') as f:
            self.dataset_config = yaml.safe_load(f)
        
        with open(db_config_path, 'r', encoding='utf-8') as f:
            self.db_config_full = yaml.safe_load(f)
            # 向后兼容：默认使用 database 字段
            self.db_config = self.db_config_full.get('database', {})
            # 多数据库配置
            self.databases = self.db_config_full.get('databases', {})
        
        # 默认使用单数据库配置的 schema_extractor（向后兼容）
        self.schema_extractor = SchemaExtractor(self.db_config)
    
    def preprocess(self, dataset_name: str = None):
        """
        执行数据预处理
        
        Args:
            dataset_name: 数据集名称，如果为None则使用默认数据集
        """
        if dataset_name is None:
            dataset_name = self.dataset_config.get('default_dataset', 'spatial_qa')
        
        print(f"\n{'='*60}")
        print(f"开始预处理数据集: {dataset_name}")
        print(f"{'='*60}\n")
        
        # 获取数据集配置
        dataset_info = self.dataset_config['datasets'].get(dataset_name)
        if not dataset_info:
            raise ValueError(f"未找到数据集配置: {dataset_name}")
        
        # 创建数据加载器
        loader_class_name = dataset_info['loader_class']
        data_loader = DataLoaderFactory.create(loader_class_name, dataset_info)
        
        # 1. 加载原始数据
        print("步骤 1/4: 加载原始数据...")
        raw_data = data_loader.load_raw_data(dataset_info['data_path'])
        print(f"成功加载 {len(raw_data)} 条原始数据\n")
        
        # 2. 提取问题和SQL
        print("步骤 2/4: 提取问题和SQL...")
        extracted_data = data_loader.extract_questions_and_sqls(raw_data)
        print(f"成功提取 {len(extracted_data)} 条有效数据\n")
        
        # 2.5. SpatialSQL 专用：SpatiaLite -> PostGIS 方言转换（仅扩展，不改动其他数据集）
        if dataset_info.get('use_sql_dialect_adapter') or dataset_name == 'spatialsql_pg':
            extracted_data = self._apply_sql_dialect_adapter(
                extracted_data,
                dataset_name,
                dataset_info,
            )
        
        # 3. 提取Schema（支持多数据库）
        print("步骤 3/4: 提取数据库Schema...")
        schema, schema_file = self._get_or_extract_schema(dataset_name, dataset_info)
        print(f"Schema长度: {len(schema)} 字符\n")
        
        # 4. 整合数据并保存
        print("步骤 4/4: 整合数据并保存...")
        self._save_preprocessed_data(
            extracted_data,
            schema,
            schema_file,
            dataset_name,
            data_loader,
        )
        
        print(f"\n{'='*60}")
        print(f"数据预处理完成!")
        print(f"{'='*60}\n")
    
    def _apply_sql_dialect_adapter(
        self,
        extracted_data: List[Dict],
        dataset_name: str,
        dataset_info: Dict[str, Any],
    ) -> List[Dict]:
        """对启用方言转换的数据集，将源 SQL 转为 PostgreSQL/PostGIS 并落盘报告。"""
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        dataset_output_dir = get_preprocessed_output_dir(preprocessing_config, dataset_name)
        os.makedirs(dataset_output_dir, exist_ok=True)
        unconverted_path = os.path.join(dataset_output_dir, 'unconverted_sqls.jsonl')
        unconverted_all = []
        split_issue_counter: Dict[str, Counter] = defaultdict(Counter)
        adapter_name = dataset_info.get('sql_dialect_adapter')
        if not adapter_name and dataset_name == 'spatialsql_pg':
            adapter_name = 'spatialite_to_postgis'

        if adapter_name == 'duckdb_to_postgis':
            converter = lambda sql, table_prefix=None: convert_duckdb_to_postgis(sql, table_prefix=table_prefix)
            classifier = lambda issues: 'duckdb_translation_gap' if issues else None
        else:
            converter = lambda sql, table_prefix=None: convert_spatialite_to_postgis(sql, table_prefix=table_prefix)
            classifier = lambda issues: classify_spatialsql_failure(issues=issues)

        for item in extracted_data:
            table_prefix = None
            metadata = item.get('metadata', {})
            if dataset_name == 'spatialsql_pg':
                split = metadata.get('split', '')
                if split:
                    table_prefix = f"{split}_"
            
            orig = item.get('gold_sql', '')
            converted, issues = converter(orig, table_prefix=table_prefix)
            item['gold_sql'] = converted
            item['source_backend'] = item.get('source_backend', 'sqlite')
            item['target_backend'] = item.get('target_backend', 'postgres')
            item['source_split'] = item.get('source_split') or metadata.get('split', '')
            item['target_table_prefix'] = item.get('target_table_prefix') or (table_prefix or '')
            item['repair_source'] = 'rule'
            item['repair_status'] = 'rule_validated' if not issues else 'rule_needs_review'
            if issues:
                split_key = (
                    metadata.get('split')
                    or metadata.get('level')
                    or metadata.get('family')
                    or 'unknown'
                )
                for issue in issues:
                    split_issue_counter[split_key][classifier([issue]) or 'sql_rule_gap'] += 1
                unconverted_all.append({
                    'source_id': metadata.get('source_id', item.get('id')),
                    'group': split_key,
                    'field': 'gold_sql',
                    'original': orig[:500],
                    'converted': converted[:500],
                    'issues': issues,
                    'classification': classifier(issues),
                })
            candidates = item.get('gold_sql_candidates', [])
            if candidates:
                new_candidates = []
                for i, c in enumerate(candidates):
                    c_conv, c_issues = converter(c, table_prefix=table_prefix)
                    new_candidates.append(c_conv)
                    if c_issues:
                        split_key = (
                            metadata.get('split')
                            or metadata.get('level')
                            or metadata.get('family')
                            or 'unknown'
                        )
                        for issue in c_issues:
                            split_issue_counter[split_key][classifier([issue]) or 'sql_rule_gap'] += 1
                        unconverted_all.append({
                            'source_id': metadata.get('source_id', item.get('id')),
                            'group': split_key,
                            'field': f'gold_sql_candidates[{i}]',
                            'original': c[:500],
                            'converted': c_conv[:500],
                            'issues': c_issues,
                            'classification': classifier(c_issues),
                        })
                item['gold_sql_candidates'] = new_candidates
        report_path = os.path.join(dataset_output_dir, 'sql_conversion_report.json')
        report = {
            'dataset': dataset_name,
            'adapter': adapter_name,
            'total_items': len(extracted_data),
            'issue_count': len(unconverted_all),
            'issues_by_group': {
                group: dict(counter)
                for group, counter in sorted(split_issue_counter.items())
            },
            'details': unconverted_all,
        }
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        adapter_label = (
            "DuckDB->PostGIS"
            if adapter_name == 'duckdb_to_postgis'
            else "SpatiaLite->PostGIS"
        )
        if unconverted_all:
            with open(unconverted_path, 'w', encoding='utf-8') as f:
                for rec in unconverted_all:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            print(
                f"{adapter_label} 转换完成；未覆盖/警告 "
                f"{len(unconverted_all)} 条已写入 {unconverted_path}\n"
            )
        else:
            print(f"{adapter_label} 转换完成，无未覆盖项。\n")
        return extracted_data
    
    def _get_or_extract_schema(self, dataset_name: str, dataset_info: Dict) -> Tuple[str, str]:
        """
        获取或提取Schema（支持缓存和多数据库）
        
        Args:
            dataset_name: 数据集名称
            dataset_info: 数据集配置信息
        """
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        schema_cache_dir = get_schema_cache_dir(preprocessing_config)
        
        # 根据数据集配置选择数据库
        db_name = dataset_info.get('database', 'default')
        if db_name != 'default' and db_name in self.databases:
            # 使用数据集指定的数据库
            db_config = self.databases[db_name]
            schema_key = db_name
            print(f"使用数据库: {db_name} ({db_config['database']})")
        else:
            # 使用默认数据库（向后兼容）
            db_config = self.db_config
            schema_key = self.db_config.get('database', 'default')
            print(f"使用默认数据库: {db_config.get('database', 'postgres')}")

        schema_file = get_schema_cache_file(schema_cache_dir, schema_key)
        
        # 创建对应数据库的 schema_extractor
        schema_extractor = SchemaExtractor(db_config)
        
        # 尝试从缓存加载
        cached_schema = schema_extractor.load_schema_from_file(schema_file)
        if cached_schema:
            print(f"从缓存加载Schema: {schema_file}")
            return cached_schema, schema_file
        
        # 从数据库提取
        print("从数据库提取Schema...")
        try:
            schema_extractor.connect()
            schema = schema_extractor.extract_schema()
            schema_extractor.close()
            
            # 保存到缓存
            schema_extractor.save_schema_to_file(schema, schema_file)
            return schema, schema_file
        except Exception as e:
            print(f"Schema提取失败: {str(e)}")
            return "-- Schema提取失败", schema_file
    
    def _save_preprocessed_data(
        self,
        extracted_data: List[Dict],
        schema: str,
        schema_file: Optional[str],
        dataset_name: str,
        data_loader,
    ):
        """保存预处理后的数据"""
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        dataset_output_dir = get_preprocessed_output_dir(preprocessing_config, dataset_name)
        os.makedirs(dataset_output_dir, exist_ok=True)
        
        # 获取数据集元信息
        dataset_info = data_loader.get_dataset_info()
        grouping_fields = dataset_info.get('grouping_fields', [])
        
        if grouping_fields:
            # 有分组字段，按分组保存
            self._save_grouped_data(
                extracted_data,
                schema,
                schema_file,
                dataset_output_dir,
                grouping_fields,
                dataset_name,
            )
        else:
            # 无分组，保存为单个文件
            self._save_single_file(
                extracted_data,
                schema,
                schema_file,
                dataset_output_dir,
                dataset_name,
            )
    
    def _save_grouped_data(
        self,
        extracted_data: List[Dict],
        schema: str,
        schema_file: Optional[str],
        output_dir: str,
        grouping_fields: List[str],
        dataset_name: str,
    ):
        """按分组保存数据"""
        # 假设只有一个分组字段（如level）
        group_field = grouping_fields[0]
        
        # 按分组字段分组
        grouped_data = {}
        for item in extracted_data:
            group_value = item['metadata'].get(group_field)
            if group_value not in grouped_data:
                grouped_data[group_value] = []
            grouped_data[group_value].append(item)
        
        # 为每个分组保存文件
        for group_value, items in grouped_data.items():
            # 为每条数据添加schema
            for item in items:
                item.pop('schema', None)
                if schema_file:
                    item['schema_file'] = schema_file
                else:
                    item['schema'] = schema
                item['dataset'] = dataset_name
            
            output_file = get_group_samples_file(
                dataset_name,
                output_dir,
                group_field,
                group_value,
            )
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"保存 {group_field}={group_value}: {len(items)} 条数据 -> {output_file}")
    
    def _save_single_file(
        self,
        extracted_data: List[Dict],
        schema: str,
        schema_file: Optional[str],
        output_dir: str,
        dataset_name: str,
    ):
        """保存为单个文件"""
        for item in extracted_data:
            item.pop('schema', None)
            if schema_file:
                item['schema_file'] = schema_file
            else:
                item['schema'] = schema
            item['dataset'] = dataset_name
        
        output_file = get_single_dataset_samples_file(output_dir)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=2)
        print(f"保存数据: {len(extracted_data)} 条 -> {output_file}")

"""数据加载统一入口 - 工厂模式"""
import yaml
import json
import os
import sys
from typing import Dict, List, Any

# 添加src目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loaders.spatial_qa_loader import SpatialQALoader
from loaders.spatial_sql_loader import SpatialSQLLoader
from schema_extractor import SchemaExtractor


class DataLoaderFactory:
    """数据加载器工厂类"""
    
    # 注册数据加载器映射（仅扩展，不修改原有键）
    _loaders = {
        'SpatialQALoader': SpatialQALoader,
        'SpatialSQLLoader': SpatialSQLLoader,
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
            extracted_data = self._apply_sql_dialect_adapter(extracted_data, dataset_name)
        
        # 3. 提取Schema（支持多数据库）
        print("步骤 3/4: 提取数据库Schema...")
        schema = self._get_or_extract_schema(dataset_name, dataset_info)
        print(f"Schema长度: {len(schema)} 字符\n")
        
        # 4. 整合数据并保存
        print("步骤 4/4: 整合数据并保存...")
        self._save_preprocessed_data(extracted_data, schema, dataset_name, data_loader)
        
        print(f"\n{'='*60}")
        print(f"数据预处理完成!")
        print(f"{'='*60}\n")
    
    def _apply_sql_dialect_adapter(self, extracted_data: List[Dict], dataset_name: str) -> List[Dict]:
        """对 spatialsql_pg 等启用方言转换的数据集，将 SpatiaLite SQL 转为 PostGIS，并落盘未覆盖清单。"""
        try:
            from sql_dialect_adapter import convert_spatialite_to_postgis
        except ImportError:
            from src.sql_dialect_adapter import convert_spatialite_to_postgis
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        output_dir = preprocessing_config.get('output_dir', 'data/preprocessed')
        dataset_output_dir = os.path.join(output_dir, dataset_name)
        os.makedirs(dataset_output_dir, exist_ok=True)
        unconverted_path = os.path.join(dataset_output_dir, 'unconverted_sqls.jsonl')
        unconverted_all = []
        for item in extracted_data:
            # 获取表名前缀（对于 spatialsql_pg，使用 split 如 dataset1_ada）
            table_prefix = None
            metadata = item.get('metadata', {})
            if dataset_name == 'spatialsql_pg':
                split = metadata.get('split', '')
                if split:
                    table_prefix = f"{split}_"
            
            # 主 gold_sql
            orig = item.get('gold_sql', '')
            converted, issues = convert_spatialite_to_postgis(orig, table_prefix=table_prefix)
            item['gold_sql'] = converted
            if issues:
                unconverted_all.append({
                    'source_id': metadata.get('source_id', item.get('id')),
                    'field': 'gold_sql',
                    'original': orig[:500],
                    'converted': converted[:500],
                    'issues': issues,
                })
            # gold_sql_candidates
            candidates = item.get('gold_sql_candidates', [])
            if candidates:
                new_candidates = []
                for i, c in enumerate(candidates):
                    c_conv, c_issues = convert_spatialite_to_postgis(c, table_prefix=table_prefix)
                    new_candidates.append(c_conv)
                    if c_issues:
                        unconverted_all.append({
                            'source_id': metadata.get('source_id', item.get('id')),
                            'field': f'gold_sql_candidates[{i}]',
                            'original': c[:500],
                            'converted': c_conv[:500],
                            'issues': c_issues,
                        })
                item['gold_sql_candidates'] = new_candidates
        if unconverted_all:
            with open(unconverted_path, 'w', encoding='utf-8') as f:
                for rec in unconverted_all:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            print(f"SpatiaLite->PostGIS 转换完成；未覆盖/警告 {len(unconverted_all)} 条已写入 {unconverted_path}\n")
        else:
            print("SpatiaLite->PostGIS 转换完成，无未覆盖项。\n")
        return extracted_data
    
    def _get_or_extract_schema(self, dataset_name: str, dataset_info: Dict) -> str:
        """
        获取或提取Schema（支持缓存和多数据库）
        
        Args:
            dataset_name: 数据集名称
            dataset_info: 数据集配置信息
        """
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        schema_cache_dir = preprocessing_config.get('schema_cache_dir', 'data/schemas')
        
        # 根据数据集配置选择数据库
        db_name = dataset_info.get('database', 'default')
        if db_name != 'default' and db_name in self.databases:
            # 使用数据集指定的数据库
            db_config = self.databases[db_name]
            schema_file = os.path.join(schema_cache_dir, f'database_schema_{db_name}.txt')
            print(f"使用数据库: {db_name} ({db_config['database']})")
        else:
            # 使用默认数据库（向后兼容）
            db_config = self.db_config
            schema_file = os.path.join(schema_cache_dir, 'database_schema.txt')
            print(f"使用默认数据库: {db_config.get('database', 'postgres')}")
        
        # 创建对应数据库的 schema_extractor
        schema_extractor = SchemaExtractor(db_config)
        
        # 尝试从缓存加载
        cached_schema = schema_extractor.load_schema_from_file(schema_file)
        if cached_schema:
            print(f"从缓存加载Schema: {schema_file}")
            return cached_schema
        
        # 从数据库提取
        print("从数据库提取Schema...")
        try:
            schema_extractor.connect()
            schema = schema_extractor.extract_schema()
            schema_extractor.close()
            
            # 保存到缓存
            schema_extractor.save_schema_to_file(schema, schema_file)
            return schema
        except Exception as e:
            print(f"Schema提取失败: {str(e)}")
            return "-- Schema提取失败"
    
    def _save_preprocessed_data(self, extracted_data: List[Dict], schema: str, 
                                dataset_name: str, data_loader):
        """保存预处理后的数据"""
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        output_dir = preprocessing_config.get('output_dir', 'data/preprocessed')
        dataset_output_dir = os.path.join(output_dir, dataset_name)
        os.makedirs(dataset_output_dir, exist_ok=True)
        
        # 获取数据集元信息
        dataset_info = data_loader.get_dataset_info()
        grouping_fields = dataset_info.get('grouping_fields', [])
        
        if grouping_fields:
            # 有分组字段，按分组保存
            self._save_grouped_data(extracted_data, schema, dataset_output_dir, 
                                   grouping_fields, dataset_name)
        else:
            # 无分组，保存为单个文件
            self._save_single_file(extracted_data, schema, dataset_output_dir, 
                                  dataset_name)
    
    def _save_grouped_data(self, extracted_data: List[Dict], schema: str, 
                          output_dir: str, grouping_fields: List[str], dataset_name: str):
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
                item['schema'] = schema
                item['dataset'] = dataset_name
            
            output_file = os.path.join(output_dir, f"{group_field}{group_value}_with_schema.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"保存 {group_field}={group_value}: {len(items)} 条数据 -> {output_file}")
    
    def _save_single_file(self, extracted_data: List[Dict], schema: str, 
                         output_dir: str, dataset_name: str):
        """保存为单个文件"""
        for item in extracted_data:
            item['schema'] = schema
            item['dataset'] = dataset_name
        
        output_file = os.path.join(output_dir, f"{dataset_name}_with_schema.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=2)
        print(f"保存数据: {len(extracted_data)} 条 -> {output_file}")

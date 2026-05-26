"""Schema提取模块 - 从PostgreSQL数据库提取完整Schema"""
import os
from typing import Any, Dict, List, Optional

import psycopg2

from src.datasets.db_routing import apply_search_path, resolve_schema_name


class SchemaExtractor:
    """PostgreSQL Schema提取器"""
    
    def __init__(self, db_config: Dict):
        """
        初始化Schema提取器
        
        Args:
            db_config: 数据库配置信息
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                connect_timeout=self.db_config.get('timeout', {}).get('connection_timeout', 10)
            )
            self.cursor = self.conn.cursor()
            apply_search_path(self.cursor, self.db_config)
            print(f"成功连接到数据库: {self.db_config['database']}")
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def extract_schema(self) -> str:
        """
        提取完整的数据库Schema
        
        Returns:
            Schema字符串（包含所有表结构、字段、PostGIS扩展等）
        """
        if not self.conn:
            self.connect()

        schema_name = resolve_schema_name(self.db_config)
        schema_parts = []

        # 1. 获取所有用户表
        self.cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """, (schema_name,))
        tables = [row[0] for row in self.cursor.fetchall()]
        
        print(f"发现 {len(tables)} 个表")
        
        # 2. 对每个表提取详细信息
        for table in tables:
            schema_parts.append(self._extract_table_schema(table))
        
        # 3. 获取PostGIS扩展信息
        postgis_info = self._extract_postgis_info()
        if postgis_info:
            schema_parts.insert(0, postgis_info)
        
        return "\n\n".join(schema_parts)
    
    def _extract_table_schema(self, table_name: str) -> str:
        """提取单个表的Schema"""
        schema_name = resolve_schema_name(self.db_config)
        # 获取表的列信息
        self.cursor.execute(f"""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema_name, table_name))
        
        columns = self.cursor.fetchall()
        
        column_definitions: List[str] = []
        
        for col_name, data_type, max_length, is_nullable, default in columns:
            col_def = f"    {col_name} {data_type}"
            if max_length:
                col_def += f"({max_length})"
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            if default:
                col_def += f" DEFAULT {default}"
            column_definitions.append(col_def)

        pk_cols = self._fetch_primary_key_columns(schema_name, table_name)
        if pk_cols:
            column_definitions.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

        for fk in self._fetch_foreign_key_constraints(schema_name, table_name):
            local_columns = ", ".join(fk["local_columns"])
            ref_columns = ", ".join(fk["referenced_columns"])
            column_definitions.append(
                f"    FOREIGN KEY ({local_columns}) REFERENCES {fk['referenced_table']} ({ref_columns})"
            )

        create_statement = f"CREATE TABLE {table_name} (\n"
        create_statement += ",\n".join(column_definitions)
        create_statement += "\n);"
        return create_statement

    def _fetch_primary_key_columns(self, schema_name: str, table_name: str) -> List[str]:
        self.cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
            """,
            (schema_name, table_name),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def _fetch_foreign_key_constraints(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT
                kcu.constraint_name,
                kcu.column_name,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            JOIN information_schema.referential_constraints rc
              ON tc.constraint_name = rc.constraint_name
             AND tc.table_schema = rc.constraint_schema
            JOIN information_schema.key_column_usage ccu
              ON rc.unique_constraint_name = ccu.constraint_name
             AND rc.unique_constraint_schema = ccu.constraint_schema
             AND ccu.ordinal_position = kcu.position_in_unique_constraint
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY kcu.constraint_name, kcu.ordinal_position;
            """,
            (schema_name, table_name),
        )
        grouped: Dict[str, Dict[str, Any]] = {}
        for constraint_name, column_name, referenced_table, referenced_column, _ordinal in self.cursor.fetchall():
            constraint = grouped.setdefault(
                constraint_name,
                {
                    "referenced_table": referenced_table,
                    "local_columns": [],
                    "referenced_columns": [],
                },
            )
            constraint["local_columns"].append(column_name)
            constraint["referenced_columns"].append(referenced_column)
        return list(grouped.values())
    
    def _extract_postgis_info(self) -> Optional[str]:
        """提取PostGIS扩展信息"""
        try:
            self.cursor.execute("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname = 'postgis';
            """)
            result = self.cursor.fetchone()
            if result:
                return f"-- PostGIS Extension: {result[0]} version {result[1]}"
        except:
            pass
        return None
    
    def save_schema_to_file(self, schema: str, output_path: str):
        """
        保存Schema到文件
        
        Args:
            schema: Schema字符串
            output_path: 输出文件路径
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(schema)
        print(f"Schema已保存到: {output_path}")
    
    def load_schema_from_file(self, schema_path: str) -> Optional[str]:
        """
        从文件加载Schema
        
        Args:
            schema_path: Schema文件路径
            
        Returns:
            Schema字符串，如果文件不存在则返回None
        """
        if os.path.exists(schema_path):
            with open(schema_path, 'r', encoding='utf-8') as f:
                return f.read()
        return None

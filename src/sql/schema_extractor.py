"""Schema提取模块 - 从PostgreSQL数据库提取完整Schema"""
import os
from typing import Dict, Optional

import psycopg2


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
        
        schema_parts = []
        
        # 1. 获取所有用户表
        self.cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
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
        # 获取表的列信息
        self.cursor.execute(f"""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = self.cursor.fetchall()
        
        # 构建CREATE TABLE语句
        create_statement = f"CREATE TABLE {table_name} (\n"
        column_definitions = []
        
        for col_name, data_type, max_length, is_nullable, default in columns:
            col_def = f"    {col_name} {data_type}"
            if max_length:
                col_def += f"({max_length})"
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            if default:
                col_def += f" DEFAULT {default}"
            column_definitions.append(col_def)
        
        create_statement += ",\n".join(column_definitions)
        create_statement += "\n);"
        
        # 获取主键信息
        self.cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
            AND tc.table_name = %s
            AND tc.constraint_type = 'PRIMARY KEY';
        """, (table_name,))
        
        pk_cols = [row[0] for row in self.cursor.fetchall()]
        if pk_cols:
            create_statement += f"\n-- Primary Key: {', '.join(pk_cols)}"
        
        return create_statement
    
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

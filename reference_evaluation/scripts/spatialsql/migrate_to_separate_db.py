#!/usr/bin/env python3
"""
将 SpatialSQL 的 SQLite/SpatiaLite 数据库迁移到独立的 spatial_sql PostgreSQL 数据库。
所有表直接放在 public schema 下，使用表名前缀区分不同的 dataset 和 domain。
例如: dataset1_ada_provinces, dataset1_ada_cities 等
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

# 项目根目录
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

VERSIONS = ["dataset1", "dataset2"]
DOMAINS = ["ada", "edu", "tourism", "traffic"]

# SpatiaLite 内部表 / 视图，不迁移
SQLITE_SKIP_PATTERN = re.compile(
    r"^(sqlite_|spatial_ref_sys|geometry_columns|spatialite_|SpatialIndex|"
    r"ElementaryGeometries|KNN|virtuoso|views_geometry_columns|"
    r"virts_geometry_columns|geom_cols_ref_sys|geometry_columns_auth|"
    r"spatial_ref_sys_all|sqlite_sequence)$",
    re.I,
)


def load_db_config() -> dict:
    """加载 spatial_sql 数据库配置"""
    import yaml
    config_path = REPO_ROOT / "config" / "db_config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    # 获取 spatial_sql 数据库配置
    return data.get("databases", {}).get("spatial_sql", {})


def pg_connection_string(cfg: dict) -> str:
    return (
        f"PG:host={cfg['host']} port={cfg['port']} dbname={cfg['database']} "
        f"user={cfg['user']} password={cfg['password']}"
    )


def list_sqlite_tables(sqlite_path: str) -> list[str]:
    """列出 SQLite 数据库中的所有用户表（排除系统表）"""
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        names = [row[0] for row in cur.fetchall() if row[0]]
    finally:
        conn.close()
    return [n for n in names if not SQLITE_SKIP_PATTERN.match(n)]


def run_ogr2ogr(
    sqlite_path: str,
    table: str,
    pg_conn_str: str,
    target_table_name: str,
) -> tuple[bool, str]:
    """
    使用 ogr2ogr 迁移单个表到 PostgreSQL
    
    Args:
        sqlite_path: SQLite 数据库路径
        table: 源表名
        pg_conn_str: PostgreSQL 连接字符串
        target_table_name: 目标表名（包含前缀）
    
    Returns:
        (成功标志, 错误信息)
    """
    try:
        cmd = [
            "ogr2ogr",
            "-f", "PostgreSQL",
            pg_conn_str,
            sqlite_path,
            table,
            "-nln", target_table_name,  # 新表名
            "-lco", "OVERWRITE=YES",
            "-lco", "GEOMETRY_NAME=geom",
            "-lco", "FID=ogc_fid",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            return False, result.stderr
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Timeout after 300s"
    except FileNotFoundError:
        return False, "ogr2ogr not found (GDAL not installed)"
    except Exception as e:
        return False, str(e)


def migrate_all_databases():
    """迁移所有 SpatialSQL 数据库到 spatial_sql"""
    
    # 1. 加载配置
    db_cfg = load_db_config()
    pg_conn_str = pg_connection_string(db_cfg)
    
    sdbdatasets_path = REPO_ROOT / "sdbdatasets"
    if not sdbdatasets_path.exists():
        print(f"❌ 错误: {sdbdatasets_path} 不存在")
        return
    
    print(f"{'='*70}")
    print(f"开始迁移 SpatialSQL 数据到独立数据库: {db_cfg['database']}")
    print(f"{'='*70}\n")
    
    # 2. 准备迁移报告
    report = {
        "target_database": db_cfg['database'],
        "target_host": db_cfg['host'],
        "sdbdatasets_path": str(sdbdatasets_path),
        "databases": {}
    }
    
    total_dbs = 0
    total_tables = 0
    success_tables = 0
    
    # 3. 遍历所有 dataset 和 domain
    for version in VERSIONS:
        for domain in DOMAINS:
            db_key = f"{version}_{domain}"
            total_dbs += 1
            
            sqlite_path = sdbdatasets_path / version / domain / f"{domain}.sqlite"
            
            if not sqlite_path.exists():
                print(f"⚠️  跳过 {db_key}: 文件不存在")
                report["databases"][db_key] = {
                    "status": "skipped",
                    "reason": "file not found"
                }
                continue
            
            print(f"\n{'='*70}")
            print(f"处理: {db_key}")
            print(f"SQLite: {sqlite_path}")
            print(f"{'='*70}")
            
            # 获取所有表
            try:
                tables = list_sqlite_tables(str(sqlite_path))
                print(f"找到 {len(tables)} 个用户表")
            except Exception as e:
                print(f"❌ 读取表列表失败: {e}")
                report["databases"][db_key] = {
                    "status": "failed",
                    "error": str(e)
                }
                continue
            
            # 迁移每个表
            db_report = {"tables": {}}
            for table in tables:
                total_tables += 1
                # 目标表名：dataset1_ada_provinces
                target_table = f"{version}_{domain}_{table}"
                
                print(f"  迁移表: {table} -> {target_table} ... ", end="", flush=True)
                
                success, error = run_ogr2ogr(
                    str(sqlite_path),
                    table,
                    pg_conn_str,
                    target_table
                )
                
                if success:
                    print("✅")
                    db_report["tables"][table] = {
                        "status": "ok",
                        "target_name": target_table
                    }
                    success_tables += 1
                else:
                    print(f"❌\n    错误: {error}")
                    db_report["tables"][table] = {
                        "status": "failed",
                        "error": error,
                        "target_name": target_table
                    }
            
            db_report["status"] = "completed"
            db_report["sqlite_path"] = str(sqlite_path)
            report["databases"][db_key] = db_report
    
    # 4. 保存迁移报告
    report_path = REPO_ROOT / "scripts" / "spatialsql" / "migration_report_separate_db.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # 5. 打印总结
    print(f"\n{'='*70}")
    print(f"迁移完成总结")
    print(f"{'='*70}")
    print(f"处理数据库: {total_dbs}")
    print(f"迁移表总数: {total_tables}")
    print(f"成功: {success_tables}")
    print(f"失败: {total_tables - success_tables}")
    print(f"\n详细报告: {report_path}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    migrate_all_databases()

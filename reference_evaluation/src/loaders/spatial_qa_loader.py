"""Spatial QA数据集加载器"""
import os
import sys
from typing import List, Dict, Any
import openpyxl

# 添加src目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.base_data_loader import BaseDataLoader


class SpatialQALoader(BaseDataLoader):
    """Spatial QA数据集加载器 - 处理Excel格式的数据"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get('data_path', 'Spatial QA')
        self.levels = config.get('levels', [1, 2, 3])
        self.columns = config.get('columns', {})
    
    def load_raw_data(self, data_path: str) -> List[Dict]:
        """
        加载原始Excel数据
        
        Args:
            data_path: 数据目录路径
            
        Returns:
            原始数据列表
        """
        all_data = []
        
        for level in self.levels:
            level_file = os.path.join(data_path, f"Level {level}", f"level{level}.xlsx")
            
            if not os.path.exists(level_file):
                print(f"警告: 文件不存在 {level_file}")
                continue
            
            try:
                # 使用openpyxl读取Excel
                workbook = openpyxl.load_workbook(level_file, read_only=True)
                sheet = workbook.active
                
                # 获取表头
                headers = [cell.value for cell in sheet[1]]
                
                # 读取数据行
                for row in sheet.iter_rows(min_row=2, values_only=True):
                    if row[0] is not None:  # 跳过空行
                        row_data = dict(zip(headers, row))
                        row_data['level'] = level
                        all_data.append(row_data)
                
                workbook.close()
                print(f"成功加载 Level {level}: {len([d for d in all_data if d['level'] == level])} 条数据")
                
            except Exception as e:
                print(f"错误: 加载 {level_file} 失败 - {str(e)}")
                continue
        
        return all_data
    
    def extract_questions_and_sqls(self, raw_data: List[Dict]) -> List[Dict]:
        """
        提取问题和SQL答案
        
        Args:
            raw_data: 原始数据列表
            
        Returns:
            格式化后的数据列表
        """
        question_col = self.columns.get('question', 'Question')
        sql_col = self.columns.get('sql', 'SQL')
        
        extracted_data = []
        
        for idx, row in enumerate(raw_data, start=1):
            item = {
                "id": idx,
                "question": row.get(question_col, '').strip() if row.get(question_col) else '',
                "gold_sql": row.get(sql_col, '').strip() if row.get(sql_col) else '',
                "metadata": {
                    "level": row.get('level', 1)
                }
            }
            
            # 跳过空数据
            if item['question'] and item['gold_sql']:
                extracted_data.append(item)
        
        return extracted_data
    
    def get_dataset_info(self) -> Dict:
        """
        返回Spatial QA数据集元信息
        
        Returns:
            数据集元信息字典
        """
        return {
            "name": "spatial_qa",
            "grouping_fields": ["level"],
            "grouping_values": {
                "level": self.levels
            }
        }

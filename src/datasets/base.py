"""数据加载器抽象基类"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseDataLoader(ABC):
    """
    数据加载器抽象基类
    为不同格式的数据集提供统一的加载接口，支持未来数据集扩展
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据加载器
        
        Args:
            config: 数据集配置信息
        """
        self.config = config
    
    @abstractmethod
    def load_raw_data(self, data_path: str) -> List[Dict]:
        """
        加载原始数据（Excel/JSON/CSV等）
        
        Args:
            data_path: 数据文件路径
            
        Returns:
            原始数据列表
        """
        pass
    
    @abstractmethod
    def extract_questions_and_sqls(self, raw_data: Any) -> List[Dict]:
        """
        提取问题和SQL答案
        
        Args:
            raw_data: 原始数据
            
        Returns:
            格式化后的数据列表，每条包含：
            {
                "id": 1,
                "question": "...",
                "gold_sql": "...",
                "metadata": {"level": 1, ...}  # 可选的分组信息
            }
        """
        pass
    
    @abstractmethod
    def get_dataset_info(self) -> Dict:
        """
        返回数据集元信息（名称、分层结构等）
        
        Returns:
            数据集元信息字典：
            {
                "name": "spatial_qa",
                "grouping_fields": ["level"],  # 分组字段列表，无分层则为[]
                "grouping_values": {
                    "level": [1, 2, 3]  # 每个字段的可能值
                }
            }
        """
        pass

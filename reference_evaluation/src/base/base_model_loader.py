"""模型加载器抽象基类"""
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseModelLoader(ABC):
    """
    模型加载器抽象基类
    为不同架构的模型提供统一的加载和推理接口，支持未来模型扩展
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化模型加载器
        
        Args:
            config: 模型配置信息
        """
        self.config = config
        self.model = None
        self.tokenizer = None
    
    @abstractmethod
    def load_model(self, model_path: str, **kwargs):
        """
        加载模型和tokenizer
        
        Args:
            model_path: 模型路径
            **kwargs: 其他加载参数
        """
        pass
    
    @abstractmethod
    def generate_sql(self, prompt: str, **gen_kwargs) -> str:
        """
        根据prompt生成SQL
        
        Args:
            prompt: 输入提示词
            **gen_kwargs: 生成参数
            
        Returns:
            生成的SQL语句
        """
        pass
    
    @abstractmethod
    def get_model_info(self) -> Dict:
        """
        返回模型元信息（名称、参数量等）
        
        Returns:
            模型元信息字典
        """
        pass

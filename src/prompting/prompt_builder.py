"""Prompt构建模块 - 根据不同配置动态构建prompt"""
from typing import Dict, Optional


class PromptBuilder:
    """Prompt构建器 - 支持4种配置的动态prompt生成"""
    
    def __init__(self, config: Dict):
        """
        初始化Prompt构建器
        
        Args:
            config: Prompt配置信息
        """
        self.config = config
    
    def build_prompt(self, question: str, schema: str, 
                     config_type: str = 'base',
                     rag_context: Optional[str] = None,
                     keyword_context: Optional[str] = None) -> str:
        """
        根据配置类型构建prompt
        
        Args:
            question: 用户问题
            schema: 数据库Schema
            config_type: 配置类型 (base/rag/keyword/full)
            rag_context: RAG检索的context（可选）
            keyword_context: Keyword检索的context（可选）
            
        Returns:
            完整的prompt字符串
        """
        # 基础prompt模板
        prompt_parts = [
            "你是一个spatial text2sql专家。请根据以下信息生成PostgreSQL+PostGIS的SQL查询语句。",
            "",
            "## 数据库Schema",
            schema,
            ""
        ]
        
        # 根据配置类型添加额外context
        if config_type in ['rag', 'full'] and rag_context:
            prompt_parts.append(rag_context)
            prompt_parts.append("")
        
        if config_type in ['keyword', 'full'] and keyword_context:
            prompt_parts.append(keyword_context)
            prompt_parts.append("")
        
        # 用户问题
        prompt_parts.extend([
            "## 用户问题",
            question,
            "",
            "## 要求",
            "1. 只输出可执行的SQL语句，不要包含任何解释或注释",
            "2. 使用PostGIS空间函数处理空间查询",
            "3. 确保SQL语法正确且可在PostgreSQL中执行",
            "",
            "SQL:"
        ])
        
        return "\n".join(prompt_parts)
    
    def build_batch_prompts(self, questions: list, schema: str,
                           config_type: str = 'base',
                           rag_contexts: Optional[list] = None,
                           keyword_contexts: Optional[list] = None) -> list:
        """
        批量构建prompts
        
        Args:
            questions: 问题列表
            schema: 数据库Schema
            config_type: 配置类型
            rag_contexts: RAG context列表（可选）
            keyword_contexts: Keyword context列表（可选）
            
        Returns:
            prompt列表
        """
        prompts = []
        
        for i, question in enumerate(questions):
            rag_ctx = rag_contexts[i] if rag_contexts and i < len(rag_contexts) else None
            kw_ctx = keyword_contexts[i] if keyword_contexts and i < len(keyword_contexts) else None
            
            prompt = self.build_prompt(
                question=question,
                schema=schema,
                config_type=config_type,
                rag_context=rag_ctx,
                keyword_context=kw_ctx
            )
            prompts.append(prompt)
        
        return prompts
    
    @staticmethod
    def get_config_description(config_type: str) -> str:
        """
        获取配置类型的描述
        
        Args:
            config_type: 配置类型
            
        Returns:
            配置描述字符串
        """
        descriptions = {
            'base': 'Question + Schema',
            'rag': 'Question + Schema + RAG',
            'keyword': 'Question + Schema + Keyword',
            'full': 'Question + Schema + RAG + Keyword'
        }
        return descriptions.get(config_type, 'Unknown')

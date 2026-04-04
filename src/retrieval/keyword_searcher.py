"""Keyword检索模块 - 基于TF-IDF的关键词匹配检索"""
import json
import os
import re
from typing import List, Dict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class KeywordSearcher:
    """Keyword检索器 - 基于TF-IDF的关键词相似度"""
    
    def __init__(self, config: Dict):
        """
        初始化Keyword检索器
        
        Args:
            config: Keyword检索配置信息
        """
        self.config = config
        self.top_k = config.get('top_k', 3)
        self.method = config.get('method', 'tfidf')
        self.doc_source = config.get('doc_source', 'postgis_extracted.json')
        
        self.documents = []
        self.vectorizer = None
        self.tfidf_matrix = None
    
    def load_documents(self):
        """加载文档并构建TF-IDF矩阵"""
        print("\n" + "="*60)
        print("加载Keyword检索文档")
        print("="*60 + "\n")
        
        # 加载文档
        if not os.path.exists(self.doc_source):
            raise FileNotFoundError(f"文档文件不存在: {self.doc_source}")
        
        with open(self.doc_source, 'r', encoding='utf-8') as f:
            raw_docs = json.load(f)
        
        print(f"成功加载 {len(raw_docs)} 个文档")
        
        # 处理文档
        for idx, doc in enumerate(raw_docs):
            doc_text = self._format_function_doc(doc)
            self.documents.append({
                'id': idx,
                'text': doc_text,
                'function_id': doc.get('function_id', ''),
                'description': doc.get('description', ''),
                'examples': doc.get('examples', [])
            })
        
        # 构建TF-IDF矩阵
        print("构建TF-IDF矩阵...")
        texts = [doc['text'] for doc in self.documents]
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.tfidf_matrix = self.vectorizer.fit_transform(texts)
        
        print(f"TF-IDF矩阵构建完成: {self.tfidf_matrix.shape}")
        print("="*60 + "\n")
    
    def _format_function_doc(self, doc: Dict) -> str:
        """格式化函数文档为可检索的文本"""
        parts = []
        
        # 函数名
        function_id = doc.get('function_id', '')
        if function_id:
            parts.append(function_id)
        
        # 函数定义
        if 'function_definitions' in doc and doc['function_definitions']:
            for func_def in doc['function_definitions']:
                func_name = func_def.get('function_name', '')
                if func_name:
                    parts.append(func_name)
        
        # 描述
        description = doc.get('description', '')
        if description:
            parts.append(description[:500])  # 限制长度
        
        return " ".join(parts)
    
    def search(self, question: str) -> List[str]:
        """
        根据问题进行关键词检索
        
        Args:
            question: 用户问题
            
        Returns:
            相关文档列表（格式化为自然语言描述）
        """
        if not self.documents:
            self.load_documents()
        
        # 提取PostGIS函数名（如ST_Intersects, ST_Buffer等）
        postgis_functions = self._extract_postgis_functions(question)
        
        # 如果提取到了函数名，优先精确匹配
        if postgis_functions:
            matched_docs = self._exact_match(postgis_functions)
            if matched_docs:
                return matched_docs[:self.top_k]
        
        # TF-IDF相似度检索
        return self._tfidf_search(question)
    
    def _extract_postgis_functions(self, text: str) -> List[str]:
        """
        从文本中提取PostGIS函数名
        
        Args:
            text: 输入文本
            
        Returns:
            提取到的函数名列表
        """
        # PostGIS函数通常以ST_开头
        pattern = r'\b(ST_[A-Za-z]+)\b'
        matches = re.findall(pattern, text, re.IGNORECASE)
        return list(set([m.upper() for m in matches]))
    
    def _exact_match(self, function_names: List[str]) -> List[str]:
        """
        精确匹配函数名
        
        Args:
            function_names: 函数名列表
            
        Returns:
            匹配到的文档列表
        """
        matched_docs = []
        
        for func_name in function_names:
            for doc in self.documents:
                # 检查function_id或text中是否包含函数名
                if (func_name.lower() in doc['function_id'].lower() or 
                    func_name.lower() in doc['text'].lower()):
                    
                    formatted_doc = self._format_search_result(doc)
                    if formatted_doc not in matched_docs:
                        matched_docs.append(formatted_doc)
        
        return matched_docs
    
    def _tfidf_search(self, question: str) -> List[str]:
        """
        使用TF-IDF进行相似度检索
        
        Args:
            question: 用户问题
            
        Returns:
            相关文档列表
        """
        # 将问题转换为TF-IDF向量
        question_vec = self.vectorizer.transform([question])
        
        # 计算余弦相似度
        similarities = cosine_similarity(question_vec, self.tfidf_matrix).flatten()
        
        # 获取top-k最相似的文档
        top_indices = np.argsort(similarities)[::-1][:self.top_k]
        
        # 格式化结果
        results = []
        for idx in top_indices:
            if similarities[idx] > 0:  # 只返回相似度>0的结果
                doc = self.documents[idx]
                formatted_doc = self._format_search_result(doc)
                results.append(formatted_doc)
        
        return results
    
    def _format_search_result(self, doc: Dict) -> str:
        """
        格式化搜索结果
        
        Args:
            doc: 文档字典
            
        Returns:
            格式化的字符串
        """
        parts = []
        
        # 函数名
        if doc['function_id']:
            parts.append(f"Function: {doc['function_id']}")
        
        # 描述
        if doc['description']:
            desc = doc['description'][:300]  # 限制长度
            parts.append(f"Description: {desc}")
        
        # 示例SQL（取第一个）
        if doc['examples']:
            example = doc['examples'][0]
            if 'steps' in example and example['steps']:
                step = example['steps'][0]
                if 'sql' in step:
                    sql = step['sql'][:200]  # 限制长度
                    parts.append(f"Example SQL: {sql}")
        
        return "\n".join(parts)
    
    def format_context(self, retrieved_docs: List[str]) -> str:
        """
        将检索到的文档格式化为context字符串
        
        Args:
            retrieved_docs: 检索到的文档列表
            
        Returns:
            格式化的context字符串
        """
        if not retrieved_docs:
            return ""
        
        context_parts = ["## 相关PostGIS函数（关键词匹配）"]
        for i, doc in enumerate(retrieved_docs, 1):
            context_parts.append(f"\n### 函数 {i}")
            context_parts.append(doc)
        
        return "\n".join(context_parts)

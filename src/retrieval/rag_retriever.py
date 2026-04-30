"""RAG检索模块 - 使用ChromaDB构建向量数据库并进行语义检索"""
import json
import os
from typing import List, Dict, Optional

import chromadb
from sentence_transformers import SentenceTransformer

# 设置使用Hugging Face镜像加速模型下载
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'


class RAGRetriever:
    """RAG检索器 - 基于ChromaDB的语义检索"""
    
    def __init__(self, config: Dict):
        """
        初始化RAG检索器
        
        Args:
            config: RAG配置信息
        """
        self.config = config
        self.top_k = config.get('top_k', 5)
        self.embedding_model_name = config.get('embedding_model', 
                                               'sentence-transformers/all-MiniLM-L6-v2')
        self.vector_db_path = config.get('vector_db_path', 'rag_db')
        self.doc_source = config.get('doc_source', 'postgis_extracted.json')
        self.chunk_strategy = config.get('chunk_strategy', 'function_id')
        
        self.embedding_model = None
        self.client = None
        self.collection = None
    
    def build_index(self):
        """构建或加载向量索引"""
        print("\n" + "="*60)
        print("RAG索引构建")
        print("="*60 + "\n")
        
        # 初始化ChromaDB
        print(f"步骤 1/4: 初始化ChromaDB (路径: {self.vector_db_path})")
        os.makedirs(self.vector_db_path, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.vector_db_path)
        
        # 检查是否已存在集合
        try:
            self.collection = self.client.get_collection(name="postgis_functions")
            doc_count = self.collection.count()
            print(f"找到现有索引，包含 {doc_count} 个文档\n")
            return
        except:
            print("未找到现有索引，开始构建新索引\n")
        
        # 加载嵌入模型
        print(f"步骤 2/4: 加载嵌入模型 ({self.embedding_model_name})")
        # 首先尝试从本地缓存加载
        local_model_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
        if os.path.exists(local_model_path):
            print(f"  从本地缓存加载: {local_model_path}")
            self.embedding_model = SentenceTransformer(local_model_path, local_files_only=True)
        else:
            print(f"  本地缓存不存在，从HuggingFace镜像下载: {self.embedding_model_name}")
            self.embedding_model = SentenceTransformer(self.embedding_model_name)
        print("嵌入模型加载完成\n")
        
        # 加载文档
        print(f"步骤 3/4: 加载文档 ({self.doc_source})")
        documents = self._load_documents()
        print(f"成功加载 {len(documents)} 个文档\n")
        
        # 构建索引
        print("步骤 4/4: 构建向量索引")
        self._create_index(documents)
        print(f"索引构建完成，共 {len(documents)} 个文档\n")
        
        print("="*60)
        print("RAG索引构建完成!")
        print("="*60 + "\n")
    
    def _load_documents(self) -> List[Dict]:
        """加载PostGIS文档"""
        if not os.path.exists(self.doc_source):
            raise FileNotFoundError(f"文档文件不存在: {self.doc_source}")
        
        with open(self.doc_source, 'r', encoding='utf-8') as f:
            raw_docs = json.load(f)
        
        # 根据切片策略处理文档
        documents = []
        for idx, doc in enumerate(raw_docs):
            if self.chunk_strategy == 'function_id':
                # 按function_id切分，每个函数作为一个文档单元
                doc_text = self._format_function_doc(doc)
                documents.append({
                    'id': f"func_{idx}",
                    'text': doc_text,
                    'metadata': {
                        'function_id': doc.get('function_id', ''),
                        'chapter': doc.get('chapter_info', '')
                    }
                })
        
        return documents
    
    def _format_function_doc(self, doc: Dict) -> str:
        """
        格式化函数文档为可检索的文本
        
        Args:
            doc: 原始文档字典
            
        Returns:
            格式化后的文本
        """
        parts = []
        
        # 函数名
        function_id = doc.get('function_id', '')
        if function_id:
            parts.append(f"Function: {function_id}")
        
        # 函数定义
        if 'function_definitions' in doc and doc['function_definitions']:
            for func_def in doc['function_definitions']:
                func_name = func_def.get('function_name', '')
                signature = func_def.get('signature_str', '')
                if signature:
                    parts.append(f"Signature: {signature}")
        
        # 描述
        description = doc.get('description', '')
        if description:
            parts.append(f"Description: {description}")
        
        # 示例（取前2个）
        if 'examples' in doc and doc['examples']:
            for i, example in enumerate(doc['examples'][:2], 1):
                example_name = example.get('name', '')
                if example_name:
                    parts.append(f"Example {i}: {example_name}")
                
                # 示例中的SQL
                if 'steps' in example:
                    for step in example['steps']:
                        sql = step.get('sql', '')
                        question = step.get('question', '')
                        if question:
                            parts.append(f"  Question: {question}")
                        if sql:
                            parts.append(f"  SQL: {sql[:200]}")  # 限制长度
        
        return "\n".join(parts)
    
    def _create_index(self, documents: List[Dict]):
        """创建向量索引"""
        # 创建集合
        self.collection = self.client.create_collection(
            name="postgis_functions",
            metadata={"hnsw:space": "cosine"}
        )
        
        # 批量添加文档
        batch_size = 100
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            
            ids = [doc['id'] for doc in batch]
            texts = [doc['text'] for doc in batch]
            metadatas = [doc['metadata'] for doc in batch]
            
            # 生成嵌入
            embeddings = self.embedding_model.encode(texts, show_progress_bar=True).tolist()
            
            # 添加到集合
            self.collection.add(
                ids=ids,
                embeddings=embeddings,
                documents=texts,
                metadatas=metadatas
            )
            
            print(f"  已处理 {min(i+batch_size, len(documents))}/{len(documents)} 个文档")
    
    def retrieve(self, question: str, item: Optional[Dict] = None) -> List[str]:
        """
        根据问题检索相关文档
        
        Args:
            question: 用户问题
            
        Returns:
            相关文档列表（格式化为自然语言描述）
        """
        del item
        if self.collection is None:
            # 如果集合未加载，加载现有集合
            if self.client is None:
                self.client = chromadb.PersistentClient(path=self.vector_db_path)
            self.collection = self.client.get_collection(name="postgis_functions")
        
        if self.embedding_model is None:
            # 首先尝试从本地缓存加载
            local_model_path = os.path.expanduser("~/.cache/huggingface/hub/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/c9745ed1d9f207416be6d2e6f8de32d1f16199bf")
            if os.path.exists(local_model_path):
                print(f"从本地缓存加载嵌入模型: {local_model_path}")
                self.embedding_model = SentenceTransformer(local_model_path, local_files_only=True)
            else:
                print(f"本地缓存不存在，从HuggingFace镜像加载嵌入模型: {self.embedding_model_name}")
                self.embedding_model = SentenceTransformer(self.embedding_model_name)
        
        # 生成问题嵌入
        question_embedding = self.embedding_model.encode(question).tolist()
        
        # 检索
        results = self.collection.query(
            query_embeddings=[question_embedding],
            n_results=self.top_k
        )
        
        # 格式化结果
        retrieved_docs = []
        if results['documents'] and results['documents'][0]:
            for doc in results['documents'][0]:
                retrieved_docs.append(doc)
        
        return retrieved_docs
    
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
        
        context_parts = ["## 相关PostGIS函数文档"]
        for i, doc in enumerate(retrieved_docs, 1):
            context_parts.append(f"\n### 文档 {i}")
            context_parts.append(doc)
        
        return "\n".join(context_parts)

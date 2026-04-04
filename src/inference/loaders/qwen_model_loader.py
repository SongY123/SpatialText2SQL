"""Qwen系列模型加载器"""
import re
import os
from typing import Dict, Any

from transformers import AutoModelForCausalLM, AutoTokenizer

# 设置使用Hugging Face镜像加速模型下载
os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

from src.inference.base import BaseModelLoader

# 导入torch
try:
    import torch
except ImportError:
    print("警告: torch未安装，模型推理将无法工作")
    torch = None


class QwenModelLoader(BaseModelLoader):
    """Qwen系列模型加载器"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.model_path = os.path.expanduser(config.get('model_path', ''))
        self.device_map = config.get('device_map', 'auto')
        self.generation_config = config.get('generation_config', {})
    
    def load_model(self, model_path: str = None, **kwargs):
        """
        加载Qwen模型和tokenizer
        
        Args:
            model_path: 模型路径（可选，如果不提供则使用配置中的路径）
            **kwargs: 其他加载参数
        """
        if model_path is None:
            model_path = self.model_path
        
        model_path = os.path.expanduser(model_path)
        
        print(f"\n加载模型: {model_path}")
        
        try:
            # 加载tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                model_path,
                trust_remote_code=True
            )
            print("✓ Tokenizer加载成功")
            
            # 加载模型
            self.model = AutoModelForCausalLM.from_pretrained(
                model_path,
                device_map=self.device_map,
                trust_remote_code=True,
                **kwargs
            )
            print(f"✓ 模型加载成功 (device: {self.device_map})")
            
        except Exception as e:
            print(f"✗ 模型加载失败: {str(e)}")
            raise
    
    def generate_sql(self, prompt: str, **gen_kwargs) -> str:
        """
        根据prompt生成SQL
        
        Args:
            prompt: 输入提示词
            **gen_kwargs: 生成参数（会覆盖配置中的默认参数）
            
        Returns:
            生成的SQL语句
        """
        if self.model is None or self.tokenizer is None:
            raise RuntimeError("模型未加载，请先调用load_model()")
        
        # 合并生成配置
        gen_config = {**self.generation_config, **gen_kwargs}
        
        # Tokenize
        inputs = self.tokenizer(prompt, return_tensors="pt")
        inputs = {k: v.to(self.model.device) for k, v in inputs.items()}
        
        # 生成
        if torch is None:
            raise RuntimeError("torch未安装")
        
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=gen_config.get('max_new_tokens', 512),
                temperature=gen_config.get('temperature', 0.1),
                top_p=gen_config.get('top_p', 0.9),
                do_sample=gen_config.get('do_sample', False),
                repetition_penalty=gen_config.get('repetition_penalty', 1.1),
                pad_token_id=self.tokenizer.eos_token_id
            )
        
        # 解码
        generated_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # 提取SQL（去除prompt部分）
        sql = self._extract_sql(generated_text, prompt)
        
        return sql
    
    def _extract_sql(self, generated_text: str, prompt: str) -> str:
        """
        从生成的文本中提取SQL语句
        
        Args:
            generated_text: 完整的生成文本
            prompt: 输入的prompt
            
        Returns:
            提取出的SQL语句
        """
        # 移除prompt部分
        if generated_text.startswith(prompt):
            sql = generated_text[len(prompt):].strip()
        else:
            sql = generated_text.strip()

        # 移除markdown代码块标记
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)

        # 提取第一个完整的SQL语句（从 SQL 关键字开始，到第一个分号结束）
        sql_keywords_pattern = r'(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)\s+.*?;'
        match = re.search(sql_keywords_pattern, sql, re.IGNORECASE | re.DOTALL)

        if match:
            sql = match.group(0)
        else:
            # 如果没有找到标准格式，尝试找到第一个分号之前的内容，
            # 但需要确保以 SQL 关键字开头
            sql_keywords = r'(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)'
            keyword_match = re.search(sql_keywords, sql, re.IGNORECASE)
            if keyword_match:
                start_pos = keyword_match.start()
                sql = sql[start_pos:]
                # 找到第一个分号
                semicolon_pos = sql.find(';')
                if semicolon_pos > 0:
                    sql = sql[:semicolon_pos + 1]
                else:
                    # 如果没有分号，找到第一个中文字符或明显的非 SQL 文本
                    chinese_pattern = r'[\u4e00-\u9fff]'
                    chinese_match = re.search(chinese_pattern, sql)
                    if chinese_match:
                        sql = sql[:chinese_match.start()].strip()
                        # 确保以分号结尾
                        if sql and not sql.endswith(';'):
                            last_semicolon = sql.rfind(';')
                            if last_semicolon > 0:
                                sql = sql[:last_semicolon + 1]

        # 移除注释
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)

        # 清理多余的空白
        sql = re.sub(r'\s+', ' ', sql)
        sql = sql.strip()

        # 确保以分号结尾
        if sql and not sql.endswith(';'):
            sql += ';'

        return sql
    
    def unload(self):
        """
        卸载模型并清理 GPU 内存
        """
        if self.model is not None:
            del self.model
            self.model = None
        
        if self.tokenizer is not None:
            del self.tokenizer
            self.tokenizer = None
        
        # 清理 GPU 缓存
        if torch is not None and torch.cuda.is_available():
            for i in range(torch.cuda.device_count()):
                with torch.cuda.device(i):
                    torch.cuda.empty_cache()
                    torch.cuda.ipc_collect()
        
        # 垃圾回收
        import gc
        gc.collect()
    
    def __del__(self):
        """析构函数：确保对象销毁时清理资源"""
        try:
            self.unload()
        except Exception:
            pass
    
    def get_model_info(self) -> Dict:
        """
        返回模型元信息
        
        Returns:
            模型元信息字典
        """
        info = {
            'model_path': self.model_path,
            'device_map': self.device_map,
            'loaded': self.model is not None
        }
        
        if self.model is not None:
            try:
                # 尝试获取模型参数量
                total_params = sum(p.numel() for p in self.model.parameters())
                info['total_parameters'] = total_params
                info['total_parameters_billions'] = total_params / 1e9
            except:
                pass
        
        return info

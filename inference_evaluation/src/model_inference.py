"""模型推理统一入口 - 工厂模式 + 批量推理"""
import yaml
import json
import os
import sys
from typing import Dict, List, Any
from tqdm import tqdm

# 添加src目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loaders.qwen_model_loader import QwenModelLoader


class ModelLoaderFactory:
    """模型加载器工厂类"""
    
    # 注册模型加载器映射
    _loaders = {
        'QwenModelLoader': QwenModelLoader
    }
    
    @classmethod
    def create(cls, model_type: str, config: Dict[str, Any]):
        """
        创建模型加载器实例
        
        Args:
            model_type: 模型类型对应的加载器类名
            config: 模型配置
            
        Returns:
            模型加载器实例
        """
        loader_class = cls._loaders.get(model_type)
        if not loader_class:
            raise ValueError(f"未知的模型加载器类型: {model_type}")
        return loader_class(config)
    
    @classmethod
    def register_loader(cls, name: str, loader_class):
        """
        注册新的模型加载器
        
        Args:
            name: 加载器名称
            loader_class: 加载器类
        """
        cls._loaders[name] = loader_class


class ModelInference:
    """模型推理器 - 批量推理，带进度条"""
    
    def __init__(self, model_config_path: str, eval_config_path: str):
        """
        初始化模型推理器
        
        Args:
            model_config_path: 模型配置文件路径
            eval_config_path: 评估配置文件路径
        """
        # 加载配置
        with open(model_config_path, 'r', encoding='utf-8') as f:
            self.model_config = yaml.safe_load(f)
        
        with open(eval_config_path, 'r', encoding='utf-8') as f:
            self.eval_config = yaml.safe_load(f)
        
        self.inference_config = self.model_config.get('inference', {})
        self.results_config = self.eval_config.get('results', {})
    
    def run_inference(self, model_name: str, config_type: str, 
                     prompts: List[str], data_items: List[Dict],
                     save_dir: str):
        """
        运行模型推理
        
        Args:
            model_name: 模型名称
            config_type: 配置类型 (base/rag/keyword/full)
            prompts: prompt列表
            data_items: 数据项列表（包含id, question, gold_sql等）
            save_dir: 结果保存目录
        """
        print(f"\n{'='*70}")
        print(f"模型推理: {model_name} | 配置: {config_type}")
        print(f"{'='*70}\n")
        
        # 获取模型配置
        model_info = self.model_config['models'].get(model_name)
        if not model_info:
            raise ValueError(f"未找到模型配置: {model_name}")
        
        # 创建模型加载器
        loader_class_name = model_info['loader_class']
        model_loader = ModelLoaderFactory.create(loader_class_name, model_info)
        
        # 加载模型
        model_loader.load_model()
        
        # 准备结果列表
        results = []
        
        # 批量推理
        batch_size = self.inference_config.get('batch_size', 1)
        save_interval = self.inference_config.get('save_interval', 10)
        show_progress = self.inference_config.get('show_progress', True)
        
        # 使用tqdm显示进度
        iterator = tqdm(enumerate(prompts), total=len(prompts), 
                       desc=f"{model_name}-{config_type}",
                       disable=not show_progress)
        
        for idx, prompt in iterator:
            try:
                # 生成SQL
                predicted_sql = model_loader.generate_sql(prompt)
                
                # 记录结果
                result_item = {
                    'id': data_items[idx]['id'],
                    'question': data_items[idx]['question'],
                    'gold_sql': data_items[idx]['gold_sql'],
                    'predicted_sql': predicted_sql,
                    'metadata': data_items[idx].get('metadata', {})
                }
                results.append(result_item)
                
                # 定期保存
                if (idx + 1) % save_interval == 0:
                    self._save_intermediate_results(results, save_dir, model_name, config_type)
                
            except Exception as e:
                print(f"\n错误: 处理第{idx+1}条数据时出错 - {str(e)}")
                # 记录错误
                results.append({
                    'id': data_items[idx]['id'],
                    'question': data_items[idx]['question'],
                    'gold_sql': data_items[idx]['gold_sql'],
                    'predicted_sql': '',
                    'error': str(e),
                    'metadata': data_items[idx].get('metadata', {})
                })
        
        # 最终保存
        self._save_final_results(results, save_dir, model_name, config_type)

        # 释放模型和清理GPU内存，避免在多个配置/模型之间累积占用
        print("\n" + "="*70)
        print("清理 GPU 内存...")
        print("="*70)
        
        try:
            # 记录清理前的 GPU 内存使用
            try:
                import torch
                if torch.cuda.is_available():
                    before_mem = []
                    for i in range(torch.cuda.device_count()):
                        mem_allocated = torch.cuda.memory_allocated(i) / 1024**3
                        before_mem.append(f"GPU{i}: {mem_allocated:.2f}GB")
                    print(f"清理前: {', '.join(before_mem)}")
            except Exception:
                pass
            
            # 调用模型加载器的 unload 方法
            if hasattr(model_loader, 'unload'):
                model_loader.unload()
                print("✅ 已卸载模型并清理 GPU 缓存")
            else:
                # 兼容旧版本：手动删除
                if hasattr(model_loader, 'model') and model_loader.model is not None:
                    del model_loader.model
                    model_loader.model = None
                if hasattr(model_loader, 'tokenizer') and model_loader.tokenizer is not None:
                    del model_loader.tokenizer
                    model_loader.tokenizer = None
                print("✅ 已删除模型和 tokenizer 引用")
            
            # 删除整个 model_loader 对象
            del model_loader
            print("✅ 已删除 model_loader 对象")

            # 执行垃圾回收
            try:
                import gc
                gc.collect()
                print("✅ 执行垃圾回收")
            except Exception:
                pass

            # 再次清理所有 GPU 的缓存（确保彻底）
            try:
                import torch
                if torch.cuda.is_available():
                    for i in range(torch.cuda.device_count()):
                        with torch.cuda.device(i):
                            torch.cuda.empty_cache()
                            torch.cuda.ipc_collect()
                    print(f"✅ 已清理 {torch.cuda.device_count()} 个 GPU 的缓存")
                    
                    # 最后一次垃圾回收
                    gc.collect()
                    
                    # 记录清理后的 GPU 内存使用
                    after_mem = []
                    for i in range(torch.cuda.device_count()):
                        mem_allocated = torch.cuda.memory_allocated(i) / 1024**3
                        after_mem.append(f"GPU{i}: {mem_allocated:.2f}GB")
                    print(f"清理后: {', '.join(after_mem)}")
            except Exception as e:
                print(f"⚠️  GPU 缓存清理出现异常（可忽略）: {e}")
        except Exception as e:
            print(f"⚠️  内存清理出现异常（可忽略）: {e}")
        
        print("="*70)
        print(f"\n推理完成! 共处理 {len(results)} 条数据")
        print(f"结果已保存到: {save_dir}")

        return results
    
    def _save_intermediate_results(self, results: List[Dict], save_dir: str, 
                                   model_name: str, config_type: str):
        """保存中间结果"""
        os.makedirs(save_dir, exist_ok=True)
        output_file = os.path.join(save_dir, f"{model_name}_{config_type}_temp.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    
    def _save_final_results(self, results: List[Dict], save_dir: str,
                           model_name: str, config_type: str):
        """保存最终结果"""
        os.makedirs(save_dir, exist_ok=True)
        output_file = os.path.join(save_dir, f"{model_name}_{config_type}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 删除临时文件
        temp_file = os.path.join(save_dir, f"{model_name}_{config_type}_temp.json")
        if os.path.exists(temp_file):
            os.remove(temp_file)

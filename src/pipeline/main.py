"""主流程控制 - 参数解析，流程编排，结果汇总"""
import argparse
import json
import os
import sys

import yaml


class MainPipeline:
    """主流程控制器"""
    
    def __init__(self, args):
        """
        初始化主流程
        
        Args:
            args: 命令行参数
        """
        self.args = args
        self.config_dir = args.config_dir
        
        # 加载配置
        self.dataset_config = self._load_config('dataset_config.yaml')
        self.db_config = self._load_config('db_config.yaml')
        self.model_config = self._load_config('model_config.yaml')
        self.eval_config = self._load_config('eval_config.yaml')
        
        # 获取数据集名称
        self.dataset_name = args.dataset or self.dataset_config.get('default_dataset', 'spatial_qa')
        self.dataset_info_dict = self.dataset_config['datasets'][self.dataset_name]
        
        # 获取模型列表
        if args.models:
            self.model_names = args.models
        else:
            self.model_names = self.model_config.get('default_models', [])
        
        # 获取配置列表
        if args.configs:
            self.config_types = args.configs
        else:
            self.config_types = self.eval_config.get('default_configs', ['base', 'rag', 'keyword', 'full'])
    
    def _load_config(self, filename: str) -> dict:
        """加载配置文件"""
        config_path = os.path.join(self.config_dir, filename)
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _get_dataset_db_config(self) -> dict:
        """
        根据数据集配置获取对应的数据库配置
        
        Returns:
            数据库配置字典
        """
        # 获取数据集指定的数据库名称
        db_name = self.dataset_info_dict.get('database', 'default')
        
        # 如果指定了具体数据库且存在于 databases 配置中
        if db_name != 'default' and 'databases' in self.db_config:
            databases = self.db_config['databases']
            if db_name in databases:
                return databases[db_name]
        
        # 否则使用默认数据库配置（向后兼容）
        return self.db_config.get('database', {})
    
    def run(self):
        """运行主流程"""
        print("\n" + "="*80)
        print("Spatial Text2SQL 推理评估框架")
        print("="*80 + "\n")
        
        # 步骤1: 数据预处理
        if self.args.preprocess:
            self._run_preprocessing()
        
        # 步骤2: 构建RAG索引
        if self.args.build_rag:
            self._build_rag_index()
        
        # 步骤3: 推理
        all_eval_results = []
        if self.args.inference:
            all_eval_results = self._run_inference_and_evaluation()
        
        # 步骤4: 单独评估（如果只有 --evaluate 没有 --inference）
        if self.args.evaluate and not self.args.inference:
            all_eval_results = self._run_evaluation_only()
        
        # 步骤5: 生成报告
        if self.args.evaluate and all_eval_results:
            self._generate_final_report(all_eval_results)
        
        print("\n" + "="*80)
        print("所有任务完成!")
        print("="*80 + "\n")
    
    def _run_preprocessing(self):
        """运行数据预处理"""
        from src.datasets.processing import DataPreprocessor

        print("\n" + "="*80)
        print("步骤 1: 数据预处理")
        print("="*80 + "\n")
        
        preprocessor = DataPreprocessor(
            dataset_config_path=os.path.join(self.config_dir, 'dataset_config.yaml'),
            db_config_path=os.path.join(self.config_dir, 'db_config.yaml')
        )
        preprocessor.preprocess(self.dataset_name)
    
    def _build_rag_index(self):
        """构建RAG索引"""
        from src.retrieval.rag_retriever import RAGRetriever

        print("\n" + "="*80)
        print("步骤 2: 构建RAG索引")
        print("="*80 + "\n")
        
        rag_config = self.eval_config.get('rag', {})
        rag_retriever = RAGRetriever(rag_config)
        rag_retriever.build_index()
    
    def _run_inference_and_evaluation(self) -> list:
        """运行推理和评估"""
        from src.datasets.processing import DataLoaderFactory
        from src.evaluation.evaluator import Evaluator
        from src.inference.model_inference import ModelInference
        from src.prompting.prompt_builder import PromptBuilder
        from src.retrieval.keyword_searcher import KeywordSearcher
        from src.retrieval.rag_retriever import RAGRetriever

        print("\n" + "="*80)
        print("步骤 3: 模型推理和评估")
        print("="*80 + "\n")
        
        # 加载预处理后的数据
        preprocessed_data = self._load_preprocessed_data()
        
        # 初始化检索器
        rag_retriever = None
        keyword_searcher = None
        
        if 'rag' in self.config_types or 'full' in self.config_types:
            rag_config = self.eval_config.get('rag', {})
            rag_retriever = RAGRetriever(rag_config)
            rag_retriever.build_index()  # 加载已有索引
        
        if 'keyword' in self.config_types or 'full' in self.config_types:
            keyword_config = self.eval_config.get('keyword_search', {})
            keyword_searcher = KeywordSearcher(keyword_config)
            keyword_searcher.load_documents()
        
        # 初始化组件
        prompt_builder = PromptBuilder(self.eval_config)
        model_inference = ModelInference(
            model_config_path=os.path.join(self.config_dir, 'model_config.yaml'),
            eval_config_path=os.path.join(self.config_dir, 'eval_config.yaml')
        )
        # 使用数据集对应的数据库配置
        dataset_db_config = self._get_dataset_db_config()
        evaluator = Evaluator(
            db_config=dataset_db_config,
            eval_config=self.eval_config
        )
        
        # 获取数据集元信息（工厂创建，兼容 spatial_qa / spatialsql_pg 等）
        loader_class_name = self.dataset_info_dict['loader_class']
        data_loader = DataLoaderFactory.create(loader_class_name, self.dataset_info_dict)
        dataset_info = data_loader.get_dataset_info()
        
        # 所有评估结果
        all_eval_results = []
        
        # 对每个模型和每个配置运行推理和评估
        total_tasks = len(self.model_names) * len(self.config_types)
        current_task = 0
        
        for model_name in self.model_names:
            for config_type in self.config_types:
                current_task += 1
                print(f"\n[{current_task}/{total_tasks}] 处理: {model_name} - {config_type}")
                
                # 准备prompts
                prompts = self._prepare_prompts(
                    preprocessed_data,
                    config_type,
                    prompt_builder,
                    rag_retriever,
                    keyword_searcher
                )
                
                # 运行推理
                results_dir = os.path.join(
                    self.eval_config['results']['predictions_dir'],
                    model_name,
                    config_type
                )
                predictions = model_inference.run_inference(
                    model_name=model_name,
                    config_type=config_type,
                    prompts=prompts,
                    data_items=preprocessed_data,
                    save_dir=results_dir
                )
                
                # 运行评估
                if self.args.evaluate:
                    eval_result = evaluator.evaluate(
                        predictions=predictions,
                        dataset_info=dataset_info,
                        model_name=model_name,
                        config_type=config_type
                    )
                    
                    # 保存评估结果
                    eval_dir = self.eval_config['results']['evaluations_dir']
                    evaluator.save_evaluation(eval_result, eval_dir)
                    all_eval_results.append(eval_result)
        
        return all_eval_results
    
    def _load_preprocessed_data(self) -> list:
        """加载预处理后的数据"""
        preprocessing_config = self.dataset_config.get('preprocessing', {})
        output_dir = preprocessing_config.get('output_dir', 'data/preprocessed')
        dataset_dir = os.path.join(output_dir, self.dataset_name)
        
        # 根据分组加载数据
        grouping = self.dataset_info_dict.get('grouping', {})
        grouping_fields = grouping.get('fields', [])
        
        all_data = []
        
        if grouping_fields:
            # 有分组，加载所有分组文件
            group_field = grouping_fields[0]
            group_values = grouping.get('values', {}).get(group_field, [])
            
            for gv in group_values:
                file_path = os.path.join(dataset_dir, f"{group_field}{gv}_with_schema.json")
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        all_data.extend(data)
        else:
            # 无分组，加载单个文件
            file_path = os.path.join(dataset_dir, f"{self.dataset_name}_with_schema.json")
            with open(file_path, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
        
        print(f"加载预处理数据: {len(all_data)} 条")
        return all_data
    
    def _prepare_prompts(self, data_items: list, config_type: str,
                        prompt_builder,
                        rag_retriever, keyword_searcher) -> list:
        """准备prompts"""
        print(f"  准备prompts ({config_type})...")
        
        prompts = []
        
        for item in data_items:
            question = item['question']
            schema = item['schema']
            
            # 获取RAG context
            rag_context = None
            if config_type in ['rag', 'full'] and rag_retriever:
                retrieved_docs = rag_retriever.retrieve(question)
                rag_context = rag_retriever.format_context(retrieved_docs)
            
            # 获取Keyword context
            keyword_context = None
            if config_type in ['keyword', 'full'] and keyword_searcher:
                retrieved_docs = keyword_searcher.search(question)
                keyword_context = keyword_searcher.format_context(retrieved_docs)
            
            # 构建prompt
            prompt = prompt_builder.build_prompt(
                question=question,
                schema=schema,
                config_type=config_type,
                rag_context=rag_context,
                keyword_context=keyword_context
            )
            prompts.append(prompt)
        
        return prompts
    
    def _run_evaluation_only(self) -> list:
        """
        单独评估已有的预测结果文件
        
        Returns:
            评估结果列表
        """
        from src.datasets.processing import DataLoaderFactory
        from src.evaluation.evaluator import Evaluator

        print("\n" + "="*80)
        print("步骤: 评估已有预测结果")
        print("="*80 + "\n")
        
        # 初始化评估器
        # 使用数据集对应的数据库配置
        dataset_db_config = self._get_dataset_db_config()
        evaluator = Evaluator(
            db_config=dataset_db_config,
            eval_config=self.eval_config
        )
        
        # 获取数据集元信息（工厂创建，兼容多数据集）
        loader_class_name = self.dataset_info_dict['loader_class']
        data_loader = DataLoaderFactory.create(loader_class_name, self.dataset_info_dict)
        dataset_info = data_loader.get_dataset_info()
        
        # 获取预测结果目录
        predictions_dir = self.eval_config['results']['predictions_dir']
        eval_dir = self.eval_config['results']['evaluations_dir']
        
        all_eval_results = []
        
        # 遍历所有模型和配置
        for model_name in self.model_names:
            model_dir = os.path.join(predictions_dir, model_name)
            if not os.path.exists(model_dir):
                print(f"警告: 模型 {model_name} 的预测结果目录不存在: {model_dir}")
                continue
            
            # 遍历所有配置类型
            for config_type in self.config_types:
                config_dir = os.path.join(model_dir, config_type)
                prediction_file = os.path.join(config_dir, f"{model_name}_{config_type}.json")
                
                if not os.path.exists(prediction_file):
                    print(f"警告: 预测结果文件不存在: {prediction_file}")
                    continue
                
                print(f"\n评估: {model_name} - {config_type}")
                print(f"  加载预测结果: {prediction_file}")
                
                # 加载预测结果
                try:
                    with open(prediction_file, 'r', encoding='utf-8') as f:
                        predictions = json.load(f)
                    
                    if not predictions:
                        print(f"  警告: 预测结果文件为空")
                        continue
                    
                    # 运行评估
                    eval_result = evaluator.evaluate(
                        predictions=predictions,
                        dataset_info=dataset_info,
                        model_name=model_name,
                        config_type=config_type
                    )
                    
                    # 保存评估结果
                    evaluator.save_evaluation(eval_result, eval_dir)
                    all_eval_results.append(eval_result)
                    
                except Exception as e:
                    print(f"  错误: 评估失败 - {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        if not all_eval_results:
            print("\n警告: 没有找到任何可评估的预测结果文件")
        
        return all_eval_results
    
    def _generate_final_report(self, all_eval_results: list):
        """生成最终报告"""
        from src.datasets.processing import DataLoaderFactory
        from src.evaluation.report_generator import ReportGenerator

        print("\n" + "="*80)
        print("步骤 4: 生成最终报告")
        print("="*80 + "\n")
        
        # 获取数据集元信息（工厂创建，兼容多数据集）
        loader_class_name = self.dataset_info_dict['loader_class']
        data_loader = DataLoaderFactory.create(loader_class_name, self.dataset_info_dict)
        dataset_info = data_loader.get_dataset_info()
        
        # 生成报告
        report_gen = ReportGenerator(dataset_info)
        report_text = report_gen.generate_report(all_eval_results)
        
        # 打印报告
        print(report_text)
        
        # 保存汇总结果
        summary_file = self.eval_config['results']['summary_file']
        report_gen.save_summary(all_eval_results, summary_file)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Spatial Text2SQL 推理评估框架')
    
    # 流程控制
    parser.add_argument('--preprocess', action='store_true', help='运行数据预处理')
    parser.add_argument('--build-rag', action='store_true', help='构建RAG索引')
    parser.add_argument('--inference', action='store_true', help='运行推理')
    parser.add_argument('--evaluate', action='store_true', help='运行评估')
    
    # 配置选项
    parser.add_argument('--config-dir', type=str, default='config', help='配置文件目录')
    parser.add_argument('--dataset', type=str, help='数据集名称')
    parser.add_argument('--models', nargs='+', help='模型列表')
    parser.add_argument('--configs', nargs='+', choices=['base', 'rag', 'keyword', 'full'], 
                       help='配置类型列表')
    
    args = parser.parse_args()
    
    # 如果没有指定任何操作，显示帮助
    if not (args.preprocess or args.build_rag or args.inference or args.evaluate):
        parser.print_help()
        sys.exit(0)
    
    # 运行主流程
    pipeline = MainPipeline(args)
    pipeline.run()


if __name__ == '__main__':
    main()

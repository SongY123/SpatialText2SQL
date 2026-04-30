# Prompt Enhancements

增强版 prompt 模板统一放在这里。当前三个 dataset-specific enhancement
共享同一个模板：

- `prompts/prompt_enhancements/text2sql_prompt_enhanced.txt`

各数据集 adapter 仍保留，用来控制是否启用 sample/content information
和 dataset context provider，但共享模板只渲染以下输入段落：

- `## Database Schema`
- `## Content Information`
- `## User Question`

模板不渲染 grounding hints、RAG context、keyword context 或 schema semantics，
也不加入 dataset-specific 规则或 PostGIS 函数级规则。
输出格式要求模型把最终 SQL 包在三重反引号 SQL code fence 中。

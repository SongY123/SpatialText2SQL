"""Prompt builder backed by a standalone template file."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .prompt_enhancements.registry import PromptEnhancementRegistry
from .schema_compactor import DEFAULT_PROJECT_ROOT, SchemaCompactor
from .sample_data_provider import PostgresSampleDataProvider


class PromptBuilder:
    """Build prompts from a shared text template plus structured context."""

    def __init__(self, config: Dict):
        self.config = config
        project_root = config.get("project_root")
        self.project_root = Path(project_root).resolve() if project_root else DEFAULT_PROJECT_ROOT
        template_path = config.get("prompt_template_path")
        self.template_path = (
            Path(template_path).resolve()
            if template_path
            else self.project_root / "prompts" / "text2sql_prompt.txt"
        )
        self.ablation_configs = config.get("ablation_configs", {})
        self.prompt_styles = config.get("prompt_styles", {})
        self._template_cache: Dict[str, str] = {}
        self.schema_compactor = SchemaCompactor(project_root=self.project_root)
        self.sample_data_provider = config.get("sample_data_provider") or PostgresSampleDataProvider(
            project_root=self.project_root,
            db_config_path=config.get("db_config_path"),
        )
        self.prompt_enhancement_registry = (
            config.get("prompt_enhancement_registry")
            or PromptEnhancementRegistry(self.project_root)
        )
    
    def build_prompt(
        self,
        question: str,
        schema: str,
        config_type: str = 'base',
        rag_context: Optional[str] = None,
        keyword_context: Optional[str] = None,
        dataset_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        metadata = metadata or {}
        config_spec = self._resolve_ablation_config(config_type)
        prompt_style = str(config_spec.get("prompt_style") or "default")
        style_spec = self._resolve_prompt_style(prompt_style, dataset_name)
        template_text = self._load_template_text(prompt_style, style_spec)
        compact_schema = self.schema_compactor.compact_schema(
            schema=schema,
            question=question,
            dataset_name=dataset_name,
            metadata=metadata,
        )
        sample_data_block = ""
        if style_spec.get("include_sample_data", True):
            sample_data_block = self.sample_data_provider.build_sample_data(
                dataset_name=dataset_name or "",
                metadata=metadata,
                compact_schema=compact_schema,
            )
        grounding_block = self._build_grounding_block(
            dataset_name=dataset_name,
            metadata=metadata,
            style_spec=style_spec,
        )
        schema_semantics_block = self._build_schema_semantics_block(
            dataset_name=dataset_name,
            metadata=metadata,
            style_spec=style_spec,
            compact_schema=compact_schema,
        )
        placeholders = {
            "schema_block": compact_schema.strip(),
            "schema_semantics_block": schema_semantics_block.strip(),
            "sample_data_block": sample_data_block.strip(),
            "content_information_block": sample_data_block.strip(),
            "rag_block": self._stringify_value(
                rag_context if config_spec.get("use_rag") else None,
            ),
            "keyword_block": self._stringify_value(
                keyword_context if config_spec.get("use_keyword") else None,
            ),
            "grounding_block": grounding_block.strip(),
            "question_block": (question or "").strip(),
        }
        return self._render_template(template_text, placeholders)

    @staticmethod
    def _stringify_value(value: Any) -> str:
        return str(value).strip() if value not in (None, "") else ""

    def _render_template(self, template_text: str, placeholders: Dict[str, str]) -> str:
        rendered = template_text
        for key, value in placeholders.items():
            rendered = rendered.replace(f"{{{{{key}}}}}", value.strip())
        rendered = re.sub(r"\{\{[^{}]+\}\}", "", rendered)
        return self._cleanup_rendered_prompt(rendered)

    def _resolve_ablation_config(self, config_type: str) -> Dict[str, Any]:
        config = self.ablation_configs.get(config_type)
        if config is not None:
            return config
        return {
            "use_rag": config_type in ["rag", "full"],
            "use_keyword": config_type in ["keyword", "full"],
            "prompt_style": "default",
        }

    def _resolve_prompt_style(
        self,
        prompt_style: str,
        dataset_name: Optional[str],
    ) -> Dict[str, Any]:
        default_style = self.prompt_styles.get("default", {})
        style_config = self.prompt_styles.get(prompt_style, {})
        merged = dict(default_style)
        merged.update(style_config)
        if merged.get("dataset_specific") and dataset_name:
            merged.update(
                self.prompt_enhancement_registry.resolve_dataset_override(dataset_name)
            )
        return merged

    def _load_template_text(self, prompt_style: str, style_spec: Dict[str, Any]) -> str:
        template_path = style_spec.get("template_path")
        if template_path:
            path = Path(template_path)
            if not path.is_absolute():
                path = (self.project_root / template_path).resolve()
        else:
            path = self.template_path

        cache_key = f"{prompt_style}:{path}"
        if cache_key not in self._template_cache:
            self._template_cache[cache_key] = path.read_text(encoding="utf-8")
        return self._template_cache[cache_key]

    def _build_grounding_block(
        self,
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
        style_spec: Dict[str, Any],
    ) -> str:
        if not style_spec.get("use_dataset_context"):
            return ""
        return self.prompt_enhancement_registry.build_grounding_block(
            dataset_name or "",
            metadata,
        )

    def _build_schema_semantics_block(
        self,
        dataset_name: Optional[str],
        metadata: Dict[str, Any],
        style_spec: Dict[str, Any],
        compact_schema: str,
    ) -> str:
        if not style_spec.get("use_dataset_context"):
            return ""
        build_fn = getattr(
            self.prompt_enhancement_registry,
            "build_schema_semantics_block",
            None,
        )
        if build_fn is None:
            return ""
        return build_fn(
            dataset_name or "",
            metadata,
            compact_schema,
        )

    @staticmethod
    def _cleanup_rendered_prompt(rendered: str) -> str:
        preamble, sections = PromptBuilder._split_prompt_sections(rendered)
        cleaned_sections: List[Tuple[str, List[str]]] = []

        for header, body_lines in sections:
            cleaned_body = [
                line.rstrip()
                for line in body_lines
                if not PromptBuilder._is_empty_metadata_line(line)
            ]
            cleaned_body = PromptBuilder._trim_blank_lines(cleaned_body)
            if cleaned_body:
                cleaned_sections.append((header, cleaned_body))

        prompt_lines = PromptBuilder._trim_blank_lines([line.rstrip() for line in preamble])
        for header, body in cleaned_sections:
            if prompt_lines:
                prompt_lines.append("")
            prompt_lines.append(header)
            prompt_lines.extend(body)

        return "\n".join(PromptBuilder._trim_blank_lines(prompt_lines))

    @staticmethod
    def _split_prompt_sections(rendered: str) -> Tuple[List[str], List[Tuple[str, List[str]]]]:
        preamble: List[str] = []
        sections: List[Tuple[str, List[str]]] = []
        current_header: Optional[str] = None
        current_body: List[str] = []

        for line in rendered.splitlines():
            if line.startswith("## "):
                if current_header is None:
                    pass
                else:
                    sections.append((current_header, current_body))
                current_header = line.rstrip()
                current_body = []
                continue

            if current_header is None:
                preamble.append(line.rstrip())
            else:
                current_body.append(line.rstrip())

        if current_header is not None:
            sections.append((current_header, current_body))

        return preamble, sections

    @staticmethod
    def _is_empty_metadata_line(line: str) -> bool:
        return bool(re.match(r"^\s*-\s+[A-Za-z0-9_ ]+:\s*$", line))

    @staticmethod
    def _trim_blank_lines(lines: List[str]) -> List[str]:
        start = 0
        end = len(lines)
        while start < end and not lines[start].strip():
            start += 1
        while end > start and not lines[end - 1].strip():
            end -= 1
        trimmed = lines[start:end]
        compacted: List[str] = []
        previous_blank = False
        for line in trimmed:
            is_blank = not line.strip()
            if is_blank and previous_blank:
                continue
            compacted.append(line)
            previous_blank = is_blank
        return compacted
    
    def build_batch_prompts(
        self,
        questions: list,
        schema: str,
        config_type: str = 'base',
        rag_contexts: Optional[list] = None,
        keyword_contexts: Optional[list] = None,
        dataset_name: Optional[str] = None,
        metadatas: Optional[list] = None,
    ) -> list:
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
            metadata = metadatas[i] if metadatas and i < len(metadatas) else None
            
            prompt = self.build_prompt(
                question=question,
                schema=schema,
                config_type=config_type,
                rag_context=rag_ctx,
                keyword_context=kw_ctx,
                dataset_name=dataset_name,
                metadata=metadata,
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
            'rag': 'Question + Schema + Retrieved Context',
            'keyword': 'Question + Schema + Keyword Context',
            'full': 'Question + Schema + Retrieved Context + Keyword Context'
        }
        return descriptions.get(config_type, 'Unknown')

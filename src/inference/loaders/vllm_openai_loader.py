"""基于 OpenAI 兼容接口的 vLLM 模型加载器。"""
from typing import Any, Dict

from src.inference.base import BaseModelLoader
from src.inference.sql_utils import extract_sql_from_text

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


class VllmOpenAILoader(BaseModelLoader):
    """通过 OpenAI 兼容接口调用 vLLM 服务。"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_base = config.get("api_base", "").rstrip("/")
        self.remote_model = config.get("model", "")
        self.api_key = config.get("api_key", "")
        self.timeout = config.get("timeout", 600)
        self.generation_config = config.get("generation_config", {})
        self.client = None

    def load_model(self, model_path: str = None, **kwargs):
        """
        初始化 OpenAI 兼容客户端。

        Args:
            model_path: 保留基类签名兼容，vLLM 场景下不使用
            **kwargs: 额外参数，暂不使用
        """
        if OpenAI is None:
            raise ImportError("openai 未安装，请先安装 openai 依赖")

        if not self.api_base:
            raise ValueError("vLLM 配置缺少 api_base")

        if not self.remote_model:
            raise ValueError("vLLM 配置缺少 model")

        self.client = OpenAI(
            api_key=self.api_key or "EMPTY",
            base_url=self.api_base,
            timeout=self.timeout,
        )

        print(f"\n连接 vLLM 服务: {self.api_base}")
        print(f"✓ 使用远端模型: {self.remote_model}")

    def generate_sql(self, prompt: str, **gen_kwargs) -> str:
        """
        通过 vLLM 生成 SQL。

        Args:
            prompt: 输入提示词
            **gen_kwargs: 生成参数，会覆盖默认配置

        Returns:
            提取后的 SQL 语句
        """
        if self.client is None:
            raise RuntimeError("vLLM 客户端未初始化，请先调用 load_model()")

        gen_config = {**self.generation_config, **gen_kwargs}

        extra_body = {}
        if "repetition_penalty" in gen_config:
            extra_body["repetition_penalty"] = gen_config["repetition_penalty"]

        response = self.client.chat.completions.create(
            model=self.remote_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=gen_config.get("max_new_tokens", 512),
            temperature=gen_config.get("temperature", 0.1),
            top_p=gen_config.get("top_p", 0.9),
            extra_body=extra_body or None,
        )

        generated_text = self._extract_content(response)
        return extract_sql_from_text(generated_text, prompt="")

    def _extract_content(self, response) -> str:
        """兼容不同 content 结构，抽取文本内容。"""
        if not response.choices:
            return ""

        content = response.choices[0].message.content
        if isinstance(content, str):
            return content

        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
            return "".join(parts)

        return str(content or "")

    def unload(self):
        """释放客户端引用。"""
        self.client = None

    def get_model_info(self) -> Dict[str, Any]:
        """返回模型元信息。"""
        return {
            "api_base": self.api_base,
            "model": self.remote_model,
            "loaded": self.client is not None,
            "backend": "vllm",
        }

"""基于 OpenAI 兼容接口的 vLLM 模型加载器。"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

from src.inference.base import BaseModelLoader

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


RETRYABLE_ERROR_MARKERS = (
    "502",
    "503",
    "504",
    "bad gateway",
    "gateway timeout",
    "connection refused",
    "connection reset",
    "connection aborted",
    "timed out",
    "timeout",
    "temporarily unavailable",
)
NETWORK_ERROR_MARKERS = (
    "connection refused",
    "connection reset",
    "connection aborted",
    "timed out",
    "timeout",
    "no route to host",
    "network is unreachable",
    "temporarily unavailable",
)


class VllmSampleSkippedError(RuntimeError):
    """vLLM 样本在可恢复错误持续存在时被跳过。"""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        attempts: Optional[int] = None,
        elapsed_sec: Optional[float] = None,
        last_error: Optional[BaseException] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.attempts = attempts
        self.elapsed_sec = elapsed_sec
        self.last_error = last_error
        self.details = dict(details or {})
        if attempts is not None:
            self.details.setdefault("attempts", attempts)
        if elapsed_sec is not None:
            self.details.setdefault("elapsed_sec", elapsed_sec)
        if last_error is not None:
            self.details.setdefault("last_error_type", type(last_error).__name__)
            self.details.setdefault("last_error", str(last_error))


class VllmOpenAILoader(BaseModelLoader):
    """通过 OpenAI 兼容接口调用 vLLM 服务。"""

    THINKING_MODEL_NAME = "qwen3-235b-a22b-thinking"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_base = config.get("api_base", "").rstrip("/")
        self.remote_model = config.get("model", "")
        self.logical_model_name = config.get("logical_model_name", "")
        self.api_key = config.get("api_key", "")
        self.timeout = config.get("timeout", 600)
        self.generation_config = config.get("generation_config", {})
        self.connect_timeout = config.get("connect_timeout", 30)
        self.read_timeout = config.get("read_timeout", self.timeout)
        self.write_timeout = config.get("write_timeout", 60)
        self.request_wall_timeout = config.get("request_wall_timeout")
        self.max_retries = int(config.get("max_retries", 0))
        self.retry_backoff_sec = float(config.get("retry_backoff_sec", 1.0))
        self.max_retry_backoff_sec = float(config.get("max_retry_backoff_sec", 8.0))
        self.retry_forever_on_network_error = bool(config.get("retry_forever_on_network_error", False))
        self.network_recovery_timeout = config.get("network_recovery_timeout")
        self.sample_skip_timeout = config.get("sample_skip_timeout", 180.0)
        self.client = None

    THINK_OPEN = "<think>"
    THINK_CLOSE = "</think>"
    THINK_OPEN_REDACTED = "<redacted_thinking>"
    THINK_CLOSE_REDACTED = "</redacted_thinking>"

    def _uses_structured_thinking_output(self) -> bool:
        return self.logical_model_name == self.THINKING_MODEL_NAME

    def _prepare_prompt_for_model(self, prompt: str) -> str:
        return prompt

    @staticmethod
    def _content_to_text(content: Any) -> str:
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

    @classmethod
    def _extract_message_parts(cls, message: Any) -> Dict[str, str]:
        reasoning = (
            getattr(message, "reasoning", None)
            or getattr(message, "reasoning_content", None)
            or getattr(message, "thinking", None)
            or ""
        )
        if not isinstance(reasoning, str):
            reasoning = str(reasoning or "")

        content = cls._content_to_text(getattr(message, "content", None))
        return {"reasoning": reasoning, "content": content}

    @classmethod
    def _find_earliest_open(cls, content: str):
        hits = []
        for open_tag, close_tag in (
            (cls.THINK_OPEN, cls.THINK_CLOSE),
            (cls.THINK_OPEN_REDACTED, cls.THINK_CLOSE_REDACTED),
        ):
            idx = content.find(open_tag)
            if idx != -1:
                hits.append((idx, open_tag, close_tag))
        if not hits:
            return None
        hits.sort(key=lambda x: x[0])
        return hits[0]

    @classmethod
    def _find_earliest_close(cls, content: str):
        hits = []
        for open_tag, close_tag in (
            (cls.THINK_OPEN, cls.THINK_CLOSE),
            (cls.THINK_OPEN_REDACTED, cls.THINK_CLOSE_REDACTED),
        ):
            idx = content.find(close_tag)
            if idx != -1:
                hits.append((idx, open_tag, close_tag))
        if not hits:
            return None
        hits.sort(key=lambda x: x[0])
        return hits[0]

    @classmethod
    def _split_think_and_answer(cls, content: str) -> Dict[str, Any]:
        text = content or ""
        open_hit = cls._find_earliest_open(text)

        if open_hit is None:
            close_hit = cls._find_earliest_close(text)
            if close_hit is not None:
                close_idx, _open_tag, close_tag = close_hit
                think_body = text[:close_idx].strip()
                answer = text[close_idx + len(close_tag):].strip()
                return {
                    "think": think_body or None,
                    "answer": answer,
                    "think_incomplete": False,
                }
            return {"think": None, "answer": text, "think_incomplete": False}

        open_idx, open_tag, close_tag = open_hit

        before_open = text[:open_idx].strip()
        after_open = text[open_idx + len(open_tag):]
        close_idx = after_open.find(close_tag)

        if close_idx == -1:
            return {
                "think": after_open,
                "answer": before_open,
                "think_incomplete": True,
            }

        think_body = after_open[:close_idx].strip()
        answer = after_open[close_idx + len(close_tag):].strip()
        if before_open:
            answer = f"{before_open}\n\n{answer}".strip() if answer else before_open
        return {
            "think": think_body or None,
            "answer": answer,
            "think_incomplete": False,
        }

    @classmethod
    def _extract_final_answer_from_parts(cls, parts: Dict[str, str]) -> str:
        reasoning = (parts.get("reasoning") or "").strip()
        content = (parts.get("content") or "").strip()
        if reasoning:
            return content
        split = cls._split_think_and_answer(content)
        return split["answer"].strip()

    def _create_client(self):
        """创建 OpenAI 兼容客户端。"""
        if OpenAI is None:
            raise ImportError("openai 未安装，请先安装 openai 依赖")

        if not self.api_base:
            raise ValueError("vLLM 配置缺少 api_base")

        if not self.remote_model:
            raise ValueError("vLLM 配置缺少 model")

        return OpenAI(
            api_key=self.api_key or "EMPTY",
            base_url=self.api_base,
            timeout=self.timeout,
            max_retries=0,
        )

    def _rebuild_client(self):
        """重建客户端，避免网络错误后连接状态异常。"""
        self.client = self._create_client()
        return self.client

    def load_model(self, model_path: str = None, **kwargs):
        """
        初始化 OpenAI 兼容客户端。

        Args:
            model_path: 保留基类签名兼容，vLLM 场景下不使用
            **kwargs: 额外参数，暂不使用
        """
        self.client = self._create_client()

        print(f"\n连接 vLLM 服务: {self.api_base}")
        print(f"✓ 使用远端模型: {self.remote_model}")

    def _build_request_kwargs(self, prompt: str, gen_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """构建 chat.completions.create 的参数。"""
        gen_config = {**self.generation_config, **gen_kwargs}
        do_sample = bool(gen_config.get("do_sample", False))
        temperature = gen_config.get("temperature", 0.3 if do_sample else 0.0)
        top_p = gen_config.get("top_p", 0.8 if do_sample else 1.0)
        enable_thinking = bool(gen_config.get("enable_thinking", False))
        if self._uses_structured_thinking_output():
            enable_thinking = True

        extra_body: Dict[str, Any] = {
            "chat_template_kwargs": {
                "enable_thinking": enable_thinking,
            }
        }
        if "repetition_penalty" in gen_config:
            extra_body["repetition_penalty"] = gen_config["repetition_penalty"]

        request_kwargs: Dict[str, Any] = {
            "model": self.remote_model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": gen_config.get("max_new_tokens", 512),
            "temperature": temperature,
            "top_p": top_p,
            "extra_body": extra_body,
        }
        if "stop" in gen_config:
            request_kwargs["stop"] = gen_config["stop"]
        return request_kwargs

    def _is_retryable_error(self, exc: BaseException) -> bool:
        message = str(exc).lower()
        return any(marker in message for marker in RETRYABLE_ERROR_MARKERS)

    def _is_network_error(self, exc: BaseException) -> bool:
        message = str(exc).lower()
        return any(marker in message for marker in NETWORK_ERROR_MARKERS)

    def _get_retry_stop_reason(self, recovery_start: Optional[float], attempt: int) -> Optional[str]:
        """根据当前重试状态判断是否该停止。"""
        if recovery_start is not None:
            timeout_limit = self.network_recovery_timeout
            if timeout_limit is None:
                timeout_limit = self.sample_skip_timeout
            if timeout_limit is not None and timeout_limit >= 0:
                if time.monotonic() - recovery_start >= float(timeout_limit):
                    return "network_recovery_timeout"
            if self.retry_forever_on_network_error:
                return None

        if attempt - 1 >= self.max_retries:
            return "max_retries"
        return None

    def _run_subprocess_request(self, request_kwargs: Dict[str, Any]) -> Dict[str, str]:
        """在独立子进程中执行请求，避免主进程长期阻塞。"""
        runner_path = Path(__file__).resolve().parents[1] / "vllm_subprocess_runner.py"
        payload = {
            "api_base": self.api_base,
            "api_key": self.api_key or "EMPTY",
            "timeout": self.timeout,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
            "write_timeout": self.write_timeout,
            "request_kwargs": request_kwargs,
        }

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as fp:
            json.dump(payload, fp, ensure_ascii=False)
            payload_path = fp.name

        try:
            completed = subprocess.run(
                [sys.executable, str(runner_path), payload_path],
                capture_output=True,
                text=True,
                timeout=float(self.request_wall_timeout),
            )
        except subprocess.TimeoutExpired as exc:
            self._rebuild_client()
            raise VllmSampleSkippedError(
                f"vLLM 请求墙钟超时（>{self.request_wall_timeout}s），已跳过当前样本。",
                reason_code="wall_clock_timeout",
                elapsed_sec=float(self.request_wall_timeout),
                last_error=exc,
            ) from exc
        finally:
            try:
                Path(payload_path).unlink(missing_ok=True)
            except Exception:
                pass

        raw_output = (completed.stdout or "").strip()
        if not raw_output:
            raise RuntimeError((completed.stderr or "empty subprocess output").strip())
        body = json.loads(raw_output)
        if not body.get("ok"):
            raise RuntimeError(body.get("err") or body.get("err_type") or "subprocess request failed")
        return {
            "reasoning": body.get("reasoning", "") or "",
            "content": body.get("content", "") or "",
        }

    def _invoke_completion_isolated(self, request_kwargs: Dict[str, Any]) -> Dict[str, str]:
        """执行一次请求；配置了墙钟超时则走子进程保护。"""
        if self.client is None:
            raise RuntimeError("vLLM 客户端未初始化，请先调用 load_model()")

        if self.request_wall_timeout and float(self.request_wall_timeout) > 0:
            return self._run_subprocess_request(request_kwargs)

        response = self.client.chat.completions.create(**request_kwargs)
        if not response.choices:
            return {"reasoning": "", "content": ""}
        return self._extract_message_parts(response.choices[0].message)

    def _create_chat_completion_with_retry(self, request_kwargs: Dict[str, Any]) -> Dict[str, str]:
        """在可恢复错误上进行重试，并在超时后跳过当前样本。"""
        attempt = 0
        recovery_start: Optional[float] = None

        while True:
            attempt += 1
            try:
                return self._invoke_completion_isolated(request_kwargs)
            except VllmSampleSkippedError:
                raise
            except Exception as exc:
                if not self._is_retryable_error(exc):
                    raise

                is_network_error = self._is_network_error(exc)
                if is_network_error and recovery_start is None:
                    recovery_start = time.monotonic()
                if is_network_error:
                    self._rebuild_client()

                stop_reason = None
                if is_network_error:
                    stop_reason = self._get_retry_stop_reason(recovery_start, attempt)
                elif attempt - 1 >= self.max_retries:
                    stop_reason = "max_retries"

                if stop_reason:
                    elapsed_sec = None
                    if recovery_start is not None:
                        elapsed_sec = time.monotonic() - recovery_start
                    elif stop_reason == "max_retries":
                        elapsed_sec = 0.0
                    raise VllmSampleSkippedError(
                        f"vLLM 网络恢复等待超过 {elapsed_sec or 0.0:.1f}s，已跳过当前样本。",
                        reason_code=stop_reason,
                        attempts=attempt,
                        elapsed_sec=elapsed_sec,
                        last_error=exc,
                    ) from exc

                backoff = min(
                    self.retry_backoff_sec * max(1, 2 ** (attempt - 1)),
                    max(self.retry_backoff_sec, self.max_retry_backoff_sec),
                )
                time.sleep(max(0.0, backoff))

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

        prompt = self._prepare_prompt_for_model(prompt)
        request_kwargs = self._build_request_kwargs(prompt, gen_kwargs)
        parts = self._create_chat_completion_with_retry(request_kwargs)
        return self._extract_final_answer_from_parts(parts)

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

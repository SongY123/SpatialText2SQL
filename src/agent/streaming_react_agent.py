from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

from agentscope.agent import ReActAgent


class StreamingReActAgent(ReActAgent):
    """ReActAgent with optional text-stream callback from print() calls."""

    _stream_text_callback: Optional[Callable[[str], Any]] = None

    def set_stream_text_callback(self, callback: Optional[Callable[[str], Any]]) -> None:
        self._stream_text_callback = callback

    @staticmethod
    def _extract_text(msg: Any) -> str:
        if msg is None:
            return ""

        content = getattr(msg, "content", msg)
        if isinstance(content, str):
            return content
        if isinstance(content, dict):
            if content.get("type") == "text":
                return str(content.get("text", ""))
            return ""
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(str(block.get("text", "")))
            return "".join(parts)
        return ""

    async def print(self, *args: Any, **kwargs: Any) -> Any:
        callback = self._stream_text_callback
        if callback is not None:
            msg_obj = kwargs.get("msg")
            if msg_obj is None and args:
                msg_obj = args[0]
            text = self._extract_text(msg_obj)
            if text:
                try:
                    maybe = callback(text)
                    if asyncio.iscoroutine(maybe):
                        await maybe
                except Exception:
                    pass

        maybe = super().print(*args, **kwargs)
        if asyncio.iscoroutine(maybe):
            return await maybe
        return maybe

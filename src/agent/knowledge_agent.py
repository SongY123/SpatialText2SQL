from __future__ import annotations

from typing import Optional

from agentscope.agent import ReActAgent
from agentscope.formatter import FormatterBase
from agentscope.model import ChatModelBase
from agentscope.tool import Toolkit

from src.agent.prompts import KNOWLEDGE_PROMPT


class KnowledgeAgent(ReActAgent):
    def __init__(
        self,
        model: ChatModelBase,
        formatter: FormatterBase,
        toolkit: Optional[Toolkit] = None,
        name: str = "knowledge_agent",
        max_iters: int = 8,
    ) -> None:
        super().__init__(
            name=name,
            sys_prompt=KNOWLEDGE_PROMPT,
            model=model,
            formatter=formatter,
            toolkit=toolkit,
            max_iters=max_iters,
            parallel_tool_calls=True,
        )

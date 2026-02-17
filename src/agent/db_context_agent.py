from __future__ import annotations

from typing import Optional

from agentscope.agent import ReActAgent
from agentscope.formatter import FormatterBase
from agentscope.model import ChatModelBase
from agentscope.tool import Toolkit

from agent.prompts import DB_CONTEXT_PROMPT


class DBContextAgent(ReActAgent):
    def __init__(
        self,
        model: ChatModelBase,
        formatter: FormatterBase,
        toolkit: Optional[Toolkit] = None,
        name: str = "db_context_agent",
        max_iters: int = 8,
    ) -> None:
        super().__init__(
            name=name,
            sys_prompt=DB_CONTEXT_PROMPT,
            model=model,
            formatter=formatter,
            toolkit=toolkit,
            max_iters=max_iters,
            parallel_tool_calls=True,
        )

from __future__ import annotations

from typing import Optional

from agentscope.formatter import FormatterBase
from agentscope.model import ChatModelBase
from agentscope.tool import Toolkit

from agent.prompts import SQL_BUILDER_PROMPT
from agent.streaming_react_agent import StreamingReActAgent


class SQLBuilderAgent(StreamingReActAgent):
    def __init__(
        self,
        model: ChatModelBase,
        formatter: FormatterBase,
        toolkit: Optional[Toolkit] = None,
        name: str = "sql_builder_agent",
        max_iters: int = 10,
    ) -> None:
        super().__init__(
            name=name,
            sys_prompt=SQL_BUILDER_PROMPT,
            model=model,
            formatter=formatter,
            toolkit=toolkit,
            max_iters=max_iters,
            parallel_tool_calls=False,
        )

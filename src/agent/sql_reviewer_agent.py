from __future__ import annotations

from typing import Optional

from agentscope.agent import ReActAgent
from agentscope.formatter import FormatterBase
from agentscope.model import ChatModelBase
from agentscope.tool import Toolkit

from src.agent.prompts import SQL_REVIEWER_PROMPT


class SQLReviewerAgent(ReActAgent):
    def __init__(
        self,
        model: ChatModelBase,
        formatter: FormatterBase,
        toolkit: Optional[Toolkit] = None,
        name: str = "sql_reviewer_agent",
        max_iters: int = 6,
    ) -> None:
        super().__init__(
            name=name,
            sys_prompt=SQL_REVIEWER_PROMPT,
            model=model,
            formatter=formatter,
            toolkit=toolkit,
            max_iters=max_iters,
            parallel_tool_calls=False,
        )

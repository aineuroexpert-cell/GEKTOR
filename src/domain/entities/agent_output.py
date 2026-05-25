from typing import Any, Optional
from pydantic import BaseModel, Field


class AgentOutput(BaseModel):
    """
    Standard structured output for Gerald-SuperBrain.
    Model MUST strictly follow this schema.
    """

    thought: str = Field(
        ..., description="Мысли агента на русском языке, объясняющие логику действий"
    )
    tool_name: str = Field(
        ...,
        description="Название вызываемого инструмента (например, read_file, search, execute_python_code, final_answer)",
    )
    tool_args: dict[str, Any] = Field(
        default_factory=dict, description="Аргументы для инструмента"
    )


class ToolResult(BaseModel):
    """Result of a tool execution returned to the LLM."""

    success: bool
    output: str
    error: Optional[str] = None

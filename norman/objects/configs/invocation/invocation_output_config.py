from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Field

from norman.objects.configs.invocation.consume_mode import ConsumeMode


class InvocationOutputConfig(BaseModel):
    id: Optional[str] = Field(None, description="Optional output ID. If not provided, a new UUID will be generated")
    display_title: str = Field(description="Human-friendly name for the invocation output")
    data: Union[bytes, str, Path] = Field(description="Dictate output form to the SDK")
    consume_mode: Optional[ConsumeMode] = Field(None, description="Where the data is coming from")

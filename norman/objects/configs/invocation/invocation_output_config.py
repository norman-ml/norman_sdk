from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Field

from norman.objects.configs.invocation.consume_mode import ConsumeMode


class InvocationOutputConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the invocation output")
    display_title: str = Field(description="Human-friendly name for the invocation output")
    data: Union[bytes, str, Path] = Field(description="Dictate output form to the SDK")
    consume_mode: Optional[ConsumeMode] = Field(None, description="Where the data is coming from")

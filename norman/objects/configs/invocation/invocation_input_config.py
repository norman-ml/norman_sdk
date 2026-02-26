from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.inputs.input_source import InputSource
from pydantic import BaseModel, Field


class InvocationInputConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the invocation input")
    display_title: str = Field(description="Human friendly name for the invocation input")
    data: Union[bytes, str, Path] = Field(description="Actual input payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")

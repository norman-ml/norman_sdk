from typing import List, Optional

from pydantic import BaseModel, Field

from norman.objects.configs.invocation.invocation_input_config import InvocationInputConfig
from norman.objects.configs.invocation.invocation_output_config import InvocationOutputConfig


class InvocationConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the invocation")
    model_name: str = Field(description="Name of the model to invoke")
    inputs: List[InvocationInputConfig] = Field(description="List of invocation input configurations")
    outputs: Optional[List[InvocationOutputConfig]] = Field(default=None, description="Mapping of output names to their formats")

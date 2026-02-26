from typing import Optional

from pydantic import BaseModel, Field


class ParameterConfig(BaseModel):
    id: Optional[str] = Field(None, description="Optional parameter ID. If not provided, a new UUID will be generated")
    parameter_name: str = Field(description="Name of the matching argument defined in the model forward function signature")
    data_encoding: str = Field(description="Encoding format expected by the model forward function for this parameter data")

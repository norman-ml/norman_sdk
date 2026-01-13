from typing import Optional

from pydantic import BaseModel, Field


class ParameterConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the parameter")
    model_id: str = Field(default="0", description="Unique identifier of the parameter")
    version_id: str = Field(default="0", description="Unique identifier of the model version")
    signature_id: str = Field(default="0", description="Unique identifier of the signature")
    parameter_name: str = Field(description="Name of the matching argument defined in the model forward function signature")
    data_modality: str = Field(description="Name or identifier describing and categorizing the contents passed to the model parameter")

    channel_encoding: Optional[str] = Field(description="Encoding format expected by the model forward function for this parameter data")
    sample_encoding: Optional[str] = Field(description="Numeric or bit-level representation of each individual sample value")
    tensor_encoding: Optional[str] = Field(description="Tensor data type and representation used internally by the model for this parameter")

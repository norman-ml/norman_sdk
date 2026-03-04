from pydantic import BaseModel, Field


class ParameterConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the parameter")
    model_id: str = Field(default="0", description="Unique identifier of the parameter")
    version_id: str = Field(default="0", description="Unique identifier of the model version")
    signature_id: str = Field(default="0", description="Unique identifier of the signature")
    parameter_name: str = Field(description="Name of the matching argument defined in the model forward function signature")
    data_encoding: str = Field(description="Encoding format expected by the model forward function for this parameter data")

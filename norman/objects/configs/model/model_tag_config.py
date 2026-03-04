from pydantic import BaseModel, Field


class ModelTagConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the tag")
    model_id: str = Field(default="0", description="Unique identifier of the model")
    name: str = Field(description="Tag name")

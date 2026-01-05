from pydantic import BaseModel, Field


class TagConfig(BaseModel):
    model_name: str = Field(description="The Model name to tag")
    name: str = Field(description="Tag name")

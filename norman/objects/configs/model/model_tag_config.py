from typing import Optional

from pydantic import BaseModel, Field


class ModelTagConfig(BaseModel):
    id: Optional[str] = Field(None, description="Optional tag ID. If not provided, a new UUID will be generated")
    name: str = Field(description="Tag name")

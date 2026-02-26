from typing import List, Optional

from pydantic import BaseModel, Field

from norman.objects.configs.model.model_tag_config import ModelTagConfig
from norman.objects.configs.model.model_version_config import ModelVersionConfig


class ModelProjectionConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the model")
    name: str = Field(description="Unique name of the model")
    category: str = Field(description="Category name or identifier for the task the model solves")

    version: ModelVersionConfig = Field(description="The version of the model being acted upon")
    user_tags: Optional[List[ModelTagConfig]] = Field(None, description="Optional list of tags assigned to this model by the uploading user")

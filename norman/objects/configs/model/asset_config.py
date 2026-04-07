from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.assets.asset_name import AssetName
from norman_objects.shared.inputs.input_source import InputSource
from pydantic import BaseModel, Field


class AssetConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the asset")
    model_id: str = Field(default="0", description="Unique identifier of the model")
    version_id: str = Field(default="0", description="Unique identifier of the model version")
    asset_name: AssetName = Field(description="The type of the model asset")
    data: Union[bytes, str, Path] = Field(description="Actual asset payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")

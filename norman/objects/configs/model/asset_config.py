from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.models.asset_type import AssetType
from pydantic import BaseModel, Field


class AssetConfig(BaseModel):
    asset_name: str = Field(description="Human friendly name for the model asset")
    asset_type: AssetType = Field(description="The type of model asset")
    data: Union[bytes, str, Path] = Field(description="Actual asset payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")

from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.models.asset_name import AssetName
from pydantic import BaseModel, Field


class AssetConfig(BaseModel):
    asset_name: AssetName = Field(description="The type of model asset")
    data: Union[bytes, str, Path] = Field(description="Actual asset payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")

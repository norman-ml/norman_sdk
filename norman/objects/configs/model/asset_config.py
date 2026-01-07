from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.inputs.input_source import InputSource
from pydantic import BaseModel, Field


class AssetConfig(BaseModel):
    asset_name: str = Field(description="Human friendly name for the model asset")
    data: Union[bytes, str, Path] = Field(description="Actual asset payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")

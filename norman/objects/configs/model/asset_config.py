from pathlib import Path
from typing import Optional, Union

from norman_objects.shared.inputs.input_source import InputSource
from norman_objects.shared.parameters.data_modality import DataModality
from pydantic import BaseModel, Field


class AssetConfig(BaseModel):
    asset_name: str = Field(description="Human friendly name for the model asset")
    data: Union[bytes, str, Path] = Field(description="Actual asset payload")
    source: Optional[InputSource] = Field(None, description="Where the data is coming from")
    data_modality: Optional[DataModality] = Field(None, description="The data modality")

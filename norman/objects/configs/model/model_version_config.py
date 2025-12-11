from typing import List, Optional, Dict

from norman_objects.shared.models.http_request_type import HttpRequestType
from norman_objects.shared.models.model_hosting_location import ModelHostingLocation
from norman_objects.shared.models.model_type import ModelType
from norman_objects.shared.models.output_format import OutputFormat
from pydantic import BaseModel, Field

from norman.objects.configs.model.asset_config import AssetConfig
from norman.objects.configs.model.signature_config import SignatureConfig


class ModelVersionConfig(BaseModel):
    label: str = Field(description="Human readable version label (e.g., 'v1.0' or 'beta')")
    short_description: str = Field(description="Concise summary describing what the model does")
    long_description: str = Field(description="Detailed explanation of the model, usage, and behavior")

    assets: List[AssetConfig] = Field(description="List of associated model assets for display and execution")
    inputs: List[SignatureConfig] = Field(description="Input signatures defining model inputs and their formats")
    outputs: List[SignatureConfig] = Field(description="Output signatures defining model outputs and their formats")

    cuda_version: Optional[str] = Field(description="")
    python_version: Optional[str] = Field(description="")
    hosting_location: Optional[ModelHostingLocation] = Field(None, description="Hosting location of the model")
    model_type: Optional[ModelType] = Field(None, description="Optional model type or framework")
    request_type: Optional[HttpRequestType] = Field(None, description="Optional HTTP request type used for inference")
    url: Optional[str] = Field(None, description="URL pointing to the model, required for external models")
    output_format: Optional[OutputFormat] = Field(None, description="Optional format in which model outputs are returned")

    http_headers: Optional[Dict[str, str]] = Field(None, description="Optional HTTP headers passed to external models when called over HTTP")

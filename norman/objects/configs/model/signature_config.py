from typing import Optional

from norman_objects.shared.encoding.container_encoding import ContainerEncoding
from norman_objects.shared.modality.container_modality import ContainerModality
from norman_objects.shared.model_signatures.http_location import HttpLocation
from norman_objects.shared.model_signatures.receive_format import ReceiveFormat
from pydantic import BaseModel, Field

from norman.objects.configs.model.parameter_config import ParameterConfig


class SignatureConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the signature")
    model_id: str = Field(default="0", description="Unique identifier of the model")
    version_id: str = Field(default="0", description="Unique identifier of the model version")
    display_title: str = Field(description="Human readable name for the model signature")
    container_modality: ContainerModality = Field(description="Name or identifier describing and categorizing the contents passed to the model signature")
    data_domain: str = Field(description="Subject area for the data passed to the model signature")
    receive_format: ReceiveFormat = Field(description="How the model expects to receive inputs for the model signature")

    parameters: list[ParameterConfig]

    container_encoding: Optional[ContainerEncoding] = Field(None, description="Encoding format in which the model expects to receive inputs for the model signature")
    http_location: Optional[HttpLocation] = Field(None, description="Optional HTTP location of the value when transmitted to externally histed models over HTTP")
    hidden: Optional[bool] = Field(None, description="If true, this field is hidden from users in graphical clients")
    default_value: Optional[str] = Field(None, description="Fallback value to use when no explicit input is given by the user")

    arguments: Optional[dict[str, str]] = Field(None, description="Additional arguments to control file encoding and serialization")

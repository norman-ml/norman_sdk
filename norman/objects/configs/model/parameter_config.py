from typing import Optional

from norman_objects.shared.encoding.channel_encoding import ChannelEncoding
from norman_objects.shared.encoding.sample_encoding import SampleEncoding
from norman_objects.shared.encoding.tensor_encoding import TensorEncoding
from norman_objects.shared.modality.channel_modality import ChannelModality
from pydantic import BaseModel, Field


class ParameterConfig(BaseModel):
    id: str = Field(default="0", description="Unique identifier of the parameter")
    model_id: str = Field(default="0", description="Unique identifier of the parameter")
    version_id: str = Field(default="0", description="Unique identifier of the model version")
    signature_id: str = Field(default="0", description="Unique identifier of the signature")
    name: str = Field(description="Name of the matching argument defined in the model forward function signature")
    channel_modality: ChannelModality = Field(description="Name or identifier describing and categorizing the contents passed to the model parameter")

    channel_encoding: Optional[ChannelEncoding] = Field(description="Encoding format expected by the model forward function for this parameter data")
    sample_encoding: Optional[SampleEncoding] = Field(description="Numeric or bit-level representation of each individual sample value")
    tensor_encoding: Optional[TensorEncoding] = Field(description="Tensor data type and representation used internally by the model for this parameter")

    arguments: Optional[dict[str, str]] = Field(None, description="Additional arguments to control file encoding and serialization")
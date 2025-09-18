from typing import TypedDict, Any, Literal

InputSource = Literal["file", "primitive", "link", "path"]

class ModelInput(TypedDict):
    source: InputSource
    data: Any

class InvocationConfig(TypedDict):
    model_name: str
    inputs: dict[str, ModelInput]

class AdditionalModelFields(TypedDict, total=False):
    version_label: str
    hosting_location: Literal["Internal", "External"]
    output_format: Literal["Json", "Binary", "Text"]
    request_type: Literal["Get", "Post", "Put"]
    http_headers: dict[str, str]
    url: str


class AdditionalSignatureFields(TypedDict, total=False):
    receive_format: Literal["File", "Link", "Primitive"]
    http_location: Literal["Body", "Path", "Query"]
    default_value: str

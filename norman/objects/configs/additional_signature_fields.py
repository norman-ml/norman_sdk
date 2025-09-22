from typing import TypedDict, Literal


class AdditionalSignatureFields(TypedDict, total=False):
    receive_format: Literal["File", "Link", "Primitive"]
    http_location: Literal["Body", "Path", "Query"]
    default_value: str

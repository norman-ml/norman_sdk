from typing import TypedDict, Literal


class AdditionalModelFields(TypedDict, total=False):
    version_label: str
    hosting_location: Literal["Internal", "External"]
    output_format: Literal["Json", "Binary", "Text"]
    request_type: Literal["Get", "Post", "Put"]
    http_headers: dict[str, str]
    url: str

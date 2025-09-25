from typing import Literal, Any

from norman.objects.configs.invocation_config import InputSource


class ModelBuilder:
    def __init__(self, model_name: str, short_description: str, long_description: str):
        self._model_name = model_name
        self._short_description = short_description
        self._long_description = long_description
        self._additional_fields = {}
        self._inputs = []
        self._outputs = []
        self._assets = []

    def add_asset(self, asset_name: Literal["Logo", "File"], source: InputSource, data: Any) -> 'ModelBuilder':
        asset = {
            "asset_name": asset_name,
            "source": source,
            "data": data
        }
        self._assets.append(asset)
        return self

    def add_input(self, model_signature: dict[str, Any]) -> 'ModelBuilder':
        self._inputs.append(model_signature)
        return self

    def add_output(self, model_signature: dict[str, Any]) -> 'ModelBuilder':
        self._outputs.append(model_signature)
        return self

    def add_version_label(self, version_label: str) -> 'ModelBuilder':
        self._additional_fields["version_label"] = version_label
        return self

    def add_hosting_location(self, hosting_location: Literal["Internal", "External"]) -> 'ModelBuilder':
        self._additional_fields["hosting_location"] = hosting_location
        return self

    def add_output_format(self, output_format: Literal["Json", "Binary", "Text"]) -> 'ModelBuilder':
        self._additional_fields["output_format"] = output_format
        return self

    def add_request_type(self, request_type: Literal["Get", "Post", "Put"]) -> 'ModelBuilder':
        self._additional_fields["request_type"] = request_type
        return self

    def add_http_headers(self, http_headers: dict[str, str]) -> 'ModelBuilder':
        self._additional_fields["http_headers"] = http_headers
        return self

    def add_url(self, url: str) -> 'ModelBuilder':
        self._additional_fields["url"] = url
        return self

    def build(self) -> dict[str, Any]:
        return {
            "name": self._model_name,
            "short_description": self._short_description,
            "long_description": self._long_description,
            "assets": self._assets,
            "inputs": self._inputs,
            "outputs": self._outputs,
            **self._additional_fields
        }

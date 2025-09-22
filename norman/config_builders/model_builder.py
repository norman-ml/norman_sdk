from typing import Literal, Any

from typing_extensions import Unpack

from norman.objects.configs.additional_model_fields import AdditionalModelFields
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

    def add_additional_fields(self, **kwargs: Unpack[AdditionalModelFields]) -> 'ModelBuilder':
        self._additional_fields.update(kwargs)
        return self

    def build(self):
        config = {
            "name": self._model_name,
            "short_description": self._short_description,
            "long_description": self._long_description,
            "assets": self._assets,
            "inputs": self._inputs,
            "outputs": self._outputs
        }
        config.update(self._additional_fields)
        return config

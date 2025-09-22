from typing_extensions import Unpack

from norman.objects.configs.additional_signature_fields import AdditionalSignatureFields


class ModelSignatureBuilder:
    def __init__(self, display_title: str, data_domain: str, data_encoding: str):
        self._display_title = display_title
        self._data_domain = data_domain
        self._data_encoding = data_encoding

        self._additional_fields = {}
        self._parameters = []

    def add_parameter(self, name: str, data_domain: str, data_encoding: str) -> 'ModelSignatureBuilder':
        parameter = {
            "parameter_name": name,
            "data_domain": data_domain,
            "data_encoding": data_encoding,
        }
        self._parameters.append(parameter)
        return self

    def add_additional_fields(self, **kwargs: Unpack[AdditionalSignatureFields]) -> 'ModelSignatureBuilder':
        self._additional_fields.update(kwargs)
        return self

    def build(self):
        return {
            "display_title": self._display_title,
            "data_domain": self._data_domain,
            "data_encoding": self._data_encoding,
            "parameters": self._parameters
        }

from typing import Literal


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

    def add_http_location(self, http_location: Literal["Body", "Path", "Query"]) -> 'ModelSignatureBuilder':
        self._additional_fields["http_location"] = http_location
        return self

    def add_receive_format(self, receive_format: Literal["File", "Link", "Primitive"]) -> 'ModelSignatureBuilder':
        self._additional_fields["receive_format"] = receive_format
        return self


    def add_default_value(self, default_value: str) -> 'ModelSignatureBuilder':
        self._additional_fields["default_value"] = default_value
        return self

    def build(self):
        return {
            "display_title": self._display_title,
            "data_domain": self._data_domain,
            "data_encoding": self._data_encoding,
            "parameters": self._parameters,
            **self._additional_fields,
        }

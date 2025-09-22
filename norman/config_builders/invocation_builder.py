from typing import Literal, Any

from norman.norman_types import InputSource, InvocationConfig


class InvocationBuilder:
    def __init__(self, model_name: str):
        self._model_name = model_name
        self._inputs = {}

    def add_input(self, display_name: Literal["Logo", "File"], source: InputSource, data: Any) -> 'InvocationBuilder':
        input = {
            "source": source,
            "data": data
        }
        self._inputs[display_name] = input
        return self

    def build(self) -> InvocationConfig:
        config = {
            "model_name": self._model_name,
            "inputs": self._inputs,
        }
        return config

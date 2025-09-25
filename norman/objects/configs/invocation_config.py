from typing import TypedDict, Any, Literal

InputSource = Literal["Link", "Path", "Primitive", "Stream"]

class ModelInput(TypedDict):
    source: InputSource
    data: Any

class InvocationConfig(TypedDict):
    model_name: str
    inputs: dict[str, ModelInput]
